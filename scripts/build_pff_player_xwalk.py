#!/usr/bin/env python3
"""Build pff.player_xwalk: match pff.* player rows to core.roster athletes.

PFF's player_id namespace shares nothing with CFBD's athlete ids; this
script fills the crosswalk shell created by migration 059 so PFF grades can
join the rest of the warehouse. Matching rule (validated in
docs/brainstorms/2026-09-01-pff-plus-api.md sections 5a+: 96% raw at QB,
91% raw / ~99% suffix-fixed at CB/S/LB/DI/ED):

    normalized last-name token + first initial + resolved school

- Both sides are uppercased, periods dropped, and trailing generational
  suffixes stripped (JR, SR, II, III, IV, V -- with or without periods).
  PFF appends suffixes to the display name while CFBD's last_name retains
  them, the one systematic miss in validation.
- Multi-token surnames/particles ("Van Buren", "Del Rio-Wilson") compare on
  the FINAL surname token on both sides, so CFBD's whole-surname last_name
  and PFF's space-split display name land on the same key.
- School is the CFBD full name: pff.*.school (pre-resolved via
  pff.team_map) against core.roster.team, matched over ANY roster season --
  the roster spans seasons, and a transfer's multiple PFF school
  observations union to the same athlete id.
- NEVER guess: a key with two or more candidate athletes is left unmatched
  and reported (ambiguous), as is a key with none. Only unique matches are
  written.

Writes: INSERT ... ON CONFLICT (pff_player_id) DO UPDATE into
pff.player_xwalk (athlete_id, match_method, matched_at). Already-matched
ids are skipped unless --rebuild. Unmatched and ambiguous players go to the
stdout report only -- no row is written for them, so a later run (better
data, better rule) picks them up automatically.

Usage:
    python scripts/build_pff_player_xwalk.py             # match new ids only
    python scripts/build_pff_player_xwalk.py --rebuild   # rematch everything
    python scripts/build_pff_player_xwalk.py --dry-run   # report, write nothing

Requires SUPABASE_DB_URL (or .dlt/secrets.toml destination credentials).
The matching core (``pff_match_key``/``roster_match_key``/``match_players``)
is pure and unit-tested in tests/test_pff_player_xwalk.py.
"""

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass, field

from src.pipelines.utils.load_ledger import get_db_url

# Generational suffixes PFF appends to display names and CFBD sometimes
# retains inside last_name. Compared after uppercasing + period removal.
SUFFIX_TOKENS = frozenset({"JR", "SR", "II", "III", "IV", "V"})

MATCH_METHOD = "last_name+first_initial+school"

PFF_FAMILY_TABLES = (
    "passing_summary",
    "receiving_summary",
    "rushing_summary",
    "offense_blocking",
    "defense_summary",
)


def _name_tokens(name: str) -> list[str]:
    """Uppercase, drop periods, split, and strip trailing suffix tokens
    (never below one token -- a name that IS a suffix stays itself).
    """
    tokens = (name or "").upper().replace(".", " ").split()
    while len(tokens) > 1 and tokens[-1] in SUFFIX_TOKENS:
        tokens.pop()
    return tokens


def pff_match_key(display_name: str, school: str) -> tuple[str, str, str] | None:
    """(school, first initial, final surname token) for a PFF display name."""
    tokens = _name_tokens(display_name)
    if not tokens:
        return None
    return (school, tokens[0][0], tokens[-1])


def roster_match_key(
    first_name: str | None, last_name: str | None, school: str
) -> tuple[str, str, str] | None:
    """(school, first initial, final surname token) for a roster row."""
    first_tokens = _name_tokens(first_name or "")
    last_tokens = _name_tokens(last_name or "")
    if not first_tokens or not last_tokens:
        return None
    return (school, first_tokens[0][0], last_tokens[-1])


@dataclass(frozen=True)
class UnresolvedPlayer:
    """One PFF player the matcher refused to write: no candidate
    (unmatched) or several (ambiguous -- never guessed).
    """

    pff_player_id: int
    observations: tuple[tuple[str, str], ...]  # (display name, school) pairs
    candidates: list[str] = field(default_factory=list)


@dataclass
class MatchResult:
    matches: list[dict]
    ambiguous: list[UnresolvedPlayer]
    unmatched: list[UnresolvedPlayer]

    @property
    def total(self) -> int:
        return len(self.matches) + len(self.ambiguous) + len(self.unmatched)


def match_players(pff_players, roster_rows) -> MatchResult:
    """Pure matching core.

    Args:
        pff_players: iterable of {"pff_player_id", "player", "school"} dicts
            -- possibly several observations per id (transfers).
        roster_rows: iterable of {"athlete_id", "first_name", "last_name",
            "team"} dicts spanning any/all roster seasons.
    """
    index: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for row in roster_rows:
        key = roster_match_key(row["first_name"], row["last_name"], row["team"])
        if key:
            index[key].add(str(row["athlete_id"]))

    observations: dict[int, list[tuple[str, str]]] = defaultdict(list)
    for p in pff_players:
        observations[int(p["pff_player_id"])].append((p["player"], p["school"]))

    result = MatchResult(matches=[], ambiguous=[], unmatched=[])
    for pff_id in sorted(observations):
        obs = observations[pff_id]
        candidates: set[str] = set()
        for player, school in obs:
            key = pff_match_key(player, school)
            if key:
                candidates |= index.get(key, set())

        if len(candidates) == 1:
            result.matches.append(
                {
                    "pff_player_id": pff_id,
                    "athlete_id": next(iter(candidates)),
                    "match_method": MATCH_METHOD,
                }
            )
        elif candidates:
            result.ambiguous.append(UnresolvedPlayer(pff_id, tuple(obs), sorted(candidates)))
        else:
            result.unmatched.append(UnresolvedPlayer(pff_id, tuple(obs)))
    return result


# ---------------------------------------------------------------------------
# DB flow (not unit-tested -- runs only against the real warehouse)
# ---------------------------------------------------------------------------


def _fetch_pff_players(cur, skip_ids: set[int]) -> list[dict]:
    union = "\nUNION\n".join(
        f"SELECT player_id, player, school FROM pff.{t}" for t in PFF_FAMILY_TABLES
    )
    cur.execute(union)
    return [
        {"pff_player_id": pid, "player": player, "school": school}
        for pid, player, school in cur.fetchall()
        if int(pid) not in skip_ids
    ]


def _fetch_roster(cur) -> list[dict]:
    cur.execute(
        "SELECT DISTINCT id::text, first_name, last_name, team "
        "FROM core.roster WHERE team IS NOT NULL"
    )
    return [
        {"athlete_id": athlete_id, "first_name": first, "last_name": last, "team": team}
        for athlete_id, first, last, team in cur.fetchall()
    ]


def _write_matches(cur, matches: list[dict]) -> None:
    from psycopg2.extras import execute_values

    execute_values(
        cur,
        "INSERT INTO pff.player_xwalk (pff_player_id, athlete_id, match_method, matched_at) "
        "VALUES %s "
        "ON CONFLICT (pff_player_id) DO UPDATE SET "
        "athlete_id = EXCLUDED.athlete_id, "
        "match_method = EXCLUDED.match_method, "
        "matched_at = EXCLUDED.matched_at",
        [(m["pff_player_id"], m["athlete_id"], m["match_method"]) for m in matches],
        template="(%s, %s, %s, now())",
    )


def _report(result: MatchResult, skipped: int) -> None:
    total = result.total
    rate = (len(result.matches) / total * 100) if total else 0.0
    print(f"\n{'=' * 60}")
    print("PFF Player Xwalk Summary")
    print(f"{'=' * 60}")
    print(f"  candidates considered: {total} (already matched, skipped: {skipped})")
    print(f"  matched:   {len(result.matches):6d}  ({rate:.1f}%)")
    print(f"  ambiguous: {len(result.ambiguous):6d}  (left unmatched -- never guessed)")
    print(f"  unmatched: {len(result.unmatched):6d}")

    if result.ambiguous:
        print("\nAMBIGUOUS (multiple roster candidates; resolve by hand if needed):")
        for u in result.ambiguous:
            obs = "; ".join(f"{name} @ {school}" for name, school in u.observations)
            print(f"  pff_id={u.pff_player_id}  {obs}  candidates={u.candidates}")

    if result.unmatched:
        print("\nUNMATCHED (no roster candidate at last+initial+school):")
        for u in result.unmatched:
            obs = "; ".join(f"{name} @ {school}" for name, school in u.observations)
            print(f"  pff_id={u.pff_player_id}  {obs}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Match pff.* players to core.roster athletes and fill pff.player_xwalk."
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Rematch every PFF player id, including ones already in the xwalk",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Match and report but write nothing",
    )
    args = parser.parse_args(argv)

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        with conn.cursor() as cur:
            skip_ids: set[int] = set()
            if not args.rebuild:
                cur.execute("SELECT pff_player_id FROM pff.player_xwalk")
                skip_ids = {int(r[0]) for r in cur.fetchall()}

            pff_players = _fetch_pff_players(cur, skip_ids)
            roster = _fetch_roster(cur)
            print(
                f"{len(pff_players)} PFF player ids to match "
                f"({len(skip_ids)} already matched) against {len(roster)} roster rows"
            )

            result = match_players(pff_players, roster)

            if result.matches and not args.dry_run:
                _write_matches(cur, result.matches)
                conn.commit()
                print(f"wrote {len(result.matches)} rows to pff.player_xwalk")
            elif args.dry_run:
                print("[DRY RUN] no rows written")
    finally:
        conn.close()

    _report(result, skipped=len(skip_ids))
    return 0


if __name__ == "__main__":
    sys.exit(main())
