#!/usr/bin/env python3
"""Build pff.player_xwalk: match pff.* player rows to core.roster athletes.

PFF's player_id namespace shares nothing with CFBD's athlete ids; this
script fills the crosswalk shell created by migration 061 so PFF grades can
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
  pff.team_map) against core.roster.team. A transfer's multiple PFF school
  observations union to the same athlete id.
- Roster candidates are SEASON-SCOPED: a PFF observation from season S is
  matched against roster rows for S first, then S+-1 (roster gaps), then
  any season -- each tier only widening when the tighter one found nobody.
  The tier is resolved per observation and the candidate sets are then
  unioned, so a transfer's second school contributes its own candidates
  even when the first school hit at a tighter tier; match_method records
  the widest tier any observation needed.
  The first cut of this script matched against every roster season since
  2004, so "J. Smith @ Kentucky" collided with every J. Smith Kentucky ever
  rostered: a flat ~12% ambiguous rate across all positions/families
  (88.3% matched on the 2023-2025 backfill, 2026-09-04).
- Within a tier, two or more candidates are narrowed on the full first-name
  token (PFF "Tycoolhill Luman" vs "Tyclean Luman", both FAU); a unique
  survivor matches, otherwise the id stays ambiguous.
- NEVER guess beyond that: a key with two or more surviving candidates is
  left unmatched and reported (ambiguous), as is a key with none. Only
  unique matches are written; match_method records the tier that resolved
  it.

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
from collections import Counter, defaultdict
from dataclasses import dataclass, field

from src.pipelines.utils.load_ledger import get_db_url

# Generational suffixes PFF appends to display names and CFBD sometimes
# retains inside last_name. Compared after uppercasing + period removal.
SUFFIX_TOKENS = frozenset({"JR", "SR", "II", "III", "IV", "V"})

MATCH_METHOD = "last_name+first_initial+school"

# Candidate tiers, tightest first. Each is a predicate on (roster years for
# the athlete at this key, PFF observation season); the matcher only widens
# to the next tier when the current one yields no candidate at all.
SEASON_TIERS: tuple[tuple[str, object], ...] = (
    ("season", lambda years, season: season in years),
    ("season+-1", lambda years, season: any(abs(y - season) <= 1 for y in years)),
    ("any_season", lambda years, season: True),
)

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
        pff_players: iterable of {"pff_player_id", "player", "school",
            "season"} dicts -- possibly several observations per id
            (transfers, multiple seasons).
        roster_rows: iterable of {"athlete_id", "first_name", "last_name",
            "team", "year"} dicts spanning any/all roster seasons.
    """
    # key -> athlete_id -> roster years seen at that key
    index: dict[tuple[str, str, str], dict[str, set[int]]] = defaultdict(lambda: defaultdict(set))
    # athlete_id -> normalized first-name tokens seen on any roster row
    first_tokens: dict[str, set[str]] = defaultdict(set)
    for row in roster_rows:
        key = roster_match_key(row["first_name"], row["last_name"], row["team"])
        if not key:
            continue
        athlete = str(row["athlete_id"])
        index[key][athlete].add(int(row["year"]))
        first_tokens[athlete].add(_name_tokens(row["first_name"])[0])

    observations: dict[int, list[tuple[str, str, int]]] = defaultdict(list)
    for p in pff_players:
        observations[int(p["pff_player_id"])].append((p["player"], p["school"], int(p["season"])))

    result = MatchResult(matches=[], ambiguous=[], unmatched=[])
    for pff_id in sorted(observations):
        obs = observations[pff_id]
        obs_pairs = tuple((name, school) for name, school, _ in dict.fromkeys(obs))

        # Resolve the tier PER OBSERVATION, then union. A transfer seen at
        # A/2024 (exact-season hit) and B/2023 (roster gap, +-1 hit) must
        # contribute both candidate sets so a conflicting identity surfaces
        # as ambiguous instead of the first observation winning silently.
        candidates: set[str] = set()
        widest_tier = -1
        for player, school, season in obs:
            key = pff_match_key(player, school)
            if not key:
                continue
            for tier_idx, (_, in_tier) in enumerate(SEASON_TIERS):
                found = {a for a, years in index.get(key, {}).items() if in_tier(years, season)}
                if found:
                    candidates |= found
                    widest_tier = max(widest_tier, tier_idx)
                    break
        if not candidates:
            result.unmatched.append(UnresolvedPlayer(pff_id, obs_pairs))
            continue

        method = f"{MATCH_METHOD}+{SEASON_TIERS[widest_tier][0]}"
        if len(candidates) > 1:
            pff_firsts = {_name_tokens(name)[0] for name, _, _ in obs if _name_tokens(name)}
            narrowed = {a for a in candidates if first_tokens[a] & pff_firsts}
            # Report the first-name survivors when there are any (the
            # useful list for hand review); an empty narrowing (nickname
            # vs legal name) keeps the full set and never guesses.
            if narrowed:
                candidates = narrowed
            if len(narrowed) == 1:
                method += "+first_name"

        if len(candidates) == 1:
            result.matches.append(
                {
                    "pff_player_id": pff_id,
                    "athlete_id": next(iter(candidates)),
                    "match_method": method,
                }
            )
        else:
            result.ambiguous.append(UnresolvedPlayer(pff_id, obs_pairs, sorted(candidates)))
    return result


# ---------------------------------------------------------------------------
# DB flow (not unit-tested -- runs only against the real warehouse)
# ---------------------------------------------------------------------------


def _fetch_pff_players(cur, skip_ids: set[int]) -> list[dict]:
    union = "\nUNION\n".join(
        f"SELECT player_id, player, school, season FROM pff.{t}" for t in PFF_FAMILY_TABLES
    )
    cur.execute(union)
    return [
        {"pff_player_id": pid, "player": player, "school": school, "season": season}
        for pid, player, school, season in cur.fetchall()
        if int(pid) not in skip_ids
    ]


def _fetch_roster(cur) -> list[dict]:
    cur.execute(
        "SELECT DISTINCT id::text, first_name, last_name, team, year "
        "FROM core.roster WHERE team IS NOT NULL AND year IS NOT NULL"
    )
    return [
        {
            "athlete_id": athlete_id,
            "first_name": first,
            "last_name": last,
            "team": team,
            "year": year,
        }
        for athlete_id, first, last, team, year in cur.fetchall()
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
    by_method = Counter(m["match_method"] for m in result.matches)
    for method, n in sorted(by_method.items(), key=lambda kv: -kv[1]):
        print(f"    {method:<52} {n:6d}")
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
