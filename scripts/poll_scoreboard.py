#!/usr/bin/env python3
"""Poll CFBD /scoreboard and write live.scoreboard_snapshots rows.

Tier 3 analytics (docs/plans/2026-07-21-tier3-analytics-plan.md), Pillar D,
Phase 8 ("Live wiring"). Backs the Saturday in-game dashboard: one ONE call
to /scoreboard per invocation (via .github/workflows/live-scoreboard.yml's
5-minute cron), computing the house closed-form live win probability inline
for every in-progress or completed-today game and appending a row to
live.scoreboard_snapshots per game (src/schemas/migrations/028_live_schema.sql).

Architecture mirrors scripts/compute_predictions.py: the WP math
(`house_live_home_wp`, `clamp`), clock parsing (`parse_clock`), and the
dedup hash (`snapshot_hash`) are pure -- plain floats/strings, no I/O -- so
they are fully unit-testable without a database or network (see
tests/test_live_wp.py). `parse_scoreboard_game` is also pure, but defensive
by design: CFBD's /scoreboard ScoreboardGame shape is only loosely pinned
down (see docs/plans/2026-07-21-p34-realtime-go-no-go.md's fetched OpenAPI
spec plus this file's own field-name fallbacks below), so it tries several
plausible key names per field and returns None (logging a warning) for
entries it can't turn into a valid row, rather than raising. Everything
below `# --- I/O layer ---` is a thin wrapper: fetch /scoreboard, fetch
house Elo + wp_params, drive the math, upsert-or-skip into
live.scoreboard_snapshots.

House live WP formula (from migration 028's header -- reproduced here only
as a one-line pointer, not re-derived):

    f = clamp(seconds_remaining / 3600, eps, 1)
    projected = current_margin + pregame_expected_margin * f
    home_wp = Phi(projected / (sigma * sqrt(f)))

where current_margin = home_points - away_points, pregame_expected_margin
comes from house Elo (scripts.compute_predictions.elo_margin, imported
below -- not re-derived), sigma comes from live.wp_params (id=1, fit by
scripts/calibrate_live_wp.py), and Phi is the standard normal CDF via
stdlib math.erf.

Usage:
    python scripts/poll_scoreboard.py
        Fetch /scoreboard once, compute house_live_home_wp for every
        in-progress or completed-today game, and insert a
        live.scoreboard_snapshots row per game -- skipping any game whose
        computed snapshot_hash matches its own latest stored snapshot (a
        TV-timeout tick with no actual state change). Prints:
            SCOREBOARD_POLL games=<n> inserted=<i> deduped=<d> statuses={...}
        A clean no-op (games=0, exit 0) when /scoreboard returns nothing
        (off-season) or nothing is currently in-progress/completed-today.

    python scripts/poll_scoreboard.py --dry-run
        Same fetch + compute, but no DB writes -- for the workflow_dispatch
        manual smoke test. Still reads live.wp_params / house Elo (both
        read-only) to compute a realistic house_live_home_wp.
"""

import argparse
import hashlib
import logging
import math
import sys
from collections import Counter

from scripts.compute_predictions import elo_margin, fetch_elo_current, resolve_elo
from src.pipelines.config.years import get_current_season
from src.pipelines.utils.api_client import get_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# =============================================================================
# Pure core -- no I/O, no DB, unit-tested directly (tests/test_live_wp.py).
# =============================================================================


def clamp(value: float, lo: float, hi: float) -> float:
    """Clamp `value` into [lo, hi]."""
    return max(lo, min(hi, value))


def house_live_home_wp(
    current_margin: float,
    pregame_expected_margin: float,
    seconds_remaining: float,
    sigma: float,
    eps: float = 1.0 / 3600.0,
) -> float:
    """House closed-form live home win probability (migration 028's header).

        f = clamp(seconds_remaining / 3600, eps, 1)
        projected = current_margin + pregame_expected_margin * f
        home_wp = Phi(projected / (sigma * sqrt(f)))

    Boundary behavior: as f -> 0 (game ending), wp -> {0, 1} by the sign of
    current_margin (eps keeps the denominator off zero at the final tick
    instead of producing a NaN); at f = 1 (kickoff, current_margin = 0), wp
    reduces to Phi(pregame_expected_margin / sigma) -- monotone in, but not
    numerically identical to, the Elo-logistic pregame win probability
    (different model family). Phi is the standard normal CDF, computed via
    stdlib math.erf (no scipy/numpy dependency).
    """
    f = clamp(seconds_remaining / 3600.0, eps, 1.0)
    projected = current_margin + pregame_expected_margin * f
    z = projected / (sigma * math.sqrt(f))
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def parse_clock(clock: str | None, period: int | None) -> int | None:
    """Seconds remaining in regulation, from an "MM:SS" clock string + period.

    Regulation is 4 x 900s periods (3600s total): seconds_remaining =
    (4 - period) * 900 + clock_seconds.

    Overtime rule (documented, deliberate simplification): period > 4
    collapses to seconds_remaining = 0 rather than attempting to model OT's
    untimed, sudden-death-after-2OT format on a 3600s regulation clock. That
    0 then runs through house_live_home_wp's own clamp(..., eps, 1), which
    floors it back up to the eps epsilon -- so OT states are driven almost
    entirely by the current score, which is the intended behavior, not a
    bug.

    Returns None (rather than raising) for a missing/malformed clock or a
    missing period -- callers store the raw snapshot either way but skip
    computing house_live_home_wp for that tick when this returns None.
    """
    if period is not None and period > 4:
        return 0
    if clock is None or period is None:
        return None
    try:
        minutes_str, seconds_str = str(clock).split(":")
        minutes = int(minutes_str)
        seconds = int(seconds_str)
    except (ValueError, AttributeError):
        return None
    if minutes < 0 or not (0 <= seconds < 60):
        return None
    clock_seconds = minutes * 60 + seconds
    remaining_periods = max(4 - period, 0)
    return remaining_periods * 900 + clock_seconds


def snapshot_hash(
    game_id: int,
    period: int | None,
    clock: str | None,
    home_points: int | None,
    away_points: int | None,
    possession: str | None,
) -> str:
    """MD5 over the fields that define "the same game state".

    Used to dedup TV-timeout poll ticks: if a game's (period, clock,
    home_points, away_points, possession) are unchanged since its latest
    stored snapshot, the new tick is skipped rather than inserted --
    live.scoreboard_snapshots is otherwise append-only (migration 028's
    header). Any one of these fields changing (a play happened, the clock
    ticked, possession flipped) produces a different hash and a new row.
    """
    raw = "|".join(str(v) for v in (game_id, period, clock, home_points, away_points, possession))
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


# Status strings are normalized (lower-cased, spaces -> underscores) before
# comparison, since CFBD's exact casing/spacing isn't contractually pinned
# down. A few plausible spellings are included per bucket defensively; games
# in neither bucket (e.g. "scheduled", not started yet) are skipped.
IN_PROGRESS_STATUSES = {"in_progress", "inprogress", "live"}
COMPLETED_STATUSES = {"completed", "final", "closed"}
RELEVANT_STATUSES = IN_PROGRESS_STATUSES | COMPLETED_STATUSES


def _first(d: dict, *keys: str, default=None):
    """Return the first present, non-None value among `keys` in dict `d`."""
    for key in keys:
        value = d.get(key)
        if value is not None:
            return value
    return default


def parse_scoreboard_game(raw: dict) -> dict | None:
    """Defensively extract the fields a live.scoreboard_snapshots row needs
    from one /scoreboard response entry (CFBD's ScoreboardGame shape).

    Field-name assumptions (see docs/plans/2026-07-21-p34-realtime-go-no-go.md's
    fetched-live OpenAPI spec, cross-checked against this task's brief):
      - game id: `id`, falling back to `gameId`.
      - team identity: `homeTeam` / `awayTeam` objects, each with a
        `name` (fallback `team`, `school`) and `points`.
      - `status`, `period`, `clock`, `possession` are top-level fields.
        `possession` may itself be a nested object with a `team`/`name`
        key (rare) -- both shapes are handled.
      - market line: a top-level `betting` object with `spread` and
        `overUnder`.
      - live win probability: CFBD's own spec places this at
        `homeTeam.winProbability` / `awayTeam.winProbability`; a flat
        `homeWinProbability` (or `winProbability`) field is also tried
        first in case a future response shape flattens it.
      - `neutralSite`, `season`, `week`, `seasonType` follow the same
        camelCase convention CFBD uses elsewhere (core.games' loader).

    Returns None (logging a warning) if the entry has no usable game id or
    is missing a home/away team name -- these can't become a valid
    snapshot row. Never raises; malformed entries are the caller's problem
    to skip, not this function's to crash on.
    """
    game_id_raw = _first(raw, "id", "gameId")
    if game_id_raw is None:
        logger.warning("Skipping /scoreboard entry with no id/gameId: %r", raw)
        return None
    try:
        game_id = int(game_id_raw)
    except (TypeError, ValueError):
        logger.warning("Skipping /scoreboard entry with non-numeric id/gameId: %r", game_id_raw)
        return None

    home = raw.get("homeTeam")
    away = raw.get("awayTeam")
    if not isinstance(home, dict) or not isinstance(away, dict):
        logger.warning("game_id=%s: homeTeam/awayTeam missing or malformed, skipping", game_id)
        return None

    home_team = _first(home, "name", "team", "school")
    away_team = _first(away, "name", "team", "school")
    if not home_team or not away_team:
        logger.warning("game_id=%s: missing home/away team name, skipping", game_id)
        return None

    betting = raw.get("betting")
    if not isinstance(betting, dict):
        betting = {}

    status_raw = raw.get("status")
    status = str(status_raw).strip().lower().replace(" ", "_") if status_raw is not None else None

    possession_raw = raw.get("possession")
    if isinstance(possession_raw, dict):
        possession = _first(possession_raw, "team", "name")
    else:
        possession = possession_raw

    cfbd_home_wp = _first(raw, "homeWinProbability", "winProbability")
    if cfbd_home_wp is None:
        cfbd_home_wp = _first(home, "winProbability")

    return {
        "game_id": game_id,
        "season": raw.get("season"),
        "week": raw.get("week"),
        "season_type": _first(raw, "seasonType", "season_type"),
        "status": status,
        "period": raw.get("period"),
        "clock": raw.get("clock"),
        "home_team": str(home_team),
        "away_team": str(away_team),
        "home_points": _first(home, "points"),
        "away_points": _first(away, "points"),
        "possession": possession,
        "neutral_site": bool(_first(raw, "neutralSite", "neutral_site", default=False)),
        "spread": _first(betting, "spread"),
        "over_under": _first(betting, "overUnder", "over_under"),
        "cfbd_home_wp": float(cfbd_home_wp) if cfbd_home_wp is not None else None,
    }


# =============================================================================
# --- I/O layer --- (thin: fetch /scoreboard + house Elo/wp_params, write)
# =============================================================================


def get_db_url() -> str:
    """Get database URL from dlt secrets or environment.

    Copied from scripts/compute_house_elo.py's get_db_url pattern (each
    compute_*.py / poll script keeps its own copy rather than importing
    across scripts for this one utility).
    """
    import os

    import dlt

    url = None
    try:
        creds = dlt.secrets.get("destination.postgres.credentials")
        if creds:
            url = str(creds)
    except Exception:
        pass

    if not url:
        url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")

    if not url:
        raise RuntimeError(
            "No database URL found. Set destination.postgres.credentials in "
            ".dlt/secrets.toml or SUPABASE_DB_URL environment variable."
        )

    if "options=" not in url:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}options=-c%20statement_timeout%3D0"

    return url


def fetch_wp_params(conn) -> tuple[float, float | None]:
    """(sigma, blend_weight) from live.wp_params id=1. Falls back to the
    migration 028 seed (sigma=16.0) with a warning if the row is somehow
    missing (shouldn't happen -- the migration seeds it on ON CONFLICT DO
    NOTHING)."""
    with conn.cursor() as cur:
        cur.execute("SELECT sigma, blend_weight FROM live.wp_params WHERE id = 1")
        row = cur.fetchone()
    if row is None:
        logger.warning("live.wp_params has no id=1 row; falling back to seed sigma=16.0")
        return 16.0, None
    sigma, blend_weight = row
    return float(sigma), (float(blend_weight) if blend_weight is not None else None)


def fetch_latest_hashes(conn, game_ids: list[int]) -> dict[int, str]:
    """Latest stored snapshot_hash per game_id, for the dedup check."""
    if not game_ids:
        return {}
    query = """
        SELECT DISTINCT ON (game_id) game_id, snapshot_hash
        FROM live.scoreboard_snapshots
        WHERE game_id = ANY(%s)
        ORDER BY game_id, captured_at DESC
    """
    with conn.cursor() as cur:
        cur.execute(query, (game_ids,))
        return dict(cur.fetchall())


_INSERT_SQL = """
    INSERT INTO live.scoreboard_snapshots (
        season, week, season_type, game_id, status, period, clock,
        seconds_remaining, home_team, away_team, home_points, away_points,
        possession, spread, over_under, cfbd_home_wp, house_live_home_wp,
        pregame_expected_margin, snapshot_hash
    ) VALUES %s
"""

_ROW_COLUMNS = [
    "season",
    "week",
    "season_type",
    "game_id",
    "status",
    "period",
    "clock",
    "seconds_remaining",
    "home_team",
    "away_team",
    "home_points",
    "away_points",
    "possession",
    "spread",
    "over_under",
    "cfbd_home_wp",
    "house_live_home_wp",
    "pregame_expected_margin",
    "snapshot_hash",
]


def write_snapshots(conn, rows: list[dict]) -> None:
    from psycopg2.extras import execute_values

    if not rows:
        return
    values = [tuple(r[c] for c in _ROW_COLUMNS) for r in rows]
    with conn.cursor() as cur:
        execute_values(cur, _INSERT_SQL, values)
    conn.commit()


def run(conn, raw_games: list[dict], dry_run: bool = False) -> None:
    """Parse + filter /scoreboard, compute WP, write (or print) snapshot rows."""
    all_parsed = []
    for raw in raw_games:
        try:
            game = parse_scoreboard_game(raw)
        except Exception:
            logger.exception("Failed to parse a /scoreboard entry, skipping: %r", raw)
            continue
        if game is not None:
            all_parsed.append(game)

    relevant = [g for g in all_parsed if g["status"] in RELEVANT_STATUSES]
    status_counts = Counter(g["status"] for g in relevant)

    if not relevant:
        logger.info("No in-progress or completed-today games found; nothing to do")
        print("SCOREBOARD_POLL games=0 inserted=0 deduped=0 statuses={}")
        return

    elo_current = fetch_elo_current(conn)
    sigma, _blend_weight = fetch_wp_params(conn)
    game_ids = [g["game_id"] for g in relevant]
    latest_hashes = fetch_latest_hashes(conn, game_ids)

    rows_to_insert: list[dict] = []
    deduped = 0
    for game in relevant:
        season = game["season"] if game["season"] is not None else get_current_season()
        home_elo = resolve_elo(game["home_team"], season, elo_current)
        away_elo = resolve_elo(game["away_team"], season, elo_current)
        pregame_expected_margin = elo_margin(home_elo, away_elo, game["neutral_site"])

        seconds_remaining = parse_clock(game["clock"], game["period"])
        home_points = game["home_points"] if game["home_points"] is not None else 0
        away_points = game["away_points"] if game["away_points"] is not None else 0
        current_margin = home_points - away_points

        house_wp = None
        if seconds_remaining is not None:
            house_wp = house_live_home_wp(
                current_margin, pregame_expected_margin, seconds_remaining, sigma
            )

        game_hash = snapshot_hash(
            game["game_id"],
            game["period"],
            game["clock"],
            home_points,
            away_points,
            game["possession"],
        )

        if latest_hashes.get(game["game_id"]) == game_hash:
            deduped += 1
            if dry_run:
                logger.info(f"[dry-run] game_id={game['game_id']} DEDUP (hash unchanged)")
            continue

        row = {
            "season": season,
            "week": game["week"],
            "season_type": game["season_type"],
            "game_id": game["game_id"],
            "status": game["status"],
            "period": game["period"],
            "clock": game["clock"],
            "seconds_remaining": seconds_remaining,
            "home_team": game["home_team"],
            "away_team": game["away_team"],
            "home_points": home_points,
            "away_points": away_points,
            "possession": game["possession"],
            "spread": game["spread"],
            "over_under": game["over_under"],
            "cfbd_home_wp": game["cfbd_home_wp"],
            "house_live_home_wp": house_wp,
            "pregame_expected_margin": pregame_expected_margin,
            "snapshot_hash": game_hash,
        }
        rows_to_insert.append(row)
        if dry_run:
            logger.info(
                f"[dry-run] game_id={game['game_id']} {game['home_team']} {home_points}-"
                f"{away_points} {game['away_team']} status={game['status']} "
                f"period={game['period']} clock={game['clock']} "
                f"house_wp={house_wp} cfbd_wp={game['cfbd_home_wp']}"
            )

    if not dry_run:
        write_snapshots(conn, rows_to_insert)
    else:
        conn.rollback()

    print(
        f"SCOREBOARD_POLL games={len(relevant)} inserted={len(rows_to_insert)} "
        f"deduped={deduped} statuses={dict(status_counts)}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Poll CFBD /scoreboard once and write live.scoreboard_snapshots rows "
        "with the house closed-form live win probability."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch + compute + print only; no DB writes (workflow_dispatch smoke test).",
    )
    args = parser.parse_args()

    client = get_client()
    try:
        raw_games = client.get("/scoreboard", params={"classification": "fbs"})
    finally:
        client.close()

    if not raw_games:
        logger.info("No games returned from /scoreboard (off-season or no games today)")
        print("SCOREBOARD_POLL games=0 inserted=0 deduped=0 statuses={}")
        return

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        run(conn, raw_games, dry_run=args.dry_run)
    except Exception:
        conn.rollback()
        logger.exception("Scoreboard poll failed")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
