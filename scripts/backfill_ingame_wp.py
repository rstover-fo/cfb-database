#!/usr/bin/env python3
"""Resumable backfill + weekly forward-fill driver for metrics.win_probability
(docs/plans/2026-07-21-tier3-analytics-plan.md, Pillar D / Phase 7).

metrics.win_probability was effectively empty: the old loader
(src.pipelines.sources.metrics.win_probability_resource) called
`/metrics/wp?year=` but that endpoint is gameId-scoped, so every call 400'd
and was silently skipped. The fix is
src.pipelines.sources.metrics.win_probability_by_game_resource(game_ids),
which calls `/metrics/wp?gameId={id}` once per game. This script is the
driver: it finds completed games in core.games, skips ones already loaded,
and runs the new resource over the rest in small dlt pipeline.run() batches.

Budget: the CFBD monthly call budget is ALREADY enforced globally by
src.pipelines.sources.base.make_request (via
src.pipelines.utils.rate_limiter.get_rate_limiter) -- every /metrics/wp call
this script makes goes through that same client/make_request path, so a
RuntimeError is raised there if the monthly budget (Tier 3: 75,000
calls/month, see .dlt/config.toml) runs out mid-run. This script does NOT
re-implement that check.

--max-calls is a SEPARATE, per-run guard, not a monthly one: /metrics/wp is
one API call per game, so it caps how many games a single invocation of this
script processes (default 2000). The one-time 2015+ backfill is ~9,600 calls
-- too many for one comfortable run -- so --max-calls lets it be split across
several bounded, resumable invocations (e.g. repeated deploy/cron runs) that
each stop cleanly and report how many games are still pending; the next run
picks up where the last one left off via the loaded-game_id skip logic below.

Resumability: a game is considered loaded if its id already appears in
metrics.win_probability.game_id (SELECT DISTINCT -- fine at this table's
scale). Re-running this script is always safe.

Usage:
    python scripts/backfill_ingame_wp.py                  # one-time backfill, --from 2015
    python scripts/backfill_ingame_wp.py --from 2018
    python scripts/backfill_ingame_wp.py --season 2026     # weekly forward-fill (daily workflow)
    python scripts/backfill_ingame_wp.py --max-calls 500
    python scripts/backfill_ingame_wp.py --chunk-size 100

Each dlt pipeline.run() batch prints a machine-readable line:
    WP_BACKFILL chunk={i} games={n} loaded_rows={r} remaining={m}
where `remaining` is games still pending (not yet loaded) after that chunk,
including any left out entirely by --max-calls. Exits 0 whether or not there
was work to do (a clean no-op when nothing is pending is not a failure).
"""

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_FROM_SEASON = 2015
DEFAULT_MAX_CALLS = 2000
DEFAULT_CHUNK_SIZE = 200

# Completed games from a given season onward, oldest season/id first so a
# --max-calls-bounded run makes steady, deterministic progress through
# history rather than jumping around on each invocation.
GAMES_FROM_SEASON_QUERY = """
    SELECT id, season
    FROM core.games
    WHERE completed = true
      AND season >= %s
    ORDER BY season, id
"""

# Single-season variant for the weekly forward-fill (--season).
GAMES_FOR_SEASON_QUERY = """
    SELECT id, season
    FROM core.games
    WHERE completed = true
      AND season = %s
    ORDER BY season, id
"""


def get_db_url() -> str:
    """Get database URL from dlt secrets or environment.

    Copied from scripts/compute_house_elo.py's get_db_url pattern (each
    compute_*.py / backfill script keeps its own copy rather than importing
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


# =============================================================================
# Pure helpers -- no I/O, no DB, unit-tested directly (tests/test_ingame_wp_backfill.py).
# =============================================================================


def filter_unloaded(game_ids: list[int], loaded_ids: set[int]) -> list[int]:
    """Return the subset of `game_ids` not already present in `loaded_ids`.

    Order-preserving. `loaded_ids` is the resumability set (game_ids already
    in metrics.win_probability).
    """
    return [game_id for game_id in game_ids if game_id not in loaded_ids]


def apply_budget(game_ids: list[int], max_calls: int) -> tuple[list[int], int]:
    """Cap `game_ids` to at most `max_calls` entries (one API call per game).

    Returns (selected, remaining) where `selected` is the ordered prefix of
    `game_ids` to actually load this run, and `remaining` is how many were
    left out by the cap (0 if everything fit within budget). A non-positive
    max_calls selects nothing and leaves everything remaining.
    """
    if max_calls <= 0:
        return [], len(game_ids)
    selected = game_ids[:max_calls]
    remaining = max(0, len(game_ids) - max_calls)
    return selected, remaining


def chunk_ids(game_ids: list[int], chunk_size: int) -> list[list[int]]:
    """Partition `game_ids` into order-preserving chunks of at most `chunk_size`.

    Each chunk becomes one dlt pipeline.run() call, so a crash mid-backfill
    loses at most one chunk's worth of progress (already-loaded games are
    skipped on the next run regardless).
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    return [game_ids[i : i + chunk_size] for i in range(0, len(game_ids), chunk_size)]


# =============================================================================
# I/O layer -- thin: fetch core.games + already-loaded ids, drive the resource,
# write results, print progress.
# =============================================================================


def loaded_game_ids(cur) -> set[int]:
    """Return the set of game_ids already present in metrics.win_probability.

    Returns an empty set (rather than erroring) if the table doesn't exist
    yet -- pre-backfill, dlt may never have created it at all, since the old
    broken loader's calls all 400'd before any row was ever written.
    """
    cur.execute("SELECT to_regclass('metrics.win_probability')")
    if cur.fetchone()[0] is None:
        return set()
    cur.execute("SELECT DISTINCT game_id FROM metrics.win_probability")
    return {row[0] for row in cur.fetchall()}


def target_games(cur, from_season: int | None, season: int | None) -> list[tuple[int, int]]:
    """Fetch (game_id, season) pairs of completed games in scope, in load order."""
    if season is not None:
        cur.execute(GAMES_FOR_SEASON_QUERY, (season,))
    else:
        cur.execute(GAMES_FROM_SEASON_QUERY, (from_season,))
    return [(row[0], row[1]) for row in cur.fetchall()]


def rows_loaded(pipeline, resource_name: str) -> int:
    """Best-effort row count for `resource_name` from the pipeline's last run.

    Reads dlt's own normalize-step metrics (row counts per table) rather than
    tracking counts ourselves. Falls back to 0 if the trace isn't available
    (e.g. nothing was extracted).
    """
    try:
        trace = pipeline.last_trace
        if trace is None:
            return 0
        normalize_info = trace.last_normalize_info
        if normalize_info is None:
            return 0
        return int(normalize_info.row_counts.get(resource_name, 0))
    except Exception:
        logger.debug("Could not read row counts from pipeline trace", exc_info=True)
        return 0


def run_backfill(
    from_season: int | None,
    season: int | None,
    max_calls: int,
    chunk_size: int,
) -> None:
    """Drive one invocation of the backfill/forward-fill. Always completes cleanly."""
    import dlt
    import psycopg2

    from src.pipelines.sources.metrics import win_probability_by_game_resource

    conn = psycopg2.connect(get_db_url())
    try:
        with conn.cursor() as cur:
            games = target_games(cur, from_season, season)
            loaded = loaded_game_ids(cur)
    finally:
        conn.close()

    total = len(games)
    pending = filter_unloaded([game_id for game_id, _ in games], loaded)
    skipped = total - len(pending)
    logger.info(
        f"{total} completed game(s) in scope, {skipped} already loaded, {len(pending)} pending"
    )

    to_load, left_out_by_budget = apply_budget(pending, max_calls)

    if not to_load:
        if pending:
            logger.info(
                f"--max-calls={max_calls} leaves {left_out_by_budget} game(s) pending "
                "this run; re-run to continue"
            )
            print(f"WP_BACKFILL chunk=0 games=0 loaded_rows=0 remaining={left_out_by_budget}")
        else:
            logger.info("Nothing to do -- every in-scope game is already loaded")
        return

    chunks = chunk_ids(to_load, chunk_size)
    logger.info(
        f"Loading {len(to_load)} game(s) in {len(chunks)} chunk(s) of up to {chunk_size} "
        f"({left_out_by_budget} more pending beyond --max-calls={max_calls})"
    )

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_ingame_wp_backfill",
        destination="postgres",
        dataset_name="metrics",
    )

    processed = 0
    for i, chunk in enumerate(chunks, start=1):
        info = pipeline.run(win_probability_by_game_resource(chunk))
        loaded_rows = rows_loaded(pipeline, "win_probability_by_game")
        processed += len(chunk)
        remaining = len(pending) - processed
        logger.info(f"Chunk {i}/{len(chunks)}: {chunk[0]}..{chunk[-1]} -> {info}")
        print(
            f"WP_BACKFILL chunk={i} games={len(chunk)} loaded_rows={loaded_rows} "
            f"remaining={remaining}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill (or weekly forward-fill) metrics.win_probability via "
            "win_probability_by_game_resource"
        )
    )
    parser.add_argument(
        "--from",
        dest="from_season",
        type=int,
        default=None,
        help=(
            f"One-time backfill: all completed games season >= YYYY (default {DEFAULT_FROM_SEASON})"
        ),
    )
    parser.add_argument(
        "--season",
        dest="season",
        type=int,
        default=None,
        help="Weekly forward-fill: only completed games in this single season",
    )
    parser.add_argument(
        "--max-calls",
        type=int,
        default=DEFAULT_MAX_CALLS,
        help=f"Per-run cap on /metrics/wp calls, i.e. games (default {DEFAULT_MAX_CALLS})",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Games per dlt pipeline.run() batch (default {DEFAULT_CHUNK_SIZE})",
    )
    args = parser.parse_args()

    if args.season is not None and args.from_season is not None:
        parser.error("--from and --season are mutually exclusive")

    from_season = args.from_season
    if args.season is None and from_season is None:
        from_season = DEFAULT_FROM_SEASON

    try:
        run_backfill(from_season, args.season, args.max_calls, args.chunk_size)
    except Exception:
        logger.exception("WP backfill run failed")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
