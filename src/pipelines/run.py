"""CLI entry point for CFB database pipelines."""

import argparse
import logging
import sys
from typing import NoReturn

import dlt

from .sources.betting import betting_source
from .sources.coaches import coach_profiles_source, coach_tenures_source, coaches_source
from .sources.conferences import conferences_source
from .sources.draft import draft_source
from .sources.game_stats import game_stats_source
from .sources.games import games_source
from .sources.metrics import metrics_ppa_predicted_source, metrics_source, metrics_wp_source
from .sources.passing import passing_source
from .sources.player_overview import player_overview_source
from .sources.playoffs import playoffs_source
from .sources.plays import plays_source
from .sources.rankings import rankings_source
from .sources.ratings import ratings_source
from .sources.recruiting import recruiting_source
from .sources.reference import reference_source
from .sources.rosters import rosters_source
from .sources.stats import stats_source
from .sources.wepa import wepa_source
from .utils.rate_limiter import get_rate_limiter

logger = logging.getLogger(__name__)


def batch_years(years: list[int], batch_size: int) -> list[list[int]]:
    """Split years into batches of specified size.

    Args:
        years: List of years to batch
        batch_size: Number of years per batch

    Returns:
        List of year batches
    """
    return [years[i : i + batch_size] for i in range(0, len(years), batch_size)]


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        description="CFB Database - Load college football data into Supabase",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load reference data (teams, conferences, venues)
  python -m src.pipelines.run --source reference

  # Load current season games
  python -m src.pipelines.run --source games --mode incremental

  # Backfill historical games
  python -m src.pipelines.run --source games --mode backfill --years 2020 2021 2022

  # Backfill game stats in batches (avoids timeout on large merges)
  python -m src.pipelines.run --source game_stats --years 2020 2021 2022 2023 2024 --batch-size 2

  # Check pipeline status and rate limits
  python -m src.pipelines.run --status
        """,
    )

    parser.add_argument(
        "--source",
        choices=[
            "reference",
            "games",
            "game_stats",
            "plays",
            "stats",
            "ratings",
            "recruiting",
            "betting",
            "draft",
            "metrics",
            "metrics_wp",
            "metrics_ppa_predicted",
            "rankings",
            "rosters",
            "wepa",
            "passing",
            "playoffs",
            "coaches",
            "coach_tenures",
            "coach_profiles",
            "conferences",
            "player_overview",
            "all",
        ],
        help="Data source to load",
    )

    parser.add_argument(
        "--mode",
        choices=["incremental", "backfill"],
        default="incremental",
        help="Load mode: incremental (current season) or backfill (historical)",
    )

    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        help="Specific years to load (for backfill mode)",
    )

    parser.add_argument(
        "--teams",
        type=str,
        nargs="+",
        help="Team names (required for rosters source, e.g., --teams Alabama Georgia)",
    )

    parser.add_argument(
        "--status",
        action="store_true",
        help="Show pipeline status and rate limit usage",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be loaded without making API calls",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Process years in batches of N (e.g., --batch-size 2 for 2 years at a time)",
    )

    parser.add_argument(
        "--replace",
        action="store_true",
        help="Use replace disposition instead of merge (faster for bulk loads, use for game_stats)",
    )

    parser.add_argument(
        "--weekly",
        action="store_true",
        help="Load game_stats week-by-week (~35K rows per merge) to avoid Supabase timeouts",
    )

    return parser


def show_status():
    """Display pipeline status and rate limit info."""
    rate_limiter = get_rate_limiter()
    status = rate_limiter.get_status()

    print("\n=== CFB Database Pipeline Status ===\n")
    print(f"Month:           {status['month']}")
    print(f"API Calls Used:  {status['calls_used']:,}")
    print(f"Remaining:       {status['remaining']:,}")
    print(f"Monthly Budget:  {status['monthly_budget']:,}")
    print(f"Usage:           {status['usage_percent']:.1f}%")
    print()


def run_reference_pipeline():
    """Run the reference data pipeline."""
    print("\n=== Loading Reference Data ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_reference",
        destination="postgres",
        dataset_name="ref",
    )

    source = reference_source()
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")
    print(f"Loaded packages: {info.load_packages}")

    return info


def run_games_pipeline(years: list[int] | None = None, mode: str = "incremental"):
    """Run the games data pipeline.

    Two sequential loads, not one: games commits first, THEN drives (and
    media/weather/records). Within a single load package dlt's per-table
    merge jobs commit independently and in no guaranteed order, so a drives
    merge can reach its deferred fk_drives_game check before the games merge
    that carries a new parent row has committed. Sequencing removes the
    race; the shared games_cache guarantees the drives orphan filter sees
    exactly the /games response the first load merged (see games_source).
    """
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Games Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_games",
        destination="postgres",
        dataset_name="core",
    )

    games_cache: dict[int, list[dict]] = {}

    games_only = games_source(years=years, mode=mode, games_cache=games_cache).with_resources(
        "games"
    )
    info = pipeline.run(games_only)
    print(f"\nLoad info (games): {info}")

    rest = games_source(years=years, mode=mode, games_cache=games_cache).with_resources(
        "drives", "game_media", "game_weather", "records"
    )
    info_rest = pipeline.run(rest)
    print(f"\nLoad info (drives/media/weather/records): {info_rest}")

    return info_rest


def run_game_stats_pipeline(
    years: list[int] | None = None,
    mode: str = "incremental",
    batch_size: int | None = None,
    use_replace: bool = False,
):
    """Run the game stats pipeline (team/player box scores only).

    Args:
        years: Specific years to load
        mode: "incremental" or "backfill"
        batch_size: If set, process years in batches of this size
        use_replace: If True, use replace disposition instead of merge
    """
    years_str = f"years={years}" if years else f"mode={mode}"
    disposition = "replace" if use_replace else "merge"
    print(f"\n=== Loading Game Stats Data ({years_str}, disposition={disposition}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_game_stats",
        destination="postgres",
        dataset_name="core",
    )

    # Determine base disposition
    base_disposition = "replace" if use_replace else "merge"

    # If no batching or no years specified, run normally
    if batch_size is None or years is None:
        source = game_stats_source(years=years, mode=mode, disposition=base_disposition)
        info = pipeline.run(source)
        print(f"\nLoad info: {info}")
        return info

    # Batch mode: process years in chunks
    batches = batch_years(years, batch_size)
    print(f"Processing {len(years)} years in {len(batches)} batches of up to {batch_size}")

    all_info = []
    for i, year_batch in enumerate(batches, 1):
        # First batch uses replace (truncate+insert), subsequent use append
        if use_replace:
            batch_disposition = "replace" if i == 1 else "append"
        else:
            batch_disposition = "merge"
        print(
            f"\n--- Batch {i}/{len(batches)}: years {year_batch}"
            f" (disposition={batch_disposition}) ---"
        )
        source = game_stats_source(years=year_batch, mode=mode, disposition=batch_disposition)
        info = pipeline.run(source)
        all_info.append(info)
        print(f"Batch {i} complete: {info}")

    print(f"\n=== All {len(batches)} batches complete ===")
    return all_info


def run_game_stats_weekly(
    years: list[int],
    use_replace: bool = False,
):
    """Load game stats week-by-week for small merge batches.

    Each (year, season_type, week) tuple gets its own pipeline.run() call,
    keeping merge batches at ~35K rows to avoid Supabase timeouts.

    Args:
        years: List of years to load
        use_replace: If True, first batch uses replace, rest use append
    """
    print(f"\n=== Loading Game Stats Weekly (years={years}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_game_stats",
        destination="postgres",
        dataset_name="core",
    )

    first = True
    total_runs = 0

    for year in years:
        for season_type in ["regular", "postseason"]:
            max_week = 15 if season_type == "regular" else 5
            for week in range(1, max_week + 1):
                if use_replace and first:
                    disposition = "replace"
                else:
                    disposition = "merge"
                first = False

                label = f"{year} {season_type} week {week}"
                print(f"  [{total_runs + 1}] {label} (disposition={disposition})")

                source = game_stats_source(
                    years=[year],
                    season_type=season_type,
                    weeks=[week],
                    disposition=disposition,
                )
                info = pipeline.run(source)
                total_runs += 1
                print(f"       -> {info}")

    print(f"\n=== Weekly loading complete: {total_runs} runs across {len(years)} years ===")
    return total_runs


def run_plays_pipeline(years: list[int] | None = None, mode: str = "incremental"):
    """Run the plays data pipeline."""
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Plays Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_plays",
        destination="postgres",
        dataset_name="core",
    )

    source = plays_source(years=years, mode=mode)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


def run_stats_pipeline(
    years: list[int] | None = None,
    mode: str = "incremental",
    only: list[str] | None = None,
):
    """Run the stats data pipeline.

    `only` restricts the run to named resources -- see stats_source: the
    source's cost is dominated by play_stats, which is one request per game.
    """
    years_str = f"years={years}" if years else f"mode={mode}"
    only_str = f", only={only}" if only else ""
    print(f"\n=== Loading Stats Data ({years_str}{only_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_stats",
        destination="postgres",
        dataset_name="stats",
    )

    source = stats_source(years=years, mode=mode, only=only)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


def run_ratings_pipeline(years: list[int] | None = None, mode: str = "incremental"):
    """Run the ratings data pipeline."""
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Ratings Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_ratings",
        destination="postgres",
        dataset_name="ratings",
    )

    source = ratings_source(years=years, mode=mode)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


def run_recruiting_pipeline(years: list[int] | None = None, mode: str = "incremental"):
    """Run the recruiting data pipeline."""
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Recruiting Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_recruiting",
        destination="postgres",
        dataset_name="recruiting",
    )

    source = recruiting_source(years=years, mode=mode)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


def run_betting_pipeline(years: list[int] | None = None, mode: str = "incremental"):
    """Run the betting data pipeline."""
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Betting Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_betting",
        destination="postgres",
        dataset_name="betting",
    )

    source = betting_source(years=years, mode=mode)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


def run_draft_pipeline(years: list[int] | None = None, mode: str = "incremental"):
    """Run the draft data pipeline."""
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Draft Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_draft",
        destination="postgres",
        dataset_name="draft",
    )

    source = draft_source(years=years, mode=mode)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


def run_metrics_pipeline(years: list[int] | None = None, mode: str = "incremental"):
    """Run the metrics data pipeline."""
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Metrics Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_metrics",
        destination="postgres",
        dataset_name="metrics",
    )

    source = metrics_source(years=years, mode=mode)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


def run_metrics_ppa_predicted_pipeline():
    """Run the predicted-PPA down/distance lookup table load.

    ~120 calls (4 downs x 30 distances) building a static lookup table --
    NOT part of the daily `metrics` source (see metrics.py's module
    docstring and docs/pipeline-manifest.md row 48) and NOT in
    scripts/load_season.py's SOURCE_ORDER. Opt in explicitly via
    `--source metrics_ppa_predicted`.
    """
    print("\n=== Loading Predicted PPA Lookup (down x distance) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_metrics_ppa_predicted",
        destination="postgres",
        dataset_name="metrics",
    )

    source = metrics_ppa_predicted_source()
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


def run_passing_pipeline(years: list[int] | None = None, mode: str = "incremental"):
    """Run the /passing charting pipeline (air yards, aDOT, depth/direction/
    location, YAC -- see passing.py's module docstring). Data starts 2025;
    earlier years are skipped per-resource with zero calls.
    """
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Passing Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_passing",
        destination="postgres",
        dataset_name="stats",
    )

    source = passing_source(years=years, mode=mode)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


def run_playoffs_pipeline(years: list[int] | None = None, mode: str = "incremental"):
    """Run the CFP bracket/games/participants pipeline."""
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Playoffs Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_playoffs",
        destination="postgres",
        dataset_name="core",
    )

    source = playoffs_source(years=years, mode=mode)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


def run_coaches_pipeline(years: list[int] | None = None, mode: str = "incremental"):
    """Run the year-driven coach-seasons pipeline.

    Does NOT invoke coach_tenures -- that endpoint is per-team fan-out and
    is deliberately excluded from this runner and from
    scripts/load_season.py's SOURCE_ORDER; see run_coach_tenures_pipeline.
    """
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Coach Seasons Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_coaches",
        destination="postgres",
        dataset_name="ref",
    )

    source = coaches_source(years=years, mode=mode)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


def run_coach_tenures_pipeline(
    teams: list[str] | None = None,
    years: list[int] | None = None,
):
    """Run the per-team coach-tenures pipeline.

    Backfill/preseason only -- NOT part of scripts/load_season.py's
    SOURCE_ORDER (one call per team). `teams=None` resolves the team list
    from the schedule via `scheduled_teams`, mirroring
    `run_rosters_pipeline`; pass `teams` explicitly to override.
    """
    if teams is None:
        if not years:
            raise ValueError(
                "coach_tenures needs either an explicit team list or years to resolve one"
            )
        import psycopg2

        conn = psycopg2.connect(_metrics_wp_db_url())
        try:
            teams = scheduled_teams(conn, years)
        finally:
            conn.close()
        if not teams:
            raise RuntimeError(
                f"No teams with scheduled games in {years}: core.games has no rows for "
                f"{'that season' if len(years) == 1 else 'those seasons'}. "
                "/coaches/tenures is requested per team, so there is nothing to ask for. "
                f"Load the schedule first (--sources games --season {years[0]}), or pass "
                "an explicit team list."
            )
        print(f"Resolved {len(teams)} teams with {years} games from core.games")

    print(f"\n=== Loading Coach Tenures Data ({len(teams)} teams) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_coach_tenures",
        destination="postgres",
        dataset_name="ref",
    )

    source = coach_tenures_source(teams=teams)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


# Coach ids considered for profile loading: every id ever seen in
# ref.coach_seasons (coach__id -- dlt's flattened name for the nested
# `coach.id` field, see coach_seasons_resource). Guarded against
# UndefinedTable the same way _METRICS_WP_GAMES_QUERY is: coach_seasons has
# never been backfilled against the live database as of this writing (see
# docs/pipeline-manifest.md row 63), so a fresh run must degrade to "no
# candidates" rather than error.
_COACH_PROFILES_CANDIDATES_QUERY = """
    SELECT DISTINCT coach__id FROM ref.coach_seasons WHERE coach__id IS NOT NULL
"""

# ref.coach_profiles doesn't exist until the first successful pipeline.run()
# of coach_profiles_resource creates it (dlt table-on-first-write, same as
# metrics.win_probability -- see run_metrics_wp_pipeline above).
_COACH_PROFILES_EXISTING_QUERY = "SELECT id FROM ref.coach_profiles"

# Ceiling on coach ids fetched per run -- one API call each. Not a usage
# estimate the way most ESTIMATED_CALLS entries are: the candidate set is
# every coach id ever seen in coach_seasons, so the backlog starts large (one
# per historical coach) and then drains to near-zero -- new head-coaching
# hires are the only ongoing source, a handful a year. Capping bounds a
# single run's cost the same way MAX_WP_GAMES_PER_RUN bounds win-probability's
# backlog, rather than fetching the whole history-to-date in one run.
MAX_COACH_PROFILES_PER_RUN = 200


def run_coach_profiles_pipeline(max_coaches: int = MAX_COACH_PROFILES_PER_RUN) -> dict:
    """Load coach profiles for coach ids missing from ref.coach_profiles.

    Unlike the other run_*_pipeline functions, this is not a year- or
    team-fetch-all -- `/coaches/profile` requires one API call per coachId
    (see coach_profiles_resource) -- so call volume is bounded to ids that
    don't already have a profile row, computed as a set difference (DB
    UndefinedTable on either side degrades to "empty", not an error) rather
    than a SQL LEFT JOIN, mirroring run_metrics_wp_pipeline.

    Args:
        max_coaches: Ceiling on coach ids fetched this run (one API call
            each). None disables the cap for a deliberate full backfill.

    Returns:
        Summary dict: candidates, `missing` (the FULL backlog, not the
        capped slice), `excluded_misses` (candidates skipped because
        meta.fanout_misses recorded a 400/404 for them within
        FANOUT_MISS_RETRY_DAYS -- PR #75 review finding A), `loaded_this_run`,
        `deferred`, batches, and the list of dlt LoadInfo objects (one per
        batch).
    """
    import psycopg2
    import psycopg2.errors

    print(f"\n=== Loading Coach Profiles (cap={max_coaches}) ===\n")

    conn = psycopg2.connect(_metrics_wp_db_url())
    conn.autocommit = True  # each statement stands alone; no transaction to poison on error
    try:
        with conn.cursor() as cur:
            try:
                cur.execute(_COACH_PROFILES_CANDIDATES_QUERY)
                candidate_ids = [row[0] for row in cur.fetchall()]
            except psycopg2.errors.UndefinedTable:
                logger.info("ref.coach_seasons does not exist yet; treating as empty")
                candidate_ids = []

            try:
                cur.execute(_COACH_PROFILES_EXISTING_QUERY)
                existing_ids = {row[0] for row in cur.fetchall()}
            except psycopg2.errors.UndefinedTable:
                logger.info("ref.coach_profiles does not exist yet; treating as empty")
                existing_ids = set()

            recent_misses = _fetch_recent_fanout_misses(cur, "coach_profiles")
    finally:
        conn.close()

    not_yet_profiled = [cid for cid in candidate_ids if cid not in existing_ids]
    excluded_misses = sum(1 for cid in not_yet_profiled if str(cid) in recent_misses)
    missing = [cid for cid in not_yet_profiled if str(cid) not in recent_misses]
    total_missing = len(missing)

    # Bound the run. Reported, never silent -- see MAX_WP_GAMES_PER_RUN's
    # comment for why an unbounded backlog going unnoticed is the failure
    # mode this guards against.
    deferred = 0
    if max_coaches is not None and total_missing > max_coaches:
        deferred = total_missing - max_coaches
        missing = missing[:max_coaches]

    print(
        f"  {len(candidate_ids)} known coach id(s) in ref.coach_seasons, "
        f"{len(existing_ids)} already have a profile, {excluded_misses} recent "
        f"terminal miss(es) excluded, {total_missing} missing"
    )
    if deferred:
        msg = (
            f"  Backlog capped at {max_coaches} coach(es) this run; {deferred} deferred "
            f"to a later run. Re-run to continue draining."
        )
        print(msg)
        logger.info(msg.strip())

    if not missing:
        print("  Nothing to load.")
        return {
            "candidates": len(candidate_ids),
            "missing": 0,
            "excluded_misses": excluded_misses,
            "loaded_this_run": 0,
            "deferred": 0,
            "batches": 0,
        }

    rate_limiter = get_rate_limiter()
    if not rate_limiter.check_budget(len(missing)):
        msg = (
            f"API budget insufficient for {len(missing)} coach-profile calls "
            f"({rate_limiter.remaining} calls remaining this month)"
        )
        logger.error(msg)
        print(f"  ERROR: {msg}")
        return {
            "candidates": len(candidate_ids),
            "missing": total_missing,
            "excluded_misses": excluded_misses,
            "loaded_this_run": 0,
            "deferred": deferred,
            "batches": 0,
            "error": msg,
        }

    batches = batch_years(missing, 50)  # generic chunker, works on any list
    print(f"  Processing {len(missing)} coach(es) in {len(batches)} batches of up to 50")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_coach_profiles",
        destination="postgres",
        dataset_name="ref",
    )

    all_info = []
    for i, id_batch in enumerate(batches, 1):
        print(f"\n  --- Batch {i}/{len(batches)}: {len(id_batch)} coach(es) ---")
        batch_misses: list[tuple[str, int]] = []
        source = coach_profiles_source(coach_ids=id_batch, misses=batch_misses)
        info = pipeline.run(source)
        all_info.append(info)
        print(f"  Batch {i} complete: {info}")
        if batch_misses:
            _record_fanout_misses("coach_profiles", batch_misses)
            print(f"  Recorded {len(batch_misses)} new/renewed miss(es) in meta.fanout_misses")

    print(f"\n=== Coach profiles load complete: {len(batches)} batches ===")
    return {
        "candidates": len(candidate_ids),
        "missing": total_missing,
        "excluded_misses": excluded_misses,
        "loaded_this_run": len(missing),
        "deferred": deferred,
        "batches": len(batches),
        "info": all_info,
    }


def run_conferences_pipeline(years: list[int] | None = None, mode: str = "incremental"):
    """Run the conference affiliations/changes pipeline."""
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Conferences Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_conferences",
        destination="postgres",
        dataset_name="ref",
    )

    source = conferences_source(years=years, mode=mode)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


def _metrics_wp_db_url() -> str:
    """Get database URL from dlt secrets or environment.

    Copied from scripts/refresh_marts.py's get_db_url pattern (the convention
    every script that needs a raw psycopg2 connection follows -- see
    scripts/compute_house_elo.py's copy of the same docstring). Duplicated
    here rather than imported from scripts/ to avoid a src -> scripts
    dependency; src.pipelines.run is also installed as the `cfb-pipeline`
    console script (pyproject.toml) and shouldn't depend on the repo's
    top-level scripts/ directory being importable.
    """
    import os

    import dlt as _dlt

    url = None
    try:
        creds = _dlt.secrets.get("destination.postgres.credentials")
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

    return url


# Games considered for win-probability loading: completed, both scores
# present. Restricted to the requested seasons so a single-season daily call
# doesn't rescan all of history.
# Newest first. The steady-state case -- games that finished since the last run
# -- must never be starved behind a historical backlog once MAX_GAMES_PER_RUN
# starts truncating, and recency is the best available proxy for "CFBD actually
# has win-probability data for this game".
_METRICS_WP_GAMES_QUERY = """
    SELECT id, season
    FROM core.games
    WHERE completed = true
      AND home_points IS NOT NULL
      AND away_points IS NOT NULL
      AND season = ANY(%s)
    ORDER BY season DESC, start_date DESC NULLS LAST, id DESC
"""

# Ceiling on games fetched per run -- one API call each.
#
# Without it the backlog is unbounded: the 2026-07-26 daily load found 2,517
# games of a FINISHED 2025 season still missing win probability and queued all
# of them, ~3% of the monthly budget in a single run, every day. The backlog
# does not drain on its own because a game CFBD has no data for stays missing
# and is re-requested forever.
#
# Skipping the source entirely once a season is final was the first fix and was
# wrong (PR #54 review): missing games include ones whose requests failed during
# the quota exhaustion, and the unattended daily path would never retry them.
# Capping keeps every game eligible while bounding what a single run can cost,
# so an interrupted backfill still drains -- 2,517 games in ~9 days at this
# ceiling -- instead of being abandoned or re-attempted in full.
MAX_WP_GAMES_PER_RUN = 300

# metrics.win_probability doesn't exist until the first successful pipeline.run()
# of win_probability_resource creates it (dlt table-on-first-write, same as
# betting.line_snapshots -- see src/schemas/migrations/020's header). Handled
# by catching UndefinedTable in run_metrics_wp_pipeline below.
_METRICS_WP_EXISTING_QUERY = """
    SELECT DISTINCT game_id
    FROM metrics.win_probability
    WHERE season = ANY(%s)
"""


def run_metrics_wp_pipeline(
    seasons: list[int] | None = None,
    batch_size: int = 50,
    max_games: int | None = MAX_WP_GAMES_PER_RUN,
) -> dict:
    """Load in-game win probability for completed games missing it.

    Unlike the other run_*_pipeline functions, this isn't a year-fetch-all --
    /metrics/wp requires one API call per gameId (docs/pipeline-manifest.md
    row 47), so the call volume must be bounded to games that don't already
    have win-probability rows. Queries core.games for completed games in
    `seasons`, LEFT JOIN-style against the game_ids already present in
    metrics.win_probability (computed as a set difference here rather than a
    SQL LEFT JOIN so the "table doesn't exist yet" case -- true on a fresh
    backfill -- degrades to "nothing loaded yet" instead of an error).

    Missing games are loaded in batches of `batch_size` games per
    pipeline.run() call (~150+ rows/game -> ~8.5K rows/merge at the default
    50, mirroring run_game_stats_weekly's proven batch size for staying under
    Supabase's statement timeout).

    Budget math (Tier 4 = 125,000 calls/month, see docs/pipeline-manifest.md
    and src/pipelines/utils/rate_limiter.py): one call per missing game.
    Full 2014+ backfill is ~12,000 completed FBS games -> ~12K calls, a
    one-time cost well inside the monthly budget. Steady-state daily/weekly
    incremental loads only see newly-completed games since the last run --
    ~70 games/week in-season (scripts/load_season.py's ESTIMATED_CALLS
    entry), negligible against the existing ~22K/month worst-case daily-load
    total.

    Args:
        seasons: Seasons to check for missing win-probability data. Defaults
            to the current season (matches every other run_*_pipeline's
            incremental default).
        batch_size: Games per pipeline.run() call.
        max_games: Ceiling on games fetched this run (one API call each), newest
            first. None disables the cap for a deliberate full backfill. See
            MAX_WP_GAMES_PER_RUN for why the default is not None.

    Returns:
        Summary dict: seasons, games considered, `missing` (the FULL backlog,
        not the capped slice), `loaded_this_run`, `deferred`, batches run, and
        the list of dlt LoadInfo objects (one per batch).
    """
    import psycopg2
    import psycopg2.errors

    if seasons is None:
        from .config.years import get_current_season

        seasons = [get_current_season()]

    print(f"\n=== Loading Win Probability Data (seasons={seasons}) ===\n")

    conn = psycopg2.connect(_metrics_wp_db_url())
    conn.autocommit = True  # each statement stands alone; no transaction to poison on error
    try:
        with conn.cursor() as cur:
            cur.execute(_METRICS_WP_GAMES_QUERY, (seasons,))
            candidate_games = cur.fetchall()  # [(game_id, season), ...]

            try:
                cur.execute(_METRICS_WP_EXISTING_QUERY, (seasons,))
                existing_ids = {row[0] for row in cur.fetchall()}
            except psycopg2.errors.UndefinedTable:
                # metrics.win_probability hasn't been created yet (no prior
                # successful load) -- treat as "nothing loaded", not an error.
                logger.info("metrics.win_probability does not exist yet; treating as empty")
                existing_ids = set()
    finally:
        conn.close()

    missing = [(gid, season) for gid, season in candidate_games if gid not in existing_ids]
    total_missing = len(missing)

    # Bound the run. Reported, never silent: a truncated backlog that looked
    # like a completed one is how "2,517 missing" went unnoticed for months.
    deferred = 0
    if max_games is not None and total_missing > max_games:
        deferred = total_missing - max_games
        missing = missing[:max_games]

    game_seasons = dict(missing)
    game_ids = [gid for gid, _ in missing]

    print(
        f"  {len(candidate_games)} completed games in {seasons}, "
        f"{len(existing_ids)} already have win probability, "
        f"{total_missing} missing"
    )
    if deferred:
        msg = (
            f"  Backlog capped at {max_games} game(s) this run; {deferred} deferred "
            f"to a later run (newest first). Re-run to continue draining."
        )
        print(msg)
        logger.info(msg.strip())

    if not game_ids:
        print("  Nothing to load.")
        return {
            "seasons": seasons,
            "candidates": len(candidate_games),
            "missing": 0,
            "loaded_this_run": 0,
            "deferred": 0,
            "batches": 0,
        }

    rate_limiter = get_rate_limiter()
    if not rate_limiter.check_budget(len(game_ids)):
        msg = (
            f"API budget insufficient for {len(game_ids)} win-probability calls "
            f"({rate_limiter.remaining} calls remaining this month)"
        )
        logger.error(msg)
        print(f"  ERROR: {msg}")
        return {
            "seasons": seasons,
            "candidates": len(candidate_games),
            "missing": total_missing,
            "loaded_this_run": 0,
            "deferred": deferred,
            "batches": 0,
            "error": msg,
        }

    batches = batch_years(game_ids, batch_size)  # generic chunker, works on any list
    print(f"  Processing {len(game_ids)} games in {len(batches)} batches of up to {batch_size}")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_metrics_wp",
        destination="postgres",
        dataset_name="metrics",
    )

    all_info = []
    for i, game_batch in enumerate(batches, 1):
        print(f"\n  --- Batch {i}/{len(batches)}: {len(game_batch)} games ---")
        source = metrics_wp_source(game_ids=game_batch, game_seasons=game_seasons)
        info = pipeline.run(source)
        all_info.append(info)
        print(f"  Batch {i} complete: {info}")

    print(f"\n=== Win probability load complete: {len(batches)} batches ===")
    return {
        "seasons": seasons,
        "candidates": len(candidate_games),
        "missing": total_missing,
        "loaded_this_run": len(game_ids),
        "deferred": deferred,
        "batches": len(batches),
        "info": all_info,
    }


# Same "finished season" definition as scripts/load_season.py's
# season_is_final (and scripts/train_model.py's independent copy of the
# same rule for its refit guard) -- duplicated here rather than imported.
# scripts/ has no __init__.py and is not part of the installed package
# (pyproject.toml's `packages = ["src"]`); src.pipelines.run is also
# installed as the `cfb-pipeline` console script and must not depend on the
# repo's top-level scripts/ directory being importable -- see
# _metrics_wp_db_url's docstring for the same reasoning applied to DB URLs.
_SEASON_COMPLETE_THRESHOLD = 0.99
_MIN_GAMES_FOR_FINISHED_SEASON = 100


def _season_is_final(conn, season: int) -> bool:
    """True when `season` has essentially every scheduled game completed.

    Mirrors scripts/load_season.py::season_is_final -- see that function's
    docstring and load_season.py's IMMUTABLE_ONCE_FINAL comment block for
    why a tolerance (not literal 100%) and a games-count floor are both
    needed. Used by run_player_overview_pipeline's completed-season gate: a
    season's usage/PPA/box-score totals mutate weekly while it is still
    being played, so loading its overview rows early would freeze them
    stale.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS n,
                   AVG(CASE WHEN COALESCE(completed, false) THEN 1.0 ELSE 0.0 END) AS pct
            FROM core.games
            WHERE season = %s
            """,
            (season,),
        )
        n, pct = cur.fetchone()
    if not n or n < _MIN_GAMES_FOR_FINISHED_SEASON:
        return False
    return float(pct or 0.0) >= _SEASON_COMPLETE_THRESHOLD


def _dedup_rows(*row_lists) -> list[tuple]:
    """Union any number of row lists into a de-duplicated list, stable order.

    Pure -- no DB. Used to combine stats.player_usage and
    metrics.ppa_players_season candidates (each fetched with its own
    UndefinedTable-guarded query, so a SQL-level UNION across both isn't
    available) without losing a player who appears in only one of the two.
    """
    seen = set()
    result: list[tuple] = []
    for rows in row_lists:
        for row in rows:
            key = tuple(row)
            if key not in seen:
                seen.add(key)
                result.append(key)
    return result


def _fetch_rows_or_empty(cur, query: str, params: tuple, table_desc: str) -> list:
    """Run `query`, returning [] instead of raising if `table_desc` doesn't
    exist yet (dlt table-on-first-write -- see run_metrics_wp_pipeline's
    UndefinedTable handling above, generalized for player_overview's three
    guarded tables)."""
    import psycopg2.errors

    try:
        cur.execute(query, params)
        return cur.fetchall()
    except psycopg2.errors.UndefinedTable:
        logger.info(f"{table_desc} does not exist yet; treating as empty")
        return []


# ---------------------------------------------------------------------------
# meta.fanout_misses (src/schemas/migrations/056_fanout_miss_ledger.sql):
# shared terminal-miss ledger for the coach_profiles and
# player_season_overview drainers (PR #75 review finding A, P1 x2). Without
# this, a 400/404 for a given id was logged and skipped by the resource
# (coaches.py::coach_profiles_resource, player_overview.py::
# player_season_overview_resource) but never persisted anywhere, so the same
# id stayed in the "missing" set forever -- re-spending its API call every
# run and, once a backlog is small, eventually crowding out real work behind
# the 200/250-cap slices. Placed here, next to _fetch_rows_or_empty, because
# _fetch_recent_fanout_misses is built directly on it.
# ---------------------------------------------------------------------------

# Days a recorded miss keeps excluding its key from a drainer's candidate
# set. Bounded, not permanent: a coach profile or player overview CFBD has
# not published YET is indistinguishable from one it will never publish, so
# re-eligibility after a window lets late-published data self-heal instead
# of being excluded forever.
FANOUT_MISS_RETRY_DAYS = 30

_FANOUT_RECENT_MISSES_QUERY = """
    SELECT key FROM meta.fanout_misses
    WHERE source = %s AND last_attempt_at > now() - make_interval(days => %s)
"""


def _fetch_recent_fanout_misses(cur, source: str) -> set[str]:
    """Keys in meta.fanout_misses attempted within FANOUT_MISS_RETRY_DAYS for
    `source` ('coach_profiles' | 'player_season_overview') -- excluded from
    that drainer's candidate set so a terminal 400/404 isn't re-spent every
    run. UndefinedTable (migration 056 not yet applied) degrades to an empty
    set via _fetch_rows_or_empty, i.e. no exclusion -- a drainer must still
    run normally before the migration lands.
    """
    rows = _fetch_rows_or_empty(
        cur,
        _FANOUT_RECENT_MISSES_QUERY,
        (source, FANOUT_MISS_RETRY_DAYS),
        "meta.fanout_misses",
    )
    return {row[0] for row in rows}


_FANOUT_MISS_UPSERT_SQL = """
    INSERT INTO meta.fanout_misses (source, key, status_code)
    VALUES %s
    ON CONFLICT (source, key) DO UPDATE SET
        attempts = meta.fanout_misses.attempts + 1,
        status_code = EXCLUDED.status_code,
        last_attempt_at = now()
"""


def _record_fanout_misses(source: str, misses: list[tuple[str, int]]) -> None:
    """Persist one batch's (key, status_code) misses to meta.fanout_misses.

    Opens its own connection -- this module's existing per-call idiom (see
    _metrics_wp_db_url's docstring for why run.py doesn't thread one
    connection through every helper); the drainer's candidate-query
    connection is already closed by the time a batch's pipeline.run()
    completes. UndefinedTable (migration 056 not yet applied) is logged and
    swallowed, not raised: a drainer must not crash because misses couldn't
    be persisted, only lose the exclusion benefit on the next run.
    """
    if not misses:
        return

    import psycopg2
    import psycopg2.errors
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(_metrics_wp_db_url())
    conn.autocommit = True  # each statement stands alone; no transaction to poison on error
    try:
        with conn.cursor() as cur:
            try:
                execute_values(
                    cur,
                    _FANOUT_MISS_UPSERT_SQL,
                    [(source, key, code) for key, code in misses],
                )
            except psycopg2.errors.UndefinedTable:
                logger.warning(
                    "meta.fanout_misses missing; apply migration 056 -- misses not persisted"
                )
    finally:
        conn.close()


_PLAYER_OVERVIEW_USAGE_SEASONS_QUERY = "SELECT DISTINCT season FROM stats.player_usage"
_PLAYER_OVERVIEW_PPA_SEASONS_QUERY = "SELECT DISTINCT season FROM metrics.ppa_players_season"

_PLAYER_OVERVIEW_USAGE_CANDIDATES_QUERY = (
    "SELECT DISTINCT season, id FROM stats.player_usage WHERE season = %s"
)
_PLAYER_OVERVIEW_PPA_CANDIDATES_QUERY = (
    "SELECT DISTINCT season, id FROM metrics.ppa_players_season WHERE season = %s"
)

# stats.player_season_overview doesn't exist until the first successful
# pipeline.run() of player_season_overview_resource creates it (dlt
# table-on-first-write, same as metrics.win_probability above).
_PLAYER_OVERVIEW_EXISTING_QUERY = (
    "SELECT season, id FROM stats.player_season_overview WHERE season = %s"
)

# Ceiling on player-seasons fetched per run -- one API call each. Like
# MAX_COACH_PROFILES_PER_RUN, this bounds a backlog rather than estimating a
# steady-state cost: a freshly-finished season can start with several
# thousand candidates (2025's stats.player_usage/ppa_players_season union is
# ~5,200 rows), and the completed-season gate below means the backlog only
# ever grows by one season's worth at a time, at a known cadence (each
# January), not continuously.
MAX_PLAYER_OVERVIEW_PER_RUN = 250


def run_player_overview_pipeline(
    seasons: list[int] | None = None,
    max_players: int = MAX_PLAYER_OVERVIEW_PER_RUN,
    batch_size: int = 50,
) -> dict:
    """Load player season overviews for (season, player) pairs missing from
    stats.player_season_overview, newest completed season first.

    `/player/season/overview` requires both `year` and `playerId` -- one
    call per player per season -- so, like run_coach_profiles_pipeline and
    run_metrics_wp_pipeline, this is a DB set-difference drainer rather than
    a year-fetch-all. Two things specific to this endpoint:

    - **Completed-seasons gate**: a season's usage/PPA totals mutate weekly
      while it is in progress, so a season is only eligible once
      `_season_is_final` says so -- an in-progress season is excluded
      entirely rather than loaded early and re-loaded. From January (once
      the just-finished season's games are marked complete) the daily path
      starts draining it at `max_players`/run; the gate is what keeps every
      loaded row immutable-correct without needing IMMUTABLE_ONCE_FINAL
      membership (see scripts/load_season.py's exclusion comment).
    - **Two candidate sources unioned in Python**: stats.player_usage and
      metrics.ppa_players_season are combined via `_dedup_rows` rather than
      a SQL UNION, because each table's query is independently guarded
      against UndefinedTable (see _fetch_rows_or_empty) -- neither table
      alone is a complete "every player who played this season" list.

    Args:
        seasons: Seasons to consider. None resolves every distinct season
            present in stats.player_usage or metrics.ppa_players_season,
            newest first. Every season -- whether passed explicitly or
            resolved -- is filtered by the completed-season gate; an
            explicitly-passed in-progress season yields zero candidates
            rather than bypassing the gate.
        max_players: Ceiling on (season, player) pairs fetched this run
            (one API call each), across all eligible seasons combined,
            newest season first. None disables the cap for a deliberate
            full backfill.
        batch_size: Player-seasons per pipeline.run() call.

    Returns:
        Summary dict: `seasons` considered, `eligible_seasons` (post-gate),
        `missing` (the FULL backlog, not the capped slice),
        `excluded_misses` (candidates skipped because meta.fanout_misses
        recorded a 400/404 for them within FANOUT_MISS_RETRY_DAYS -- PR #75
        review finding A), `loaded_this_run`, `deferred`, batches, and the
        list of dlt LoadInfo objects (one per batch).
    """
    import psycopg2

    print(f"\n=== Loading Player Season Overview (cap={max_players}) ===\n")

    conn = psycopg2.connect(_metrics_wp_db_url())
    conn.autocommit = True  # each statement stands alone; no transaction to poison on error
    try:
        with conn.cursor() as cur:
            if seasons is None:
                usage_seasons = [
                    r[0]
                    for r in _fetch_rows_or_empty(
                        cur, _PLAYER_OVERVIEW_USAGE_SEASONS_QUERY, (), "stats.player_usage"
                    )
                ]
                ppa_seasons = [
                    r[0]
                    for r in _fetch_rows_or_empty(
                        cur,
                        _PLAYER_OVERVIEW_PPA_SEASONS_QUERY,
                        (),
                        "metrics.ppa_players_season",
                    )
                ]
                candidate_seasons = sorted({*usage_seasons, *ppa_seasons}, reverse=True)
            else:
                candidate_seasons = sorted(set(seasons), reverse=True)

            eligible_seasons = [s for s in candidate_seasons if _season_is_final(conn, s)]

            # Fetched once, outside the per-season loop -- the ledger isn't
            # scoped by season, so there's nothing to gain by re-querying it
            # for every eligible season.
            recent_misses = _fetch_recent_fanout_misses(cur, "player_season_overview")

            missing: list[tuple] = []
            excluded_misses = 0
            for season in eligible_seasons:
                usage_rows = _fetch_rows_or_empty(
                    cur,
                    _PLAYER_OVERVIEW_USAGE_CANDIDATES_QUERY,
                    (season,),
                    "stats.player_usage",
                )
                ppa_rows = _fetch_rows_or_empty(
                    cur,
                    _PLAYER_OVERVIEW_PPA_CANDIDATES_QUERY,
                    (season,),
                    "metrics.ppa_players_season",
                )
                season_candidates = _dedup_rows(usage_rows, ppa_rows)

                existing_rows = _fetch_rows_or_empty(
                    cur,
                    _PLAYER_OVERVIEW_EXISTING_QUERY,
                    (season,),
                    "stats.player_season_overview",
                )
                existing_ids = {tuple(r) for r in existing_rows}

                not_yet_loaded = [row for row in season_candidates if row not in existing_ids]
                excluded_misses += sum(
                    1 for row in not_yet_loaded if f"{row[0]}:{row[1]}" in recent_misses
                )
                missing.extend(
                    row for row in not_yet_loaded if f"{row[0]}:{row[1]}" not in recent_misses
                )
    finally:
        conn.close()

    total_missing = len(missing)

    # Bound the run. Reported, never silent -- see MAX_WP_GAMES_PER_RUN's
    # comment for why an unbounded backlog going unnoticed is the failure
    # mode this guards against.
    deferred = 0
    if max_players is not None and total_missing > max_players:
        deferred = total_missing - max_players
        missing = missing[:max_players]

    print(
        f"  {len(candidate_seasons)} season(s) with player data, "
        f"{len(eligible_seasons)} completed and eligible {eligible_seasons}, "
        f"{excluded_misses} recent terminal miss(es) excluded, "
        f"{total_missing} missing player-season(s)"
    )
    if deferred:
        msg = (
            f"  Backlog capped at {max_players} player-season(s) this run; {deferred} "
            f"deferred to a later run (newest season first). Re-run to continue draining."
        )
        print(msg)
        logger.info(msg.strip())

    if not missing:
        print("  Nothing to load.")
        return {
            "seasons": candidate_seasons,
            "eligible_seasons": eligible_seasons,
            "missing": 0,
            "excluded_misses": excluded_misses,
            "loaded_this_run": 0,
            "deferred": 0,
            "batches": 0,
        }

    rate_limiter = get_rate_limiter()
    if not rate_limiter.check_budget(len(missing)):
        msg = (
            f"API budget insufficient for {len(missing)} player-overview calls "
            f"({rate_limiter.remaining} calls remaining this month)"
        )
        logger.error(msg)
        print(f"  ERROR: {msg}")
        return {
            "seasons": candidate_seasons,
            "eligible_seasons": eligible_seasons,
            "missing": total_missing,
            "excluded_misses": excluded_misses,
            "loaded_this_run": 0,
            "deferred": deferred,
            "batches": 0,
            "error": msg,
        }

    batches = batch_years(missing, batch_size)  # generic chunker, works on any list
    print(
        f"  Processing {len(missing)} player-season(s) in {len(batches)} "
        f"batches of up to {batch_size}"
    )

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_player_overview",
        destination="postgres",
        dataset_name="stats",
    )

    all_info = []
    for i, batch in enumerate(batches, 1):
        print(f"\n  --- Batch {i}/{len(batches)}: {len(batch)} player-season(s) ---")
        batch_misses: list[tuple[str, int]] = []
        source = player_overview_source(player_seasons=batch, misses=batch_misses)
        info = pipeline.run(source)
        all_info.append(info)
        print(f"  Batch {i} complete: {info}")
        if batch_misses:
            _record_fanout_misses("player_season_overview", batch_misses)
            print(f"  Recorded {len(batch_misses)} new/renewed miss(es) in meta.fanout_misses")

    print(f"\n=== Player season overview load complete: {len(batches)} batches ===")
    return {
        "seasons": candidate_seasons,
        "eligible_seasons": eligible_seasons,
        "missing": total_missing,
        "excluded_misses": excluded_misses,
        "loaded_this_run": len(missing),
        "deferred": deferred,
        "batches": len(batches),
        "info": all_info,
    }


def run_rankings_pipeline(years: list[int] | None = None, mode: str = "incremental"):
    """Run the rankings data pipeline."""
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Rankings Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_rankings",
        destination="postgres",
        dataset_name="core",
    )

    source = rankings_source(years=years, mode=mode)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


_SCHEDULED_TEAMS_QUERY = """
    SELECT home_team AS team FROM core.games WHERE season = ANY(%s)
    UNION
    SELECT away_team FROM core.games WHERE season = ANY(%s)
    ORDER BY team
"""


def scheduled_teams(conn, years: list[int]) -> list[str]:
    """Every team with a scheduled game in `years`.

    /roster is one request per team, so the team list is the whole cost of a
    roster load. Deriving it from the schedule asks for exactly the teams the
    warehouse has games for -- 350 for 2026, FCS opponents included -- instead
    of a hand-maintained list that silently rots, or ref.teams' 1,922 rows of
    which most never play an FBS opponent.
    """
    with conn.cursor() as cur:
        cur.execute(_SCHEDULED_TEAMS_QUERY, (years, years))
        return [row[0] for row in cur.fetchall() if row[0]]


def run_rosters_pipeline(
    teams: list[str] | None = None,
    years: list[int] | None = None,
    mode: str = "incremental",
):
    """Run the rosters data pipeline.

    `teams=None` resolves the list from the schedule (see scheduled_teams), so
    an orchestrated load can request a season's rosters without carrying a
    team list. Passing `teams` explicitly still wins.
    """
    if teams is None:
        if not years:
            raise ValueError("rosters needs either an explicit team list or years to resolve one")
        import psycopg2

        conn = psycopg2.connect(_metrics_wp_db_url())
        try:
            teams = scheduled_teams(conn, years)
        finally:
            conn.close()
        if not teams:
            # Never return quietly here. load_season records a returning
            # runner as [OK], so an empty resolution would report a
            # successful roster load that made zero /roster requests -- the
            # same silent-no-op shape as the finished-season skip turning a
            # backfill into nothing, and as `--sources rosters` logging "No
            # runner for source" and exiting 0, which is why core.roster had
            # no 2026 rows in the first place.
            raise RuntimeError(
                f"No teams with scheduled games in {years}: core.games has no rows for "
                f"{'that season' if len(years) == 1 else 'those seasons'}. /roster is "
                "requested per team, so there is nothing to ask for. Load the schedule "
                f"first (--sources games --season {years[0]}), or pass an explicit team list."
            )
        print(f"Resolved {len(teams)} teams with {years} games from core.games")

    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Rosters Data ({years_str}, {len(teams)} teams) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_rosters",
        destination="postgres",
        dataset_name="core",
    )

    source = rosters_source(teams=teams, years=years, mode=mode)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


def run_wepa_pipeline(years: list[int] | None = None, mode: str = "incremental"):
    """Run the WEPA (opponent-adjusted EPA) data pipeline."""
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading WEPA Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_wepa",
        destination="postgres",
        dataset_name="metrics",
    )

    source = wepa_source(years=years, mode=mode)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


def main() -> NoReturn:
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()

    # Handle status check
    if args.status:
        show_status()
        sys.exit(0)

    # Require source if not status check
    if not args.source:
        parser.print_help()
        sys.exit(1)

    # Check rate limit before starting
    rate_limiter = get_rate_limiter()
    if not rate_limiter.check_budget(10):  # Need at least 10 calls
        print(f"ERROR: API budget nearly exhausted. {rate_limiter.remaining} calls remaining.")
        print("Wait for next month or upgrade your CFBD tier.")
        sys.exit(1)

    # Validate rosters requires --teams
    if args.source == "rosters" and not args.teams:
        print("ERROR: --teams is required for rosters source")
        print("Example: --source rosters --teams Alabama Georgia 'Ohio State'")
        sys.exit(1)

    # Dry run mode
    if args.dry_run:
        print(f"[DRY RUN] Would load source: {args.source}")
        print(f"[DRY RUN] Mode: {args.mode}")
        if args.years:
            print(f"[DRY RUN] Years: {args.years}")
        if args.teams:
            print(f"[DRY RUN] Teams: {args.teams}")
        if args.weekly and args.source == "game_stats" and args.years:
            total_runs = sum(15 + 5 for _ in args.years)
            print(f"[DRY RUN] Weekly mode: ~{total_runs} pipeline.run() calls")
        elif args.batch_size and args.years:
            batches = batch_years(args.years, args.batch_size)
            print(f"[DRY RUN] Batch size: {args.batch_size}")
            print(f"[DRY RUN] Would run {len(batches)} batches: {batches}")
        sys.exit(0)

    # Run the appropriate pipeline
    # Weekly mode for game_stats: route to per-week loader
    if args.weekly and args.source == "game_stats":
        if not args.years:
            print("ERROR: --weekly requires --years")
            sys.exit(1)
        run_game_stats_weekly(args.years, use_replace=args.replace)
        show_status()
        sys.exit(0)

    source_runners = {
        "reference": lambda: run_reference_pipeline(),
        "games": lambda: run_games_pipeline(args.years, args.mode),
        "game_stats": lambda: run_game_stats_pipeline(
            args.years, args.mode, args.batch_size, args.replace
        ),
        "plays": lambda: run_plays_pipeline(args.years, args.mode),
        "stats": lambda: run_stats_pipeline(args.years, args.mode),
        "ratings": lambda: run_ratings_pipeline(args.years, args.mode),
        "recruiting": lambda: run_recruiting_pipeline(args.years, args.mode),
        "betting": lambda: run_betting_pipeline(args.years, args.mode),
        "draft": lambda: run_draft_pipeline(args.years, args.mode),
        "metrics": lambda: run_metrics_pipeline(args.years, args.mode),
        "metrics_wp": lambda: run_metrics_wp_pipeline(args.years, args.batch_size or 50),
        "metrics_ppa_predicted": lambda: run_metrics_ppa_predicted_pipeline(),
        "rankings": lambda: run_rankings_pipeline(args.years, args.mode),
        "rosters": lambda: run_rosters_pipeline(args.teams, args.years, args.mode),
        "wepa": lambda: run_wepa_pipeline(args.years, args.mode),
        "passing": lambda: run_passing_pipeline(args.years, args.mode),
        "playoffs": lambda: run_playoffs_pipeline(args.years, args.mode),
        "coaches": lambda: run_coaches_pipeline(args.years, args.mode),
        "coach_tenures": lambda: run_coach_tenures_pipeline(args.teams, args.years),
        "coach_profiles": lambda: run_coach_profiles_pipeline(),
        "conferences": lambda: run_conferences_pipeline(args.years, args.mode),
        "player_overview": lambda: run_player_overview_pipeline(),
    }

    if args.source == "all":
        # Run all pipelines. Continuing past a failure is deliberate -- one
        # broken source should not cost the other twelve their data -- but the
        # exit code has to carry it, or a scheduled `--source all` reports a
        # clean load while a source failed. Roster resolution now RAISES on an
        # unloaded schedule precisely so it cannot no-op silently; swallowing
        # that here would put the silence straight back.
        failed = []
        for name, runner in source_runners.items():
            try:
                runner()
            except Exception as e:
                print(f"ERROR in {name}: {e}")
                failed.append(name)
                continue
        if failed:
            show_status()
            print(f"\n{len(failed)} of {len(source_runners)} sources failed: {', '.join(failed)}")
            sys.exit(1)
    else:
        runner = source_runners.get(args.source)
        if runner:
            runner()
        else:
            print(f"Unknown source: {args.source}")
            sys.exit(1)

    # Show final status
    show_status()
    sys.exit(0)


if __name__ == "__main__":
    main()
