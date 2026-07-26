#!/usr/bin/env python3
"""Load or refresh all data for a specific season.

Orchestrates pipeline sources in dependency order and refreshes materialized views.

Usage:
    python scripts/load_season.py                                   # Load current season
    python scripts/load_season.py --season 2025                     # Load everything for 2025
    python scripts/load_season.py --season 2025 --sources games,stats  # Load specific sources
    python scripts/load_season.py --season 2025 --dry-run           # Show what would run
    python scripts/load_season.py --season 2025 --skip-refresh      # Load data, skip mart refresh
    python scripts/load_season.py --season 2025 --weekly            # game_stats week-by-week
"""

import argparse
import logging
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Source loading order matters: dependencies first
SOURCE_ORDER = [
    "reference",  # Teams, conferences, venues (no year filter)
    "games",  # Game results
    "game_stats",  # Team and player box scores
    "plays",  # Play-by-play (largest dataset)
    "stats",  # Aggregated season stats
    "ratings",  # SP+, Elo, FPI, SRS
    "rankings",  # AP, Coaches polls
    "recruiting",  # Recruits, team composites
    "betting",  # Betting lines
    "draft",  # NFL draft picks
    "metrics",  # PPA, pregame win probability
    "metrics_wp",  # In-game win probability -- game-id-driven, see run_metrics_wp_pipeline
    "rosters",  # Team rosters
]

# Estimated API calls per source per season (rough averages)
ESTIMATED_CALLS = {
    "reference": 10,
    "games": 15,
    "game_stats": 200,
    "plays": 400,
    "stats": 20,
    "ratings": 10,
    "rankings": 20,
    "recruiting": 15,
    "betting": 5,
    "draft": 5,
    "metrics": 30,
    # One call per completed game still missing from metrics.win_probability.
    # ~70 is a typical in-season week's worth of newly-completed FBS+FCS
    # games (the steady-state daily/weekly incremental case); a full-season
    # backfill run resolves far more missing games and costs proportionally
    # more -- this estimate is for the dry-run printout, not a hard cap.
    "metrics_wp": 70,
    "rosters": 150,
}


# Sources whose data for a FINISHED season cannot change, so re-fetching them
# daily buys nothing and costs the whole budget.
#
# WHY THIS EXISTS. The daily workflow calls load_season.py --weekly with no
# --season, so get_current_season() resolves to `year - 1` until August: every
# off-season run re-ingested the entire, complete, immutable previous season.
# plays fans out to one /plays/stats call PER GAME (~1,600 for a full season)
# and rosters to one call per team, against a 75,000/month budget -- roughly
# 2,000 calls a day for data that cannot have changed. That is what exhausted
# the quota and produced the 2026-07-25 run that spent three hours being
# rate-limited (see api_client.RateLimitExhausted).
#
# NOT on this list, deliberately:
#   reference   -- no year filter and ~10 calls; always cheap, always current.
#   metrics_wp  -- already self-limiting; it only fetches games still missing
#                  win probability, so a fully-backfilled season costs nothing.
IMMUTABLE_ONCE_FINAL = frozenset(
    {
        "games",
        "game_stats",
        "plays",
        "stats",
        "ratings",
        "rankings",
        "recruiting",
        "betting",
        "draft",
        "metrics",
        "rosters",
    }
)

# A season counts as finished on the same terms train_model.py uses for its
# refit guard -- one definition of "finished" in the codebase, not two. A
# permanently un-completed row (a cancellation) must not freeze a season as
# unfinished forever, hence a tolerance rather than requiring literal 100%.
SEASON_COMPLETE_THRESHOLD = 0.99
MIN_GAMES_FOR_FINISHED_SEASON = 100


def season_is_final(conn, season: int) -> bool:
    """True when `season` has essentially every scheduled game completed."""
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
    if not n or n < MIN_GAMES_FOR_FINISHED_SEASON:
        return False
    return float(pct or 0.0) >= SEASON_COMPLETE_THRESHOLD


def sources_to_skip(active_sources, season_final: bool, allow_skip: bool):
    """Immutable sources to skip for a finished season.

    ``allow_skip`` is False whenever the caller named a season or specific
    sources explicitly. A backfill (``--season 2019 --sources plays``) targets
    a finished season BY DEFINITION, so skipping there would silently turn
    every backfill into a no-op -- the failure mode this guard must not
    introduce. Only the unattended daily path, which passes neither, is
    eligible.
    """
    if not (allow_skip and season_final):
        return []
    return [s for s in active_sources if s in IMMUTABLE_ONCE_FINAL]


def upcoming_schedule_season(season: int, month: int) -> int | None:
    """Return the next season to schedule-refresh during the off-season.

    Before August, get_current_season() still points at the previous season,
    but the upcoming season's schedule is already published on CFBD. Auto-mode
    loads keep it fresh via a cheap games-only pull (~15 calls) so the daily
    automation never depends on a manual dispatch to pick up the new season.
    """
    return season + 1 if month < 8 else None


def load_season(
    season: int,
    sources: list[str] | None = None,
    dry_run: bool = False,
    skip_refresh: bool = False,
    weekly: bool = False,
    upcoming_schedule: int | None = None,
    allow_skip_final: bool = False,
) -> dict:
    """Load or refresh all data for a given season.

    Args:
        season: The season year to load
        sources: Specific sources to load (None = all)
        dry_run: If True, show plan without executing
        skip_refresh: If True, skip mart refresh after loading
        weekly: If True, load game_stats week-by-week (~35K rows per merge)
        upcoming_schedule: If set, also refresh this season's schedule tables
            (games source only) after the main load

    Returns:
        Summary dict with timing and row counts
    """
    from src.pipelines.run import (
        run_betting_pipeline,
        run_draft_pipeline,
        run_game_stats_pipeline,
        run_game_stats_weekly,
        run_games_pipeline,
        run_metrics_pipeline,
        run_metrics_wp_pipeline,
        run_plays_pipeline,
        run_rankings_pipeline,
        run_ratings_pipeline,
        run_recruiting_pipeline,
        run_reference_pipeline,
        run_stats_pipeline,
    )
    from src.pipelines.utils.rate_limiter import get_rate_limiter

    # Determine which sources to run
    active_sources = sources if sources else [s for s in SOURCE_ORDER if s != "rosters"]

    # Validate sources
    valid = set(SOURCE_ORDER)
    invalid = [s for s in active_sources if s not in valid]
    if invalid:
        logger.error(f"Unknown sources: {invalid}. Valid: {sorted(valid)}")
        return {"error": f"Unknown sources: {invalid}"}

    # Drop immutable sources for a finished season before estimating, so the
    # dry-run figure and the budget check reflect what will actually be
    # fetched rather than what would have been.
    skipped_final = []
    if allow_skip_final:
        import psycopg2

        from scripts.compute_predictions import get_db_url

        conn_check = psycopg2.connect(get_db_url())
        try:
            final = season_is_final(conn_check, season)
        finally:
            conn_check.close()
        skipped_final = sources_to_skip(active_sources, final, allow_skip_final)
        if skipped_final:
            saved = sum(ESTIMATED_CALLS.get(s, 50) for s in skipped_final)
            logger.info(
                "Season %d is finished; skipping %d immutable source(s) -- %s "
                "(~%d API calls saved). Data for a completed season cannot "
                "change; pass --season %d or --no-skip-final to force a reload.",
                season,
                len(skipped_final),
                ", ".join(skipped_final),
                saved,
                season,
            )
            active_sources = [s for s in active_sources if s not in skipped_final]
        elif final:
            logger.info("Season %d is finished but no immutable source was selected", season)

    # Estimate API calls
    total_est = sum(ESTIMATED_CALLS.get(s, 50) for s in active_sources)

    # Check rate limit budget
    rate_limiter = get_rate_limiter()
    status = rate_limiter.get_status()
    remaining = status["remaining"]

    logger.info(f"Season: {season}")
    logger.info(f"Sources: {', '.join(active_sources)}")
    logger.info(f"Estimated API calls: ~{total_est:,}")
    logger.info(f"Rate limit remaining: {remaining:,}")

    if total_est > remaining:
        logger.warning(
            f"Estimated calls ({total_est:,}) may exceed remaining budget ({remaining:,})"
        )

    if dry_run:
        print(f"\n[DRY RUN] Would load {len(active_sources)} sources for season {season}")
        for src in active_sources:
            est = ESTIMATED_CALLS.get(src, 50)
            print(f"  {src:15s}  ~{est:,} API calls")
        print(f"\n  Total estimated:  ~{total_est:,} calls")
        print(f"  Budget remaining: {remaining:,} calls")
        if upcoming_schedule:
            print(
                f"  + Refresh {upcoming_schedule} schedule + betting lines "
                "(games + betting sources, ~20 calls)"
            )
        if not skip_refresh:
            print("  + Refresh all materialized views after loading")
        return {"dry_run": True, "estimated_calls": total_est}

    # Map source names to runner functions
    game_stats_runner = (
        (lambda: run_game_stats_weekly(years=[season]))
        if weekly
        else (lambda: run_game_stats_pipeline(years=[season]))
    )
    runners = {
        "reference": lambda: run_reference_pipeline(),
        "games": lambda: run_games_pipeline(years=[season]),
        "game_stats": game_stats_runner,
        "plays": lambda: run_plays_pipeline(years=[season]),
        "stats": lambda: run_stats_pipeline(years=[season]),
        "ratings": lambda: run_ratings_pipeline(years=[season]),
        "rankings": lambda: run_rankings_pipeline(years=[season]),
        "recruiting": lambda: run_recruiting_pipeline(years=[season]),
        "betting": lambda: run_betting_pipeline(years=[season]),
        "draft": lambda: run_draft_pipeline(years=[season]),
        "metrics": lambda: run_metrics_pipeline(years=[season]),
        "metrics_wp": lambda: run_metrics_wp_pipeline(seasons=[season]),
    }

    results = {}
    total_start = time.time()

    for src in active_sources:
        runner = runners.get(src)
        if not runner:
            logger.warning(f"No runner for source: {src} (skipping)")
            continue

        logger.info(f"Loading {src} for season {season}...")
        src_start = time.time()
        try:
            info = runner()
            elapsed = time.time() - src_start
            results[src] = {"status": "ok", "duration_s": round(elapsed, 1), "info": str(info)}
            logger.info(f"  {src} completed in {elapsed:.1f}s")
        except Exception as e:
            elapsed = time.time() - src_start
            results[src] = {"status": "error", "duration_s": round(elapsed, 1), "error": str(e)}
            logger.error(f"  {src} failed after {elapsed:.1f}s: {e}")

    # Off-season: keep the upcoming season's published schedule and betting
    # lines fresh. Betting matters here because line_snapshots only records
    # pending games -- pre-August, only the upcoming season has any, so
    # skipping it would lose exactly the preseason line-movement history the
    # append-only snapshot feature exists to capture.
    if upcoming_schedule:
        upcoming_runners = {
            "games_upcoming": lambda: run_games_pipeline(years=[upcoming_schedule]),
            "betting_upcoming": lambda: run_betting_pipeline(years=[upcoming_schedule]),
        }
        for name, runner in upcoming_runners.items():
            logger.info(f"Refreshing upcoming season {upcoming_schedule}: {name}...")
            src_start = time.time()
            try:
                info = runner()
                elapsed = time.time() - src_start
                results[name] = {
                    "status": "ok",
                    "duration_s": round(elapsed, 1),
                    "info": str(info),
                }
            except Exception as e:
                elapsed = time.time() - src_start
                results[name] = {
                    "status": "error",
                    "duration_s": round(elapsed, 1),
                    "error": str(e),
                }
                logger.error(f"  {name} failed after {elapsed:.1f}s: {e}")

    # Refresh marts
    if not skip_refresh:
        logger.info("Refreshing materialized views...")
        from scripts.refresh_marts import refresh_marts

        refresh_start = time.time()
        failures = refresh_marts(concurrently=True)
        refresh_elapsed = time.time() - refresh_start
        results["_mart_refresh"] = {
            "status": "ok" if failures == 0 else "partial",
            "duration_s": round(refresh_elapsed, 1),
            "failures": failures,
        }

    total_elapsed = time.time() - total_start

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"Season {season} Load Summary")
    print(f"{'=' * 60}")
    successes = sum(1 for r in results.values() if r["status"] == "ok")
    errors = sum(1 for r in results.values() if r["status"] == "error")
    for name, res in results.items():
        status_icon = "OK" if res["status"] == "ok" else "FAIL"
        print(f"  [{status_icon:4s}] {name:25s} {res['duration_s']:>8.1f}s")
    print(f"{'=' * 60}")
    print(f"  Total: {total_elapsed:.1f}s | {successes} succeeded, {errors} failed")

    return {
        "season": season,
        "skipped_final": skipped_final,
        "total_duration_s": round(total_elapsed, 1),
        "successes": successes,
        "errors": errors,
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Load all data for a specific season")
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Season year to load (default: current season)",
    )
    parser.add_argument(
        "--sources",
        type=str,
        default=None,
        help="Comma-separated list of sources to load (default: all)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show plan without executing")
    parser.add_argument(
        "--skip-refresh", action="store_true", help="Skip mart refresh after loading"
    )
    parser.add_argument(
        "--no-skip-final",
        action="store_true",
        help="Load immutable sources even for a finished season. Only relevant "
        "on the unattended path (no --season, no --sources), where they are "
        "skipped by default because their data cannot change.",
    )
    parser.add_argument(
        "--weekly",
        action="store_true",
        help="Load game_stats week-by-week (~35K rows per merge) to avoid timeouts",
    )
    args = parser.parse_args()

    season = args.season
    upcoming = None
    if season is None:
        from datetime import datetime

        from src.pipelines.config.years import get_current_season

        season = get_current_season()
        upcoming = upcoming_schedule_season(season, datetime.now().month)
        logger.info(f"No --season given; using current season {season}")
        if upcoming:
            logger.info(f"Off-season: will also refresh the {upcoming} schedule")

    sources = args.sources.split(",") if args.sources else None

    # The daily workflow runs with neither --season nor --sources. Anything
    # more specific is a deliberate operator request -- most often a backfill,
    # which targets a finished season by definition -- so the skip is off.
    allow_skip_final = args.season is None and not args.sources and not args.no_skip_final

    summary = load_season(
        season=season,
        sources=sources,
        dry_run=args.dry_run,
        skip_refresh=args.skip_refresh,
        weekly=args.weekly,
        upcoming_schedule=upcoming,
        allow_skip_final=allow_skip_final,
    )

    if summary.get("errors", 0) > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
