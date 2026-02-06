#!/usr/bin/env python3
"""Load or refresh all data for a specific season.

Orchestrates pipeline sources in dependency order and refreshes materialized views.

Usage:
    python scripts/load_season.py --season 2025                     # Load everything for 2025
    python scripts/load_season.py --season 2025 --sources games,stats  # Load specific sources
    python scripts/load_season.py --season 2025 --dry-run           # Show what would run
    python scripts/load_season.py --season 2025 --skip-refresh      # Load data, skip mart refresh
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
    "metrics",  # PPA, win probability
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
    "rosters": 150,
}


def load_season(
    season: int,
    sources: list[str] | None = None,
    dry_run: bool = False,
    skip_refresh: bool = False,
) -> dict:
    """Load or refresh all data for a given season.

    Args:
        season: The season year to load
        sources: Specific sources to load (None = all)
        dry_run: If True, show plan without executing
        skip_refresh: If True, skip mart refresh after loading

    Returns:
        Summary dict with timing and row counts
    """
    from src.pipelines.run import (
        run_betting_pipeline,
        run_draft_pipeline,
        run_game_stats_pipeline,
        run_games_pipeline,
        run_metrics_pipeline,
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
        if not skip_refresh:
            print("  + Refresh all materialized views after loading")
        return {"dry_run": True, "estimated_calls": total_est}

    # Map source names to runner functions
    runners = {
        "reference": lambda: run_reference_pipeline(),
        "games": lambda: run_games_pipeline(years=[season]),
        "game_stats": lambda: run_game_stats_pipeline(years=[season]),
        "plays": lambda: run_plays_pipeline(years=[season]),
        "stats": lambda: run_stats_pipeline(years=[season]),
        "ratings": lambda: run_ratings_pipeline(years=[season]),
        "rankings": lambda: run_rankings_pipeline(years=[season]),
        "recruiting": lambda: run_recruiting_pipeline(years=[season]),
        "betting": lambda: run_betting_pipeline(years=[season]),
        "draft": lambda: run_draft_pipeline(years=[season]),
        "metrics": lambda: run_metrics_pipeline(years=[season]),
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
        "total_duration_s": round(total_elapsed, 1),
        "successes": successes,
        "errors": errors,
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Load all data for a specific season")
    parser.add_argument("--season", type=int, required=True, help="Season year to load")
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
    args = parser.parse_args()

    sources = args.sources.split(",") if args.sources else None

    summary = load_season(
        season=args.season,
        sources=sources,
        dry_run=args.dry_run,
        skip_refresh=args.skip_refresh,
    )

    if summary.get("errors", 0) > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
