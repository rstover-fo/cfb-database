"""CLI entry point for CFB database pipelines."""

import argparse
import sys
from typing import NoReturn

import dlt

from .sources.betting import betting_source
from .sources.draft import draft_source
from .sources.games import games_source
from .sources.metrics import metrics_source
from .sources.players import players_source
from .sources.plays import plays_source
from .sources.rankings import rankings_source
from .sources.ratings import ratings_source
from .sources.recruiting import recruiting_source
from .sources.reference import reference_source
from .sources.stats import stats_source
from .utils.rate_limiter import get_rate_limiter


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

  # Check pipeline status and rate limits
  python -m src.pipelines.run --status
        """,
    )

    parser.add_argument(
        "--source",
        choices=[
            "reference",
            "games",
            "plays",
            "stats",
            "ratings",
            "recruiting",
            "betting",
            "draft",
            "metrics",
            "rankings",
            "players",
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
        "--status",
        action="store_true",
        help="Show pipeline status and rate limit usage",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be loaded without making API calls",
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
    """Run the games data pipeline."""
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Games Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_games",
        destination="postgres",
        dataset_name="core",
    )

    source = games_source(years=years, mode=mode)
    info = pipeline.run(source)

    print(f"\nLoad info: {info}")

    return info


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


def run_stats_pipeline(years: list[int] | None = None, mode: str = "incremental"):
    """Run the stats data pipeline."""
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Stats Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_stats",
        destination="postgres",
        dataset_name="stats",
    )

    source = stats_source(years=years, mode=mode)
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


def run_players_pipeline(years: list[int] | None = None, mode: str = "incremental"):
    """Run the players data pipeline."""
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Players Data ({years_str}) ===\n")

    pipeline = dlt.pipeline(
        pipeline_name="cfbd_players",
        destination="postgres",
        dataset_name="core",
    )

    source = players_source(years=years, mode=mode)
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

    # Dry run mode
    if args.dry_run:
        print(f"[DRY RUN] Would load source: {args.source}")
        print(f"[DRY RUN] Mode: {args.mode}")
        if args.years:
            print(f"[DRY RUN] Years: {args.years}")
        sys.exit(0)

    # Run the appropriate pipeline
    source_runners = {
        "reference": lambda: run_reference_pipeline(),
        "games": lambda: run_games_pipeline(args.years, args.mode),
        "plays": lambda: run_plays_pipeline(args.years, args.mode),
        "stats": lambda: run_stats_pipeline(args.years, args.mode),
        "ratings": lambda: run_ratings_pipeline(args.years, args.mode),
        "recruiting": lambda: run_recruiting_pipeline(args.years, args.mode),
        "betting": lambda: run_betting_pipeline(args.years, args.mode),
        "draft": lambda: run_draft_pipeline(args.years, args.mode),
        "metrics": lambda: run_metrics_pipeline(args.years, args.mode),
        "rankings": lambda: run_rankings_pipeline(args.years, args.mode),
        "players": lambda: run_players_pipeline(args.years, args.mode),
    }

    if args.source == "all":
        # Run all pipelines
        for name, runner in source_runners.items():
            try:
                runner()
            except Exception as e:
                print(f"ERROR in {name}: {e}")
                continue
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
