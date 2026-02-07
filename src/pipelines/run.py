"""CLI entry point for CFB database pipelines."""

import argparse
import sys
from typing import NoReturn

import dlt

from .sources.betting import betting_source
from .sources.draft import draft_source
from .sources.game_stats import game_stats_source
from .sources.games import games_source
from .sources.metrics import metrics_source
from .sources.plays import plays_source
from .sources.rankings import rankings_source
from .sources.ratings import ratings_source
from .sources.recruiting import recruiting_source
from .sources.reference import reference_source
from .sources.rosters import rosters_source
from .sources.stats import stats_source
from .sources.wepa import wepa_source
from .utils.rate_limiter import get_rate_limiter


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
            "rankings",
            "rosters",
            "wepa",
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


def run_rosters_pipeline(
    teams: list[str],
    years: list[int] | None = None,
    mode: str = "incremental",
):
    """Run the rosters data pipeline."""
    years_str = f"years={years}" if years else f"mode={mode}"
    print(f"\n=== Loading Rosters Data ({years_str}, teams={teams}) ===\n")

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
        "rankings": lambda: run_rankings_pipeline(args.years, args.mode),
        "rosters": lambda: run_rosters_pipeline(args.teams, args.years, args.mode),
        "wepa": lambda: run_wepa_pipeline(args.years, args.mode),
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
