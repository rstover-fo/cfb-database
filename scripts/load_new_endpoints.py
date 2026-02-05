"""Load the 5 new endpoints added in API completion sprint."""

import dlt

from src.pipelines.sources.betting import team_ats_resource
from src.pipelines.sources.metrics import fg_expected_points_resource
from src.pipelines.sources.stats import game_havoc_resource, play_stats_resource

# Use a limited year range for initial testing (recent years with more data availability)
TEST_YEARS = [2020, 2021, 2022, 2023, 2024, 2025]


def load_fg_expected_points():
    """Load fg_expected_points (static lookup)."""
    pipeline = dlt.pipeline(
        pipeline_name="cfbd_fg_ep",
        destination="postgres",
        dataset_name="metrics",
    )
    print("\n=== Loading fg_expected_points (static lookup) ===")
    info = pipeline.run(fg_expected_points_resource())
    print(f"Load info: {info}")
    return info


def load_team_ats(years):
    """Load team_ats for specified years."""
    pipeline = dlt.pipeline(
        pipeline_name="cfbd_team_ats",
        destination="postgres",
        dataset_name="betting",
    )
    print(f"\n=== Loading team_ats for years {years} ===")
    info = pipeline.run(team_ats_resource(years))
    print(f"Load info: {info}")
    return info


def load_play_stats(years):
    """Load play_stats for specified years (large table!).

    Iterates by gameId to avoid API 2000 record limit per request.

    Args:
        years: List of years to load
    """
    pipeline = dlt.pipeline(
        pipeline_name="cfbd_play_stats",
        destination="postgres",
        dataset_name="stats",
    )
    print(f"\n=== Loading play_stats for years {years} ===")
    print("    (Iterating by gameId for complete data - this will take a while)")
    info = pipeline.run(play_stats_resource(years=years))
    print(f"Load info: {info}")
    return info


def load_game_havoc(years):
    """Load game_havoc for specified years."""
    pipeline = dlt.pipeline(
        pipeline_name="cfbd_game_havoc",
        destination="postgres",
        dataset_name="stats",
    )
    print(f"\n=== Loading game_havoc for years {years} ===")
    info = pipeline.run(game_havoc_resource(years))
    print(f"Load info: {info}")
    return info


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        endpoint = sys.argv[1]
        if endpoint == "fg_expected_points":
            load_fg_expected_points()
        elif endpoint == "team_ats":
            years = [int(y) for y in sys.argv[2:]] if len(sys.argv) > 2 else TEST_YEARS
            load_team_ats(years)
        elif endpoint == "play_stats":
            years = [int(y) for y in sys.argv[2:]] if len(sys.argv) > 2 else TEST_YEARS
            load_play_stats(years)
        elif endpoint == "game_havoc":
            years = [int(y) for y in sys.argv[2:]] if len(sys.argv) > 2 else TEST_YEARS
            load_game_havoc(years)
        else:
            print(f"Unknown endpoint: {endpoint}")
            print("Usage: python scripts/load_new_endpoints.py <endpoint> [years...]")
            print("Endpoints: fg_expected_points, team_ats, play_stats, game_havoc")
            sys.exit(1)
    else:
        # Load all new endpoints
        print("Loading all new endpoints...")
        load_fg_expected_points()
        load_team_ats(TEST_YEARS)
        load_game_havoc(TEST_YEARS)
        # play_stats is large - load separately
        print("\nNote: play_stats is a large table. Run separately with:")
        print("  python scripts/load_new_endpoints.py play_stats 2024 2025")
