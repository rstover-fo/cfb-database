#!/usr/bin/env python3
"""Refresh all materialized views in dependency order.

Usage:
    python scripts/refresh_marts.py                    # Refresh all marts
    python scripts/refresh_marts.py --no-concurrent    # Without CONCURRENTLY (blocks reads)
    python scripts/refresh_marts.py --schema marts     # Only marts schema
    python scripts/refresh_marts.py --schema analytics # Only analytics schema
    python scripts/refresh_marts.py --dry-run          # Print SQL without executing
"""

import argparse
import logging
import sys
from datetime import datetime

import dlt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Order matters: dependencies must refresh first
# _game_epa_calc -> team_epa_season
# team_season_summary -> conference_standings (analytics)
#
# NOTE: EPA views (_game_epa_calc, team_epa_season, situational_splits, defensive_havoc)
# take 10-15 minutes each because they process 2.7M plays. For Supabase, these require
# statement_timeout=0 which the script sets automatically.

MARTS_VIEWS = [
    # Core EPA views (order matters: _game_epa_calc first)
    # These are slow (~12 min each) due to processing 2.7M plays
    "marts._game_epa_calc",
    "marts.team_season_summary",
    "marts.team_epa_season",
    # Situational and play-level analysis (also slow ~12 min)
    "marts.situational_splits",
    "marts.defensive_havoc",
    # Drive-level views (faster ~1 min)
    "marts.scoring_opportunities",
    # Historical and reference (fast)
    "marts.matchup_history",
    "marts.recruiting_class",
    "marts.coach_record",
]

ANALYTICS_VIEWS = [
    "analytics.team_season_summary",
    "analytics.player_career_stats",
    "analytics.conference_standings",  # Depends on team_season_summary
    "analytics.team_recruiting_trend",
    "analytics.game_results",
]


def get_db_url() -> str:
    """Get database URL from dlt secrets or environment.

    Adds statement_timeout=0 for long-running EPA view refreshes.
    """
    import os

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

    # Add statement_timeout=0 for long-running EPA views
    # This is required for Supabase which has a default timeout
    if "options=" not in url:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}options=-c%20statement_timeout%3D0"

    return url


def refresh_view(view_name: str, conn, concurrently: bool, dry_run: bool) -> bool:
    """Refresh a single materialized view. Returns True if successful."""
    refresh_type = "CONCURRENTLY" if concurrently else ""
    sql = f"REFRESH MATERIALIZED VIEW {refresh_type} {view_name}"

    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Refreshing {view_name}...")

    if dry_run:
        print(f"  {sql};")
        return True

    cursor = conn.cursor()
    try:
        start = datetime.now()
        cursor.execute(sql)
        conn.commit()
        elapsed = (datetime.now() - start).total_seconds()
        logger.info(f"  ✓ {view_name} refreshed ({elapsed:.2f}s)")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"  ✗ {view_name} failed: {e}")
        return False
    finally:
        cursor.close()


def refresh_marts(
    schema: str | None = None,
    concurrently: bool = True,
    dry_run: bool = False,
) -> int:
    """Refresh materialized views. Returns count of failures."""
    # Build view list based on schema filter
    views = []
    if schema is None or schema == "marts":
        views.extend(MARTS_VIEWS)
    if schema is None or schema == "analytics":
        views.extend(ANALYTICS_VIEWS)

    if not views:
        logger.error(f"No views found for schema: {schema}")
        return 1

    logger.info(f"Refreshing {len(views)} materialized view(s)")
    if concurrently:
        logger.info("Using CONCURRENTLY (reads not blocked)")
    else:
        logger.info("Not using CONCURRENTLY (reads blocked during refresh)")

    if dry_run:
        for view in views:
            refresh_view(view, conn=None, concurrently=concurrently, dry_run=True)
        return 0

    import psycopg2

    db_url = get_db_url()
    conn = psycopg2.connect(db_url)

    failures = 0
    try:
        for view in views:
            if not refresh_view(view, conn, concurrently, dry_run):
                failures += 1
    finally:
        conn.close()

    if failures:
        logger.warning(f"{failures} view(s) failed to refresh")
    else:
        logger.info("All views refreshed successfully")

    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh materialized views")
    parser.add_argument(
        "--no-concurrent",
        action="store_true",
        help="Don't use CONCURRENTLY (blocks reads during refresh)",
    )
    parser.add_argument(
        "--schema",
        choices=["marts", "analytics"],
        help="Only refresh views in this schema",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print SQL without executing",
    )
    args = parser.parse_args()

    failures = refresh_marts(
        schema=args.schema,
        concurrently=not args.no_concurrent,
        dry_run=args.dry_run,
    )
    sys.exit(failures)


if __name__ == "__main__":
    main()
