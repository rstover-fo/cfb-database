#!/usr/bin/env python3
"""Refresh all materialized views in dependency order.

Usage:
    python scripts/refresh_marts.py                    # Refresh all marts
    python scripts/refresh_marts.py --no-concurrent    # Without CONCURRENTLY (blocks reads)
    python scripts/refresh_marts.py --schema marts     # Only marts schema
    python scripts/refresh_marts.py --schema analytics # Only analytics schema
    python scripts/refresh_marts.py --dry-run          # Print SQL without executing
    python scripts/refresh_marts.py --views marts.house_elo,marts.house_elo_game
    python scripts/refresh_marts.py --changed ratings.sdv_ratings_weekly --dry-run
"""

import argparse
import logging
import sys
from datetime import datetime

import dlt

from src.pipelines.utils.refresh_plan import REFRESH_GRAPH

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Keep the public lists for existing callers, but derive both from the registry.

MARTS_VIEWS = list(REFRESH_GRAPH.plan(schema="marts").views)
ANALYTICS_VIEWS = list(REFRESH_GRAPH.plan(schema="analytics").views)


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
    views: list[str] | None = None,
    changed: list[str] | None = None,
) -> int:
    """Refresh selected SQL descendants; return failed plus blocked count.

    Explicit views are registry-validated and dependency-ordered. Other parents
    are assumed current, as with the existing targeted refresh contract.
    Changed relations must already be committed; their SQL descendants are
    selected automatically. This does not execute ingestion or compute jobs.
    """
    try:
        plan = REFRESH_GRAPH.plan(views=views, changed=changed, schema=schema)
    except ValueError as exc:
        logger.error("Invalid refresh plan: %s", exc)
        return 1
    views = list(plan.views)
    if not views:
        logger.info("No dependent materialized views require refresh")
        return 0

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
        # get_db_url() appends statement_timeout=0 as an `options=` STARTUP
        # parameter, but Supabase's session pooler (Supavisor) silently
        # ignores startup options -- the role-default 2-minute timeout still
        # applied, and marts.play_epa (the one refresh that exceeds it under
        # load) was cancelled at exactly ~120s in two runs on 2026-08-30.
        # Setting it as a real session statement survives the pooler; it
        # lives inside this try so a setup failure still returns the pooled
        # connection via the finally below.
        with conn.cursor() as _cur:
            _cur.execute("SET statement_timeout = 0")
        conn.commit()

        unsuccessful: set[str] = set()
        for view in views:
            blocked_by = REFRESH_GRAPH.ancestors(view).intersection(unsuccessful)
            if blocked_by:
                logger.error("Blocked %s after upstream failure: %s", view, sorted(blocked_by))
                unsuccessful.add(view)
                failures += 1
            elif not refresh_view(view, conn, concurrently, dry_run):
                unsuccessful.add(view)
                failures += 1
    finally:
        conn.close()

    if failures:
        logger.warning(f"{failures} view(s) failed or were blocked")
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
    parser.add_argument(
        "--views",
        help=(
            "Comma-separated registered matview names to refresh in dependency order "
            "(overrides --schema and the full layered list)"
        ),
    )
    parser.add_argument(
        "--changed",
        help="Committed relations; refresh SQL descendants (overrides --schema)",
    )
    args = parser.parse_args()
    if args.views is not None and args.changed is not None:
        parser.error("--views and --changed cannot be combined")

    view_list = (
        [v.strip() for v in args.views.split(",") if v.strip()] if args.views is not None else None
    )

    failures = refresh_marts(
        schema=args.schema,
        concurrently=not args.no_concurrent,
        dry_run=args.dry_run,
        views=view_list,
        changed=[v.strip() for v in args.changed.split(",") if v.strip()]
        if args.changed is not None
        else None,
    )
    sys.exit(failures)


if __name__ == "__main__":
    main()
