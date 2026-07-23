"""Run schema migration scripts against Supabase Postgres.

Usage:
    python scripts/run_migrations.py                    # Run all migrations
    python scripts/run_migrations.py --from 002         # Run from 002 onwards
    python scripts/run_migrations.py --only 009         # Run only 009
    python scripts/run_migrations.py --dry-run          # Print SQL without executing
    python scripts/run_migrations.py --file src/schemas/public/008_trajectory_averages_function.sql

--file applies a single SQL file outside MIGRATION_ORDER -- the supported path
for one-off changes to public/, api/, and functions/ definitions.
"""

import argparse
import logging
import sys
from collections import deque
from pathlib import Path

import dlt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCHEMAS_DIR = Path(__file__).parent.parent / "src" / "schemas"

MIGRATION_ORDER = [
    "001_reference.sql",
    "002_core.sql",
    "003_stats.sql",
    "004_ratings.sql",
    "005_recruiting.sql",
    "006_betting.sql",
    "007_draft.sql",
    "008_metrics.sql",
    "009_variant_columns.sql",
    "010_analyze.sql",
    "011_partition_plays.sql",
    "012_foreign_keys.sql",
    "013_analytics_views.sql",
    "014_positions.sql",
    "015_plays_score_diff.sql",
    "016_analytics_indexes.sql",
    "017_era_reference.sql",
    "018_transactional_triggers.sql",
]


def get_db_url() -> str:
    """Get database URL from dlt secrets."""
    try:
        creds = dlt.secrets.get("destination.postgres.credentials")
        if creds:
            return str(creds)
    except Exception:
        pass

    import os

    url = os.environ.get("SUPABASE_DB_URL")
    if url:
        return url

    raise RuntimeError(
        "No database URL found. Set destination.postgres.credentials in "
        ".dlt/secrets.toml or SUPABASE_DB_URL environment variable."
    )


def _disable_statement_timeout(conn) -> None:
    """Heavy DDL (large matview creates) exceeds Supabase's default timeout,
    and multi-million-row hash joins spill to disk under the small default
    work_mem. One admin session; 64MB is safe on small compute tiers."""
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")
        cur.execute("SET work_mem = '64MB'")
    conn.commit()


def run_migration(sql_file: Path, conn, dry_run: bool = False) -> None:
    """Execute a single migration file."""
    sql = sql_file.read_text()
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Running {sql_file.name}")

    if dry_run:
        print(f"\n-- {sql_file.name}")
        print(sql)
        return

    # Surface server-side RAISE NOTICE output (validation files and probes
    # report through it); psycopg2's default notices list caps at 50.
    conn.notices = deque(maxlen=500)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        for notice in conn.notices:
            logger.info(f"  {notice.strip()}")
        logger.info(f"  {sql_file.name} completed successfully")
    except Exception as e:
        conn.rollback()
        for notice in conn.notices:
            logger.info(f"  {notice.strip()}")
        logger.error(f"  {sql_file.name} FAILED: {e}")
        raise
    finally:
        cursor.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run schema migrations")
    parser.add_argument("--from", dest="from_num", help="Start from migration number (e.g., 002)")
    parser.add_argument("--only", help="Run only this migration number (e.g., 009)")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    parser.add_argument(
        "--file",
        dest="sql_file",
        help="Apply a single SQL file (path relative to repo root or absolute)",
    )
    args = parser.parse_args()

    if args.sql_file:
        sql_path = Path(args.sql_file)
        if not sql_path.is_absolute():
            sql_path = Path(__file__).parent.parent / sql_path
        if not sql_path.exists():
            logger.error(f"SQL file not found: {sql_path}")
            sys.exit(1)

        if args.dry_run:
            run_migration(sql_path, conn=None, dry_run=True)
            return

        import psycopg2

        conn = psycopg2.connect(get_db_url())
        try:
            _disable_statement_timeout(conn)
            run_migration(sql_path, conn)
        finally:
            conn.close()
        return

    # Filter migrations
    migrations = MIGRATION_ORDER
    if args.only:
        migrations = [m for m in migrations if m.startswith(args.only)]
        if not migrations:
            logger.error(f"No migration found starting with {args.only}")
            sys.exit(1)
    elif args.from_num:
        migrations = [m for m in migrations if m[:3] >= args.from_num]

    # Verify files exist
    for m in migrations:
        path = SCHEMAS_DIR / m
        if not path.exists():
            logger.error(f"Migration file not found: {path}")
            sys.exit(1)

    logger.info(f"Running {len(migrations)} migration(s): {', '.join(migrations)}")

    if args.dry_run:
        for m in migrations:
            run_migration(SCHEMAS_DIR / m, conn=None, dry_run=True)
        return

    # Connect and run
    import psycopg2

    db_url = get_db_url()
    conn = psycopg2.connect(db_url)

    try:
        _disable_statement_timeout(conn)
        for m in migrations:
            run_migration(SCHEMAS_DIR / m, conn)
        logger.info("All migrations completed successfully")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
