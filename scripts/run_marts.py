#!/usr/bin/env python3
"""Deploy mart SQL files to Supabase Postgres.

Runs CREATE/DROP statements for materialized views and regular views in
src/schemas/marts/. This is separate from run_migrations.py which handles
core schema DDL.

Usage:
    python scripts/run_marts.py                    # Run all marts
    python scripts/run_marts.py --from 017         # Run from 017 onwards
    python scripts/run_marts.py --only 017         # Run only files starting with 017
    python scripts/run_marts.py --dry-run          # Print SQL without executing
    python scripts/run_marts.py --list             # List available mart files
"""

import argparse
import logging
import sys
from pathlib import Path

import dlt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MARTS_DIR = Path(__file__).parent.parent / "src" / "schemas" / "marts"


def get_db_url() -> str:
    """Get database URL from dlt secrets or environment."""
    import os

    try:
        creds = dlt.secrets.get("destination.postgres.credentials")
        if creds:
            return str(creds)
    except Exception:
        pass

    url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if url:
        return url

    raise RuntimeError(
        "No database URL found. Set destination.postgres.credentials in "
        ".dlt/secrets.toml or SUPABASE_DB_URL environment variable."
    )


def get_mart_files() -> list[Path]:
    """Get all mart SQL files sorted by filename."""
    if not MARTS_DIR.exists():
        logger.error(f"Marts directory not found: {MARTS_DIR}")
        sys.exit(1)

    files = sorted(MARTS_DIR.glob("*.sql"))
    if not files:
        logger.error(f"No SQL files found in {MARTS_DIR}")
        sys.exit(1)

    return files


def run_mart(sql_file: Path, conn, dry_run: bool = False) -> None:
    """Execute a single mart SQL file."""
    sql = sql_file.read_text()
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Running {sql_file.name}")

    if dry_run:
        print(f"\n-- {sql_file.name}")
        print(sql[:500] + "..." if len(sql) > 500 else sql)
        return

    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        logger.info(f"  {sql_file.name} completed successfully")
    except Exception as e:
        conn.rollback()
        logger.error(f"  {sql_file.name} FAILED: {e}")
        raise
    finally:
        cursor.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy mart SQL files")
    parser.add_argument(
        "--from",
        dest="from_num",
        help="Start from mart number (e.g., 017)",
    )
    parser.add_argument(
        "--only",
        help="Run only marts starting with this number (e.g., 017)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print SQL without executing",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available mart files and exit",
    )
    args = parser.parse_args()

    all_files = get_mart_files()

    if args.list:
        print(f"Mart files in {MARTS_DIR}:\n")
        for f in all_files:
            print(f"  {f.name}")
        print(f"\nTotal: {len(all_files)} files")
        return

    # Filter files
    files = all_files
    if args.only:
        files = [f for f in files if f.name.startswith(args.only)]
        if not files:
            logger.error(f"No mart files found starting with {args.only}")
            sys.exit(1)
    elif args.from_num:
        files = [f for f in files if f.name[:3] >= args.from_num]

    logger.info(f"Running {len(files)} mart file(s)")

    if args.dry_run:
        for f in files:
            run_mart(f, conn=None, dry_run=True)
        return

    # Connect and run
    import psycopg2

    db_url = get_db_url()
    conn = psycopg2.connect(db_url)

    try:
        for f in files:
            run_mart(f, conn)
        logger.info("All marts deployed successfully")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
