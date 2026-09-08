#!/usr/bin/env python3
"""Plan or deploy a dependency-complete mart release.

Real mart changes require a checksum-verified release manifest so the engine can
validate the live dependency closure and execute the full release atomically.
The legacy file selectors remain available only for listing and SQL inspection.

Usage:
    python scripts/run_marts.py --release path/to/release.json
    python scripts/run_marts.py --release path/to/release.json --plan
    python scripts/run_marts.py --dry-run
    python scripts/run_marts.py --only 017 --dry-run
    python scripts/run_marts.py --list
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.bootstrap_warehouse import _redact_error, validate_database_url  # noqa: E402
from scripts.mart_release import execute_release, load_release, plan_release  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MARTS_DIR = REPO_ROOT / "src" / "schemas" / "marts"
DATABASE_ENV = "SUPABASE_DB_URL"
MANAGED_RELEASE_GUIDANCE = (
    "real mart deployment requires --release <manifest>; "
    "use --plan to preview the dependency closure or --dry-run to inspect legacy SQL"
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
    """Display legacy SQL; real single-file mart execution is unsupported."""
    if not dry_run:
        raise RuntimeError(MANAGED_RELEASE_GUIDANCE)

    sql = sql_file.read_text()
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Running {sql_file.name}")

    print(f"\n-- {sql_file.name}")
    print(sql[:500] + "..." if len(sql) > 500 else sql)


def get_release_db_url() -> str:
    """Return the explicit release database URI; never consult dlt/libpq fallbacks."""
    database_url = os.environ.get(DATABASE_ENV)
    if not database_url:
        raise RuntimeError(f"{DATABASE_ENV} is required; no other credential source is used")
    try:
        validate_database_url(database_url)
    except Exception as exc:
        # Reuse the bootstrap validator while naming this command's supported env var.
        message = str(exc).replace("WAREHOUSE_DB_URL", DATABASE_ENV)
        raise ValueError(message) from exc
    return database_url


def _plan_payload(plan, *, read_only: bool) -> dict[str, object]:
    """Build stable JSON output without serializing Path or connection objects."""
    return {
        "read_only": read_only,
        "valid": plan.valid,
        "roots": list(plan.roots),
        "live_closure": [{"kind": obj.kind, "identity": obj.identity} for obj in plan.live_closure],
        "declared_restores": [
            {"kind": obj.kind, "identity": obj.identity} for obj in plan.declared_restores
        ],
        "files": [
            {"path": str(release_file.path), "sha256": release_file.sha256}
            for release_file in plan.files
        ],
        "warnings": list(plan.warnings),
        "blockers": list(plan.blockers),
    }


def run_release(release_path: Path, *, read_only: bool) -> int:
    """Validate first, then plan or atomically execute one managed release."""
    database_url: str | None = None
    conn = None
    try:
        manifest = load_release(release_path, REPO_ROOT)
        database_url = get_release_db_url()

        import psycopg2

        conn = psycopg2.connect(database_url)
        plan = plan_release(conn, manifest) if read_only else execute_release(conn, manifest)
        print(json.dumps(_plan_payload(plan, read_only=read_only), indent=2, sort_keys=True))
        return 0 if plan.valid else 1
    except Exception as exc:
        logger.error(_redact_error(exc, database_url))
        return 1
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                logger.error(_redact_error(exc, database_url))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Plan or deploy a managed mart release")
    selectors = parser.add_mutually_exclusive_group()
    selectors.add_argument(
        "--from",
        dest="from_num",
        help="Select legacy SQL from this mart number (inspection only)",
    )
    selectors.add_argument(
        "--only",
        help="Select legacy SQL starting with this number (inspection only)",
    )
    selectors.add_argument(
        "--all",
        action="store_true",
        help="Select all legacy mart files (inspection only)",
    )
    parser.add_argument(
        "--release",
        type=Path,
        help="Checksum-verified dependency-complete mart release manifest",
    )
    parser.add_argument(
        "--plan",
        action="store_true",
        help="Preview a release in a read-only transaction",
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
    args = parser.parse_args(argv)

    legacy_options = args.from_num or args.only or args.all or args.dry_run or args.list
    if args.release and legacy_options:
        parser.error("--release cannot be combined with legacy selectors, --dry-run, or --list")
    if args.plan and not args.release:
        parser.error("--plan requires --release <manifest>")
    if args.release:
        return run_release(args.release, read_only=args.plan)
    if not args.dry_run and not args.list:
        parser.error(MANAGED_RELEASE_GUIDANCE)

    all_files = get_mart_files()

    if args.list:
        print(f"Mart files in {MARTS_DIR}:\n")
        for f in all_files:
            print(f"  {f.name}")
        print(f"\nTotal: {len(all_files)} files")
        return 0

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
        return 0

    # Guarded above: all non-release legacy paths are read-only diagnostics.
    raise AssertionError("unreachable real legacy mart execution")


if __name__ == "__main__":
    raise SystemExit(main())
