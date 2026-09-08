"""Audit or explicitly repair the managed ``core.plays`` indexes."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import psycopg2  # noqa: E402

from scripts.bootstrap_warehouse import _redact_error, validate_database_url  # noqa: E402
from src.pipelines.utils.play_indexes import (  # noqa: E402
    EXPECTED_PLAY_INDEXES,
    inspect_play_indexes,
    repair_play_indexes,
)

DATABASE_ENV = "WAREHOUSE_DB_URL"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit core.plays indexes or run an explicit transactional repair"
    )
    parser.add_argument(
        "--db-url",
        help=f"Explicit PostgreSQL URL (default: {DATABASE_ENV})",
    )
    parser.add_argument(
        "--index",
        action="append",
        choices=tuple(EXPECTED_PLAY_INDEXES),
        default=[],
        help="Managed index to select; repeat for more than one",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the selected safe repairs in one write-blocking maintenance transaction",
    )
    return parser


def _database_url(explicit: str | None) -> str:
    database_url = explicit or os.environ.get(DATABASE_ENV)
    if not database_url:
        raise RuntimeError(f"{DATABASE_ENV} is required when --db-url is omitted")
    validate_database_url(database_url)
    return database_url


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    database_url: str | None = None
    conn = None
    try:
        if args.apply and not args.index:
            raise ValueError("--apply requires at least one explicit --index selection")
        database_url = _database_url(args.db_url)
        conn = psycopg2.connect(database_url)
        if args.apply:
            report = repair_play_indexes(conn, args.index)
        else:
            with conn.cursor() as cur:
                cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                report = inspect_play_indexes(cur, args.index or None)
            conn.rollback()
        print(json.dumps({"apply": args.apply, **report}, indent=2, sort_keys=True))
        return 0 if report["valid"] else 1
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        print(
            json.dumps(
                {
                    "error": {
                        "message": _redact_error(exc, database_url),
                        "type": type(exc).__name__,
                    },
                    "valid": False,
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
