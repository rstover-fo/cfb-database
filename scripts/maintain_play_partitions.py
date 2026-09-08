"""Inspect or create the bounded ``core.plays`` season partition horizon."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import psycopg2  # noqa: E402

from src.pipelines.utils import load_ledger  # noqa: E402
from src.pipelines.utils.partitions import ensure_play_partitions  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate core.plays partitions and optionally create the bounded horizon"
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="*",
        default=(),
        help="Additional historical seasons to require (default: rolling current/next years)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Atomically create missing partitions after full catalog validation",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    conn = None
    try:
        conn = psycopg2.connect(load_ledger.get_db_url())
        plan = ensure_play_partitions(conn, args.years, create=args.apply)
        print(
            json.dumps(
                {
                    "apply": args.apply,
                    "created_years": list(plan.created_years),
                    "existing_years": list(plan.existing_years),
                    "missing_years": list(plan.missing_years),
                    "required_years": list(plan.required_years),
                    "valid": True,
                },
                sort_keys=True,
            )
        )
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "error": {
                        "message": str(exc) or type(exc).__name__,
                        "type": type(exc).__name__,
                    },
                    "valid": False,
                },
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
