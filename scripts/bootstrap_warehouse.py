"""Apply or inspect the managed warehouse migration manifest.

Usage:
    python scripts/bootstrap_warehouse.py bootstrap
    python scripts/bootstrap_warehouse.py upgrade --target <migration-id>
    python scripts/bootstrap_warehouse.py plan
    python scripts/bootstrap_warehouse.py status

The database connection is read only from ``WAREHOUSE_DB_URL``. Planning,
status, and ``--dry-run`` operations ask the migration engine to use a read-only
transaction.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlsplit

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.warehouse_migrations import (  # noqa: E402
    Manifest,
    MigrationPlan,
    MigrationStateError,
    apply_manifest,
    load_manifest,
)

DEFAULT_MANIFEST = REPO_ROOT / "src" / "schemas" / "warehouse-manifest.json"
DATABASE_ENV = "WAREHOUSE_DB_URL"
AMBIENT_LIBPQ_TARGET_ENV = frozenset(
    {
        "PGDATABASE",
        "PGHOST",
        "PGHOSTADDR",
        "PGPASSFILE",
        "PGPASSWORD",
        "PGPORT",
        "PGSERVICE",
        "PGSERVICEFILE",
        "PGUSER",
    }
)
FORBIDDEN_URI_QUERY_KEYS = frozenset(
    {
        "database",
        "dbname",
        "host",
        "hostaddr",
        "passfile",
        "password",
        "port",
        "service",
        "servicefile",
        "user",
    }
)
REQUIRED_CONNECTION_FIELDS = ("host", "port", "dbname", "user", "password")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bootstrap, upgrade, or inspect the managed warehouse schema"
    )
    parser.add_argument("action", choices=("bootstrap", "upgrade", "plan", "status"))
    parser.add_argument(
        "--target",
        help="Stop at this migration ID (must be present in the selected manifest)",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help="Manifest path, absolute or relative to the repository root",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan in a read-only transaction without applying migrations",
    )
    return parser


def _resolve_manifest_path(path: Path) -> Path:
    return path if path.is_absolute() else REPO_ROOT / path


def _require_target_in_manifest(manifest: Manifest, target: str | None) -> None:
    if target is not None and all(migration.id != target for migration in manifest.migrations):
        raise ValueError(f"target migration is not present in manifest: {target}")


def _plan_payload(plan: MigrationPlan, *, dry_run: bool) -> dict[str, object]:
    action_counts = {action: 0 for action in ("apply", "reapply", "skip", "deferred")}
    steps: list[dict[str, object]] = []
    for step in plan.steps:
        action_counts[step.action] += 1
        steps.append(
            {
                "order": step.order,
                "id": step.id,
                "path": step.path,
                "kind": step.kind,
                "checksum": step.checksum,
                "action": step.action,
            }
        )
    return {
        "mode": plan.mode,
        "target": plan.target,
        "dry_run": dry_run,
        "read_only": dry_run or plan.mode in {"plan", "status"},
        "ledger_installed": plan.ledger_installed,
        "valid": plan.valid,
        "summary": {
            **action_counts,
            "pending": len(plan.pending),
            "total": len(plan.steps),
        },
        "diagnostics": list(plan.diagnostics),
        "steps": steps,
    }


def _print_json(payload: dict[str, object], *, file=None) -> None:
    if file is None:
        file = sys.stdout
    print(json.dumps(payload, indent=2, sort_keys=True), file=file)


def _redact_error(exc: Exception, database_url: str | None = None) -> str:
    """Return useful exception text without disclosing connection credentials."""
    message = str(exc) or type(exc).__name__
    secrets: set[str] = set()
    if database_url:
        secrets.add(database_url)
        try:
            parsed = urlsplit(database_url)
        except ValueError:
            parsed = None
        if parsed is not None:
            for value in (parsed.username, parsed.password):
                if value:
                    secrets.add(value)
                    secrets.add(unquote(value))
    for secret in sorted(secrets, key=len, reverse=True):
        message = message.replace(secret, "[REDACTED]")
    message = re.sub(r"(?i)postgres(?:ql)?://[^\s]+", "[REDACTED_DATABASE_URL]", message)
    message = re.sub(r"(?i)(password\s*=\s*)\S+", r"\1[REDACTED]", message)
    return message


def validate_database_url(database_url: str) -> None:
    """Reject libpq URLs that can inherit their target or password from ambient state."""
    if re.search(r"%(?![0-9A-Fa-f]{2})", database_url):
        raise ValueError(f"{DATABASE_ENV} contains an invalid percent escape")
    try:
        parsed_url = urlsplit(database_url)
        parsed_port = parsed_url.port
    except ValueError as exc:
        raise ValueError(f"{DATABASE_ENV} is not a valid PostgreSQL URL") from exc
    if parsed_url.scheme not in {"postgres", "postgresql"}:
        raise ValueError(f"{DATABASE_ENV} must use postgres:// or postgresql://")
    if parsed_url.fragment:
        raise ValueError(f"{DATABASE_ENV} must not contain a URL fragment")

    try:
        query_items = parse_qsl(parsed_url.query, keep_blank_values=True, strict_parsing=True)
    except ValueError as exc:
        raise ValueError(f"{DATABASE_ENV} contains a malformed query string") from exc
    query_keys: set[str] = set()
    for raw_key, value in query_items:
        key = raw_key.lower()
        if not key or not value:
            raise ValueError(f"{DATABASE_ENV} query parameters must have nonempty keys and values")
        if key in query_keys:
            raise ValueError(f"{DATABASE_ENV} contains duplicate query parameter: {key}")
        if key in FORBIDDEN_URI_QUERY_KEYS:
            raise ValueError(
                f"{DATABASE_ENV} must put target and credentials in the URL authority/path; "
                f"query parameter {key!r} is not allowed"
            )
        query_keys.add(key)

    try:
        from psycopg2.extensions import parse_dsn

        connection_fields = parse_dsn(database_url)
    except Exception as exc:
        raise ValueError(f"{DATABASE_ENV} is not a valid PostgreSQL URL") from exc
    missing = [field for field in REQUIRED_CONNECTION_FIELDS if not connection_fields.get(field)]
    if missing:
        raise ValueError(f"{DATABASE_ENV} must explicitly include nonempty " + ", ".join(missing))
    if parsed_port is None:
        raise ValueError(f"{DATABASE_ENV} must explicitly include a port")
    host = unquote(connection_fields["host"])
    if "/" in host or "," in host or "\x00" in host:
        raise ValueError(f"{DATABASE_ENV} must name one TCP host, not a socket or host list")

    ambient = sorted(name for name in AMBIENT_LIBPQ_TARGET_ENV if os.environ.get(name))
    if ambient:
        raise ValueError(
            "ambient libpq target or credential variables are not allowed: " + ", ".join(ambient)
        )


def connect_database(database_url: str):
    """Open the explicit warehouse database URL with the supported driver."""
    import psycopg2

    validate_database_url(database_url)
    return psycopg2.connect(database_url)


def _error_payload(message: str, *, error_type: str) -> dict[str, object]:
    return {"error": {"type": error_type, "message": message}, "valid": False}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    database_url: str | None = None

    try:
        manifest = load_manifest(_resolve_manifest_path(args.manifest), REPO_ROOT)
        _require_target_in_manifest(manifest, args.target)
    except Exception as exc:
        _print_json(
            _error_payload(_redact_error(exc), error_type=type(exc).__name__),
            file=sys.stderr,
        )
        return 1

    database_url = os.environ.get(DATABASE_ENV)
    if not database_url:
        _print_json(
            _error_payload(
                f"{DATABASE_ENV} is required; no other credential source is used",
                error_type="ConfigurationError",
            ),
            file=sys.stderr,
        )
        return 1

    try:
        validate_database_url(database_url)
    except Exception as exc:
        _print_json(
            _error_payload(_redact_error(exc, database_url), error_type=type(exc).__name__),
            file=sys.stderr,
        )
        return 1

    try:
        conn = connect_database(database_url)
    except Exception as exc:
        _print_json(
            _error_payload(_redact_error(exc, database_url), error_type=type(exc).__name__),
            file=sys.stderr,
        )
        return 1

    result = 1
    plan: MigrationPlan | None = None
    try:
        try:
            plan = apply_manifest(
                conn,
                manifest,
                mode=args.action,
                target=args.target,
                dry_run=args.dry_run,
            )
        except MigrationStateError as exc:
            plan = exc.plan
        except Exception as exc:
            _print_json(
                _error_payload(_redact_error(exc, database_url), error_type=type(exc).__name__),
                file=sys.stderr,
            )

        if plan is not None:
            _print_json(_plan_payload(plan, dry_run=args.dry_run))
            result = 0 if plan.valid else 1
    finally:
        try:
            conn.close()
        except Exception as exc:
            _print_json(
                _error_payload(_redact_error(exc, database_url), error_type=type(exc).__name__),
                file=sys.stderr,
            )
            result = 1
    return result


if __name__ == "__main__":
    raise SystemExit(main())
