#!/usr/bin/env python3
"""Run the explicitly selected FPI source-receipt canary.

The canary is intentionally limited to one registered source and one explicit
season.  Both modes inspect the actual publisher connection and validate the
registered artifact.  ``publish`` additionally requires the runtime login to
be able to assume the bounded publisher role, then publishes the already
verified bytes through the normal receipt-backed adapter.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import parse_dsn

from src.pipelines.config.source_publication_assets import SDV_FPI_PUBLICATION
from src.pipelines.sources.flat_files import (
    REGISTRY,
    ParseContext,
    resolve_fetch_url,
    resolve_parser,
)
from src.pipelines.utils import flat_file_publication as publication
from src.pipelines.utils.file_fetcher import FetchedFile, fetch_file

SOURCE = "sdv_fpi_weekly"
PUBLISHER_ROLE = "warehouse_source_publisher"
SHA256_RE = re.compile(r"[0-9a-f]{64}")
PROJECT_REF_RE = re.compile(r"[a-z0-9]{20}")
DIRECT_HOST_RE = re.compile(r"^db\.([a-z0-9]{20})\.supabase\.co$")
POOLER_HOST_RE = re.compile(r"^[a-z0-9-]+(?:\.[a-z0-9-]+)*\.pooler\.supabase\.com$")
POOLER_USER_RE = re.compile(r"^[^.]+\.([a-z0-9]{20})$")
FORBIDDEN_DSN_ROUTING_PARAMETERS = frozenset({"hostaddr", "service", "servicefile"})
FORBIDDEN_LIBPQ_ROUTING_ENV = ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE")
ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_MANIFEST = ROOT / "src/schemas/production-manifest.json"


class CanaryError(RuntimeError):
    """A known canary validation failure safe to report without a traceback."""


@dataclass(frozen=True)
class Artifact:
    content: bytes
    sha256: str
    rows: int
    weeks: tuple[int, ...]

    def evidence(self) -> dict[str, Any]:
        return {
            "sha256": self.sha256,
            "rows": self.rows,
            "weeks": list(self.weeks),
        }


def _sha256(value: str) -> str:
    if SHA256_RE.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("must be exactly 64 lowercase hexadecimal characters")
    return value


def _project_ref(value: str) -> str:
    if PROJECT_REF_RE.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("must be a 20-character lowercase project reference")
    return value


def _season(value: str) -> int:
    try:
        season = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be one integer season") from error
    try:
        SDV_FPI_PUBLICATION.coverage_key(season)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error
    return season


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate or publish one checksum-pinned SDV FPI season receipt"
    )
    parser.add_argument("action", choices=("preflight", "publish"))
    parser.add_argument("--source", choices=(SOURCE,), required=True)
    parser.add_argument("--season", type=_season, required=True)
    parser.add_argument("--expected-sha256", type=_sha256, required=True)
    parser.add_argument("--expected-project-ref", type=_project_ref, required=True)
    return parser


def _expected_migrations() -> tuple[tuple[str, str, str, str, int], ...]:
    manifest = json.loads(PRODUCTION_MANIFEST.read_text())
    expected = []
    for order, entry in enumerate(manifest["migrations"], start=1):
        path = ROOT / entry["path"]
        expected.append(
            (
                entry["id"],
                entry["path"],
                entry["kind"],
                hashlib.sha256(path.read_bytes()).hexdigest(),
                order,
            )
        )
    return tuple(expected)


def _dsn_project_evidence(dsn: str, expected_project_ref: str) -> list[str]:
    ambient_routing = [name for name in FORBIDDEN_LIBPQ_ROUTING_ENV if os.environ.get(name)]
    if ambient_routing:
        raise CanaryError(
            "ambient libpq routing overrides are forbidden: " + ", ".join(ambient_routing)
        )
    try:
        parameters = parse_dsn(dsn)
    except psycopg2.Error as error:
        raise CanaryError(
            f"database connection configuration is invalid "
            f"(type={type(error).__name__}, code={error.pgcode or 'none'})"
        ) from None

    explicit_routing = sorted(FORBIDDEN_DSN_ROUTING_PARAMETERS.intersection(parameters))
    if explicit_routing:
        raise CanaryError(
            "database connection contains forbidden routing parameters: "
            + ", ".join(explicit_routing)
        )

    host = parameters.get("host", "")
    user = parameters.get("user", "")
    evidence = []
    host_match = DIRECT_HOST_RE.fullmatch(host)
    user_match = POOLER_USER_RE.fullmatch(user)
    if host_match:
        if host_match.group(1) != expected_project_ref:
            raise CanaryError("database connection identifies a different Supabase project")
        if user_match and user_match.group(1) != expected_project_ref:
            raise CanaryError("database host and username identify different Supabase projects")
        evidence.append("direct_host_project_ref")
    elif POOLER_HOST_RE.fullmatch(host):
        if user_match is None or user_match.group(1) != expected_project_ref:
            raise CanaryError("database pooler connection does not identify the expected project")
        evidence.append("pooler_user_project_ref")
    else:
        raise CanaryError("database connection does not identify the expected Supabase project")
    return evidence


def _inspect_target(
    dsn: str,
    expected_project_ref: str,
    source: str,
    season: int,
    *,
    connector=psycopg2.connect,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Validate target identity and source plan in a read-only transaction."""
    project_evidence = _dsn_project_evidence(dsn, expected_project_ref)
    expected_migrations = _expected_migrations()
    expected_by_id = {row[0]: row for row in expected_migrations}

    try:
        conn = connector(dsn, connect_timeout=10, application_name="source-receipt-canary")
        try:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '15s'")
                cur.execute("SET LOCAL lock_timeout = '5s'")
                cur.execute(
                    """
                    SELECT current_database()::text, current_user::text, session_user::text,
                           pg_catalog.pg_has_role(
                               session_user, 'warehouse_source_publisher', 'SET'
                           )
                    """
                )
                identity_row = cur.fetchone()
                if identity_row is None or len(identity_row) != 4:
                    raise CanaryError("database returned incomplete runtime identity")
                database, current_user, session_user, can_set_role = identity_row
                if type(can_set_role) is not bool:
                    raise CanaryError("database returned invalid role activation evidence")

                cur.execute(
                    """
                    SELECT migration_id, path, kind, checksum, manifest_order
                    FROM warehouse_control.schema_migrations
                    ORDER BY manifest_order
                    """
                )
                actual_migrations = tuple(cur.fetchall())
                if actual_migrations != expected_migrations:
                    actual_by_id = {row[0]: row for row in actual_migrations}
                    expected_ids = set(expected_by_id)
                    actual_ids = set(actual_by_id)
                    missing = sorted(expected_ids - actual_ids)
                    unexpected = sorted(actual_ids - expected_ids)
                    changed = sorted(
                        migration_id
                        for migration_id, expected in expected_by_id.items()
                        if migration_id in actual_by_id and actual_by_id[migration_id] != expected
                    )
                    differences = []
                    if missing:
                        differences.append("missing=" + ",".join(missing))
                    if changed:
                        differences.append("changed=" + ",".join(changed))
                    if unexpected:
                        differences.append("unexpected=" + ",".join(unexpected))
                    raise CanaryError(
                        "production migration ledger does not exactly match reviewed manifest: "
                        + "; ".join(differences)
                    )

                plan = None
                assumed_role = None
                if can_set_role:
                    cur.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(PUBLISHER_ROLE)))
                    cur.execute("SELECT current_user::text")
                    role_row = cur.fetchone()
                    assumed_role = role_row[0] if role_row else None
                    if assumed_role != PUBLISHER_ROLE:
                        raise CanaryError("database did not assume the bounded publisher role")
                    cur.execute(
                        "SELECT warehouse_source_batch.get_source_plan(%s,%s)",
                        (source, season),
                    )
                    plan_row = cur.fetchone()
                    if plan_row is None:
                        raise CanaryError("bounded publisher returned no source plan")
                    plan = publication._validated_plan(plan_row[0], SDV_FPI_PUBLICATION, season)
            conn.rollback()
        except BaseException:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()
    except CanaryError:
        raise
    except psycopg2.Error as error:
        raise CanaryError(
            f"database inspection failed "
            f"(type={type(error).__name__}, code={error.pgcode or 'none'})"
        ) from None

    identity = {
        "expected_project_ref": expected_project_ref,
        "project_ref_evidence": project_evidence,
        "database": database,
        "current_user": current_user,
        "session_user": session_user,
        "can_set_role": can_set_role,
        "assumed_role": assumed_role,
        "activation_required": not can_set_role,
        "migration_ledger": {
            "verified": True,
            "entries": [
                {"id": migration_id, "checksum": checksum}
                for migration_id, _, _, checksum, _ in expected_migrations
            ],
        },
    }
    return identity, plan


def _download_and_validate(source: str, season: int, expected_sha256: str) -> Artifact:
    spec = REGISTRY[source]
    url = resolve_fetch_url(spec, season)
    if url is None:
        raise CanaryError("registered canary source has no URL")
    fetched: FetchedFile = fetch_file(url, timeout=30, retries=1)
    computed_sha256 = hashlib.sha256(fetched.content).hexdigest()
    if fetched.sha256 != computed_sha256:
        raise CanaryError("fetcher SHA-256 does not match downloaded bytes")
    if computed_sha256 != expected_sha256:
        raise CanaryError(
            f"artifact SHA-256 mismatch: expected {expected_sha256}, got {computed_sha256}"
        )

    context = ParseContext(
        source=source,
        snapshot_date=date.today(),
        season=season,
        source_url=fetched.source_url,
        file_name=os.path.basename(fetched.source_url),
    )
    raw_count = publication._raw_row_count(fetched.content, SDV_FPI_PUBLICATION, season)
    rows = list(resolve_parser(spec.parser)(fetched.content, context))
    publication._validate_parsed_rows(rows, raw_count, SDV_FPI_PUBLICATION, season)
    weeks = tuple(sorted({row["week"] for row in rows}))
    return Artifact(fetched.content, computed_sha256, len(rows), weeks)


def _publish_pinned(
    dsn: str,
    source: str,
    season: int,
    artifact: Artifact,
) -> dict[str, Any]:
    """Publish once from a temporary copy of the already verified bytes."""
    original_get_db_url = publication.get_db_url
    try:
        publication.get_db_url = lambda: dsn
        with tempfile.TemporaryDirectory(prefix="sdv-fpi-receipt-canary-") as directory:
            path = Path(directory) / f"cfb_fpi_weekly_{season}.parquet"
            path.write_bytes(artifact.content)
            result = publication.run_source_publication(
                REGISTRY[source], file_path=str(path), season=season
            )
    finally:
        publication.get_db_url = original_get_db_url
    return result


def run_canary(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    dsn = publication.get_db_url()
    identity, plan = _inspect_target(
        dsn,
        args.expected_project_ref,
        args.source,
        args.season,
    )
    if args.action == "publish" and not identity["can_set_role"]:
        raise CanaryError("publisher role activation is required before publish")

    artifact = _download_and_validate(args.source, args.season, args.expected_sha256)
    payload: dict[str, Any] = {
        "action": args.action,
        "source": args.source,
        "season": args.season,
        "identity": identity,
        "source_plan": plan,
        "artifact": artifact.evidence(),
    }
    if args.action == "preflight":
        payload["status"] = "ready" if plan is not None else "activation_required"
        return payload, 0

    result = _publish_pinned(dsn, args.source, args.season, artifact)
    payload["publication"] = result
    if result.get("status") != "loaded":
        payload["status"] = "failed"
        return payload, 1
    if (
        result.get("sha") != artifact.sha256
        or result.get("rows") != artifact.rows
        or not result.get("run_id")
        or not result.get("generation_id")
    ):
        payload["status"] = "failed_result_validation"
        return payload, 1
    payload["status"] = "loaded"
    return payload, 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        payload, status = run_canary(args)
    except (CanaryError, publication.SourcePublicationError) as error:
        payload = {
            "action": args.action,
            "source": args.source,
            "season": args.season,
            "status": "failed",
            "error": str(error),
        }
        status = 1
    print(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
