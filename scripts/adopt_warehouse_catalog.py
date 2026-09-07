"""Prepare and apply a truthful production warehouse catalog-adoption receipt.

This is deliberately separate from bootstrap.  Adoption proves that a reviewed
schema-only capture matches the current catalog, then records one receipt root;
it never claims that the bootstrap baseline or historical migrations ran.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import export_warehouse_catalog as exporter  # noqa: E402
from scripts.bootstrap_warehouse import (  # noqa: E402
    _redact_error,
    validate_database_url,
)
from scripts.warehouse_migrations import (  # noqa: E402
    _CREATE_LEDGER_SQL,
    ADVISORY_LOCK_KEYS,
    _build_plan,
    _ensure_idle_connection,
    _execute,
    _ledger_installed,
    _read_applied,
    _record_initial,
    load_manifest,
)

DATABASE_ENV = "SUPABASE_DB_URL"
CANONICALIZATION = "warehouse-catalog-v1"
RECEIPT_TYPE = "warehouse-production-catalog-adoption"
RECEIPT_STATEMENT = (
    "Catalog equivalence observed; no historical migration was executed or recorded."
)
RECEIPT_FILENAME = "catalog-adoption-receipt.json"
MANIFEST_FILENAME = "production-manifest.json"
ROOT_FILENAME = "catalog-adoption-root.sql"
SCHEMA_FILENAME = "schema.sql"
METADATA_FILENAME = "catalog.json"
CANONICAL_SCHEMA_FILENAME = "canonical-schema.sql"
CANONICAL_METADATA_FILENAME = "canonical-metadata.json"
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_REVISION_RE = re.compile(r"[0-9a-f]{40,64}\Z")
_DUMP_VERSION_RE = re.compile(r"^-- Dumped (?:from database|by pg_dump) version .+$")
_DUMP_RESTRICT_RE = re.compile(r"^\\restrict [A-Za-z0-9]+$")
_DUMP_UNRESTRICT_RE = re.compile(r"^\\unrestrict [A-Za-z0-9]+$")
_ROLE_NAMES = ("analyst_ro", "anon", "authenticated", "service_role")
_METADATA_KEYS = (
    "schemas",
    "extensions",
    "object_counts",
    "owners",
    "relation_owners",
    "function_owners",
    "memberships",
    "schema_owners",
    "role_attributes",
    "membership_options",
)


class AdoptionError(RuntimeError):
    """The receipt, live catalog, or ledger is not safe to adopt."""


@dataclass(frozen=True)
class CatalogFingerprint:
    digest: str
    schema_digest: str
    metadata_digest: str


@dataclass(frozen=True)
class AdoptionBundle:
    manifest: object
    receipt: dict[str, object]
    fingerprint: CatalogFingerprint
    receipt_path: Path


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonicalize_schema_dump(raw: bytes) -> bytes:
    """Remove only pg_dump's bounded header/footer version and restrict noise."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise AdoptionError("schema-only dump is not UTF-8") from exc
    lines = text.splitlines(keepends=True)
    nonblank = [index for index, line in enumerate(lines) if line.strip()]
    last_nonblank = nonblank[-1] if nonblank else -1
    kept: list[str] = []
    for index, line in enumerate(lines):
        content = line.rstrip("\r\n")
        # pg_dump emits both version comments and \restrict in its small preamble.  Bounding
        # them to the preamble avoids deleting identical text from a function body.
        header_noise = index < 20 and (
            _DUMP_VERSION_RE.fullmatch(content) or _DUMP_RESTRICT_RE.fullmatch(content)
        )
        footer_noise = index == last_nonblank and _DUMP_UNRESTRICT_RE.fullmatch(content)
        if not header_noise and not footer_noise:
            kept.append(line)
    return "".join(kept).encode("utf-8")


def _sortable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def canonicalize_metadata(metadata: dict[str, object]) -> bytes:
    """Encode stable catalog ownership, extension, count, and membership facts."""
    missing = [key for key in _METADATA_KEYS if key not in metadata]
    if missing:
        raise AdoptionError("catalog metadata is missing: " + ", ".join(missing))
    normalized: dict[str, object] = {}
    for key in _METADATA_KEYS:
        value = metadata[key]
        if not isinstance(value, list):
            raise AdoptionError(f"catalog metadata field {key!r} must be a list")
        normalized[key] = sorted(value, key=_sortable_json)
    return (_sortable_json(normalized) + "\n").encode("ascii")


def fingerprint_catalog(schema: bytes, metadata: dict[str, object]) -> CatalogFingerprint:
    canonical_schema = canonicalize_schema_dump(schema)
    canonical_metadata = canonicalize_metadata(metadata)
    schema_digest = _sha256(canonical_schema)
    metadata_digest = _sha256(canonical_metadata)
    address = {
        "canonicalization": CANONICALIZATION,
        "metadata_sha256": metadata_digest,
        "schema_sha256": schema_digest,
    }
    return CatalogFingerprint(
        digest=_sha256(_sortable_json(address).encode("ascii")),
        schema_digest=schema_digest,
        metadata_digest=metadata_digest,
    )


def _supplement_metadata(conn: object, metadata: dict[str, object]) -> dict[str, object]:
    schemas = metadata.get("schemas")
    if not isinstance(schemas, list) or not all(isinstance(item, str) for item in schemas):
        raise AdoptionError("exported catalog schemas must be a list of names")
    with conn.cursor() as cur:
        _execute(
            cur,
            "SELECT nspname, pg_get_userbyid(nspowner) FROM pg_namespace "
            "WHERE nspname = ANY(%s) ORDER BY nspname",
            (schemas,),
        )
        schema_owners = cur.fetchall()
        owner_names = sorted(
            {
                row[3]
                for key in ("relation_owners", "function_owners")
                for row in metadata.get(key, [])
                if isinstance(row, (list, tuple)) and len(row) > 3 and isinstance(row[3], str)
            }
            | {row[1] for row in schema_owners}
        )
        role_seeds = sorted(set(_ROLE_NAMES) | set(owner_names))
        _execute(
            cur,
            "WITH RECURSIVE role_closure(roleid) AS ("
            " SELECT oid FROM pg_roles WHERE rolname = ANY(%s)"
            " UNION SELECT membership.roleid FROM pg_auth_members AS membership"
            " JOIN role_closure AS child ON child.roleid = membership.member"
            ") SELECT role.rolname, role.rolsuper, role.rolinherit, role.rolcreaterole, "
            "role.rolcreatedb, role.rolcanlogin, role.rolreplication, role.rolbypassrls "
            "FROM pg_roles AS role JOIN role_closure ON role_closure.roleid = role.oid "
            "ORDER BY role.rolname",
            (role_seeds,),
        )
        role_attributes = cur.fetchall()
        _execute(
            cur,
            "WITH RECURSIVE role_closure(roleid) AS ("
            " SELECT oid FROM pg_roles WHERE rolname = ANY(%s)"
            " UNION SELECT membership.roleid FROM pg_auth_members AS membership"
            " JOIN role_closure AS child ON child.roleid = membership.member"
            ") SELECT parent.rolname, member.rolname, membership.admin_option, "
            "membership.inherit_option, membership.set_option "
            "FROM pg_auth_members AS membership "
            "JOIN pg_roles AS parent ON parent.oid = membership.roleid "
            "JOIN pg_roles AS member ON member.oid = membership.member "
            "JOIN role_closure ON role_closure.roleid = membership.member ORDER BY 1, 2",
            (role_seeds,),
        )
        membership_options = cur.fetchall()
    supplemented = dict(metadata)
    supplemented["schema_owners"] = schema_owners
    supplemented["role_attributes"] = role_attributes
    supplemented["membership_options"] = membership_options
    return supplemented


def _capture_catalog(conn: object, database_url: str, output: Path) -> CatalogFingerprint:
    previous_output = exporter.OUTPUT
    try:
        exporter.OUTPUT = output
        # The exporter prints a human-readable copy of its metadata.  Keep this CLI's stdout JSON.
        with contextlib.redirect_stdout(io.StringIO()):
            exporter._export_snapshot(conn, database_url)
    finally:
        exporter.OUTPUT = previous_output
    schema_path = output / SCHEMA_FILENAME
    metadata_path = output / METADATA_FILENAME
    try:
        schema = schema_path.read_bytes()
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AdoptionError(f"catalog exporter did not produce a valid snapshot: {exc}") from exc
    if not isinstance(metadata, dict):
        raise AdoptionError("exported catalog metadata must be an object")
    metadata = _supplement_metadata(conn, metadata)
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    canonical_schema = canonicalize_schema_dump(schema)
    canonical_metadata = canonicalize_metadata(metadata)
    (output / CANONICAL_SCHEMA_FILENAME).write_bytes(canonical_schema)
    (output / CANONICAL_METADATA_FILENAME).write_bytes(canonical_metadata)
    return fingerprint_catalog(schema, metadata)


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _root_sql(receipt: dict[str, object], receipt_digest: str) -> bytes:
    entry = receipt["manifest"]["root"]  # type: ignore[index]
    fingerprint = receipt["fingerprint"]  # type: ignore[index]
    migration_id = entry["id"]
    catalog_digest = fingerprint["digest"]
    return (
        "-- Production catalog-adoption receipt.\n"
        f"-- Receipt JSON SHA256: {receipt_digest}\n"
        f"-- {RECEIPT_STATEMENT}\n"
        "DO $catalog_adoption$\n"
        "BEGIN\n"
        "    IF current_setting('warehouse_control.catalog_adoption_receipt', true)\n"
        f"            IS DISTINCT FROM '{migration_id}' THEN\n"
        "        RAISE EXCEPTION 'catalog-adoption root requires a verified "
        "adoption transaction';\n"
        "    END IF;\n"
        "END\n"
        "$catalog_adoption$;\n"
        "SELECT\n"
        f"    '{migration_id}'::text AS catalog_adoption_receipt_id,\n"
        f"    '{catalog_digest}'::text AS observed_catalog_sha256;\n"
    ).encode()


def prepare_bundle(
    conn: object,
    database_url: str,
    *,
    output: Path,
    source_revision: str,
    capture_provenance: str,
    root_path: str,
    captured_at: str | None = None,
) -> dict[str, object]:
    """Capture a read-only catalog and write a new, reviewable adoption bundle."""
    if not _REVISION_RE.fullmatch(source_revision):
        raise AdoptionError("source revision must be a lowercase 40-64 character hex digest")
    if not capture_provenance.strip():
        raise AdoptionError("capture provenance must be nonempty")
    relative_root = PurePosixPath(root_path)
    if (
        relative_root.is_absolute()
        or "\\" in root_path
        or any(part in {"", ".", ".."} for part in relative_root.parts)
        or relative_root.as_posix() != root_path
    ):
        raise AdoptionError("root path must be a canonical repository-relative POSIX path")
    if not root_path.endswith(".sql"):
        raise AdoptionError("root path must name a .sql file")
    if output.exists():
        raise AdoptionError(
            f"output already exists; refusing to overwrite immutable capture: {output}"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="warehouse-adoption-", dir=output.parent))
    try:
        fingerprint = _capture_catalog(conn, database_url, work)
        if captured_at is None:
            captured_at = datetime.now(UTC).isoformat()
        capture_date = captured_at[:10].replace("-", "")
        if not re.fullmatch(r"[0-9]{8}", capture_date):
            raise AdoptionError("captured-at must start with an ISO YYYY-MM-DD date")
        migration_id = (
            f"warehouse.production.catalog-adopted.{capture_date}.{fingerprint.digest[:12]}"
        )
        entry = {"id": migration_id, "path": root_path, "kind": "immutable"}
        receipt: dict[str, object] = {
            "version": 1,
            "receipt_type": RECEIPT_TYPE,
            "statement": RECEIPT_STATEMENT,
            "source_revision": source_revision,
            "capture_provenance": {
                "captured_at": captured_at,
                "reference": capture_provenance,
                "raw_catalog_sha256": _sha256((work / METADATA_FILENAME).read_bytes()),
                "raw_schema_sha256": _sha256((work / SCHEMA_FILENAME).read_bytes()),
            },
            "fingerprint": {
                "algorithm": "sha256",
                "canonicalization": CANONICALIZATION,
                "digest": fingerprint.digest,
                "canonical_schema_sha256": fingerprint.schema_digest,
                "canonical_metadata_sha256": fingerprint.metadata_digest,
                "scopes": list(_METADATA_KEYS),
            },
            "manifest": {"version": 1, "root": entry},
        }
        receipt_bytes = _json_bytes(receipt)
        root_bytes = _root_sql(receipt, _sha256(receipt_bytes))
        manifest = {"version": 1, "migrations": [entry]}
        (work / RECEIPT_FILENAME).write_bytes(receipt_bytes)
        (work / ROOT_FILENAME).write_bytes(root_bytes)
        (work / MANIFEST_FILENAME).write_bytes(_json_bytes(manifest))
        work.rename(output)
    except Exception:
        shutil.rmtree(work, ignore_errors=True)
        raise
    return {
        "action": "prepare",
        "output": str(output),
        "migration_id": migration_id,
        "fingerprint": fingerprint.digest,
        "read_only": True,
        "files": sorted(path.name for path in output.iterdir()),
    }


def _load_json_object(path: Path) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise AdoptionError(f"duplicate JSON key in adoption receipt: {key}")
            result[key] = value
        return result

    try:
        value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicates)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdoptionError(f"cannot read adoption receipt {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AdoptionError("adoption receipt must be a JSON object")
    return value


def load_bundle(
    manifest_path: Path, receipt_path: Path, *, root: Path = REPO_ROOT
) -> AdoptionBundle:
    """Strictly bind the receipt, executable root SQL, and production manifest."""
    receipt = _load_json_object(receipt_path)
    required = {
        "version",
        "receipt_type",
        "statement",
        "source_revision",
        "capture_provenance",
        "fingerprint",
        "manifest",
    }
    if set(receipt) != required:
        raise AdoptionError("adoption receipt has unexpected or missing top-level fields")
    if receipt["version"] != 1 or receipt["receipt_type"] != RECEIPT_TYPE:
        raise AdoptionError("unsupported adoption receipt identity or version")
    if receipt["statement"] != RECEIPT_STATEMENT:
        raise AdoptionError("adoption receipt truth statement changed")
    if not isinstance(receipt["source_revision"], str) or not _REVISION_RE.fullmatch(
        receipt["source_revision"]
    ):
        raise AdoptionError("adoption receipt source revision is malformed")
    provenance = receipt["capture_provenance"]
    if not isinstance(provenance, dict) or set(provenance) != {
        "captured_at",
        "reference",
        "raw_catalog_sha256",
        "raw_schema_sha256",
    }:
        raise AdoptionError("adoption receipt capture provenance is malformed")
    if not all(
        isinstance(provenance[key], str) and provenance[key] for key in ("captured_at", "reference")
    ) or not all(
        isinstance(provenance[key], str) and _SHA256_RE.fullmatch(provenance[key])
        for key in ("raw_catalog_sha256", "raw_schema_sha256")
    ):
        raise AdoptionError("adoption receipt capture provenance contains invalid values")
    fingerprint_value = receipt["fingerprint"]
    if not isinstance(fingerprint_value, dict) or set(fingerprint_value) != {
        "algorithm",
        "canonicalization",
        "digest",
        "canonical_schema_sha256",
        "canonical_metadata_sha256",
        "scopes",
    }:
        raise AdoptionError("adoption receipt fingerprint is malformed")
    if (
        fingerprint_value["algorithm"] != "sha256"
        or fingerprint_value["canonicalization"] != CANONICALIZATION
        or fingerprint_value["scopes"] != list(_METADATA_KEYS)
    ):
        raise AdoptionError("adoption receipt fingerprint contract changed")
    digests = (
        fingerprint_value["digest"],
        fingerprint_value["canonical_schema_sha256"],
        fingerprint_value["canonical_metadata_sha256"],
    )
    if not all(isinstance(value, str) and _SHA256_RE.fullmatch(value) for value in digests):
        raise AdoptionError("adoption receipt fingerprint contains an invalid SHA256 digest")
    manifest_claim = receipt["manifest"]
    if not isinstance(manifest_claim, dict) or set(manifest_claim) != {"version", "root"}:
        raise AdoptionError("adoption receipt manifest claim is malformed")
    manifest = load_manifest(manifest_path, root)
    if not manifest.migrations:
        raise AdoptionError("production manifest must contain the adoption root")
    root_claim = manifest_claim["root"]
    first = manifest.migrations[0]
    actual_entry = {"id": first.id, "path": first.path, "kind": first.kind}
    if (
        manifest_claim["version"] != 1
        or not isinstance(root_claim, dict)
        or set(root_claim) != {"id", "path", "kind"}
        or root_claim != actual_entry
    ):
        raise AdoptionError("production manifest root does not match the adoption receipt")
    if not first.id.startswith("warehouse.production.catalog-adopted."):
        raise AdoptionError("production manifest does not start with a catalog-adoption root")
    if first.kind != "immutable":
        raise AdoptionError("catalog-adoption root must be immutable")
    forbidden_history = {
        "warehouse.platform.20260907",
        "warehouse.baseline.pre-f05.20260907",
        "warehouse.static-seeds.20260907",
        "warehouse.baseline.ready.20260907",
        "warehouse.f05.064",
        "warehouse.f06.065",
    }
    found_history = [
        migration.id for migration in manifest.migrations if migration.id in forbidden_history
    ]
    if found_history:
        raise AdoptionError(
            "production manifest must not claim bootstrap or historical migration execution: "
            + ", ".join(found_history)
        )
    expected_sql = _root_sql(receipt, _sha256(receipt_path.read_bytes())).decode("utf-8")
    if first.sql != expected_sql:
        raise AdoptionError("adoption root SQL does not exactly bind the reviewed receipt")
    return AdoptionBundle(
        manifest=manifest,
        receipt=receipt,
        fingerprint=CatalogFingerprint(*digests),
        receipt_path=receipt_path,
    )


def _assert_fingerprint(expected: CatalogFingerprint, actual: CatalogFingerprint) -> None:
    if actual != expected:
        raise AdoptionError(
            "live catalog fingerprint does not match the reviewed receipt "
            f"(expected {expected.digest}, found {actual.digest})"
        )


def _begin_locked_snapshot(conn: object, cur: object, *, read_only: bool) -> None:
    """Acquire the session lock before establishing the repeatable-read snapshot.

    A transaction-level advisory-lock SELECT itself fixes a repeatable-read snapshot.  If it
    waits, that snapshot can predate a competing holder's commit.  A brief autocommit session
    lock closes that race; after BEGIN, the transaction lock takes over before it is released.
    """
    conn.autocommit = True
    _execute(cur, "SELECT pg_advisory_lock(%s, %s)", ADVISORY_LOCK_KEYS)
    conn.autocommit = False
    began = False
    try:
        transaction_sql = "BEGIN ISOLATION LEVEL REPEATABLE READ"
        if read_only:
            transaction_sql += " READ ONLY"
        _execute(cur, transaction_sql)
        began = True
        _execute(cur, "SELECT pg_advisory_xact_lock(%s, %s)", ADVISORY_LOCK_KEYS)
        _execute(cur, "SELECT pg_advisory_unlock(%s, %s)", ADVISORY_LOCK_KEYS)
        row = cur.fetchone()
        if row is not None and row[0] is not True:
            raise AdoptionError("failed to transfer catalog adoption advisory lock")
    except Exception:
        if began:
            conn.rollback()
        # If transfer did not finish, release the session-level acquisition explicitly.
        try:
            conn.autocommit = True
            _execute(cur, "SELECT pg_advisory_unlock(%s, %s)", ADVISORY_LOCK_KEYS)
        finally:
            conn.autocommit = False
        raise


def _validate_existing_ledger(cur: object, bundle: AdoptionBundle) -> tuple[object, ...]:
    applied = _read_applied(cur)
    plan = _build_plan(
        bundle.manifest,  # type: ignore[arg-type]
        mode="upgrade",
        target=None,
        ledger_installed=True,
        applied=applied,
    )
    if not plan.valid:
        raise AdoptionError(
            "existing ledger does not match production manifest: " + "; ".join(plan.diagnostics)
        )
    if plan.pending:
        raise AdoptionError("production manifest has pending migrations; run the upgrade command")
    return applied


def adopt_catalog(conn: object, database_url: str, bundle: AdoptionBundle) -> dict[str, object]:
    """Validate managed history, or compare and transactionally install one receipt root."""
    _ensure_idle_connection(conn)
    cur = conn.cursor()
    capture_dir = Path(tempfile.mkdtemp(prefix="warehouse-adoption-check-"))
    try:
        _begin_locked_snapshot(conn, cur, read_only=False)
        _execute(cur, "SET LOCAL standard_conforming_strings = on")
        installed = _ledger_installed(cur)
        if installed:
            applied = _validate_existing_ledger(cur, bundle)
            if len(applied) > 1:
                conn.rollback()
                return {
                    "action": "adopt",
                    "valid": True,
                    "adopted": False,
                    "noop": True,
                    "catalog_verification": "not_checked_after_managed_upgrades",
                    "catalog_matches": None,
                    "expected_fingerprint": bundle.fingerprint.digest,
                    "actual_fingerprint": None,
                    "ledger_entries": len(applied),
                }
            actual = _capture_catalog(conn, database_url, capture_dir)
            _assert_fingerprint(bundle.fingerprint, actual)
            conn.rollback()
            return {
                "action": "adopt",
                "valid": True,
                "adopted": False,
                "noop": True,
                "catalog_verification": "matches_adoption_receipt",
                "catalog_matches": True,
                "expected_fingerprint": bundle.fingerprint.digest,
                "actual_fingerprint": actual.digest,
                "fingerprint": actual.digest,
                "ledger_entries": len(applied),
            }
        if len(bundle.manifest.migrations) != 1:  # type: ignore[attr-defined]
            raise AdoptionError(
                "unledgered adoption requires a root-only production manifest; "
                "apply later migrations with upgrade"
            )
        actual = _capture_catalog(conn, database_url, capture_dir)
        _assert_fingerprint(bundle.fingerprint, actual)
        first = bundle.manifest.migrations[0]  # type: ignore[attr-defined]
        _execute(
            cur,
            "SELECT set_config('warehouse_control.catalog_adoption_receipt', %s, true)",
            (first.id,),
        )
        _execute(cur, _CREATE_LEDGER_SQL)
        _execute(cur, first.sql)
        observed = cur.fetchone()
        if observed != (first.id, actual.digest):
            raise AdoptionError("adoption root SQL returned an unexpected receipt identity")
        _record_initial(cur, first, 1)
        conn.commit()
        return {
            "action": "adopt",
            "valid": True,
            "adopted": True,
            "noop": False,
            "catalog_verification": "matches_adoption_receipt",
            "catalog_matches": True,
            "expected_fingerprint": bundle.fingerprint.digest,
            "actual_fingerprint": actual.digest,
            "fingerprint": actual.digest,
            "ledger_entries": 1,
            "migration_id": first.id,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        shutil.rmtree(capture_dir, ignore_errors=True)
        cur.close()


def catalog_status(conn: object, database_url: str, bundle: AdoptionBundle) -> dict[str, object]:
    """Report adoption state; compare the receipt only before managed catalog evolution."""
    _ensure_idle_connection(conn)
    cur = conn.cursor()
    capture_dir = Path(tempfile.mkdtemp(prefix="warehouse-adoption-status-"))
    try:
        _begin_locked_snapshot(conn, cur, read_only=True)
        _execute(cur, "SET LOCAL standard_conforming_strings = on")
        installed = _ledger_installed(cur)
        applied: tuple[object, ...] = ()
        ledger_valid = False
        diagnostic: str | None = None
        if installed:
            try:
                applied = _validate_existing_ledger(cur, bundle)
                ledger_valid = True
            except AdoptionError as exc:
                diagnostic = str(exc)
                applied = _read_applied(cur)
            if not ledger_valid or len(applied) > 1:
                conn.rollback()
                return {
                    "action": "status",
                    "valid": ledger_valid,
                    "catalog_verification": "not_checked_after_managed_upgrades"
                    if ledger_valid
                    else "not_checked_invalid_ledger",
                    "catalog_matches": None,
                    "expected_fingerprint": bundle.fingerprint.digest,
                    "actual_fingerprint": None,
                    "ledger_installed": True,
                    "ledger_valid": ledger_valid,
                    "ledger_entries": len(applied),
                    "adoption_ready": False,
                    "diagnostic": diagnostic,
                    "read_only": True,
                }
        actual = _capture_catalog(conn, database_url, capture_dir)
        matches = actual == bundle.fingerprint
        conn.rollback()
        return {
            "action": "status",
            "valid": matches and (not installed or ledger_valid),
            "catalog_verification": "matches_adoption_receipt"
            if matches
            else "differs_from_adoption_receipt",
            "catalog_matches": matches,
            "expected_fingerprint": bundle.fingerprint.digest,
            "actual_fingerprint": actual.digest,
            "ledger_installed": installed,
            "ledger_valid": ledger_valid,
            "ledger_entries": len(applied),
            "adoption_ready": matches and not installed,
            "diagnostic": diagnostic,
            "read_only": True,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        shutil.rmtree(capture_dir, ignore_errors=True)
        cur.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare, adopt, or inspect production catalog adoption"
    )
    subparsers = parser.add_subparsers(dest="action", required=True)
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--output", type=Path, required=True)
    prepare.add_argument("--source-revision", required=True)
    prepare.add_argument("--capture-provenance", required=True)
    prepare.add_argument("--captured-at")
    prepare.add_argument("--root-path", required=True)
    for action in ("adopt", "status"):
        selected = subparsers.add_parser(action)
        selected.add_argument("--manifest", type=Path, required=True)
        selected.add_argument("--receipt", type=Path, required=True)
    return parser


def _connect(database_url: str):
    import psycopg2

    validate_database_url(database_url)
    return psycopg2.connect(database_url)


def _print_json(value: object, *, file=None) -> None:
    print(json.dumps(value, indent=2, sort_keys=True), file=file or sys.stdout)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    database_url = os.environ.get(DATABASE_ENV)
    if not database_url:
        _print_json({"valid": False, "error": f"{DATABASE_ENV} is required"}, file=sys.stderr)
        return 1
    conn = None
    try:
        validate_database_url(database_url)
        bundle = None
        if args.action in {"adopt", "status"}:
            manifest_path = (
                args.manifest if args.manifest.is_absolute() else REPO_ROOT / args.manifest
            )
            receipt_path = args.receipt if args.receipt.is_absolute() else REPO_ROOT / args.receipt
            bundle = load_bundle(manifest_path, receipt_path)
        conn = _connect(database_url)
        if args.action == "prepare":
            conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
            result = prepare_bundle(
                conn,
                database_url,
                output=args.output.resolve(),
                source_revision=args.source_revision,
                capture_provenance=args.capture_provenance,
                root_path=args.root_path,
                captured_at=args.captured_at,
            )
            conn.rollback()
        elif args.action == "adopt":
            result = adopt_catalog(conn, database_url, bundle)
        else:
            result = catalog_status(conn, database_url, bundle)
        _print_json(result)
        return 0 if result.get("valid", True) else 1
    except Exception as exc:
        _print_json(
            {"valid": False, "error": _redact_error(exc, database_url)},
            file=sys.stderr,
        )
        return 1
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
