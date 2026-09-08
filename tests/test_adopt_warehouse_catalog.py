"""Unit coverage for truthful, fingerprint-gated production catalog adoption."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

import scripts.adopt_warehouse_catalog as adoption
from scripts.warehouse_migrations import apply_manifest


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.results = []
        self.closed = False

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        self.conn.executions.append((normalized, params))
        if self.conn.fail_on and self.conn.fail_on in sql:
            raise RuntimeError("injected adoption failure")
        if "to_regclass('warehouse_control.schema_migrations')" in sql:
            self.results = [(self.conn.ledger_installed,)]
        elif normalized.startswith("SELECT migration_id, path, kind, checksum"):
            self.results = list(self.conn.applied)
        elif "set_config('warehouse_control.catalog_adoption_receipt'" in sql:
            self.conn.adoption_context = True
            self.results = [(params[0],)]
        elif "AS catalog_adoption_receipt_id" in sql:
            if not self.conn.adoption_context:
                raise RuntimeError("catalog-adoption root requires a verified adoption transaction")
            migration_id = self.conn.expected_id
            self.results = [(migration_id, self.conn.actual.digest)]
        elif "pg_advisory_unlock" in sql:
            self.results = [(True,)]
        elif "FROM pg_namespace WHERE nspname = ANY" in normalized:
            self.results = []
        elif "FROM pg_namespace" in normalized:
            self.results = []
        else:
            self.results = []

    def fetchone(self):
        return self.results[0] if self.results else None

    def fetchall(self):
        return self.results

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class FakeConnection:
    autocommit = False

    def __init__(
        self,
        actual,
        *,
        ledger_installed=False,
        applied=(),
        fail_on=None,
    ):
        self.actual = actual
        self.ledger_installed = ledger_installed
        self.applied = applied
        self.fail_on = fail_on
        self.expected_id = None
        self.adoption_context = False
        self.executions = []
        self.commits = 0
        self.rollbacks = 0

    def get_transaction_status(self):
        return 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _fingerprint(char="a"):
    return adoption.CatalogFingerprint(char * 64, "b" * 64, "c" * 64)


def _make_bundle(tmp_path: Path, fingerprint=None, *, extra=()):
    fingerprint = fingerprint or _fingerprint()
    root_path = "adoption.sql"
    migration_id = "warehouse.production.catalog-adopted.20260907." + fingerprint.digest[:12]
    entry = {"id": migration_id, "path": root_path, "kind": "immutable"}
    receipt = {
        "version": 1,
        "receipt_type": adoption.RECEIPT_TYPE,
        "statement": adoption.RECEIPT_STATEMENT,
        "source_revision": "d" * 40,
        "capture_provenance": {
            "captured_at": "2026-09-07T12:00:00+00:00",
            "reference": "https://github.example/actions/runs/1",
            "raw_catalog_sha256": "e" * 64,
            "raw_schema_sha256": "f" * 64,
        },
        "fingerprint": {
            "algorithm": "sha256",
            "canonicalization": adoption.CANONICALIZATION,
            "digest": fingerprint.digest,
            "canonical_schema_sha256": fingerprint.schema_digest,
            "canonical_metadata_sha256": fingerprint.metadata_digest,
            "scopes": list(adoption._METADATA_KEYS),
        },
        "manifest": {"version": 1, "root": entry},
    }
    receipt_path = tmp_path / "receipt.json"
    receipt_path.write_bytes(adoption._json_bytes(receipt))
    (tmp_path / root_path).write_bytes(
        adoption._root_sql(receipt, hashlib.sha256(receipt_path.read_bytes()).hexdigest())
    )
    migrations = [entry, *extra]
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"version": 1, "migrations": migrations}) + "\n")
    bundle = adoption.load_bundle(manifest_path, receipt_path, root=tmp_path)
    return bundle, manifest_path, receipt_path


def _queries(conn):
    return [sql for sql, _ in conn.executions]


def test_schema_canonicalization_removes_only_version_headers_and_random_tokens():
    first = (
        b"-- PostgreSQL database dump\n-- Dumped from database version 17.6\n"
        b"\\restrict AAA\nCREATE TABLE x();\n\\unrestrict AAA\n"
    )
    second = (
        b"-- PostgreSQL database dump\n-- Dumped from database version 17.7\n"
        b"\\restrict BBB\nCREATE TABLE x();\n\\unrestrict BBB\n"
    )

    assert adoption.canonicalize_schema_dump(first) == adoption.canonicalize_schema_dump(second)
    assert b"PostgreSQL database dump" in adoption.canonicalize_schema_dump(first)
    assert b"CREATE TABLE x" in adoption.canonicalize_schema_dump(first)

    body = b"\n".join([b"-- stable"] * 20 + [b"-- Dumped by pg_dump version body", b""])
    assert b"-- Dumped by pg_dump version body" in adoption.canonicalize_schema_dump(body)


def test_metadata_fingerprint_sorts_facts_but_covers_owners_extensions_and_memberships():
    metadata = {key: [] for key in adoption._METADATA_KEYS}
    metadata["extensions"] = [["vector", "0.8", "extensions"], ["pg_trgm", "1.6", "public"]]
    metadata["relation_owners"] = [["core", "games", "r", "postgres"]]
    metadata["membership_options"] = [["reader", "anon", False, True, True]]
    reversed_metadata = {key: list(reversed(value)) for key, value in metadata.items()}

    assert adoption.canonicalize_metadata(metadata) == adoption.canonicalize_metadata(
        reversed_metadata
    )
    changed = dict(metadata)
    changed["relation_owners"] = [["core", "games", "r", "other"]]
    assert adoption.fingerprint_catalog(b"SELECT 1;\n", metadata) != adoption.fingerprint_catalog(
        b"SELECT 1;\n", changed
    )


def test_metadata_role_closure_seeds_function_owners_and_consumer_roles():
    class Cursor:
        def __init__(self):
            self.calls = []
            self.result = []

        def execute(self, sql, params=None):
            self.calls.append((sql, params))
            if "SELECT nspname, pg_get_userbyid" in sql:
                self.result = [("api", "schema_owner")]
            else:
                self.result = []

        def fetchall(self):
            return self.result

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class Connection:
        cursor_instance = Cursor()

        def cursor(self):
            return self.cursor_instance

    conn = Connection()
    metadata = {
        "schemas": ["api"],
        "relation_owners": [],
        "function_owners": [["api", "secure_fn", "", "function_owner", True]],
    }

    adoption._supplement_metadata(conn, metadata)

    role_seeds = conn.cursor_instance.calls[1][1][0]
    assert {"anon", "authenticated", "function_owner", "schema_owner"} <= set(role_seeds)
    assert "True" not in role_seeds


def test_prepare_writes_reviewable_immutable_bundle(monkeypatch, tmp_path):
    actual = _fingerprint()
    output = tmp_path / "capture"

    def capture(conn, url, work):
        (work / adoption.SCHEMA_FILENAME).write_text("SELECT 1;\n")
        (work / adoption.METADATA_FILENAME).write_text("{}\n")
        return actual

    monkeypatch.setattr(adoption, "_capture_catalog", capture)
    result = adoption.prepare_bundle(
        object(),
        "postgresql://unused",
        output=output,
        source_revision="d" * 40,
        capture_provenance="actions/run/123",
        captured_at="2026-09-07T12:00:00+00:00",
        root_path="src/schemas/adoptions/20260907_production_catalog_receipt.sql",
    )

    assert result["read_only"] is True
    receipt = json.loads((output / adoption.RECEIPT_FILENAME).read_text())
    manifest = json.loads((output / adoption.MANIFEST_FILENAME).read_text())
    assert receipt["statement"] == adoption.RECEIPT_STATEMENT
    assert receipt["fingerprint"]["digest"] == actual.digest
    assert manifest["migrations"] == [receipt["manifest"]["root"]]
    root = (output / adoption.ROOT_FILENAME).read_text()
    assert "catalog_adoption_receipt_id" in root
    assert "requires a verified adoption transaction" in root
    with pytest.raises(adoption.AdoptionError, match="refusing to overwrite"):
        adoption.prepare_bundle(
            object(),
            "postgresql://unused",
            output=output,
            source_revision="d" * 40,
            capture_provenance="actions/run/123",
            root_path="src/schemas/adoptions/receipt.sql",
        )


def test_load_bundle_rejects_receipt_or_root_tampering(tmp_path):
    _, manifest_path, receipt_path = _make_bundle(tmp_path)
    receipt = json.loads(receipt_path.read_text())
    receipt["source_revision"] = "not-a-revision"
    receipt_path.write_text(json.dumps(receipt))

    with pytest.raises(adoption.AdoptionError, match="source revision"):
        adoption.load_bundle(manifest_path, receipt_path, root=tmp_path)


def test_production_manifest_rejects_synthetic_historical_ids(tmp_path):
    historical = tmp_path / "065.sql"
    historical.write_text("SELECT 65;\n")
    with pytest.raises(adoption.AdoptionError, match="historical migration"):
        _make_bundle(
            tmp_path,
            extra=(
                {
                    "id": "warehouse.f06.065",
                    "path": "065.sql",
                    "kind": "immutable",
                },
            ),
        )


def test_drift_fails_before_any_ledger_mutation(monkeypatch, tmp_path):
    bundle, _, _ = _make_bundle(tmp_path)
    conn = FakeConnection(_fingerprint("9"))
    monkeypatch.setattr(adoption, "_capture_catalog", lambda conn, url, output: conn.actual)

    with pytest.raises(adoption.AdoptionError, match="does not match"):
        adoption.adopt_catalog(conn, "postgresql://unused", bundle)

    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert not any("CREATE SCHEMA warehouse_control" in sql for sql in _queries(conn))


def test_success_records_only_executed_receipt_root(monkeypatch, tmp_path):
    bundle, _, _ = _make_bundle(tmp_path)
    conn = FakeConnection(bundle.fingerprint)
    conn.expected_id = bundle.manifest.migrations[0].id
    monkeypatch.setattr(adoption, "_capture_catalog", lambda conn, url, output: conn.actual)

    result = adoption.adopt_catalog(conn, "postgresql://unused", bundle)

    assert result["adopted"] is True
    assert conn.commits == 1
    assert conn.rollbacks == 0
    queries = _queries(conn)
    create_index = next(
        i for i, sql in enumerate(queries) if "CREATE SCHEMA warehouse_control" in sql
    )
    root_index = next(i for i, sql in enumerate(queries) if "catalog_adoption_receipt_id" in sql)
    record_index = next(
        i
        for i, sql in enumerate(queries)
        if sql.startswith("INSERT INTO warehouse_control.schema_migrations")
    )
    assert create_index < root_index < record_index
    recorded_params = conn.executions[record_index][1]
    assert recorded_params[0] == bundle.manifest.migrations[0].id
    assert all(
        fragment not in repr(conn.executions)
        for fragment in ("warehouse.f05.064", "warehouse.f06.065", "warehouse.baseline")
    )


def test_failure_after_ledger_creation_rolls_back_everything(monkeypatch, tmp_path):
    bundle, _, _ = _make_bundle(tmp_path)
    conn = FakeConnection(bundle.fingerprint, fail_on="catalog_adoption_receipt_id")
    conn.expected_id = bundle.manifest.migrations[0].id
    monkeypatch.setattr(adoption, "_capture_catalog", lambda conn, url, output: conn.actual)

    with pytest.raises(RuntimeError, match="injected"):
        adoption.adopt_catalog(conn, "postgresql://unused", bundle)

    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert any("CREATE SCHEMA warehouse_control" in sql for sql in _queries(conn))


def test_repeat_adoption_exact_history_is_validated_noop(monkeypatch, tmp_path):
    bundle, _, _ = _make_bundle(tmp_path)
    root = bundle.manifest.migrations[0]
    applied = ((root.id, root.path, root.kind, root.checksum, 1),)
    conn = FakeConnection(bundle.fingerprint, ledger_installed=True, applied=applied)
    monkeypatch.setattr(adoption, "_capture_catalog", lambda conn, url, output: conn.actual)

    result = adoption.adopt_catalog(conn, "postgresql://unused", bundle)

    assert result["noop"] is True
    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert not any("CREATE SCHEMA warehouse_control" in sql for sql in _queries(conn))


def test_repeat_adoption_rejects_other_manifest_history(monkeypatch, tmp_path):
    bundle, _, _ = _make_bundle(tmp_path)
    conn = FakeConnection(
        bundle.fingerprint,
        ledger_installed=True,
        applied=(("warehouse.platform.20260907", "old.sql", "immutable", "1" * 64, 1),),
    )
    monkeypatch.setattr(adoption, "_capture_catalog", lambda conn, url, output: conn.actual)

    with pytest.raises(adoption.AdoptionError, match="does not match production manifest"):
        adoption.adopt_catalog(conn, "postgresql://unused", bundle)

    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_evolved_valid_ledger_is_noop_without_comparing_original_fingerprint(monkeypatch, tmp_path):
    forward = tmp_path / "forward.sql"
    forward.write_text("ALTER TABLE core.games ADD COLUMN managed integer;\n")
    extra = {
        "id": "warehouse.forward.066",
        "path": "forward.sql",
        "kind": "immutable",
    }
    bundle, _, _ = _make_bundle(tmp_path, extra=(extra,))
    root, migration = bundle.manifest.migrations
    applied = (
        (root.id, root.path, root.kind, root.checksum, 1),
        (migration.id, migration.path, migration.kind, migration.checksum, 2),
    )
    conn = FakeConnection(_fingerprint("9"), ledger_installed=True, applied=applied)
    monkeypatch.setattr(
        adoption,
        "_capture_catalog",
        lambda *args: pytest.fail("managed evolution must not be compared to the old receipt"),
    )

    result = adoption.adopt_catalog(conn, "postgresql://unused", bundle)

    assert result["valid"] is True
    assert result["noop"] is True
    assert result["catalog_verification"] == "not_checked_after_managed_upgrades"
    assert result["catalog_matches"] is None
    assert result["actual_fingerprint"] is None
    assert result["ledger_entries"] == 2


def test_status_accepts_evolved_valid_ledger_but_rejects_tampered_later_checksum(
    monkeypatch, tmp_path
):
    forward = tmp_path / "forward.sql"
    forward.write_text("SELECT 66;\n")
    extra = {
        "id": "warehouse.forward.066",
        "path": "forward.sql",
        "kind": "immutable",
    }
    bundle, _, _ = _make_bundle(tmp_path, extra=(extra,))
    root, migration = bundle.manifest.migrations
    valid_applied = (
        (root.id, root.path, root.kind, root.checksum, 1),
        (migration.id, migration.path, migration.kind, migration.checksum, 2),
    )
    monkeypatch.setattr(
        adoption,
        "_capture_catalog",
        lambda *args: pytest.fail("evolved ledger status must validate history only"),
    )

    valid = adoption.catalog_status(
        FakeConnection(_fingerprint("9"), ledger_installed=True, applied=valid_applied),
        "postgresql://unused",
        bundle,
    )
    assert valid["valid"] is True
    assert valid["catalog_verification"] == "not_checked_after_managed_upgrades"
    assert valid["catalog_matches"] is None

    tampered = (
        valid_applied[0],
        (migration.id, migration.path, migration.kind, "0" * 64, 2),
    )
    invalid = adoption.catalog_status(
        FakeConnection(_fingerprint("9"), ledger_installed=True, applied=tampered),
        "postgresql://unused",
        bundle,
    )
    assert invalid["valid"] is False
    assert invalid["ledger_entries"] == 2
    assert "checksum changed" in invalid["diagnostic"]
    with pytest.raises(adoption.AdoptionError, match="checksum changed"):
        adoption.adopt_catalog(
            FakeConnection(_fingerprint("9"), ledger_installed=True, applied=tampered),
            "postgresql://unused",
            bundle,
        )


def test_generic_bootstrap_cannot_fabricate_catalog_adoption(tmp_path):
    bundle, _, _ = _make_bundle(tmp_path)
    conn = FakeConnection(bundle.fingerprint)
    conn.expected_id = bundle.manifest.migrations[0].id

    with pytest.raises(RuntimeError, match="verified adoption transaction"):
        apply_manifest(conn, bundle.manifest, mode="bootstrap")

    assert conn.commits == 0
    assert conn.rollbacks == 1
