"""Executed pg_dump and metadata fingerprint checks on disposable PostgreSQL 17."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import threading
import time

import psycopg2
import pytest

import scripts.adopt_warehouse_catalog as adoption
from scripts.warehouse_migrations import apply_manifest
from tests.test_warehouse_migrations_sql import query
from tests.test_warehouse_migrations_sql import warehouse_db as _warehouse_db_fixture  # noqa: F401


@pytest.fixture(name="warehouse_db")
def _adoption_warehouse_db(request):
    return request.getfixturevalue("_warehouse_db_fixture")


def test_real_pg_dump_and_metadata_capture_is_stable_and_detects_ddl(
    warehouse_db, monkeypatch, tmp_path
):
    container = os.environ.get("F06_TEST_DUMP_CONTAINER")
    if not container:
        if os.environ.get("F06_REQUIRE_DB") == "1":
            pytest.fail("F06_TEST_DUMP_CONTAINER is required for mandatory adoption integration")
        pytest.skip("set F06_TEST_DUMP_CONTAINER to the disposable PostgreSQL 17 container")
    conn, target = warehouse_db
    query(conn, "CREATE SCHEMA core; CREATE TABLE core.capture_probe(id integer PRIMARY KEY)")
    real_run = subprocess.run

    def docker_exec(args, *, env, stdout, check):
        image_index = args.index("postgres:17")
        dump_args = args[image_index + 1 :]
        selected_env = dict(env)
        selected_env["PGHOST"] = "127.0.0.1"
        selected_env["PGPORT"] = "5432"
        exec_args = ["docker", "exec", "-i"]
        for name in ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD", "PGOPTIONS"):
            if name in selected_env:
                exec_args.extend(["-e", f"{name}={selected_env[name]}"])
        exec_args.extend([container, *dump_args])
        return real_run(exec_args, stdout=stdout, check=check)

    monkeypatch.setattr(adoption.exporter.subprocess, "run", docker_exec)
    conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
    bundle_output = tmp_path / "bundle-output"
    prepared = adoption.prepare_bundle(
        conn,
        target,
        output=bundle_output,
        source_revision="d" * 40,
        capture_provenance="disposable-postgres-17",
        captured_at="2026-09-07T12:00:00+00:00",
        root_path="adoption.sql",
    )
    conn.rollback()
    conn.set_session(readonly=False, isolation_level="READ COMMITTED")
    shutil.copy(bundle_output / adoption.ROOT_FILENAME, bundle_output / "adoption.sql")
    bundle = adoption.load_bundle(
        bundle_output / adoption.MANIFEST_FILENAME,
        bundle_output / adoption.RECEIPT_FILENAME,
        root=bundle_output,
    )
    assert prepared["fingerprint"] == bundle.fingerprint.digest

    query(conn, "ALTER TABLE core.capture_probe ADD COLUMN observed text")
    with pytest.raises(adoption.AdoptionError, match="does not match"):
        adoption.adopt_catalog(conn, target, bundle)
    assert query(conn, "SELECT to_regnamespace('warehouse_control')") == [(None,)]

    query(conn, "ALTER TABLE core.capture_probe DROP COLUMN observed")
    applied = adoption.adopt_catalog(conn, target, bundle)
    assert applied["adopted"] is True
    assert adoption.adopt_catalog(conn, target, bundle)["noop"] is True
    status = adoption.catalog_status(conn, target, bundle)
    assert status["valid"] is True
    assert status["ledger_entries"] == 1
    rows = query(
        conn,
        "SELECT migration_id, path, kind FROM warehouse_control.schema_migrations",
    )
    assert rows == [(bundle.manifest.migrations[0].id, "adoption.sql", "immutable")]
    assert all(fragment not in repr(rows) for fragment in ("baseline", ".064", ".065"))
    assert query(
        conn,
        "SELECT has_schema_privilege('public', 'warehouse_control', 'USAGE')",
    ) == [(False,)]

    # Hold the shared migration lock, start an adopter, and commit DDL before releasing it.
    # The adopter must take its repeatable-read snapshot after the wait and see the drift.
    locker = psycopg2.connect(target)
    with locker.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(%s, %s)", adoption.ADVISORY_LOCK_KEYS)
    locker.commit()
    waiting = psycopg2.connect(target)
    waiting_pid = waiting.get_backend_pid()
    outcome = []

    def wait_and_adopt():
        try:
            adoption.adopt_catalog(waiting, target, bundle)
        except Exception as exc:  # captured for assertion in the test thread
            outcome.append(exc)

    thread = threading.Thread(target=wait_and_adopt)
    thread.start()
    for _ in range(100):
        with locker.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_locks "
                "WHERE pid = %s AND locktype = 'advisory' AND NOT granted)",
                (waiting_pid,),
            )
            is_waiting = cur.fetchone()[0]
        locker.commit()
        if is_waiting:
            break
        time.sleep(0.05)
    else:
        pytest.fail("adopter did not reach the advisory-lock wait")
    query(locker, "ALTER TABLE core.capture_probe ADD COLUMN after_lock integer")
    with locker.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(%s, %s)", adoption.ADVISORY_LOCK_KEYS)
        assert cur.fetchone() == (True,)
    locker.commit()
    thread.join(timeout=30)
    waiting.close()
    locker.close()
    assert not thread.is_alive()
    assert len(outcome) == 1
    assert isinstance(outcome[0], adoption.AdoptionError)
    assert "does not match" in str(outcome[0])

    query(conn, "ALTER TABLE core.capture_probe DROP COLUMN after_lock")
    forward_path = bundle_output / "forward.sql"
    forward_path.write_text("ALTER TABLE core.capture_probe ADD COLUMN managed_forward integer;\n")
    manifest_path = bundle_output / adoption.MANIFEST_FILENAME
    manifest_payload = json.loads(manifest_path.read_text())
    manifest_payload["migrations"].append(
        {
            "id": "warehouse.forward.066",
            "path": "forward.sql",
            "kind": "immutable",
        }
    )
    manifest_path.write_text(json.dumps(manifest_payload, indent=2) + "\n")
    evolved = adoption.load_bundle(
        manifest_path,
        bundle_output / adoption.RECEIPT_FILENAME,
        root=bundle_output,
    )
    plan = apply_manifest(conn, evolved.manifest, mode="upgrade")
    assert [step.id for step in plan.pending] == ["warehouse.forward.066"]
    evolved_status = adoption.catalog_status(conn, target, evolved)
    assert evolved_status["valid"] is True
    assert evolved_status["catalog_verification"] == "not_checked_after_managed_upgrades"
    assert evolved_status["catalog_matches"] is None
    assert evolved_status["actual_fingerprint"] is None
    assert adoption.adopt_catalog(conn, target, evolved)["noop"] is True
    assert query(conn, "SELECT count(*) FROM warehouse_control.schema_migrations") == [(2,)]

    forward_path.write_text("SELECT 'tampered';\n")
    tampered = adoption.load_bundle(
        manifest_path,
        bundle_output / adoption.RECEIPT_FILENAME,
        root=bundle_output,
    )
    assert adoption.catalog_status(conn, target, tampered)["valid"] is False
    with pytest.raises(adoption.AdoptionError, match="checksum changed"):
        adoption.adopt_catalog(conn, target, tampered)
