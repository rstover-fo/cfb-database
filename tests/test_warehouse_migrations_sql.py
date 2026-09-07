"""Executed F06 ledger behavior on explicitly selected disposable loopback Postgres."""

import json
import os
import uuid
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
import pytest
from psycopg2 import sql
from psycopg2.extensions import make_dsn

from scripts.warehouse_migrations import (
    ADVISORY_LOCK_KEYS,
    MigrationStateError,
    apply_manifest,
    load_manifest,
)


@pytest.fixture
def warehouse_db():
    dsn = os.environ.get("F06_TEST_DB_URL")
    if not dsn:
        if os.environ.get("F06_REQUIRE_DB") == "1":
            pytest.fail("F06_TEST_DB_URL is required for mandatory bootstrap integration")
        pytest.skip("set F06_TEST_DB_URL to a disposable loopback PostgreSQL cluster")
    parsed = urlparse(dsn)
    if parsed.scheme not in {"postgres", "postgresql"} or parsed.hostname not in {
        "localhost",
        "127.0.0.1",
        "::1",
    }:
        pytest.fail("F06_TEST_DB_URL must be an explicitly selected loopback PostgreSQL URL")
    # Only our randomly named database is created/dropped; no pre-existing schema is removed.
    name = "f06_test_" + uuid.uuid4().hex
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name)))
    target = make_dsn(dsn, dbname=name)
    conn = psycopg2.connect(target)
    try:
        yield conn, target
    finally:
        conn.close()
        with admin.cursor() as cur:
            cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        admin.close()


def query(conn, statement, values=None):
    with conn.cursor() as cur:
        cur.execute(statement, values)
        result = cur.fetchall() if cur.description else None
    conn.commit()
    return result


def manifest_at(root: Path, entries):
    root.mkdir(exist_ok=True)
    for name, kind, source in entries:
        (root / f"{name}.sql").write_text(source)
    path = root / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "migrations": [
                    {"id": name, "path": f"{name}.sql", "kind": kind} for name, kind, _ in entries
                ],
            }
        )
    )
    return load_manifest(path, root)


def test_bootstrap_upgrade_noop_and_changed_applied_bytes(warehouse_db, tmp_path):
    conn, _ = warehouse_db
    entries = [
        (
            "baseline",
            "immutable",
            "CREATE TABLE public.f06_values(id int PRIMARY KEY); "
            "INSERT INTO public.f06_values VALUES(1);",
        ),
        (
            "upgrade",
            "immutable",
            "ALTER TABLE public.f06_values ADD COLUMN note text DEFAULT 'retained';",
        ),
    ]
    manifest = manifest_at(tmp_path, entries)
    apply_manifest(conn, manifest, mode="bootstrap", target="baseline")
    apply_manifest(conn, manifest, mode="upgrade")
    assert query(conn, "SELECT * FROM public.f06_values") == [(1, "retained")]
    assert not apply_manifest(conn, manifest, mode="bootstrap").pending
    assert query(conn, "SELECT count(*) FROM warehouse_control.schema_migrations") == [(2,)]
    entries[0] = ("baseline", "immutable", "DROP TABLE public.f06_values;")
    changed = manifest_at(tmp_path, entries)
    with pytest.raises(MigrationStateError):
        apply_manifest(conn, changed, mode="upgrade")
    assert query(conn, "SELECT * FROM public.f06_values") == [(1, "retained")]


def test_failed_batch_rolls_back_schema_and_ledger(warehouse_db, tmp_path):
    conn, _ = warehouse_db
    manifest = manifest_at(
        tmp_path,
        [
            ("baseline", "immutable", "CREATE TABLE public.f06_atomic(id int);"),
            ("broken", "immutable", "SELECT nonexistent_f06_function();"),
        ],
    )
    with pytest.raises(psycopg2.Error):
        apply_manifest(conn, manifest, mode="bootstrap")
    assert query(
        conn,
        "SELECT to_regclass('public.f06_atomic'), "
        "to_regclass('warehouse_control.schema_migrations')",
    ) == [(None, None)]


def test_repeatable_history_preserves_both_executions(warehouse_db, tmp_path):
    conn, _ = warehouse_db
    entries = [
        ("baseline", "immutable", "CREATE TABLE public.f06_source(id int);"),
        ("view", "repeatable", "CREATE VIEW public.f06_view AS SELECT 1 AS n;"),
    ]
    apply_manifest(conn, manifest_at(tmp_path, entries), mode="bootstrap")
    entries[1] = ("view", "repeatable", "CREATE OR REPLACE VIEW public.f06_view AS SELECT 2 AS n;")
    changed = manifest_at(tmp_path, entries)
    apply_manifest(conn, changed, mode="upgrade")
    assert not apply_manifest(conn, changed, mode="upgrade").pending
    assert query(conn, "SELECT * FROM public.f06_view") == [(2,)]
    assert query(
        conn,
        "SELECT count(DISTINCT checksum) FROM warehouse_control.repeatable_migration_executions",
    ) == [(2,)]


def test_unmanaged_database_and_dryrun_fail_closed(warehouse_db, tmp_path):
    conn, _ = warehouse_db
    manifest = manifest_at(tmp_path, [("base", "immutable", "CREATE SCHEMA core;")])
    with pytest.raises(MigrationStateError):
        apply_manifest(conn, manifest, mode="upgrade")
    apply_manifest(conn, manifest, mode="bootstrap", dry_run=True)
    assert query(conn, "SELECT to_regnamespace('core'), to_regnamespace('warehouse_control')") == [
        (None, None)
    ]
    query(conn, "CREATE SCHEMA core")
    with pytest.raises(MigrationStateError):
        apply_manifest(conn, manifest, mode="bootstrap")


def test_migration_lock_covers_competing_connection(warehouse_db, tmp_path):
    conn, target = warehouse_db
    manifest = manifest_at(tmp_path, [("base", "immutable", "CREATE SCHEMA core;")])
    contender = psycopg2.connect(target, options="-c lock_timeout=200ms")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s,%s)", ADVISORY_LOCK_KEYS)
        with pytest.raises(psycopg2.errors.LockNotAvailable):
            apply_manifest(contender, manifest, mode="bootstrap")
        conn.rollback()
        apply_manifest(contender, manifest, mode="bootstrap")
        assert not apply_manifest(conn, manifest, mode="upgrade").pending
    finally:
        contender.close()


def test_forward_columns_precede_changed_repeatable_views(warehouse_db, tmp_path):
    conn, _ = warehouse_db
    entries = [
        ("base", "immutable", "CREATE TABLE public.f06_source(a int);"),
        ("view", "repeatable", "CREATE VIEW public.f06_view AS SELECT a FROM public.f06_source;"),
    ]
    apply_manifest(conn, manifest_at(tmp_path, entries), mode="bootstrap")
    entries[1] = (
        "view",
        "repeatable",
        "CREATE OR REPLACE VIEW public.f06_view AS SELECT a,b FROM public.f06_source;",
    )
    entries.append(("add_column", "immutable", "ALTER TABLE public.f06_source ADD COLUMN b int;"))
    apply_manifest(conn, manifest_at(tmp_path, entries), mode="upgrade")
    assert query(
        conn,
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name='f06_view' ORDER BY ordinal_position",
    ) == [("a",), ("b",)]


def test_autocommit_rejected_without_partial_database_work(warehouse_db, tmp_path):
    conn, _ = warehouse_db
    conn.autocommit = True
    manifest = manifest_at(tmp_path, [("base", "immutable", "CREATE SCHEMA core;")])
    with pytest.raises(RuntimeError, match="autocommit"):
        apply_manifest(conn, manifest, mode="bootstrap")
    assert query(conn, "SELECT to_regnamespace('core'), to_regnamespace('warehouse_control')") == [
        (None, None)
    ]


def test_ledger_stays_private_under_broad_host_default_grants(warehouse_db, tmp_path):
    conn, _ = warehouse_db
    query(
        conn,
        """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='anon') THEN CREATE ROLE anon; END IF;
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='authenticated') THEN
                CREATE ROLE authenticated;
            END IF;
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='analyst_ro') THEN
                CREATE ROLE analyst_ro;
            END IF;
        END $$;
        ALTER DEFAULT PRIVILEGES GRANT ALL ON SCHEMAS TO anon, authenticated, analyst_ro;
        ALTER DEFAULT PRIVILEGES GRANT ALL ON TABLES TO anon, authenticated, analyst_ro;
        ALTER DEFAULT PRIVILEGES GRANT ALL ON SEQUENCES TO anon, authenticated, analyst_ro;
    """,
    )
    manifest = manifest_at(tmp_path, [("base", "immutable", "CREATE SCHEMA core;")])
    apply_manifest(conn, manifest, mode="bootstrap")
    for role in ("anon", "authenticated", "analyst_ro"):
        assert query(
            conn,
            """
            SELECT has_schema_privilege(%s, 'warehouse_control', 'USAGE,CREATE'),
                has_table_privilege(%s, 'warehouse_control.schema_migrations',
                    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'),
                has_table_privilege(%s, 'warehouse_control.repeatable_migration_executions',
                    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'),
                has_sequence_privilege(%s,
                    'warehouse_control.repeatable_migration_executions_execution_id_seq',
                    'USAGE,SELECT,UPDATE'),
                has_schema_privilege(%s, 'core', 'USAGE')
        """,
            (role, role, role, role, role),
        ) == [(False, False, False, False, True)]
        for command in (
            "SELECT * FROM warehouse_control.schema_migrations",
            "DELETE FROM warehouse_control.schema_migrations",
            "TRUNCATE warehouse_control.schema_migrations CASCADE",
            "SELECT * FROM warehouse_control.repeatable_migration_executions",
            "DELETE FROM warehouse_control.repeatable_migration_executions",
            "SELECT nextval('warehouse_control.repeatable_migration_executions_execution_id_seq')",
        ):
            with conn.cursor() as cur:
                cur.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
                with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                    cur.execute(command)
            conn.rollback()
    assert query(conn, "SELECT count(*) FROM warehouse_control.schema_migrations") == [(1,)]

    # The private ledger policy must not rewrite the host's defaults.
    query(conn, "CREATE SCHEMA f06_normal; CREATE TABLE f06_normal.rows(id int)")
    with conn.cursor() as cur:
        cur.execute("SET LOCAL ROLE anon")
        cur.execute("INSERT INTO f06_normal.rows VALUES (1)")
        cur.execute("SELECT id FROM f06_normal.rows")
        assert cur.fetchall() == [(1,)]
    conn.rollback()
