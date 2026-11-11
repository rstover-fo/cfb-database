"""The catalog drift check, executed against real managed-warehouse builds."""

import json
import uuid
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import pytest
from psycopg2 import errors, sql
from psycopg2.extensions import TRANSACTION_STATUS_IDLE, make_dsn

from scripts import check_catalog_drift as drift
from scripts.warehouse_migrations import apply_manifest, load_manifest
from tests.test_warehouse_migrations_sql import warehouse_db  # noqa: F401

ROOT = Path(__file__).parents[1]
MANIFEST_PATH = ROOT / "src" / "schemas" / "warehouse-manifest.json"
FACTS = {"server_version": "17", "schemas": [], "items": 0}

USER_RELATION_LOCKS = """
SELECT count(*) FROM pg_locks l
JOIN pg_class c ON c.oid = l.relation
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE l.pid = pg_backend_pid() AND l.locktype = 'relation'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
"""

# What a daily dlt load does on its own when a provider payload grows.
DLT_EVOLUTION = """
ALTER TABLE core.games ADD COLUMN attendance__v_double double precision;
CREATE TABLE core.games__weather_notes (
    value text,
    _dlt_parent_id varchar NOT NULL,
    _dlt_list_idx bigint NOT NULL,
    _dlt_id varchar NOT NULL UNIQUE
);
CREATE TABLE core.plays_y2027 PARTITION OF core.plays FOR VALUES IN (2027);
"""

# Changes somebody made by hand, outside the manifest. Several are dressed up
# to look like dlt's work.
MANUAL_CHANGES = """
ALTER TABLE core.games DROP COLUMN notes;
ALTER TABLE core.games ADD COLUMN by_hand integer DEFAULT 0;
CREATE TABLE core.rogue_manual (id int);
CREATE VIEW api.games_raw AS SELECT * FROM core.games;
CREATE TABLE core.games_bak AS SELECT * FROM core.games;
ALTER FUNCTION ref.get_era(integer) VOLATILE;
GRANT INSERT ON core.games TO anon;
GRANT INSERT ON core.games__weather_notes TO anon;
ALTER VIEW api.team_detail SET (security_barrier = true);
DROP INDEX marts.team_season_summary_wins_idx;
CREATE TABLE core._dlt_games_backup AS SELECT * FROM core.games;
CREATE TABLE core.games__defaulted (_dlt_id varchar NOT NULL UNIQUE, v text DEFAULT 'x');
CREATE TABLE core.plays_rest PARTITION OF core.plays DEFAULT;
CREATE SEQUENCE core.by_hand_seq;
ALTER EVENT TRIGGER warehouse_source_invalidate_ddl DISABLE;
DROP EVENT TRIGGER warehouse_source_invalidate_drop;
CREATE TYPE core.ambiguous_one AS ENUM ('a, b', 'c');
CREATE TYPE core.ambiguous_two AS ENUM ('a', 'b, c');
"""


@contextmanager
def second_database(dsn):
    """Another randomly named database on the fixture's disposable cluster."""
    name = "f06_test_" + uuid.uuid4().hex
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name))
            )
        try:
            yield make_dsn(dsn, dbname=name)
        finally:
            with admin.cursor() as cur:
                cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
    finally:
        admin.close()


def build(dsn):
    conn = psycopg2.connect(dsn)
    try:
        apply_manifest(conn, load_manifest(MANIFEST_PATH, ROOT), mode="bootstrap")
    finally:
        conn.close()


def capture(dsn):
    conn = psycopg2.connect(dsn)
    try:
        return drift.capture_inventory(conn)
    finally:
        conn.close()


def execute(dsn, statements):
    conn = psycopg2.connect(dsn)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(statements)
    finally:
        conn.close()


def report(expected, production, *, details=True):
    return drift.build_report(
        expected,
        production,
        {},
        expected_facts=FACTS,
        production_facts=FACTS,
        include_details=details,
    )


def keyed(entries, *, access):
    return {
        (entry["kind"], entry["key"]): entry["status"]
        for entry in entries
        if (entry["kind"] in drift.ACCESS_KINDS) == access
    }


def test_two_managed_builds_have_identical_catalogs(warehouse_db):  # noqa: F811
    _, first = warehouse_db
    build(first)
    with second_database(first) as second:
        build(second)
        expected, facts = capture(first)
        production, _ = capture(second)

    assert facts["items"] > 5000
    assert {"api", "core", "marts", "public", "warehouse_control"} <= set(facts["schemas"])
    assert not any(schema.endswith("_staging") for schema in facts["schemas"])
    assert drift.diff_inventories(expected, production) == []
    # Names are schema-qualified, so the comparison does not depend on either
    # database's search_path.
    index = expected[("index", "marts.team_season_summary_wins_idx")]
    assert " ON marts.team_season_summary " in index.value
    assert ("definition", "api.team_detail") in expected
    # The safeguards the ledger installs as event triggers are part of the catalog.
    event_triggers = {key for kind, key in expected if kind == "event_trigger"}
    assert {"warehouse_source_invalidate_ddl", "warehouse_publication_invalidate_drop"} <= (
        event_triggers
    )
    order = json.loads(expected[("column_order", "core.plays")].value)
    assert order.index("id") < order.index("_dlt_id") and len(order) > 10


def test_capture_cannot_write_and_leaves_no_transaction_open(warehouse_db):  # noqa: F811
    _, dsn = warehouse_db
    build(dsn)
    conn = psycopg2.connect(dsn)
    try:
        drift.capture_inventory(conn)

        assert conn.info.transaction_status == TRANSACTION_STATUS_IDLE
        with conn.cursor() as cur, pytest.raises(errors.ReadOnlySqlTransaction):
            cur.execute("CREATE TABLE core.written_by_the_check (id int)")
    finally:
        conn.close()


def test_only_view_definitions_lock_user_relations(warehouse_db):  # noqa: F811
    _, dsn = warehouse_db
    build(dsn)
    conn = psycopg2.connect(dsn)
    conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
    try:
        with conn.cursor() as cur:
            drift._begin(cur)
            items = {}
            facts = drift._read_catalog(cur, items)
            cur.execute(USER_RELATION_LOCKS)
            # The snapshot that reads the whole catalog blocks nobody's DDL.
            assert cur.fetchone() == (0,)

            drift._read_definitions(cur, items, facts["schemas"])
            cur.execute(USER_RELATION_LOCKS)
            assert cur.fetchone()[0] > 0
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(USER_RELATION_LOCKS)
            assert cur.fetchone() == (0,)
    finally:
        conn.close()


def test_capture_gives_up_quickly_when_a_view_is_locked(warehouse_db, monkeypatch):  # noqa: F811
    _, dsn = warehouse_db
    build(dsn)
    monkeypatch.setattr(drift, "LOCK_TIMEOUT", "300ms")
    blocker = psycopg2.connect(dsn)
    conn = psycopg2.connect(dsn)
    try:
        with blocker.cursor() as cur:
            cur.execute("LOCK TABLE api.team_detail IN ACCESS EXCLUSIVE MODE")

        with pytest.raises(errors.LockNotAvailable):
            drift.capture_inventory(conn)

        assert conn.info.transaction_status == TRANSACTION_STATUS_IDLE
    finally:
        blocker.rollback()
        blocker.close()
        conn.close()


def test_dlt_evolution_is_separated_from_manual_changes(warehouse_db):  # noqa: F811
    _, dsn = warehouse_db
    build(dsn)
    expected, _ = capture(dsn)
    assert "core.games" in drift.dlt_managed_relations(expected)

    execute(dsn, DLT_EVOLUTION)
    after_load, _ = capture(dsn)
    loaded = report(expected, after_load)

    # A table created in a dlt schema gets its siblings' access from the
    # schema's default privileges, so dlt's own additions raise nothing.
    assert loaded["summary"]["drift"] == 0
    assert keyed(loaded["ingest_evolved"], access=False) == {
        ("column", "core.games.attendance__v_double"): drift.UNEXPECTED,
        ("relation", "core.games__weather_notes"): drift.UNEXPECTED,
        ("relation", "core.plays_y2027"): drift.UNEXPECTED,
    }

    execute(dsn, MANUAL_CHANGES)
    after_edits, _ = capture(dsn)
    edited = report(expected, after_edits)

    assert keyed(edited["ingest_evolved"], access=False) == {
        ("column", "core.games.attendance__v_double"): drift.UNEXPECTED,
        ("relation", "core.plays_y2027"): drift.UNEXPECTED,
    }
    assert keyed(edited["drift"], access=False) == {
        ("column", "core.games.notes"): drift.MISSING,
        ("column", "core.games.by_hand"): drift.UNEXPECTED,
        ("relation", "core.rogue_manual"): drift.UNEXPECTED,
        ("relation", "api.games_raw"): drift.UNEXPECTED,
        ("relation", "core.games_bak"): drift.UNEXPECTED,
        # dlt made this table, but it no longer looks like its siblings.
        ("relation", "core.games__weather_notes"): drift.UNEXPECTED,
        ("relation", "core._dlt_games_backup"): drift.UNEXPECTED,
        ("relation", "core.games__defaulted"): drift.UNEXPECTED,
        ("relation", "core.plays_rest"): drift.UNEXPECTED,
        ("relation", "core.by_hand_seq"): drift.UNEXPECTED,
        ("event_trigger", "warehouse_source_invalidate_ddl"): drift.CHANGED,
        ("event_trigger", "warehouse_source_invalidate_drop"): drift.MISSING,
        ("enum", "core.ambiguous_one"): drift.UNEXPECTED,
        ("enum", "core.ambiguous_two"): drift.UNEXPECTED,
        ("function", "ref.get_era(p_year integer)"): drift.CHANGED,
        ("index", "marts.team_season_summary_wins_idx"): drift.MISSING,
    }
    access = keyed(edited["drift"], access=True)
    assert access[("grant", "relation core.games -> anon")] == drift.CHANGED
    assert access[("relation_options", "api.team_detail")] == drift.CHANGED
    assert access[("grant", "relation core.games__weather_notes -> anon")] == drift.UNEXPECTED
    for extra in ("core.rogue_manual", "api.games_raw", "core.games_bak"):
        assert access[("owner", f"relation {extra}")] == drift.UNEXPECTED
    # A sequence with no explicit ACL reports the built-in sequence defaults.
    owner_grant = after_edits[("grant", "relation core.by_hand_seq -> postgres")]
    assert owner_grant.value == "SELECT, UPDATE, USAGE"
    assert after_edits[("relation", "core.plays_y2027")].value == (
        "table; partition of core.plays; bound FOR VALUES IN ('2027')"
    )
    # Enum labels keep their boundaries, so these two differ.
    assert after_edits[("enum", "core.ambiguous_one")].value == '["a, b", "c"]'
    assert after_edits[("enum", "core.ambiguous_two")].value == '["a", "b, c"]'

    hidden = report(expected, after_edits, details=False)
    assert hidden["summary"]["drift"] == edited["summary"]["drift"]
    withheld = [entry for entry in hidden["drift"] if entry["kind"] in drift.ACCESS_KINDS]
    assert withheld and all(entry.get("redacted") for entry in withheld)
    assert sum(entry["count"] for entry in withheld) == len(access)
    assert set().union(*withheld) == {"kind", "schema", "status", "count", "redacted"}
    assert "-> anon" not in json.dumps(hidden)
    function = next(entry for entry in hidden["drift"] if entry["kind"] == "function")
    assert function["production"].startswith("[withheld: ")
    assert "VOLATILE" not in json.dumps(hidden["drift"])
