"""Executed F07 dependency coverage, consumer contracts, and atomic rollback."""

import hashlib
import json
from pathlib import Path

import psycopg2
import pytest

from scripts.warehouse_migrations import apply_manifest, load_manifest
from tests.test_warehouse_migrations_sql import query
from tests.test_warehouse_migrations_sql import warehouse_db as _warehouse_db_fixture  # noqa: F401

ROOT = Path(__file__).parents[1]


@pytest.fixture(name="warehouse_db")
def _release_warehouse_db(request):
    return request.getfixturevalue("_warehouse_db_fixture")


def write_release(root, sources, *, restores=None, validations=None, consumers=None):
    root.mkdir(exist_ok=True)
    files = []
    for index, source in enumerate(sources):
        path = root / f"{index:02}.sql"
        path.write_text(source)
        files.append({"path": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    data = {
        "version": 1,
        "files": files,
        "roots": ["marts.release_parent"],
        "restores": [
            {"kind": "relation", "identity": name}
            for name in restores
            or ["marts.release_parent", "marts.release_child", "api.release_view"]
        ],
        "validations": validations
        or [
            {
                "name": "consumer",
                "role": "anon",
                "query": "SELECT count(*) = 1 FROM api.release_view",
                "covers": [],
            }
        ],
    }
    if consumers is not None:
        data["consumers"] = consumers
    path = root / "release.json"
    path.write_text(json.dumps(data))
    from scripts.mart_release import load_release

    return load_release(path, root)


SETUP = """
DO $$ BEGIN CREATE ROLE anon NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
CREATE SCHEMA marts;
CREATE SCHEMA api;
GRANT USAGE ON SCHEMA api TO anon;
CREATE TABLE public.release_source(id integer PRIMARY KEY, value integer);
INSERT INTO public.release_source VALUES (1, 7);
CREATE MATERIALIZED VIEW marts.release_parent AS SELECT * FROM public.release_source;
CREATE UNIQUE INDEX release_parent_id ON marts.release_parent(id);
CREATE MATERIALIZED VIEW marts.release_child AS SELECT * FROM marts.release_parent;
CREATE UNIQUE INDEX release_child_id ON marts.release_child(id);
CREATE VIEW api.release_view AS SELECT * FROM marts.release_child;
GRANT SELECT ON api.release_view TO anon;
"""
PARENT = """
DROP MATERIALIZED VIEW marts.release_parent CASCADE;
CREATE MATERIALIZED VIEW marts.release_parent AS
SELECT id, value + 1 AS value FROM public.release_source;
CREATE UNIQUE INDEX release_parent_id ON marts.release_parent(id);
"""
CHILD = """
CREATE MATERIALIZED VIEW marts.release_child AS SELECT * FROM marts.release_parent;
CREATE UNIQUE INDEX release_child_id ON marts.release_child(id);
CREATE VIEW api.release_view AS SELECT * FROM marts.release_child;
"""


def snapshot(conn):
    return query(
        conn,
        """
        SELECT c.oid, n.nspname, c.relname, pg_get_viewdef(c.oid),
               pg_get_userbyid(c.relowner), c.relacl::text
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname IN ('marts','api') AND c.relkind IN ('m','v') ORDER BY 2,3
    """,
    )


def consumer_read(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL ROLE anon")
            cur.execute("SELECT * FROM api.release_view")
            return cur.fetchall()
    finally:
        conn.rollback()


def test_release_atomic_failure_then_success(warehouse_db, tmp_path):
    from scripts.mart_release import execute_release, plan_release

    conn, _ = warehouse_db
    query(conn, SETUP)
    before = snapshot(conn)
    broken = write_release(tmp_path / "broken", [PARENT, CHILD, "SELECT 1/0;"])
    assert plan_release(conn, broken).valid
    assert snapshot(conn) == before  # planning executes no DDL
    with pytest.raises(psycopg2.errors.DivisionByZero):
        execute_release(conn, broken)
    assert snapshot(conn) == before
    assert consumer_read(conn) == [(1, 7)]
    good = write_release(tmp_path / "good", [PARENT, CHILD])
    assert execute_release(conn, good).valid
    assert consumer_read(conn) == [(1, 8)]
    assert execute_release(conn, good).valid
    assert consumer_read(conn) == [(1, 8)]


def test_incomplete_and_new_dependency_fail_before_mutation(warehouse_db, tmp_path):
    from scripts.mart_release import ReleaseBlockedError, execute_release, plan_release

    conn, _ = warehouse_db
    query(conn, SETUP)
    before = snapshot(conn)
    incomplete = write_release(tmp_path / "incomplete", [PARENT], restores=["marts.release_parent"])
    assert not plan_release(conn, incomplete).valid
    with pytest.raises(ReleaseBlockedError):
        execute_release(conn, incomplete)
    assert snapshot(conn) == before
    complete = write_release(tmp_path / "complete", [PARENT, CHILD])
    assert plan_release(conn, complete).valid
    query(conn, "CREATE VIEW api.unplanned_consumer AS SELECT * FROM marts.release_child")
    changed = snapshot(conn)
    assert not plan_release(conn, complete).valid
    with pytest.raises(ReleaseBlockedError):
        execute_release(conn, complete)
    assert snapshot(conn) == changed


def test_incompatible_columns_roll_back(warehouse_db, tmp_path):
    from scripts.mart_release import ReleaseExecutionError, execute_release, plan_release

    conn, _ = warehouse_db
    query(conn, SETUP)
    before = snapshot(conn)
    altered = PARENT.replace("value + 1 AS value", "(value + 1)::bigint AS value")
    release = write_release(tmp_path, [altered, CHILD])
    assert plan_release(conn, release).valid
    with pytest.raises(ReleaseExecutionError, match="contract"):
        execute_release(conn, release)
    assert snapshot(conn) == before
    assert consumer_read(conn) == [(1, 7)]


def test_missing_index_rolls_back(warehouse_db, tmp_path):
    from scripts.mart_release import ReleaseExecutionError, execute_release, plan_release

    conn, _ = warehouse_db
    query(conn, SETUP)
    before = snapshot(conn)
    release = write_release(
        tmp_path,
        [
            PARENT.replace(
                "CREATE UNIQUE INDEX release_parent_id ON marts.release_parent(id);", ""
            ),
            CHILD,
        ],
    )
    assert plan_release(conn, release).valid
    with pytest.raises(ReleaseExecutionError, match="contract"):
        execute_release(conn, release)
    assert snapshot(conn) == before
    assert consumer_read(conn) == [(1, 7)]


def test_unpopulated_replacement_rolls_back_without_consumer_assertion(warehouse_db, tmp_path):
    from scripts.mart_release import ReleaseExecutionError, execute_release, plan_release

    conn, _ = warehouse_db
    query(conn, SETUP)
    before = snapshot(conn)
    parent = PARENT.replace(
        "FROM public.release_source;", "FROM public.release_source WITH NO DATA;"
    )
    child = CHILD.replace("FROM marts.release_parent;", "FROM marts.release_parent WITH NO DATA;")
    release = write_release(
        tmp_path,
        [parent, child],
        validations=[{"name": "trivial", "role": "anon", "query": "SELECT true", "covers": []}],
    )
    assert plan_release(conn, release).valid
    with pytest.raises(ReleaseExecutionError, match="contract"):
        execute_release(conn, release)
    assert snapshot(conn) == before
    assert consumer_read(conn) == [(1, 7)]


@pytest.mark.parametrize("assertion", ["SELECT 1", "SELECT 1::numeric"])
def test_numeric_assertion_is_not_boolean_success(warehouse_db, tmp_path, assertion):
    from scripts.mart_release import ReleaseExecutionError, execute_release

    conn, _ = warehouse_db
    query(conn, SETUP)
    before = snapshot(conn)
    release = write_release(
        tmp_path,
        [PARENT, CHILD],
        validations=[{"name": "numeric", "role": "anon", "query": assertion, "covers": []}],
    )
    with pytest.raises(ReleaseExecutionError, match="boolean"):
        execute_release(conn, release)
    assert snapshot(conn) == before
    assert consumer_read(conn) == [(1, 7)]


def test_declared_dynamic_consumer_requires_and_runs_assertion(warehouse_db, tmp_path):
    from scripts.mart_release import ReleaseBlockedError, execute_release, plan_release

    conn, _ = warehouse_db
    query(conn, SETUP)
    query(
        conn,
        """
        CREATE FUNCTION api.dynamic_release_value() RETURNS integer LANGUAGE plpgsql AS $$
        DECLARE result integer;
        BEGIN EXECUTE 'SELECT value FROM api.release_view' INTO result; RETURN result; END $$;
        GRANT EXECUTE ON FUNCTION api.dynamic_release_value() TO anon;
    """,
    )
    consumer = "api.dynamic_release_value()"
    uncovered = write_release(tmp_path / "uncovered", [PARENT, CHILD], consumers=[consumer])
    assert not plan_release(conn, uncovered).valid
    with pytest.raises(ReleaseBlockedError):
        execute_release(conn, uncovered)
    covered = write_release(
        tmp_path / "covered",
        [PARENT, CHILD],
        consumers=[consumer],
        validations=[
            {
                "name": "dynamic",
                "role": "anon",
                "query": "SELECT api.dynamic_release_value() = 8",
                "covers": [consumer],
            }
        ],
    )
    assert plan_release(conn, covered).valid
    assert execute_release(conn, covered).valid
    assert consumer_read(conn) == [(1, 8)]


def test_unrelated_public_consumer_change_rolls_back(warehouse_db, tmp_path):
    from scripts.mart_release import ReleaseExecutionError, execute_release, plan_release

    conn, _ = warehouse_db
    query(conn, SETUP)
    query(conn, "CREATE VIEW public.unrelated_consumer AS SELECT 17 AS value")
    before = snapshot(conn)
    release = write_release(
        tmp_path,
        [PARENT, CHILD, "CREATE OR REPLACE VIEW public.unrelated_consumer AS SELECT 19 AS value"],
    )
    assert plan_release(conn, release).valid
    with pytest.raises(ReleaseExecutionError, match="outside"):
        execute_release(conn, release)
    assert snapshot(conn) == before
    assert query(conn, "SELECT * FROM public.unrelated_consumer") == [(17,)]


def test_recreation_does_not_leak_new_default_grants(warehouse_db, tmp_path):
    from scripts.mart_release import execute_release, plan_release

    conn, _ = warehouse_db
    query(conn, SETUP)
    query(conn, "ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO PUBLIC")
    release = write_release(tmp_path, [PARENT, CHILD])
    assert plan_release(conn, release).valid
    assert execute_release(conn, release).valid
    assert consumer_read(conn) == [(1, 8)]
    assert query(conn, "SELECT has_table_privilege('anon', 'marts.release_parent', 'SELECT')") == [
        (False,)
    ]


def test_composite_dependent_type_blocks_release(warehouse_db, tmp_path):
    from scripts.mart_release import ReleaseBlockedError, execute_release, plan_release

    conn, _ = warehouse_db
    query(conn, SETUP)
    query(conn, "CREATE TYPE public.release_payload AS (item marts.release_parent)")
    before = snapshot(conn)
    release = write_release(tmp_path, [PARENT, CHILD])
    plan = plan_release(conn, release)
    assert not plan.valid, plan
    with pytest.raises(ReleaseBlockedError):
        execute_release(conn, release)
    assert snapshot(conn) == before
    assert query(conn, "SELECT to_regtype('public.release_payload') IS NOT NULL") == [(True,)]


def test_row_type_function_requires_explicit_restoration(warehouse_db, tmp_path):
    from scripts.mart_release import (
        ObjectIdentity,
        ReleaseBlockedError,
        execute_release,
        plan_release,
    )

    conn, _ = warehouse_db
    query(conn, SETUP)
    query(
        conn,
        "CREATE FUNCTION api.release_fn(x marts.release_parent) RETURNS integer "
        "LANGUAGE sql IMMUTABLE AS 'SELECT ($1).id'",
    )
    before = snapshot(conn)
    release = write_release(tmp_path, [PARENT, CHILD])
    plan = plan_release(conn, release)
    assert not plan.valid, plan
    assert ObjectIdentity("function", "api.release_fn(x marts.release_parent)") in plan.live_closure
    with pytest.raises(ReleaseBlockedError):
        execute_release(conn, release)
    assert snapshot(conn) == before
    assert query(conn, "SELECT api.release_fn(ROW(1,7)::marts.release_parent)") == [(1,)]


@pytest.mark.parametrize(
    "metadata",
    [
        "COMMENT ON MATERIALIZED VIEW marts.release_parent IS 'preserve mart documentation'",
        "GRANT SELECT (id) ON marts.release_parent TO anon",
        "CREATE RULE release_no_delete AS ON DELETE TO api.release_view DO INSTEAD NOTHING",
    ],
)
def test_omitted_metadata_rolls_back(warehouse_db, tmp_path, metadata):
    from scripts.mart_release import ReleaseExecutionError, execute_release, plan_release

    conn, _ = warehouse_db
    query(conn, SETUP)
    query(conn, metadata)
    before = snapshot(conn)
    release = write_release(tmp_path, [PARENT, CHILD])
    assert plan_release(conn, release).valid
    with pytest.raises(ReleaseExecutionError, match="contract"):
        execute_release(conn, release)
    assert snapshot(conn) == before
    assert consumer_read(conn) == [(1, 7)]


def test_real_trajectory_release_preserves_invoker_contract(warehouse_db):
    from scripts.mart_release import execute_release, load_release, plan_release

    conn, _ = warehouse_db
    apply_manifest(
        conn, load_manifest(ROOT / "src/schemas/warehouse-manifest.json", ROOT), mode="bootstrap"
    )
    release = load_release(ROOT / "deploys/mart-releases/trajectory.json", ROOT)
    plan = plan_release(conn, release)
    assert plan.valid, plan
    assert execute_release(conn, release).valid
    for role in ("anon", "authenticated"):
        try:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL ROLE {role}")
                cur.execute("SELECT * FROM public.team_season_trajectory LIMIT 1")
                assert cur.fetchall() == []
                with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                    cur.execute("SELECT * FROM scouting.players LIMIT 1")
        finally:
            conn.rollback()
