"""Executed dependency-generation refresh checks on disposable PostgreSQL 17."""

from __future__ import annotations

import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from time import monotonic, sleep

import psycopg2
import pytest
from psycopg2 import sql

from tests.test_asset_freshness_sql import freshness_db as _freshness_db  # noqa: F401
from tests.test_asset_publication_sql import (
    publication_args,
    publish,
    publisher_query,
    start_run,
)
from tests.test_asset_publication_sql import publication_db as _publication_db  # noqa: F401
from tests.test_warehouse_migrations_sql import query
from tests.test_warehouse_migrations_sql import warehouse_db as _warehouse_db  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "src/schemas/migrations/070_generation_enforced_refresh.sql"
SOURCE = "analytics.house_elo_game"
MART = "marts.house_elo_game"
PROTOCOL = "house-elo-game-refresh-v1"


@pytest.fixture
def generation_db(request):
    conn, target = request.getfixturevalue("_freshness_db")
    query(conn, MIGRATION.read_text())
    return conn, target


def refresher_query(conn, statement, values=None, *, commit=True):
    with conn.cursor() as cur:
        cur.execute("SET LOCAL ROLE warehouse_refresher")
        cur.execute(statement, values)
        result = cur.fetchall() if cur.description else None
    if commit:
        conn.commit()
    return result


def set_source_policy(conn, value="100 years"):
    query(
        conn,
        "UPDATE meta.asset_freshness_policies "
        "SET expected_refresh_interval=%s WHERE asset_key=%s AND coverage_key='source-wide'",
        (value, SOURCE),
    )


def get_plan(conn):
    return refresher_query(conn, "SELECT warehouse_refresh.get_house_elo_game_plan()")[0][0]


def start_refresh(conn, plan, run_id=None):
    run_id = str(run_id or uuid.uuid4())
    result = refresher_query(
        conn,
        "SELECT warehouse_refresh.start_house_elo_game_refresh(%s,%s::jsonb)",
        (run_id, json.dumps(plan)),
    )[0][0]
    assert str(result) == run_id
    return run_id


def finish_refresh(conn, run_id, generation_id=None, *, commit=True):
    generation_id = str(generation_id or uuid.uuid4())
    result = refresher_query(
        conn,
        "SELECT * FROM warehouse_refresh.publish_house_elo_game_refresh(%s,%s)",
        (run_id, generation_id),
        commit=commit,
    )[0]
    return generation_id, result


def fail_refresh(conn, run_id, generation_id=None, outcome="failed"):
    generation_id = str(generation_id or uuid.uuid4())
    result = refresher_query(
        conn,
        "SELECT warehouse_refresh.fail_house_elo_game_refresh(%s,%s,%s)",
        (run_id, generation_id, outcome),
    )[0][0]
    return generation_id, result


def record_source_failure(conn, outcome="failed"):
    run_id = start_run(conn)
    source_generation, mart_generation = str(uuid.uuid4()), str(uuid.uuid4())
    publisher_query(
        conn,
        "SELECT * FROM warehouse_publication.record_house_elo_failure(%s,%s,%s,%s,%s)",
        (str(run_id), source_generation, mart_generation, outcome, "publication_failed"),
    )
    return source_generation, mart_generation


def prepare_refresh(conn, *, policy="100 years"):
    source_args = publication_args(conn)
    publish(conn, source_args)
    set_source_policy(conn, policy)
    plan = get_plan(conn)
    run_id = start_refresh(conn, plan)
    return source_args, plan, run_id


def current_generations(conn):
    return dict(
        query(
            conn,
            "SELECT asset_key,generation_id::text FROM meta.asset_current_generations "
            "ORDER BY asset_key",
        )
    )


def wait_for_lock(conn, application_name):
    deadline = monotonic() + 5
    while monotonic() < deadline:
        if query(
            conn,
            "SELECT wait_event_type='Lock' FROM pg_stat_activity "
            "WHERE datname=current_database() AND application_name=%s AND state='active'",
            (application_name,),
        ) == [(True,)]:
            return
        sleep(0.02)
    pytest.fail(f"{application_name} never waited for the controlled refresh lock")


def denied(conn, role, statement):
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cur.execute(statement)
    finally:
        conn.rollback()


def test_plan_is_fixed_and_pins_current_generations(generation_db):
    conn, _ = generation_db
    source_args = publication_args(conn)
    publish(conn, source_args)
    set_source_policy(conn, "2 days")

    plan = get_plan(conn)
    assert plan == {
        "protocol": PROTOCOL,
        "assets": [MART],
        "coverage_key": "source-wide",
        "input_asset": SOURCE,
        "source_generation_id": source_args[1],
        "mart_generation_id": source_args[2],
        "expected_refresh_interval": "2 days",
        "unversioned_input_assets": ["core.games"],
    }


def test_plan_blocks_missing_input_or_undeclared_policy(generation_db):
    conn, _ = generation_db
    with pytest.raises(psycopg2.Error, match="source|current|generation|input"):
        get_plan(conn)
    conn.rollback()

    publish(conn, publication_args(conn))
    with pytest.raises(psycopg2.Error, match="cadence|interval|policy"):
        get_plan(conn)
    conn.rollback()


def test_refresh_atomically_records_exact_input_and_keeps_source_pointer(generation_db):
    conn, _ = generation_db
    source_args, plan, run_id = prepare_refresh(conn)
    before_source = current_generations(conn)[SOURCE]
    generation_id, result = finish_refresh(conn, run_id)

    assert str(result[0]) == generation_id
    assert result[1] is False
    assert result[2] == 1
    pointers = current_generations(conn)
    assert pointers[SOURCE] == before_source == source_args[1]
    assert pointers[MART] == generation_id
    assert query(
        conn,
        """
        SELECT r.asset_key,r.outcome,r.coverage->>'complete',
               i.input_asset_key,i.input_generation_id::text,
               o.operation_kind,o.outcome
        FROM meta.asset_receipts r
        JOIN meta.asset_receipt_inputs i USING (generation_id)
        JOIN meta.operation_runs o USING (operation_run_id)
        WHERE r.generation_id=%s
        """,
        (generation_id,),
    ) == [(MART, "succeeded", "true", SOURCE, plan["source_generation_id"], "refresh", "succeeded")]


def test_refresh_rollback_exposes_no_partial_receipt_edge_or_pointer(generation_db):
    conn, _ = generation_db
    _, _, run_id = prepare_refresh(conn)
    before = current_generations(conn)
    receipt_count = query(conn, "SELECT count(*) FROM meta.asset_receipts")[0][0]
    edge_count = query(conn, "SELECT count(*) FROM meta.asset_receipt_inputs")[0][0]

    generation_id, _ = finish_refresh(conn, run_id, commit=False)
    conn.rollback()
    assert current_generations(conn) == before
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts") == [(receipt_count,)]
    assert query(conn, "SELECT count(*) FROM meta.asset_receipt_inputs") == [(edge_count,)]
    assert query(
        conn, "SELECT count(*) FROM meta.asset_receipts WHERE generation_id=%s", (generation_id,)
    ) == [(0,)]


def test_expected_no_data_refresh_is_complete_and_keeps_zero_rows(generation_db):
    conn, _ = generation_db
    query(conn, "UPDATE core.games SET completed=false,home_points=NULL,away_points=NULL")
    args = list(publication_args(conn))
    args[6], args[7] = "[]", "[]"
    publish(conn, tuple(args))
    set_source_policy(conn)
    plan = get_plan(conn)
    run_id = start_refresh(conn, plan)

    generation_id, result = finish_refresh(conn, run_id)
    assert str(result[0]) == generation_id
    assert result[1:] == (False, 0)
    assert query(
        conn,
        "SELECT outcome,coverage->>'complete',row_delta->>'published_rows' "
        "FROM meta.asset_receipts WHERE generation_id=%s",
        (generation_id,),
    ) == [("expected_no_data", "true", "0")]
    assert query(conn, "SELECT count(*) FROM marts.house_elo_game") == [(0,)]


@pytest.mark.parametrize("outcome", ["failed", "partial", "deferred", "blocked"])
def test_latest_failed_input_blocks_a_pinned_refresh(generation_db, outcome):
    conn, _ = generation_db
    _, plan, run_id = prepare_refresh(conn)
    before = current_generations(conn)
    record_source_failure(conn, outcome)

    generation_id = str(uuid.uuid4())
    with pytest.raises(psycopg2.Error, match="latest|input|source|failed|blocked"):
        finish_refresh(conn, run_id, generation_id)
    conn.rollback()
    assert current_generations(conn) == before
    assert query(
        conn, "SELECT count(*) FROM meta.asset_receipts WHERE generation_id=%s", (generation_id,)
    ) == [(0,)]
    assert plan["source_generation_id"] == before[SOURCE]


@pytest.mark.parametrize("outcome", ["failed", "blocked"])
def test_refresh_failure_receipt_is_mart_only_and_never_repoints(generation_db, outcome):
    conn, _ = generation_db
    _, _, run_id = prepare_refresh(conn)
    before = current_generations(conn)
    generation_id, result = fail_refresh(conn, run_id, outcome=outcome)

    assert result >= 0
    assert current_generations(conn) == before
    assert query(
        conn,
        "SELECT asset_key,outcome,published_at,coverage->>'complete' "
        "FROM meta.asset_receipts WHERE generation_id=%s",
        (generation_id,),
    ) == [(MART, outcome, None, "false")]
    assert query(
        conn,
        "SELECT count(*) FROM meta.asset_receipt_inputs WHERE generation_id=%s",
        (generation_id,),
    ) == [(0,)]
    assert query(
        conn,
        "SELECT operation_kind,outcome FROM meta.operation_runs WHERE operation_run_id=%s",
        (run_id,),
    ) == [("refresh", outcome)]


def test_policy_change_and_stale_source_both_fail_closed(generation_db):
    conn, _ = generation_db
    _, _, run_id = prepare_refresh(conn)
    set_source_policy(conn, "99 years")
    generation_id = str(uuid.uuid4())
    with pytest.raises(psycopg2.Error, match="policy|interval|plan|changed"):
        finish_refresh(conn, run_id, generation_id)
    conn.rollback()
    assert query(
        conn, "SELECT count(*) FROM meta.asset_receipts WHERE generation_id=%s", (generation_id,)
    ) == [(0,)]

    set_source_policy(conn, "1 microsecond")
    sleep(0.01)
    with pytest.raises(psycopg2.Error, match="stale|cadence|interval"):
        get_plan(conn)
    conn.rollback()


def test_two_refreshes_pinned_to_one_mart_generation_have_one_winner(generation_db):
    conn, target = generation_db
    _, plan, _ = prepare_refresh(conn)
    run_ids = [start_refresh(conn, plan), start_refresh(conn, plan)]
    generation_ids = [str(uuid.uuid4()), str(uuid.uuid4())]

    def contender(values):
        other = psycopg2.connect(target)
        try:
            try:
                result = refresher_query(
                    other,
                    "SELECT * FROM warehouse_refresh.publish_house_elo_game_refresh(%s,%s)",
                    values,
                )[0]
                return "ok", result
            except psycopg2.Error as exc:
                other.rollback()
                return "error", str(exc)
        finally:
            other.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(contender, zip(run_ids, generation_ids, strict=True)))
    assert sorted(status for status, _ in results) == ["error", "ok"]
    assert "changed" in next(detail for status, detail in results if status == "error").lower()
    winner = next(result for status, result in results if status == "ok")
    assert current_generations(conn)[MART] == str(winner[0])
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts WHERE asset_key=%s", (MART,)) == [
        (2,)
    ]


def test_exact_replay_never_restores_an_older_mart_pointer(generation_db):
    conn, _ = generation_db
    _, _, first_run = prepare_refresh(conn)
    first_generation, first_result = finish_refresh(conn, first_run)

    second_plan = get_plan(conn)
    second_run = start_refresh(conn, second_plan)
    second_generation, _ = finish_refresh(conn, second_run)
    assert current_generations(conn)[MART] == second_generation

    replay_generation, replay = finish_refresh(conn, first_run, first_generation)
    assert replay_generation == first_generation
    assert str(replay[0]) == str(first_result[0]) == first_generation
    assert replay[1] is True
    assert current_generations(conn)[MART] == second_generation


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE analytics.house_elo_game SET home_postgame_elo=1600",
        "UPDATE analytics.house_elo_current SET rating=1600",
    ],
)
def test_source_dml_queued_during_refresh_invalidates_child_after_commit(generation_db, statement):
    conn, target = generation_db
    _, _, run_id = prepare_refresh(conn)
    generation_id, _ = finish_refresh(conn, run_id, commit=False)
    application_name = "generation-refresh-source-writer"
    started = Event()
    observer = psycopg2.connect(target)

    def source_writer():
        other = psycopg2.connect(target, application_name=application_name)
        try:
            with other.cursor() as cur:
                cur.execute("SET statement_timeout='10s'")
                started.set()
                cur.execute(statement)
            other.commit()
        finally:
            other.close()

    with ThreadPoolExecutor(max_workers=1) as pool:
        try:
            future = pool.submit(source_writer)
            assert started.wait(timeout=5)
            wait_for_lock(observer, application_name)
            conn.commit()
            future.result(timeout=10)
        finally:
            conn.rollback()
            observer.close()

    assert query(
        conn, "SELECT count(*) FROM meta.asset_receipts WHERE generation_id=%s", (generation_id,)
    ) == [(1,)]
    assert current_generations(conn) == {}


def test_source_failure_receipt_waits_until_refresh_admission_commits(generation_db):
    conn, target = generation_db
    _, _, run_id = prepare_refresh(conn)
    failure_run = start_run(conn)
    source_failure, mart_failure = str(uuid.uuid4()), str(uuid.uuid4())
    generation_id, _ = finish_refresh(conn, run_id, commit=False)
    application_name = "generation-refresh-failure-writer"
    started = Event()
    observer = psycopg2.connect(target)

    def failure_writer():
        other = psycopg2.connect(target, application_name=application_name)
        try:
            with other.cursor() as cur:
                cur.execute("SET statement_timeout='10s'")
            started.set()
            publisher_query(
                other,
                "SELECT * FROM warehouse_publication.record_house_elo_failure(%s,%s,%s,%s,%s)",
                (
                    str(failure_run),
                    source_failure,
                    mart_failure,
                    "failed",
                    "publication_failed",
                ),
            )
        finally:
            other.close()

    with ThreadPoolExecutor(max_workers=1) as pool:
        try:
            future = pool.submit(failure_writer)
            assert started.wait(timeout=5)
            wait_for_lock(observer, application_name)
            conn.commit()
            future.result(timeout=10)
        finally:
            conn.rollback()
            observer.close()

    assert current_generations(conn)[MART] == generation_id
    assert query(
        conn,
        "SELECT asset_key,outcome FROM meta.asset_receipts "
        "WHERE generation_id IN (%s,%s) ORDER BY asset_key",
        (source_failure, mart_failure),
    ) == [(SOURCE, "failed"), (MART, "failed")]


def test_source_expiring_while_refresh_waits_cannot_publish(generation_db):
    conn, target = generation_db
    source_args = publication_args(conn)
    publish(conn, source_args)
    set_source_policy(conn, "2 seconds")
    plan = get_plan(conn)
    run_id = start_refresh(conn, plan)
    generation_id = str(uuid.uuid4())
    application_name = "generation-refresh-expiry-waiter"
    started = Event()
    blocker = psycopg2.connect(target)
    observer = psycopg2.connect(target)

    def waiting_refresh():
        other = psycopg2.connect(target, application_name=application_name)
        try:
            with other.cursor() as cur:
                cur.execute("SET statement_timeout='10s'")
            started.set()
            try:
                finish_refresh(other, run_id, generation_id)
                return "ok", None, ""
            except psycopg2.Error as exc:
                other.rollback()
                return "error", exc.pgcode, str(exc)
        finally:
            other.close()

    try:
        with blocker.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW marts.house_elo_game")
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(waiting_refresh)
            try:
                assert started.wait(timeout=5)
                wait_for_lock(observer, application_name)
                sleep(2.1)
                blocker.commit()
                status, pgcode, detail = future.result(timeout=10)
            finally:
                blocker.rollback()
    finally:
        blocker.rollback()
        blocker.close()
        observer.close()

    assert (status, pgcode) == ("error", "55000")
    assert "became stale" in detail.lower()
    assert query(
        conn, "SELECT count(*) FROM meta.asset_receipts WHERE generation_id=%s", (generation_id,)
    ) == [(0,)]
    assert query(
        conn, "SELECT outcome FROM meta.operation_runs WHERE operation_run_id=%s", (run_id,)
    ) == [("running",)]
    assert current_generations(conn) == {SOURCE: source_args[1]}


@pytest.mark.parametrize("change", ["disabled_event_guard", "asset_oid_mismatch"])
def test_invalid_guard_or_registered_oid_blocks_refresh(generation_db, change):
    conn, _ = generation_db
    _, _, run_id = prepare_refresh(conn)
    before = current_generations(conn)
    if change == "disabled_event_guard":
        query(conn, "ALTER EVENT TRIGGER warehouse_publication_invalidate_ddl DISABLE")
    else:
        query(
            conn,
            "UPDATE meta.asset_publication_locks "
            "SET relation_oid='analytics.house_elo_current'::regclass "
            "WHERE asset_key=%s",
            (MART,),
        )

    generation_id = str(uuid.uuid4())
    with pytest.raises(psycopg2.Error, match="guard|identity|invalid"):
        finish_refresh(conn, run_id, generation_id)
    conn.rollback()
    assert current_generations(conn) == before
    assert query(
        conn, "SELECT count(*) FROM meta.asset_receipts WHERE generation_id=%s", (generation_id,)
    ) == [(0,)]


def test_migration_is_idempotent_and_role_has_only_bounded_rpc_access(generation_db):
    conn, _ = generation_db
    query(conn, MIGRATION.read_text())
    assert query(
        conn,
        "SELECT rolcanlogin,rolinherit,rolsuper,rolcreatedb,rolcreaterole,"
        "rolreplication,rolbypassrls FROM pg_roles WHERE rolname='warehouse_refresher'",
    ) == [(False, False, False, False, False, False, False)]
    for relation in (
        "analytics.house_elo_game",
        "analytics.house_elo_current",
        "marts.house_elo_game",
        "meta.asset_receipts",
        "meta.asset_current_generations",
        "meta.asset_receipt_inputs",
        "meta.asset_freshness_policies",
        "meta.operation_runs",
    ):
        assert query(
            conn,
            "SELECT has_table_privilege('warehouse_refresher',%s,"
            "'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')",
            (relation,),
        ) == [(False,)]
    assert query(
        conn,
        "SELECT has_function_privilege('warehouse_refresher',"
        "'warehouse_refresh.require_house_elo_refresh_run(uuid)','EXECUTE'),"
        "has_function_privilege('warehouse_refresher',"
        "'warehouse_quota.start_operation_run(uuid,text,text,jsonb,text,text)','EXECUTE'),"
        "has_function_privilege('warehouse_refresher',"
        "'warehouse_quota.finish_operation_run(uuid,text,text)','EXECUTE')",
    ) == [(False, False, False)]
    for role in (
        "anon",
        "authenticated",
        "analyst_ro",
        "publication_bystander",
        "warehouse_publisher",
    ):
        denied(conn, role, "SELECT warehouse_refresh.get_house_elo_game_plan()")
    denied(conn, "warehouse_refresher", "SELECT * FROM meta.asset_receipts")


@pytest.mark.parametrize("reapply", [False, True])
@pytest.mark.parametrize("direction", ["member", "parent"])
def test_migration_rejects_existing_memberships(request, reapply, direction):
    conn, _ = request.getfixturevalue("_freshness_db")
    if reapply:
        query(conn, MIGRATION.read_text())
    else:
        query(
            conn,
            "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles "
            "WHERE rolname='warehouse_refresher') THEN "
            "CREATE ROLE warehouse_refresher NOLOGIN NOINHERIT; END IF; END $$",
        )
    other_role = "generation_member_" + uuid.uuid4().hex
    query(conn, sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(other_role)))
    granted, member = (
        ("warehouse_refresher", other_role)
        if direction == "member"
        else (other_role, "warehouse_refresher")
    )
    try:
        query(
            conn, sql.SQL("GRANT {} TO {}").format(sql.Identifier(granted), sql.Identifier(member))
        )
        with pytest.raises(psycopg2.Error, match="bounded NOLOGIN role"):
            query(conn, MIGRATION.read_text())
        conn.rollback()
        if not reapply:
            assert query(
                conn, "SELECT to_regprocedure('warehouse_refresh.get_house_elo_game_plan()')"
            ) == [(None,)]
    finally:
        conn.rollback()
        query(conn, sql.SQL("DROP ROLE {}").format(sql.Identifier(other_role)))


def test_reapply_rejects_preexisting_refresher_direct_read_access(generation_db):
    conn, _ = generation_db
    query(conn, "GRANT SELECT ON meta.asset_receipts TO warehouse_refresher")
    with pytest.raises(psycopg2.Error, match="unsafe direct access"):
        query(conn, MIGRATION.read_text())
    conn.rollback()


def test_reapply_rejects_preexisting_publication_rpc_access(generation_db):
    conn, _ = generation_db
    query(conn, "GRANT USAGE ON SCHEMA warehouse_publication TO warehouse_refresher")
    query(
        conn,
        "GRANT EXECUTE ON FUNCTION warehouse_publication.start_house_elo_run(uuid) "
        "TO warehouse_refresher",
    )
    with pytest.raises(psycopg2.Error, match="unrelated quota or publication access"):
        query(conn, MIGRATION.read_text())
    conn.rollback()


def test_reapply_rejects_unexpected_same_name_overload(generation_db):
    conn, _ = generation_db
    query(
        conn,
        "CREATE FUNCTION warehouse_refresh.get_house_elo_game_plan(text) "
        "RETURNS jsonb LANGUAGE sql AS $$ SELECT '{}'::jsonb $$",
    )
    with pytest.raises(psycopg2.Error, match="trusted migration-owned routines namespace"):
        query(conn, MIGRATION.read_text())
    conn.rollback()


def test_start_replay_is_exact_and_other_session_cannot_adopt_run(generation_db):
    conn, target = generation_db
    source_args = publication_args(conn)
    publish(conn, source_args)
    set_source_policy(conn)
    plan = get_plan(conn)
    run_id = str(uuid.uuid4())
    start_refresh(conn, plan, run_id)
    start_refresh(conn, plan, run_id)

    changed_plan = plan | {"source_generation_id": str(uuid.uuid4())}
    with pytest.raises(psycopg2.Error, match="different|invalid|scope|operation"):
        start_refresh(conn, changed_plan, run_id)
    conn.rollback()

    other_role = "generation_caller_" + uuid.uuid4().hex
    query(
        conn, sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(other_role)).as_string(conn)
    )
    query(
        conn,
        sql.SQL("GRANT warehouse_refresher TO {}")
        .format(sql.Identifier(other_role))
        .as_string(conn),
    )
    other = psycopg2.connect(target)
    other_run = str(uuid.uuid4())
    try:
        query(
            other,
            sql.SQL("SET SESSION AUTHORIZATION {}")
            .format(sql.Identifier(other_role))
            .as_string(other),
        )
        start_refresh(other, plan, other_run)
        with pytest.raises(psycopg2.Error, match="belong|session|operation"):
            start_refresh(conn, plan, other_run)
        conn.rollback()
        with pytest.raises(psycopg2.Error, match="belong|session|operation"):
            fail_refresh(conn, other_run)
        conn.rollback()
        assert query(
            conn,
            "SELECT recorded_by,outcome FROM meta.operation_runs WHERE operation_run_id=%s",
            (other_run,),
        ) == [(other_role, "running")]
    finally:
        other.close()
        query(conn, sql.SQL("DROP ROLE {}").format(sql.Identifier(other_role)).as_string(conn))


def test_real_python_adapter_publishes_one_atomic_refresh(generation_db, monkeypatch):
    from scripts import refresh_marts as runner

    conn, target = generation_db
    source_args = publication_args(conn)
    publish(conn, source_args)
    set_source_policy(conn)
    before = current_generations(conn)
    monkeypatch.setattr(runner, "get_db_url", lambda: target)

    assert runner.refresh_marts(views=[MART], require_receipts=True) == 0

    after = current_generations(conn)
    assert after[SOURCE] == before[SOURCE] == source_args[1]
    assert after[MART] != before[MART]
    assert query(
        conn,
        """
        SELECT r.outcome,r.coverage->>'complete',i.input_asset_key,
               i.input_generation_id::text,o.operation_kind,o.outcome
        FROM meta.asset_receipts r
        JOIN meta.asset_receipt_inputs i USING(generation_id)
        JOIN meta.operation_runs o USING(operation_run_id)
        WHERE r.generation_id=%s
        """,
        (after[MART],),
    ) == [("succeeded", "true", SOURCE, before[SOURCE], "refresh", "succeeded")]


def test_reapply_rejects_direct_maintain_refresh_bypass(generation_db):
    conn, _ = generation_db
    query(conn, "GRANT MAINTAIN ON marts.house_elo_game TO warehouse_refresher")
    with pytest.raises(psycopg2.Error, match="unsafe maintenance access"):
        query(conn, MIGRATION.read_text())
    conn.rollback()
