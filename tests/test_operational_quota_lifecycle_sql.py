"""Executed lifecycle invariants for the private F14 quota ledger."""

from __future__ import annotations

import uuid

import psycopg2
import pytest

from tests.test_operational_quota_sql import add_period, reserve, runtime_query, start_run
from tests.test_operational_quota_sql import quota_db as _quota_db_fixture  # noqa: F401
from tests.test_warehouse_migrations_sql import query
from tests.test_warehouse_migrations_sql import warehouse_db as _warehouse_db  # noqa: F401


@pytest.fixture(name="quota_db")
def _lifecycle_quota_db(request):
    return request.getfixturevalue("_quota_db_fixture")


def test_operation_run_uuid_replay_requires_exact_context(quota_db):
    conn, _ = quota_db
    run_id = start_run(conn)

    replayed = runtime_query(
        conn,
        """
        SELECT warehouse_quota.start_operation_run(
            %s, 'extract', 'pytest', '{"sources":["games"]}'::jsonb,
            'fixture-plan', 'fixture-revision'
        )
        """,
        (str(run_id),),
    )
    assert str(replayed[0][0]) == str(run_id)
    assert query(
        conn,
        "SELECT count(*) FROM meta.operation_runs WHERE operation_run_id=%s",
        (str(run_id),),
    ) == [(1,)]

    with pytest.raises(psycopg2.Error, match="different context"):
        runtime_query(
            conn,
            """
            SELECT warehouse_quota.start_operation_run(
                %s, 'extract', 'pytest', '{"sources":["plays"]}'::jsonb,
                'fixture-plan', 'fixture-revision'
            )
            """,
            (str(run_id),),
        )
    conn.rollback()
    assert query(
        conn,
        "SELECT requested_scope FROM meta.operation_runs WHERE operation_run_id=%s",
        (str(run_id),),
    ) == [({"sources": ["games"]},)]


def test_success_finish_waits_for_reserved_and_dispatched_attempts(quota_db):
    conn, _ = quota_db
    run_id = start_run(conn)
    period_start, _ = add_period(conn, limit=2)
    reserved_id = uuid.uuid4()
    dispatched_id = uuid.uuid4()
    reserve(conn, reserved_id, run_id, period_start)
    reserve(conn, dispatched_id, run_id, period_start)
    runtime_query(
        conn,
        "SELECT warehouse_quota.mark_cfbd_attempt_dispatched(%s)",
        (str(dispatched_id),),
    )

    with pytest.raises(psycopg2.Error, match="cannot succeed operation with 2 pending API attempt"):
        runtime_query(
            conn,
            "SELECT warehouse_quota.finish_operation_run(%s, 'succeeded', NULL)",
            (str(run_id),),
        )
    conn.rollback()

    runtime_query(
        conn,
        """
        SELECT warehouse_quota.record_cfbd_attempt_result(
            %s, 'unknown', NULL, 'dispatch_unconfirmed'
        )
        """,
        (str(reserved_id),),
    )
    runtime_query(
        conn,
        """
        SELECT warehouse_quota.record_cfbd_attempt_result(
            %s, 'succeeded', 200, NULL
        )
        """,
        (str(dispatched_id),),
    )
    assert runtime_query(
        conn,
        "SELECT warehouse_quota.finish_operation_run(%s, 'succeeded', NULL)",
        (str(run_id),),
    ) == [(0,)]
    assert query(
        conn,
        """
        SELECT outcome,
            ARRAY(
                SELECT state FROM meta.api_request_attempts
                WHERE operation_run_id=%s ORDER BY state
            )
        FROM meta.operation_runs WHERE operation_run_id=%s
        """,
        (str(run_id), str(run_id)),
    ) == [("succeeded", ["succeeded", "unknown"])]


def test_terminal_operation_context_and_result_cannot_change(quota_db):
    conn, _ = quota_db
    run_id = start_run(conn)
    assert runtime_query(
        conn,
        "SELECT warehouse_quota.finish_operation_run(%s, 'failed', 'fixture failure')",
        (str(run_id),),
    ) == [(0,)]
    original = query(
        conn,
        """
        SELECT outcome, error_summary, finished_at
        FROM meta.operation_runs WHERE operation_run_id=%s
        """,
        (str(run_id),),
    )

    assert runtime_query(
        conn,
        "SELECT warehouse_quota.finish_operation_run(%s, 'failed', 'fixture failure')",
        (str(run_id),),
    ) == [(0,)]
    with pytest.raises(psycopg2.Error, match="different result"):
        runtime_query(
            conn,
            "SELECT warehouse_quota.finish_operation_run(%s, 'blocked', 'fixture failure')",
            (str(run_id),),
        )
    conn.rollback()
    with pytest.raises(psycopg2.Error, match="different context"):
        runtime_query(
            conn,
            """
            SELECT warehouse_quota.start_operation_run(
                %s, 'extract', 'changed-initiator',
                '{"sources":["games"]}'::jsonb, 'fixture-plan', 'fixture-revision'
            )
            """,
            (str(run_id),),
        )
    conn.rollback()
    assert (
        query(
            conn,
            """
        SELECT outcome, error_summary, finished_at
        FROM meta.operation_runs WHERE operation_run_id=%s
        """,
            (str(run_id),),
        )
        == original
    )


def test_owner_update_cannot_change_reservation_identity(quota_db):
    conn, _ = quota_db
    run_id = start_run(conn)
    period_start, _ = add_period(conn)
    attempt_id = uuid.uuid4()
    reserve(conn, attempt_id, run_id, period_start)
    assert query(
        conn,
        """
        SELECT current_user = pg_catalog.pg_get_userbyid(c.relowner)
        FROM pg_catalog.pg_class AS c
        WHERE c.oid='meta.api_request_attempts'::regclass
        """,
    ) == [(True,)]
    original = query(
        conn,
        """
        SELECT endpoint_class, retry_ordinal, request_context, reserved_at
        FROM meta.api_request_attempts WHERE attempt_id=%s
        """,
        (str(attempt_id),),
    )

    with pytest.raises(psycopg2.Error, match="reservation identity is immutable"):
        query(
            conn,
            "UPDATE meta.api_request_attempts SET endpoint_class='plays' WHERE attempt_id=%s",
            (str(attempt_id),),
        )
    conn.rollback()
    assert (
        query(
            conn,
            """
        SELECT endpoint_class, retry_ordinal, request_context, reserved_at
        FROM meta.api_request_attempts WHERE attempt_id=%s
        """,
            (str(attempt_id),),
        )
        == original
    )


def test_owner_cannot_delete_or_truncate_attempt_history(quota_db):
    conn, _ = quota_db
    run_id = start_run(conn)
    period_start, _ = add_period(conn)
    attempt_id = uuid.uuid4()
    reserve(conn, attempt_id, run_id, period_start)

    statements = (
        ("DELETE FROM meta.api_request_attempts WHERE attempt_id=%s", (str(attempt_id),)),
        ("DELETE FROM meta.api_request_attempts WHERE false", None),
        ("TRUNCATE meta.api_request_attempts", None),
    )
    for statement, args in statements:
        with pytest.raises(psycopg2.Error, match="append-only and cannot be removed"):
            query(conn, statement, args)
        conn.rollback()
        assert query(conn, "SELECT count(*) FROM meta.api_request_attempts") == [(1,)]


def test_invalid_attempt_states_are_rejected_without_mutation(quota_db):
    conn, _ = quota_db
    run_id = start_run(conn)
    period_start, _ = add_period(conn)
    attempt_id = uuid.uuid4()
    reserve(conn, attempt_id, run_id, period_start)

    with pytest.raises(psycopg2.Error, match="Invalid API attempt state transition"):
        query(
            conn,
            """
            UPDATE meta.api_request_attempts
            SET state='succeeded', dispatched_at=clock_timestamp(),
                result_recorded_at=clock_timestamp(), http_status=200
            WHERE attempt_id=%s
            """,
            (str(attempt_id),),
        )
    conn.rollback()
    with pytest.raises(psycopg2.Error, match="terminal attempt state is invalid"):
        runtime_query(
            conn,
            """
            SELECT warehouse_quota.record_cfbd_attempt_result(
                %s, 'invalid-state', NULL, NULL
            )
            """,
            (str(attempt_id),),
        )
    conn.rollback()
    assert query(
        conn,
        """
        SELECT state, dispatched_at, result_recorded_at, http_status, error_category
        FROM meta.api_request_attempts WHERE attempt_id=%s
        """,
        (str(attempt_id),),
    ) == [("reserved", None, None, None, None)]
