"""Executed F14 quota-ledger behavior on an isolated loopback PostgreSQL database."""

from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic, sleep

import psycopg2
import pytest

from tests.test_warehouse_migrations_sql import query
from tests.test_warehouse_migrations_sql import warehouse_db as _warehouse_db  # noqa: F401

MIGRATION = (
    Path(__file__).resolve().parents[1] / "src/schemas/migrations/066_operational_quota_ledger.sql"
)
ACCOUNT = "fixture-shared-cfbd-cbbd-pool"


@pytest.fixture
def quota_db(request):
    conn, target = request.getfixturevalue("_warehouse_db")
    query(
        conn,
        """
        DO $roles$ BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='anon') THEN
                CREATE ROLE anon NOLOGIN;
            END IF;
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='authenticated') THEN
                CREATE ROLE authenticated NOLOGIN;
            END IF;
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='analyst_ro') THEN
                CREATE ROLE analyst_ro NOLOGIN;
            END IF;
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='quota_bystander') THEN
                CREATE ROLE quota_bystander NOLOGIN;
            END IF;
        END $roles$;
        ALTER DEFAULT PRIVILEGES GRANT ALL ON SCHEMAS
            TO anon, authenticated, analyst_ro, quota_bystander;
        ALTER DEFAULT PRIVILEGES GRANT ALL ON TABLES
            TO anon, authenticated, analyst_ro, quota_bystander;
        ALTER DEFAULT PRIVILEGES GRANT ALL ON FUNCTIONS
            TO anon, authenticated, analyst_ro, quota_bystander;
        """,
    )
    query(conn, MIGRATION.read_text())
    query(conn, MIGRATION.read_text())
    return conn, target


def runtime_query(conn, statement, args=None):
    with conn.cursor() as cur:
        cur.execute("SET LOCAL ROLE warehouse_ingest")
        cur.execute(statement, args)
        result = cur.fetchall() if cur.description else None
    conn.commit()
    return result


def start_run(conn, run_id=None):
    run_id = run_id or uuid.uuid4()
    runtime_query(
        conn,
        """
        SELECT warehouse_quota.start_operation_run(%s, 'extract', 'pytest', %s::jsonb, %s, %s)
        """,
        (str(run_id), '{"sources":["games"]}', "fixture-plan", "fixture-revision"),
    )
    return run_id


def add_period(conn, *, limit=2, start=None, end=None, account=ACCOUNT):
    now = datetime.now(UTC)
    start = start or now - timedelta(hours=1)
    end = end or now + timedelta(hours=1)
    query(
        conn,
        """
        INSERT INTO meta.api_quota_periods
            (account_key, period_start_at, period_end_at, attempt_limit)
        VALUES (%s, %s, %s, %s)
        """,
        (account, start, end, limit),
    )
    return start, end


def reserve(conn, attempt_id, run_id, period_start, *, endpoint="games", context=None):
    context = context or '{"season":2026,"week":1}'
    return runtime_query(
        conn,
        """
        SELECT * FROM warehouse_quota.reserve_cfbd_attempt(
            %s, %s, %s, %s, %s, 0, %s::jsonb
        )
        """,
        (str(attempt_id), str(run_id), ACCOUNT, period_start, endpoint, context),
    )[0]


def test_schema_is_idempotent_private_and_runtime_is_bounded(quota_db):
    conn, _ = quota_db
    role_flags = query(
        conn,
        """
        SELECT rolcanlogin, rolinherit, rolsuper, rolcreatedb, rolcreaterole,
            rolreplication, rolbypassrls
        FROM pg_roles WHERE rolname='warehouse_ingest'
        """,
    )
    assert role_flags == [(False, False, False, False, False, False, False)]

    for role in ("anon", "authenticated", "analyst_ro", "quota_bystander"):
        assert query(
            conn,
            """
            SELECT has_table_privilege(%s, 'meta.operation_runs', 'SELECT,INSERT,UPDATE,DELETE'),
                has_table_privilege(%s, 'meta.api_quota_periods', 'SELECT,INSERT,UPDATE,DELETE'),
                has_table_privilege(%s, 'meta.api_request_attempts', 'SELECT,INSERT,UPDATE,DELETE'),
                has_function_privilege(%s,
                    'warehouse_quota.reserve_cfbd_attempt(uuid,uuid,text,timestamptz,text,integer,jsonb)',
                    'EXECUTE'),
                has_schema_privilege(%s, 'meta', 'CREATE')
            """,
            (role, role, role, role, role),
        ) == [(False, False, False, False, True)]
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL ROLE {role}")
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cur.execute("SELECT * FROM meta.api_quota_periods")
        conn.rollback()

    assert query(
        conn,
        """
        SELECT has_schema_privilege('warehouse_ingest','warehouse_quota','USAGE'),
            has_schema_privilege('warehouse_ingest','warehouse_quota','CREATE'),
            has_table_privilege('warehouse_ingest','meta.api_quota_periods','SELECT'),
            has_function_privilege('warehouse_ingest',
                'warehouse_quota.reserve_cfbd_attempt(uuid,uuid,text,timestamptz,text,integer,jsonb)',
                'EXECUTE')
        """,
    ) == [(True, False, False, True)]
    assert query(
        conn,
        """
        SELECT count(*) FROM pg_proc p
        JOIN pg_namespace n ON n.oid=p.pronamespace
        CROSS JOIN LATERAL aclexplode(
            COALESCE(p.proacl, acldefault('f',p.proowner))
        ) acl
        WHERE n.nspname='warehouse_quota'
          AND p.proname IN (
              'start_operation_run','finish_operation_run','reserve_cfbd_attempt',
              'mark_cfbd_attempt_dispatched','record_cfbd_attempt_result'
          )
          AND acl.grantee=0
        """,
    ) == [(0,)]
    run_id = start_run(conn)
    period_start, _ = add_period(conn)
    reserve(conn, uuid.uuid4(), run_id, period_start)
    with conn.cursor() as cur:
        cur.execute("SET LOCAL ROLE warehouse_ingest")
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("UPDATE meta.api_quota_periods SET attempt_limit=999")
    conn.rollback()


def test_periods_require_finite_nonoverlapping_explicit_bounds(quota_db):
    conn, _ = quota_db
    now = datetime.now(UTC)
    first_start, first_end = add_period(
        conn, start=now - timedelta(hours=2), end=now - timedelta(hours=1)
    )
    # Adjacent half-open periods are valid.
    add_period(conn, start=first_end, end=now + timedelta(hours=1))
    with pytest.raises(psycopg2.errors.ExclusionViolation):
        query(
            conn,
            """
            INSERT INTO meta.api_quota_periods
                (account_key, period_start_at, period_end_at, attempt_limit)
            VALUES (%s, %s, %s, 10)
            """,
            (ACCOUNT, first_start + timedelta(minutes=1), first_end + timedelta(minutes=1)),
        )
    conn.rollback()
    with pytest.raises(psycopg2.errors.CheckViolation):
        query(
            conn,
            """
            INSERT INTO meta.api_quota_periods
                (account_key, period_start_at, period_end_at, attempt_limit)
            VALUES ('infinite-fixture', '-infinity', 'infinity', 10)
            """,
        )
    conn.rollback()

    run_id = start_run(conn)
    with pytest.raises(psycopg2.Error, match="not configured"):
        reserve(conn, uuid.uuid4(), run_id, now, endpoint="unconfigured")
    conn.rollback()
    with pytest.raises(psycopg2.Error, match="not active"):
        reserve(conn, uuid.uuid4(), run_id, first_start, endpoint="expired")
    conn.rollback()


def test_reservation_retry_is_exact_and_does_not_double_charge(quota_db):
    conn, _ = quota_db
    run_id = start_run(conn)
    period_start, _ = add_period(conn)
    attempt_id = uuid.uuid4()
    first = reserve(conn, attempt_id, run_id, period_start)
    retried = reserve(conn, attempt_id, run_id, period_start)
    assert first[2:] == (1, 2, False)
    assert retried[0:4] == first[0:4]
    assert retried[4] is True
    assert query(
        conn,
        "SELECT reserved_attempts FROM meta.api_quota_periods WHERE account_key=%s",
        (ACCOUNT,),
    ) == [(1,)]
    with pytest.raises(psycopg2.Error, match="different context"):
        reserve(conn, attempt_id, run_id, period_start, endpoint="plays")
    conn.rollback()
    assert query(conn, "SELECT count(*) FROM meta.api_request_attempts") == [(1,)]


def test_rollback_is_free_but_committed_crash_reservation_stays_charged(quota_db):
    conn, target = quota_db
    run_id = start_run(conn)
    period_start, _ = add_period(conn)
    rolled_back_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute("SET LOCAL ROLE warehouse_ingest")
        cur.execute(
            "SELECT * FROM warehouse_quota.reserve_cfbd_attempt(%s,%s,%s,%s,'games',0,'{}')",
            (str(rolled_back_id), str(run_id), ACCOUNT, period_start),
        )
    conn.rollback()
    assert query(conn, "SELECT reserved_attempts FROM meta.api_quota_periods") == [(0,)]

    committed_id = uuid.uuid4()
    reserve(conn, committed_id, run_id, period_start)
    observer = psycopg2.connect(target)
    try:
        assert query(
            observer,
            "SELECT state FROM meta.api_request_attempts WHERE attempt_id=%s",
            (str(committed_id),),
        ) == [("reserved",)]
        assert query(observer, "SELECT reserved_attempts FROM meta.api_quota_periods") == [(1,)]
    finally:
        observer.close()

    assert runtime_query(
        conn,
        "SELECT warehouse_quota.finish_operation_run(%s, 'failed', 'fixture crash')",
        (str(run_id),),
    ) == [(1,)]
    with pytest.raises(psycopg2.Error, match="already terminal"):
        reserve(conn, uuid.uuid4(), run_id, period_start)
    conn.rollback()


def test_dispatch_rechecks_period_and_terminal_results_do_not_refund(quota_db):
    conn, _ = quota_db
    run_id = start_run(conn)
    period_start, _ = add_period(conn, limit=3)
    attempt_id = uuid.uuid4()
    reserve(conn, attempt_id, run_id, period_start)
    runtime_query(
        conn,
        "SELECT warehouse_quota.mark_cfbd_attempt_dispatched(%s)",
        (str(attempt_id),),
    )
    with pytest.raises(psycopg2.Error, match="do not send it again"):
        runtime_query(
            conn,
            "SELECT warehouse_quota.mark_cfbd_attempt_dispatched(%s)",
            (str(attempt_id),),
        )
    conn.rollback()
    completed_at = runtime_query(
        conn,
        "SELECT warehouse_quota.record_cfbd_attempt_result(%s,'http_error',429,'rate_limited')",
        (str(attempt_id),),
    )[0][0]
    assert runtime_query(
        conn,
        "SELECT warehouse_quota.record_cfbd_attempt_result(%s,'http_error',429,'rate_limited')",
        (str(attempt_id),),
    ) == [(completed_at,)]
    with pytest.raises(psycopg2.Error, match="different terminal result"):
        runtime_query(
            conn,
            "SELECT warehouse_quota.record_cfbd_attempt_result(%s,'succeeded',200,NULL)",
            (str(attempt_id),),
        )
    conn.rollback()
    success_id = uuid.uuid4()
    reserve(conn, success_id, run_id, period_start)
    runtime_query(
        conn, "SELECT warehouse_quota.mark_cfbd_attempt_dispatched(%s)", (str(success_id),)
    )
    # The persisted invariant also rejects NULL (CHECK otherwise accepts UNKNOWN).
    for invalid_status in (None, 500):
        with pytest.raises(psycopg2.errors.CheckViolation):
            query(
                conn,
                "UPDATE meta.api_request_attempts SET state='succeeded', "
                "result_recorded_at=clock_timestamp(), http_status=%s WHERE attempt_id=%s",
                (invalid_status, str(success_id)),
            )
        conn.rollback()
    for invalid_status in (None, 500):
        with pytest.raises(psycopg2.Error, match="requires 2xx"):
            runtime_query(
                conn,
                "SELECT warehouse_quota.record_cfbd_attempt_result(%s,'succeeded',%s,NULL)",
                (str(success_id), invalid_status),
            )
        conn.rollback()
    runtime_query(
        conn,
        "SELECT warehouse_quota.record_cfbd_attempt_result(%s,'succeeded',200,NULL)",
        (str(success_id),),
    )
    no_data_id = uuid.uuid4()
    reserve(conn, no_data_id, run_id, period_start)
    runtime_query(
        conn, "SELECT warehouse_quota.mark_cfbd_attempt_dispatched(%s)", (str(no_data_id),)
    )
    runtime_query(
        conn,
        "SELECT warehouse_quota.record_cfbd_attempt_result(%s,'expected_no_data',204,NULL)",
        (str(no_data_id),),
    )
    # Re-applying populated schema preserves the charged counter and terminal rows.
    query(conn, MIGRATION.read_text())
    query(conn, MIGRATION.read_text())
    with pytest.raises(psycopg2.Error, match="exhausted"):
        reserve(conn, uuid.uuid4(), run_id, period_start)
    conn.rollback()
    assert query(conn, "SELECT reserved_attempts FROM meta.api_quota_periods") == [(3,)]
    assert query(
        conn, "SELECT state FROM meta.api_request_attempts ORDER BY period_attempt_number"
    ) == [("http_error",), ("succeeded",), ("expected_no_data",)]


def test_expired_period_blocks_dispatch_but_not_unknown_result(quota_db):
    conn, _ = quota_db
    run_id = start_run(conn)
    period_start, _ = add_period(conn)
    attempt_id = uuid.uuid4()
    reserve(conn, attempt_id, run_id, period_start)
    query(
        conn,
        """
        UPDATE meta.api_quota_periods
        SET period_end_at=clock_timestamp() - interval '1 second'
        WHERE account_key=%s AND period_start_at=%s
        """,
        (ACCOUNT, period_start),
    )
    with pytest.raises(psycopg2.Error, match="not active at dispatch"):
        runtime_query(
            conn,
            "SELECT warehouse_quota.mark_cfbd_attempt_dispatched(%s)",
            (str(attempt_id),),
        )
    conn.rollback()
    runtime_query(
        conn,
        "SELECT warehouse_quota.record_cfbd_attempt_result("
        "%s,'unknown',NULL,'dispatch_unconfirmed')",
        (str(attempt_id),),
    )
    assert query(
        conn,
        "SELECT state, reserved_attempts "
        "FROM meta.api_request_attempts CROSS JOIN meta.api_quota_periods",
    ) == [("unknown", 1)]


def test_concurrent_workers_cannot_reserve_above_cap(quota_db):
    conn, target = quota_db
    run_ids = [start_run(conn) for _ in range(8)]
    period_start, _ = add_period(conn, limit=3)

    def contender(number):
        worker = psycopg2.connect(target)
        try:
            try:
                reserve(
                    worker,
                    uuid.uuid5(uuid.NAMESPACE_URL, f"quota-worker-{number}"),
                    run_ids[number],
                    period_start,
                )
                return "reserved"
            except psycopg2.Error as exc:
                worker.rollback()
                assert "exhausted" in str(exc)
                return "exhausted"
        finally:
            worker.close()

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(contender, range(8)))
    assert outcomes.count("reserved") == 3
    assert outcomes.count("exhausted") == 5
    assert query(conn, "SELECT reserved_attempts FROM meta.api_quota_periods") == [(3,)]
    assert query(conn, "SELECT count(*) FROM meta.api_request_attempts") == [(3,)]


@pytest.mark.parametrize("same_context", [False, True])
def test_concurrent_same_uuid_serializes_before_charging_period(quota_db, same_context):
    conn, target = quota_db
    run_id = start_run(conn)
    period_start, _ = add_period(conn, limit=2)
    attempt_id = uuid.uuid4()

    def same_attempt(number):
        worker = psycopg2.connect(target)
        try:
            try:
                result = reserve(
                    worker,
                    attempt_id,
                    run_id,
                    period_start,
                    endpoint=("games" if same_context or number == 0 else "plays"),
                )
                return "reused" if result[4] else "reserved"
            except psycopg2.Error as exc:
                worker.rollback()
                assert "different context" in str(exc)
                return "conflict"
        finally:
            worker.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(same_attempt, range(2)))
    expected = ["reserved", "reused"] if same_context else ["conflict", "reserved"]
    assert sorted(outcomes) == expected
    assert query(conn, "SELECT reserved_attempts FROM meta.api_quota_periods") == [(1,)]
    assert query(conn, "SELECT count(*) FROM meta.api_request_attempts") == [(1,)]


def test_reservation_uses_clock_after_waiting_for_period_lock(quota_db):
    conn, target = quota_db
    run_id = start_run(conn)
    now = datetime.now(UTC)
    period_start, _ = add_period(
        conn,
        start=now - timedelta(seconds=1),
        end=now + timedelta(seconds=3),
    )
    locker = psycopg2.connect(target)
    waiter = psycopg2.connect(target)
    try:
        with locker.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM meta.api_quota_periods
                WHERE account_key=%s AND period_start_at=%s FOR UPDATE
                """,
                (ACCOUNT, period_start),
            )

        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(reserve, waiter, uuid.uuid4(), run_id, period_start)
            deadline = monotonic() + 2
            while monotonic() < deadline:
                waiting = query(
                    conn,
                    "SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s",
                    (waiter.get_backend_pid(),),
                )
                if waiting == [("Lock",)]:
                    break
                sleep(0.01)
            else:
                locker.rollback()
                pytest.fail("reservation did not block on the held period lock")
            remaining = (now + timedelta(seconds=3) - datetime.now(UTC)).total_seconds()
            if remaining <= 0:
                locker.rollback()
                pytest.fail("waiter must start before expiry to test a fresh wall clock")
            sleep(remaining + 0.05)
            locker.commit()
            with pytest.raises(psycopg2.Error, match="not active"):
                pending.result(timeout=5)
        waiter.rollback()
    finally:
        locker.close()
        waiter.close()
    assert query(conn, "SELECT reserved_attempts FROM meta.api_quota_periods") == [(0,)]


def test_ordinary_default_public_function_execution_is_revoked(request):
    conn, _ = request.getfixturevalue("_warehouse_db")
    query(
        conn,
        """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='quota_bystander') THEN
                CREATE ROLE quota_bystander NOLOGIN;
            END IF;
        END $$;
        CREATE FUNCTION public.quota_public_control() RETURNS integer
            LANGUAGE sql AS 'SELECT 1';
        """,
    )
    assert query(
        conn,
        "SELECT proacl IS NULL FROM pg_proc "
        "WHERE oid='public.quota_public_control()'::regprocedure",
    ) == [(True,)]
    query(conn, MIGRATION.read_text())
    query(conn, "GRANT USAGE ON SCHEMA warehouse_quota TO quota_bystander")
    assert query(
        conn,
        "SELECT count(*), bool_and(NOT has_function_privilege('quota_bystander', "
        "p.oid, 'EXECUTE')) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE n.nspname='warehouse_quota'",
    ) == [(7, True)]
    with conn.cursor() as cur:
        cur.execute("SET LOCAL ROLE quota_bystander")
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute(
                "SELECT warehouse_quota.start_operation_run(%s,'extract','fixture','{}',NULL,NULL)",
                (str(uuid.uuid4()),),
            )
    conn.rollback()
    assert query(
        conn,
        "SELECT has_function_privilege('quota_bystander', "
        "'public.quota_public_control()', 'EXECUTE')",
    ) == [(True,)]


def test_meta_overload_cannot_capture_private_quota_rpc(quota_db):
    conn, _ = quota_db
    with conn.cursor() as cur:
        cur.execute("SET LOCAL ROLE quota_bystander")
        cur.execute("""
            CREATE FUNCTION meta.reserve_cfbd_attempt(text,uuid,text,timestamptz,text,integer,jsonb)
            RETURNS integer LANGUAGE sql AS 'SELECT -1'
        """)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SET LOCAL ROLE quota_bystander")
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("""
                CREATE FUNCTION warehouse_quota.reserve_cfbd_attempt(
                    text,uuid,text,timestamptz,text,integer,jsonb
                ) RETURNS integer LANGUAGE sql AS 'SELECT -1'
            """)
    conn.rollback()
    run_id = start_run(conn)
    period_start, _ = add_period(conn)
    result = reserve(conn, uuid.uuid4(), run_id, period_start)
    assert result[2] == 1
    assert query(conn, "SELECT reserved_attempts FROM meta.api_quota_periods") == [(1,)]


def test_transport_and_unknown_results_cannot_claim_http_responses(quota_db):
    conn, _ = quota_db
    run_id = start_run(conn)
    period_start, _ = add_period(conn, limit=3)
    for dispatched in (False, True):
        attempt_id = uuid.uuid4()
        reserve(conn, attempt_id, run_id, period_start)
        if dispatched:
            runtime_query(
                conn, "SELECT warehouse_quota.mark_cfbd_attempt_dispatched(%s)", (str(attempt_id),)
            )
        correct = "response_unobserved" if dispatched else "dispatch_unconfirmed"
        wrong = "dispatch_unconfirmed" if dispatched else "response_unobserved"
        for state, status, category in (
            ("unknown", 500, correct),
            ("unknown", None, wrong),
            ("unknown", None, None),
            ("transport_error", 429, "timeout"),
        ):
            with pytest.raises(psycopg2.Error):
                runtime_query(
                    conn,
                    "SELECT warehouse_quota.record_cfbd_attempt_result(%s,%s,%s,%s)",
                    (str(attempt_id), state, status, category),
                )
            conn.rollback()
            with pytest.raises(psycopg2.Error):
                query(
                    conn,
                    "UPDATE meta.api_request_attempts SET state=%s, http_status=%s, "
                    "error_category=%s, result_recorded_at=clock_timestamp() WHERE attempt_id=%s",
                    (state, status, category, str(attempt_id)),
                )
            conn.rollback()
        runtime_query(
            conn,
            "SELECT warehouse_quota.record_cfbd_attempt_result(%s,'unknown',NULL,%s)",
            (str(attempt_id), correct),
        )
    attempt_id = uuid.uuid4()
    reserve(conn, attempt_id, run_id, period_start)
    runtime_query(
        conn, "SELECT warehouse_quota.mark_cfbd_attempt_dispatched(%s)", (str(attempt_id),)
    )
    runtime_query(
        conn,
        "SELECT warehouse_quota.record_cfbd_attempt_result(%s,'transport_error',NULL,'timeout')",
        (str(attempt_id),),
    )
    assert query(
        conn,
        "SELECT state,http_status FROM meta.api_request_attempts ORDER BY period_attempt_number",
    ) == [("unknown", None), ("unknown", None), ("transport_error", None)]


@pytest.mark.parametrize(
    "setup, message",
    [
        ("ALTER SCHEMA warehouse_quota OWNER TO quota_bystander", "owned by the migration role"),
        ("GRANT CREATE ON SCHEMA warehouse_quota TO quota_bystander", "untrusted CREATE"),
        (
            "CREATE DOMAIN warehouse_quota.mark_cfbd_attempt_dispatched AS uuid; "
            "ALTER DOMAIN warehouse_quota.mark_cfbd_attempt_dispatched OWNER TO quota_bystander",
            "unexpected type",
        ),
        (
            "CREATE FUNCTION warehouse_quota.mark_cfbd_attempt_dispatched(uuid) "
            "RETURNS boolean LANGUAGE sql AS 'SELECT true'; "
            "ALTER FUNCTION warehouse_quota.mark_cfbd_attempt_dispatched(uuid) "
            "OWNER TO quota_bystander",
            "unexpected or untrusted routine",
        ),
        (
            "CREATE FUNCTION warehouse_quota.mark_cfbd_attempt_dispatched(text) "
            "RETURNS boolean LANGUAGE sql AS 'SELECT true'",
            "unexpected or untrusted routine",
        ),
        (
            "CREATE FUNCTION warehouse_quota.mark_cfbd_attempt_dispatched(uuid, text DEFAULT '') "
            "RETURNS boolean LANGUAGE sql AS 'SELECT true'",
            "unexpected or untrusted routine",
        ),
        (
            "CREATE FUNCTION warehouse_quota.mark_cfbd_attempt_dispatched(VARIADIC text[]) "
            "RETURNS boolean LANGUAGE sql AS 'SELECT true'",
            "unexpected or untrusted routine",
        ),
    ],
)
def test_untrusted_existing_namespace_is_rejected_without_adoption(request, setup, message):
    conn, _ = request.getfixturevalue("_warehouse_db")
    query(
        conn,
        "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='quota_bystander') "
        "THEN CREATE ROLE quota_bystander NOLOGIN; END IF; END $$; "
        "CREATE SCHEMA warehouse_quota; " + setup,
    )
    snapshot_sql = """
        SELECT n.nspowner, n.nspacl::text, p.oid, p.proowner, p.proacl::text,
            t.oid, t.typowner
        FROM pg_namespace n LEFT JOIN pg_proc p ON p.pronamespace=n.oid
        LEFT JOIN pg_type t ON t.typnamespace=n.oid
        WHERE n.nspname='warehouse_quota' ORDER BY p.oid, t.oid
    """
    before = query(conn, snapshot_sql)
    with pytest.raises(psycopg2.Error, match=message):
        query(conn, MIGRATION.read_text())
    conn.rollback()
    assert query(conn, snapshot_sql) == before
    assert query(conn, "SELECT to_regclass('meta.api_request_attempts')") == [(None,)]
