"""Executed class-separated quota admission and transport transactions."""

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg2
import pytest

from tests.test_operational_quota_sql import ACCOUNT, add_period, reserve, runtime_query, start_run
from tests.test_operational_quota_sql import quota_db as _quota_db  # noqa: F401
from tests.test_warehouse_migrations_sql import query
from tests.test_warehouse_migrations_sql import warehouse_db as _warehouse_db  # noqa: F401

MIGRATION = (
    Path(__file__).resolve().parents[1] / "src/schemas/migrations/067_cfbd_transport_admission.sql"
)


@pytest.fixture
def transport_db(request):
    conn, target = request.getfixturevalue("_quota_db")
    query(conn, MIGRATION.read_text())
    query(conn, MIGRATION.read_text())
    return conn, target


def control_period(conn, limit=2):
    now = datetime.now(UTC)
    query(
        conn,
        "INSERT INTO meta.api_quota_periods "
        "(account_key,budget_class,period_start_at,period_end_at,attempt_limit) "
        "VALUES (%s,'reconciliation',%s,%s,%s)",
        (ACCOUNT, now - timedelta(minutes=1), now + timedelta(minutes=10), limit),
    )


def transport_reserve(conn, run, endpoint, attempt=None):
    return runtime_query(
        conn,
        "SELECT * FROM warehouse_quota.reserve_cfbd_transport_attempt(%s,%s,%s,%s,0,'{}')",
        (str(attempt or uuid.uuid4()), str(run), ACCOUNT, endpoint),
    )[0]


def test_exhausted_extraction_still_allows_bounded_control(transport_db):
    conn, _ = transport_db
    run = start_run(conn)
    add_period(conn, limit=1)
    control_period(conn, limit=2)
    transport_reserve(conn, run, "/games")
    with pytest.raises(psycopg2.Error, match="exhausted"):
        transport_reserve(conn, run, "/scoreboard")
    conn.rollback()
    for endpoint in ("/info", "/info/usage"):
        attempt = transport_reserve(conn, run, endpoint)[0]
        runtime_query(conn, "SELECT warehouse_quota.mark_cfbd_attempt_dispatched(%s)", (attempt,))
        runtime_query(
            conn,
            "SELECT warehouse_quota.record_cfbd_attempt_result(%s,'succeeded',200,NULL)",
            (attempt,),
        )
    with pytest.raises(psycopg2.Error, match="exhausted"):
        transport_reserve(conn, run, "/info")
    conn.rollback()
    assert query(
        conn, "SELECT budget_class,reserved_attempts FROM meta.api_quota_periods ORDER BY 1"
    ) == [("extraction", 1), ("reconciliation", 2)]


def test_control_is_exact_and_cannot_be_selected_via_legacy_rpc(transport_db):
    conn, _ = transport_db
    run = start_run(conn)
    control_period(conn)
    for endpoint in (
        "/info/other",
        "/scoreboard",
        "/INFO",
        "/info?x=1",
        "https://evil/info",
        "/info/",
    ):
        with pytest.raises(psycopg2.Error):
            transport_reserve(conn, run, endpoint)
        conn.rollback()
    start = query(conn, "SELECT period_start_at FROM meta.api_quota_periods")[0][0]
    with pytest.raises(psycopg2.Error, match="not configured"):
        reserve(conn, uuid.uuid4(), run, start, endpoint="/info")
    conn.rollback()
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        runtime_query(
            conn,
            "SELECT * FROM warehouse_quota.reserve_cfbd_budget_attempt("
            "%s,%s,%s,%s,'/info',0,'{}','reconciliation')",
            (str(uuid.uuid4()), str(run), ACCOUNT, start),
        )
    conn.rollback()
    assert query(conn, "SELECT reserved_attempts FROM meta.api_quota_periods") == [(0,)]


def test_existing_extraction_history_survives_upgrade(request):
    conn, _ = request.getfixturevalue("_quota_db")
    run = start_run(conn)
    start, _ = add_period(conn)
    attempt = uuid.uuid4()
    original = reserve(conn, attempt, run, start)
    query(conn, MIGRATION.read_text())
    query(conn, MIGRATION.read_text())
    assert reserve(conn, attempt, run, start)[:4] == original[:4]
    runtime_query(conn, "SELECT warehouse_quota.mark_cfbd_attempt_dispatched(%s)", (str(attempt),))
    assert query(conn, "SELECT budget_class,state FROM meta.api_request_attempts") == [
        ("extraction", "dispatched")
    ]
    with pytest.raises(psycopg2.Error, match="immutable"):
        query(conn, "UPDATE meta.api_request_attempts SET budget_class='reconciliation'")
    conn.rollback()


def test_control_concurrency_cannot_exceed_cap(transport_db):
    conn, target = transport_db
    runs = [start_run(conn) for _ in range(6)]
    control_period(conn, limit=2)

    def worker(run):
        with psycopg2.connect(target) as other:
            try:
                transport_reserve(other, run, "/info")
                return True
            except psycopg2.Error as e:
                assert "exhausted" in str(e)
                return False

    with ThreadPoolExecutor(max_workers=6) as pool:
        assert sum(pool.map(worker, runs)) == 2
    assert query(conn, "SELECT reserved_attempts FROM meta.api_quota_periods") == [(2,)]


def test_control_replay_context_and_expiry(transport_db):
    conn, _ = transport_db
    run = start_run(conn)
    control_period(conn)
    attempt = uuid.uuid4()
    first = transport_reserve(conn, run, "/info", attempt)
    assert transport_reserve(conn, run, "/info", attempt)[4] is True
    with pytest.raises(psycopg2.Error, match="different context"):
        transport_reserve(conn, run, "/info/usage", attempt)
    conn.rollback()
    query(
        conn,
        "UPDATE meta.api_quota_periods SET period_end_at=clock_timestamp()-interval '1 second'",
    )
    assert transport_reserve(conn, run, "/info", attempt)[:4] == first[:4]
    with pytest.raises(psycopg2.Error, match="not active at dispatch"):
        runtime_query(
            conn, "SELECT warehouse_quota.mark_cfbd_attempt_dispatched(%s)", (str(attempt),)
        )
    conn.rollback()


@pytest.mark.parametrize("expected_empty", [False, True])
def test_real_adapter_commits_parent_attempt_and_response(
    transport_db, monkeypatch, expected_empty
):
    import httpx

    from src.pipelines.utils.api_client import CFBDClient
    from src.pipelines.utils.quota_admission import quota_operation

    conn, target = transport_db
    add_period(conn, limit=2)
    monkeypatch.setenv("CFBD_QUOTA_MODE", "durable")
    monkeypatch.setenv("CFBD_QUOTA_DB_URL", target)
    monkeypatch.setenv("CFBD_QUOTA_ACCOUNT", ACCOUNT)
    observed = []

    def http_send(endpoint, params):
        observed.append(
            query(conn, "SELECT state FROM meta.api_request_attempts ORDER BY reserved_at")
        )
        assert query(conn, "SELECT outcome FROM meta.operation_runs") == [("running",)]
        return httpx.Response(
            200, json=[], request=httpx.Request("GET", "https://api.collegefootballdata.com/games")
        )

    with quota_operation("sql-test", {}) as operation:
        client = CFBDClient(api_key="fixture")
        monkeypatch.setattr(client._client, "get", http_send)
        try:
            assert client.get("/scoreboard", expected_empty=expected_empty) == []
        finally:
            client.close()
    assert observed == [[("dispatched",)]]
    assert query(
        conn,
        "SELECT outcome FROM meta.operation_runs WHERE operation_run_id=%s",
        (operation.run_id,),
    ) == [("succeeded",)]
    assert query(conn, "SELECT state,http_status FROM meta.api_request_attempts") == [
        ("expected_no_data" if expected_empty else "succeeded", 200)
    ]


def test_same_operation_can_reconcile_after_extraction_denial(transport_db, monkeypatch):
    import httpx

    from src.pipelines.utils.api_client import CFBDClient
    from src.pipelines.utils.quota_admission import QuotaDeniedError, quota_operation

    conn, target = transport_db
    add_period(conn, limit=1)
    control_period(conn, limit=1)
    monkeypatch.setenv("CFBD_QUOTA_MODE", "durable")
    monkeypatch.setenv("CFBD_QUOTA_DB_URL", target)
    monkeypatch.setenv("CFBD_QUOTA_ACCOUNT", ACCOUNT)
    sends = []
    with quota_operation("test", {}):
        with CFBDClient(api_key="fixture") as client:

            def send(endpoint, params):
                sends.append(endpoint)
                return httpx.Response(200, json=[], request=httpx.Request("GET", "https://fixture"))

            monkeypatch.setattr(client._client, "get", send)
            client.get("/games")
            with pytest.raises(QuotaDeniedError):
                client.get("/games")
            client.get("/info")
    assert sends == ["/games", "/info"]
    assert query(
        conn, "SELECT budget_class,reserved_attempts FROM meta.api_quota_periods ORDER BY 1"
    ) == [("extraction", 1), ("reconciliation", 1)]


def test_fresh_process_observes_prior_usage(transport_db):
    import subprocess
    import sys

    conn, target = transport_db
    add_period(conn, limit=1)
    code = """
import httpx
from src.pipelines.utils.api_client import CFBDClient
from src.pipelines.utils.quota_admission import quota_operation, QuotaAdmissionError
try:
    with quota_operation("fresh-process", {}):
        with CFBDClient(api_key="fixture") as client:
            def send(*args, **kwargs):
                print("SENT")
                return httpx.Response(200,json=[],request=httpx.Request("GET","https://fixture"))
            client._client.get = send
            client.get("/games")
except QuotaAdmissionError:
    print("DENIED")
"""
    import os

    env = dict(
        os.environ, CFBD_QUOTA_MODE="durable", CFBD_QUOTA_DB_URL=target, CFBD_QUOTA_ACCOUNT=ACCOUNT
    )
    first = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True, check=True
    )
    second = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True, check=True
    )
    assert first.stdout.strip() == "SENT"
    assert second.stdout.strip() == "DENIED"
    assert query(conn, "SELECT reserved_attempts FROM meta.api_quota_periods") == [(1,)]


def test_new_rpc_default_grants_are_private(transport_db):
    conn, _ = transport_db
    for role in ("anon", "authenticated", "analyst_ro", "quota_bystander"):
        assert query(
            conn,
            "SELECT bool_and(NOT has_function_privilege(%s,p.oid,'EXECUTE')) "
            "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname='warehouse_quota'",
            (role,),
        ) == [(True,)]
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        runtime_query(conn, "UPDATE meta.api_quota_periods SET attempt_limit=100")
    conn.rollback()


def test_new_attempt_uses_next_window_while_old_charge_remains(transport_db):
    conn, _ = transport_db
    run = start_run(conn)
    control_period(conn, limit=1)
    first = transport_reserve(conn, run, "/info")
    query(
        conn,
        "UPDATE meta.api_quota_periods SET period_end_at=clock_timestamp()-interval '1 second'",
    )
    next_start = query(conn, "SELECT period_end_at FROM meta.api_quota_periods")[0][0]
    query(
        conn,
        "INSERT INTO meta.api_quota_periods "
        "(account_key,budget_class,period_start_at,period_end_at,attempt_limit) "
        "VALUES (%s,'reconciliation',%s,clock_timestamp()+interval '10 minutes',1)",
        (ACCOUNT, next_start),
    )
    second = transport_reserve(conn, run, "/info")
    assert second[0] != first[0]
    assert second[2] == first[2] == 1
    assert query(
        conn, "SELECT reserved_attempts FROM meta.api_quota_periods ORDER BY period_start_at"
    ) == [(1,), (1,)]
