"""Transport ordering, fail-closed boundaries and enrolled CLI lifecycles."""

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from unittest.mock import Mock

import httpx
import pytest

from src.pipelines.utils import api_client
from src.pipelines.utils import quota_admission as quota


class RecordingOperation(quota.QuotaOperation):
    def __init__(self):
        super().__init__("unused", "fixture", "test", {})
        self.calls = []

    def _call(self, statement, args=(), **kwargs):
        self.calls.append((statement, args))
        return []


@pytest.fixture
def admitted(monkeypatch):
    operation = RecordingOperation()
    monkeypatch.setattr(quota, "_ACTIVE", operation)
    monkeypatch.setattr(api_client, "_CONTROL_BREAKER", api_client.RateLimitBreaker())
    monkeypatch.setattr(api_client.time, "sleep", lambda _: None)
    with api_client.CFBDClient(api_key="fixture") as client:
        yield operation, client


def response(status):
    return httpx.Response(status, json=[], request=httpx.Request("GET", "https://fixture/games"))


def test_each_retry_is_reserved_dispatched_and_recorded(admitted, monkeypatch):
    operation, client = admitted
    sends = []
    responses = iter([response(429), response(503), httpx.ConnectError("fixture"), response(200)])

    def send(*args, **kwargs):
        assert "mark_cfbd_attempt_dispatched" in operation.calls[-1][0]
        sends.append(operation.calls[-1][1][0])
        item = next(responses)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(client._client, "get", send)
    assert client.get("/games") == []
    assert len(set(sends)) == 4
    reservations = [a for s, a in operation.calls if "reserve_cfbd_transport" in s]
    results = [a for s, a in operation.calls if "record_cfbd_attempt_result" in s]
    assert [a[4] for a in reservations] == [0, 1, 2, 3]
    assert [(a[1], a[2]) for a in results] == [
        ("http_error", 429),
        ("http_error", 503),
        ("transport_error", None),
        ("succeeded", 200),
    ]


@pytest.mark.parametrize("status", [401, 403, 404])
def test_nonretryable_response_has_one_terminal_record(admitted, monkeypatch, status):
    operation, client = admitted
    send = Mock(return_value=response(status))
    monkeypatch.setattr(client._client, "get", send)
    with pytest.raises(httpx.HTTPStatusError):
        client.get("/games")
    assert send.call_count == 1
    assert operation.calls[-1][1][1:3] == ("http_error", status)


@pytest.mark.parametrize("boundary", ["reserve", "dispatch", "result"])
def test_accounting_failure_stops_send_and_retry(admitted, monkeypatch, boundary):
    operation, client = admitted

    def failed(*args):
        operation._unavailable.set()
        raise quota.QuotaAdmissionError("failed")

    monkeypatch.setattr(operation, boundary, failed)
    send = Mock(return_value=response(503))
    monkeypatch.setattr(client._client, "get", send)
    with pytest.raises(quota.QuotaAdmissionError):
        client.get("/games")
    assert send.call_count == (1 if boundary == "result" else 0)
    with pytest.raises(quota.QuotaAdmissionError):
        client.get("/info")
    assert send.call_count == (1 if boundary == "result" else 0)


def test_control_does_not_share_extraction_circuit(admitted, monkeypatch):
    operation, client = admitted
    for _ in range(client._breaker.threshold):
        client._breaker.record_rate_limited()
    monkeypatch.setattr(client._client, "get", Mock(return_value=response(200)))
    with pytest.raises(api_client.RateLimitCircuitOpen):
        client.get("/games")
    assert not operation.calls
    assert client.get("/info") == []
    assert client._breaker.is_open()


def test_concurrent_thread_cannot_send_after_result_failure(admitted, monkeypatch):
    operation, client = admitted
    entered = threading.Event()
    release = threading.Event()
    second_started = threading.Event()
    send = Mock(return_value=response(200))
    monkeypatch.setattr(client._client, "get", send)

    def result(*args):
        entered.set()
        assert release.wait(5)
        operation._unavailable.set()
        raise quota.QuotaAdmissionError("write failed")

    monkeypatch.setattr(operation, "result", result)

    def second():
        second_started.set()
        return client.get("/games")

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(client.get, "/games")
        assert entered.wait(5)
        other = pool.submit(second)
        assert second_started.wait(5)
        release.set()
        for future in (first, other):
            with pytest.raises(quota.QuotaAdmissionError):
                future.result(timeout=5)
    assert send.call_count == 1


def test_database_failure_is_sanitized_and_latched(monkeypatch):
    monkeypatch.setattr(quota.psycopg2, "connect", Mock(side_effect=RuntimeError("SECRET DSN")))
    operation = quota.QuotaOperation("SECRET DSN", "fixture", "test", {})
    with pytest.raises(quota.QuotaAdmissionError) as error:
        operation.start()
    assert "SECRET" not in str(error.value)
    assert operation._unavailable.is_set()
    assert operation.outcome == "failed"


def test_uncertain_commit_never_allows_http(monkeypatch):
    connection = Mock()
    connection.cursor.return_value.__enter__ = Mock(return_value=Mock())
    connection.cursor.return_value.__exit__ = Mock(return_value=False)
    connection.commit.side_effect = RuntimeError("commit response lost")
    monkeypatch.setattr(quota.psycopg2, "connect", Mock(return_value=connection))
    operation = quota.QuotaOperation("fixture", "fixture", "test", {})
    with pytest.raises(quota.QuotaAdmissionError):
        operation.reserve("/games", 0, {})
    assert operation._unavailable.is_set()
    assert connection.close.called


def test_enrollment_requires_configuration_and_parent_commit(monkeypatch):
    monkeypatch.setenv("CFBD_QUOTA_MODE", "durable")
    monkeypatch.delenv("CFBD_QUOTA_DB_URL", raising=False)
    with pytest.raises(quota.QuotaAdmissionError):
        with quota.quota_operation("test", {}):
            pytest.fail("must not enter")
    monkeypatch.setenv("CFBD_QUOTA_DB_URL", "fixture")
    monkeypatch.setenv("CFBD_QUOTA_ACCOUNT", "fixture")
    calls = []

    def call(self, statement, args=(), **kwargs):
        if "start_operation" in statement:
            assert quota.active_operation() is None
        calls.append((statement, args))

    monkeypatch.setattr(quota.QuotaOperation, "_call", call)
    with quota.quota_operation("test", {}) as operation:
        assert quota.active_operation() is operation
        assert "start_operation" in calls[0][0]
    assert quota.active_operation() is None
    assert calls[-1][1][1] == "succeeded"


def test_durable_client_without_enrollment_denies_http(monkeypatch):
    monkeypatch.setenv("CFBD_QUOTA_MODE", "durable")
    with api_client.CFBDClient(api_key="fixture") as client:
        send = Mock()
        monkeypatch.setattr(client._client, "get", send)
        with pytest.raises(quota.QuotaAdmissionError):
            client.get("/games")
        send.assert_not_called()


def test_enrolled_make_request_ignores_json_gate(admitted, monkeypatch):
    from src.pipelines.sources import base

    _, client = admitted
    monkeypatch.setattr(base, "get_rate_limiter", Mock(side_effect=AssertionError("JSON gate")))
    monkeypatch.setattr(client._client, "get", Mock(return_value=response(200)))
    assert base.make_request(client, "/games") == []


def test_load_main_records_returned_errors(monkeypatch):
    from scripts import load_season

    operation = RecordingOperation()

    @contextmanager
    def context(*args, **kwargs):
        yield operation

    monkeypatch.setattr(quota, "quota_operation", context)
    monkeypatch.setattr(load_season, "load_season", Mock(return_value={"errors": 1}))
    monkeypatch.setattr("sys.argv", ["load_season", "--season", "2026"])
    with pytest.raises(SystemExit) as error:
        load_season.main()
    assert error.value.code == 1
    assert operation.outcome == "failed"


def test_live_dry_run_still_enrolls_transport(monkeypatch):
    from scripts import poll_scoreboard

    seen = []

    @contextmanager
    def context(*args, **kwargs):
        seen.append((args, kwargs))
        yield RecordingOperation()

    monkeypatch.setattr(quota, "quota_operation", context)
    client = Mock()
    client.get.return_value = []
    monkeypatch.setattr(poll_scoreboard, "get_client", Mock(return_value=client))
    monkeypatch.setattr("sys.argv", ["poll_scoreboard", "--dry-run"])
    poll_scoreboard.main()
    assert seen[0][0][0] == "poll_scoreboard"
    assert seen[0][1].get("enabled", True)
    client.close.assert_called_once()


@pytest.mark.parametrize(
    "status,outcome,exit_code",
    [("ok", "partial", 0), ("complete", "succeeded", 0), ("finalize_failed", "failed", 1)],
)
def test_historical_summary_preserves_run_outcome(monkeypatch, status, outcome, exit_code):
    from scripts import backfill_refresh

    operation = RecordingOperation()

    @contextmanager
    def context(*args, **kwargs):
        yield operation

    monkeypatch.setattr(quota, "quota_operation", context)
    monkeypatch.setattr(backfill_refresh, "get_db_url", lambda: "fixture")
    monkeypatch.setattr(quota.psycopg2, "connect", Mock(return_value=Mock()))
    monkeypatch.setattr(backfill_refresh, "run_campaign", Mock(return_value={"status": status}))
    monkeypatch.setattr("sys.argv", ["backfill_refresh", "--campaign", "fixture"])
    with pytest.raises(SystemExit) as error:
        backfill_refresh.main()
    assert error.value.code == exit_code
    assert operation.outcome == outcome


@pytest.mark.parametrize("network", [False, True])
def test_exhausted_transient_failure_cannot_leave_successful_run(admitted, monkeypatch, network):
    operation, client = admitted
    monkeypatch.setattr(client, "TRANSIENT_MAX_RETRIES", 0)
    send = (
        Mock(side_effect=httpx.ConnectError("fixture"))
        if network
        else Mock(return_value=response(503))
    )
    monkeypatch.setattr(client._client, "get", send)
    with pytest.raises((httpx.HTTPStatusError, httpx.RequestError)):
        client.get("/games")
    assert operation.outcome == "partial"
