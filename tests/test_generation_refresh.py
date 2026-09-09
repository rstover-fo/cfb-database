"""Focused tests for the opt-in receipt-required mart refresh path."""

from unittest.mock import MagicMock

import psycopg2
import pytest

from scripts import refresh_marts as runner


class _SqlError(RuntimeError):
    def __init__(self, message: str, pgcode: str | None = None):
        super().__init__(message)
        self.pgcode = pgcode


class _Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.connection.events.append(("execute", " ".join(sql.split()), params))
        if "get_house_elo_game_plan" in sql:
            if self.connection.plan_error is not None:
                raise self.connection.plan_error
            self.result = (self.connection.plan,)
        elif "start_house_elo_game_refresh" in sql:
            self.result = (params[0],)
        elif "publish_house_elo_game_refresh" in sql:
            if self.connection.publish_error is not None:
                raise self.connection.publish_error
            self.result = (params[1], False, 42)
        elif "fail_house_elo_game_refresh" in sql:
            self.result = (1,)

    def fetchone(self):
        result, self.result = self.result, None
        return result


class _Connection:
    def __init__(
        self,
        *,
        plan=None,
        plan_error: BaseException | None = None,
        publish_error: BaseException | None = None,
        fail_on_commit: int | None = None,
        commit_error: BaseException | None = None,
    ):
        self.plan = plan if plan is not None else {"opaque": {"generation": "source-a"}}
        self.plan_error = plan_error
        self.publish_error = publish_error
        self.fail_on_commit = fail_on_commit
        self.commit_error = commit_error or ConnectionError("commit acknowledgement lost")
        self.events = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.commits += 1
        self.events.append(("commit", self.commits))
        if self.commits == self.fail_on_commit:
            raise self.commit_error

    def rollback(self):
        self.rollbacks += 1
        self.events.append(("rollback", self.rollbacks))

    def close(self):
        self.closed = True


def _run(monkeypatch, connection, **kwargs):
    monkeypatch.setattr(runner, "get_db_url", lambda: "postgresql://test")
    monkeypatch.setattr(psycopg2, "connect", lambda _url: connection)
    return runner.refresh_marts(views=["marts.house_elo_game"], require_receipts=True, **kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"views": ["marts.house_elo", "marts.house_elo_game"]},
        {"changed": ["analytics.house_elo_game"]},
    ],
)
def test_receipt_mode_rejects_full_mixed_and_changed_plans_before_database(monkeypatch, kwargs):
    monkeypatch.setattr(
        runner,
        "get_db_url",
        lambda: pytest.fail("invalid receipt plans must not resolve database credentials"),
    )
    assert runner.refresh_marts(require_receipts=True, **kwargs) == 1


def test_receipt_dry_run_is_offline_and_does_not_claim_live_validation(monkeypatch, capsys):
    monkeypatch.setattr(
        runner,
        "get_db_url",
        lambda: pytest.fail("dry-run must remain offline"),
    )

    assert (
        runner.refresh_marts(views=["marts.house_elo_game"], require_receipts=True, dry_run=True)
        == 0
    )

    output = capsys.readouterr().out
    assert "receipt-required ordinary refresh" in output
    assert "analytics.house_elo_game/source-wide" in output
    assert "get_house_elo_game_plan" in output
    assert "fail_house_elo_game_refresh" in output
    assert "start_house_elo_game_refresh" in output
    assert "publish_house_elo_game_refresh" in output
    assert "Live generation, cadence, and staleness validation is not run" in output
    assert "CONCURRENTLY" not in output


def test_legacy_refresh_path_is_unchanged(monkeypatch):
    connection = MagicMock()
    monkeypatch.setattr(runner, "get_db_url", lambda: "postgresql://test")
    monkeypatch.setattr(psycopg2, "connect", lambda _url: connection)
    calls = []

    def refresh_view(name, conn, concurrently, dry_run):
        calls.append((name, conn, concurrently, dry_run))
        return True

    monkeypatch.setattr(runner, "refresh_view", refresh_view)

    assert runner.refresh_marts(views=["marts.house_elo_game"]) == 0
    assert calls == [("marts.house_elo_game", connection, True, False)]
    setup_cursor = connection.cursor.return_value.__enter__.return_value
    executed = [call.args[0] for call in setup_cursor.execute.call_args_list]
    assert not any("warehouse_refresh" in sql for sql in executed)
    connection.close.assert_called_once()


def test_plan_is_opaque_and_start_commits_before_publication(monkeypatch):
    plan = {"opaque": ["source-generation", {"policy": "3 days"}]}
    connection = _Connection(plan=plan)

    assert _run(monkeypatch, connection) == 0

    start_index = next(
        index
        for index, event in enumerate(connection.events)
        if event[0] == "execute" and "start_house_elo_game_refresh" in event[1]
    )
    publish_index = next(
        index
        for index, event in enumerate(connection.events)
        if event[0] == "execute" and "publish_house_elo_game_refresh" in event[1]
    )
    start_params = connection.events[start_index][2]
    assert start_params[1].adapted is plan
    assert any(event[0] == "commit" for event in connection.events[start_index:publish_index])
    assert connection.commits == 3  # session setup, durable operation start, publication
    assert connection.rollbacks == 0
    assert connection.closed


@pytest.mark.parametrize("pgcode", ["40001", "55000"])
def test_definite_generation_or_readiness_failure_records_blocked(monkeypatch, pgcode):
    connection = _Connection(publish_error=_SqlError("input changed", pgcode))

    assert _run(monkeypatch, connection) == 1

    failures = [
        event
        for event in connection.events
        if event[0] == "execute" and "fail_house_elo_game_refresh" in event[1]
    ]
    assert len(failures) == 1
    assert failures[0][2][2] == "blocked"
    assert connection.rollbacks == 1
    assert connection.commits == 3  # setup, durable operation start, blocked receipt


def test_definite_refresh_failure_records_failed(monkeypatch):
    connection = _Connection(publish_error=_SqlError("refresh failed", "XX000"))

    assert _run(monkeypatch, connection) == 1

    failure = next(
        event
        for event in connection.events
        if event[0] == "execute" and "fail_house_elo_game_refresh" in event[1]
    )
    assert failure[2][2] == "failed"


def test_keyboard_interrupt_during_publication_records_failure_then_reraises(monkeypatch):
    connection = _Connection(publish_error=KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt):
        _run(monkeypatch, connection)

    failures = [
        event
        for event in connection.events
        if event[0] == "execute" and "fail_house_elo_game_refresh" in event[1]
    ]
    assert len(failures) == 1
    assert failures[0][2][2] == "failed"
    assert connection.rollbacks == 1
    assert connection.commits == 3  # setup, durable operation start, failure receipt
    assert connection.closed


def test_unknown_publication_commit_never_records_failure_or_retries(monkeypatch, caplog):
    connection = _Connection(fail_on_commit=3)

    assert _run(monkeypatch, connection) == 1

    statements = [event[1] for event in connection.events if event[0] == "execute"]
    assert sum("publish_house_elo_game_refresh" in sql for sql in statements) == 1
    assert not any("fail_house_elo_game_refresh" in sql for sql in statements)
    assert "unknown commit outcome" in caplog.text


def test_preplan_failure_does_not_start_or_record_a_receipt(monkeypatch):
    connection = _Connection(plan_error=_SqlError("source cadence is undeclared", "55000"))

    assert _run(monkeypatch, connection) == 1

    statements = [event[1] for event in connection.events if event[0] == "execute"]
    assert any("get_house_elo_game_plan" in sql for sql in statements)
    assert not any("start_house_elo_game_refresh" in sql for sql in statements)
    assert not any("fail_house_elo_game_refresh" in sql for sql in statements)


def test_unknown_operation_start_commit_stops_before_publication(monkeypatch, caplog):
    connection = _Connection(fail_on_commit=2)
    assert _run(monkeypatch, connection) == 1
    statements = [event[1] for event in connection.events if event[0] == "execute"]
    assert sum("start_house_elo_game_refresh" in statement for statement in statements) == 1
    assert not any("publish_house_elo_game_refresh" in statement for statement in statements)
    assert not any("fail_house_elo_game_refresh" in statement for statement in statements)
    assert "operation start has an unknown commit outcome" in caplog.text
    assert connection.closed


@pytest.mark.parametrize("commit_number", [2, 3])
def test_keyboard_interrupt_during_uncertain_commit_never_records_failure(
    monkeypatch, commit_number
):
    connection = _Connection(
        fail_on_commit=commit_number,
        commit_error=KeyboardInterrupt(),
    )

    with pytest.raises(KeyboardInterrupt):
        _run(monkeypatch, connection)

    statements = [event[1] for event in connection.events if event[0] == "execute"]
    assert sum("start_house_elo_game_refresh" in statement for statement in statements) == 1
    assert sum("publish_house_elo_game_refresh" in statement for statement in statements) == (
        commit_number == 3
    )
    assert not any("fail_house_elo_game_refresh" in statement for statement in statements)
    assert connection.closed


def test_keyboard_interrupt_before_start_reraises_without_failure_receipt(monkeypatch):
    connection = _Connection(plan_error=KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt):
        _run(monkeypatch, connection)

    statements = [event[1] for event in connection.events if event[0] == "execute"]
    assert any("get_house_elo_game_plan" in statement for statement in statements)
    assert not any("start_house_elo_game_refresh" in statement for statement in statements)
    assert not any("fail_house_elo_game_refresh" in statement for statement in statements)
    assert connection.closed
