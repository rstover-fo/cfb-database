"""Session limits, statistics, and cancellation for mart refresh."""

import logging
import signal
import time
from unittest.mock import MagicMock

import psycopg2
import pytest

from scripts import refresh_marts as runner
from src.pipelines.config.refresh_assets import RefreshAsset
from src.pipelines.utils.refresh_plan import RefreshGraph


def reverse_registered_graph():
    return RefreshGraph(
        [
            RefreshAsset("marts.leaf", ("marts.middle",), "test"),
            RefreshAsset("marts.middle", ("marts.parent",), "test"),
            RefreshAsset("marts.parent", ("core.input",), "test"),
            RefreshAsset("marts.independent", (), "test"),
        ],
        ["core.input", "core.unused"],
    )


def _patch_db(monkeypatch, connection):
    monkeypatch.setattr(runner, "REFRESH_GRAPH", reverse_registered_graph())
    monkeypatch.setattr(runner, "get_db_url", lambda: "postgresql://test")
    monkeypatch.setattr(psycopg2, "connect", lambda _url: connection)


def _session_calls(connection):
    cursor = connection.cursor.return_value.__enter__.return_value
    calls = []

    def execute(sql, params=None):
        calls.append((sql, params))

    cursor.execute.side_effect = execute
    cursor.fetchall.return_value = []
    return cursor, calls


@pytest.mark.parametrize(
    ("statement_timeout_seconds", "lock_timeout_seconds"),
    [(2700, 300), (10, 20)],
)
def test_session_timeouts_use_configured_milliseconds(
    monkeypatch, statement_timeout_seconds, lock_timeout_seconds
):
    connection = MagicMock()
    _patch_db(monkeypatch, connection)
    _cursor, calls = _session_calls(connection)
    monkeypatch.setattr(runner, "refresh_view", lambda *args, **kwargs: True)

    assert (
        runner.refresh_marts(
            views=["marts.independent"],
            statement_timeout_seconds=statement_timeout_seconds,
            lock_timeout_seconds=lock_timeout_seconds,
        )
        == 0
    )

    timeout_calls = [(sql, params) for sql, params in calls if "set_config" in sql]
    assert timeout_calls == [
        (
            "SELECT set_config('statement_timeout', %s, false)",
            (str(statement_timeout_seconds * 1000),),
        ),
        (
            "SELECT set_config('lock_timeout', %s, false)",
            (str(lock_timeout_seconds * 1000),),
        ),
    ]
    assert not any("statement_timeout = 0" in sql for sql, _params in calls)
    connection.close.assert_called_once()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"statement_timeout_seconds": 0},
        {"statement_timeout_seconds": -1},
        {"lock_timeout_seconds": 0},
        {"lock_timeout_seconds": -5},
    ],
)
def test_non_positive_timeouts_are_rejected(monkeypatch, kwargs):
    monkeypatch.setattr(
        runner,
        "get_db_url",
        lambda: pytest.fail("rejected timeouts must not resolve a database"),
    )
    with pytest.raises(ValueError):
        runner.refresh_marts(**kwargs)


def test_successful_refresh_analyzes_the_view():
    connection = MagicMock()
    calls = []
    connection.cursor.return_value.execute.side_effect = lambda sql, params=None: calls.append(sql)

    assert runner.refresh_view("marts.parent", connection, True, False) is True

    assert calls == [
        "REFRESH MATERIALIZED VIEW CONCURRENTLY marts.parent",
        "ANALYZE marts.parent",
    ]
    assert connection.commit.call_count == 2
    connection.rollback.assert_not_called()
    connection.cursor.return_value.close.assert_called_once()


def test_failed_refresh_does_not_analyze():
    connection = MagicMock()
    calls = []

    def execute(sql, params=None):
        calls.append(sql)
        raise RuntimeError("refresh failed")

    connection.cursor.return_value.execute.side_effect = execute

    assert runner.refresh_view("marts.parent", connection, True, False) is False

    assert calls == ["REFRESH MATERIALIZED VIEW CONCURRENTLY marts.parent"]
    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()
    connection.cursor.return_value.close.assert_called_once()


def test_analyze_failure_after_refresh_still_succeeds(caplog):
    connection = MagicMock()
    calls = []

    def execute(sql, params=None):
        calls.append(sql)
        if sql.startswith("ANALYZE"):
            raise RuntimeError("analyze failed")

    connection.cursor.return_value.execute.side_effect = execute

    with caplog.at_level(logging.WARNING):
        assert runner.refresh_view("marts.parent", connection, True, False) is True

    assert "ANALYZE marts.parent" in calls
    assert "analyze failed" in caplog.text
    connection.commit.assert_called_once()
    connection.rollback.assert_called_once()


def test_external_ancestors_are_analyzed_before_refresh(monkeypatch):
    connection = MagicMock()
    _patch_db(monkeypatch, connection)
    _cursor, calls = _session_calls(connection)
    order = []

    def execute(sql, params=None):
        calls.append((sql, params))
        order.append(sql)

    _cursor.execute.side_effect = execute

    def refresh(name, *_args, **_kwargs):
        order.append(f"refresh {name}")
        return True

    monkeypatch.setattr(runner, "refresh_view", refresh)

    assert runner.refresh_marts(views=["marts.leaf", "marts.independent"]) == 0

    analyze_sql = [sql for sql, _params in calls if sql.startswith("ANALYZE")]
    assert analyze_sql == ["ANALYZE core.input"]
    assert order.index("ANALYZE core.input") < order.index("refresh marts.leaf")
    assert "ANALYZE core.unused" not in order
    assert "ANALYZE marts.parent" not in order


def test_source_analyze_failure_still_refreshes(monkeypatch, caplog):
    connection = MagicMock()
    _patch_db(monkeypatch, connection)
    cursor, _calls = _session_calls(connection)
    refreshed = []

    def execute(sql, params=None):
        if isinstance(sql, str) and sql.startswith("ANALYZE"):
            raise RuntimeError("source analyze failed")

    cursor.execute.side_effect = execute

    def refresh(name, *_args, **_kwargs):
        refreshed.append(name)
        return True

    monkeypatch.setattr(runner, "refresh_view", refresh)

    with caplog.at_level(logging.WARNING):
        assert runner.refresh_marts(views=["marts.leaf"]) == 0

    assert refreshed == ["marts.leaf"]
    assert "source analyze failed" in caplog.text


def test_preflight_logs_existing_refresh_sessions(monkeypatch, caplog):
    connection = MagicMock()
    _patch_db(monkeypatch, connection)
    cursor, calls = _session_calls(connection)

    def execute(sql, params=None):
        calls.append((sql, params))
        if "pg_stat_activity" in sql:
            cursor.fetchall.return_value = [
                (
                    4242,
                    "active",
                    "1:00:00",
                    "REFRESH MATERIALIZED VIEW CONCURRENTLY marts.play_epa",
                )
            ]

    cursor.execute.side_effect = execute
    monkeypatch.setattr(runner, "refresh_view", lambda *args, **kwargs: True)

    with caplog.at_level(logging.WARNING):
        assert runner.refresh_marts(views=["marts.independent"]) == 0

    preflight = next(sql for sql, _params in calls if "pg_stat_activity" in sql)
    assert "pid" in preflight
    assert "now() - query_start" in preflight
    assert "left(query, 100)" in preflight
    assert "REFRESH MATERIALIZED VIEW%" in preflight
    assert "pg_backend_pid()" in preflight
    # An idle session's pg_stat_activity.query is its LAST statement; only a
    # genuinely running REFRESH may be reported.
    assert "state = 'active'" in preflight
    assert "pg_cancel_backend" not in preflight
    assert "pg_terminate_backend" not in preflight
    assert "4242" in caplog.text
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY marts.play_epa" in caplog.text


def test_preflight_failure_does_not_fail_the_run(monkeypatch, caplog):
    connection = MagicMock()
    _patch_db(monkeypatch, connection)
    cursor, _calls = _session_calls(connection)

    def execute(sql, params=None):
        if "pg_stat_activity" in sql:
            raise RuntimeError("permission denied")

    cursor.execute.side_effect = execute
    monkeypatch.setattr(runner, "refresh_view", lambda *args, **kwargs: True)

    with caplog.at_level(logging.WARNING):
        assert runner.refresh_marts(views=["marts.independent"]) == 0

    assert "permission denied" in caplog.text


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_termination_signal_cancels_and_counts_remaining_views(monkeypatch, caplog, signum):
    connection = MagicMock()
    _patch_db(monkeypatch, connection)
    cursor, _calls = _session_calls(connection)
    refreshed = []
    previous = signal.getsignal(signum)

    def execute(sql, params=None):
        if "statement_timeout" in sql:
            signal.raise_signal(signum)

    cursor.execute.side_effect = execute

    def refresh(name, *_args, **_kwargs):
        refreshed.append(name)
        return True

    monkeypatch.setattr(runner, "refresh_view", refresh)

    with caplog.at_level(logging.WARNING):
        assert runner.refresh_marts() == 4

    assert refreshed == []
    connection.cancel.assert_called()
    assert "not refreshed" in caplog.text
    assert signal.getsignal(signum) == previous


def test_sigterm_during_refresh_cancels_the_statement_and_stops(monkeypatch, caplog):
    connection = MagicMock()
    _patch_db(monkeypatch, connection)
    _session_calls(connection)
    refresh_sql = []

    def execute(sql, params=None):
        refresh_sql.append(sql)
        if isinstance(sql, str) and sql.startswith("REFRESH"):
            signal.raise_signal(signal.SIGTERM)
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                if connection.cancel.called:
                    raise psycopg2.extensions.QueryCanceledError(
                        "canceling statement due to user request"
                    )
                time.sleep(0.01)
            raise AssertionError("in-flight refresh was not cancelled")

    connection.cursor.return_value.execute.side_effect = execute

    with caplog.at_level(logging.INFO):
        assert runner.refresh_marts() == 4

    assert refresh_sql == ["REFRESH MATERIALIZED VIEW CONCURRENTLY marts.parent"]
    connection.cancel.assert_called()
    assert "not refreshed" in caplog.text
    assert "marts.parent failed" in caplog.text


def test_sigterm_during_source_analyze_cancels_and_skips_remaining(monkeypatch, caplog):
    connection = MagicMock()
    two_source_graph = RefreshGraph(
        [RefreshAsset("marts.only", ("core.a", "core.b"), "test")],
        ["core.a", "core.b"],
    )
    monkeypatch.setattr(runner, "REFRESH_GRAPH", two_source_graph)
    monkeypatch.setattr(runner, "get_db_url", lambda: "postgresql://test")
    monkeypatch.setattr(psycopg2, "connect", lambda _url: connection)
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = []
    analyze_sql = []

    def execute(sql, params=None):
        if isinstance(sql, str) and sql.startswith("ANALYZE"):
            analyze_sql.append(sql)
            signal.raise_signal(signal.SIGTERM)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if connection.cancel.called:
                    raise psycopg2.extensions.QueryCanceledError(
                        "canceling statement due to user request"
                    )
                time.sleep(0.01)
            raise AssertionError("in-flight source ANALYZE was not cancelled")

    cursor.execute.side_effect = execute
    monkeypatch.setattr(
        runner,
        "refresh_view",
        lambda *_args, **_kwargs: pytest.fail("must not refresh after termination"),
    )

    with caplog.at_level(logging.WARNING):
        assert runner.refresh_marts() == 1

    assert analyze_sql == ["ANALYZE core.a"]
    connection.cancel.assert_called()
    assert "next relation was core.b" in caplog.text
    assert "not refreshed" in caplog.text


def test_dry_run_prints_analyze_and_does_not_connect(monkeypatch, capsys):
    monkeypatch.setattr(runner, "REFRESH_GRAPH", reverse_registered_graph())

    def forbidden():
        pytest.fail("dry-run must not connect")

    monkeypatch.setattr(runner, "get_db_url", forbidden)
    monkeypatch.setattr(psycopg2, "connect", lambda _url: pytest.fail("dry-run must not connect"))

    assert runner.refresh_marts(views=["marts.leaf", "marts.independent"], dry_run=True) == 0

    output = capsys.readouterr().out
    assert "ANALYZE core.input;" in output
    assert "ANALYZE core.unused" not in output
    leaf_refresh = output.index("REFRESH MATERIALIZED VIEW CONCURRENTLY marts.leaf;")
    assert output.index("ANALYZE core.input;") < leaf_refresh
    assert leaf_refresh < output.index("ANALYZE marts.leaf;")
    assert "ANALYZE marts.independent;" in output
    independent_refresh = output.index("REFRESH MATERIALIZED VIEW CONCURRENTLY marts.independent;")
    assert independent_refresh < output.index("ANALYZE marts.independent;")


def test_receipt_dry_run_does_not_print_analyze(monkeypatch, capsys):
    monkeypatch.setattr(runner, "get_db_url", lambda: pytest.fail("dry-run must not connect"))

    assert (
        runner.refresh_marts(views=["marts.house_elo_game"], require_receipts=True, dry_run=True)
        == 0
    )

    assert "ANALYZE" not in capsys.readouterr().out


def test_cli_timeout_flags_reach_refresh_marts(monkeypatch):
    captured = {}

    def fake_refresh(**kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(runner, "refresh_marts", fake_refresh)
    monkeypatch.setattr(
        "sys.argv",
        [
            "refresh_marts.py",
            "--statement-timeout-seconds",
            "15",
            "--lock-timeout-seconds",
            "25",
            "--dry-run",
        ],
    )

    with pytest.raises(SystemExit) as exit_info:
        runner.main()

    assert exit_info.value.code == 0
    assert captured["statement_timeout_seconds"] == 15
    assert captured["lock_timeout_seconds"] == 25
    assert captured["dry_run"] is True


def test_cli_timeout_defaults_reach_refresh_marts(monkeypatch):
    captured = {}

    def fake_refresh(**kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(runner, "refresh_marts", fake_refresh)
    monkeypatch.setattr("sys.argv", ["refresh_marts.py", "--dry-run"])

    with pytest.raises(SystemExit) as exit_info:
        runner.main()

    assert exit_info.value.code == 0
    assert captured["statement_timeout_seconds"] == 2700
    assert captured["lock_timeout_seconds"] == 300


@pytest.mark.parametrize(
    ("flag", "value"),
    [
        ("--statement-timeout-seconds", "0"),
        ("--statement-timeout-seconds", "-5"),
        ("--lock-timeout-seconds", "0"),
        ("--lock-timeout-seconds", "nope"),
    ],
)
def test_cli_rejects_non_positive_timeouts(monkeypatch, flag, value):
    monkeypatch.setattr("sys.argv", ["refresh_marts.py", flag, value])

    with pytest.raises(SystemExit) as exit_info:
        runner.main()

    assert exit_info.value.code == 2
