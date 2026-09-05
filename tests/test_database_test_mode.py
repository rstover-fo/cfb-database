"""Regression tests for explicit live-database test opt-in."""

import sys
from unittest.mock import Mock

import pytest

conftest = sys.modules["tests.conftest"]


def _pytest_config(*, live_db: bool) -> Mock:
    config = Mock()
    config.getoption.return_value = live_db
    return config


def test_db_conn_skips_before_loading_credentials_by_default(monkeypatch):
    load_dsn = Mock(side_effect=AssertionError("credentials must remain unread"))
    connect = Mock(side_effect=AssertionError("database connection must remain unopened"))
    monkeypatch.setattr(conftest, "_load_postgres_dsn", load_dsn)
    monkeypatch.setattr(conftest.psycopg2, "connect", connect)

    fixture = conftest.db_conn.__wrapped__(_pytest_config(live_db=False))

    with pytest.raises(pytest.skip.Exception, match="require --live-db"):
        next(fixture)
    load_dsn.assert_not_called()
    connect.assert_not_called()


def test_db_conn_opt_in_preserves_connection_lifecycle(monkeypatch):
    load_dsn = Mock(return_value="postgresql://configured-database")
    connection = Mock()
    connect = Mock(return_value=connection)
    monkeypatch.setattr(conftest, "_load_postgres_dsn", load_dsn)
    monkeypatch.setattr(conftest.psycopg2, "connect", connect)

    fixture = conftest.db_conn.__wrapped__(_pytest_config(live_db=True))

    assert next(fixture) is connection
    assert connection.autocommit is True
    load_dsn.assert_called_once_with()
    connect.assert_called_once_with("postgresql://configured-database")

    with pytest.raises(StopIteration):
        next(fixture)
    connection.close.assert_called_once_with()


def test_db_conn_opt_in_without_credentials_still_skips(monkeypatch, tmp_path):
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monkeypatch.setattr(conftest, "PROJECT_ROOT", tmp_path)

    fixture = conftest.db_conn.__wrapped__(_pytest_config(live_db=True))

    with pytest.raises(pytest.skip.Exception, match="Secrets file not found"):
        next(fixture)
