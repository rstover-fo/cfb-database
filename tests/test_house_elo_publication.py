"""Opt-in Elo producer ordering and commit-uncertainty behavior."""

from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from scripts import compute_house_elo as elo


class Connection:
    def __init__(self, *, fail_publish=False, fail_commit=False):
        self.events = []
        self.commits = 0
        self.fail_publish = fail_publish
        self.fail_commit = fail_commit
        self.publication = None

    def cursor(self):
        return Cursor(self)

    def commit(self):
        self.commits += 1
        self.events.append(("commit", self.commits))
        if self.fail_commit and self.commits == 3:
            raise OSError("sensitive connection details")

    def rollback(self):
        self.events.append(("rollback", None))


class Cursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, statement, args=None):
        self.conn.events.append((statement, args))
        if "publish_house_elo(" in statement:
            self.conn.publication = args
            if self.conn.fail_publish:
                raise ValueError("sensitive server error")

    def fetchone(self):
        return (None, None, "fixture-digest")


@pytest.fixture
def inputs(monkeypatch):
    game = {
        "id": 1,
        "season": 2026,
        "week": 1,
        "season_type": "regular",
        "start_date": datetime(2026, 9, 1, tzinfo=UTC),
        "neutral_site": False,
        "home_team": "A",
        "away_team": "B",
        "home_points": 28,
        "away_points": 14,
        "home_pregame_elo": None,
        "away_pregame_elo": None,
    }
    monkeypatch.setattr(elo, "fetch_max_season", lambda conn: 2026)
    monkeypatch.setattr(elo, "load_games_by_season", lambda *args: {2026: [game]})
    monkeypatch.setattr(elo, "fetch_scheduled_counts", lambda *args: {2026: {"A": 12, "B": 12}})
    return game


def test_full_publication_matches_legacy_engine_and_snapshot(inputs, monkeypatch):
    legacy_seasons, legacy_snapshots = [], []
    monkeypatch.setattr(elo, "write_season", lambda conn, season, rows: legacy_seasons.extend(rows))
    monkeypatch.setattr(elo, "write_snapshot", lambda conn, rows: legacy_snapshots.extend(rows))
    expected = elo.run_full(None, 1869, 2026)
    conn = Connection()
    assert elo.run_full_published(conn) == expected == legacy_seasons
    args = conn.publication
    assert args[6].adapted == expected
    assert args[7].adapted == legacy_snapshots
    assert b"2026-09-01T00:00:00+00:00" in args[6].getquoted()
    statements = [event[0] for event in conn.events]
    repeatable = statements.index("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    fresh = statements.index("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
    assert "commit" in statements[repeatable:fresh]
    assert conn.commits == 3
    assert not any("record_house_elo_failure" in str(statement) for statement in statements)


def test_known_failure_rolls_back_then_records_failure(inputs):
    conn = Connection(fail_publish=True)
    with pytest.raises(elo.PublicationError, match="publication failed") as error:
        elo.run_full_published(conn)
    assert "sensitive" not in str(error.value)
    statements = [event[0] for event in conn.events]
    failure = next(
        i for i, statement in enumerate(statements) if "record_house_elo_failure" in statement
    )
    assert statements.index("rollback") < failure
    assert conn.commits == 3


def test_lost_commit_acknowledgement_never_records_contradictory_failure(inputs):
    conn = Connection(fail_commit=True)
    with pytest.raises(elo.PublicationError, match="inspect receipt IDs") as error:
        elo.run_full_published(conn)
    assert "sensitive" not in str(error.value)
    assert not any("record_house_elo_failure" in str(event) for event in conn.events)
    assert conn.commits == 3


def test_empty_full_input_publishes_explicit_empty_replacement(inputs, monkeypatch):
    monkeypatch.setattr(elo, "fetch_max_season", lambda conn: None)
    monkeypatch.setattr(elo, "load_games_by_season", lambda *args: {})
    conn = Connection()
    assert elo.run_full_published(conn) == []
    assert conn.publication[6].adapted == conn.publication[7].adapted == []
    assert conn.publication[8:10] == (1869, 1869)


@pytest.mark.parametrize("mode", [["--incremental"], ["--season", "2026"]])
def test_partial_modes_cannot_opt_in_before_connecting(monkeypatch, mode):
    import psycopg2

    connect = Mock()
    monkeypatch.setattr(psycopg2, "connect", connect)
    monkeypatch.setattr("sys.argv", ["compute_house_elo.py", *mode, "--publish-receipts"])
    with pytest.raises(SystemExit) as error:
        elo.main()
    assert error.value.code == 2
    connect.assert_not_called()
