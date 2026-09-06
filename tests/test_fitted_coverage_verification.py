"""Execute the daily verifier's real SQL against canonical pending fixtures."""

import sqlite3

import pytest

from scripts.verify_load import Report, check_fitted_coverage


@pytest.fixture
def warehouse():
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        ATTACH DATABASE ':memory:' AS core;
        ATTACH DATABASE ':memory:' AS predictions;
        CREATE TABLE core.games (id INTEGER PRIMARY KEY, season INTEGER, completed BOOLEAN);
        CREATE TABLE predictions.game_predictions (game_id INTEGER, model_version TEXT);
    """)
    yield conn
    conn.close()


def test_only_incomplete_superseded_original_is_empty_pending(warehouse, capsys):
    warehouse.execute("INSERT INTO core.games VALUES (401866625, 2026, false)")
    report = Report()
    check_fitted_coverage(warehouse.cursor(), report)
    assert report.failures == 0
    assert "no canonical pending games" in capsys.readouterr().out


def test_superseded_completed_row_does_not_advance_pending_anchor(warehouse, capsys):
    warehouse.executemany(
        "INSERT INTO core.games VALUES (?, ?, ?)",
        [
            (401866625, 2027, True),
            (401917058, 2026, False),
        ],
    )
    report = Report()
    check_fitted_coverage(warehouse.cursor(), report)
    assert report.failures == 1
    assert "season=2026: fitted_v1 covers 0/1" in capsys.readouterr().out


def test_large_covered_season_cannot_hide_empty_next_season(warehouse, capsys):
    warehouse.executemany(
        "INSERT INTO core.games VALUES (?, 2026, false)", [(n,) for n in range(1, 1601)]
    )
    warehouse.executemany(
        "INSERT INTO core.games VALUES (?, 2027, false)", [(n,) for n in range(1601, 1621)]
    )
    warehouse.executemany(
        "INSERT INTO predictions.game_predictions VALUES (?, 'fitted_v1')",
        [(n,) for n in range(1, 1601)],
    )
    report = Report()
    check_fitted_coverage(warehouse.cursor(), report)
    assert report.failures == 1
    output = capsys.readouterr().out
    assert "[PASS] fitted_coverage: season=2026" in output
    assert "[FAIL] fitted_coverage: season=2027" in output


def test_replacement_and_rematch_count_once_despite_prediction_snapshots(warehouse, capsys):
    warehouse.executemany(
        "INSERT INTO core.games VALUES (?, 2026, false)", [(401866625,), (401917058,), (123,)]
    )
    warehouse.executemany(
        "INSERT INTO predictions.game_predictions VALUES (?, 'fitted_v1')",
        [(401917058,), (401917058,), (123,)],
    )
    report = Report()
    check_fitted_coverage(warehouse.cursor(), report)
    assert report.failures == 0
    assert "covers 2/2" in capsys.readouterr().out
