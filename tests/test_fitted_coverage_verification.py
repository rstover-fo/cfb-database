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
        CREATE TABLE core.games (
            id INTEGER PRIMARY KEY,
            season INTEGER,
            completed BOOLEAN,
            start_date TEXT
        );
        CREATE TABLE predictions.game_predictions (
            game_id INTEGER,
            model_version TEXT,
            evaluation_mode TEXT,
            published_at TEXT
        );
    """)
    yield conn
    conn.close()


def test_only_incomplete_superseded_original_is_empty_pending(warehouse, capsys):
    warehouse.execute("INSERT INTO core.games VALUES (401866625, 2026, false, NULL)")
    report = Report()
    check_fitted_coverage(warehouse.cursor(), report)
    assert report.failures == 0
    assert "no canonical pending games" in capsys.readouterr().out


def test_superseded_completed_row_does_not_advance_pending_anchor(warehouse, capsys):
    warehouse.executemany(
        "INSERT INTO core.games VALUES (?, ?, ?, ?)",
        [
            (401866625, 2027, True, "2027-09-01T12:00:00Z"),
            (401917058, 2026, False, "2026-09-01T12:00:00Z"),
        ],
    )
    report = Report()
    check_fitted_coverage(warehouse.cursor(), report)
    assert report.failures == 1
    assert "season=2026: fitted_v1 covers 0/1" in capsys.readouterr().out


def test_large_covered_season_cannot_hide_empty_next_season(warehouse, capsys):
    warehouse.executemany(
        "INSERT INTO core.games VALUES (?, 2026, false, NULL)", [(n,) for n in range(1, 1601)]
    )
    warehouse.executemany(
        "INSERT INTO core.games VALUES (?, 2027, false, NULL)", [(n,) for n in range(1601, 1621)]
    )
    warehouse.executemany(
        """INSERT INTO predictions.game_predictions
           VALUES (?, 'fitted_v1', 'published_forecast', '2026-08-01T12:00:00Z')""",
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
        "INSERT INTO core.games VALUES (?, 2026, false, NULL)",
        [(401866625,), (401917058,), (123,)],
    )
    warehouse.executemany(
        """INSERT INTO predictions.game_predictions
           VALUES (?, 'fitted_v1', 'published_forecast', '2026-08-01T12:00:00Z')""",
        [(401917058,), (401917058,), (123,)],
    )
    report = Report()
    check_fitted_coverage(warehouse.cursor(), report)
    assert report.failures == 0
    assert "covers 2/2" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("evaluation_mode", "published_at"),
    [
        ("legacy_unknown", "2026-08-01T12:00:00Z"),
        ("walk_forward_reconstruction", None),
        ("hindsight_experiment", None),
        ("published_forecast", None),
    ],
)
def test_nonpublished_or_unpublished_rows_do_not_satisfy_coverage(
    warehouse, capsys, evaluation_mode, published_at
):
    warehouse.execute("INSERT INTO core.games VALUES (1, 2026, false, NULL)")
    warehouse.execute(
        "INSERT INTO predictions.game_predictions VALUES (1, 'fitted_v1', ?, ?)",
        (evaluation_mode, published_at),
    )

    report = Report()
    check_fitted_coverage(warehouse.cursor(), report)

    assert report.failures == 1
    assert "covers 0/1" in capsys.readouterr().out


def test_at_or_after_known_kickoff_does_not_satisfy_coverage(warehouse, capsys):
    kickoff = "2026-09-01T12:00:00Z"
    warehouse.execute("INSERT INTO core.games VALUES (1, 2026, false, ?)", (kickoff,))
    warehouse.executemany(
        "INSERT INTO predictions.game_predictions VALUES (1, 'fitted_v1', ?, ?)",
        [
            ("published_forecast", kickoff),
            ("published_forecast", "2026-09-01T12:00:01Z"),
        ],
    )

    report = Report()
    check_fitted_coverage(warehouse.cursor(), report)

    assert report.failures == 1
    assert "covers 0/1" in capsys.readouterr().out


def test_strictly_prekickoff_published_row_satisfies_coverage(warehouse, capsys):
    warehouse.execute("INSERT INTO core.games VALUES (1, 2026, false, '2026-09-01T12:00:00Z')")
    warehouse.execute(
        """INSERT INTO predictions.game_predictions
           VALUES (1, 'fitted_v1', 'published_forecast', '2026-09-01T11:59:59Z')"""
    )

    report = Report()
    check_fitted_coverage(warehouse.cursor(), report)

    assert report.failures == 0
    assert "covers 1/1" in capsys.readouterr().out
