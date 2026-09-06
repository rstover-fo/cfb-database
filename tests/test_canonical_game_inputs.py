"""Regression fixtures for canonical modeled-game SQL inputs.

SQLite runs the portable query shapes against the reviewed postponed/replacement
pair. It proves the original cannot enter modeled inputs even if a provider
incorrectly marks it completed 0-0; PostgreSQL cursor/type behavior remains a
separate integration concern.
"""

import sqlite3

from scripts.compute_adjusted_epa import PLAY_QUERY, TEAM_LIST_QUERY
from scripts.compute_adjusted_epa_week import PLAY_QUERY_WEEK, TARGET_WEEK_INDEX_QUERY
from scripts.compute_house_elo import GAMES_QUERY, SCHEDULED_COUNTS_QUERY


def _sqlite_query(query: str) -> str:
    return query.replace("%s", "?").replace("NULLS LAST", "")


def _warehouse() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.executescript(
        """
        ATTACH DATABASE ':memory:' AS core;
        ATTACH DATABASE ':memory:' AS marts;
        CREATE TABLE core.games (
            id INTEGER PRIMARY KEY, season INTEGER, week INTEGER, season_type TEXT,
            start_date TEXT, neutral_site BOOLEAN, home_team TEXT, away_team TEXT,
            home_points INTEGER, away_points INTEGER, home_pregame_elo REAL,
            away_pregame_elo REAL, completed BOOLEAN
        );
        CREATE TABLE marts.play_epa (
            game_id INTEGER, season INTEGER, offense TEXT, defense TEXT,
            epa REAL, is_garbage_time BOOLEAN
        );
        """
    )
    # The first event was postponed, but an upstream completed=True/0-0 value
    # must not make it a played model input. The Sunday replacement and a later
    # genuine rematch both remain canonical events.
    db.executemany(
        "INSERT INTO core.games VALUES (?, 2026, ?, 'regular', ?, false, 'Campbell', "
        "'Western Carolina', ?, ?, NULL, NULL, ?)",
        [
            (401866625, 1, "2026-09-05", 0, 0, True),
            (401917058, 2, "2026-09-06", 24, 17, True),
            (777777777, 3, "2026-10-01", 21, 20, True),
        ],
    )
    db.executemany(
        "INSERT INTO marts.play_epa VALUES (?, 2026, 'Campbell', 'Western Carolina', ?, false)",
        [
            (401866625, 999.0),
            (401917058, 0.4),
            (777777777, 0.2),
        ],
    )
    return db


def test_house_elo_results_and_schedule_counts_exclude_only_superseded_event():
    db = _warehouse()
    try:
        result_ids = [row[0] for row in db.execute(_sqlite_query(GAMES_QUERY), (2026, 2026))]
        assert result_ids == [401917058, 777777777]

        counts = db.execute(
            _sqlite_query(SCHEDULED_COUNTS_QUERY), (2026, 2026, 2026, 2026)
        ).fetchall()
        assert counts == [(2026, "Campbell", 2), (2026, "Western Carolina", 2)]
    finally:
        db.close()


def test_epa_play_team_and_weekly_schedule_inputs_exclude_superseded_event():
    db = _warehouse()
    try:
        fit_epas = [row[2] for row in db.execute(_sqlite_query(PLAY_QUERY), (2026,))]
        assert fit_epas == [0.4, 0.2]
        assert db.execute(_sqlite_query(TEAM_LIST_QUERY), (2026, 2026)).fetchall() == [
            ("Campbell",),
            ("Western Carolina",),
        ]

        weekly_epas = [row[2] for row in db.execute(_sqlite_query(PLAY_QUERY_WEEK), (2026,))]
        assert weekly_epas == [0.4, 0.2]
        target_weeks = db.execute(_sqlite_query(TARGET_WEEK_INDEX_QUERY), (2026,)).fetchall()
        assert target_weeks == [(2,), (3,)]
    finally:
        db.close()


def test_epa_team_layout_remains_play_derived_for_unmatched_provider_rows():
    db = _warehouse()
    try:
        db.execute(
            "INSERT INTO marts.play_epa VALUES "
            "(999, 2026, 'Unmatched A', 'Unmatched B', 0.1, false)"
        )
        teams = {r[0] for r in db.execute(_sqlite_query(TEAM_LIST_QUERY), (2026, 2026))}
        assert teams == {"Campbell", "Western Carolina", "Unmatched A", "Unmatched B"}
    finally:
        db.close()
