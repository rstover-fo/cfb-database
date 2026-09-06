"""SQLite regression coverage for canonical preseason-backtest inputs.

The schedule-count query is executed unchanged except for DB-API placeholder
adaptation. PostgreSQL's ``DISTINCT ON`` is unavailable in SQLite, so the
week-one CTE's identity predicate is also asserted structurally.
"""

import sqlite3

from scripts.backtest_preseason import _week1_games_query, fetch_scheduled_counts
from src.pipelines.game_identity import eligible_game_sql


class _Cursor:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self._cursor.close()

    def execute(self, query, params):
        self._cursor.execute(
            query.replace("%(season)s", ":season").replace("%(season_type)s", ":season_type"),
            params,
        )

    def fetchall(self):
        return self._cursor.fetchall()


class _Connection:
    def __init__(self):
        self.db = sqlite3.connect(":memory:")

    def cursor(self):
        return _Cursor(self.db.cursor())


def test_backtest_schedule_counts_exclude_superseded_original_and_keep_rematch():
    conn = _Connection()
    try:
        conn.db.executescript(
            """
            ATTACH DATABASE ':memory:' AS core;
            CREATE TABLE core.games (
                id INTEGER PRIMARY KEY, season INTEGER, season_type TEXT,
                home_team TEXT, away_team TEXT, completed BOOLEAN,
                home_points INTEGER, away_points INTEGER
            );
            """
        )
        conn.db.executemany(
            "INSERT INTO core.games VALUES (?, 2026, 'regular', 'Campbell', "
            "'Western Carolina', ?, ?, ?)",
            [
                (401866625, True, 0, 0),
                (401917058, True, 24, 17),
                (777777777, True, 21, 20),
            ],
        )

        assert fetch_scheduled_counts(conn, 2026) == {"Campbell": 2, "Western Carolina": 2}
    finally:
        conn.db.close()


def test_week_one_cte_and_scored_games_filter_the_superseded_identity():
    query = _week1_games_query()
    week1 = query[query.index("WITH week1 AS") : query.index(")\n        SELECT g.id")]

    assert eligible_game_sql("game_id") in week1
    assert eligible_game_sql("g.id") in query
