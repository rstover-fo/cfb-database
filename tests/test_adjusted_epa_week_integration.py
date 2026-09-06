"""Offline SQL/consumer checks for weekly EPA; no live Postgres or warehouse.

SQLite executes the portable schedule, team, play, and staging SQL on tiny
fixtures. The adapter changes DB-API placeholders and batched insertion only;
it does not validate Postgres server cursors, types, or deployment state.
"""

import sqlite3

import pytest

pytest.importorskip("numpy")

from scripts import build_features  # noqa: E402
from scripts import compute_adjusted_epa_week as weekly  # noqa: E402
from scripts.compute_predictions import fetch_epa_week_coefs, lookup_epa_coefs_asof  # noqa: E402


class OfflineCursor:
    def __init__(self, cursor):
        self.cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.cursor.close()

    def execute(self, query, params=()):
        return self.cursor.execute(query.replace("%s", "?"), params)

    def fetchall(self):
        return self.cursor.fetchall()

    def __iter__(self):
        return iter(self.cursor)


class OfflineConnection:
    def __init__(self):
        self.db = sqlite3.connect(":memory:")
        self.cursor_names = []

    def cursor(self, name=None):
        self.cursor_names.append(name)
        return OfflineCursor(self.db.cursor())

    def commit(self):
        self.db.commit()


@pytest.fixture
def warehouse(monkeypatch):
    conn = OfflineConnection()
    conn.db.executescript("""
        ATTACH DATABASE ':memory:' AS core;
        ATTACH DATABASE ':memory:' AS marts;
        ATTACH DATABASE ':memory:' AS analytics;
        CREATE TABLE core.games (
            id INTEGER PRIMARY KEY, season INTEGER, week INTEGER,
            season_type TEXT, completed BOOLEAN, home_team TEXT, neutral_site BOOLEAN
        );
        CREATE TABLE marts.play_epa (
            game_id INTEGER, season INTEGER, offense TEXT, defense TEXT,
            epa REAL, is_garbage_time BOOLEAN
        );
        CREATE TABLE analytics.adjusted_epa_week_build (
            team TEXT, season INTEGER, week_index INTEGER, off_coef REAL,
            def_coef REAL, hfa_coef REAL, mu REAL, plays INTEGER,
            lambda REAL, n_teams INTEGER,
            UNIQUE(team, season, week_index)
        );
    """)

    def execute_values(cursor, query, rows):
        placeholders = ", ".join("?" for _ in rows[0])
        cursor.cursor.executemany(query.replace("%s", f"({placeholders})"), rows)

    monkeypatch.setattr("psycopg2.extras.execute_values", execute_values)
    yield conn
    conn.db.close()


def add_game(conn, game_id, week, *, season_type="regular", season=2026, completed=False):
    conn.db.execute(
        "INSERT INTO core.games VALUES (?, ?, ?, ?, ?, 'Alpha', false)",
        (game_id, season, week, season_type, completed),
    )


def add_plays(conn, game_id, *, count=160, epa=0.4, season=2026):
    # Both home and away offensive plays identify the unpenalized intercept/HFA.
    conn.db.executemany(
        "INSERT INTO marts.play_epa VALUES (?, ?, ?, ?, ?, false)",
        [(game_id, season, "Alpha", "Bravo", epa)] * count
        + [(game_id, season, "Bravo", "Alpha", -0.1)] * count,
    )


def rebuild(conn):
    result = weekly.fit_season_weeks(conn, 2026)
    assert result is not None
    rows, teams = result
    weekly.write_season(conn, 2026, rows)
    return rows, teams


def test_schedule_targets_survive_sql_staging_and_feature_resolution(warehouse, monkeypatch):
    add_game(warehouse, 1, 1, completed=True)
    add_game(warehouse, 2, 2)
    add_game(warehouse, 3, 2)  # Multiple games share a boundary.
    add_game(warehouse, 4, 5)  # No artificial weeks 3/4 across this schedule gap.
    add_game(warehouse, 5, 1, season_type="postseason")
    add_game(warehouse, 6, 2, season_type="postseason")
    add_game(warehouse, 7, 3, season=2025)
    add_game(warehouse, 8, None)
    add_game(warehouse, 9, 6, season_type="allstar")
    add_plays(warehouse, 1)
    warehouse.db.executemany(
        "INSERT INTO marts.play_epa VALUES (1, 2026, 'Alpha', 'Bravo', ?, ?)",
        [(9999.0, True), (None, False)],
    )

    rows, teams = rebuild(warehouse)

    assert teams == ["Alpha", "Bravo"]
    assert {r["week_index"] for r in rows} == {2, 5, 101, 102}
    assert len(rows) == len(teams) * 4
    assert all(r["plays"] == 160 for r in rows)
    assert "adjusted_epa_week_plays_2026" in warehouse.cursor_names
    stored = build_features.fetch_adj_epa_week_rows(warehouse, 2026)
    prior = {"Alpha": {"off_coef": 9.0, "def_coef": 8.0, "hfa_coef": 7.0}}
    for target in (2, 5, 101, 102):
        resolved = build_features.resolve_adj_epa("Alpha", 2026, target, stored, prior)
        expected = next(r for r in rows if r["team"] == "Alpha" and r["week_index"] == target)
        assert resolved["source"] == "week"
        assert resolved["off"] == pytest.approx(expected["off_coef"])
    assert (
        build_features.resolve_adj_epa("Alpha", 2026, 1, stored, prior)["source"] == "prior_season"
    )
    assert build_features.resolve_adj_epa("Idle", 2026, 2, stored, {})["off"] is None

    asof_rows = fetch_epa_week_coefs(warehouse, 2026)
    blend_coefs, source = lookup_epa_coefs_asof(asof_rows, {}, "Alpha", 2026, 2)
    assert source == "week"
    assert blend_coefs is not None
    entering_two = next(r for r in stored["Alpha"] if r["week_index"] == 2)
    assert blend_coefs["off_coef"] == pytest.approx(entering_two["off_coef"])

    # Exercise actual feature-row assembly. Only the large, unrelated raw-feature
    # SQL and prior-season lookup are stubbed; this-season staging reads are real.
    raw = dict.fromkeys(build_features._INSERT_COLUMNS)
    raw.update(
        season=2026,
        season_type="regular",
        week=2,
        week_index=2,
        team="Alpha",
        game_id=2,
        games_played_to_date=1,
        elo_pregame_raw=1500.0,
    )
    monkeypatch.setattr(build_features, "fetch_feature_rows", lambda *_: [raw])
    monkeypatch.setattr(build_features, "fetch_adj_epa_full_rows", lambda *_: prior)
    built = build_features.build_season_rows(warehouse, 2026, {})
    assert built[0]["adj_epa_source"] == "week"
    assert built[0]["adj_epa_off"] == pytest.approx(entering_two["off_coef"])


def test_midweek_and_late_correction_rebuilds_replace_only_the_target_season(warehouse):
    add_game(warehouse, 1, 1, completed=True)
    add_game(warehouse, 2, 2)
    add_game(warehouse, 3, 3)
    add_plays(warehouse, 1)
    initial, _ = rebuild(warehouse)
    warehouse.db.execute(
        "INSERT INTO analytics.adjusted_epa_week_build "
        "VALUES ('Earlier', 2025, 2, 1, 2, 3, 4, 5, 6, 1)"
    )

    add_plays(warehouse, 2, epa=4.0)
    midweek, _ = rebuild(warehouse)
    assert [r for r in midweek if r["week_index"] == 2] == [
        r for r in initial if r["week_index"] == 2
    ]
    assert [r for r in midweek if r["week_index"] == 3] != [
        r for r in initial if r["week_index"] == 3
    ]

    warehouse.db.execute("UPDATE marts.play_epa SET epa = epa + 1 WHERE game_id = 1")
    corrected, _ = rebuild(warehouse)
    assert [r for r in corrected if r["week_index"] == 2] != [
        r for r in midweek if r["week_index"] == 2
    ]
    repeated, _ = rebuild(warehouse)
    assert repeated == corrected
    assert warehouse.db.execute(
        "SELECT season, COUNT(*) FROM analytics.adjusted_epa_week_build "
        "GROUP BY season ORDER BY season"
    ).fetchall() == [(2025, 1), (2026, 4)]


def test_thin_play_coverage_keeps_the_prior_season_label(warehouse):
    add_game(warehouse, 1, 1, completed=True)
    add_game(warehouse, 2, 2)
    add_plays(warehouse, 1, count=70)
    rebuild(warehouse)
    stored = build_features.fetch_adj_epa_week_rows(warehouse, 2026)
    prior = {"Alpha": {"off_coef": 0.1, "def_coef": -0.1, "hfa_coef": 0.01}}
    result = build_features.resolve_adj_epa("Alpha", 2026, 2, stored, prior)
    assert stored["Alpha"][0]["plays"] == 70
    assert result["source"] == "prior_season"
    assert result["off"] == 0.1


def test_scheduled_season_without_plays_is_a_clean_noop(warehouse):
    add_game(warehouse, 1, 1)
    add_game(warehouse, 2, 2)
    assert weekly.fit_season_weeks(warehouse, 2026) is None


def test_neutral_only_opening_uses_fallback_and_removes_obsolete_snapshots(warehouse):
    add_game(warehouse, 1, 1, completed=True)
    add_game(warehouse, 2, 2)
    add_plays(warehouse, 1)
    rows, _ = rebuild(warehouse)
    assert len(rows) == 2

    # A venue correction removes the variation that identified the old HFA.
    warehouse.db.execute("UPDATE core.games SET neutral_site = true WHERE id = 1")
    rows, _ = rebuild(warehouse)
    assert rows == []
    stored = build_features.fetch_adj_epa_week_rows(warehouse, 2026)
    assert stored == {}
    prior = {"Alpha": {"off_coef": 0.1, "def_coef": -0.1, "hfa_coef": 0.01}}
    resolved = build_features.resolve_adj_epa("Alpha", 2026, 2, stored, prior)
    assert resolved["source"] == "prior_season"
    assert resolved["off"] == 0.1
    missing = build_features.resolve_adj_epa("Bravo", 2026, 2, stored, prior)
    assert missing["source"] is None
    assert missing["off"] is None
