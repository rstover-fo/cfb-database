"""Offline checks for fit eligibility, lifecycle delegation, and event exclusions."""

import sqlite3
from unittest.mock import Mock

import numpy as np
import pytest

from scripts import score_fitted as scoring
from scripts import train_model as training
from src.pipelines.game_identity import SUPERSEDED_GAME_REPLACEMENTS


def frozen_fit(train_through):
    beta = np.zeros(len(training.FEATURE_NAMES))
    beta[0] = train_through - 2020
    return {
        "train_through": train_through,
        "feature_means": dict.fromkeys(training.TEAM_WEEK_SOURCE_COLUMNS, 0.0),
        "diff_means": dict.fromkeys(training.FEATURE_NAMES, 0.0),
        "diff_stds": dict.fromkeys(training.FEATURE_NAMES, 1.0),
        "beta_margin": beta,
        "beta_winprob": np.zeros(len(training.FEATURE_NAMES)),
        "platt_a": 1.0,
        "platt_b": 0.0,
    }


def game(season, game_id=1):
    return {
        "game_id": game_id,
        "season": season,
        "season_type": "regular",
        "week": 1,
        "home_team": "Alpha",
        "away_team": "Bravo",
        "neutral_site": False,
        "start_date": None,
        "home_tw": dict.fromkeys(training.TEAM_WEEK_SOURCE_COLUMNS, 0.0),
        "away_tw": dict.fromkeys(training.TEAM_WEEK_SOURCE_COLUMNS, 0.0),
    }


@pytest.fixture
def upcoming_io(monkeypatch):
    """Mock only I/O; selection, vectorization, row assembly, and gates execute."""
    names = (
        "fetch_pending_game_count",
        "fetch_pending_seasons",
        "fetch_upcoming_games",
        "fetch_available_train_through",
        "load_fit",
        "table_exists",
        "fetch_market_from_lines",
        "write_upcoming",
    )
    mocks = {name: Mock(name=name) for name in names}
    for name, mock in mocks.items():
        monkeypatch.setattr(scoring, name, mock)
    mocks["fetch_pending_game_count"].return_value = 1
    mocks["fetch_pending_seasons"].return_value = [2026]
    mocks["fetch_upcoming_games"].return_value = [game(2026)]
    mocks["fetch_available_train_through"].return_value = [2025]
    mocks["load_fit"].side_effect = lambda _conn, season: frozen_fit(season)
    mocks["table_exists"].return_value = False
    mocks["fetch_market_from_lines"].return_value = {}
    return mocks


@pytest.mark.parametrize("train_through", [2026, 2027, None])
def test_score_game_rejects_ineligible_fit_before_vectorization(train_through):
    # No vectors: the temporal guard must run before touching feature inputs.
    with pytest.raises(ValueError, match="must be before prediction season=2026"):
        scoring.score_game({"season": 2026}, {"train_through": train_through})


def test_score_game_accepts_a_strictly_prior_fit():
    margin, probability = scoring.score_game(game(2026), frozen_fit(2025))
    assert margin == 5.0
    assert probability == 0.5


def test_mixed_pending_seasons_use_their_own_latest_eligible_fits(upcoming_io, capsys):
    upcoming_io["fetch_pending_game_count"].return_value = 3
    upcoming_io["fetch_pending_seasons"].return_value = [2026, 2027]
    upcoming_io["fetch_upcoming_games"].return_value = [game(2027, 3), game(2026, 1), game(2026, 2)]
    upcoming_io["fetch_available_train_through"].return_value = [2027, 2025, 2026]

    def write(_conn, rows):
        assert [call.args[1] for call in upcoming_io["load_fit"].call_args_list] == [2025, 2026]
        assert [(r["game_id"], r["expected_home_margin"]) for r in rows] == [
            (3, 6.0),
            (1, 5.0),
            (2, 5.0),
        ]

    upcoming_io["write_upcoming"].side_effect = write
    scoring.run_upcoming(object())
    upcoming_io["write_upcoming"].assert_called_once()
    output = capsys.readouterr().out
    assert "season=2026 rows=2 model=fitted_v1 train_through=2025" in output
    assert "season=2027 rows=1 model=fitted_v1 train_through=2026" in output


@pytest.mark.parametrize("available", [[], [2026], [2026, 2027]])
def test_no_eligible_fit_prevents_all_writes(upcoming_io, available):
    upcoming_io["fetch_available_train_through"].return_value = available
    with pytest.raises(ValueError, match="no eligible"):
        scoring.run_upcoming(object())
    upcoming_io["load_fit"].assert_not_called()
    upcoming_io["write_upcoming"].assert_not_called()


def test_pending_season_without_features_still_needs_an_eligible_fit(upcoming_io):
    upcoming_io["fetch_pending_game_count"].return_value = 2
    upcoming_io["fetch_pending_seasons"].return_value = [2025, 2026]
    with pytest.raises(ValueError, match="train_through_season < 2025"):
        scoring.run_upcoming(object())
    upcoming_io["write_upcoming"].assert_not_called()


def test_all_required_fits_load_before_any_scoring_or_writes(upcoming_io, monkeypatch):
    upcoming_io["fetch_pending_game_count"].return_value = 2
    upcoming_io["fetch_pending_seasons"].return_value = [2026, 2027]
    upcoming_io["fetch_upcoming_games"].return_value = [game(2026), game(2027, 2)]
    upcoming_io["fetch_available_train_through"].return_value = [2025, 2026]
    upcoming_io["load_fit"].side_effect = [frozen_fit(2025), RuntimeError("missing coefficients")]
    score = Mock()
    monkeypatch.setattr(scoring, "score_game", score)
    with pytest.raises(RuntimeError, match="missing coefficients"):
        scoring.run_upcoming(object())
    score.assert_not_called()
    upcoming_io["write_upcoming"].assert_not_called()


def test_empty_pending_scope_does_not_require_a_fit(upcoming_io):
    upcoming_io["fetch_pending_game_count"].return_value = 0
    upcoming_io["fetch_upcoming_games"].return_value = []
    scoring.run_upcoming(object())
    upcoming_io["fetch_available_train_through"].assert_not_called()
    upcoming_io["write_upcoming"].assert_not_called()


def test_missing_feature_coverage_still_fails_without_writes(upcoming_io):
    upcoming_io["fetch_upcoming_games"].return_value = []
    with pytest.raises(SystemExit) as error:
        scoring.run_upcoming(object())
    assert error.value.code == 1
    upcoming_io["write_upcoming"].assert_not_called()


def test_partial_coverage_keeps_existing_write_then_fail_policy(upcoming_io):
    upcoming_io["fetch_pending_game_count"].return_value = 10
    with pytest.raises(SystemExit) as error:
        scoring.run_upcoming(object())
    assert error.value.code == 1
    upcoming_io["write_upcoming"].assert_called_once()


def test_backfill_requires_exact_prior_vintage_even_when_older_fits_exist(monkeypatch):
    load = Mock(side_effect=RuntimeError("missing exact prior fit"))
    write = Mock()
    monkeypatch.setattr(scoring, "load_fit", load)
    monkeypatch.setattr(scoring, "write_backfill_season", write)
    monkeypatch.setattr(scoring, "fetch_available_train_through", Mock(return_value=[2024]))
    conn = object()
    with pytest.raises(RuntimeError, match="missing exact prior fit"):
        scoring.run_backfill(conn, 2026, 2026)
    load.assert_called_once_with(conn, 2025)
    write.assert_not_called()


class ResultCursor:
    def __init__(self, results):
        self.results = iter(results)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, query, params=None):
        self.rows = next(self.results)

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


@pytest.mark.parametrize("closed_season", [2025, None])
def test_refit_delegates_finality_newest_first_and_keeps_contract_filter(
    monkeypatch, closed_season
):
    current = dict.fromkeys(training.TEAM_WEEK_SOURCE_COLUMNS, 0.0)
    incompatible = {**current, "removed_feature": 0.0}
    conn = Mock()
    conn.cursor.return_value = ResultCursor(
        [
            [(season,) for season in range(2027, training.TRAIN_START_SEASON - 1, -1)],
            [(2025, current), (2024, incompatible), (2023, None)],
        ]
    )
    final = Mock(
        side_effect=lambda _conn, season: closed_season is not None and season <= closed_season
    )
    monkeypatch.setattr(training, "season_is_final", final)
    assert training.fetch_refit_state(conn) == (closed_season, [2025] if closed_season else [])
    assert [call.args[1] for call in final.call_args_list] == (
        [2027, 2026, 2025, *range(training.TRAIN_START_SEASON, 2025)]
        if closed_season
        else list(range(2027, training.TRAIN_START_SEASON - 1, -1))
    )


@pytest.mark.parametrize("gap_kind", ["missing", "unfinished"])
@pytest.mark.parametrize("gap_season", [2015, 2020])
def test_refit_frontier_stops_before_a_gap_inside_the_training_window(
    monkeypatch, gap_kind, gap_season
):
    seasons = list(range(2025, training.TRAIN_START_SEASON - 1, -1))
    if gap_kind == "missing":
        seasons.remove(gap_season)
    conn = Mock()
    conn.cursor.return_value = ResultCursor([[(season,) for season in seasons], []])
    final = Mock(side_effect=lambda _conn, season: season != gap_season)
    monkeypatch.setattr(training, "season_is_final", final)
    frontier, existing = training.fetch_refit_state(conn)
    assert frontier == (gap_season - 1 if gap_season > training.TRAIN_START_SEASON else None)
    assert existing == []
    assert [call.args[1] for call in final.call_args_list].count(2025) == 1
    if gap_kind == "missing":
        assert gap_season not in [call.args[1] for call in final.call_args_list]


def test_premature_existing_fit_cannot_block_the_eligible_annual_refit(monkeypatch):
    current = dict.fromkeys(training.TEAM_WEEK_SOURCE_COLUMNS, 0.0)
    conn = Mock()
    conn.cursor.return_value = ResultCursor(
        [
            [(season,) for season in range(2026, training.TRAIN_START_SEASON - 1, -1)],
            [(2024, current), (2026, current)],
        ]
    )
    monkeypatch.setattr(training, "season_is_final", lambda _conn, season: season <= 2025)
    frontier, existing = training.fetch_refit_state(conn)
    assert (frontier, existing) == (2025, [2024])
    assert training.stale_score_seasons(frontier, existing) == [2026]


def test_loaded_fit_carries_the_vintage_checked_by_score_game():
    fit = frozen_fit(2025)
    metadata = [(1.0, 0.0, fit["feature_means"], fit["diff_means"], fit["diff_stds"])]
    coefs = [
        (component, index, feature, values[index])
        for component, values in (("margin", fit["beta_margin"]), ("winprob", fit["beta_winprob"]))
        for index, feature in enumerate(training.FEATURE_NAMES)
    ]
    conn = Mock()
    conn.cursor.return_value = ResultCursor([metadata, coefs])
    loaded = scoring.load_fit(conn, 2025)
    assert loaded["train_through"] == 2025
    assert scoring.score_game(game(2026), loaded) == (5.0, 0.5)


class SQLiteCursor:
    def __init__(self, cursor, *, dictionaries=False):
        self.cursor = cursor
        self.dictionaries = dictionaries

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.cursor.close()

    def execute(self, query, params=()):
        if "ANY(%s)" in query:
            # SQLite adapter for PostgreSQL's array membership predicate only.
            query = query.replace("= ANY(%s)", "IN (" + ",".join("?" for _ in params[0]) + ")")
            params = params[0]
        self.cursor.execute(query.replace("%s", "?"), params)

    def fetchall(self):
        rows = self.cursor.fetchall()
        if self.dictionaries:
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in rows]
        return rows

    def fetchone(self):
        return self.cursor.fetchone()


class SQLiteConnection:
    def __init__(self):
        self.db = sqlite3.connect(":memory:")

    def cursor(self, *, cursor_factory=None):
        return SQLiteCursor(self.db.cursor(), dictionaries=cursor_factory is not None)


@pytest.fixture
def game_database():
    conn = SQLiteConnection()
    conn.db.executescript("""
        ATTACH DATABASE ':memory:' AS core;
        ATTACH DATABASE ':memory:' AS features;
        CREATE TABLE core.games (
            id INTEGER PRIMARY KEY, season INTEGER, season_type TEXT,
            week INTEGER, start_date TEXT, neutral_site BOOLEAN,
            home_team TEXT, away_team TEXT, home_points INTEGER,
            away_points INTEGER, completed BOOLEAN
        );
    """)
    columns = ", ".join(f"{column} REAL" for column in training.TEAM_WEEK_SOURCE_COLUMNS)
    conn.db.execute(f"CREATE TABLE features.team_week (game_id INTEGER, team TEXT, {columns})")
    yield conn
    conn.db.close()


def insert_game(conn, game_id, season, *, completed=False):
    conn.db.execute(
        "INSERT INTO core.games VALUES (?, ?, 'regular', 1, NULL, false, 'Alpha', 'Bravo', "
        "21, 14, ?)",
        (game_id, season, completed),
    )
    conn.db.executemany(
        "INSERT INTO features.team_week (game_id, team) VALUES (?, ?)",
        [(game_id, "Alpha"), (game_id, "Bravo")],
    )


@pytest.mark.parametrize("original_completed", [False, True])
def test_pending_scope_excludes_reviewed_original_in_count_seasons_and_scoring(
    game_database, original_completed
):
    original, replacement = next(iter(SUPERSEDED_GAME_REPLACEMENTS.items()))
    insert_game(game_database, 1, 2025, completed=True)
    insert_game(game_database, original, 2028, completed=original_completed)
    insert_game(game_database, replacement, 2026)
    insert_game(game_database, 2, 2027)
    assert scoring.fetch_pending_game_count(game_database) == 2
    assert scoring.fetch_pending_seasons(game_database) == [2026, 2027]
    assert [g["game_id"] for g in scoring.fetch_upcoming_games(game_database)] == [replacement, 2]


def test_training_and_backfill_exclude_superseded_results(game_database):
    original, replacement = next(iter(SUPERSEDED_GAME_REPLACEMENTS.items()))
    insert_game(game_database, original, 2026, completed=True)
    insert_game(game_database, replacement, 2026, completed=True)
    assert [g["game_id"] for g in training.fetch_games(game_database, [2026])] == [replacement]
    assert [g["game_id"] for g in scoring.fetch_backfill_games(game_database, 2026)] == [
        replacement
    ]
