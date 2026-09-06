"""Reviewed event identities across schedule and actual prediction SQL."""

import sqlite3
from copy import deepcopy

import pytest

from scripts.compute_predictions import BACKFILL_GAMES_QUERY, TARGET_GAMES_QUERY
from src.pipelines.game_identity import (
    CANCELLED_GAME_IDS,
    eligible_game_sql,
    select_canonical_game_rows,
)


def event(game_id, **changes):
    return dict(game_id=game_id, season=2026, home_team="Campbell", away_team="WCU") | changes


def test_schedule_resolution_preserves_replacement_and_rematches():
    rows = [event(401866625), event(401917058), event(123)]
    before = deepcopy(rows)
    assert [r["game_id"] for r in select_canonical_game_rows(rows)] == [401917058, 123]
    assert rows == before


@pytest.mark.parametrize("field", ["season", "home_team", "away_team"])
def test_missing_pair_identity_is_not_agreement(field):
    original, replacement = event(401866625), event(401917058)
    original[field] = replacement[field] = None
    with pytest.raises(ValueError, match="disagree"):
        select_canonical_game_rows([original, replacement])


def test_missing_replacement_is_not_a_cancelled_contest():
    with pytest.raises(ValueError, match="without replacement"):
        select_canonical_game_rows([event(401866625)])


def test_sql_rejects_expression_as_identifier():
    with pytest.raises(ValueError, match="invalid"):
        eligible_game_sql("g.id); DROP TABLE core.games")


def test_reviewed_cancellations_are_excluded_without_mutating_provider_rows():
    rows = [event(game_id) for game_id in sorted(CANCELLED_GAME_IDS)] + [event(123)]
    before = deepcopy(rows)
    assert select_canonical_game_rows(rows) == [event(123)]
    assert rows == before
    with sqlite3.connect(":memory:") as conn:
        conn.execute("CREATE TABLE games (id INTEGER)")
        conn.executemany("INSERT INTO games VALUES (?)", [(r["game_id"],) for r in rows])
        assert conn.execute(f"SELECT id FROM games g WHERE {eligible_game_sql()}").fetchall() == [
            (123,)
        ]


@pytest.mark.parametrize("original_completed", [False, True])
def test_real_upcoming_query_excludes_original_even_without_replacement(original_completed):
    with sqlite3.connect(":memory:") as conn:
        conn.executescript("""
            ATTACH DATABASE ':memory:' AS core;
            CREATE TABLE core.games (
                id INTEGER, season INTEGER, week INTEGER, season_type TEXT,
                start_date TEXT, neutral_site BOOLEAN, home_team TEXT, away_team TEXT,
                completed BOOLEAN
            );
        """)
        conn.executemany(
            "INSERT INTO core.games VALUES (?, ?, 1, 'regular', '2026-09-06', false, 'A', 'B', ?)",
            [(401866625, 2027, original_completed), (123, 2026, False)],
        )
        # A bogus future completed original must not advance the target window.
        assert [r[0] for r in conn.execute(TARGET_GAMES_QUERY)] == [123]
        conn.execute(
            "INSERT INTO core.games VALUES "
            "(401917058, 2026, 1, 'regular', '2026-09-06', false, 'A', 'B', false)"
        )
        assert {r[0] for r in conn.execute(TARGET_GAMES_QUERY)} == {123, 401917058}


def test_backfill_excludes_stale_original_elo_rows():
    with sqlite3.connect(":memory:") as conn:
        conn.executescript("""
            ATTACH DATABASE ':memory:' AS analytics;
            CREATE TABLE analytics.house_elo_game (
                game_id INTEGER, season INTEGER, week INTEGER, season_type TEXT,
                start_date TEXT, neutral_site BOOLEAN, home_team TEXT, away_team TEXT,
                home_pregame_elo REAL, away_pregame_elo REAL
            );
        """)
        conn.executemany(
            "INSERT INTO analytics.house_elo_game VALUES "
            "(?, 2026, 1, 'regular', '2026-09-06', false, 'A', 'B', 1500, 1500)",
            [(401866625,), (401917058,), (123,)],
        )
        result = conn.execute(BACKFILL_GAMES_QUERY.replace("%s", "?"), (2026,))
        assert {r[0] for r in result} == {401917058, 123}
