"""Pure fixtures for the shared, conservative season lifecycle policy."""

from datetime import UTC, datetime

from src.pipelines.season_lifecycle import (
    SeasonResolutions,
    classify_season,
    season_close_floor,
    season_is_final,
)


def _game(game_id: int, season: int = 2025, **changes) -> dict:
    row = {
        "game_id": game_id,
        "season": season,
        "season_type": "postseason" if game_id == 100 else "regular",
        "start_date": datetime(season + 1, 1, 10, tzinfo=UTC),
        "completed": True,
        "home_id": game_id * 2,
        "home_team": f"Home {game_id}",
        "home_points": 24,
        "away_id": game_id * 2 + 1,
        "away_team": f"Away {game_id}",
        "away_points": 17,
    }
    row.update(changes)
    return row


def _complete_schedule(season: int = 2025, games: int = 100) -> list[dict]:
    rows = [_game(game_id, season) for game_id in range(1, games + 1)]
    if rows:
        rows[-1]["season_type"] = "postseason"
    return rows


def _as_of(season: int = 2025) -> datetime:
    return datetime(season + 1, 2, 1, tzinfo=UTC)


def test_complete_historical_season_closes_after_floor():
    result = classify_season(_complete_schedule(), 2025, as_of=_as_of())

    assert result.is_final is True
    assert result.reason == "all canonical games are resolved"


def test_calendar_floor_blocks_a_complete_partial_current_schedule():
    result = classify_season(
        _complete_schedule(2026),
        2026,
        as_of=datetime(2026, 10, 1, tzinfo=UTC),
    )

    assert result.is_final is False
    assert "calendar floor" in result.reason


def test_2020_disruption_uses_july_floor():
    assert season_close_floor(2020) == datetime(2021, 7, 1, tzinfo=UTC)
    assert not classify_season(
        _complete_schedule(2020),
        2020,
        as_of=datetime(2021, 6, 30, 23, 59, tzinfo=UTC),
    ).is_final
    assert classify_season(
        _complete_schedule(2020),
        2020,
        as_of=datetime(2021, 7, 1, tzinfo=UTC),
    ).is_final


def test_completed_2020_spring_schedule_closes_after_july_floor():
    rows = _complete_schedule(2020)
    for row in rows:
        row["season_type"] = "spring_regular"
    rows[-1]["season_type"] = "spring_postseason"

    result = classify_season(
        rows,
        2020,
        as_of=datetime(2021, 7, 1, tzinfo=UTC),
    )

    assert result.is_final is True
    assert result.scheduled_games == 100
    assert rows[0]["season_type"] == "spring_regular"
    assert rows[-1]["season_type"] == "spring_postseason"


def test_incomplete_spring_game_blocks_2020_closure():
    rows = _complete_schedule(2020)
    rows[0].update(
        season_type="spring_regular",
        completed=False,
        home_points=None,
        away_points=None,
    )
    rows[-1]["season_type"] = "spring_postseason"

    result = classify_season(
        rows,
        2020,
        as_of=datetime(2021, 7, 1, tzinfo=UTC),
    )

    assert result.is_final is False
    assert result.unresolved_game_ids == (1,)


def test_spring_postseason_satisfies_postseason_coverage():
    rows = _complete_schedule(2020)
    for row in rows:
        row["season_type"] = "regular"
    rows[-1]["season_type"] = "spring_postseason"

    result = classify_season(
        rows,
        2020,
        as_of=datetime(2021, 7, 1, tzinfo=UTC),
    )

    assert result.is_final is True


def test_regular_only_truncated_schedule_cannot_close():
    rows = _complete_schedule()
    for row in rows:
        row["season_type"] = "regular"

    result = classify_season(rows, 2025, as_of=_as_of())

    assert result.is_final is False
    assert result.reason == "schedule has no postseason games"


def test_postseason_only_truncated_schedule_cannot_close():
    rows = _complete_schedule()
    for row in rows:
        row["season_type"] = "postseason"

    result = classify_season(rows, 2025, as_of=_as_of())

    assert result.is_final is False
    assert result.reason == "schedule has no regular games"


def test_sparse_complete_schedule_cannot_close():
    result = classify_season(_complete_schedule(games=99), 2025, as_of=_as_of())

    assert result.is_final is False
    assert "only 99 canonical games" in result.reason


def test_unfinished_allstar_game_does_not_hold_season_open():
    rows = _complete_schedule()
    rows.append(
        _game(
            101,
            season_type="allstar",
            completed=False,
            home_points=None,
            away_points=None,
        )
    )

    result = classify_season(rows, 2025, as_of=_as_of())

    assert result.is_final is True
    assert result.scheduled_games == 100


def test_unknown_or_missing_season_type_remains_unresolved():
    for season_type in (None, "spring"):
        rows = _complete_schedule()
        rows.append(_game(101, season_type=season_type))

        result = classify_season(rows, 2025, as_of=_as_of())

        assert result.is_final is False
        assert result.unresolved_game_ids == (101,)
        assert "unknown season type" in result.reason


def test_mixed_season_rows_refuse_classification():
    rows = _complete_schedule()
    rows[-1]["season"] = 2024

    result = classify_season(rows, 2025, as_of=_as_of())

    assert result.is_final is False
    assert result.unresolved_game_ids == (100,)
    assert "do not belong to season 2025" in result.reason


def test_playoff_tail_blocks_finality():
    rows = _complete_schedule()
    rows[-1].update(completed=False, home_points=None, away_points=None)

    result = classify_season(rows, 2025, as_of=_as_of())

    assert result.is_final is False
    assert result.unresolved_game_ids == (100,)
    assert "100" in result.reason


def test_future_postseason_game_blocks_finality_even_if_marked_complete():
    rows = _complete_schedule()
    rows[-1]["start_date"] = datetime(2026, 2, 2, tzinfo=UTC)

    result = classify_season(rows, 2025, as_of=_as_of())

    assert result.is_final is False
    assert result.unresolved_game_ids == (100,)


def test_missing_kickoff_or_completed_score_is_unresolved():
    rows = _complete_schedule()
    rows[0]["start_date"] = None
    rows[1]["home_points"] = None

    result = classify_season(rows, 2025, as_of=_as_of())

    assert result.is_final is False
    assert result.unresolved_game_ids == (1, 2)


def test_old_incomplete_row_is_not_inferred_cancelled_from_age():
    rows = _complete_schedule(2022)
    rows[0].update(completed=False, home_points=None, away_points=None)

    result = classify_season(
        rows,
        2022,
        as_of=datetime(2026, 9, 1, tzinfo=UTC),
    )

    assert result.is_final is False
    assert result.unresolved_game_ids == (1,)


def test_reviewed_cancellation_allows_historical_closure_without_completion():
    rows = _complete_schedule(2022, games=101)
    rows[0].update(completed=False, home_points=None, away_points=None)
    resolutions = SeasonResolutions(
        cancelled_game_ids=frozenset({1}),
        superseded_game_replacements={},
    )

    result = classify_season(
        rows,
        2022,
        as_of=datetime(2026, 9, 1, tzinfo=UTC),
        resolutions=resolutions,
    )

    assert result.is_final is True
    assert result.scheduled_games == 100


def test_reviewed_postponement_uses_completed_replacement():
    rows = _complete_schedule()
    replacement = _game(
        1001,
        home_team="Campbell",
        away_team="Western Carolina",
    )
    original = _game(
        1000,
        home_team="Campbell",
        away_team="Western Carolina",
        completed=False,
        home_points=None,
        away_points=None,
    )
    rows.extend([original, replacement])
    resolutions = SeasonResolutions(frozenset(), {1000: 1001})

    result = classify_season(rows, 2025, as_of=_as_of(), resolutions=resolutions)

    assert result.is_final is True
    assert result.scheduled_games == 101


def test_postponement_with_unfinished_replacement_stays_open():
    rows = _complete_schedule()
    original = _game(1000, home_team="A", away_team="B", completed=False)
    replacement = _game(
        1001,
        home_team="A",
        away_team="B",
        completed=False,
        home_points=None,
        away_points=None,
    )
    rows.extend([original, replacement])

    result = classify_season(
        rows,
        2025,
        as_of=_as_of(),
        resolutions=SeasonResolutions(frozenset(), {1000: 1001}),
    )

    assert result.is_final is False
    assert result.unresolved_game_ids == (1001,)


def test_postponement_identity_mismatch_refuses_suppression():
    rows = _complete_schedule()
    rows.extend(
        [
            _game(1000, home_team="A", away_team="B"),
            _game(1001, home_team="A", away_team="Different"),
        ]
    )

    result = classify_season(
        rows,
        2025,
        as_of=_as_of(),
        resolutions=SeasonResolutions(frozenset(), {1000: 1001}),
    )

    assert result.is_final is False
    assert "disagree on away_team" in result.reason


class _Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.description = [(name,) for name in rows[0]] if rows else []
        self.executed = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params):
        self.executed = (query, params)

    def fetchall(self):
        return [tuple(row.values()) for row in self.rows]


class _Connection:
    def __init__(self, rows):
        self.cursor_instance = _Cursor(rows)

    def cursor(self):
        return self.cursor_instance


def test_database_wrapper_uses_shared_classifier_contract():
    conn = _Connection(_complete_schedule())

    assert season_is_final(conn, 2025, as_of=_as_of()) is True
    query, params = conn.cursor_instance.executed
    assert "FROM core.games" in query
    assert params == (2025,)
