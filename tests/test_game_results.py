"""Regression coverage for narrow, durable CFBD game-result corrections."""

from unittest.mock import MagicMock, patch

import pytest
from dlt.extract.exceptions import ResourceExtractionError

from src.pipelines.game_results import apply_verified_result_correction


def _taylor_game(**changes):
    return {
        "id": 401677463,
        "season": 2024,
        "homeId": 620,
        "awayId": 190,
        "homePoints": None,
        "awayPoints": None,
        "completed": False,
    } | changes


def test_correction_fills_missing_scores_including_zero_and_is_idempotent():
    sul_ross = {
        "id": 401773541,
        "season": 2025,
        "homeId": 2834,
        "awayId": 2025,
        "homePoints": None,
        "awayPoints": None,
        "completed": False,
    }

    corrected = apply_verified_result_correction(sul_ross)

    assert corrected == sul_ross | {"homePoints": 0, "awayPoints": 62, "completed": True}
    assert sul_ross["homePoints"] is None
    assert apply_verified_result_correction(corrected) == corrected


@pytest.mark.parametrize("field, value", [("season", 2023), ("homeId", 1), ("awayId", 1)])
def test_correction_rejects_known_id_with_different_game_identity(field, value):
    with pytest.raises(ValueError, match="mismatching"):
        apply_verified_result_correction(_taylor_game(**{field: value}))


@pytest.mark.parametrize("field, value", [("homePoints", 62), ("awayPoints", 13)])
def test_correction_rejects_conflicting_non_null_scores(field, value):
    with pytest.raises(ValueError, match="conflicts"):
        apply_verified_result_correction(_taylor_game(**{field: value}))


def test_unrelated_game_is_unchanged_without_mutating_input():
    game = {"id": 99, "homePoints": None, "completed": False}

    corrected = apply_verified_result_correction(game)

    assert corrected == game
    assert corrected is not game


def test_games_resource_applies_correction_after_partition_season_without_mutating_cache():
    from src.pipelines.sources.games import games_resource

    raw_game = _taylor_game()
    del raw_game["season"]  # Normal CFBD /games payload shape remains supported.
    expected_raw = dict(raw_game)
    cache = {2024: [raw_game]}
    client = MagicMock()
    with patch("src.pipelines.sources.games.get_client", return_value=client):
        rows = list(games_resource([2024], cache))

    assert rows == [_taylor_game(homePoints=63, awayPoints=12, completed=True)]
    assert raw_game == expected_raw
    client.close.assert_called_once()


def test_games_resource_rejects_reviewed_payload_with_stale_supplied_season():
    """The actual resource generator must inspect CFBD's season before overwrite."""
    from src.pipelines.sources.games import games_resource

    raw_game = _taylor_game(season=2023)
    client = MagicMock()
    with patch("src.pipelines.sources.games.get_client", return_value=client):
        with pytest.raises(
            ResourceExtractionError, match="source season=2023, requested season=2024"
        ):
            list(games_resource([2024], {2024: [raw_game]}))

    assert raw_game == _taylor_game(season=2023)
    client.close.assert_called_once()
