"""Tests for WEPA (opponent-adjusted EPA) data source."""

from unittest.mock import MagicMock, patch

import pytest

from src.pipelines.sources.wepa import (
    wepa_players_kicking_resource,
    wepa_players_passing_resource,
    wepa_players_rushing_resource,
)


def test_wepa_team_season_resource_yields_data():
    """WEPA endpoint should yield opponent-adjusted EPA by team/season."""
    from src.pipelines.sources.wepa import wepa_team_season_resource

    mock_response = [
        {
            "team": "Alabama",
            "year": 2024,
            "offense": {"overall": 0.25, "passing": 0.18, "rushing": 0.32},
            "defense": {"overall": -0.15, "passing": -0.12, "rushing": -0.18},
        },
        {
            "team": "Georgia",
            "year": 2024,
            "offense": {"overall": 0.22},
            "defense": {"overall": -0.18},
        },
    ]

    with (
        patch("src.pipelines.sources.wepa.get_client") as mock_get_client,
        patch("src.pipelines.sources.wepa.make_request") as mock_make_request,
    ):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_make_request.return_value = mock_response

        results = list(wepa_team_season_resource(years=[2024]))

        assert len(results) == 2
        assert results[0]["team"] == "Alabama"
        assert results[0]["year"] == 2024


def test_wepa_team_season_iterates_years():
    """Should call API for each year."""
    from src.pipelines.sources.wepa import wepa_team_season_resource

    with (
        patch("src.pipelines.sources.wepa.get_client") as mock_get_client,
        patch("src.pipelines.sources.wepa.make_request") as mock_make_request,
    ):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_make_request.return_value = []

        list(wepa_team_season_resource(years=[2023, 2024]))

        # Should call API twice (once per year)
        assert mock_make_request.call_count == 2


def test_wepa_players_passing_resource_yields_data():
    """WEPA passing endpoint should yield player passing EPA."""
    from src.pipelines.sources.wepa import wepa_players_passing_resource

    mock_response = [
        {
            "id": 12345,
            "name": "Jalen Milroe",
            "team": "Alabama",
            "overall": 0.28,
        },
    ]

    with (
        patch("src.pipelines.sources.wepa.get_client") as mock_get_client,
        patch("src.pipelines.sources.wepa.make_request") as mock_make_request,
    ):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_make_request.return_value = mock_response

        results = list(wepa_players_passing_resource(years=[2024]))

        assert len(results) == 1
        assert results[0]["id"] == 12345


PLAYER_RESOURCES = [
    wepa_players_passing_resource,
    wepa_players_rushing_resource,
    wepa_players_kicking_resource,
]


@pytest.mark.parametrize("resource_fn", PLAYER_RESOURCES, ids=lambda fn: fn.__name__)
class TestPlayerIdCoalesce:
    """CFBD renamed the player-id field on at least one /wepa/players/*
    endpoint sometime after the existing 2014-2025 rows were loaded
    (observed 2026-08-30, backfill run 33333482499: dlt's
    UnboundColumnException on wepa_players_passing -- "id ... did not
    receive any data" -- despite an HTTP 200 response). All three player
    resources share the same fix, so these run against all three."""

    def test_id_already_present_is_unchanged(self, resource_fn):
        mock_response = [{"id": 12345, "name": "Player A"}]

        with (
            patch("src.pipelines.sources.wepa.get_client") as mock_get_client,
            patch("src.pipelines.sources.wepa.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = mock_response

            results = list(resource_fn(years=[2024]))

        assert len(results) == 1
        assert results[0]["id"] == 12345  # untouched -- still the original int

    def test_player_id_field_is_coalesced_and_stamped_as_string(self, resource_fn):
        mock_response = [{"playerId": 777, "name": "Player B"}]

        with (
            patch("src.pipelines.sources.wepa.get_client") as mock_get_client,
            patch("src.pipelines.sources.wepa.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = mock_response

            results = list(resource_fn(years=[2024]))

        assert len(results) == 1
        assert results[0]["id"] == "777"
        assert isinstance(results[0]["id"], str)
        assert results[0]["playerId"] == 777  # original renamed field kept too

    def test_athlete_id_field_is_coalesced_and_stamped_as_string(self, resource_fn):
        mock_response = [{"athleteId": 888, "name": "Player C"}]

        with (
            patch("src.pipelines.sources.wepa.get_client") as mock_get_client,
            patch("src.pipelines.sources.wepa.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = mock_response

            results = list(resource_fn(years=[2024]))

        assert len(results) == 1
        assert results[0]["id"] == "888"
        assert isinstance(results[0]["id"], str)
        assert results[0]["athleteId"] == 888

    def test_nested_athlete_id_is_coalesced_and_stamped_as_string(self, resource_fn):
        mock_response = [{"athlete": {"id": 999, "name": "Player D"}, "name": "Player D"}]

        with (
            patch("src.pipelines.sources.wepa.get_client") as mock_get_client,
            patch("src.pipelines.sources.wepa.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = mock_response

            results = list(resource_fn(years=[2024]))

        assert len(results) == 1
        assert results[0]["id"] == "999"
        assert isinstance(results[0]["id"], str)
        assert results[0]["athlete"] == {"id": 999, "name": "Player D"}  # preserved

    def test_no_id_candidate_is_skipped_but_siblings_still_yielded(self, resource_fn, caplog):
        mock_response = [
            {"name": "No Id Player"},
            {"playerId": 42, "name": "Has Id Player"},
        ]

        with (
            patch("src.pipelines.sources.wepa.get_client") as mock_get_client,
            patch("src.pipelines.sources.wepa.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = mock_response

            with caplog.at_level("WARNING"):
                results = list(resource_fn(years=[2024]))

        assert len(results) == 1
        assert results[0]["id"] == "42"
        assert any("id" in record.message.lower() for record in caplog.records)


def test_wepa_source_returns_all_resources():
    """WEPA source should return all WEPA resources."""
    from src.pipelines.sources.wepa import wepa_source

    with patch("src.pipelines.sources.wepa.get_client") as mock_get_client:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        source = wepa_source(years=[2024])

        # Source should return a DltSource with resources
        assert source is not None
        # Should have 4 resources: team_season, players_passing, players_rushing, players_kicking
        resource_names = [r.name for r in source.resources.values()]
        assert len(resource_names) == 4
        assert "wepa_team_season" in resource_names
        assert "wepa_players_passing" in resource_names
        assert "wepa_players_rushing" in resource_names
        assert "wepa_players_kicking" in resource_names
