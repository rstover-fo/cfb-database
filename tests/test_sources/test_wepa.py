"""Tests for WEPA (opponent-adjusted EPA) data source."""

import pytest
from unittest.mock import patch, MagicMock


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

    with patch("src.pipelines.sources.wepa.get_client") as mock_get_client, \
         patch("src.pipelines.sources.wepa.make_request") as mock_make_request:
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

    with patch("src.pipelines.sources.wepa.get_client") as mock_get_client, \
         patch("src.pipelines.sources.wepa.make_request") as mock_make_request:
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

    with patch("src.pipelines.sources.wepa.get_client") as mock_get_client, \
         patch("src.pipelines.sources.wepa.make_request") as mock_make_request:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_make_request.return_value = mock_response

        results = list(wepa_players_passing_resource(years=[2024]))

        assert len(results) == 1
        assert results[0]["id"] == 12345


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
