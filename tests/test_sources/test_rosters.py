"""Tests for roster data source."""

import pytest
from unittest.mock import patch, MagicMock


def test_rosters_resource_yields_players():
    """Roster endpoint should yield player records with team/year context."""
    from src.pipelines.sources.rosters import rosters_resource

    mock_response = [
        {"id": 12345, "first_name": "Jalen", "last_name": "Milroe", "position": "QB", "jersey": 4},
        {"id": 12346, "first_name": "Ryan", "last_name": "Williams", "position": "WR", "jersey": 2},
    ]

    with patch("src.pipelines.sources.rosters.get_client") as mock_get_client, \
         patch("src.pipelines.sources.rosters.make_request") as mock_make_request:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_make_request.return_value = mock_response

        results = list(rosters_resource(teams=["Alabama"], years=[2024]))

        assert len(results) == 2
        assert results[0]["id"] == 12345
        assert results[0]["team"] == "Alabama"
        assert results[0]["year"] == 2024


def test_rosters_resource_iterates_teams_and_years():
    """Should call API for each team/year combination."""
    from src.pipelines.sources.rosters import rosters_resource

    with patch("src.pipelines.sources.rosters.get_client") as mock_get_client, \
         patch("src.pipelines.sources.rosters.make_request") as mock_make_request:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_make_request.return_value = []

        list(rosters_resource(teams=["Alabama", "Georgia"], years=[2023, 2024]))

        # 2 teams x 2 years = 4 API calls
        assert mock_make_request.call_count == 4


def test_rosters_resource_handles_empty_response():
    """Should handle teams with no roster data gracefully."""
    from src.pipelines.sources.rosters import rosters_resource

    with patch("src.pipelines.sources.rosters.get_client") as mock_get_client, \
         patch("src.pipelines.sources.rosters.make_request") as mock_make_request:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_make_request.return_value = []

        results = list(rosters_resource(teams=["Alabama"], years=[2024]))

        assert len(results) == 0


def test_rosters_source_returns_resource():
    """Rosters source should return the rosters resource."""
    from src.pipelines.sources.rosters import rosters_source

    with patch("src.pipelines.sources.rosters.get_client") as mock_get_client:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        source = rosters_source(teams=["Alabama"], years=[2024])

        # Source should return a list containing the resource
        assert source is not None
