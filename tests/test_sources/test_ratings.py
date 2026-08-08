"""Tests for ratings data sources."""

from unittest.mock import MagicMock, patch

from src.pipelines.sources.ratings import core_ratings_resource, ratings_source


def test_ratings_source_returns_all_resources():
    """ratings_source should expose all six rating systems."""
    source = ratings_source(years=[2024])

    assert set(source.resources.keys()) == {
        "sp_ratings",
        "elo_ratings",
        "fpi_ratings",
        "srs_ratings",
        "core_ratings",
        "sp_conference_ratings",
    }


def test_core_ratings_resource_yields_rows_with_year():
    """Rows pass through with year stamped, one request per year."""
    mock_response = [
        {
            "year": 2024,
            "throughSeasonType": "postseason",
            "throughWeek": 1,
            "team": "Ohio State",
            "conference": "Big Ten",
            "overall": 42.1,
            "offense": 21.3,
            "defense": -20.8,
            "offensePlays": 950,
            "defensePlays": 940,
            "modelVersion": "core_v1",
        }
    ]

    with (
        patch("src.pipelines.sources.ratings.get_client") as mock_get_client,
        patch("src.pipelines.sources.ratings.make_request") as mock_make_request,
    ):
        mock_get_client.return_value = MagicMock()
        mock_make_request.return_value = mock_response

        results = list(core_ratings_resource(years=[2024]))

        assert len(results) == 1
        assert results[0]["year"] == 2024
        assert results[0]["team"] == "Ohio State"
        mock_make_request.assert_called_once()
        assert mock_make_request.call_args.args[1] == "/ratings/core"
        assert mock_make_request.call_args.kwargs["params"] == {"year": 2024}


def test_core_ratings_resource_skips_pre_2016_years():
    """CORE is published from 2016; earlier years must not spend API calls."""
    with (
        patch("src.pipelines.sources.ratings.get_client") as mock_get_client,
        patch("src.pipelines.sources.ratings.make_request") as mock_make_request,
    ):
        mock_get_client.return_value = MagicMock()
        mock_make_request.return_value = []

        list(core_ratings_resource(years=[2004, 2015, 2016, 2017]))

        requested_years = [
            call.kwargs["params"]["year"] for call in mock_make_request.call_args_list
        ]
        assert requested_years == [2016, 2017]
