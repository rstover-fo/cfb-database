"""Tests for ratings data sources."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx

from src.pipelines.sources.ratings import core_ratings_resource, ratings_source

FIXTURES = Path(__file__).parent.parent / "fixtures" / "cfbd_2026"


def _http_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://api.collegefootballdata.com/ratings/srs/expanded")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


def test_ratings_source_returns_all_resources():
    """ratings_source should expose all seven rating systems."""
    source = ratings_source(years=[2024])

    assert set(source.resources.keys()) == {
        "sp_ratings",
        "elo_ratings",
        "fpi_ratings",
        "srs_ratings",
        "core_ratings",
        "sp_conference_ratings",
        "srs_expanded",
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


class TestSrsExpandedRatingsResource:
    def test_one_call_per_year_with_year_param(self):
        from src.pipelines.sources.ratings import srs_expanded_ratings_resource

        with (
            patch("src.pipelines.sources.ratings.get_client") as mock_get_client,
            patch("src.pipelines.sources.ratings.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(srs_expanded_ratings_resource(years=[2005, 2024]))

            assert mock_make_request.call_count == 2
            calls = mock_make_request.call_args_list
            assert calls[0].args[1] == "/ratings/srs/expanded"
            assert calls[0].kwargs["params"] == {"year": 2005}
            assert calls[1].kwargs["params"] == {"year": 2024}

    def test_yields_rows_from_fixture(self):
        from src.pipelines.sources.ratings import srs_expanded_ratings_resource

        with open(FIXTURES / "ratings_srs_expanded.json") as f:
            fixture = json.load(f)

        with (
            patch("src.pipelines.sources.ratings.get_client") as mock_get_client,
            patch("src.pipelines.sources.ratings.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = fixture

            results = list(srs_expanded_ratings_resource(years=[2005]))

            assert len(results) == len(fixture)
            assert results[0]["year"] == 2005
            assert results[0]["team"] == fixture[0]["team"]

    def test_400_response_is_skipped(self):
        from src.pipelines.sources.ratings import srs_expanded_ratings_resource

        with (
            patch("src.pipelines.sources.ratings.get_client") as mock_get_client,
            patch("src.pipelines.sources.ratings.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = _http_error(400)

            assert list(srs_expanded_ratings_resource(years=[2004])) == []

    def test_empty_response_is_skipped(self):
        from src.pipelines.sources.ratings import srs_expanded_ratings_resource

        with (
            patch("src.pipelines.sources.ratings.get_client") as mock_get_client,
            patch("src.pipelines.sources.ratings.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            assert list(srs_expanded_ratings_resource(years=[2004])) == []

    def test_merge_disposition_and_primary_key(self):
        from src.pipelines.sources.ratings import srs_expanded_ratings_resource

        with patch("src.pipelines.sources.ratings.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = srs_expanded_ratings_resource(years=[2024])

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == {"year", "team"}
