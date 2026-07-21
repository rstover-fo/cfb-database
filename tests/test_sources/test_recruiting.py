"""Tests for recruiting source v2 response-shape adaptations."""

from unittest.mock import MagicMock, patch

from src.pipelines.sources.recruiting import recruiting_groups_resource


def test_recruiting_groups_stamps_year_and_requests_year_range():
    """v2 /recruiting/groups has no year in the response; the resource must
    request startYear=endYear=year and stamp year onto every row."""
    mock_response = [
        {
            "team": "Baylor",
            "conference": "Big 12",
            "positionGroup": "All Positions",
            "averageRating": 0.83,
            "totalRating": 42.7,
            "commits": 51,
            "averageStars": 2.96,
        },
        {
            "team": "Baylor",
            "conference": "Big 12",
            "positionGroup": None,  # nullable in the v2 schema
            "averageRating": 0.80,
            "totalRating": 12.0,
            "commits": 15,
            "averageStars": 2.8,
        },
    ]

    with (
        patch("src.pipelines.sources.recruiting.get_client") as mock_get_client,
        patch("src.pipelines.sources.recruiting.make_request") as mock_make_request,
    ):
        mock_get_client.return_value = MagicMock()
        mock_make_request.return_value = mock_response

        rows = list(recruiting_groups_resource(years=[2025]))

        (_, path), kwargs = mock_make_request.call_args
        params = kwargs["params"]

    assert path == "/recruiting/groups"
    assert params == {"startYear": 2025, "endYear": 2025}
    assert all(r["year"] == 2025 for r in rows)
    # NULL positionGroup would violate the (year, team, position_group) merge key
    assert rows[1]["positionGroup"] == "All Positions"
