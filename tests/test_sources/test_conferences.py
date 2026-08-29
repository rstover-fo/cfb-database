"""Tests for the conference-membership source (affiliations + changes).

Fixtures are trimmed 2026-08-29 probe captures
(tests/fixtures/cfbd_2026/conferences_*.json).
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx

FIXTURES = Path(__file__).parent.parent / "fixtures" / "cfbd_2026"


def _load(name):
    with open(FIXTURES / name) as f:
        return json.load(f)


def _http_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://api.collegefootballdata.com/conferences/changes")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


def test_conferences_source_returns_both_resources():
    from src.pipelines.sources.conferences import conferences_source

    with patch("src.pipelines.sources.conferences.get_client") as mock_get_client:
        mock_get_client.return_value = MagicMock()

        source = conferences_source(years=[2024])

        assert set(source.resources.keys()) == {"conference_affiliations", "conference_changes"}


class TestConferenceAffiliationsResource:
    def test_exactly_one_unfiltered_call(self):
        """/conferences/affiliations takes no required parameters -- a
        single bulk call covers all teams/years."""
        from src.pipelines.sources.conferences import conference_affiliations_resource

        fixture = _load("conferences_affiliations.json")

        with (
            patch("src.pipelines.sources.conferences.get_client") as mock_get_client,
            patch("src.pipelines.sources.conferences.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = fixture

            results = list(conference_affiliations_resource())

            mock_make_request.assert_called_once()
            assert mock_make_request.call_args.args[1] == "/conferences/affiliations"
            assert mock_make_request.call_args.kwargs.get("params") is None
            assert len(results) == len(fixture)
            assert results[0]["teamId"] == fixture[0]["teamId"]

    def test_merge_disposition_and_primary_key(self):
        from src.pipelines.sources.conferences import conference_affiliations_resource

        with patch("src.pipelines.sources.conferences.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = conference_affiliations_resource()

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == {"team_id", "conference_id", "start_year"}


class TestConferenceChangesResource:
    def test_one_call_per_year_with_year_param(self):
        from src.pipelines.sources.conferences import conference_changes_resource

        with (
            patch("src.pipelines.sources.conferences.get_client") as mock_get_client,
            patch("src.pipelines.sources.conferences.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(conference_changes_resource(years=[2023, 2024]))

            assert mock_make_request.call_count == 2
            calls = mock_make_request.call_args_list
            assert calls[0].args[1] == "/conferences/changes"
            assert calls[0].kwargs["params"] == {"year": 2023}
            assert calls[1].kwargs["params"] == {"year": 2024}

    def test_yields_rows_with_effective_year_already_present(self):
        """The response carries `effectiveYear` natively -- nothing needs to
        be stamped, unlike cfp_games/cfp_participants."""
        from src.pipelines.sources.conferences import conference_changes_resource

        fixture = _load("conferences_changes.json")
        assert "effectiveYear" in fixture[0]

        with (
            patch("src.pipelines.sources.conferences.get_client") as mock_get_client,
            patch("src.pipelines.sources.conferences.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = fixture

            results = list(conference_changes_resource(years=[2024]))

            assert len(results) == len(fixture)
            assert results[0]["effectiveYear"] == fixture[0]["effectiveYear"]

    def test_400_response_is_skipped(self):
        from src.pipelines.sources.conferences import conference_changes_resource

        with (
            patch("src.pipelines.sources.conferences.get_client") as mock_get_client,
            patch("src.pipelines.sources.conferences.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = _http_error(400)

            assert list(conference_changes_resource(years=[2024])) == []

    def test_merge_disposition_and_primary_key(self):
        from src.pipelines.sources.conferences import conference_changes_resource

        with patch("src.pipelines.sources.conferences.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = conference_changes_resource(years=[2024])

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == {"effective_year", "team_id"}
