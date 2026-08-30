"""Tests for the CFP playoff bracket/games/participants source.

Fixtures are trimmed 2026-08-29 probe captures
(tests/fixtures/cfbd_2026/playoffs_cfp*.json).
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
    request = httpx.Request("GET", "https://api.collegefootballdata.com/playoffs/cfp")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


def test_playoffs_source_returns_three_resources():
    from src.pipelines.sources.playoffs import playoffs_source

    source = playoffs_source(years=[2024])

    assert set(source.resources.keys()) == {"cfp_bracket", "cfp_games", "cfp_participants"}


class TestCfpBracketResource:
    def test_skips_pre_2014_years_without_spending_calls(self):
        """CFP_START = 2014; a full backfill must not spend calls on earlier
        years that always come back empty (clone of
        test_core_ratings_resource_skips_pre_2016_years)."""
        from src.pipelines.sources.playoffs import cfp_bracket_resource

        with (
            patch("src.pipelines.sources.playoffs.get_client") as mock_get_client,
            patch("src.pipelines.sources.playoffs.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(cfp_bracket_resource(years=[2004, 2013, 2014, 2024]))

            requested_years = [
                call.kwargs["params"]["year"] for call in mock_make_request.call_args_list
            ]
            assert requested_years == [2014, 2024]

    def test_yields_the_nested_bracket_document(self):
        from src.pipelines.sources.playoffs import cfp_bracket_resource

        fixture = _load("playoffs_cfp.json")

        with (
            patch("src.pipelines.sources.playoffs.get_client") as mock_get_client,
            patch("src.pipelines.sources.playoffs.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = fixture

            results = list(cfp_bracket_resource(years=[2024]))

            assert len(results) == 1
            assert results[0]["season"] == 2024
            assert "participants" in results[0]
            assert "rounds" in results[0]
            mock_make_request.assert_called_once()
            assert mock_make_request.call_args.args[1] == "/playoffs/cfp"
            assert mock_make_request.call_args.kwargs["params"] == {"year": 2024}

    def test_400_response_is_skipped(self):
        from src.pipelines.sources.playoffs import cfp_bracket_resource

        with (
            patch("src.pipelines.sources.playoffs.get_client") as mock_get_client,
            patch("src.pipelines.sources.playoffs.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = _http_error(400)

            results = list(cfp_bracket_resource(years=[2024]))

            assert results == []

    def test_empty_response_is_skipped(self):
        from src.pipelines.sources.playoffs import cfp_bracket_resource

        with (
            patch("src.pipelines.sources.playoffs.get_client") as mock_get_client,
            patch("src.pipelines.sources.playoffs.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            results = list(cfp_bracket_resource(years=[2024]))

            assert results == []


class TestCfpGamesResource:
    def test_skips_pre_2014_years(self):
        from src.pipelines.sources.playoffs import cfp_games_resource

        with (
            patch("src.pipelines.sources.playoffs.get_client") as mock_get_client,
            patch("src.pipelines.sources.playoffs.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(cfp_games_resource(years=[2013, 2014]))

            requested_years = [
                call.kwargs["params"]["year"] for call in mock_make_request.call_args_list
            ]
            assert requested_years == [2014]

    def test_stamps_season_onto_every_row(self):
        """The response carries no `season` field -- it must be stamped from
        the request year."""
        from src.pipelines.sources.playoffs import cfp_games_resource

        fixture = _load("playoffs_cfp_games.json")
        assert "season" not in fixture[0]

        with (
            patch("src.pipelines.sources.playoffs.get_client") as mock_get_client,
            patch("src.pipelines.sources.playoffs.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = fixture

            results = list(cfp_games_resource(years=[2024]))

            assert len(results) == len(fixture)
            for row in results:
                assert row["season"] == 2024
            assert mock_make_request.call_args.args[1] == "/playoffs/cfp/games"
            assert mock_make_request.call_args.kwargs["params"] == {"year": 2024}

    def test_400_response_is_skipped(self):
        from src.pipelines.sources.playoffs import cfp_games_resource

        with (
            patch("src.pipelines.sources.playoffs.get_client") as mock_get_client,
            patch("src.pipelines.sources.playoffs.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = _http_error(400)

            assert list(cfp_games_resource(years=[2024])) == []


class TestCfpParticipantsResource:
    def test_skips_pre_2014_years(self):
        from src.pipelines.sources.playoffs import cfp_participants_resource

        with (
            patch("src.pipelines.sources.playoffs.get_client") as mock_get_client,
            patch("src.pipelines.sources.playoffs.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(cfp_participants_resource(years=[2013, 2014]))

            requested_years = [
                call.kwargs["params"]["year"] for call in mock_make_request.call_args_list
            ]
            assert requested_years == [2014]

    def test_stamps_season_onto_every_row(self):
        from src.pipelines.sources.playoffs import cfp_participants_resource

        fixture = _load("playoffs_cfp_participants.json")
        assert "season" not in fixture[0]

        with (
            patch("src.pipelines.sources.playoffs.get_client") as mock_get_client,
            patch("src.pipelines.sources.playoffs.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = fixture

            results = list(cfp_participants_resource(years=[2024]))

            assert len(results) == len(fixture)
            for row in results:
                assert row["season"] == 2024
                assert "team" in row
            assert mock_make_request.call_args.args[1] == "/playoffs/cfp/participants"
            assert mock_make_request.call_args.kwargs["params"] == {"year": 2024}

    def test_merge_disposition_and_primary_key(self):
        from src.pipelines.sources.playoffs import cfp_participants_resource

        with patch("src.pipelines.sources.playoffs.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = cfp_participants_resource(years=[2024])

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == {"season", "team__id"}
