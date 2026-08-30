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

    def test_stamps_season_when_missing_from_response(self):
        """The 2025 /playoffs/cfp response evidently omits the top-level
        `season` field the 2024 fixture carries -- this is what broke the
        live 2025 backfill (UnboundColumnException on the `season` PK).
        Stamp it from the request year when absent."""
        from src.pipelines.sources.playoffs import cfp_bracket_resource

        fixture = _load("playoffs_cfp.json")
        record_without_season = dict(fixture[0])
        del record_without_season["season"]

        with (
            patch("src.pipelines.sources.playoffs.get_client") as mock_get_client,
            patch("src.pipelines.sources.playoffs.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = [record_without_season]

            results = list(cfp_bracket_resource(years=[2025]))

            assert len(results) == 1
            assert results[0]["season"] == 2025

    def test_bare_dict_response_yields_one_row(self):
        """The live 2025 response is a bare JSON object, not a one-element
        array like 2024 -- `yield from data` on a dict iterates its string
        keys, which is what broke the live 2025 backfill a second time
        (`'str' object has no attribute 'get'`). A dict response must
        normalize to a single yielded record."""
        from src.pipelines.sources.playoffs import cfp_bracket_resource

        fixture = _load("playoffs_cfp.json")
        bare_object = dict(fixture[0])

        with (
            patch("src.pipelines.sources.playoffs.get_client") as mock_get_client,
            patch("src.pipelines.sources.playoffs.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = bare_object

            results = list(cfp_bracket_resource(years=[2025]))

            assert len(results) == 1
            assert results[0]["season"] == 2024  # API-provided value preserved
            assert "participants" in results[0]
            assert "rounds" in results[0]

    def test_bare_dict_response_stamps_season_when_missing(self):
        from src.pipelines.sources.playoffs import cfp_bracket_resource

        fixture = _load("playoffs_cfp.json")
        bare_object = dict(fixture[0])
        del bare_object["season"]

        with (
            patch("src.pipelines.sources.playoffs.get_client") as mock_get_client,
            patch("src.pipelines.sources.playoffs.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = bare_object

            results = list(cfp_bracket_resource(years=[2025]))

            assert len(results) == 1
            assert results[0]["season"] == 2025

    def test_keeps_api_provided_season(self):
        """When the response does carry `season` (the 2024 shape), the API
        value wins over the request year -- the stamp is defensive only."""
        from src.pipelines.sources.playoffs import cfp_bracket_resource

        fixture = _load("playoffs_cfp.json")
        assert fixture[0]["season"] == 2024

        with (
            patch("src.pipelines.sources.playoffs.get_client") as mock_get_client,
            patch("src.pipelines.sources.playoffs.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = fixture

            # Request year deliberately differs from the fixture's own season
            # to prove the API-provided value is preserved, not overwritten.
            results = list(cfp_bracket_resource(years=[2025]))

            assert results[0]["season"] == 2024


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
