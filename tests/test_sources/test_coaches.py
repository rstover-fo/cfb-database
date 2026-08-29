"""Tests for the coach-seasons / coach-tenures sources.

`/coaches/seasons` and `/coaches/tenures` both 400'd on every probe call
made while building this source (bulk calls require coachId/team/year --
see the module docstring), so these tests construct fixtures directly from
the CFBD OpenAPI spec's DetailedCoachSeason / CoachTenure schemas rather
than a captured response.
"""

from unittest.mock import MagicMock, patch

import httpx
import pytest


def _http_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://api.collegefootballdata.com/coaches/seasons")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


def _coach_season_row(coach_id=101, year=2024, team_id=333, school="Ohio State"):
    return {
        "coach": {"id": coach_id, "firstName": "Ryan", "lastName": "Day"},
        "team": {"id": team_id, "school": school, "conference": "Big Ten"},
        "year": year,
        "games": 13,
        "wins": 11,
        "losses": 2,
        "ties": 0,
        "winPercentage": 0.846,
    }


class TestCoachesSource:
    def test_returns_only_coach_seasons(self):
        """coach_tenures must NOT be part of the default cfbd_coaches source
        -- it is per-team fan-out and deliberately opt-in (own source
        function), mirroring metrics_wp_source's split from metrics_source."""
        from src.pipelines.sources.coaches import coaches_source

        source = coaches_source(years=[2024])

        assert set(source.resources.keys()) == {"coach_seasons"}


class TestCoachSeasonsResource:
    def test_one_call_per_year_with_year_param(self):
        from src.pipelines.sources.coaches import coach_seasons_resource

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(coach_seasons_resource(years=[2023, 2024]))

            assert mock_make_request.call_count == 2
            calls = mock_make_request.call_args_list
            assert calls[0].args[1] == "/coaches/seasons"
            assert calls[0].kwargs["params"] == {"year": 2023}
            assert calls[1].kwargs["params"] == {"year": 2024}

    def test_yields_well_formed_rows(self):
        from src.pipelines.sources.coaches import coach_seasons_resource

        row = _coach_season_row()

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = [row]

            results = list(coach_seasons_resource(years=[2024]))

            assert len(results) == 1
            assert results[0]["coach"]["id"] == 101
            assert results[0]["team"]["id"] == 333
            assert results[0]["year"] == 2024

    def test_row_missing_pk_field_is_skipped_defensively(self):
        """A coach or team without an id (or a missing year) must not reach
        dlt's merge -- it would either crash the load or, worse, merge under
        a NULL key."""
        from src.pipelines.sources.coaches import coach_seasons_resource

        good = _coach_season_row(coach_id=101)
        missing_coach_id = _coach_season_row(coach_id=202)
        missing_coach_id["coach"] = {"firstName": "No", "lastName": "Id"}
        missing_team = dict(_coach_season_row(coach_id=303))
        missing_team["team"] = None

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = [good, missing_coach_id, missing_team]

            results = list(coach_seasons_resource(years=[2024]))

            assert len(results) == 1
            assert results[0]["coach"]["id"] == 101

    def test_400_response_is_skipped(self):
        from src.pipelines.sources.coaches import coach_seasons_resource

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = _http_error(400)

            assert list(coach_seasons_resource(years=[2024])) == []

    def test_merge_disposition_and_primary_key(self):
        from src.pipelines.sources.coaches import coach_seasons_resource

        with patch("src.pipelines.sources.coaches.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = coach_seasons_resource(years=[2024])

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == {"coach__id", "year", "team__id"}


class TestCoachTenuresSource:
    def test_requires_teams(self):
        from src.pipelines.sources.coaches import coach_tenures_source

        with pytest.raises(ValueError, match="teams parameter is required"):
            coach_tenures_source(teams=[])

    def test_returns_coach_tenures_resource(self):
        from src.pipelines.sources.coaches import coach_tenures_source

        with patch("src.pipelines.sources.coaches.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            source = coach_tenures_source(teams=["Alabama"])

            assert set(source.resources.keys()) == {"coach_tenures"}


class TestCoachTenuresResource:
    def test_one_call_per_team_with_team_param(self):
        from src.pipelines.sources.coaches import coach_tenures_resource

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(coach_tenures_resource(teams=["Alabama", "Georgia"]))

            assert mock_make_request.call_count == 2
            calls = mock_make_request.call_args_list
            assert calls[0].args[1] == "/coaches/tenures"
            assert calls[0].kwargs["params"] == {"team": "Alabama"}
            assert calls[1].kwargs["params"] == {"team": "Georgia"}

    def test_yields_rows_with_top_level_id(self):
        from src.pipelines.sources.coaches import coach_tenures_resource

        row = {
            "id": 555,
            "coach": {"id": 101, "firstName": "Nick", "lastName": "Saban"},
            "team": {"id": 333, "school": "Alabama"},
            "hireDate": "2007-01-03",
            "startYear": 2007,
            "endYear": 2023,
            "effectiveStart": None,
            "effectiveEnd": None,
            "isInterim": False,
            "active": False,
            "seasons": 17,
            "record": {"games": 215, "wins": 201, "losses": 29, "ties": 0, "winPercentage": 0.874},
            "attributionComplete": True,
        }

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = [row]

            results = list(coach_tenures_resource(teams=["Alabama"]))

            assert len(results) == 1
            assert results[0]["id"] == 555

    def test_row_missing_id_is_skipped(self):
        from src.pipelines.sources.coaches import coach_tenures_resource

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = [{"coach": {"id": 1}}]

            assert list(coach_tenures_resource(teams=["Alabama"])) == []

    def test_400_response_skips_team_and_continues(self):
        from src.pipelines.sources.coaches import coach_tenures_resource

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(400), [{"id": 1}]]

            results = list(coach_tenures_resource(teams=["FCS Team", "Alabama"]))

            assert len(results) == 1
            assert results[0]["id"] == 1

    def test_merge_disposition_and_primary_key(self):
        from src.pipelines.sources.coaches import coach_tenures_resource

        with patch("src.pipelines.sources.coaches.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = coach_tenures_resource(teams=["Alabama"])

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == {"id"}
