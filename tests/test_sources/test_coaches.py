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


class TestCoachProfilesSource:
    def test_requires_coach_ids(self):
        from src.pipelines.sources.coaches import coach_profiles_source

        with pytest.raises(ValueError, match="coach_ids parameter is required"):
            coach_profiles_source(coach_ids=[])

    def test_returns_coach_profiles_resource(self):
        from src.pipelines.sources.coaches import coach_profiles_source

        with patch("src.pipelines.sources.coaches.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            source = coach_profiles_source(coach_ids=[101])

            assert set(source.resources.keys()) == {"coach_profiles"}


class TestCoachProfilesResource:
    def test_one_call_per_coach_id_with_coach_id_param(self):
        from src.pipelines.sources.coaches import coach_profiles_resource

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = {"id": 101}

            list(coach_profiles_resource(coach_ids=[101, 202]))

            assert mock_make_request.call_count == 2
            calls = mock_make_request.call_args_list
            assert calls[0].args[1] == "/coaches/profile"
            assert calls[0].kwargs["params"] == {"coachId": 101}
            assert calls[1].kwargs["params"] == {"coachId": 202}

    def test_yields_the_bare_object_response(self):
        """CoachProfile per the OpenAPI spec is a single object, not a list."""
        from src.pipelines.sources.coaches import coach_profiles_resource

        row = {
            "id": 101,
            "firstName": "Ryan",
            "lastName": "Day",
            "displayName": "Ryan Day",
            "currentTeam": {"id": 333, "school": "Ohio State", "conference": "Big Ten"},
            "career": {
                "games": 90,
                "wins": 80,
                "losses": 10,
                "ties": 0,
                "winPercentage": 0.889,
                "seasons": 7,
                "teams": 1,
                "firstYear": 2019,
                "lastYear": 2026,
            },
            "birthDate": None,
            "almaMater": {"id": 55, "school": "New Hampshire"},
            "graduationYear": None,
            "wikidataId": None,
            "hallOfFameYear": None,
        }

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = row

            results = list(coach_profiles_resource(coach_ids=[101]))

            assert len(results) == 1
            assert results[0]["id"] == 101
            assert results[0]["currentTeam"]["school"] == "Ohio State"

    def test_response_wrapped_in_a_single_item_list_is_also_accepted(self):
        from src.pipelines.sources.coaches import coach_profiles_resource

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = [{"id": 101}]

            results = list(coach_profiles_resource(coach_ids=[101]))

            assert len(results) == 1
            assert results[0]["id"] == 101

    def test_row_missing_id_is_skipped(self):
        from src.pipelines.sources.coaches import coach_profiles_resource

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = {"firstName": "No", "lastName": "Id"}

            assert list(coach_profiles_resource(coach_ids=[101])) == []

    def test_400_response_skips_coach_and_continues(self):
        from src.pipelines.sources.coaches import coach_profiles_resource

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(400), {"id": 202}]

            results = list(coach_profiles_resource(coach_ids=[101, 202]))

            assert len(results) == 1
            assert results[0]["id"] == 202

    def test_404_response_skips_coach_and_continues(self):
        from src.pipelines.sources.coaches import coach_profiles_resource

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(404), {"id": 202}]

            results = list(coach_profiles_resource(coach_ids=[101, 202]))

            assert len(results) == 1
            assert results[0]["id"] == 202

    def test_other_status_errors_are_not_swallowed(self):
        from dlt.extract.exceptions import ResourceExtractionError

        from src.pipelines.sources.coaches import coach_profiles_resource

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = _http_error(500)

            with pytest.raises(ResourceExtractionError) as exc_info:
                list(coach_profiles_resource(coach_ids=[101]))

            assert isinstance(exc_info.value.__cause__, httpx.HTTPStatusError)
            assert exc_info.value.__cause__.response.status_code == 500

    def test_merge_disposition_and_primary_key(self):
        from src.pipelines.sources.coaches import coach_profiles_resource

        with patch("src.pipelines.sources.coaches.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = coach_profiles_resource(coach_ids=[101])

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == {"id"}


# ---------------------------------------------------------------------------
# run_coach_profiles_pipeline (src/pipelines/run.py) -- mocked psycopg2/
# rate-limiter/dlt, no DB, no network. Mirrors
# test_sources/test_metrics_wp.py's TestRunMetricsWpPipelineBatching /
# TestRunMetricsWpPipelineBudgetGuard split, since coach_profiles is the
# same DB-set-difference-drainer shape as metrics_wp.
# ---------------------------------------------------------------------------


def _mock_coach_profiles_conn(candidate_ids, existing_ids=None, existing_raises=None):
    """Build a MagicMock psycopg2 connection matching
    run_coach_profiles_pipeline's `with conn.cursor() as cur:` usage, with
    the candidates query returning `candidate_ids` and the existing-ids
    query either returning `existing_ids` or raising `existing_raises`."""
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    if existing_raises is not None:
        cur.fetchall.side_effect = [[(cid,) for cid in candidate_ids], existing_raises]
    else:
        cur.fetchall.side_effect = [
            [(cid,) for cid in candidate_ids],
            [(cid,) for cid in (existing_ids or [])],
        ]
    return conn


class TestRunCoachProfilesPipelineBatching:
    def test_chunks_missing_coaches_into_batches_of_50(self):
        from src.pipelines.run import run_coach_profiles_pipeline

        conn = _mock_coach_profiles_conn(candidate_ids=list(range(1, 121)), existing_ids=[])

        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = "load-info"

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.coach_profiles_source") as mock_source,
        ):
            result = run_coach_profiles_pipeline(max_coaches=200)

        assert result["missing"] == 120
        assert result["batches"] == 3
        assert mock_pipeline.run.call_count == 3
        batch_sizes = [len(call.kwargs["coach_ids"]) for call in mock_source.call_args_list]
        assert batch_sizes == [50, 50, 20]

    def test_already_profiled_coaches_are_excluded(self):
        from src.pipelines.run import run_coach_profiles_pipeline

        conn = _mock_coach_profiles_conn(candidate_ids=[1, 2, 3], existing_ids=[1, 2])

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.coach_profiles_source") as mock_source,
        ):
            result = run_coach_profiles_pipeline()

        assert result["candidates"] == 3
        assert result["missing"] == 1
        mock_source.assert_called_once()
        assert mock_source.call_args.kwargs["coach_ids"] == [3]

    def test_undefined_table_on_fresh_backfill_treated_as_empty(self):
        """ref.coach_seasons/ref.coach_profiles don't exist until their
        first successful load (dlt table-on-first-write) -- a fresh
        backfill must treat that as 'nothing loaded yet', not crash."""
        import psycopg2.errors

        from src.pipelines.run import run_coach_profiles_pipeline

        conn = _mock_coach_profiles_conn(
            candidate_ids=[1, 2], existing_raises=psycopg2.errors.UndefinedTable()
        )

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.coach_profiles_source") as mock_source,
        ):
            result = run_coach_profiles_pipeline()

        assert result["missing"] == 2
        mock_source.assert_called_once()

    def test_no_missing_coaches_skips_pipeline_run(self):
        from src.pipelines.run import run_coach_profiles_pipeline

        conn = _mock_coach_profiles_conn(candidate_ids=[1], existing_ids=[1])

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
        ):
            result = run_coach_profiles_pipeline()

        assert result["missing"] == 0
        assert result["batches"] == 0
        mock_pipeline.run.assert_not_called()

    def test_cap_defers_the_rest_of_the_backlog(self):
        from src.pipelines.run import run_coach_profiles_pipeline

        conn = _mock_coach_profiles_conn(candidate_ids=list(range(1, 11)), existing_ids=[])

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.coach_profiles_source"),
        ):
            result = run_coach_profiles_pipeline(max_coaches=4)

        assert result["missing"] == 10
        assert result["loaded_this_run"] == 4
        assert result["deferred"] == 6


class TestRunCoachProfilesPipelineBudgetGuard:
    def test_insufficient_budget_refuses_without_running_pipeline(self):
        from src.pipelines.run import run_coach_profiles_pipeline

        conn = _mock_coach_profiles_conn(candidate_ids=list(range(1, 11)), existing_ids=[])

        mock_rate_limiter = MagicMock()
        mock_rate_limiter.check_budget.return_value = False
        mock_rate_limiter.remaining = 3

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.get_rate_limiter", return_value=mock_rate_limiter),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
        ):
            result = run_coach_profiles_pipeline()

        mock_rate_limiter.check_budget.assert_called_once_with(10)
        mock_pipeline.run.assert_not_called()
        assert result["batches"] == 0
        assert "error" in result

    def test_sufficient_budget_proceeds(self):
        from src.pipelines.run import run_coach_profiles_pipeline

        conn = _mock_coach_profiles_conn(candidate_ids=[1], existing_ids=[])

        mock_rate_limiter = MagicMock()
        mock_rate_limiter.check_budget.return_value = True

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.get_rate_limiter", return_value=mock_rate_limiter),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.coach_profiles_source"),
        ):
            result = run_coach_profiles_pipeline()

        assert result["batches"] == 1
        mock_pipeline.run.assert_called_once()
