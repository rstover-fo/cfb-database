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

    def test_forwards_misses_collector_to_the_resource(self):
        from src.pipelines.sources.coaches import coach_profiles_source

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = _http_error(404)

            misses: list[tuple[str, int]] = []
            source = coach_profiles_source(coach_ids=[101], misses=misses)
            list(source.resources["coach_profiles"])

            assert misses == [("101", 404)]


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

    def test_400_appends_key_and_status_code_to_misses(self):
        """PR #75 review finding A: without this collection, a terminal
        400/404 was silently dropped and re-requested by the drainer every
        run forever."""
        from src.pipelines.sources.coaches import coach_profiles_resource

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(400), {"id": 202}]

            misses: list[tuple[str, int]] = []
            list(coach_profiles_resource(coach_ids=[101, 202], misses=misses))

            assert misses == [("101", 400)]

    def test_404_appends_key_and_status_code_to_misses(self):
        from src.pipelines.sources.coaches import coach_profiles_resource

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(404), {"id": 202}]

            misses: list[tuple[str, int]] = []
            list(coach_profiles_resource(coach_ids=[101, 202], misses=misses))

            assert misses == [("101", 404)]

    def test_miss_key_is_stringified_id(self):
        """meta.fanout_misses.key is text -- the collected key must be
        str(coach_id), not the raw int."""
        from src.pipelines.sources.coaches import coach_profiles_resource

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = _http_error(404)

            misses: list[tuple[str, int]] = []
            list(coach_profiles_resource(coach_ids=[555], misses=misses))

            assert misses == [("555", 404)]
            assert isinstance(misses[0][0], str)

    def test_misses_none_is_safe(self):
        """The default -- no collector passed -- must not raise."""
        from src.pipelines.sources.coaches import coach_profiles_resource

        with (
            patch("src.pipelines.sources.coaches.get_client") as mock_get_client,
            patch("src.pipelines.sources.coaches.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(400), _http_error(404)]

            results = list(coach_profiles_resource(coach_ids=[101, 202]))

            assert results == []

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


def _mock_coach_profiles_conn(
    candidate_ids, existing_ids=None, existing_raises=None, recent_misses=None
):
    """Build a MagicMock psycopg2 connection matching
    run_coach_profiles_pipeline's `with conn.cursor() as cur:` usage: the
    candidates query returns `candidate_ids`, the existing-ids query either
    returns `existing_ids` or raises `existing_raises`, and (PR #75 review
    finding A) the third guarded query -- _fetch_recent_fanout_misses,
    reading meta.fanout_misses via _fetch_rows_or_empty -- returns
    `recent_misses` (string keys, default none)."""
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    effects = [[(cid,) for cid in candidate_ids]]
    if existing_raises is not None:
        effects.append(existing_raises)
    else:
        effects.append([(cid,) for cid in (existing_ids or [])])
    effects.append([(key,) for key in (recent_misses or [])])
    cur.fetchall.side_effect = effects
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


# ---------------------------------------------------------------------------
# meta.fanout_misses integration (PR #75 review finding A): recent-miss
# exclusion from the candidate set, excluded-count reporting, per-batch
# recording, and UndefinedTable degradation on both the read and write side.
# ---------------------------------------------------------------------------


class TestRunCoachProfilesPipelineFanoutMisses:
    def test_recent_miss_excluded_from_missing(self):
        from src.pipelines.run import run_coach_profiles_pipeline

        conn = _mock_coach_profiles_conn(
            candidate_ids=[1, 2, 3], existing_ids=[], recent_misses=["2"]
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
        assert mock_source.call_args.kwargs["coach_ids"] == [1, 3]

    def test_excluded_miss_count_is_reported(self):
        from src.pipelines.run import run_coach_profiles_pipeline

        conn = _mock_coach_profiles_conn(
            candidate_ids=[1, 2, 3], existing_ids=[], recent_misses=["2", "3"]
        )

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.coach_profiles_source"),
        ):
            result = run_coach_profiles_pipeline()

        assert result["excluded_misses"] == 2
        assert result["missing"] == 1

    def test_excluded_misses_reported_even_when_nothing_to_load(self):
        from src.pipelines.run import run_coach_profiles_pipeline

        conn = _mock_coach_profiles_conn(candidate_ids=[1], existing_ids=[], recent_misses=["1"])

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
        ):
            result = run_coach_profiles_pipeline()

        assert result["missing"] == 0
        assert result["excluded_misses"] == 1
        mock_pipeline.run.assert_not_called()

    def test_record_fanout_misses_called_once_per_batch(self):
        """coach_profiles_source is called with a `misses` collector; when
        the resource populates it (simulated here by the fake source
        implementation mutating the list it's handed), the pipeline must
        persist it via _record_fanout_misses once per batch -- three
        batches of 50/50/20 (mirroring TestRunCoachProfilesPipelineBatching's
        chunking test), only the last id of each batch missing this time."""
        from src.pipelines.run import run_coach_profiles_pipeline

        conn = _mock_coach_profiles_conn(candidate_ids=list(range(1, 121)), existing_ids=[])

        mock_pipeline = MagicMock()
        recorded = []

        def fake_source(coach_ids, *, misses=None):
            if misses is not None:
                misses.append((str(coach_ids[-1]), 404))
            return MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.coach_profiles_source", side_effect=fake_source),
            patch(
                "src.pipelines.run._record_fanout_misses",
                side_effect=lambda source, misses: recorded.append((source, misses)),
            ) as mock_record,
        ):
            result = run_coach_profiles_pipeline(max_coaches=200)

        assert result["batches"] == 3
        assert mock_record.call_count == 3
        assert recorded == [
            ("coach_profiles", [("50", 404)]),
            ("coach_profiles", [("100", 404)]),
            ("coach_profiles", [("120", 404)]),
        ]

    def test_record_fanout_misses_not_called_when_batch_has_no_misses(self):
        from src.pipelines.run import run_coach_profiles_pipeline

        conn = _mock_coach_profiles_conn(candidate_ids=[1], existing_ids=[])

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.coach_profiles_source"),
            patch("src.pipelines.run._record_fanout_misses") as mock_record,
        ):
            run_coach_profiles_pipeline()

        mock_record.assert_not_called()

    def test_undefined_table_reading_fanout_misses_yields_no_exclusion(self):
        """meta.fanout_misses not yet created (migration 056 not applied)
        must degrade to 'no exclusion', not crash the drainer."""
        import psycopg2.errors

        from src.pipelines.run import run_coach_profiles_pipeline

        conn = _mock_coach_profiles_conn(candidate_ids=[1, 2], existing_ids=[])
        cur = conn.cursor.return_value.__enter__.return_value
        # Override the third fetchall (recent-misses query) to raise instead
        # of returning an empty list.
        cur.fetchall.side_effect = [
            [(1,), (2,)],
            [],
            psycopg2.errors.UndefinedTable(),
        ]

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.coach_profiles_source") as mock_source,
        ):
            result = run_coach_profiles_pipeline()

        assert result["missing"] == 2
        assert result["excluded_misses"] == 0
        mock_source.assert_called_once()

    def test_undefined_table_writing_fanout_misses_warns_not_crashes(self, caplog):
        """migration 056 not applied: _record_fanout_misses must swallow
        UndefinedTable and log a warning, never propagate."""
        import psycopg2.errors

        from src.pipelines.run import _record_fanout_misses

        write_conn = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=write_conn),
            patch(
                "psycopg2.extras.execute_values",
                side_effect=psycopg2.errors.UndefinedTable(),
            ),
            caplog.at_level("WARNING"),
        ):
            _record_fanout_misses("coach_profiles", [("101", 404)])

        assert any("migration 056" in rec.message for rec in caplog.records)
        write_conn.close.assert_called_once()

    def test_record_fanout_misses_noop_on_empty_list(self):
        """No misses to record -- must not even open a connection."""
        from src.pipelines.run import _record_fanout_misses

        with patch("psycopg2.connect") as mock_connect:
            _record_fanout_misses("coach_profiles", [])

        mock_connect.assert_not_called()


class TestFanoutMissUpsertSqlDrift:
    def test_upsert_sql_shape(self):
        from src.pipelines.run import _FANOUT_MISS_UPSERT_SQL

        sql = " ".join(_FANOUT_MISS_UPSERT_SQL.split())
        assert "INSERT INTO meta.fanout_misses (source, key, status_code)" in sql
        assert "ON CONFLICT (source, key) DO UPDATE SET" in sql
        assert "attempts = meta.fanout_misses.attempts + 1" in sql
        assert "status_code = EXCLUDED.status_code" in sql
        assert "last_attempt_at = now()" in sql

    def test_record_fanout_misses_uses_execute_values_with_upsert_sql(self):
        from src.pipelines.run import _FANOUT_MISS_UPSERT_SQL, _record_fanout_misses

        conn = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("psycopg2.extras.execute_values") as mock_execute_values,
        ):
            _record_fanout_misses("coach_profiles", [("101", 404), ("202", 400)])

        mock_execute_values.assert_called_once()
        args, _ = mock_execute_values.call_args
        _, sql, values = args
        assert sql == _FANOUT_MISS_UPSERT_SQL
        assert values == [("coach_profiles", "101", 404), ("coach_profiles", "202", 400)]
