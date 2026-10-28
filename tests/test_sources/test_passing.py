"""Tests for the /passing charting source (spec v5.25.0, air yards, aDOT,
pass depth/direction/location, YAC).

Fixtures are 2026-08-30 probe captures (tests/fixtures/cfbd_2026/passing_*.json).
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

FIXTURES = Path(__file__).parent.parent / "fixtures" / "cfbd_2026"

# The three game-grain resources all walk the same (season_type, week) pairs
# -- regular season weeks 1-16, postseason weeks 1-4 -- cloned from
# stats.py's player_success_game_resource.
EXPECTED_WEEK_PARAMS = [{"seasonType": "regular", "week": w} for w in range(1, 17)] + [
    {"seasonType": "postseason", "week": w} for w in range(1, 5)
]


def _load_fixture(name):
    with open(FIXTURES / name) as f:
        return json.load(f)


def _http_error(status_code: int, path: str = "/passing/plays") -> httpx.HTTPStatusError:
    request = httpx.Request("GET", f"https://api.collegefootballdata.com{path}")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


def test_passing_source_returns_the_three_week_grain_resources():
    """Was five until 2026-10-22, when the two season-grain aggregates moved
    into passing_season_source (Daily Season Load #99-#101 -- see the split
    tests below)."""
    from src.pipelines.sources.passing import passing_source

    source = passing_source(years=[2025])

    assert set(source.resources.keys()) == {
        "passing_plays",
        "passing_player_games",
        "passing_team_games",
    }


class TestPassingSeasonSourceSplit:
    """2026-10-22 (Daily Season Load #99-#101): /passing/players/season timed
    out after retries three daily runs straight -- as a client read timeout,
    then as an upstream 504 once PR #155's 120s read budget outlasted CFBD's
    own gateway. A dlt source failure discards every sibling resource's
    extract, so each timeout also threw away the ~60 week-grain calls the
    passing source had already fetched. The two season-grain aggregates now
    ship in their own source (passing_season_source), following the pattern
    PR #157 established for the player_success split."""

    def test_passing_season_source_contains_only_the_two_season_resources(self):
        from src.pipelines.sources.passing import passing_season_source

        source = passing_season_source(years=[2025])

        assert set(source.resources.keys()) == {
            "passing_player_season",
            "passing_team_season",
        }

    def test_passing_source_no_longer_carries_the_season_resources(self):
        """The isolation guarantee: with the season aggregates outside
        passing_source, a /passing/players/season failure structurally cannot
        discard the week-grain extract."""
        from src.pipelines.sources.passing import passing_source

        names = set(passing_source(years=[2025]).resources.keys())

        assert "passing_player_season" not in names
        assert "passing_team_season" not in names

    def test_split_preserves_the_pre_split_resource_set(self):
        """The union across both sources is exactly the pre-split five-resource
        list -- no resource gained, lost, or duplicated, so a caller loading
        both sources still loads everything."""
        from src.pipelines.sources.passing import passing_season_source, passing_source

        week_names = set(passing_source(years=[2025]).resources.keys())
        season_names = set(passing_season_source(years=[2025]).resources.keys())

        assert week_names & season_names == set()
        assert week_names | season_names == {
            "passing_plays",
            "passing_player_games",
            "passing_team_games",
            "passing_player_season",
            "passing_team_season",
        }

    def test_table_schemas_and_pks_are_unchanged(self):
        """The split reuses the same resource functions, so table names, merge
        dispositions, and primary keys are byte-identical to the pre-split
        load -- no DB change, existing rows untouched."""
        from src.pipelines.sources.passing import passing_season_source

        source = passing_season_source(years=[2025])
        expected = {
            "passing_player_season": {"season", "player_id", "team"},
            "passing_team_season": {"season", "team"},
        }
        for name, pk in expected.items():
            resource = source.resources[name]
            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            assert schema["name"] == name
            pk_columns = {
                col_name for col_name, col in schema["columns"].items() if col.get("primary_key")
            }
            assert pk_columns == pk

    def test_split_source_keeps_the_cfbd_passing_schema_identity(self):
        """PR #157's P1 finding, applied here: the live tables' column layouts
        are a product of the stored `cfbd_passing` dlt schema's history. A new
        schema name would start a fresh schema and re-infer types against the
        existing destination tables on first load instead of adopting them, so
        the split source must keep dlt schema name `cfbd_passing`."""
        from src.pipelines.sources.passing import passing_season_source, passing_source

        assert passing_season_source(years=[2025]).schema.name == "cfbd_passing"
        assert passing_season_source(years=[2025]).schema.name == (
            passing_source(years=[2025]).schema.name
        )

    def test_runner_keeps_the_cfbd_passing_pipeline_identity(self):
        """Companion to the schema-identity test above: dlt restores stored
        schemas from the destination keyed by pipeline_name, so the runner
        must reuse pipeline_name="cfbd_passing" (and dataset_name="stats") or
        a fresh CI runner would normalize against an empty schema. Same
        source-text pattern as tests/test_backfill_refresh.py's pipeline
        identity test (no DB, no dlt.pipeline() call)."""
        import inspect

        from src.pipelines.run import run_passing_season_pipeline

        src = inspect.getsource(run_passing_season_pipeline)
        assert 'pipeline_name="cfbd_passing"' in src
        assert 'dataset_name="stats"' in src


class TestResourceCompositionTable:
    """Locks in table/PK/write-disposition per the spec's resource table."""

    @pytest.mark.parametrize(
        "resource_name,pk",
        [
            ("passing_plays_resource", {"game_id", "play_id"}),
            ("passing_player_games_resource", {"game_id", "player_id"}),
            ("passing_team_games_resource", {"game_id", "team"}),
            ("passing_player_season_resource", {"season", "player_id", "team"}),
            ("passing_team_season_resource", {"season", "team"}),
        ],
    )
    def test_merge_disposition_and_primary_key(self, resource_name, pk):
        import src.pipelines.sources.passing as passing_mod

        with patch("src.pipelines.sources.passing.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource_fn = getattr(passing_mod, resource_name)
            resource = resource_fn(years=[2025])

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == pk


class TestEraGuardSkipsPre2025:
    """PASSING_DATA_START = 2025 -- 2024/2014 return 200 with zero rows
    (probed live, two runs); the era guard must skip them without spending
    a call at all."""

    @pytest.mark.parametrize(
        "resource_name",
        [
            "passing_plays_resource",
            "passing_player_games_resource",
            "passing_team_games_resource",
            "passing_player_season_resource",
            "passing_team_season_resource",
        ],
    )
    def test_pre_2025_years_spend_zero_calls(self, resource_name):
        import src.pipelines.sources.passing as passing_mod

        with (
            patch("src.pipelines.sources.passing.get_client") as mock_get_client,
            patch("src.pipelines.sources.passing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            resource_fn = getattr(passing_mod, resource_name)
            results = list(resource_fn(years=[2004, 2014, 2024]))

            mock_make_request.assert_not_called()
            assert results == []


class TestWeekIterationParamSequence:
    """The three game-grain endpoints 400 on a bare year (week-or-team
    required); this asserts the exact (year, seasonType, week) sequence."""

    @pytest.mark.parametrize(
        "resource_name,path",
        [
            ("passing_plays_resource", "/passing/plays"),
            ("passing_player_games_resource", "/passing/players/games"),
            ("passing_team_games_resource", "/passing/teams/games"),
        ],
    )
    def test_iterates_regular_1_16_and_postseason_1_4(self, resource_name, path):
        import src.pipelines.sources.passing as passing_mod

        with (
            patch("src.pipelines.sources.passing.get_client") as mock_get_client,
            patch("src.pipelines.sources.passing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            resource_fn = getattr(passing_mod, resource_name)
            list(resource_fn(years=[2025]))

            calls = mock_make_request.call_args_list
            assert len(calls) == 20
            for call in calls:
                assert call.args[1] == path
            actual_params = [
                {"seasonType": c.kwargs["params"]["seasonType"], "week": c.kwargs["params"]["week"]}
                for c in calls
            ]
            assert actual_params == EXPECTED_WEEK_PARAMS
            assert all(c.kwargs["params"]["year"] == 2025 for c in calls)

    def test_400_week_is_skipped_and_continues(self):
        from src.pipelines.sources.passing import passing_plays_resource

        fixture = _load_fixture("passing_plays.json")
        responses = [_http_error(400)] + [[]] * 4 + [fixture] + [[]] * 14
        assert len(responses) == 20

        with (
            patch("src.pipelines.sources.passing.get_client") as mock_get_client,
            patch("src.pipelines.sources.passing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = responses

            results = list(passing_plays_resource(years=[2025]))

            assert len(results) == len(fixture)

    def test_empty_week_yields_no_rows_and_does_not_crash(self):
        from src.pipelines.sources.passing import passing_player_games_resource

        with (
            patch("src.pipelines.sources.passing.get_client") as mock_get_client,
            patch("src.pipelines.sources.passing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            results = list(passing_player_games_resource(years=[2025]))

            assert results == []


class TestSeasonGrainResources:
    """/passing/players/season and /passing/teams/season take a bare year --
    exactly one call per requested year, no week iteration."""

    @pytest.mark.parametrize(
        "resource_name,path",
        [
            ("passing_player_season_resource", "/passing/players/season"),
            ("passing_team_season_resource", "/passing/teams/season"),
        ],
    )
    def test_one_call_per_year_with_bare_year_params(self, resource_name, path):
        import src.pipelines.sources.passing as passing_mod

        with (
            patch("src.pipelines.sources.passing.get_client") as mock_get_client,
            patch("src.pipelines.sources.passing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            resource_fn = getattr(passing_mod, resource_name)
            list(resource_fn(years=[2025, 2026]))

            calls = mock_make_request.call_args_list
            assert len(calls) == 2
            assert [c.kwargs["params"] for c in calls] == [{"year": 2025}, {"year": 2026}]
            assert all(c.args[1] == path for c in calls)

    def test_400_response_is_skipped_and_continues(self):
        from src.pipelines.sources.passing import passing_team_season_resource

        with (
            patch("src.pipelines.sources.passing.get_client") as mock_get_client,
            patch("src.pipelines.sources.passing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(400, "/passing/teams/season"), []]

            results = list(passing_team_season_resource(years=[2025, 2026]))

            assert results == []

    def test_empty_200_yields_no_rows(self):
        from src.pipelines.sources.passing import passing_player_season_resource

        with (
            patch("src.pipelines.sources.passing.get_client") as mock_get_client,
            patch("src.pipelines.sources.passing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            assert list(passing_player_season_resource(years=[2025])) == []

    def test_yields_fixture_rows(self):
        from src.pipelines.sources.passing import passing_player_season_resource

        fixture = _load_fixture("passing_players_season.json")

        with (
            patch("src.pipelines.sources.passing.get_client") as mock_get_client,
            patch("src.pipelines.sources.passing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = fixture

            results = list(passing_player_season_resource(years=[2025]))

            assert len(results) == len(fixture)
            for row in results:
                assert row["season"] == 2025
                assert isinstance(row["playerId"], str)


class TestTeamGamesNestedShapeSurvivesExtraction:
    """dlt does the offense__*/defense__* flattening downstream at normalize
    time -- this asserts the record shape yielded from the resource itself
    (still nested), loaded through the fixture-driven resource path."""

    def test_nested_offense_defense_dicts_survive_to_yielded_records(self):
        from src.pipelines.sources.passing import passing_team_games_resource

        fixture = _load_fixture("passing_teams_games.json")

        with (
            patch("src.pipelines.sources.passing.get_client") as mock_get_client,
            patch("src.pipelines.sources.passing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            # First week call (regular week 1) returns the fixture; every
            # other week is empty so only one set of rows is yielded.
            mock_make_request.side_effect = [fixture] + [[]] * 19

            results = list(passing_team_games_resource(years=[2025]))

            assert len(results) == len(fixture)
            for row in results:
                assert isinstance(row["offense"], dict)
                assert isinstance(row["defense"], dict)
                assert "totalAirYards" in row["offense"]
                assert "totalYardsAfterCatch" in row["defense"]
            assert results[0]["gameId"] == fixture[0]["gameId"]
            assert results[0]["team"] == fixture[0]["team"]


class TestPassingPlaysNullableChartingFields:
    """The charting fields are nullable -- parseStatus="partial" marks an
    incompletely-charted play. This locks in that nulls pass through
    unmodified rather than being coerced to zero."""

    def test_null_charting_fields_pass_through(self):
        from src.pipelines.sources.passing import passing_plays_resource

        fixture = _load_fixture("passing_plays.json")

        with (
            patch("src.pipelines.sources.passing.get_client") as mock_get_client,
            patch("src.pipelines.sources.passing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [fixture] + [[]] * 19

            results = list(passing_plays_resource(years=[2025]))

            assert len(results) == len(fixture)
            for row in results:
                assert row["airYards"] is None
                assert row["passDepth"] is None
                assert row["parseStatus"] == "partial"
                assert isinstance(row["clock"], dict)
                assert isinstance(row["passerId"], str)
                assert isinstance(row["targetId"], str)
