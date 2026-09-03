"""Tests for the /rushing charting source (CFBD spec v5.26.0): rusher
attribution, rush direction, direction-eligible/available coverage.

Mirrors tests/test_sources/test_passing.py line for line where the two
sources behave the same; diverges only where rushing's shape differs
(attribution status, direction buckets, no *_attempts_available naming
parity -- rushing uses rushingYardsAvailable/directionEligibleAttempts/
directionAvailableAttempts/touchdownStatusAvailable instead).

Fixtures at tests/fixtures/cfbd_2026/rushing_*.json are 2026-09-03 live
probe captures (5 records each, real API responses), not spec-derived
placeholders. One test
below (`test_null_rusher_id_team_rush_is_yielded_not_dropped`) uses a
synthetic in-test record instead of a fixture row because none of the 5
captured plays happens to be a team-attributed (unattributed rusher) rush.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

FIXTURES = Path(__file__).parent.parent / "fixtures" / "cfbd_2026"

# The three game-grain resources all walk the same (season_type, week) pairs
# -- regular season weeks 1-16, postseason weeks 1-4 -- cloned from
# passing.py's _iter_season_weeks (itself cloned from stats.py).
EXPECTED_WEEK_PARAMS = [{"seasonType": "regular", "week": w} for w in range(1, 17)] + [
    {"seasonType": "postseason", "week": w} for w in range(1, 5)
]


def _load_fixture(name):
    with open(FIXTURES / name) as f:
        return json.load(f)


def _http_error(status_code: int, path: str = "/rushing/plays") -> httpx.HTTPStatusError:
    request = httpx.Request("GET", f"https://api.collegefootballdata.com{path}")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


def test_rushing_source_returns_five_resources():
    from src.pipelines.sources.rushing import rushing_source

    source = rushing_source(years=[2025])

    assert set(source.resources.keys()) == {
        "rushing_plays",
        "rushing_player_games",
        "rushing_team_games",
        "rushing_player_season",
        "rushing_team_season",
    }


class TestSourceModeDispatch:
    """rushing_source(years=None) resolves years from `mode` before building
    its five resources. Calling rushing_source(...) normally would build a
    real DltSource, and a resource function patched to return a plain
    MagicMock breaks dlt's pipe-construction (`InvalidResourceDataTypeMultiplePipes`)
    -- so this drives the undecorated function via `__wrapped__` (dlt
    decorators preserve it via functools.wraps) and patches each
    `*_resource` function to capture the `years` list it was built with,
    without needing to construct a real DltSource or iterate any
    generator."""

    RESOURCE_NAMES = [
        "rushing_plays_resource",
        "rushing_player_games_resource",
        "rushing_team_games_resource",
        "rushing_player_season_resource",
        "rushing_team_season_resource",
    ]

    def test_years_none_incremental_mode_uses_current_season(self):
        import src.pipelines.sources.rushing as rushing_mod

        patchers = [
            patch.object(rushing_mod, name, MagicMock(return_value=object()))
            for name in self.RESOURCE_NAMES
        ]
        mocks = [p.start() for p in patchers]
        try:
            with patch("src.pipelines.sources.rushing.get_current_season", return_value=2026):
                rushing_mod.rushing_source.__wrapped__(years=None, mode="incremental")
        finally:
            for p in patchers:
                p.stop()

        for mock in mocks:
            mock.assert_called_once_with([2026])

    def test_years_none_backfill_mode_uses_full_stats_range(self):
        import src.pipelines.sources.rushing as rushing_mod
        from src.pipelines.config.years import YEAR_RANGES

        patchers = [
            patch.object(rushing_mod, name, MagicMock(return_value=object()))
            for name in self.RESOURCE_NAMES
        ]
        mocks = [p.start() for p in patchers]
        try:
            rushing_mod.rushing_source.__wrapped__(years=None, mode="backfill")
        finally:
            for p in patchers:
                p.stop()

        expected_years = YEAR_RANGES["stats"].to_list()
        for mock in mocks:
            mock.assert_called_once_with(expected_years)


class TestResourceCompositionTable:
    """Locks in table/PK/write-disposition per KTD3."""

    @pytest.mark.parametrize(
        "resource_name,pk",
        [
            ("rushing_plays_resource", {"game_id", "play_id"}),
            ("rushing_player_games_resource", {"game_id", "player_id"}),
            ("rushing_team_games_resource", {"game_id", "team"}),
            ("rushing_player_season_resource", {"season", "player_id", "team"}),
            ("rushing_team_season_resource", {"season", "team"}),
        ],
    )
    def test_merge_disposition_and_primary_key(self, resource_name, pk):
        import src.pipelines.sources.rushing as rushing_mod

        with patch("src.pipelines.sources.rushing.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource_fn = getattr(rushing_mod, resource_name)
            resource = resource_fn(years=[2025])

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == pk


class TestEraGuardSkipsPre2025:
    """RUSHING_DATA_START = 2025 -- years before it must skip without
    spending a call at all (mirrors passing's PASSING_DATA_START guard)."""

    @pytest.mark.parametrize(
        "resource_name",
        [
            "rushing_plays_resource",
            "rushing_player_games_resource",
            "rushing_team_games_resource",
            "rushing_player_season_resource",
            "rushing_team_season_resource",
        ],
    )
    def test_pre_2025_years_spend_zero_calls(self, resource_name):
        import src.pipelines.sources.rushing as rushing_mod

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            resource_fn = getattr(rushing_mod, resource_name)
            results = list(resource_fn(years=[2004, 2014, 2024]))

            mock_make_request.assert_not_called()
            assert results == []

    def test_mixed_pre_and_post_guard_years_only_call_for_the_eligible_year(self):
        """A single invocation spanning both sides of RUSHING_DATA_START must
        skip the pre-2025 year with zero calls and walk weeks only for the
        eligible year -- not just "pre-2025 alone costs nothing" (covered
        above) but "a pre-2025 year mixed into the same call never leaks a
        call of its own"."""
        from src.pipelines.sources.rushing import rushing_plays_resource

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(rushing_plays_resource(years=[2014, 2025]))

            calls = mock_make_request.call_args_list
            assert len(calls) == 20
            assert all(c.kwargs["params"]["year"] == 2025 for c in calls)


class TestWeekIterationParamSequence:
    """The three game-grain endpoints walk weeks like passing's
    corresponding resources; this asserts the exact (year, seasonType,
    week) sequence and cost (~20 calls/season/resource)."""

    @pytest.mark.parametrize(
        "resource_name,path",
        [
            ("rushing_plays_resource", "/rushing/plays"),
            ("rushing_player_games_resource", "/rushing/players/games"),
            ("rushing_team_games_resource", "/rushing/teams/games"),
        ],
    )
    def test_iterates_regular_1_16_and_postseason_1_4(self, resource_name, path):
        import src.pipelines.sources.rushing as rushing_mod

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            resource_fn = getattr(rushing_mod, resource_name)
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
        from src.pipelines.sources.rushing import rushing_plays_resource

        fixture = _load_fixture("rushing_plays.json")
        responses = [_http_error(400)] + [[]] * 4 + [fixture] + [[]] * 14
        assert len(responses) == 20

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = responses

            results = list(rushing_plays_resource(years=[2025]))

            assert len(results) == len(fixture)

    def test_500_propagates(self):
        """A non-400 HTTP error is not caught by the resource; iterating the
        dlt resource wraps it in ResourceExtractionError, whose __cause__ is
        the original httpx.HTTPStatusError -- confirms the error is not
        silently swallowed."""
        from dlt.extract.exceptions import ResourceExtractionError

        from src.pipelines.sources.rushing import rushing_plays_resource

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = _http_error(500)

            with pytest.raises(ResourceExtractionError) as exc_info:
                list(rushing_plays_resource(years=[2025]))

            assert isinstance(exc_info.value.__cause__, httpx.HTTPStatusError)
            assert exc_info.value.__cause__.response.status_code == 500

    def test_empty_week_yields_no_rows_and_does_not_crash(self):
        from src.pipelines.sources.rushing import rushing_player_games_resource

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            results = list(rushing_player_games_resource(years=[2025]))

            assert results == []


class TestSeasonGrainResources:
    """/rushing/players/season and /rushing/teams/season take a bare year --
    exactly one call per requested year, no week iteration."""

    @pytest.mark.parametrize(
        "resource_name,path",
        [
            ("rushing_player_season_resource", "/rushing/players/season"),
            ("rushing_team_season_resource", "/rushing/teams/season"),
        ],
    )
    def test_one_call_per_year_with_bare_year_params(self, resource_name, path):
        import src.pipelines.sources.rushing as rushing_mod

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            resource_fn = getattr(rushing_mod, resource_name)
            list(resource_fn(years=[2025, 2026]))

            calls = mock_make_request.call_args_list
            assert len(calls) == 2
            assert [c.kwargs["params"] for c in calls] == [{"year": 2025}, {"year": 2026}]
            assert all(c.args[1] == path for c in calls)

    def test_400_response_is_skipped_and_continues(self):
        from src.pipelines.sources.rushing import rushing_team_season_resource

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(400, "/rushing/teams/season"), []]

            results = list(rushing_team_season_resource(years=[2025, 2026]))

            assert results == []

    def test_empty_200_yields_no_rows(self):
        from src.pipelines.sources.rushing import rushing_player_season_resource

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            assert list(rushing_player_season_resource(years=[2025])) == []

    def test_yields_fixture_rows(self):
        from src.pipelines.sources.rushing import rushing_player_season_resource

        fixture = _load_fixture("rushing_players_season.json")

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = fixture

            results = list(rushing_player_season_resource(years=[2025]))

            assert len(results) == len(fixture)
            for row in results:
                assert row["season"] == 2025
                assert isinstance(row["playerId"], str)


class TestGameGrainRowsStampedWithSeasonWeekSeasonType:
    """The API stamps season/week/seasonType already; this locks in the
    setdefault safety net passes fixture rows through unchanged."""

    def test_player_games_fixture_rows_carry_season_week_season_type(self):
        """The API already stamps season/week/seasonType on captured rows
        (all 5 fixture rows carry week=5/regular/2025); setdefault is a
        safety net that does not override an existing value, so this
        asserts the fixture's own stamped values survive unchanged."""
        from src.pipelines.sources.rushing import rushing_player_games_resource

        fixture = _load_fixture("rushing_players_games.json")

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [fixture] + [[]] * 19

            results = list(rushing_player_games_resource(years=[2025]))

            assert len(results) == len(fixture)
            for row, fixture_row in zip(results, fixture, strict=True):
                assert row["season"] == fixture_row["season"] == 2025
                assert row["seasonType"] == fixture_row["seasonType"]
                assert row["week"] == fixture_row["week"]

    def test_plays_fixture_rows_carry_season_week_season_type(self):
        """Same setdefault safety-net coverage as the player-games test
        above, for the play-grain resource (all 5 fixture rows carry
        week=5/regular/2025)."""
        from src.pipelines.sources.rushing import rushing_plays_resource

        fixture = _load_fixture("rushing_plays.json")

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [fixture] + [[]] * 19

            results = list(rushing_plays_resource(years=[2025]))

            assert len(results) == len(fixture)
            for row, fixture_row in zip(results, fixture, strict=True):
                assert row["season"] == fixture_row["season"] == 2025
                assert row["seasonType"] == fixture_row["seasonType"]
                assert row["week"] == fixture_row["week"]

    def test_team_games_fixture_rows_carry_season_week_season_type(self):
        """Same setdefault safety-net coverage as the player-games test
        above, for the team-game-grain resource (all 5 fixture rows carry
        week=5/regular/2025)."""
        from src.pipelines.sources.rushing import rushing_team_games_resource

        fixture = _load_fixture("rushing_teams_games.json")

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [fixture] + [[]] * 19

            results = list(rushing_team_games_resource(years=[2025]))

            assert len(results) == len(fixture)
            for row, fixture_row in zip(results, fixture, strict=True):
                assert row["season"] == fixture_row["season"] == 2025
                assert row["seasonType"] == fixture_row["seasonType"]
                assert row["week"] == fixture_row["week"]


class TestTeamGamesNestedShapeSurvivesExtraction:
    """dlt does the offense__*/defense__* flattening downstream at normalize
    time -- this asserts the record shape yielded from the resource itself
    (still nested), loaded through the fixture-driven resource path."""

    def test_nested_offense_defense_dicts_survive_to_yielded_records(self):
        from src.pipelines.sources.rushing import rushing_team_games_resource

        fixture = _load_fixture("rushing_teams_games.json")

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [fixture] + [[]] * 19

            results = list(rushing_team_games_resource(years=[2025]))

            assert len(results) == len(fixture)
            for row in results:
                assert isinstance(row["offense"], dict)
                assert isinstance(row["defense"], dict)
                assert "directions" in row["offense"]
                assert "touchdownStatusAvailable" in row["offense"]
                assert "rushingTouchdowns" in row["defense"]
                assert set(row["offense"]["directions"].keys()) == {
                    "unknown",
                    "right",
                    "middle",
                    "left",
                }
                assert len(row["offense"]["directions"]["left"]) == 15
            assert results[0]["gameId"] == fixture[0]["gameId"]
            assert results[0]["team"] == fixture[0]["team"]


class TestRushingPlaysAttributionAndNullableFields:
    """attributionStatus/parseStatus/rusherId etc. are the rushing-specific
    fields diverging from passing's charting shape (KTD5: parseStatus
    'invalid' is its own bucket, never folded into 'partial'; the source
    itself does not filter on parseStatus or attributionStatus)."""

    def test_fixture_rows_pass_through_with_attribution_fields_intact(self):
        from src.pipelines.sources.rushing import rushing_plays_resource

        fixture = _load_fixture("rushing_plays.json")

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [fixture] + [[]] * 19

            results = list(rushing_plays_resource(years=[2025]))

            assert len(results) == len(fixture)
            for row in results:
                assert row["attributionStatus"] == "individual"
                assert row["parseStatus"] == "partial"
                assert isinstance(row["clock"], dict)
                assert isinstance(row["rusherId"], str)
                assert row["rushDirection"] is None
                assert row["directionAnalysisEligible"] is True

    def test_null_rusher_id_team_rush_is_yielded_not_dropped(self):
        """None of the 5 live-captured plays happens to be a team-attributed
        rush (unattributed to an individual rusher), so this scenario -- a
        row with rusherId null and isTeamRush true must survive the
        resource, not be filtered -- uses a synthetic record built from the
        real fixture's shape rather than a fixture file edit."""
        from src.pipelines.sources.rushing import rushing_plays_resource

        template = _load_fixture("rushing_plays.json")[0]
        team_rush_row = {
            **template,
            "playId": "999999999999999999",
            "rusherId": None,
            "rusher": None,
            "rushDirection": None,
            "isTeamRush": True,
            "attributionStatus": "team",
            "directionAnalysisEligible": False,
            "parseStatus": "complete",
        }

        with (
            patch("src.pipelines.sources.rushing.get_client") as mock_get_client,
            patch("src.pipelines.sources.rushing.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [[team_rush_row]] + [[]] * 19

            results = list(rushing_plays_resource(years=[2025]))

            assert len(results) == 1
            assert results[0]["rusherId"] is None
            assert results[0]["isTeamRush"] is True
            assert results[0]["attributionStatus"] == "team"
