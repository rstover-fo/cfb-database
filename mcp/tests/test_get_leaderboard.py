"""Tests for the get_leaderboard tool: api.leaderboard_teams / api.team_wepa_season."""

import json

import httpx
import pytest
import respx

from cfb_mcp.server import LeaderboardMetric, get_leaderboard
from tests.conftest import TEST_BASE_URL


@pytest.mark.asyncio
@respx.mock
async def test_get_leaderboard_epa_metric_uses_leaderboard_teams():
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/leaderboard_teams").mock(
        return_value=httpx.Response(200, json=[{"team": "Oklahoma", "epa_rank": 1}])
    )

    result = json.loads(await get_leaderboard(season=2024, metric=LeaderboardMetric.EPA))

    assert result["_source"] == "api.leaderboard_teams"
    request = route.calls.last.request
    assert request.url.params["season"] == "eq.2024"
    assert request.url.params["order"] == "epa_rank.asc"
    # Ranks are within-classification (2026-07-22 contract change): without
    # this filter, FCS/lower rank-1 teams would interleave with FBS leaders.
    assert request.url.params["classification"] == "eq.fbs"


@pytest.mark.asyncio
@respx.mock
async def test_get_leaderboard_wepa_metric_uses_team_wepa_season():
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/team_wepa_season").mock(
        return_value=httpx.Response(200, json=[{"team": "Oklahoma", "epa_rank": 1}])
    )

    result = json.loads(await get_leaderboard(season=2024, metric=LeaderboardMetric.WEPA))

    assert result["_source"] == "api.team_wepa_season"
    request = route.calls.last.request
    assert request.url.params["season"] == "eq.2024"
    assert request.url.params["order"] == "epa_rank.asc"


@pytest.mark.parametrize(
    "metric,expected_order",
    [
        (LeaderboardMetric.WINS, "wins_rank.asc"),
        (LeaderboardMetric.PPG, "ppg_rank.asc"),
        (LeaderboardMetric.SCORING_DEFENSE, "defense_ppg_rank.asc"),
        (LeaderboardMetric.SP_RATING, "sp_rank.asc"),
    ],
)
@pytest.mark.asyncio
@respx.mock
async def test_get_leaderboard_metric_order_mapping(metric, expected_order):
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/leaderboard_teams").mock(
        return_value=httpx.Response(200, json=[])
    )
    await get_leaderboard(season=2024, metric=metric)
    assert route.calls.last.request.url.params["order"] == expected_order


@pytest.mark.asyncio
@respx.mock
async def test_get_leaderboard_row_cap_enforced():
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/leaderboard_teams").mock(
        return_value=httpx.Response(200, json=[])
    )
    await get_leaderboard(season=2024, metric=LeaderboardMetric.WINS, limit=100)
    assert route.calls.last.request.url.params["limit"] == "100"


@pytest.mark.asyncio
@respx.mock
async def test_get_leaderboard_empty_result():
    respx.get(f"{TEST_BASE_URL}/rest/v1/leaderboard_teams").mock(
        return_value=httpx.Response(200, json=[])
    )
    result = await get_leaderboard(season=1800, metric=LeaderboardMetric.WINS)
    assert "No leaderboard data found" in result
