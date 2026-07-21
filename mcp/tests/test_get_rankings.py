"""Tests for the get_rankings tool: api.poll_rankings."""

import json

import httpx
import pytest
import respx

from cfb_mcp.server import get_rankings
from tests.conftest import TEST_BASE_URL


@pytest.mark.asyncio
@respx.mock
async def test_get_rankings_defaults_to_regular_season_type():
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/poll_rankings").mock(
        return_value=httpx.Response(200, json=[{"school": "Oklahoma", "rank": 5}])
    )

    result = json.loads(await get_rankings(season=2024, week=8, poll="AP Top 25"))

    assert result["_source"] == "api.poll_rankings"
    request = route.calls.last.request
    assert request.url.params["season"] == "eq.2024"
    assert request.url.params["season_type"] == "eq.regular"
    assert request.url.params["week"] == "eq.8"
    assert request.url.params["poll"] == "eq.AP Top 25"
    assert request.url.params["order"] == "week.asc,poll.asc,rank.asc"


@pytest.mark.asyncio
@respx.mock
async def test_get_rankings_postseason_override():
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/poll_rankings").mock(
        return_value=httpx.Response(200, json=[{"school": "Michigan", "rank": 1}])
    )

    from cfb_mcp.server import PollSeasonType

    await get_rankings(season=2023, season_type=PollSeasonType.POSTSEASON)

    request = route.calls.last.request
    assert request.url.params["season_type"] == "eq.postseason"


@pytest.mark.asyncio
@respx.mock
async def test_get_rankings_empty_result():
    respx.get(f"{TEST_BASE_URL}/rest/v1/poll_rankings").mock(
        return_value=httpx.Response(200, json=[])
    )
    result = await get_rankings(season=1800)
    assert "No rankings found" in result
