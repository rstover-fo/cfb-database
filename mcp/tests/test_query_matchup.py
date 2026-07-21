"""Tests for the query_matchup tool: api.matchup, alphabetically-normalized pair."""

import json

import httpx
import pytest
import respx

from cfb_mcp.server import query_matchup
from tests.conftest import TEST_BASE_URL


@pytest.mark.asyncio
@respx.mock
async def test_query_matchup_normalizes_team_order():
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/matchup").mock(
        return_value=httpx.Response(200, json=[{"team1": "Oklahoma", "team2": "Texas"}])
    )

    # Texas < Oklahoma alphabetically is false ("O" < "T"), so team1 should be
    # Oklahoma regardless of argument order.
    result = json.loads(await query_matchup(team_a="Texas", team_b="Oklahoma"))

    assert result["_source"] == "api.matchup"
    request = route.calls.last.request
    assert request.url.params["team1"] == "eq.Oklahoma"
    assert request.url.params["team2"] == "eq.Texas"


@pytest.mark.asyncio
@respx.mock
async def test_query_matchup_same_params_either_argument_order():
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/matchup").mock(
        return_value=httpx.Response(200, json=[{"team1": "Oklahoma", "team2": "Texas"}])
    )

    await query_matchup(team_a="Oklahoma", team_b="Texas")
    first_call_params = dict(route.calls.last.request.url.params)

    await query_matchup(team_a="Texas", team_b="Oklahoma")
    second_call_params = dict(route.calls.last.request.url.params)

    assert first_call_params == second_call_params


@pytest.mark.asyncio
@respx.mock
async def test_query_matchup_not_found():
    respx.get(f"{TEST_BASE_URL}/rest/v1/matchup").mock(return_value=httpx.Response(200, json=[]))
    result = await query_matchup(team_a="Oklahoma", team_b="Rice")
    assert "No matchup history found" in result
