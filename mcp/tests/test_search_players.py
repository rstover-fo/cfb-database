"""Tests for the search_players tool: get_player_search then get_player_detail."""

import json

import httpx
import pytest
import respx

from cfb_mcp.server import search_players
from tests.conftest import TEST_BASE_URL


@pytest.mark.asyncio
@respx.mock
async def test_search_players_fetches_detail_for_top_hit():
    search_route = respx.post(f"{TEST_BASE_URL}/rest/v1/rpc/get_player_search").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"player_id": "123", "name": "Caleb Williams", "similarity_score": 0.9},
                {"player_id": "456", "name": "Caleb Williamson", "similarity_score": 0.5},
            ],
        )
    )
    detail_route = respx.post(f"{TEST_BASE_URL}/rest/v1/rpc/get_player_detail").mock(
        return_value=httpx.Response(200, json=[{"player_id": "123", "pass_yds": 4000}])
    )

    result = json.loads(await search_players(query="Caleb Williams", team="Oklahoma", season=2022))

    assert result["search"]["_source"] == "public.get_player_search"
    assert result["search"]["count"] == 2
    assert result["top_hit_detail"]["_source"] == "public.get_player_detail"
    assert result["top_hit_detail"]["rows"][0]["player_id"] == "123"

    search_body = json.loads(search_route.calls.last.request.content)
    assert search_body == {
        "p_query": "Caleb Williams",
        "p_limit": 25,
        "p_team": "Oklahoma",
        "p_season": 2022,
    }

    detail_body = json.loads(detail_route.calls.last.request.content)
    assert detail_body == {"p_player_id": "123", "p_season": 2022}


@pytest.mark.asyncio
@respx.mock
async def test_search_players_no_results():
    respx.post(f"{TEST_BASE_URL}/rest/v1/rpc/get_player_search").mock(
        return_value=httpx.Response(200, json=[])
    )
    result = await search_players(query="Nobody Realname")
    assert "No players found" in result


@pytest.mark.asyncio
@respx.mock
async def test_search_players_detail_failure_preserves_search_results():
    respx.post(f"{TEST_BASE_URL}/rest/v1/rpc/get_player_search").mock(
        return_value=httpx.Response(200, json=[{"player_id": "123", "name": "X"}])
    )
    respx.post(f"{TEST_BASE_URL}/rest/v1/rpc/get_player_detail").mock(
        return_value=httpx.Response(500, json={"message": "boom"})
    )

    result = json.loads(await search_players(query="X"))

    assert result["search"]["count"] == 1
    assert "top_hit_detail_error" in result
    assert result["top_hit_detail_error"].startswith("Error:")


@pytest.mark.asyncio
@respx.mock
async def test_search_players_row_cap_passed_through():
    route = respx.post(f"{TEST_BASE_URL}/rest/v1/rpc/get_player_search").mock(
        return_value=httpx.Response(200, json=[])
    )
    await search_players(query="X", limit=100)
    body = json.loads(route.calls.last.request.content)
    assert body["p_limit"] == 100
