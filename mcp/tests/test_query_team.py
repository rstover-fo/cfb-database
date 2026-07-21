"""Tests for the query_team tool: api.team_detail + api.team_history."""

import json

import httpx
import pytest
import respx

from cfb_mcp.server import query_team
from tests.conftest import TEST_BASE_URL


@pytest.mark.asyncio
@respx.mock
async def test_query_team_success_shapes_both_sources():
    detail_route = respx.get(f"{TEST_BASE_URL}/rest/v1/team_detail").mock(
        return_value=httpx.Response(200, json=[{"school": "Oklahoma", "wins": 10}])
    )
    history_route = respx.get(f"{TEST_BASE_URL}/rest/v1/team_history").mock(
        return_value=httpx.Response(
            200, json=[{"team": "Oklahoma", "season": 2024}, {"team": "Oklahoma", "season": 2023}]
        )
    )

    result = json.loads(await query_team(team="Oklahoma"))

    assert result["team_detail"]["_source"] == "api.team_detail"
    assert result["team_detail"]["count"] == 1
    assert result["team_history"]["_source"] == "api.team_history"
    assert result["team_history"]["count"] == 2

    detail_request = detail_route.calls.last.request
    assert detail_request.url.params["school"] == "eq.Oklahoma"
    assert detail_request.headers["Accept-Profile"] == "api"

    history_request = history_route.calls.last.request
    assert history_request.url.params["team"] == "eq.Oklahoma"
    assert history_request.url.params["order"] == "season.desc"
    assert history_request.url.params["limit"] == "100"


@pytest.mark.asyncio
@respx.mock
async def test_query_team_not_found_returns_plain_message():
    respx.get(f"{TEST_BASE_URL}/rest/v1/team_detail").mock(
        return_value=httpx.Response(200, json=[])
    )
    respx.get(f"{TEST_BASE_URL}/rest/v1/team_history").mock(
        return_value=httpx.Response(200, json=[])
    )

    result = await query_team(team="Nonexistent State")

    assert result.startswith("No team found matching 'Nonexistent State'")


@pytest.mark.asyncio
@respx.mock
async def test_query_team_error_path_returns_error_string():
    respx.get(f"{TEST_BASE_URL}/rest/v1/team_detail").mock(
        return_value=httpx.Response(500, json={"message": "boom"})
    )

    result = await query_team(team="Oklahoma")

    assert result.startswith("Error:")
