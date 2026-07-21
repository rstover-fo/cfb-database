"""Tests for the query_games tool: api.game_detail."""

import json

import httpx
import pytest
import respx

from cfb_mcp.server import query_games
from tests.conftest import TEST_BASE_URL


@pytest.mark.asyncio
@respx.mock
async def test_query_games_filters_and_or_clause():
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/game_detail").mock(
        return_value=httpx.Response(200, json=[{"game_id": 1}])
    )

    result = json.loads(await query_games(season=2024, week=5, team="Oklahoma", min_excitement=6.5))

    assert result["_source"] == "api.game_detail"
    assert result["count"] == 1

    request = route.calls.last.request
    assert request.url.params["season"] == "eq.2024"
    assert request.url.params["week"] == "eq.5"
    assert request.url.params["or"] == '(home_team.eq."Oklahoma",away_team.eq."Oklahoma")'
    assert request.url.params["excitement_index"] == "gte.6.5"
    assert request.url.params["order"] == "start_date.desc"
    assert request.url.params["limit"] == "100"
    # Tool-level guard: reads must stay confined to the contracted api schema
    assert request.headers["Accept-Profile"] == "api"


@pytest.mark.asyncio
@respx.mock
async def test_query_games_quotes_reserved_chars_in_team_name():
    """Parens/commas are structural in PostgREST logic trees; quoting keeps
    names like "Miami (OH)" from corrupting the or= filter."""
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/game_detail").mock(
        return_value=httpx.Response(200, json=[{"game_id": 1}])
    )

    await query_games(season=2024, team="Miami (OH)")

    or_param = route.calls.last.request.url.params["or"]
    assert or_param == '(home_team.eq."Miami (OH)",away_team.eq."Miami (OH)")'


@pytest.mark.asyncio
@respx.mock
async def test_query_games_limit_is_capped_at_100():
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/game_detail").mock(
        return_value=httpx.Response(200, json=[])
    )
    await query_games(season=2024, limit=100)
    assert route.calls.last.request.url.params["limit"] == "100"


@pytest.mark.asyncio
@respx.mock
async def test_query_games_no_filters_still_works():
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/game_detail").mock(
        return_value=httpx.Response(200, json=[{"game_id": 1}])
    )
    await query_games()
    request = route.calls.last.request
    assert "season" not in request.url.params
    assert "or" not in request.url.params


@pytest.mark.asyncio
@respx.mock
async def test_query_games_empty_result():
    respx.get(f"{TEST_BASE_URL}/rest/v1/game_detail").mock(
        return_value=httpx.Response(200, json=[])
    )
    result = await query_games(season=1899)
    assert result == "No games found matching the given filters."


@pytest.mark.asyncio
@respx.mock
async def test_query_games_error_path():
    respx.get(f"{TEST_BASE_URL}/rest/v1/game_detail").mock(
        return_value=httpx.Response(404, json={"message": "not found"})
    )
    result = await query_games(season=2024)
    assert result.startswith("Error:")


@pytest.mark.asyncio
async def test_missing_env_returns_error_string_not_exception(monkeypatch):
    """Config errors must surface as the tool's friendly Error: string, not an
    MCP exception — client construction happens inside each tool's try block."""
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_ANON_KEY", raising=False)

    result = await query_games(season=2024)

    assert isinstance(result, str)
    assert result.startswith("Error: SUPABASE_URL is not set")
