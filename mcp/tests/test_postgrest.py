"""Unit tests for cfb_mcp.postgrest: config, operator builders, row cap, error mapping."""

import httpx
import pytest
import respx

from cfb_mcp.postgrest import (
    DEFAULT_ROW_CAP,
    PostgrestClient,
    PostgrestConfig,
    PostgrestError,
    eq,
    gte,
    in_,
    lte,
)
from tests.conftest import TEST_BASE_URL


def test_operator_builders():
    assert eq(2024) == "eq.2024"
    assert eq("Oklahoma") == "eq.Oklahoma"
    assert gte(6.5) == "gte.6.5"
    assert lte(10) == "lte.10"
    assert in_(["a", "b", "c"]) == "in.(a,b,c)"
    assert in_([1, 2]) == "in.(1,2)"


def test_config_missing_url_raises(monkeypatch):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.setenv("SUPABASE_ANON_KEY", "key")
    with pytest.raises(PostgrestError, match="SUPABASE_URL is not set"):
        PostgrestConfig.from_env()


def test_config_missing_key_raises(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", TEST_BASE_URL)
    monkeypatch.delenv("SUPABASE_ANON_KEY", raising=False)
    with pytest.raises(PostgrestError, match="SUPABASE_ANON_KEY is not set"):
        PostgrestConfig.from_env()


def test_config_strips_trailing_slash(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", TEST_BASE_URL + "/")
    monkeypatch.setenv("SUPABASE_ANON_KEY", "key")
    config = PostgrestConfig.from_env()
    assert config.base_url == TEST_BASE_URL


@pytest.mark.asyncio
@respx.mock
async def test_select_sends_headers_and_params():
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/team_detail").mock(
        return_value=httpx.Response(200, json=[{"school": "Oklahoma"}])
    )
    client = PostgrestClient()
    rows = await client.select("team_detail", {"school": eq("Oklahoma")}, profile="api", limit=1)

    assert rows == [{"school": "Oklahoma"}]
    request = route.calls.last.request
    assert request.headers["apikey"] == "test-anon-key"
    assert request.headers["Authorization"] == "Bearer test-anon-key"
    assert request.headers["Accept-Profile"] == "api"
    assert request.url.params["school"] == "eq.Oklahoma"
    assert request.url.params["limit"] == "1"


@pytest.mark.asyncio
@respx.mock
async def test_select_row_cap_never_exceeded():
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/game_detail").mock(
        return_value=httpx.Response(200, json=[])
    )
    client = PostgrestClient()
    await client.select("game_detail", {}, profile="api", limit=99999)

    request = route.calls.last.request
    assert request.url.params["limit"] == str(DEFAULT_ROW_CAP)


@pytest.mark.asyncio
@respx.mock
async def test_select_row_cap_honors_lower_requested_limit():
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/game_detail").mock(
        return_value=httpx.Response(200, json=[])
    )
    client = PostgrestClient()
    await client.select("game_detail", {}, profile="api", limit=5)

    request = route.calls.last.request
    assert request.url.params["limit"] == "5"


@pytest.mark.asyncio
@respx.mock
async def test_rpc_sends_content_profile_and_json_body():
    route = respx.post(f"{TEST_BASE_URL}/rest/v1/rpc/get_home_away_splits").mock(
        return_value=httpx.Response(200, json=[{"location": "home"}])
    )
    client = PostgrestClient()
    rows = await client.rpc(
        "get_home_away_splits", {"p_team": "Oklahoma", "p_season": 2024}, profile="public"
    )

    assert rows == [{"location": "home"}]
    request = route.calls.last.request
    assert request.headers["Content-Profile"] == "public"
    assert "Accept-Profile" not in request.headers
    import json as _json

    assert _json.loads(request.content) == {"p_team": "Oklahoma", "p_season": 2024}


@pytest.mark.asyncio
@respx.mock
async def test_select_empty_result():
    respx.get(f"{TEST_BASE_URL}/rest/v1/matchup").mock(return_value=httpx.Response(200, json=[]))
    client = PostgrestClient()
    rows = await client.select("matchup", {}, profile="api")
    assert rows == []


@pytest.mark.asyncio
@respx.mock
async def test_select_404_maps_to_actionable_error():
    respx.get(f"{TEST_BASE_URL}/rest/v1/nonexistent_view").mock(
        return_value=httpx.Response(404, json={"message": "relation not found", "code": "42P01"})
    )
    client = PostgrestClient()
    with pytest.raises(PostgrestError, match="not found \\(404\\)"):
        await client.select("nonexistent_view", {}, profile="api")


@pytest.mark.asyncio
@respx.mock
async def test_select_403_maps_to_permission_error():
    respx.get(f"{TEST_BASE_URL}/rest/v1/team_detail").mock(
        return_value=httpx.Response(403, json={"message": "permission denied"})
    )
    client = PostgrestClient()
    with pytest.raises(PostgrestError, match="permission denied"):
        await client.select("team_detail", {}, profile="api")


@pytest.mark.asyncio
@respx.mock
async def test_select_429_maps_to_rate_limit_error():
    respx.get(f"{TEST_BASE_URL}/rest/v1/team_detail").mock(return_value=httpx.Response(429))
    client = PostgrestClient()
    with pytest.raises(PostgrestError, match="rate limited"):
        await client.select("team_detail", {}, profile="api")


@pytest.mark.asyncio
@respx.mock
async def test_select_500_maps_to_server_error():
    respx.get(f"{TEST_BASE_URL}/rest/v1/team_detail").mock(
        return_value=httpx.Response(500, json={"message": "internal error"})
    )
    client = PostgrestClient()
    with pytest.raises(PostgrestError, match="server error \\(500\\)"):
        await client.select("team_detail", {}, profile="api")


@pytest.mark.asyncio
@respx.mock
async def test_select_network_error_is_wrapped():
    respx.get(f"{TEST_BASE_URL}/rest/v1/team_detail").mock(side_effect=httpx.ConnectError("boom"))
    client = PostgrestClient()
    with pytest.raises(PostgrestError, match="network failure"):
        await client.select("team_detail", {}, profile="api")


@pytest.mark.asyncio
@respx.mock
async def test_select_timeout_is_wrapped():
    respx.get(f"{TEST_BASE_URL}/rest/v1/team_detail").mock(
        side_effect=httpx.TimeoutException("timed out")
    )
    client = PostgrestClient()
    with pytest.raises(PostgrestError, match="timed out"):
        await client.select("team_detail", {}, profile="api")
