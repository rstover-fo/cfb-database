"""Tests for the get_data_freshness tool: public.get_data_freshness RPC."""

import json

import httpx
import pytest
import respx

from cfb_mcp.server import get_data_freshness
from tests.conftest import TEST_BASE_URL


@pytest.mark.asyncio
@respx.mock
async def test_get_data_freshness_calls_rpc_with_no_args():
    route = respx.post(f"{TEST_BASE_URL}/rest/v1/rpc/get_data_freshness").mock(
        return_value=httpx.Response(200, json=[{"table_name": "games", "is_stale": False}])
    )

    result = json.loads(await get_data_freshness())

    assert result["_source"] == "public.get_data_freshness"
    assert result["count"] == 1
    request = route.calls.last.request
    assert request.headers["Content-Profile"] == "public"
    assert json.loads(request.content) == {}


@pytest.mark.asyncio
@respx.mock
async def test_get_data_freshness_empty_result():
    respx.post(f"{TEST_BASE_URL}/rest/v1/rpc/get_data_freshness").mock(
        return_value=httpx.Response(200, json=[])
    )
    result = json.loads(await get_data_freshness())
    assert result["count"] == 0


@pytest.mark.asyncio
@respx.mock
async def test_get_data_freshness_error_path():
    respx.post(f"{TEST_BASE_URL}/rest/v1/rpc/get_data_freshness").mock(
        return_value=httpx.Response(403, json={"message": "denied"})
    )
    result = await get_data_freshness()
    assert result.startswith("Error:")
