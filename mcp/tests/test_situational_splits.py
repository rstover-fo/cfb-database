"""Tests for the situational_splits tool: fan-out to the five split RPCs."""

import json

import httpx
import pytest
import respx

from cfb_mcp.server import SplitType, situational_splits
from tests.conftest import TEST_BASE_URL


@pytest.mark.parametrize(
    "split_type,rpc_name",
    [
        (SplitType.HOME_AWAY, "get_home_away_splits"),
        (SplitType.CONFERENCE, "get_conference_splits"),
        (SplitType.RED_ZONE, "get_red_zone_splits"),
        (SplitType.DOWN_DISTANCE, "get_down_distance_splits"),
        (SplitType.FIELD_POSITION, "get_field_position_splits"),
    ],
)
@pytest.mark.asyncio
@respx.mock
async def test_situational_splits_fans_out_to_correct_rpc(split_type, rpc_name):
    route = respx.post(f"{TEST_BASE_URL}/rest/v1/rpc/{rpc_name}").mock(
        return_value=httpx.Response(200, json=[{"side": "offense"}])
    )

    result = json.loads(
        await situational_splits(team="Oklahoma", season=2024, split_type=split_type)
    )

    assert result["_source"] == f"public.{rpc_name}"
    request = route.calls.last.request
    assert request.headers["Content-Profile"] == "public"
    assert json.loads(request.content) == {"p_team": "Oklahoma", "p_season": 2024}


@pytest.mark.asyncio
@respx.mock
async def test_situational_splits_empty_result():
    respx.post(f"{TEST_BASE_URL}/rest/v1/rpc/get_red_zone_splits").mock(
        return_value=httpx.Response(200, json=[])
    )
    result = await situational_splits(team="Rice", season=2005, split_type=SplitType.RED_ZONE)
    assert "No red_zone splits found" in result


@pytest.mark.asyncio
@respx.mock
async def test_situational_splits_error_path():
    respx.post(f"{TEST_BASE_URL}/rest/v1/rpc/get_home_away_splits").mock(
        return_value=httpx.Response(500, json={"message": "boom"})
    )
    result = await situational_splits(team="Oklahoma", season=2024, split_type=SplitType.HOME_AWAY)
    assert result.startswith("Error:")
