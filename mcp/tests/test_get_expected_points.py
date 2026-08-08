"""Tests for the get_expected_points tool: api.expected_points."""

import json

import httpx
import pytest
import respx

from cfb_mcp.server import EpEra, get_expected_points
from tests.conftest import TEST_BASE_URL


@pytest.mark.asyncio
@respx.mock
async def test_defaults_to_modern_era():
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/expected_points").mock(
        return_value=httpx.Response(
            200,
            json=[{"state": "d1|standard|z3", "ep_drive": 4.24, "p_td": 0.521}],
        )
    )

    result = json.loads(await get_expected_points(down=1, field_zone=3))

    assert result["_source"] == "api.expected_points"
    request = route.calls.last.request
    assert request.url.params["era"] == "eq.2021+"
    assert request.url.params["down"] == "eq.1"
    assert request.url.params["field_zone"] == "eq.3"
    assert request.url.params["order"] == "down.asc,field_zone.asc,distance_bucket.asc"


@pytest.mark.asyncio
@respx.mock
async def test_era_plus_sign_is_url_encoded_on_the_wire():
    """The era key '2021+' contains a literal plus; an unencoded '+' decodes
    as a space server-side and matches nothing. httpx must send %2B."""
    respx.get(f"{TEST_BASE_URL}/rest/v1/expected_points").mock(
        return_value=httpx.Response(200, json=[{"state": "d1|standard|z3"}])
    )

    await get_expected_points(down=1)

    raw_query = respx.calls.last.request.url.query.decode()
    assert "era=eq.2021%2B" in raw_query


@pytest.mark.asyncio
@respx.mock
async def test_historical_era_override():
    route = respx.get(f"{TEST_BASE_URL}/rest/v1/expected_points").mock(
        return_value=httpx.Response(200, json=[{"state": "d1|standard|z8", "ep_drive": 1.58}])
    )

    await get_expected_points(down=1, field_zone=8, era=EpEra.LEGACY)

    assert route.calls.last.request.url.params["era"] == "eq.2004-2013"


@pytest.mark.asyncio
@respx.mock
async def test_empty_result_names_the_bucket_vocabulary():
    """The most likely miss is a wrong distance_bucket (the vocabulary is
    down-aware); the message must teach the fix, not just say 'no rows'."""
    respx.get(f"{TEST_BASE_URL}/rest/v1/expected_points").mock(
        return_value=httpx.Response(200, json=[])
    )

    result = await get_expected_points(down=1, distance_bucket="med")

    assert "standard" in result and "xlong" in result


@pytest.mark.asyncio
@respx.mock
async def test_docstring_carries_the_interpretation_caveats():
    """The tool's docstring is what the calling model reads; the drive-basis,
    d4-conditional, and interval caveats are the contract's rules 1, 3, 4."""
    doc = get_expected_points.__doc__ or ""
    assert "NOT comparable to CFBD" in doc
    assert "GO-FOR-IT-CONDITIONAL" in doc
    assert "se_boot" in doc
