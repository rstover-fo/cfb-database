"""Base source utilities for CFBD pipelines."""

import dlt
from dlt.sources import DltResource

from ..utils.api_client import CFBDClient, get_client
from ..utils.rate_limiter import get_rate_limiter


def make_request(
    client: CFBDClient,
    endpoint: str,
    params: dict | None = None,
) -> list[dict]:
    """Make an API request and track rate limit.

    Args:
        client: CFBD API client
        endpoint: API endpoint path
        params: Query parameters

    Returns:
        API response data
    """
    rate_limiter = get_rate_limiter()

    if not rate_limiter.check_budget():
        raise RuntimeError(
            f"API budget exhausted. {rate_limiter.calls_used} calls used this month. "
            "Wait for next month or upgrade tier."
        )

    data = client.get(endpoint, params=params)
    rate_limiter.record_call()

    return data


def create_resource(
    name: str,
    endpoint: str,
    primary_key: str | list[str],
    write_disposition: str = "replace",
    params: dict | None = None,
) -> DltResource:
    """Create a dlt resource for a CFBD endpoint.

    Args:
        name: Resource name
        endpoint: API endpoint path
        primary_key: Primary key column(s)
        write_disposition: "replace" or "merge"
        params: Static query parameters

    Returns:
        dlt resource
    """

    @dlt.resource(
        name=name,
        write_disposition=write_disposition,
        primary_key=primary_key,
    )
    def _resource():
        client = get_client()
        try:
            data = make_request(client, endpoint, params)
            yield from data
        finally:
            client.close()

    return _resource()
