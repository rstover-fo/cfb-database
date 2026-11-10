"""Base source utilities for CFBD pipelines."""

from ..utils.api_client import CFBDClient
from ..utils.quota_admission import transport_operation
from ..utils.rate_limiter import get_rate_limiter


class RequestBudgetExhausted(RuntimeError):
    """The local monthly request budget cannot admit another API call."""


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
    if transport_operation() is not None:
        return client.get(endpoint, params=params)

    rate_limiter = get_rate_limiter()

    if not rate_limiter.check_budget():
        raise RequestBudgetExhausted(
            f"API budget exhausted. {rate_limiter.calls_used} calls used this month. "
            "Wait for next month or upgrade tier."
        )

    data = client.get(endpoint, params=params)
    rate_limiter.record_call()

    return data
