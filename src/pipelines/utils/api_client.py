"""HTTP client for CFBD API with retry and rate limiting."""

import logging
import time
from typing import Any

import dlt
import httpx

logger = logging.getLogger(__name__)


class RateLimitExhausted(Exception):
    """A request was rate-limited on every attempt.

    Raised instead of returning an empty result, because to a dlt resource
    ``[]`` is indistinguishable from "this game genuinely has no rows" -- the
    pipeline would record the absence as fact and move on. A 2026-07-25 daily
    load spent three hours doing exactly that against ``/plays/stats`` before
    the job timed out.
    """


class RateLimitCircuitOpen(Exception):
    """Too many consecutive requests were rate-limited; the sweep gave up.

    Distinct from RateLimitExhausted: that one means a single request failed,
    this means the API is refusing everything and continuing cannot help. A
    monthly-quota exhaustion produces an unbroken run of 429s, and the only
    useful response is to stop immediately rather than grind at one request
    per minute until the CI job is killed.

    **Terminal for the client instance, by design.** Once open the breaker
    never reopens on its own: every subsequent ``get`` raises without issuing a
    request, so the failure cannot be reset by a request that is never made.
    That is the intent -- the point is to end a doomed sweep, and the pipeline
    builds a fresh client per run. Call ``reset_rate_limit_circuit()`` to clear
    it deliberately if a caller genuinely wants to retry within one process.
    """


class CFBDClient:
    """HTTP client for College Football Data API.

    Handles authentication, retries, and rate limiting.
    """

    BASE_URL = "https://api.collegefootballdata.com"
    DEFAULT_TIMEOUT = 30.0
    MAX_RETRIES = 3
    RETRY_DELAY = 1.0

    # Consecutive fully-rate-limited requests before the circuit opens.
    # Burst throttling clears in ~10 minutes and affects a handful of calls;
    # an exhausted monthly quota 429s everything until the quota resets. After
    # this many requests have each burned their full retry budget without one
    # success, the second explanation is the only one left, and every further
    # minute of waiting is wasted CI time.
    RATE_LIMIT_CIRCUIT_THRESHOLD = 5

    # Upper bound on a server-supplied Retry-After. Without it a hostile or
    # mistaken header could park the pipeline for hours inside one sleep.
    MAX_RETRY_AFTER_SECONDS = 120

    def __init__(self, api_key: str | None = None):
        """Initialize the client.

        Args:
            api_key: CFBD API key. If not provided, reads from dlt secrets.
        """
        if api_key is None:
            api_key = dlt.secrets.get("sources.cfbd.api_key")
            if not api_key:
                raise ValueError(
                    "CFBD API key not found. Set sources.cfbd.api_key in .dlt/secrets.toml"
                )

        self._api_key = api_key
        # Consecutive requests that exhausted their retry budget entirely on
        # 429s. Reset by any successful response, so a burst block that clears
        # never trips the breaker.
        self._consecutive_rate_limited = 0
        self._client = httpx.Client(
            base_url=self.BASE_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
            },
            timeout=self.DEFAULT_TIMEOUT,
        )

    def reset_rate_limit_circuit(self) -> None:
        """Clear the rate-limit breaker so this client can issue requests again.

        The breaker is deliberately terminal (see RateLimitCircuitOpen): it
        ends a doomed sweep and the pipeline builds a fresh client per run.
        This is the explicit escape hatch for a caller that wants to retry
        within one process -- e.g. after waiting out a quota reset.
        """
        self._consecutive_rate_limited = 0

    @classmethod
    def _parse_retry_after(cls, raw: str | None) -> int:
        """Seconds to wait from a ``Retry-After`` header, clamped and defensive.

        RFC 9110 permits either a delay in seconds or an HTTP-date, and servers
        do send both -- plus, in practice, occasional nonsense. A bare
        ``int(raw)`` raises ValueError from inside the 429 handler, which
        escapes as an unrelated crash and loses the rate-limit context
        entirely. Anything unparseable falls back to the default delay.

        Also clamped to MAX_RETRY_AFTER_SECONDS so a large or hostile value
        cannot park the pipeline inside a single sleep.
        """
        default = 60
        if raw is None:
            return default
        try:
            seconds = int(str(raw).strip())
        except (TypeError, ValueError):
            logger.warning("Unparseable Retry-After header %r; using %ds", raw, default)
            return default
        if seconds < 0:
            return default
        return min(seconds, cls.MAX_RETRY_AFTER_SECONDS)

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        retries: int = MAX_RETRIES,
    ) -> list[dict]:
        """Make a GET request to the API.

        Args:
            endpoint: API endpoint path (e.g., "/teams")
            params: Query parameters
            retries: Number of retries on failure

        Returns:
            JSON response as a list of dicts

        Raises:
            RateLimitCircuitOpen: too many consecutive requests were fully
                rate-limited -- the quota is almost certainly exhausted and
                the sweep stops immediately.
            RateLimitExhausted: this request was rate-limited on every attempt.
                Deliberately raised rather than returning ``[]``, which the
                caller cannot distinguish from a genuinely empty result.
        """
        if self._consecutive_rate_limited >= self.RATE_LIMIT_CIRCUIT_THRESHOLD:
            raise RateLimitCircuitOpen(
                f"{self._consecutive_rate_limited} consecutive request(s) exhausted their "
                f"retries on HTTP 429 before {endpoint}. The API is refusing everything -- "
                "most likely the monthly quota is spent. Stopping instead of retrying; "
                "further requests cannot succeed until the quota resets."
            )

        for attempt in range(retries + 1):
            try:
                response = self._client.get(endpoint, params=params)
                response.raise_for_status()
                # Any success clears the breaker: a transient burst block that
                # resolves must not accumulate toward the quota threshold.
                self._consecutive_rate_limited = 0
                return response.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:  # Rate limited
                    retry_after = self._parse_retry_after(e.response.headers.get("Retry-After"))
                    if attempt >= retries:
                        # Out of attempts. Count it and fail loudly -- falling
                        # through to an empty list here would tell the caller
                        # this endpoint has no data.
                        self._consecutive_rate_limited += 1
                        raise RateLimitExhausted(
                            f"Rate limited on all {retries + 1} attempt(s) for {endpoint} "
                            f"(params={params}). Consecutive rate-limited requests: "
                            f"{self._consecutive_rate_limited}."
                        ) from e
                    logger.warning(
                        f"Rate limited on {endpoint}. Waiting {retry_after}s "
                        f"(attempt {attempt + 1}/{retries + 1})..."
                    )
                    time.sleep(retry_after)
                    continue
                elif e.response.status_code >= 500 and attempt < retries:
                    logger.warning(
                        f"Server error {e.response.status_code}. Retry {attempt + 1}/{retries}"
                    )
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
                    continue
                raise
            except httpx.RequestError as e:
                if attempt < retries:
                    logger.warning(f"Request failed: {e}. Retry {attempt + 1}/{retries}")
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
                    continue
                raise

        # Unreachable: every path above returns or raises. Kept as a guard so a
        # future edit that adds a `continue` cannot silently reintroduce the
        # empty-result bug this method was rewritten to remove.
        raise AssertionError(f"retry loop for {endpoint} exited without returning or raising")

    def close(self):
        """Close the HTTP client."""
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def get_client() -> CFBDClient:
    """Get a configured CFBD client.

    Returns:
        Configured CFBDClient instance
    """
    return CFBDClient()
