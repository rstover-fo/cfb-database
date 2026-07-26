"""HTTP client for CFBD API with retry and rate limiting."""

import logging
import threading
import time
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import dlt
import httpx

logger = logging.getLogger(__name__)

# Consecutive HTTP 429 responses before the run is abandoned.
#
# Burst throttling clears in ~10 minutes and affects a handful of calls; an
# exhausted monthly quota 429s everything until the quota resets. Once this many
# responses in a row have been refused without one success, the second
# explanation is the only one left and every further minute is wasted CI time.
#
# Must stay comfortably ABOVE the dlt extract worker pool (.dlt/config.toml sets
# workers = 5). The counter is shared across the run, so N workers each taking a
# single transient 429 at the same instant contribute N at once: a threshold of
# 5 against a 5-worker pool could be reached by one simultaneous blip that every
# worker would have recovered from on retry, aborting a healthy run. 10 is
# double the pool, so it needs sustained refusal rather than one bad moment.
#
# Read the other way: a fully rate-limited request contributes MAX_RETRIES + 1
# = 4, so 10 is ~2.5 exhausted requests. Against a spent quota the daily load
# aborts around six minutes in rather than grinding through every source.
#
# Deliberately NOT a multiple of MAX_RETRIES + 1. At an exact multiple the
# threshold is only ever crossed on a request's FINAL attempt, where the
# out-of-retries branch raises first, so the mid-request abort below could never
# fire and a doomed request would always sleep out its whole budget.
#
# The trade-off is deliberate and worth stating: CFBD burst-blocks for ~10
# minutes at a time, and a burst long enough to cross this threshold with no
# successes in between WILL fail the run. That is the cheaper error for an
# idempotent job that retries tomorrow and opens an issue when it fails -- the
# alternative, historically, was a three-hour run that exhausted the quota.
RATE_LIMIT_CIRCUIT_THRESHOLD = 10


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

    **Terminal once open, by design.** The breaker never reopens on its own:
    every subsequent ``get`` raises without issuing a request, so the failure
    cannot be reset by a request that is never made. That is the intent -- the
    point is to end a doomed sweep. Call ``reset_rate_limit_circuit()`` to
    clear it deliberately if a caller genuinely wants to retry in-process.
    """


class RateLimitBreaker:
    """Consecutive-429 counter shared by every client in a pipeline run.

    Process-wide rather than per-client, and that distinction is the whole
    point. Sources call ``get_client()`` per resource function -- 60+ call
    sites -- and ``load_season.py`` moves to the next source when one fails.
    Nothing catches RateLimitExhausted while keeping its client alive:
    ``play_stats_resource`` catches only ``httpx.HTTPStatusError`` (to skip
    400s), so the first exhaustion unwinds the loop and its ``finally`` closes
    the client.

    Held on the instance, the counter therefore tops out at **1** and a
    threshold of 5 can never be reached -- the breaker would be unreachable
    code. Sharing one counter across the run is what makes it fire at all: N
    sources each burning a full retry budget against a spent quota is exactly
    the wasted-CI case it exists to stop.

    **Counted per 429 response, not per exhausted request.** Sharing the
    counter was necessary but not sufficient: while each increment cost a whole
    retry budget, reaching 5 took 20 API calls and 15 minutes, and a run with
    only 4 active sources could never reach it. The 2026-07-26 daily load
    proved it -- 12 minutes of sleeping against a spent quota, final count 4,
    breaker never opened. Counting responses makes the threshold mean what it
    says.

    Locked because dlt runs sources on a worker pool, so increments and resets
    genuinely race.
    """

    def __init__(self, threshold: int = RATE_LIMIT_CIRCUIT_THRESHOLD):
        self._threshold = threshold
        self._consecutive = 0
        self._lock = threading.Lock()

    @property
    def consecutive(self) -> int:
        with self._lock:
            return self._consecutive

    @property
    def threshold(self) -> int:
        return self._threshold

    def record_success(self) -> None:
        """Clear the count. Any success anywhere means the API is answering."""
        with self._lock:
            self._consecutive = 0

    def record_rate_limited(self) -> int:
        """Count one 429 RESPONSE.

        Counted per response, not per fully-exhausted request, and the unit is
        the whole point. Counting exhausted requests made each increment cost a
        full retry budget -- 4 API calls and 3 minutes of sleep -- so the
        threshold of 5 needed 20 wasted calls and 15 minutes before it could
        fire. An off-season run only has ~4 active sources, so it could not
        fire at all: the 2026-07-26 daily load spent 12 minutes retrying a
        spent quota and finished at a count of 4, one short, having never
        opened the breaker it was built to trigger.
        """
        with self._lock:
            self._consecutive += 1
            return self._consecutive

    def is_open(self) -> bool:
        with self._lock:
            return self._consecutive >= self._threshold

    def reset(self) -> None:
        with self._lock:
            self._consecutive = 0


# Both rate-limit failures, for callers with a broad ``except Exception`` that
# must not swallow them. A source that treats a 429 like "this week has no
# games" reproduces exactly the silent-partial-data bug these exceptions were
# added to prevent, so such handlers re-raise on this tuple first.
RATE_LIMIT_ERRORS = (RateLimitExhausted, RateLimitCircuitOpen)

# The run-wide breaker. Every CFBDClient shares it unless one is injected.
_SHARED_BREAKER = RateLimitBreaker()


def reset_rate_limit_circuit() -> None:
    """Clear the run-wide breaker so new requests are issued again.

    The breaker is deliberately terminal (see RateLimitCircuitOpen): it ends a
    doomed sweep. This is the explicit escape hatch -- e.g. a caller that has
    waited out a quota reset, or a test isolating itself from a prior run.
    """
    _SHARED_BREAKER.reset()


class CFBDClient:
    """HTTP client for College Football Data API.

    Handles authentication, retries, and rate limiting.
    """

    BASE_URL = "https://api.collegefootballdata.com"
    DEFAULT_TIMEOUT = 30.0
    MAX_RETRIES = 3
    RETRY_DELAY = 1.0

    RATE_LIMIT_CIRCUIT_THRESHOLD = RATE_LIMIT_CIRCUIT_THRESHOLD

    # Upper bound on a server-supplied Retry-After. Without it a hostile or
    # mistaken header could park the pipeline for hours inside one sleep.
    MAX_RETRY_AFTER_SECONDS = 120

    def __init__(self, api_key: str | None = None, breaker: RateLimitBreaker | None = None):
        """Initialize the client.

        Args:
            api_key: CFBD API key. If not provided, reads from dlt secrets.
            breaker: Rate-limit circuit breaker. Defaults to the run-wide one
                shared by every client, which is what makes the threshold
                reachable (see RateLimitBreaker). Inject a private instance to
                isolate a caller -- or a test -- from the rest of the run.
        """
        if api_key is None:
            api_key = dlt.secrets.get("sources.cfbd.api_key")
            if not api_key:
                raise ValueError(
                    "CFBD API key not found. Set sources.cfbd.api_key in .dlt/secrets.toml"
                )

        self._api_key = api_key
        self._breaker = breaker if breaker is not None else _SHARED_BREAKER
        self._client = httpx.Client(
            base_url=self.BASE_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
            },
            timeout=self.DEFAULT_TIMEOUT,
        )

    @property
    def _consecutive_rate_limited(self) -> int:
        """Read-through to the breaker this client uses."""
        return self._breaker.consecutive

    def reset_rate_limit_circuit(self) -> None:
        """Clear this client's breaker so it can issue requests again.

        Resets the run-wide breaker unless a private one was injected, since
        that is the one blocking every client in the run.
        """
        self._breaker.reset()

    @staticmethod
    def _http_date_delay(text: str, now: datetime | None = None) -> int | None:
        """Seconds until an HTTP-date ``Retry-After``, or None if not a date.

        Never negative: a date already in the past means "retry now".
        """
        try:
            when = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return None
        if when is None:
            return None
        if when.tzinfo is None:
            # RFC 9110 fixes HTTP-dates to GMT; a missing offset is not local time.
            when = when.replace(tzinfo=UTC)
        reference = now if now is not None else datetime.now(UTC)
        return max(0, int((when - reference).total_seconds()))

    @classmethod
    def _parse_retry_after(cls, raw: str | None, now: datetime | None = None) -> int:
        """Seconds to wait from a ``Retry-After`` header, clamped and defensive.

        RFC 9110 permits either a delay in seconds or an HTTP-date, and servers
        do send both -- plus, in practice, occasional nonsense. A bare
        ``int(raw)`` raises ValueError from inside the 429 handler, which
        escapes as an unrelated crash and loses the rate-limit context
        entirely, and treating a valid HTTP-date as the 60s default retries
        before the server said to -- which can exhaust a small retry budget on
        a request that waiting would have satisfied. So: seconds first, then
        HTTP-date relative to now, then the default.

        Clamped to MAX_RETRY_AFTER_SECONDS either way, so a distant date or a
        hostile value cannot park the pipeline inside a single sleep.
        """
        default = 60
        if raw is None:
            return default
        text = str(raw).strip()
        if not text:
            return default
        try:
            seconds = int(text)
        except (TypeError, ValueError):
            seconds = cls._http_date_delay(text, now)
            if seconds is None:
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
        if self._breaker.is_open():
            raise RateLimitCircuitOpen(
                f"{self._breaker.consecutive} consecutive HTTP 429 response(s) "
                f"before {endpoint}. The API is refusing everything -- "
                "most likely the monthly quota is spent. Stopping instead of retrying; "
                "further requests cannot succeed until the quota resets."
            )

        for attempt in range(retries + 1):
            try:
                response = self._client.get(endpoint, params=params)
                response.raise_for_status()
                # Any success clears the breaker: a transient burst block that
                # resolves must not accumulate toward the quota threshold.
                self._breaker.record_success()
                return response.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:  # Rate limited
                    retry_after = self._parse_retry_after(e.response.headers.get("Retry-After"))
                    # Every 429 counts, including the ones we are about to
                    # retry. See RateLimitBreaker.record_rate_limited.
                    consecutive = self._breaker.record_rate_limited()
                    if attempt >= retries:
                        # Out of attempts. Fail loudly -- falling through to an
                        # empty list here would tell the caller this endpoint
                        # has no data.
                        raise RateLimitExhausted(
                            f"Rate limited on all {retries + 1} attempt(s) for {endpoint} "
                            f"(params={params}). Consecutive rate-limited responses: "
                            f"{consecutive}."
                        ) from e
                    if self._breaker.is_open():
                        # Checked BEFORE sleeping, not just at the top of get().
                        # Sleeping out a retry budget we already know is doomed
                        # is the exact waste the breaker exists to prevent, and
                        # the top-of-method guard alone cannot stop it because
                        # a single request can burn its whole budget without
                        # ever re-entering get().
                        raise RateLimitCircuitOpen(
                            f"{consecutive} consecutive HTTP 429 response(s), most recently "
                            f"{endpoint}. The API is refusing everything -- most likely the "
                            "monthly quota is spent. Stopping instead of sleeping "
                            f"{retry_after}s for a retry that cannot succeed."
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
