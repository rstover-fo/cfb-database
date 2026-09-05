"""Tests for CFBD API client."""

import math
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.pipelines.utils.api_client import (
    RATE_LIMIT_ERRORS,
    CFBDClient,
    RateLimitBreaker,
    RateLimitCircuitOpen,
    RateLimitExhausted,
    reset_rate_limit_circuit,
)


class TestCFBDClient:
    def test_init_with_explicit_key(self):
        client = CFBDClient(api_key="test-key")
        assert client._api_key == "test-key"
        client.close()

    def test_init_without_key_raises(self):
        with patch("src.pipelines.utils.api_client.dlt") as mock_dlt:
            mock_dlt.secrets.get.return_value = None
            with pytest.raises(ValueError, match="CFBD API key not found"):
                CFBDClient()

    def test_auth_header(self):
        client = CFBDClient(api_key="test-key")
        assert client._client.headers["authorization"] == "Bearer test-key"
        assert client._client.headers["accept"] == "application/json"
        client.close()

    def test_base_url(self):
        client = CFBDClient(api_key="test-key")
        assert str(client._client.base_url) == "https://api.collegefootballdata.com"
        client.close()

    def test_context_manager(self):
        with CFBDClient(api_key="test-key") as client:
            assert client._api_key == "test-key"

    def test_get_success(self):
        client = CFBDClient(api_key="test-key")
        mock_response = MagicMock()
        mock_response.json.return_value = [{"id": 1}]
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._client, "get", return_value=mock_response):
            result = client.get("/teams")
            assert result == [{"id": 1}]

        client.close()

    def test_get_with_params(self):
        client = CFBDClient(api_key="test-key")
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._client, "get", return_value=mock_response) as mock_get:
            client.get("/games", params={"year": 2024})
            mock_get.assert_called_once_with("/games", params={"year": 2024})

        client.close()

    def test_retry_on_server_error(self):
        client = CFBDClient(api_key="test-key")

        error_response = MagicMock()
        error_response.status_code = 500

        success_response = MagicMock()
        success_response.json.return_value = [{"id": 1}]
        success_response.raise_for_status = MagicMock()

        with patch.object(
            client._client,
            "get",
            side_effect=[
                httpx.HTTPStatusError("500", request=MagicMock(), response=error_response),
                success_response,
            ],
        ):
            with patch("src.pipelines.utils.api_client.time.sleep"):
                result = client.get("/teams")
                assert result == [{"id": 1}]

        client.close()

    def test_rate_limit_429_waits(self):
        client = CFBDClient(api_key="test-key")

        rate_response = MagicMock()
        rate_response.status_code = 429
        rate_response.headers = {"Retry-After": "2"}

        success_response = MagicMock()
        success_response.json.return_value = []
        success_response.raise_for_status = MagicMock()

        with patch.object(
            client._client,
            "get",
            side_effect=[
                httpx.HTTPStatusError("429", request=MagicMock(), response=rate_response),
                success_response,
            ],
        ):
            with patch("src.pipelines.utils.api_client.time.sleep") as mock_sleep:
                client.get("/teams")
                mock_sleep.assert_called_with(2)

        client.close()


class TestTransientRetries:
    """Two incidents sized the transient budget; neither involved 429s.

    - Daily run 33418953608 (2026-08-31): a ~90s CFBD-wide 502 outage
      (17:20:31-17:22:01) killed reference/conferences/coaches/coach_profiles.
      Each request burned "Retry 1/3..3/3" with ~6s of total backoff -- far
      under the outage window -- while `games`, whose next attempt happened to
      land at outage-end, got a 200.
    - Backfill run 33347718722: advanced_team_stats/2014 died the same way on
      three ~31s read-timeout retries.

    So transient faults (5xx, connect/read timeouts) get 6 retries with
    2/4/8/16/30/60s exponential backoff: ~120s of sleep plus request time,
    enough to bridge a minute-plus outage even when the first attempt lands
    exactly at the outage's start (elapsed ~0/2/6/14/30/60/120s) -- the
    earlier 5-retry/~60s schedule only bridged a ~90s outage for
    favorably-phased requests. The 429 budget deliberately does NOT grow
    with it -- against a spent monthly quota every extra attempt is waste."""

    @staticmethod
    def _server_error(code=502):
        r = MagicMock()
        r.status_code = code
        return httpx.HTTPStatusError(str(code), request=MagicMock(), response=r)

    @staticmethod
    def _ok(payload=None):
        r = MagicMock()
        r.json.return_value = payload if payload is not None else [{"id": 1}]
        r.raise_for_status = MagicMock()
        return r

    def test_backoff_schedule_bridges_a_minute_outage(self):
        """The schedule is the contract: 6 retries, 2/4/8/16/30/60s, 120s total."""
        assert CFBDClient.TRANSIENT_MAX_RETRIES == 6
        assert CFBDClient.TRANSIENT_BACKOFF_SECONDS == (2.0, 4.0, 8.0, 16.0, 30.0, 60.0)
        assert sum(CFBDClient.TRANSIENT_BACKOFF_SECONDS) == 120.0

    def test_502_outage_recovers_on_fifth_attempt(self):
        """Four 502s then a 200 -- the shape of run 33418953608, which the old
        3-retry budget could not survive."""
        client = CFBDClient(api_key="test-key")
        with patch.object(
            client._client,
            "get",
            side_effect=[self._server_error(502)] * 4 + [self._ok()],
        ) as mock_get:
            with patch("src.pipelines.utils.api_client.time.sleep") as mock_sleep:
                assert client.get("/teams") == [{"id": 1}]
        assert mock_get.call_count == 5
        assert [c.args[0] for c in mock_sleep.call_args_list] == [2.0, 4.0, 8.0, 16.0]
        client.close()

    def test_502_outage_recovers_on_the_final_attempt(self):
        """The full budget: six 502s then a 200, having slept all ~120s."""
        client = CFBDClient(api_key="test-key")
        with patch.object(
            client._client,
            "get",
            side_effect=[self._server_error(502)] * 6 + [self._ok()],
        ) as mock_get:
            with patch("src.pipelines.utils.api_client.time.sleep") as mock_sleep:
                assert client.get("/teams") == [{"id": 1}]
        assert mock_get.call_count == 7
        assert [c.args[0] for c in mock_sleep.call_args_list] == [2.0, 4.0, 8.0, 16.0, 30.0, 60.0]
        client.close()

    def test_worst_case_phase_90s_outage_is_bridged_by_the_final_attempt(self):
        """Regression for the finding on daily run 33418953608 (a ~90s
        API-wide 502 window, 17:20:31-17:22:01): the OLD 5-retry/~60s
        schedule bridged such an outage only for favorably-phased requests --
        a request whose first attempt lands exactly at the outage's start
        spends its entire budget inside the window (elapsed ~0/2/6/14/30/60s)
        and the final attempt still 502s. Simulate that worst case with a
        fake clock advanced by each slept amount: 502 while the outage is
        still live, 200 once it has passed. The sixth backoff step (60s) is
        what finally lands an attempt past the outage's end (elapsed 120s)."""
        client = CFBDClient(api_key="test-key")
        clock = [0.0]
        call_clocks = []

        def fake_sleep(seconds):
            clock[0] += seconds

        def fake_get(*args, **kwargs):
            call_clocks.append(clock[0])
            if clock[0] < 90.0:
                raise self._server_error(502)
            return self._ok([{"id": 1}])

        with patch.object(client._client, "get", side_effect=fake_get):
            with patch("src.pipelines.utils.api_client.time.sleep", side_effect=fake_sleep):
                result = client.get("/teams")

        assert result == [{"id": 1}]
        assert call_clocks[-1] >= 90.0
        client.close()

    def test_timeouts_exhaust_all_retries_then_raise(self):
        """Persistent read timeouts burn the whole budget and raise from INSIDE
        the guarded loop -- exactly budget + 1 attempts, no extra unguarded
        call after the final backoff."""
        client = CFBDClient(api_key="test-key")
        with patch.object(
            client._client, "get", side_effect=httpx.ReadTimeout("read timed out")
        ) as mock_get:
            with patch("src.pipelines.utils.api_client.time.sleep") as mock_sleep:
                with pytest.raises(httpx.ReadTimeout):
                    client.get("/stats/season/advanced", params={"year": 2014})
        assert mock_get.call_count == CFBDClient.TRANSIENT_MAX_RETRIES + 1
        assert [c.args[0] for c in mock_sleep.call_args_list] == [2.0, 4.0, 8.0, 16.0, 30.0, 60.0]
        client.close()

    def test_connect_errors_use_the_same_transient_budget(self):
        client = CFBDClient(api_key="test-key")
        with patch.object(
            client._client,
            "get",
            side_effect=[httpx.ConnectError("refused"), self._ok([])],
        ) as mock_get:
            with patch("src.pipelines.utils.api_client.time.sleep") as mock_sleep:
                assert client.get("/teams") == []
        assert mock_get.call_count == 2
        assert [c.args[0] for c in mock_sleep.call_args_list] == [2.0]
        client.close()

    def test_429_budget_is_untouched_by_the_transient_budget(self):
        """Rate limiting keeps its own, smaller budget: 4 attempts total,
        sleeps governed by Retry-After (not exponential backoff), then
        RateLimitExhausted. The transient change must not leak into it."""
        assert CFBDClient.MAX_RETRIES == 3
        client = CFBDClient(api_key="test-key", breaker=RateLimitBreaker())
        with patch.object(client._client, "get", side_effect=_rate_limit_error()) as mock_get:
            with patch("src.pipelines.utils.api_client.time.sleep") as mock_sleep:
                with pytest.raises(RateLimitExhausted, match="all 4 attempt"):
                    client.get("/plays/stats", params={"gameId": 401767542})
        assert mock_get.call_count == CFBDClient.MAX_RETRIES + 1  # 4, not 6
        # Retry-After ("1") governs every sleep -- no exponential schedule.
        assert [c.args[0] for c in mock_sleep.call_args_list] == [1, 1, 1]
        assert client._consecutive_rate_limited == CFBDClient.MAX_RETRIES + 1
        client.close()

    def test_4xx_is_never_retried(self):
        client = CFBDClient(api_key="test-key")
        r = MagicMock()
        r.status_code = 404
        err = httpx.HTTPStatusError("404", request=MagicMock(), response=r)
        with patch.object(client._client, "get", side_effect=err) as mock_get:
            with patch("src.pipelines.utils.api_client.time.sleep") as mock_sleep:
                with pytest.raises(httpx.HTTPStatusError):
                    client.get("/nope")
        assert mock_get.call_count == 1
        mock_sleep.assert_not_called()
        client.close()


# dlt extract worker pool size (.dlt/config.toml: workers = 5). The shared
# breaker sees all of them, so the circuit threshold has to clear this.
DLT_EXTRACT_WORKERS = 5

# Fully-rate-limited requests needed to cross the circuit threshold. Derived so
# the tests stay meaningful if the threshold or the retry budget changes.
REQUESTS_TO_TRIP = math.ceil(CFBDClient.RATE_LIMIT_CIRCUIT_THRESHOLD / (CFBDClient.MAX_RETRIES + 1))


def _rate_limited_response(retry_after="1"):
    r = MagicMock()
    r.status_code = 429
    r.headers = {"Retry-After": retry_after}
    return r


def _rate_limit_error():
    return httpx.HTTPStatusError("429", request=MagicMock(), response=_rate_limited_response())


class TestRateLimitExhaustion:
    """A 2026-07-25 daily load spent 3 hours here: every /plays/stats request
    was 429'd, each burned its retry budget, and `get` then returned an empty
    list -- which dlt recorded as "this game has no play stats". Silent data
    loss dressed as success, and the job was killed at the 3-hour mark with
    every downstream step skipped."""

    def test_exhausted_retries_raise_instead_of_returning_empty(self):
        client = CFBDClient(api_key="test-key")
        with patch.object(client._client, "get", side_effect=_rate_limit_error()):
            with patch("src.pipelines.utils.api_client.time.sleep"):
                with pytest.raises(RateLimitExhausted, match="Rate limited on all"):
                    client.get("/plays/stats", params={"gameId": 401767542})
        client.close()

    def test_empty_list_is_never_returned_for_a_rate_limited_request(self):
        """The specific regression: `[]` and "no data" are indistinguishable
        to the caller, so the failure must not be expressible as a value."""
        client = CFBDClient(api_key="test-key")
        with patch.object(client._client, "get", side_effect=_rate_limit_error()):
            with patch("src.pipelines.utils.api_client.time.sleep"):
                result = None
                try:
                    result = client.get("/plays/stats")
                except RateLimitExhausted:
                    pass
                assert result is None, "a rate-limited request must not produce a value"
        client.close()

    def test_a_success_still_returns_normally(self):
        client = CFBDClient(api_key="test-key")
        ok = MagicMock()
        ok.json.return_value = [{"id": 1}]
        ok.raise_for_status = MagicMock()
        with patch.object(client._client, "get", side_effect=[_rate_limit_error(), ok]):
            with patch("src.pipelines.utils.api_client.time.sleep"):
                assert client.get("/teams") == [{"id": 1}]
        client.close()

    def test_retry_after_is_capped(self):
        """A server-supplied Retry-After must not park the pipeline for hours."""
        client = CFBDClient(api_key="test-key")
        huge = MagicMock()
        huge.status_code = 429
        huge.headers = {"Retry-After": "86400"}
        err = httpx.HTTPStatusError("429", request=MagicMock(), response=huge)
        ok = MagicMock()
        ok.json.return_value = []
        ok.raise_for_status = MagicMock()
        with patch.object(client._client, "get", side_effect=[err, ok]):
            with patch("src.pipelines.utils.api_client.time.sleep") as mock_sleep:
                client.get("/teams")
                mock_sleep.assert_called_with(CFBDClient.MAX_RETRY_AFTER_SECONDS)
        client.close()


class TestRateLimitCircuitBreaker:
    """Quota exhaustion 429s everything. Grinding at one request per minute
    for three hours cannot succeed; the sweep must abort in seconds."""

    def _exhaust(self, client, n):
        """Drive `n` fully-rate-limited requests.

        Each records `retries + 1` 429s now that the breaker counts responses,
        so this trips the threshold far faster than it used to -- which is the
        entire point of the change. Either rate-limit error is acceptable: once
        the breaker opens mid-request the raise becomes RateLimitCircuitOpen.
        """
        with patch.object(client._client, "get", side_effect=_rate_limit_error()):
            with patch("src.pipelines.utils.api_client.time.sleep"):
                for _ in range(n):
                    with pytest.raises(RATE_LIMIT_ERRORS):
                        client.get("/plays/stats")

    def test_one_exhausted_request_counts_every_attempt(self):
        """The unit is the 429 response, not the exhausted request."""
        client = CFBDClient(api_key="test-key")
        self._exhaust(client, 1)
        assert client._consecutive_rate_limited == CFBDClient.MAX_RETRIES + 1
        client.close()

    def test_circuit_opens_after_threshold(self):
        client = CFBDClient(api_key="test-key")
        self._exhaust(client, REQUESTS_TO_TRIP)
        # The next call must fail immediately, without sleeping at all.
        with patch("src.pipelines.utils.api_client.time.sleep") as mock_sleep:
            with pytest.raises(RateLimitCircuitOpen, match="quota is spent"):
                client.get("/plays/stats")
            mock_sleep.assert_not_called()
        client.close()

    def test_circuit_stays_closed_below_threshold(self):
        client = CFBDClient(api_key="test-key")
        self._exhaust(client, 1)
        assert client._consecutive_rate_limited < CFBDClient.RATE_LIMIT_CIRCUIT_THRESHOLD
        ok = MagicMock()
        ok.json.return_value = [{"ok": True}]
        ok.raise_for_status = MagicMock()
        with patch.object(client._client, "get", return_value=ok):
            assert client.get("/teams") == [{"ok": True}]
        client.close()

    def test_a_success_resets_the_breaker(self):
        """A burst block that clears must not accumulate toward the quota
        threshold -- otherwise transient throttling eventually halts a healthy
        pipeline."""
        client = CFBDClient(api_key="test-key")
        self._exhaust(client, 1)
        ok = MagicMock()
        ok.json.return_value = []
        ok.raise_for_status = MagicMock()
        with patch.object(client._client, "get", return_value=ok):
            client.get("/teams")
        assert client._consecutive_rate_limited == 0
        # Full budget available again.
        self._exhaust(client, 1)
        with patch.object(client._client, "get", return_value=ok):
            assert client.get("/teams") == []
        client.close()

    def test_breaker_is_checked_before_sleeping_not_only_between_requests(self):
        """The regression from the 2026-07-26 daily load.

        A single request can burn its entire retry budget without re-entering
        get(), so the top-of-method guard alone never sees it. Once the
        threshold is crossed mid-request, the remaining sleeps must be
        abandoned rather than served.
        """
        client = CFBDClient(api_key="test-key")
        # Stop one request short of tripping, so the threshold is crossed
        # partway through the NEXT request rather than between requests.
        self._exhaust(client, REQUESTS_TO_TRIP - 1)
        with patch.object(client._client, "get", side_effect=_rate_limit_error()):
            with patch("src.pipelines.utils.api_client.time.sleep") as mock_sleep:
                with pytest.raises(RateLimitCircuitOpen):
                    client.get("/teams")
                # It may sleep once or twice before crossing, but it must NOT
                # serve the full retry budget: abandoning the remaining sleeps
                # is the whole behaviour under test.
                assert mock_sleep.call_count < CFBDClient.MAX_RETRIES
        client.close()

    def test_one_transient_429_per_worker_does_not_trip_the_breaker(self):
        """The regression introduced BY the per-response counting change.

        The breaker is shared across the run and dlt extracts on a worker pool
        (.dlt/config.toml: workers = 5). Counting every 429 means N concurrent
        workers each taking a single transient 429 contribute N at once, so a
        threshold at or below the pool size could be reached by one simultaneous
        blip that every worker would have recovered from -- turning five
        successful retries into an aborted run.
        """
        assert CFBDClient.RATE_LIMIT_CIRCUIT_THRESHOLD > DLT_EXTRACT_WORKERS

        client = CFBDClient(api_key="test-key")
        ok = MagicMock()
        ok.json.return_value = []
        ok.raise_for_status = MagicMock()
        # One 429 then a success, once per worker, with no reset in between
        # until each recovers -- the shape of a momentary blip across the pool.
        with patch("src.pipelines.utils.api_client.time.sleep"):
            for _ in range(DLT_EXTRACT_WORKERS):
                with patch.object(client._client, "get", side_effect=[_rate_limit_error(), ok]):
                    assert client.get("/teams") == []
        assert not client._breaker.is_open()
        client.close()

    def test_offseason_run_shape_trips_the_breaker(self):
        """Under the old per-request counting this was unreachable.

        The 2026-07-26 daily load ran 4 sources against a spent quota. Each
        burned 4 attempts and 3 minutes; the counter finished at 4 against a
        threshold of 5, so the breaker never opened and the run spent 12
        minutes sleeping. Counting responses, the same shape trips it during
        the second source.
        """
        client = CFBDClient(api_key="test-key")
        sleeps = []
        with patch.object(client._client, "get", side_effect=_rate_limit_error()):
            with patch("src.pipelines.utils.api_client.time.sleep", sleeps.append):
                for _ in range(4):  # reference, metrics_wp, games, betting
                    with pytest.raises(RATE_LIMIT_ERRORS):
                        client.get("/whatever")
        # Old behaviour slept 3x per source across all four sources.
        assert len(sleeps) < 3 * 4
        assert client._consecutive_rate_limited >= CFBDClient.RATE_LIMIT_CIRCUIT_THRESHOLD
        client.close()

    def test_non_rate_limit_errors_do_not_trip_the_breaker(self):
        client = CFBDClient(api_key="test-key")
        boom = MagicMock()
        boom.status_code = 404
        err = httpx.HTTPStatusError("404", request=MagicMock(), response=boom)
        with patch.object(client._client, "get", side_effect=err):
            with pytest.raises(httpx.HTTPStatusError):
                client.get("/nope")
        assert client._consecutive_rate_limited == 0
        client.close()


class TestRetryAfterParsing:
    """Found by the pre-PR adversarial pass: `int(header)` raised ValueError
    from inside the 429 handler, escaping as an unrelated crash that lost the
    rate-limit context. RFC 9110 allows an HTTP-date as well as seconds."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("30", 30),
            (" 45 ", 45),
            (None, 60),
            ("soon", 60),
            ("", 60),
            ("-5", 60),
            ("86400", CFBDClient.MAX_RETRY_AFTER_SECONDS),
        ],
    )
    def test_parse_retry_after(self, raw, expected):
        assert CFBDClient._parse_retry_after(raw) == expected

    def test_unparseable_header_does_not_crash_the_request(self):
        client = CFBDClient(api_key="test-key")
        bad = MagicMock()
        bad.status_code = 429
        bad.headers = {"Retry-After": "whenever"}
        err = httpx.HTTPStatusError("429", request=MagicMock(), response=bad)
        ok = MagicMock()
        ok.json.return_value = [{"ok": True}]
        ok.raise_for_status = MagicMock()
        with patch.object(client._client, "get", side_effect=[err, ok]):
            with patch("src.pipelines.utils.api_client.time.sleep") as mock_sleep:
                assert client.get("/teams") == [{"ok": True}]
                mock_sleep.assert_called_with(60)
        client.close()


class TestRetryAfterHttpDate:
    """Codex review on PR #49: a valid HTTP-date fell through to the flat 60s
    default. When the date is further out than that, the client retries before
    the server said to and can burn its whole budget -- raising
    RateLimitExhausted on a request that simply waiting would have satisfied.
    """

    NOW = datetime(2026, 10, 21, 7, 28, 0, tzinfo=UTC)

    @pytest.mark.parametrize(
        "raw,expected",
        [
            # 90s out: honored exactly, not collapsed to the 60s default.
            ("Wed, 21 Oct 2026 07:29:30 GMT", 90),
            # Already past: retry now rather than sleeping a default minute.
            ("Wed, 21 Oct 2026 07:00:00 GMT", 0),
            # Distant date still cannot park the pipeline.
            ("Fri, 25 Dec 2026 00:00:00 GMT", CFBDClient.MAX_RETRY_AFTER_SECONDS),
            # RFC 9110 fixes HTTP-dates to GMT; a missing offset is not local time.
            ("Wed, 21 Oct 2026 07:29:00", 60),
        ],
    )
    def test_http_date_is_honored_relative_to_now(self, raw, expected):
        assert CFBDClient._parse_retry_after(raw, now=self.NOW) == expected

    def test_a_date_beyond_the_default_is_actually_slept(self):
        """The regression in effect: 90s away must sleep 90, not 60."""
        client = CFBDClient(api_key="test-key")
        far = MagicMock()
        far.status_code = 429
        far.headers = {"Retry-After": format_datetime(self.NOW + timedelta(seconds=90))}
        err = httpx.HTTPStatusError("429", request=MagicMock(), response=far)
        ok = MagicMock()
        ok.json.return_value = []
        ok.raise_for_status = MagicMock()
        with patch.object(client._client, "get", side_effect=[err, ok]):
            with patch("src.pipelines.utils.api_client.time.sleep") as mock_sleep:
                with patch("src.pipelines.utils.api_client.datetime") as mock_dt:
                    mock_dt.now.return_value = self.NOW
                    client.get("/teams")
                slept = mock_sleep.call_args[0][0]
                assert 85 <= slept <= 95, f"expected ~90s from the header, slept {slept}"
        client.close()


class TestBreakerIsSharedAcrossClients:
    """Codex review on PR #49: the breaker lived on the client instance, but
    every source calls `get_client()` fresh and nothing catches
    RateLimitExhausted while keeping its client alive -- `play_stats_resource`
    catches only httpx.HTTPStatusError, so the first exhaustion unwinds the
    loop and `finally` closes the client.

    The per-instance counter therefore topped out at 1, the threshold of 5 was
    unreachable, and the breaker was dead code: a spent quota still burned a
    full retry budget separately for every one of the ~60 resources."""

    # Each fully-rate-limited request now records MAX_RETRIES + 1 responses, so
    # two sources are enough to cross a threshold of 5 (see
    # RateLimitBreaker.record_rate_limited).
    SOURCES_TO_TRIP = REQUESTS_TO_TRIP

    def _exhaust_on_a_fresh_client(self, n):
        """Mimic the real call graph: one new client per rate-limited source."""
        for _ in range(n):
            client = CFBDClient(api_key="test-key")
            try:
                with patch.object(client._client, "get", side_effect=_rate_limit_error()):
                    with patch("src.pipelines.utils.api_client.time.sleep"):
                        with pytest.raises(RATE_LIMIT_ERRORS):
                            client.get("/plays/stats")
            finally:
                client.close()

    def test_threshold_is_reachable_across_separate_clients(self):
        self._exhaust_on_a_fresh_client(self.SOURCES_TO_TRIP)
        later = CFBDClient(api_key="test-key")
        with patch("src.pipelines.utils.api_client.time.sleep") as mock_sleep:
            with pytest.raises(RateLimitCircuitOpen, match="quota is spent"):
                later.get("/games")
            mock_sleep.assert_not_called()
        later.close()

    def test_below_threshold_a_new_client_still_works(self):
        self._exhaust_on_a_fresh_client(1)
        later = CFBDClient(api_key="test-key")
        ok = MagicMock()
        ok.json.return_value = [{"ok": True}]
        ok.raise_for_status = MagicMock()
        with patch.object(later._client, "get", return_value=ok):
            assert later.get("/games") == [{"ok": True}]
        later.close()

    def test_a_success_on_any_client_clears_the_shared_count(self):
        self._exhaust_on_a_fresh_client(1)
        healthy = CFBDClient(api_key="test-key")
        ok = MagicMock()
        ok.json.return_value = []
        ok.raise_for_status = MagicMock()
        with patch.object(healthy._client, "get", return_value=ok):
            healthy.get("/teams")
        healthy.close()
        # Budget restored for everyone, so the breaker must still be closed.
        self._exhaust_on_a_fresh_client(1)
        later = CFBDClient(api_key="test-key")
        with patch.object(later._client, "get", return_value=ok):
            assert later.get("/games") == []
        later.close()

    def test_an_injected_breaker_isolates_a_client(self):
        """The escape hatch: a caller that must not be halted by the run."""
        self._exhaust_on_a_fresh_client(self.SOURCES_TO_TRIP)
        private = CFBDClient(api_key="test-key", breaker=RateLimitBreaker())
        ok = MagicMock()
        ok.json.return_value = [{"ok": True}]
        ok.raise_for_status = MagicMock()
        with patch.object(private._client, "get", return_value=ok):
            assert private.get("/teams") == [{"ok": True}]
        private.close()

    def test_module_level_reset_clears_the_run(self):
        self._exhaust_on_a_fresh_client(self.SOURCES_TO_TRIP)
        reset_rate_limit_circuit()
        later = CFBDClient(api_key="test-key")
        ok = MagicMock()
        ok.json.return_value = []
        ok.raise_for_status = MagicMock()
        with patch.object(later._client, "get", return_value=ok):
            assert later.get("/teams") == []
        later.close()


class TestCircuitIsTerminalButResettable:
    """Also from the adversarial pass: once open, the breaker raises before
    issuing any request, so a success can never reset it -- the client is dead
    for its lifetime. That is the intended abort semantics, but it must be
    documented and deliberately escapable rather than a surprise."""

    def _open_circuit(self, client):
        err = httpx.HTTPStatusError("429", request=MagicMock(), response=_rate_limited_response())
        with patch.object(client._client, "get", side_effect=err):
            with patch("src.pipelines.utils.api_client.time.sleep"):
                # Two fully-rate-limited requests now exceed the threshold,
                # since every 429 response counts rather than every exhausted
                # request. Loop until it is actually open rather than assuming
                # a fixed request count.
                while not client._breaker.is_open():
                    with pytest.raises(RATE_LIMIT_ERRORS):
                        client.get("/plays/stats")

    def test_open_circuit_does_not_self_heal_even_if_the_api_recovers(self):
        client = CFBDClient(api_key="test-key")
        self._open_circuit(client)
        ok = MagicMock()
        ok.json.return_value = [{"ok": True}]
        ok.raise_for_status = MagicMock()
        with patch.object(client._client, "get", return_value=ok):
            with pytest.raises(RateLimitCircuitOpen):
                client.get("/teams")
        client.close()

    def test_explicit_reset_restores_the_client(self):
        client = CFBDClient(api_key="test-key")
        self._open_circuit(client)
        client.reset_rate_limit_circuit()
        ok = MagicMock()
        ok.json.return_value = [{"ok": True}]
        ok.raise_for_status = MagicMock()
        with patch.object(client._client, "get", return_value=ok):
            assert client.get("/teams") == [{"ok": True}]
        client.close()


class TestResourceHandlersPropagateFailures:
    """Resource wrappers must preserve the client's explicit failure causes."""

    @staticmethod
    def _rate_limited_client():
        client = CFBDClient(api_key="test-key")
        patcher = patch.object(client._client, "get", side_effect=_rate_limit_error())
        patcher.start()
        return client, patcher

    @staticmethod
    def _assert_rate_limit_escapes(resource):
        """dlt wraps a generator exception in ResourceExtractionError, so assert
        on the chained cause -- the point is that it escaped the loop at all."""
        from dlt.extract.exceptions import ResourceExtractionError

        with pytest.raises(ResourceExtractionError) as exc_info:
            list(resource)
        causes = []
        err = exc_info.value
        while err is not None:
            causes.append(type(err))
            err = err.__cause__ or err.__context__
        assert any(c in RATE_LIMIT_ERRORS for c in causes), (
            f"rate limit was swallowed; chain was {causes}"
        )

    @pytest.mark.parametrize("exc", [RateLimitExhausted, RateLimitCircuitOpen])
    def test_rate_limit_errors_tuple_covers_both(self, exc):
        assert exc in RATE_LIMIT_ERRORS

    def test_game_team_stats_propagates_rate_limit(self):
        from src.pipelines.sources.game_stats import game_team_stats_resource

        client, patcher = self._rate_limited_client()
        try:
            with patch("src.pipelines.sources.game_stats.get_client", return_value=client):
                with patch("src.pipelines.utils.api_client.time.sleep"):
                    res = game_team_stats_resource([2026], season_type="regular", weeks=[1, 2, 3])
                    self._assert_rate_limit_escapes(res)
        finally:
            patcher.stop()
            client.close()

    def test_game_player_stats_propagates_rate_limit(self):
        from src.pipelines.sources.game_stats import game_player_stats_resource

        client, patcher = self._rate_limited_client()
        try:
            with patch("src.pipelines.sources.game_stats.get_client", return_value=client):
                with patch("src.pipelines.utils.api_client.time.sleep"):
                    res = game_player_stats_resource([2026], season_type="regular", weeks=[1, 2, 3])
                    self._assert_rate_limit_escapes(res)
        finally:
            patcher.stop()
            client.close()

    def test_roster_propagates_rate_limit_instead_of_warning(self):
        from src.pipelines.sources.rosters import rosters_resource

        client, patcher = self._rate_limited_client()
        try:
            with patch("src.pipelines.sources.rosters.get_client", return_value=client):
                with patch("src.pipelines.utils.api_client.time.sleep"):
                    res = rosters_resource(teams=["Oklahoma", "Texas"], years=[2026])
                    self._assert_rate_limit_escapes(res)
        finally:
            patcher.stop()
            client.close()

    def test_non_rate_limit_errors_are_contextual_failures(self):
        """A provider 400 is not evidence that a week has no games."""
        from dlt.extract.exceptions import ResourceExtractionError

        from src.pipelines.sources.game_stats import game_team_stats_resource
        from src.pipelines.utils.request_outcomes import request_failure_summary

        client = CFBDClient(api_key="test-key")
        boom = MagicMock()
        boom.status_code = 400
        err = httpx.HTTPStatusError("400", request=MagicMock(), response=boom)
        try:
            with patch.object(client._client, "get", side_effect=err):
                with patch("src.pipelines.sources.game_stats.get_client", return_value=client):
                    res = game_team_stats_resource([2026], season_type="regular", weeks=[1, 2])
                    with pytest.raises(ResourceExtractionError) as exc_info:
                        list(res)
            assert request_failure_summary(exc_info.value) == {
                "endpoint": "/games/teams",
                "params": {"year": 2026, "seasonType": "regular", "week": 1},
                "outcome": "failed",
                "error_type": "HTTPStatusError",
                "counts_scope": "resource_invocation",
                "counts_unit": "requests",
                "counts": {
                    "succeeded": 0,
                    "expected_no_data": 0,
                    "failed": 1,
                    "deferred": 1,
                },
            }
        finally:
            client.close()
