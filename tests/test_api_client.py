"""Tests for CFBD API client."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.pipelines.utils.api_client import (
    CFBDClient,
    RateLimitCircuitOpen,
    RateLimitExhausted,
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
        with patch.object(client._client, "get", side_effect=_rate_limit_error()):
            with patch("src.pipelines.utils.api_client.time.sleep"):
                for _ in range(n):
                    with pytest.raises(RateLimitExhausted):
                        client.get("/plays/stats")

    def test_circuit_opens_after_threshold(self):
        client = CFBDClient(api_key="test-key")
        self._exhaust(client, CFBDClient.RATE_LIMIT_CIRCUIT_THRESHOLD)
        # The next call must fail immediately, without sleeping at all.
        with patch("src.pipelines.utils.api_client.time.sleep") as mock_sleep:
            with pytest.raises(RateLimitCircuitOpen, match="quota is spent"):
                client.get("/plays/stats")
            mock_sleep.assert_not_called()
        client.close()

    def test_circuit_stays_closed_below_threshold(self):
        client = CFBDClient(api_key="test-key")
        self._exhaust(client, CFBDClient.RATE_LIMIT_CIRCUIT_THRESHOLD - 1)
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
        self._exhaust(client, CFBDClient.RATE_LIMIT_CIRCUIT_THRESHOLD - 1)
        ok = MagicMock()
        ok.json.return_value = []
        ok.raise_for_status = MagicMock()
        with patch.object(client._client, "get", return_value=ok):
            client.get("/teams")
        assert client._consecutive_rate_limited == 0
        # Full budget available again.
        self._exhaust(client, CFBDClient.RATE_LIMIT_CIRCUIT_THRESHOLD - 1)
        with patch.object(client._client, "get", return_value=ok):
            assert client.get("/teams") == []
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
            ("Wed, 21 Oct 2026 07:28:00 GMT", 60),
            ("", 60),
            ("-5", 60),
            ("86400", CFBDClient.MAX_RETRY_AFTER_SECONDS),
        ],
    )
    def test_parse_retry_after(self, raw, expected):
        assert CFBDClient._parse_retry_after(raw) == expected

    def test_malformed_header_does_not_crash_the_request(self):
        client = CFBDClient(api_key="test-key")
        bad = MagicMock()
        bad.status_code = 429
        bad.headers = {"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}
        err = httpx.HTTPStatusError("429", request=MagicMock(), response=bad)
        ok = MagicMock()
        ok.json.return_value = [{"ok": True}]
        ok.raise_for_status = MagicMock()
        with patch.object(client._client, "get", side_effect=[err, ok]):
            with patch("src.pipelines.utils.api_client.time.sleep") as mock_sleep:
                assert client.get("/teams") == [{"ok": True}]
                mock_sleep.assert_called_with(60)
        client.close()


class TestCircuitIsTerminalButResettable:
    """Also from the adversarial pass: once open, the breaker raises before
    issuing any request, so a success can never reset it -- the client is dead
    for its lifetime. That is the intended abort semantics, but it must be
    documented and deliberately escapable rather than a surprise."""

    def _open_circuit(self, client):
        err = httpx.HTTPStatusError("429", request=MagicMock(), response=_rate_limited_response())
        with patch.object(client._client, "get", side_effect=err):
            with patch("src.pipelines.utils.api_client.time.sleep"):
                for _ in range(CFBDClient.RATE_LIMIT_CIRCUIT_THRESHOLD):
                    with pytest.raises(RateLimitExhausted):
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
