"""Tests for CFBD API client."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.pipelines.utils.api_client import CFBDClient


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
