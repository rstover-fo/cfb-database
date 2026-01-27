"""HTTP client for CFBD API with retry and rate limiting."""

import logging
import time
from typing import Any

import dlt
import httpx

logger = logging.getLogger(__name__)


class CFBDClient:
    """HTTP client for College Football Data API.

    Handles authentication, retries, and rate limiting.
    """

    BASE_URL = "https://api.collegefootballdata.com"
    DEFAULT_TIMEOUT = 30.0
    MAX_RETRIES = 3
    RETRY_DELAY = 1.0

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
        self._client = httpx.Client(
            base_url=self.BASE_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
            },
            timeout=self.DEFAULT_TIMEOUT,
        )

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
        """
        for attempt in range(retries + 1):
            try:
                response = self._client.get(endpoint, params=params)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:  # Rate limited
                    retry_after = int(e.response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited. Waiting {retry_after}s...")
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

        return []

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
