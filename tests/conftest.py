"""Shared fixtures for cfb-database tests."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture
def tmp_state_file(tmp_path: Path) -> Path:
    """Provide a temporary state file path for rate limiter tests."""
    return tmp_path / "rate_limit_state.json"


@pytest.fixture
def mock_state_file(tmp_path: Path) -> Path:
    """Provide a pre-populated state file for rate limiter tests."""
    state_file = tmp_path / "rate_limit_state.json"
    state_file.write_text(
        json.dumps(
            {
                "month": "2026-01",
                "calls_used": 500,
                "monthly_budget": 75000,
                "last_updated": "2026-01-15T12:00:00",
            }
        )
    )
    return state_file


@pytest.fixture
def mock_cfbd_client():
    """Mock CFBD API client that returns empty responses."""
    with patch("src.pipelines.utils.api_client.dlt") as mock_dlt:
        mock_dlt.secrets.get.return_value = "test-api-key"
        from src.pipelines.utils.api_client import CFBDClient

        client = CFBDClient(api_key="test-api-key")
        yield client
        client.close()
