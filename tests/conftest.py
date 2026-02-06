"""Shared fixtures for cfb-database tests."""

import json
import tomllib
from pathlib import Path
from unittest.mock import patch

import psycopg2
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_postgres_dsn() -> str:
    """Read the Postgres connection string from .dlt/secrets.toml."""
    secrets_path = PROJECT_ROOT / ".dlt" / "secrets.toml"
    if not secrets_path.exists():
        pytest.skip(f"Secrets file not found: {secrets_path}")
    with open(secrets_path, "rb") as f:
        secrets = tomllib.load(f)
    try:
        return secrets["destination"]["postgres"]["credentials"]
    except KeyError:
        pytest.skip("destination.postgres.credentials not found in secrets.toml")


@pytest.fixture(scope="module")
def db_conn():
    """Module-scoped Postgres connection for database integration tests."""
    dsn = _load_postgres_dsn()
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    yield conn
    conn.close()


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
