"""Shared fixtures for cfb-database tests."""

import json
import tomllib
from pathlib import Path
from unittest.mock import patch

import psycopg2
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def _reset_rate_limit_breaker():
    """Clear the run-wide CFBD rate-limit breaker around every test.

    The breaker is deliberately process-wide (that is what makes its threshold
    reachable in production), so without this any test that trips it would
    leave every later CFBDClient raising RateLimitCircuitOpen -- an
    order-dependent failure in unrelated suites.
    """
    from src.pipelines.utils.api_client import reset_rate_limit_circuit

    reset_rate_limit_circuit()
    yield
    reset_rate_limit_circuit()


def _load_postgres_dsn() -> str:
    """Read the Postgres connection string from env var or .dlt/secrets.toml."""
    import os

    # CI: use SUPABASE_DB_URL environment variable
    env_url = os.environ.get("SUPABASE_DB_URL")
    if env_url:
        return env_url

    # Local: read from .dlt/secrets.toml
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
