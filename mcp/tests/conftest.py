"""Shared pytest fixtures for cfb_mcp tests.

No live network is used anywhere in this test suite -- every PostgREST call
is intercepted by respx. (The sandbox's outbound proxy blocks *.supabase.co
anyway, but the tests are hermetic regardless of where they run.)
"""

import pytest

TEST_BASE_URL = "https://test-project.supabase.co"
TEST_ANON_KEY = "test-anon-key"


@pytest.fixture(autouse=True)
def supabase_env(monkeypatch):
    """Every test gets a valid, fake Supabase config unless it overrides env itself."""
    monkeypatch.setenv("SUPABASE_URL", TEST_BASE_URL)
    monkeypatch.setenv("SUPABASE_ANON_KEY", TEST_ANON_KEY)
