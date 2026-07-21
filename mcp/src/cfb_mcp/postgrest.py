"""Thin async PostgREST client for the cfb-database Supabase warehouse.

Design constraints (see mcp/README.md and docs/SCHEMA_CONTRACT.md):

- PostgREST over HTTPS ONLY. No direct Postgres connection, ever.
- Reads go to ``GET {SUPABASE_URL}/rest/v1/<view>`` with an
  ``Accept-Profile: api`` header, since every readable view lives in the
  ``api`` schema (Contract Rule 4: raw tables and other schemas are not
  part of the public surface this server may touch).
- RPC calls go to ``POST {SUPABASE_URL}/rest/v1/rpc/<fn>`` with a
  ``Content-Profile: public`` header, since every callable function is a
  ``public`` schema RPC.
- Every read is capped at ``DEFAULT_ROW_CAP`` rows. Callers may request a
  smaller limit; they can never request more than the cap.

This module has no knowledge of MCP tool semantics -- it only knows how to
build PostgREST requests and turn PostgREST/network errors into short,
actionable messages that a tool can hand straight back to the calling LLM.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import httpx

DEFAULT_ROW_CAP = 100
REQUEST_TIMEOUT = 30.0


class PostgrestError(Exception):
    """Raised for any PostgREST/network failure.

    ``.message`` is pre-formatted, tool-friendly text (always starts with
    "Error: ") safe to return directly as a tool's string output.
    """

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


@dataclass(frozen=True)
class PostgrestConfig:
    """Connection settings, read from the environment at call time.

    Reading lazily (rather than at import time) keeps the module importable
    without env vars set, and lets tests set env vars per-test.
    """

    base_url: str
    anon_key: str

    @classmethod
    def from_env(cls) -> PostgrestConfig:
        base_url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
        anon_key = os.environ.get("SUPABASE_ANON_KEY", "").strip()
        if not base_url:
            raise PostgrestError(
                "Error: SUPABASE_URL is not set. Copy mcp/.env.example to .env (or set it in "
                "your MCP client's server config) and provide your Supabase project URL."
            )
        if not anon_key:
            raise PostgrestError(
                "Error: SUPABASE_ANON_KEY is not set. Copy mcp/.env.example to .env (or set it "
                "in your MCP client's server config) and provide your Supabase anon key."
            )
        return cls(base_url=base_url, anon_key=anon_key)


# --- PostgREST operator param builders ---------------------------------
#
# These build the right-hand side of a PostgREST filter, e.g.
# params={"season": eq(2024)} -> "?season=eq.2024"


def eq(value: Any) -> str:
    return f"eq.{value}"


def gte(value: Any) -> str:
    return f"gte.{value}"


def lte(value: Any) -> str:
    return f"lte.{value}"


def in_(values: Iterable[Any]) -> str:
    return "in.(" + ",".join(str(v) for v in values) + ")"


class PostgrestClient:
    """Async client for reading api.* views and calling public RPCs."""

    def __init__(self, config: PostgrestConfig | None = None):
        self._config = config or PostgrestConfig.from_env()

    def _headers(self, profile: str, *, write: bool) -> dict[str, str]:
        profile_header = "Content-Profile" if write else "Accept-Profile"
        return {
            "apikey": self._config.anon_key,
            "Authorization": f"Bearer {self._config.anon_key}",
            profile_header: profile,
            "Accept": "application/json",
        }

    async def select(
        self,
        view: str,
        params: dict[str, Any] | None = None,
        *,
        profile: str = "api",
        limit: int = DEFAULT_ROW_CAP,
    ) -> list[dict[str, Any]]:
        """GET rows from a PostgREST view (Accept-Profile: <profile>).

        The row cap is enforced here, not left to the caller: ``limit`` is
        clamped to ``DEFAULT_ROW_CAP`` even if a larger value is passed in.
        """
        capped_limit = min(limit, DEFAULT_ROW_CAP) if limit else DEFAULT_ROW_CAP
        query: dict[str, Any] = dict(params or {})
        query["limit"] = str(capped_limit)

        url = f"{self._config.base_url}/rest/v1/{view}"
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            try:
                response = await client.get(
                    url, params=query, headers=self._headers(profile, write=False)
                )
            except httpx.TimeoutException as e:
                raise PostgrestError(f"Error: request to '{view}' timed out.") from e
            except httpx.RequestError as e:
                raise PostgrestError(f"Error: network failure calling '{view}': {e}") from e

        result = _parse_response(response, view)
        return result if isinstance(result, list) else [result]

    async def rpc(
        self,
        function: str,
        args: dict[str, Any] | None = None,
        *,
        profile: str = "public",
    ) -> list[dict[str, Any]]:
        """POST to a PostgREST RPC endpoint (Content-Profile: <profile>)."""
        url = f"{self._config.base_url}/rest/v1/rpc/{function}"
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            try:
                response = await client.post(
                    url, json=args or {}, headers=self._headers(profile, write=True)
                )
            except httpx.TimeoutException as e:
                raise PostgrestError(f"Error: request to rpc/{function} timed out.") from e
            except httpx.RequestError as e:
                raise PostgrestError(f"Error: network failure calling rpc/{function}: {e}") from e

        result = _parse_response(response, f"rpc/{function}")
        if result is None:
            return []
        return result if isinstance(result, list) else [result]


def _parse_response(response: httpx.Response, source: str) -> Any:
    if response.status_code >= 400:
        raise PostgrestError(_format_error(response, source))
    if not response.content:
        return []
    return response.json()


def _format_error(response: httpx.Response, source: str) -> str:
    status = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = {}
    detail = body.get("message") or body.get("hint") or response.text[:200] or "no details"

    if status == 404:
        return (
            f"Error: '{source}' not found (404). Check the view/function name against "
            "docs/SCHEMA_CONTRACT.md -- it may not be exposed via PostgREST."
        )
    if status in (401, 403):
        return (
            f"Error: permission denied calling '{source}' ({status}). The anon key may be "
            "missing/invalid, or the object isn't granted to the anon role."
        )
    if status == 429:
        return f"Error: rate limited calling '{source}' (429). Wait a moment and retry."
    if status >= 500:
        return f"Error: '{source}' failed with a server error ({status}): {detail}"
    return f"Error: '{source}' request failed ({status}): {detail}"
