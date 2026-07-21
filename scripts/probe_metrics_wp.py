#!/usr/bin/env python3
"""Probe /metrics/wp for a handful of games to confirm its current field shape.

P3.2 Lane B (docs/pipeline-manifest.md row 47) rewrites the dead
``win_probability_resource`` in src/pipelines/sources/metrics.py to call
``/metrics/wp?gameId=<id>`` per game instead of the year-only query that
always 400'd. Two things that resource, src/schemas/api/033_game_win_probability.sql,
and src/schemas/migrations/026_win_probability_indexes.sql all assume, but
which nobody has confirmed against a live response since CFBD is reported to
have rebuilt its win-probability model in 2025:

  1. Field names -- specifically whether the response still uses playId,
     playText, homeWinProbability, down, distance, yardLine as recorded in
     this repo's 2026-01-29 investigation note, or whether the rebuilt model
     renamed/added/dropped fields (down/distance/clock/homeBall/spread are
     of particular interest -- 033's view leans on down/distance existing
     and defensively LEFT JOINs core.plays for period/clock rather than
     assuming /metrics/wp provides them).
  2. playId's type and scope -- whether it's an integer/string, and whether
     it lines up with core.plays.id (varchar) well enough to join on, or is
     an independent numbering CFBD only uses within this endpoint. 033's
     join is written defensively (CAST + LEFT JOIN, never assumed to match)
     specifically because this is unconfirmed.

This script makes NO writes -- it only calls the API (through the existing
rate-limited CFBDClient/make_request path, so calls are recorded against the
monthly budget like any other pipeline call) and prints what it got, for a
human (or the deploying orchestrator) to eyeball before trusting the SQL.

Registered as an allowlisted `compute` action script in scripts/deploy_schema.py
(COMPUTE_SCRIPTS) so it can run in CI/deploy context without a temporary
one-off workflow -- see that file's comment for removal timing.

Usage:
    python scripts/probe_metrics_wp.py
    python scripts/probe_metrics_wp.py --game-ids 401628455 400756842
"""

from __future__ import annotations

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Default probe games: one on either side of CFBD's reported 2025 WP model
# rebuild, so a field-shape diff between the two rows is visible in one run.
#
#   401628455 -- 2024 season. CONFIRMED: this repo's standard example game
#     (tests/test_api_views.py EXAMPLE_GAME_ID, api.game_player_leaders /
#     api.game_drives / api.game_plays PostgREST examples throughout the
#     docs). Reflects the current (post-rebuild) WP model.
#
#   400756843 -- 2015 season (2016-01-11 CFP National Championship,
#     Alabama-Clemson). UNVERIFIED: no 2015-era game id exists anywhere else
#     in this repo to cross-check against, and this sandbox cannot reach the
#     CFBD API or core.games to confirm it. Whoever runs this probe with API
#     access should treat a 400/404 on this id as "wrong id", not "no WP
#     data for 2015" -- swap in a confirmed pre-rebuild game id via
#     --game-ids if it fails.
DEFAULT_GAME_IDS = [401628455, 400756843]


def probe_game(client, game_id: int) -> dict | None:
    """Call /metrics/wp for one game_id and return the raw record list, or
    None if the endpoint 400/404'd (matching win_probability_resource's own
    per-game skip behavior)."""
    import httpx

    from src.pipelines.sources.base import make_request

    try:
        return make_request(client, "/metrics/wp", params={"gameId": game_id})
    except httpx.HTTPStatusError as e:
        if e.response.status_code in (400, 404):
            logger.warning(f"game {game_id}: {e.response.status_code} response, no WP data")
            return None
        raise


def summarize(game_id: int, records: list[dict] | None) -> None:
    print(f"\n===== gameId={game_id} =====")
    if records is None:
        print("  NO DATA (400/404)")
        return

    print(f"  record count: {len(records)}")
    if not records:
        print("  (empty list -- endpoint returned 200 with zero records)")
        return

    first = records[0]
    print(f"  full field list of first record ({len(first)} fields):")
    for key, value in first.items():
        print(f"    {key}: {value!r}  (python type: {type(value).__name__})")

    play_ids = [r.get("playId") for r in records[:5]]
    print(f"  playId sample values (first 5): {play_ids}")
    play_id_types = {type(r.get("playId")).__name__ for r in records}
    print(f"  playId inferred type(s) across all records: {sorted(play_id_types)}")

    for field in ("down", "distance", "clock", "homeBall", "spread"):
        present = any(field in r for r in records)
        print(f"  {field} present: {present}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Probe /metrics/wp field shape for known games")
    parser.add_argument(
        "--game-ids",
        type=int,
        nargs="+",
        default=DEFAULT_GAME_IDS,
        help=f"Game ids to probe (default: {DEFAULT_GAME_IDS})",
    )
    args = parser.parse_args(argv)

    from src.pipelines.utils.api_client import get_client

    client = get_client()
    try:
        for game_id in args.game_ids:
            records = probe_game(client, game_id)
            summarize(game_id, records)
    finally:
        client.close()

    print("\n===== done =====")


if __name__ == "__main__":
    sys.exit(main())
