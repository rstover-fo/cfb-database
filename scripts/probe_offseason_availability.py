#!/usr/bin/env python3
"""Probe whether CFBD has published a season's offseason inputs yet.

Plan: docs/plans/2026-07-25-preseason-outlook-model-plan.md section 1.4.

Six sources feed the preseason-known features in `features.team_week`
(design doc section 1f) and the Phase 2 enrichment columns, and all six lag
behind the schedule: as of 2026-07-25 the 2026 schedule (1,638 games), portal
(4,410 rows), draft (257) and recruits (3,107) were all loaded while
`stats.player_returning`, `ratings.sp_ratings`, `recruiting.team_talent`,
`recruiting.team_recruiting`, `core.roster` and `ref.coaches__seasons` had no
2026 rows at all.

Some of that was the calendar bug (`get_current_season()` returning year-1
until August, now fixed by `get_projection_seasons()`), but some of it is
simply that CFBD has not published the data yet -- preseason SP+ lands in
spring, rosters firm up in August. Those two causes look identical from the
warehouse side, and the difference decides whether a loader is broken or
merely early.

This script answers that: for each endpoint it asks CFBD directly whether
`year=<season>` returns rows, and reports. It is read-only -- it never writes
to the warehouse.

**A source that is not yet published is NOT a failure.** The loaders must log
a skip and retry the next day rather than hard-failing the daily workflow;
this probe exists so that decision is made against evidence instead of a
guess. Exit status is 0 whenever every endpoint could be reached, regardless
of how many are still unpublished; it is 1 only when a request itself failed
(auth, network, unexpected shape) -- i.e. when we could not determine
availability.

Usage:
    python scripts/probe_offseason_availability.py                # current projection season
    python scripts/probe_offseason_availability.py --season 2026

Prints one machine-readable line per endpoint:
    AVAILABILITY_PROBE season={s} endpoint={e} rows={n} status={available|unpublished}
followed by a summary line:
    AVAILABILITY_SUMMARY season={s} available={a} unpublished={u} errors={e}

Requires a CFBD API key (`.dlt/secrets.toml` `[sources.cfbd] api_key`, or the
`CFBD_API_KEY` environment variable) -- the same credential the pipelines use.
"""

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# (label, endpoint path, target table) for every source that feeds a
# preseason-known feature and lags the schedule. `year` is the query param for
# all six -- verified against src/pipelines/config/endpoints.py and the
# per-endpoint source modules.
OFFSEASON_ENDPOINTS: list[tuple[str, str, str]] = [
    ("player_returning", "/player/returning", "stats.player_returning"),
    ("sp_ratings", "/ratings/sp", "ratings.sp_ratings"),
    ("team_talent", "/talent", "recruiting.team_talent"),
    ("team_recruiting", "/recruiting/teams", "recruiting.team_recruiting"),
    ("roster", "/roster", "core.roster"),
    ("coaches", "/coaches", "ref.coaches__seasons"),
]

AVAILABLE = "available"
UNPUBLISHED = "unpublished"
ERROR = "error"


def classify(rows: list | None) -> str:
    """Grade one probe response.

    An empty list is a *successful* request that returned nothing -- CFBD has
    not published this endpoint for the season yet. `None` means the request
    itself failed, which is the only condition that should trouble a caller.
    Pure, so the three-way outcome is testable without network access.
    """
    if rows is None:
        return ERROR
    return AVAILABLE if len(rows) > 0 else UNPUBLISHED


def probe_endpoint(client, endpoint: str, season: int) -> list | None:
    """One `year=<season>` request. Returns the rows, or None if the request
    failed (logged, never raised -- one dead endpoint must not abort the
    sweep)."""
    from src.pipelines.sources.base import make_request

    try:
        rows = make_request(client, endpoint, params={"year": season})
    except Exception as e:
        logger.error("%s: request failed: %s", endpoint, e)
        return None

    if not isinstance(rows, list):
        logger.error("%s: expected a list response, got %s", endpoint, type(rows).__name__)
        return None
    return rows


def run_probe(client, season: int) -> dict[str, int]:
    """Probe every offseason endpoint for `season`; print a line each. Returns
    the {status: count} tally backing the summary line."""
    tally = {AVAILABLE: 0, UNPUBLISHED: 0, ERROR: 0}

    for label, endpoint, table in OFFSEASON_ENDPOINTS:
        rows = probe_endpoint(client, endpoint, season)
        status = classify(rows)
        tally[status] += 1
        n = len(rows) if rows is not None else 0
        print(f"AVAILABILITY_PROBE season={season} endpoint={label} rows={n} status={status}")
        if status == AVAILABLE:
            logger.info("%s -> %s: %d row(s) available for %d", endpoint, table, n, season)
        elif status == UNPUBLISHED:
            logger.info(
                "%s -> %s: not published for %d yet (loader should skip and retry)",
                endpoint,
                table,
                season,
            )

    print(
        f"AVAILABILITY_SUMMARY season={season} available={tally[AVAILABLE]} "
        f"unpublished={tally[UNPUBLISHED]} errors={tally[ERROR]}"
    )
    return tally


def resolve_season() -> int:
    """Default season to probe: the latest projection season -- the upcoming
    one whose inputs we are waiting on."""
    import psycopg2

    from scripts.build_features import get_db_url
    from src.pipelines.config.years import get_projection_seasons

    conn = psycopg2.connect(get_db_url())
    try:
        seasons = get_projection_seasons(conn)
    finally:
        conn.close()

    if not seasons:
        raise RuntimeError("core.games has no rows; pass --season explicitly")
    return seasons[-1]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Probe whether CFBD has published a season's offseason inputs"
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Season to probe (default: the latest projection season)",
    )
    args = parser.parse_args()

    season = args.season if args.season is not None else resolve_season()
    logger.info("Probing CFBD offseason input availability for season %d", season)

    from src.pipelines.utils.api_client import get_client

    client = get_client()
    try:
        tally = run_probe(client, season)
    finally:
        client.close()

    # Unpublished is an expected, reportable state -- only an unreachable
    # endpoint means we failed to determine availability.
    sys.exit(1 if tally[ERROR] else 0)


if __name__ == "__main__":
    main()
