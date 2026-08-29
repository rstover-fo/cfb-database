"""Coaching data sources - season-by-season coaching records and tenures.

Distinct from `ref.coaches` (reference.py's `/coaches`, first_name/last_name
bio rows) -- these two load the richer coach-season and coach-tenure
records from `/coaches/seasons` and `/coaches/tenures`.

`coach_seasons` is year-driven like every other resource in this module, so
it lives in `cfbd_coaches` and is cheap enough (one call per year) to run
daily. `coach_tenures` requires a `coachId` or `team` parameter -- a bare
`year` 400s ("coachId, team, or year is required") -- so it is a per-team
fan-out, like `/roster` (rosters.py). Mirroring how metrics.py splits
`win_probability` (game-id fan-out) into its own `metrics_wp_source` rather
than returning it from `metrics_source`, `coach_tenures` gets its own
`cfbd_coach_tenures` source function instead of being returned from
`cfbd_coaches` -- so the daily/incremental path never pays for it, and a
caller that wants it opts in explicitly (see
`run.py::run_coach_tenures_pipeline`, backfill/preseason only).
"""

import logging
from collections.abc import Iterator

import dlt
import httpx
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


@dlt.source(name="cfbd_coaches")
def coaches_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for year-driven coach-season records.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["games_modern"].to_list()

    return [
        coach_seasons_resource(years),
    ]


@dlt.resource(
    name="coach_seasons",
    write_disposition="merge",
    primary_key=["coach__id", "year", "team__id"],
)
def coach_seasons_resource(years: list[int]) -> Iterator[dict]:
    """Load per-season coaching records, one call per year via `?year=Y`.

    A parameterless call 400s ("coachId, team, or year is required"), which
    is why this is year-driven rather than a single bulk call.

    The live response (`DetailedCoachSeason` per the CFBD OpenAPI spec) is
    nested: `coach` ({id, firstName, lastName}), `team` ({id, school,
    conference}), plus several richer nested contexts (teamMetrics,
    recruiting, pollResume, recordSplits, scoring, cfp,
    draftFollowingSeason). dlt flattens nested dicts into `coach__id`,
    `team__id`, etc.; any nested list within those sub-objects would
    child-table normally. `year` is a required top-level field per the
    schema, so it is not stamped here.

    Coded defensively because this endpoint 400'd on every probe call made
    while building this resource (no year alone in the sandbox) -- the
    field names above come from the OpenAPI spec, not an inspected live
    response. A record missing a PK field is logged and skipped rather than
    raising, so a shape surprise degrades to a gap instead of failing the
    whole load.

    Args:
        years: List of years to load coach seasons for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading coach seasons for {year}...")

            try:
                data = make_request(client, "/coaches/seasons", params={"year": year})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No coach seasons for {year} (400 response), skipping")
                    continue
                raise

            for row in data:
                coach = row.get("coach") or {}
                team = row.get("team") or {}
                if coach.get("id") is None or team.get("id") is None or row.get("year") is None:
                    logger.warning(
                        f"Coach season row for {year} missing a PK field "
                        f"(coach.id={coach.get('id')}, team.id={team.get('id')}, "
                        f"year={row.get('year')}), skipping"
                    )
                    continue
                yield row

    finally:
        client.close()


@dlt.source(name="cfbd_coach_tenures")
def coach_tenures_source(teams: list[str]) -> DltSource:
    """Source for per-team coach tenure history.

    `/coaches/tenures` requires a `coachId` or `team` parameter (a bare
    `year` 400s), so this is a per-team fan-out -- deliberately NOT part of
    `cfbd_coaches` so the daily/incremental path never pays for it. See
    `run.py::run_coach_tenures_pipeline` for the backfill/preseason-only
    caller, which resolves `teams` from the schedule the same way
    `rosters_source` does.

    Args:
        teams: List of team names to load tenure history for.
    """
    if not teams:
        raise ValueError(
            "teams parameter is required. Provide a list of team names, "
            "e.g., teams=['Alabama', 'Georgia', 'Ohio State']"
        )

    return [
        coach_tenures_resource(teams),
    ]


@dlt.resource(
    name="coach_tenures",
    write_disposition="merge",
    primary_key=["id"],
)
def coach_tenures_resource(teams: list[str]) -> Iterator[dict]:
    """Load coach tenure history for specified teams, one call per team.

    The live `CoachTenure` response (per the CFBD OpenAPI spec) carries a
    top-level, globally unique `id` per tenure record -- used directly as
    the primary key rather than a synthesized (team, coach, startYear)
    composite, since it is both simpler and more robust to a coach having
    more than one stint at the same school.

    Args:
        teams: List of team names (e.g., ["Alabama", "Georgia"])
    """
    client = get_client()
    try:
        for team in teams:
            logger.info(f"Loading coach tenures for {team}...")

            try:
                data = make_request(client, "/coaches/tenures", params={"team": team})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No coach tenures for {team} (400 response), skipping")
                    continue
                raise

            for row in data:
                if row.get("id") is None:
                    logger.warning(f"Coach tenure row for {team} missing id, skipping: {row}")
                    continue
                yield row

    finally:
        client.close()
