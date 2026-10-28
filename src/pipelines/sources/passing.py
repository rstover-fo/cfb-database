"""Passing charting data sources (CFBD spec v5.25.0): air yards, aDOT, pass
depth/direction/location, yards after catch (YAC).

Five endpoints, all charting-derived (manually reviewed play film, not a
byproduct of play-by-play parsing): play-grain, player-game-grain,
team-game-grain, player-season-grain, team-season-grain. The charting
fields themselves (`airYards`, `passDepth`, `passDirection`, `passLocation`,
`yardsAfterCatch`, `targetYardsToGoal`, and the aggregates `totalAirYards`/
`averageDepthOfTarget`/`totalYardsAfterCatch`/`averageYardsAfterCatch`) are
NULLABLE -- a null means "not (yet) charted", never zero. `parseStatus`
("partial" vs presumably "complete") marks a play whose charting is
incomplete, and the `*AttemptsAvailable` counters on the aggregate
endpoints are the charting-coverage denominators (how many of the plays
behind this aggregate were charted at all).

PASSING_DATA_START = 2025 (probed 2026-08-30, two live runs): 2024 and 2014
both return 200 with zero rows; 2025 is fully populated (820 player-seasons,
136 team-seasons -- roughly FBS); 2026 is accumulating live during games.
Earlier years always come back empty, so the era guard below skips them
with a log line and zero calls -- the same shape as playoffs.py's
CFP_START and ratings.py's CORE_RATINGS_START.

Runtime validation trap (probed 2026-08-30, NOT reflected in CFBD's
OpenAPI required-params list, which understates this): `/passing/plays`,
`/passing/players/games`, and `/passing/teams/games` all 400 on a bare
`year` -- "team or week is required" (plays) / "passerId, team, or week is
required" (players/games, teams/games). All three walk weeks here exactly
like stats.py's player_success_game_resource: regular season weeks 1-16,
postseason weeks 1-4 (~20 calls/season each). No record cap was observed
on `/passing/plays` in probing (a single week returned 3,175 rows in one
response, 2025 week 5), so week granularity is sufficient without a
per-game fan-out.

`/passing/players/season` and `/passing/teams/season` DO take a bare year
(confirmed live: a no-params bulk call 400s, a year-scoped call succeeds)
-- one call per season each.

Total cost: 3 week-iterated resources x ~20 calls + 2 season-grain
resources x 1 call = ~62 calls/season.

Immutable-once-final caveat: `parseStatus="partial"` rows may be re-charted
by CFBD's upstream vendor after the fact, so a finished season's passing
data is only immutable-ish -- the same caveat stats.py documents for its
own charting-adjacent resources. Corrections ride explicit `--season`
re-runs, not a daily re-ingest; see scripts/load_season.py's
IMMUTABLE_ONCE_FINAL comment for "passing".

Deliberately a separate module, NOT folded into stats.py: stats.py's
module docstring documents that one resource's failure inside a dlt source
discards every sibling resource's already-fetched extract package (the
2026-08-04 play_stats/player_returning incident that burst-blocked
`ratings` and `game_stats` behind it). Five more resources sharing that
source would only widen the blast radius -- passing gets its own source
name (`cfbd_passing`) and its own extract package instead.

Since 2026-10-22 the same sibling-failure reasoning applies WITHIN this
module: the two season-grain aggregates ship in passing_season_source
(load unit `passing_season`), separate from the three week-grain resources
in passing_source, after `/passing/players/season` timed out three daily
runs straight (#99-#101) and discarded the week-grain extract each time.
Both sources keep the `cfbd_passing` dlt schema identity -- see
passing_season_source's docstring.
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

# See module docstring: probed 2026-08-30, two live runs. 2024/2014 return
# 200 with zero rows; 2025 is fully populated; 2026 accumulates live.
PASSING_DATA_START = 2025


def _iter_season_weeks() -> Iterator[tuple[str, int]]:
    """(season_type, week) pairs for the three game-grain /passing endpoints.

    Mirrors stats.py's player_success_game_resource exactly: regular season
    weeks 1-16, postseason weeks 1-4 -- the bounds that resource already
    proved correct for a week-required CFBD endpoint.
    """
    for season_type in ("regular", "postseason"):
        max_week = 16 if season_type == "regular" else 4
        for week in range(1, max_week + 1):
            yield season_type, week


@dlt.source(name="cfbd_passing")
def passing_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for CFBD's three week-grain /passing charting endpoints.

    The two season-grain endpoints (`/passing/players/season`,
    `/passing/teams/season`) ship in their own source, passing_season_source
    below, since 2026-10-22. A dlt source failure discards every sibling
    resource's extract, and Daily Season Load runs #99-#101 showed
    `/passing/players/season` timing out after retries (read timeout, then
    504 Gateway Timeout once the read budget was raised past CFBD's own
    gateway) and discarding the ~60 already-fetched week-grain calls with it,
    every day.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads
            YEAR_RANGES["stats"] (2004-2026) -- years before
            PASSING_DATA_START are skipped per-resource with zero calls via
            the era guard, the same pattern playoffs_source and
            ratings_source use for their own endpoint-specific start years.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["stats"].to_list()

    return [
        passing_plays_resource(years),
        passing_player_games_resource(years),
        passing_team_games_resource(years),
    ]


@dlt.source(name="cfbd_passing")
def passing_season_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """The two season-grain /passing aggregates, isolated from the week-grain source.

    Exactly two resources: passing_player_season (`/passing/players/season`)
    and passing_team_season (`/passing/teams/season`), one bare-year call
    each. Split out of passing_source on 2026-10-22 after Daily Season Load
    #99-#101: CFBD computes these aggregates across the whole season at
    request time, and `/passing/players/season` chronically outlasted both
    the 30s and the raised 120s read budget (reaching us as a client read
    timeout or an upstream 504). Because a dlt source failure discards every
    sibling resource's extract, each of those timeouts also threw away the
    ~60 week-grain calls passing_source had already fetched. As its own load
    unit (`passing_season` in scripts/load_season.py, NON_CRITICAL_SOURCES),
    the slow aggregate's failure costs only its own two tables and may be
    tolerated while the week-grain charting merges normally.

    DELIBERATELY name="cfbd_passing", the same dlt schema name as
    passing_source, and the runner (run.py::run_passing_season_pipeline)
    deliberately reuses pipeline_name="cfbd_passing" and dataset_name="stats"
    -- the pattern PR #157 established for the player_success split (Codex
    review on PR #157, P1). The live passing_player_season /
    passing_team_season table layouts are a product of the stored
    cfbd_passing dlt schema's history; a new pipeline/schema identity would
    start a fresh dlt schema whose first load re-infers column types against
    the existing destination tables instead of adopting them. Keeping the
    identity makes dlt restore the stored schema from the destination and
    normalize exactly as before the split. Failure isolation does not come
    from the schema name; it comes from running as a separate pipeline.run()
    call. Accepted coupling (same as the player_success split): a pending
    load package left by a passing run that failed at the LOAD stage is
    retried by whichever cfbd_passing run comes next -- in that scenario the
    run is already red from the passing failure, so the retry can only
    recover data, never hide a failure.

    The resource functions are shared with the pre-split source, so table
    names, columns, merge dispositions, and primary keys are byte-identical:
    no migration, no DB change, existing rows untouched.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads
            YEAR_RANGES["stats"] -- pre-2025 years skip via the era guard.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["stats"].to_list()

    return [
        passing_player_season_resource(years),
        passing_team_season_resource(years),
    ]


@dlt.resource(
    name="passing_plays",
    write_disposition="merge",
    primary_key=["game_id", "play_id"],
)
def passing_plays_resource(years: list[int]) -> Iterator[dict]:
    """Load play-grain pass charting: air yards, pass depth/direction/
    location, YAC -- one row per passing play.

    `clock` is a nested dict (minutes, seconds) that dlt flattens to
    `clock__minutes`/`clock__seconds` -- no manual handling needed. The API
    already stamps `season`/`week`/`seasonType` on every row; `setdefault`
    below only guards the (unobserved) case where a row omits one, mirroring
    cfp_bracket_resource's defensive stamping in playoffs.py.

    Args:
        years: List of years to load passing plays for
    """
    client = get_client()
    try:
        for year in years:
            if year < PASSING_DATA_START:
                logger.info(f"Skipping passing plays for {year} (data starts {PASSING_DATA_START})")
                continue

            for season_type, week in _iter_season_weeks():
                logger.info(f"Loading passing plays for {year} {season_type} week {week}...")
                try:
                    data = make_request(
                        client,
                        "/passing/plays",
                        params={"year": year, "seasonType": season_type, "week": week},
                    )
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 400:
                        logger.warning(
                            f"No passing plays for {year} {season_type} week {week} "
                            "(400 response), skipping"
                        )
                        continue
                    raise

                if not data:
                    continue

                for row in data:
                    row.setdefault("season", year)
                    row.setdefault("week", week)
                    row.setdefault("seasonType", season_type)
                    yield row

    finally:
        client.close()


@dlt.resource(
    name="passing_player_games",
    write_disposition="merge",
    primary_key=["game_id", "player_id"],
)
def passing_player_games_resource(years: list[int]) -> Iterator[dict]:
    """Load player-game passing aggregates: attempts, completions, total air
    yards, aDOT, total/average YAC -- one row per player per game.

    The API already stamps `season`/`week`/`seasonType` on every row;
    `setdefault` below only guards the (unobserved) case where a row omits
    one.

    Args:
        years: List of years to load player-game passing stats for
    """
    client = get_client()
    try:
        for year in years:
            if year < PASSING_DATA_START:
                logger.info(
                    f"Skipping passing player games for {year} (data starts {PASSING_DATA_START})"
                )
                continue

            for season_type, week in _iter_season_weeks():
                logger.info(f"Loading passing player games for {year} {season_type} week {week}...")
                try:
                    data = make_request(
                        client,
                        "/passing/players/games",
                        params={"year": year, "seasonType": season_type, "week": week},
                    )
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 400:
                        logger.warning(
                            f"No passing player games for {year} {season_type} week {week} "
                            "(400 response), skipping"
                        )
                        continue
                    raise

                if not data:
                    continue

                for row in data:
                    row.setdefault("season", year)
                    row.setdefault("week", week)
                    row.setdefault("seasonType", season_type)
                    yield row

    finally:
        client.close()


@dlt.resource(
    name="passing_team_games",
    write_disposition="merge",
    primary_key=["game_id", "team"],
)
def passing_team_games_resource(years: list[int]) -> Iterator[dict]:
    """Load team-game passing aggregates, offense and defense sides.

    `offense`/`defense` are nested dicts (13 metric keys each) that dlt
    flattens to `offense__*`/`defense__*` -- no child table, no manual
    handling needed. The API already stamps `season`/`week`/`seasonType` on
    every row; `setdefault` below only guards the (unobserved) case where a
    row omits one.

    Args:
        years: List of years to load team-game passing stats for
    """
    client = get_client()
    try:
        for year in years:
            if year < PASSING_DATA_START:
                logger.info(
                    f"Skipping passing team games for {year} (data starts {PASSING_DATA_START})"
                )
                continue

            for season_type, week in _iter_season_weeks():
                logger.info(f"Loading passing team games for {year} {season_type} week {week}...")
                try:
                    data = make_request(
                        client,
                        "/passing/teams/games",
                        params={"year": year, "seasonType": season_type, "week": week},
                    )
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 400:
                        logger.warning(
                            f"No passing team games for {year} {season_type} week {week} "
                            "(400 response), skipping"
                        )
                        continue
                    raise

                if not data:
                    continue

                for row in data:
                    row.setdefault("season", year)
                    row.setdefault("week", week)
                    row.setdefault("seasonType", season_type)
                    yield row

    finally:
        client.close()


@dlt.resource(
    name="passing_player_season",
    write_disposition="merge",
    primary_key=["season", "player_id", "team"],
)
def passing_player_season_resource(years: list[int]) -> Iterator[dict]:
    """Load player season passing aggregates, one call per year (a bare year
    succeeds here; a no-params bulk call 400s, confirmed live 2026-08-30).

    `team` is deliberately part of the PK alongside season/player_id -- same
    transfer-safety reasoning as stats.py's player_success_season_resource
    (a transferred player can appear under more than one team in a season).

    The API already stamps `season` on every row; `setdefault` below only
    guards the (unobserved) case where a row omits it.

    Args:
        years: List of years to load player season passing stats for
    """
    client = get_client()
    try:
        for year in years:
            if year < PASSING_DATA_START:
                logger.info(
                    f"Skipping passing player season for {year} (data starts {PASSING_DATA_START})"
                )
                continue

            logger.info(f"Loading passing player season for {year}...")
            try:
                data = make_request(client, "/passing/players/season", params={"year": year})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(
                        f"No passing player season data for {year} (400 response), skipping"
                    )
                    continue
                raise

            if not data:
                continue

            for row in data:
                row.setdefault("season", year)
                yield row

    finally:
        client.close()


@dlt.resource(
    name="passing_team_season",
    write_disposition="merge",
    primary_key=["season", "team"],
)
def passing_team_season_resource(years: list[int]) -> Iterator[dict]:
    """Load team season passing aggregates, offense and defense sides, one
    call per year (bare year succeeds).

    `offense`/`defense` are nested dicts (13 metric keys each) that dlt
    flattens to `offense__*`/`defense__*` -- no child table, no manual
    handling needed. The API already stamps `season` on every row;
    `setdefault` below only guards the (unobserved) case where a row omits
    it.

    Args:
        years: List of years to load team season passing stats for
    """
    client = get_client()
    try:
        for year in years:
            if year < PASSING_DATA_START:
                logger.info(
                    f"Skipping passing team season for {year} (data starts {PASSING_DATA_START})"
                )
                continue

            logger.info(f"Loading passing team season for {year}...")
            try:
                data = make_request(client, "/passing/teams/season", params={"year": year})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(
                        f"No passing team season data for {year} (400 response), skipping"
                    )
                    continue
                raise

            if not data:
                continue

            for row in data:
                row.setdefault("season", year)
                yield row

    finally:
        client.close()
