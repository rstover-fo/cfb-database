"""Rushing charting data source (CFBD spec v5.26.0): rusher attribution,
rush direction, direction/touchdown-status charting coverage.

Five endpoints, mirroring passing.py's structure exactly (own dlt source,
same era-guard/week-walk shape, same PK grain per resource): play-grain,
player-game-grain, team-game-grain, player-season-grain, team-season-grain.

Rushing-specific semantics (from the live spec and 2026-09-03 probe
captures at tests/fixtures/cfbd_2026/rushing_*.json):

- `attributionStatus` (play-grain) is an enum: individual/team/
  multi_carrier/unmatched/ambiguous/conflict/unlinked. A play with
  `attributionStatus="team"` has `rusherId`/`rusher` NULL and
  `isTeamRush=true` -- sacks, kneels, and other team-only or unresolved
  attempts land here, not against an individual rusher.
- `directionAnalysisEligible` (play-grain) marks the ordinary-rush analysis
  population (excludes sacks/kneels/etc.); an eligible attempt can still
  carry a NULL `rushDirection` (charting hasn't resolved direction yet --
  observed live: all 5 probed plays are `directionAnalysisEligible=true`
  with `rushDirection=null`).
- `parseStatus` (play-grain) is an enum: complete/partial/invalid.
  `invalid` is its own bucket, never folded into `partial` -- this source
  does not filter on parseStatus or attributionStatus, all rows pass
  through unchanged (KTD5).
- `rusherId`/`rusher`/`rushDirection`/`rushingYards`/`rusherYards`/
  `isRushingTouchdown`/`ppa`/`success` are nullable on plays.
- Player aggregate rows (player_games, player_season) and team aggregate
  rows (team_games, team_season) BOTH carry `sacks`/`kneels`/`teamRushes`/
  `unattributedAttempts`/`multiCarrierAttempts` counters. Player rows count
  ONLY guarded rusher attribution, so those counters are typically 0 there
  -- individually attributed sacks/kneels are folded into the player's own
  `attempts`/`individualAttempts` instead (observed: a player-game row with
  `sacks=3` and `individualAttempts=10`). Team rows additionally add
  team-only attempts and unresolved attribution via the same counters --
  so player totals do NOT sum to team totals.
- Aggregate rows (player and team grain) carry a nested `directions` object
  keyed unknown/right/middle/left, 15 metrics each (carries, yards,
  yardsPerCarry, successRate, ppa, totalPpa, lineYards, lineYardsTotal,
  secondLevelYards, secondLevelYardsTotal, openFieldYards,
  openFieldYardsTotal, stuffRate, powerSuccess, explosiveness) -- dlt
  flattens this to `directions__<dir>__<metric>` at normalize time. Team
  rows additionally nest the whole aggregate block under `offense`/
  `defense` (flattened to `offense__directions__left__carries` etc.) and
  carry `rushingTouchdowns`/`touchdownStatusAvailable`, which the
  player-grain rows do not.
- Coverage denominators: `rushingYardsAvailable`, `directionEligibleAttempts`,
  `directionAvailableAttempts`, `touchdownStatusAvailable` (team-grain
  only). Every leaderboard or rate built on this data should carry or
  filter on these the same way passing.py's `*AttemptsAvailable` columns
  are treated -- coverage denominators, not optional metadata.
- Coverage: partial in 2025 (consistent with passing's charting rollout),
  expected mostly full from 2026 per CFBD's 2026-09-02 announcement.

RUSHING_DATA_START = 2025, mirroring PASSING_DATA_START -- rushing charting
is the same vendor-charted product family as passing, released on the same
timeline.

Runtime validation (mirrors passing.py's documented 400-on-bare-year trap):
`/rushing/plays`, `/rushing/players/games`, and `/rushing/teams/games` are
expected to 400 on a bare `year` the same way their /passing counterparts
do (team or week required); this source walks weeks here exactly like
passing.py's game-grain resources: regular season weeks 1-16, postseason
weeks 1-4 (~20 calls/season each). `/rushing/players/season` and
`/rushing/teams/season` are expected to take a bare year like their
/passing counterparts -- one call per season each. A 400 on any call
(week-scoped or bare-year) is logged and skipped, never raised; any other
HTTP error propagates.

Total cost: 3 week-iterated resources x ~20 calls + 2 season-grain
resources x 1 call = ~62 calls/season -- same shape and magnitude as
passing.py.

Immutable-once-final caveat: like passing's `parseStatus="partial"`,
rushing's charted rows may be re-charted by CFBD's upstream vendor after
the fact, so a finished season's rushing data is only immutable-ish.
Corrections ride explicit `--season` re-runs, not a daily re-ingest -- see
scripts/load_season.py's IMMUTABLE_ONCE_FINAL comment for "rushing".

Deliberately a separate module, NOT folded into stats.py or passing.py:
same blast-radius reasoning as passing.py's module docstring -- one
resource's failure inside a dlt source discards every sibling resource's
already-fetched extract package, so rushing gets its own source name
(`cfbd_rushing`) and its own extract package.
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

# See module docstring: rushing charting is the same vendor-charted product
# family as passing, released on the same timeline.
RUSHING_DATA_START = 2025


def _iter_season_weeks() -> Iterator[tuple[str, int]]:
    """(season_type, week) pairs for the three game-grain /rushing endpoints.

    Mirrors passing.py's _iter_season_weeks exactly: regular season weeks
    1-16, postseason weeks 1-4.
    """
    for season_type in ("regular", "postseason"):
        max_week = 16 if season_type == "regular" else 4
        for week in range(1, max_week + 1):
            yield season_type, week


@dlt.source(name="cfbd_rushing")
def rushing_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for CFBD's five /rushing charting endpoints.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads
            YEAR_RANGES["stats"] (2004-2026) -- years before
            RUSHING_DATA_START are skipped per-resource with zero calls via
            the era guard, the same pattern passing_source uses for its
            own PASSING_DATA_START.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["stats"].to_list()

    return [
        rushing_plays_resource(years),
        rushing_player_games_resource(years),
        rushing_team_games_resource(years),
        rushing_player_season_resource(years),
        rushing_team_season_resource(years),
    ]


@dlt.resource(
    name="rushing_plays",
    write_disposition="merge",
    primary_key=["game_id", "play_id"],
)
def rushing_plays_resource(years: list[int]) -> Iterator[dict]:
    """Load play-grain rush charting: rusher attribution, rush direction,
    rushing yards -- one row per rushing play.

    `clock` is a nested dict (minutes, seconds) that dlt flattens to
    `clock__minutes`/`clock__seconds` -- no manual handling needed. The API
    already stamps `season`/`week`/`seasonType` on every row; `setdefault`
    below only guards the (unobserved) case where a row omits one, mirroring
    passing_plays_resource's defensive stamping.

    `rusherId`/`rusher`/`rushDirection` are nullable -- a `attributionStatus
    ="team"` row has both `rusherId`/`rusher` NULL and `isTeamRush=true`.
    This source does not filter on `attributionStatus` or `parseStatus`;
    every row, however attributed or charted, is yielded.

    Args:
        years: List of years to load rushing plays for
    """
    client = get_client()
    try:
        for year in years:
            if year < RUSHING_DATA_START:
                logger.info(f"Skipping rushing plays for {year} (data starts {RUSHING_DATA_START})")
                continue

            for season_type, week in _iter_season_weeks():
                logger.info(f"Loading rushing plays for {year} {season_type} week {week}...")
                try:
                    data = make_request(
                        client,
                        "/rushing/plays",
                        params={"year": year, "seasonType": season_type, "week": week},
                    )
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 400:
                        logger.warning(
                            f"No rushing plays for {year} {season_type} week {week} "
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
    name="rushing_player_games",
    write_disposition="merge",
    primary_key=["game_id", "player_id"],
)
def rushing_player_games_resource(years: list[int]) -> Iterator[dict]:
    """Load player-game rushing aggregates: attempts, yards, direction
    breakdown -- one row per player per game.

    Player totals count only guarded rusher attribution and do NOT sum to
    team totals (see module docstring) -- the `sacks`/`kneels`/`teamRushes`/
    `unattributedAttempts` counters live on this resource too (typically 0
    here, since team-only and unresolved attempts land on rushing_team_games
    instead), and individually attributed sacks/kneels are folded into this
    resource's own `attempts` count. The API already stamps
    `season`/`week`/`seasonType` on every row; `setdefault` below only
    guards the (unobserved) case where a row omits one.

    Args:
        years: List of years to load player-game rushing stats for
    """
    client = get_client()
    try:
        for year in years:
            if year < RUSHING_DATA_START:
                logger.info(
                    f"Skipping rushing player games for {year} (data starts {RUSHING_DATA_START})"
                )
                continue

            for season_type, week in _iter_season_weeks():
                logger.info(f"Loading rushing player games for {year} {season_type} week {week}...")
                try:
                    data = make_request(
                        client,
                        "/rushing/players/games",
                        params={"year": year, "seasonType": season_type, "week": week},
                    )
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 400:
                        logger.warning(
                            f"No rushing player games for {year} {season_type} week {week} "
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
    name="rushing_team_games",
    write_disposition="merge",
    primary_key=["game_id", "team"],
)
def rushing_team_games_resource(years: list[int]) -> Iterator[dict]:
    """Load team-game rushing aggregates, offense and defense sides.

    `offense`/`defense` are nested dicts (attempts, yards, direction
    breakdown, coverage denominators) that dlt flattens to `offense__*`/
    `defense__*` -- no child table, no manual handling needed. Team rows,
    unlike player rows, carry `rushingTouchdowns` and
    `touchdownStatusAvailable` inside each side. The API already stamps
    `season`/`week`/`seasonType` on every row; `setdefault` below only
    guards the (unobserved) case where a row omits one.

    Args:
        years: List of years to load team-game rushing stats for
    """
    client = get_client()
    try:
        for year in years:
            if year < RUSHING_DATA_START:
                logger.info(
                    f"Skipping rushing team games for {year} (data starts {RUSHING_DATA_START})"
                )
                continue

            for season_type, week in _iter_season_weeks():
                logger.info(f"Loading rushing team games for {year} {season_type} week {week}...")
                try:
                    data = make_request(
                        client,
                        "/rushing/teams/games",
                        params={"year": year, "seasonType": season_type, "week": week},
                    )
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 400:
                        logger.warning(
                            f"No rushing team games for {year} {season_type} week {week} "
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
    name="rushing_player_season",
    write_disposition="merge",
    primary_key=["season", "player_id", "team"],
)
def rushing_player_season_resource(years: list[int]) -> Iterator[dict]:
    """Load player season rushing aggregates, one call per year (a bare
    year is expected to succeed here, mirroring passing_player_season_resource).

    `team` is deliberately part of the PK alongside season/player_id -- same
    transfer-safety reasoning as passing.py's passing_player_season_resource
    (a transferred player can appear under more than one team in a season).

    The API already stamps `season` on every row; `setdefault` below only
    guards the (unobserved) case where a row omits it.

    Args:
        years: List of years to load player season rushing stats for
    """
    client = get_client()
    try:
        for year in years:
            if year < RUSHING_DATA_START:
                logger.info(
                    f"Skipping rushing player season for {year} (data starts {RUSHING_DATA_START})"
                )
                continue

            logger.info(f"Loading rushing player season for {year}...")
            try:
                data = make_request(client, "/rushing/players/season", params={"year": year})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(
                        f"No rushing player season data for {year} (400 response), skipping"
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
    name="rushing_team_season",
    write_disposition="merge",
    primary_key=["season", "team"],
)
def rushing_team_season_resource(years: list[int]) -> Iterator[dict]:
    """Load team season rushing aggregates, offense and defense sides, one
    call per year (bare year is expected to succeed).

    `offense`/`defense` are nested dicts (attempts, yards, direction
    breakdown, coverage denominators, `rushingTouchdowns`/
    `touchdownStatusAvailable`) that dlt flattens to `offense__*`/
    `defense__*` -- no child table, no manual handling needed. The API
    already stamps `season` on every row; `setdefault` below only guards
    the (unobserved) case where a row omits it.

    Args:
        years: List of years to load team season rushing stats for
    """
    client = get_client()
    try:
        for year in years:
            if year < RUSHING_DATA_START:
                logger.info(
                    f"Skipping rushing team season for {year} (data starts {RUSHING_DATA_START})"
                )
                continue

            logger.info(f"Loading rushing team season for {year}...")
            try:
                data = make_request(client, "/rushing/teams/season", params={"year": year})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(
                        f"No rushing team season data for {year} (400 response), skipping"
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
