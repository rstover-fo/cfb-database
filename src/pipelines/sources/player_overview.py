"""Player season overview -- box score, usage, and PPA in one per-player call.

`/player/season/overview` requires BOTH `year` and `playerId` -- there is no
year-only or player-only bulk query -- so, like `/coaches/profile` and
`/coaches/tenures` (coaches.py), this is a per-entity fan-out with a large,
slowly-mutating candidate set rather than a year-driven bulk resource.

Kept in its own module (not folded into stats.py, which already owns
`player_usage`, or metrics.py, which owns `ppa_players_season`) because the
drainer that feeds it (`run.py::run_player_overview_pipeline`) draws its
candidate player-seasons FROM both of those tables via a DB set-difference --
putting the resource in either module would make one source module depend on
another source module's table/column names for no benefit. This mirrors how
`coach_tenures`/`coach_profiles` (coaches.py) and `win_probability`
(metrics.py's `metrics_wp_source`) each got their own source function instead
of being folded into a year-driven default: the whole point is that nothing
in `scripts/load_season.py`'s year-fetch-all path should ever pay for this
fan-out by accident.

See `run.py::run_player_overview_pipeline` for the completed-season gate
(a season's totals mutate weekly while it is in progress) and the candidate
resolution (UNION of stats.player_usage and metrics.ppa_players_season,
MINUS stats.player_season_overview).
"""

import logging
from collections.abc import Iterator

import dlt
import httpx
from dlt.sources import DltSource

from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


@dlt.source(name="cfbd_player_overview")
def player_overview_source(
    player_seasons: list[tuple[int, str]],
    *,
    misses: list[tuple[str, int]] | None = None,
) -> DltSource:
    """Source for per-player, per-season box score/usage/PPA overviews.

    Args:
        player_seasons: List of (year, player_id) pairs to fetch.
        misses: When given, a list the resource appends
            (f"{year}:{player_id}", status_code) to for every 400/404 hit
            and every 5xx that survives api_client's retries -- forwarded
            straight through to `player_season_overview_resource`.
            The caller persists it to `meta.fanout_misses` via
            `run.py::_record_fanout_misses` (PR #75 review finding A) so
            the next run's candidate query can exclude a terminal miss
            instead of re-spending the call forever.
    """
    if not player_seasons:
        raise ValueError(
            "player_seasons parameter is required. Provide a list of "
            "(year, player_id) tuples, e.g., player_seasons=[(2024, '5083552')]"
        )

    return [
        player_season_overview_resource(player_seasons, misses=misses),
    ]


@dlt.resource(
    name="player_season_overview",
    write_disposition="merge",
    # team added 2026-08-30 pre-backfill for transfer safety and grain
    # consistency with player_success_season/passing_player_season
    # (cfb-app work-order task 1). CFBD normally returns one overview
    # record per player-season with a single top-level team attribution --
    # this is insurance against a per-team split (e.g. a mid-season
    # transfer reported as two rows for the same player-season), not an
    # observed one: merging on (season, id) alone would silently drop a
    # stint under last-write-wins if CFBD ever does split. The candidate
    # set-difference in run.py (run_player_overview_pipeline) still keys on
    # (season, id) only, which stays correct -- it drains a candidate once
    # per season regardless of team.
    primary_key=["season", "id", "team"],
)
def player_season_overview_resource(
    player_seasons: list[tuple[int, str]],
    *,
    misses: list[tuple[str, int]] | None = None,
) -> Iterator[dict]:
    """Load player season overviews, one call per (year, playerId).

    The live `PlayerSeasonOverview` response (per the CFBD OpenAPI spec) is
    a single object with `season`, `id` (string), `name`, `position`,
    `team`, `conference`, `games` all top-level -- the player-grain join
    spine alongside `boxScoreStats`/`usage`/`ppa`. `boxScoreStats.categories`
    is a nested array of `{name, stats: [{name, value}]}` and will
    child-table (`player_season_overview__box_score_stats__categories`,
    `...__stats`); `usage` and `ppa` are nested dicts with no arrays and
    flatten into `usage__overall`, `ppa__average__all`, `ppa__total__all`,
    etc.

    Coded defensively against the response arriving wrapped in a
    single-item list (some CFBD endpoints do this inconsistently) even
    though the OpenAPI spec declares a bare object.

    A 400 or 404 for a given (year, playerId) is logged and skipped rather
    than aborting the whole batch -- candidates come from a UNION of
    stats.player_usage and metrics.ppa_players_season, either of which may
    include a player-season CFBD has no overview computed for. A 5xx that
    exhausts api_client's per-request retries is skipped the same way: one
    player's transient gateway failure must not kill a multi-thousand-call
    dispatch (backfill run 33351198599, 2026-08-30). When `misses` is
    given, the "{year}:{player_id}" key (matching
    `meta.fanout_misses.key`'s format for this source) and status code are
    appended to it so the caller can persist the miss (PR #75 review
    finding A: without this, a terminal 400/404 was re-requested every run
    forever); the ledger's FANOUT_MISS_RETRY_DAYS window ages a 5xx skip
    back into eligibility rather than blacklisting it.

    Args:
        player_seasons: List of (year, player_id) pairs to fetch.
        misses: Keyword-only. Optional collector list; see
            `player_overview_source`'s docstring. `None` (the default) is
            safe -- no misses are collected.
    """
    client = get_client()
    try:
        for year, player_id in player_seasons:
            logger.info(f"Loading player season overview for {player_id} ({year})...")

            try:
                data = make_request(
                    client,
                    "/player/season/overview",
                    params={"year": year, "playerId": player_id},
                )
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (400, 404):
                    logger.warning(
                        f"No player season overview for {player_id} ({year}) "
                        f"({e.response.status_code} response), skipping"
                    )
                    if misses is not None:
                        misses.append((f"{year}:{player_id}", e.response.status_code))
                    continue
                if e.response.status_code >= 500:
                    # Already retried: a 5xx HTTPStatusError only reaches here
                    # after api_client.get() has burned its full retry budget
                    # (MAX_RETRIES backoff attempts) on this one call. One
                    # player's flaky gateway must not kill the whole dispatch
                    # -- backfill run 33351198599 (2026-08-30) lost ~5,600
                    # planned calls when a single transient 502 for
                    # (2024, 4879928) raised through dlt at batch 68/180.
                    # Recorded in the same meta.fanout_misses ledger as a
                    # 400/404, so FANOUT_MISS_RETRY_DAYS ages it back into
                    # eligibility -- a skipped player is deferred, never
                    # blacklisted. Rate-limit failures never take this path:
                    # 429s surface as RateLimitExhausted/RateLimitCircuitOpen
                    # (not HTTPStatusError) and still abort the run.
                    logger.warning(
                        f"Server error {e.response.status_code} for player season "
                        f"overview {player_id} ({year}) after client retries; "
                        f"recording miss and continuing"
                    )
                    if misses is not None:
                        misses.append((f"{year}:{player_id}", e.response.status_code))
                    continue
                raise

            if isinstance(data, list):
                rows = data
            elif data:
                rows = [data]
            else:
                rows = []

            for row in rows:
                if row.get("season") is None or row.get("id") is None:
                    logger.warning(
                        f"Player season overview row for {player_id} ({year}) "
                        "missing season/id, skipping"
                    )
                    continue
                yield row

    finally:
        client.close()
