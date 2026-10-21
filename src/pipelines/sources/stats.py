"""Stats data sources - team and player statistics.

Includes season stats, game stats for teams and players.
"""

import logging
import threading
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, field

import dlt
import httpx
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import RequestBudgetExhausted, make_request

logger = logging.getLogger(__name__)

ADVANCED_BOX_HEALTH_WINDOW = 10
ADVANCED_BOX_HEALTH_5XX_TRIP = 5
ADVANCED_BOX_PROBE_FALLBACK_GAME_ID = 401836219


@dataclass
class AdvancedBoxBatchReport:
    """Per-batch mutable report for the historical-refresh backfill path.

    `outage` is set (and the generator returns early, never raises) when the
    batch cannot safely continue: a probe-confirmed CFBD outage, a transport
    failure after the client's retries, rate-limit exhaustion, or any other
    unrecoverable per-game error. The caller ledgers `attempted` and exits
    nonzero.
    """

    fallback_probe_game_id: int | None = None
    attempted: list[int] = field(default_factory=list)
    server_error_game_ids: list[int] = field(default_factory=list)
    probe_game_id: int | None = None
    outage: str | None = None
    # Set instead of `outage` when the LOCAL request budget
    # (RequestBudgetExhausted from src.pipelines.sources.base.make_request)
    # refuses another call: that is a pacing condition, not a CFBD failure,
    # so the caller stops draining and exits 0 -- same contract as the
    # ledger month guard in scripts/backfill_refresh.py.
    budget_stop: str | None = None


class AdvancedBoxHealth:
    """Sliding-window health tracker for /game/box/advanced backfill calls."""

    def __init__(self, window: int = ADVANCED_BOX_HEALTH_WINDOW) -> None:
        self._lock = threading.Lock()
        self._window: deque[tuple[int, str]] = deque(maxlen=window)
        self.last_success_game_id: int | None = None

    def record_answered(self, game_id: int, *, has_data: bool) -> None:
        with self._lock:
            self._window.append((game_id, "answered"))
            if has_data:
                self.last_success_game_id = game_id

    def record_server_error(self, game_id: int) -> None:
        with self._lock:
            self._window.append((game_id, "server_error"))

    def should_probe(self) -> bool:
        with self._lock:
            if len(self._window) < ADVANCED_BOX_HEALTH_5XX_TRIP:
                return False
            server_errors = sum(1 for _gid, kind in self._window if kind == "server_error")
            answered = sum(1 for _gid, kind in self._window if kind == "answered")
            return server_errors >= ADVANCED_BOX_HEALTH_5XX_TRIP and answered == 0

    def clear_window(self) -> None:
        with self._lock:
            self._window.clear()

    def reset(self) -> None:
        with self._lock:
            self._window.clear()
            self.last_success_game_id = None

    def window_counts(self) -> tuple[int, int]:
        with self._lock:
            server_errors = sum(1 for _gid, kind in self._window if kind == "server_error")
            answered = sum(1 for _gid, kind in self._window if kind == "answered")
            return server_errors, answered


_ADVANCED_BOX_HEALTH = AdvancedBoxHealth()


def reset_advanced_box_health() -> None:
    """Clear the shared advanced-box health window (tests and in-process callers)."""
    _ADVANCED_BOX_HEALTH.reset()


@dlt.source(name="cfbd_stats")
def stats_source(
    years: list[int] | None = None,
    mode: str = "incremental",
    only: list[str] | None = None,
) -> DltSource:
    """Source for team and player statistics.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
        only: Resource names to run (default: all). The source is NOT uniformly
            priced -- play_stats issues one /plays/stats request PER GAME
            (~1,640 for a full season), player_success_game walks ~20 weeks
            (regular 1-16 + postseason 1-4) per year, game_advanced makes 2
            calls per year (regular+postseason), and every other resource
            here is a single call per year. Anything that runs daily should
            name the resources it needs instead of paying for the whole
            source.

    Note: `advanced_game_stats_resource` (the old `/game/box/advanced`
    resource) is intentionally NOT in this source's return list -- CFBD
    dropped the `year` query parameter it depended on, so every year-scoped
    call now 400s. It is retained in this module, game-id-driven, for a
    future historical-refresh campaign; see its docstring.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["stats"].to_list()

    resources = [
        team_season_stats_resource(years),
        player_season_stats_resource(years),
        advanced_team_stats_resource(years),
        player_usage_resource(years),
        player_returning_resource(years),
        play_stats_resource(years),
        game_havoc_resource(years),
        player_success_season_resource(years),
        player_success_game_resource(years),
        game_advanced_resource(years),
    ]

    if only is None:
        return resources

    by_name = {r.name: r for r in resources}
    unknown = [name for name in only if name not in by_name]
    if unknown:
        raise ValueError(f"Unknown stats resource(s): {unknown}. Valid: {sorted(by_name)}")
    return [by_name[name] for name in only]


@dlt.resource(
    name="team_season_stats",
    write_disposition="merge",
    primary_key=["season", "team", "stat_name"],
)
def team_season_stats_resource(years: list[int]) -> Iterator[dict]:
    """Load team season statistics.

    Args:
        years: List of years to load stats for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading team season stats for {year}...")

            data = make_request(client, "/stats/season", params={"year": year})

            for stat in data:
                stat["season"] = year
                yield stat

    finally:
        client.close()


@dlt.resource(
    name="player_season_stats",
    write_disposition="merge",
    primary_key=["player_id", "season", "team", "category", "stat_type"],
)
def player_season_stats_resource(years: list[int]) -> Iterator[dict]:
    """Load player season statistics.

    Args:
        years: List of years to load stats for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player season stats for {year}...")

            # Player stats endpoint can return a lot of data
            # We'll load by category to manage memory
            categories = [
                "passing",
                "rushing",
                "receiving",
                "fumbles",
                "defensive",
                "interceptions",
                "punting",
                "kicking",
                "kickReturns",
                "puntReturns",
            ]

            for category in categories:
                logger.info(f"  Category: {category}...")
                data = make_request(
                    client, "/stats/player/season", params={"year": year, "category": category}
                )

                for stat in data:
                    stat["season"] = year
                    stat["category"] = category
                    # API returns statType (e.g., "YDS", "TD"); ensure it's present for PK
                    stat["stat_type"] = stat.get("statType", stat.get("stat_type", "unknown"))
                    yield stat

    finally:
        client.close()


@dlt.resource(
    name="advanced_team_stats",
    write_disposition="merge",
    primary_key=["season", "team"],
)
def advanced_team_stats_resource(years: list[int]) -> Iterator[dict]:
    """Load advanced team statistics (EPA, success rates, etc).

    Args:
        years: List of years to load stats for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading advanced team stats for {year}...")

            data = make_request(client, "/stats/season/advanced", params={"year": year})

            for stat in data:
                stat["season"] = year
                yield stat

    finally:
        client.close()


@dlt.resource(
    name="advanced_game_stats",
    write_disposition="merge",
    primary_key=["game_id", "team"],
)
def advanced_game_stats_resource(
    game_ids: list[int],
    *,
    misses: list[int] | None = None,
    report: AdvancedBoxBatchReport | None = None,
) -> Iterator[dict]:
    """Load advanced game-level box score stats, one call per explicit game id.

    BROKEN AS YEAR-SCOPED (confirmed 2026-08-29): CFBD dropped the `year`
    query parameter from `/game/box/advanced` -- the live OpenAPI spec now
    requires a single `id` (game id) per call, and every year-only call
    always 400s. This resource previously iterated `years` and silently
    no-op'd (one wasted 400 per requested year, every stats load) --
    docs/pipeline-manifest.md row 12.

    Reworked to explicit-id mode, mirroring play_stats_resource's game_ids
    branch, and REMOVED from stats_source's default return list so a normal
    stats load no longer spends that call. Year-scoped advanced game-team
    stats are now served by `game_advanced_resource`
    (stats.game_advanced_team_stats, `/stats/game/advanced`) below, which
    still accepts `year`.

    Kept importable for a future historical-refresh campaign that walks
    core.games and backfills this table id-by-id -- one call per game, an
    unbounded fan-out that must never run from a year-driven default path
    (the same reasoning `play_stats_resource` and `metrics_wp_source`
    document for their own per-game modes).

    Args:
        game_ids: Explicit CFBD game ids to load advanced box scores for.
        misses: When given (not None), a suppressed 400, an empty 200
            (`if not data`), and a 5xx or request timeout after the client's
            transient retries were exhausted, for a game id append that id
            here -- the caller
            (scripts/backfill_refresh.py) uses this to record that game as
            'no_data' rather than 'refreshed' in meta.refresh_progress
            (PR #75 F6: a suppressed per-game 400 was previously getting a
            plain ledger row, permanently hiding a real miss from the
            campaign's backlog). Unlike play_stats_resource, an empty 200
            here IS a miss: every game with a completed box score has
            advanced stats, so nothing back means the call didn't land.
            When misses is None, only a 400 is suppressed; a 5xx or timeout
            is re-raised. When misses is set but report is None, a
            suppressed 5xx or timeout is recorded in misses and the resource
            continues with no probe or outage machinery (only
            scripts/backfill_refresh.py passes report). When report is set
            (the backfill path), health tracking, probe-on-trip, and outage
            signaling are active: a confirmed outage sets report.outage and
            returns normally so dlt keeps rows yielded so far. A per-game
            request timeout after the client's retries counts exactly like a
            persistent 5xx (run 39637616538: one ReadTimeout killed the whole
            run at dlt's extract step); other transport errors are
            endpoint-wide and stop the batch as an outage. In report mode NO
            exception is allowed to escape this generator: a connection
            failure, 429 rate-limit exhaustion, or any other unrecoverable
            per-game error also sets report.outage and returns, because a
            raise would abort the dlt load and discard the whole batch -- the
            exact bug (run 39253607809) this report contract exists to fix.
            RequestBudgetExhausted (the LOCAL budget guard) is the one
            exception that is a pacing condition rather
            than a failure: it sets report.budget_stop instead of
            report.outage, so the caller stops draining and exits 0. In every
            early-stop case the failing game is NOT appended to attempted, so
            it stays in the backlog and no ledger row is written for a call
            whose outcome is unknown.
    """
    client = get_client()
    health = _ADVANCED_BOX_HEALTH

    def _choose_probe_id() -> int:
        if health.last_success_game_id is not None:
            return health.last_success_game_id
        if report is not None and report.fallback_probe_game_id is not None:
            return report.fallback_probe_game_id
        return ADVANCED_BOX_PROBE_FALLBACK_GAME_ID

    def _run_outage_probe(last_5xx_game_id: int) -> bool:
        """Return True if the probe confirms an outage (caller should stop)."""
        probe_id = _choose_probe_id()
        if report is not None:
            report.probe_game_id = probe_id
        try:
            make_request(client, "/game/box/advanced", params={"id": probe_id})
            logger.warning(
                "Advanced box probe for game %s succeeded; recent 5xx games are bad data",
                probe_id,
            )
            health.clear_window()
            return False
        except httpx.HTTPStatusError as probe_err:
            status_code = probe_err.response.status_code
            if status_code in (400, 404):
                # A request-level rejection of this specific probe id: CFBD
                # answered, so the API is up and the recent 5xx games are bad
                # data. 401/403 deliberately do NOT land here (Greptile/Codex
                # finding on PR #156): rejected credentials mean NO call can
                # be trusted to succeed, so they fail the run like a 5xx.
                logger.warning(
                    "Advanced box probe for game %s returned HTTP %s; "
                    "treating CFBD as up (bad data for recent 5xx games)",
                    probe_id,
                    status_code,
                )
                health.clear_window()
                return False
            server_errors, answered = health.window_counts()
            message = (
                f"CFBD outage confirmed: probe game {probe_id} returned HTTP "
                f"{status_code}; last 5xx game {last_5xx_game_id}; "
                f"health window {server_errors} server-error(s), {answered} answered"
            )
            logger.error(message)
            if report is not None:
                report.outage = message
            return True
        except RequestBudgetExhausted as probe_err:
            message = f"local request budget refused the probe call: {probe_err}"
            logger.warning(message)
            if report is not None:
                report.budget_stop = message
            return True
        except Exception as probe_err:
            server_errors, answered = health.window_counts()
            message = (
                f"CFBD outage confirmed: probe game {probe_id} failed ({probe_err!r}); "
                f"last 5xx game {last_5xx_game_id}; "
                f"health window {server_errors} server-error(s), {answered} answered"
            )
            logger.error(message)
            if report is not None:
                report.outage = message
            return True

    def _note_server_failure(game_id: int, cause: str) -> bool:
        """Record one game CFBD failed to serve (5xx or a timeout, both after
        the client's transient retries): a no_data miss, plus -- in report
        mode -- a server-error mark in the health window and the
        probe-on-trip check. Returns True when the probe confirmed the batch
        must stop. Callers guarantee ``misses is not None``.
        """
        logger.warning(
            f"No advanced box score for game {game_id} ({cause} after the client's "
            "transient retries were already exhausted); recording the game as no-data "
            "(retryable later via scripts/backfill_refresh.py --requeue-no-data)"
        )
        misses.append(game_id)
        if report is not None:
            report.attempted.append(game_id)
            report.server_error_game_ids.append(game_id)
            health.record_server_error(game_id)
            if health.should_probe() and _run_outage_probe(game_id):
                return True
        return False

    try:
        total = 0
        for i, game_id in enumerate(game_ids):
            try:
                data = make_request(
                    client,
                    "/game/box/advanced",
                    params={"id": game_id},
                )
                if data:
                    total += len(data)
                    yield from data
                    if report is not None:
                        report.attempted.append(game_id)
                        health.record_answered(game_id, has_data=True)
                else:
                    logger.warning(f"No advanced box score for game {game_id} (empty response)")
                    if misses is not None:
                        misses.append(game_id)
                    if report is not None:
                        report.attempted.append(game_id)
                        health.record_answered(game_id, has_data=False)
                if (i + 1) % 100 == 0:
                    logger.info(f"  Processed {i + 1}/{len(game_ids)} games, {total} records")
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No advanced box score for game {game_id} (400 response)")
                    if misses is not None:
                        misses.append(game_id)
                    if report is not None:
                        report.attempted.append(game_id)
                        health.record_answered(game_id, has_data=False)
                    continue
                if misses is not None and e.response.status_code >= 500:
                    if _note_server_failure(game_id, f"HTTP {e.response.status_code}"):
                        return
                    continue
                if report is not None:
                    message = (
                        f"advanced box call for game {game_id} failed unrecoverably "
                        f"(HTTP {e.response.status_code}); stopping this batch so rows "
                        "loaded so far are kept and ledgered"
                    )
                    logger.error(message)
                    report.outage = message
                    return
                raise
            except httpx.TimeoutException as e:
                # Run 39637616538 (Historical Refresh #54, 2026-10-21): after
                # persistent 5xx on five games, one request raised
                # httpx.ReadTimeout and killed the whole run at dlt's extract
                # step. A per-game timeout that survived the client's retries
                # is the same signal as a persistent 5xx -- CFBD could not
                # serve THIS game -- so it gets the identical treatment:
                # no_data miss, server-error mark in the health window, and
                # the probe decides bad data vs outage. A widespread run of
                # timeouts fills the window, the probe times out too, and the
                # run stops cleanly as an outage. Other transport errors
                # (connection refused, DNS) stay on the outage stop below:
                # they are endpoint-wide, not per-game.
                if misses is not None:
                    if _note_server_failure(game_id, repr(e)):
                        return
                    continue
                raise
            except RequestBudgetExhausted as e:
                # Local budget guard, not a CFBD failure (PR #156 review
                # finding): stop draining gracefully so rows yielded so far
                # load and the attempted prefix is ledgered, and let the
                # caller treat it as a pacing stop (exit 0), never an outage.
                if report is not None:
                    message = (
                        f"local request budget refused the call for game {game_id}: {e}; "
                        "stopping this batch (pacing stop, not an error)"
                    )
                    logger.warning(message)
                    report.budget_stop = message
                    return
                raise
            except Exception as e:
                # Report mode only: a transport failure or rate-limit
                # exhaustion that survived the client's retries means CFBD is
                # unreachable -- same outage handling as a probe failure, and
                # raising here would discard the batch's loaded rows.
                if report is not None:
                    message = (
                        f"advanced box call for game {game_id} failed unrecoverably "
                        f"({e!r}); stopping this batch so rows loaded so far are "
                        "kept and ledgered"
                    )
                    logger.error(message)
                    report.outage = message
                    return
                raise
        logger.info(f"Loaded {total} total records from {len(game_ids)} games")

    finally:
        client.close()


@dlt.resource(
    name="player_usage",
    write_disposition="merge",
    primary_key=["season", "id"],
)
def player_usage_resource(years: list[int]) -> Iterator[dict]:
    """Load player usage metrics.

    Note: Data only available from ~2014+. Earlier years return 400 and are skipped.

    Args:
        years: List of years to load usage for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player usage for {year}...")

            try:
                data = make_request(
                    client,
                    "/player/usage",
                    params={"year": year},
                )
                yield from data
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No player usage data for {year} (400 response), skipping")
                    continue
                raise

    finally:
        client.close()


@dlt.resource(
    name="player_returning",
    write_disposition="merge",
    primary_key=["season", "team"],
)
def player_returning_resource(years: list[int]) -> Iterator[dict]:
    """Load returning player production data.

    Note: Data only available from ~2014+. Earlier years return 400 and are skipped.

    Args:
        years: List of years to load returning production for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player returning production for {year}...")

            try:
                data = make_request(
                    client,
                    "/player/returning",
                    params={"year": year},
                )
                yield from data
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No player returning data for {year} (400 response), skipping")
                    continue
                raise

    finally:
        client.close()


@dlt.resource(
    name="play_stats",
    write_disposition="merge",
    primary_key=["game_id", "play_id", "athlete_id", "stat_type"],
)
def play_stats_resource(
    years: list[int] | None = None,
    game_ids: list[int] | None = None,
    *,
    misses: list[int] | None = None,
) -> Iterator[dict]:
    """Load play-level statistics (player associations for each play).

    IMPORTANT: The API has a 2000 record limit per request. When loading by year,
    we iterate by gameId to ensure complete data extraction.

    Note: Data only available from ~2014+. Earlier years return 400 and are skipped.

    Args:
        years: List of years to load play stats for (will fetch game IDs from API)
        game_ids: Explicit list of game IDs to load (overrides years if provided)
        misses: game_ids mode only. When given (not None), a suppressed 400
            for a game id appends that id here -- the caller
            (scripts/backfill_refresh.py) uses this to record that game as
            'no_data' rather than 'refreshed' (PR #75 F6). An empty 200 is
            deliberately NOT treated as a miss here: zero player-stat
            associations is a legitimate outcome for early-era or
            lower-division games (mirrors the benign empty-week handling in
            the year-mode branch below), so it is not appended. Ignored in
            years mode.
    """
    if game_ids is None and years is None:
        raise ValueError("Must provide either years or game_ids")

    client = get_client()
    try:
        if game_ids is not None:
            # Direct game ID iteration
            total = 0
            for i, game_id in enumerate(game_ids):
                try:
                    data = make_request(
                        client,
                        "/plays/stats",
                        params={"gameId": game_id},
                    )
                    if data:
                        total += len(data)
                        yield from data
                    if (i + 1) % 100 == 0:
                        logger.info(f"  Processed {i + 1}/{len(game_ids)} games, {total} records")
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 400:
                        logger.warning(f"No play stats for game {game_id} (400 response)")
                        if misses is not None:
                            misses.append(game_id)
                        continue
                    raise
            logger.info(f"Loaded {total} total records from {len(game_ids)} games")
        else:
            # Year-based loading: fetch games for each year, then iterate by gameId
            for year in years:
                logger.info(f"Loading play stats for {year}...")
                try:
                    # Get all games for this year
                    games = make_request(
                        client,
                        "/games",
                        params={"year": year, "seasonType": "regular"},
                    )
                    # Also get postseason games
                    postseason = make_request(
                        client,
                        "/games",
                        params={"year": year, "seasonType": "postseason"},
                    )
                    games.extend(postseason)

                    # Only COMPLETED games. An unplayed game has no play stats,
                    # so requesting one spends a call to receive nothing --
                    # and this resource spends one call per game. From
                    # 2026-08-01 `get_current_season()` returned 2026, the
                    # season was not final so nothing was skipped, and the
                    # daily load walked all 1,638 *scheduled* 2026 games every
                    # day. It got ~370 in before CFBD started answering 429,
                    # which failed the whole `stats` extract package (taking
                    # player_returning's already-fetched payload down with it)
                    # and burst-blocked `ratings` and `game_stats` behind it.
                    # Three consecutive red daily loads, 2026-08-04 onward.
                    scheduled = [g for g in games if g.get("id")]
                    completed = [g for g in scheduled if g.get("completed")]
                    game_ids_for_year = [g["id"] for g in completed]
                    logger.info(
                        f"  Found {len(scheduled)} games for {year}, "
                        f"{len(game_ids_for_year)} completed "
                        f"({len(scheduled) - len(game_ids_for_year)} unplayed, skipped)"
                    )

                    year_total = 0
                    for i, game_id in enumerate(game_ids_for_year):
                        try:
                            data = make_request(
                                client,
                                "/plays/stats",
                                params={"gameId": game_id},
                            )
                            if data:
                                year_total += len(data)
                                yield from data
                            if (i + 1) % 100 == 0:
                                logger.info(
                                    f"    {year}: {i + 1}/{len(game_ids_for_year)} games,"
                                    f" {year_total} records"
                                )
                        except httpx.HTTPStatusError as e:
                            if e.response.status_code == 400:
                                continue
                            raise

                    logger.info(
                        f"Loaded {year}: {year_total} records from {len(game_ids_for_year)} games"
                    )

                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 400:
                        logger.warning(f"No play stats data for {year} (400 response), skipping")
                        continue
                    raise

    finally:
        client.close()


@dlt.resource(
    name="game_havoc",
    write_disposition="merge",
    primary_key=["game_id", "team"],
)
def game_havoc_resource(years: list[int]) -> Iterator[dict]:
    """Load game-level havoc statistics (TFLs, passes broken up, etc).

    Note: Data only available from ~2014+. Earlier years return 400 and are skipped.

    Args:
        years: List of years to load havoc stats for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading game havoc stats for {year}...")

            try:
                data = make_request(
                    client,
                    "/stats/game/havoc",
                    params={"year": year},
                )
                yield from data
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No game havoc data for {year} (400 response), skipping")
                    continue
                raise

    finally:
        client.close()


@dlt.resource(
    name="player_success_season",
    write_disposition="merge",
    primary_key=["season", "id", "team"],
)
def player_success_season_resource(years: list[int]) -> Iterator[dict]:
    """Load player season success-rate splits (passing/rushing), one call per year.

    Every field CFBD returns is already top-level (season, id, name,
    position, team, conference) -- id/season/team/position, the player-grain
    join spine, need no manual stamping. `passing`/`rushing` are nested
    dicts (plays, successes, successRate) that dlt flattens into
    `passing__plays`, etc.

    Note: Data only available from ~2014+. Earlier years return 400 and are skipped.

    Args:
        years: List of years to load success-rate stats for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player success rates for {year}...")

            try:
                data = make_request(client, "/stats/player/success", params={"year": year})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No player success data for {year} (400 response), skipping")
                    continue
                raise

            yield from data

    finally:
        client.close()


@dlt.resource(
    name="player_success_game",
    write_disposition="merge",
    primary_key=["game_id", "id"],
)
def player_success_game_resource(years: list[int]) -> Iterator[dict]:
    """Load player game-level success-rate splits, iterating weeks per year.

    `/stats/player/success/game` requires `week` unless `team` or
    `playerId` is given -- a year-alone call 400s -- so this walks weeks
    like game_stats.py's game_team_stats_resource: regular season weeks
    1-16, postseason weeks 1-4. An empty week (byes, early postseason
    rounds not yet played) is not an error and is skipped silently.

    Every field CFBD returns is already top-level (season, seasonType,
    week, gameId, id, name, position, team, conference, opponent) --
    id/season/team/position, the player-grain join spine, need no manual
    stamping.

    Note: Data only available from ~2014+.

    Args:
        years: List of years to load success-rate stats for
    """
    client = get_client()
    try:
        for year in years:
            for season_type in ("regular", "postseason"):
                max_week = 16 if season_type == "regular" else 4
                for week in range(1, max_week + 1):
                    logger.info(
                        f"Loading player game success rates for {year} {season_type} week {week}..."
                    )
                    try:
                        data = make_request(
                            client,
                            "/stats/player/success/game",
                            params={"year": year, "seasonType": season_type, "week": week},
                        )
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 400:
                            logger.warning(
                                f"No player game success data for {year} {season_type} "
                                f"week {week} (400 response), skipping"
                            )
                            continue
                        raise

                    if not data:
                        continue

                    yield from data

    finally:
        client.close()


@dlt.resource(
    name="game_advanced",
    table_name="game_advanced_team_stats",
    write_disposition="merge",
    primary_key=["game_id", "team"],
)
def game_advanced_resource(years: list[int]) -> Iterator[dict]:
    """Load advanced game-level team stats (EPA, success rate, line yards).

    Works year-scoped -- unlike the broken `/game/box/advanced` (see
    advanced_game_stats_resource above), `/stats/game/advanced` still
    accepts a bare `year`. Iterates seasonType regular+postseason.

    Args:
        years: List of years to load advanced game stats for
    """
    client = get_client()
    try:
        for year in years:
            for season_type in ("regular", "postseason"):
                logger.info(f"Loading advanced game stats for {year} {season_type}...")

                try:
                    data = make_request(
                        client,
                        "/stats/game/advanced",
                        params={"year": year, "seasonType": season_type},
                    )
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 400:
                        logger.warning(
                            f"No advanced game stats for {year} {season_type} "
                            "(400 response), skipping"
                        )
                        continue
                    raise

                yield from data

    finally:
        client.close()
