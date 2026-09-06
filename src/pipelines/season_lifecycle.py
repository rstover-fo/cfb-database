"""Conservative, shared season-finality policy.

Finality gates immutable-source skips, player-overview ingestion, and model
refits.  It is intentionally stricter than a completion percentage: a season
cannot close while a known contest remains unresolved, before its postseason
calendar has elapsed, or from a regular-season-only partial schedule.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time

from .game_identity import (
    CANCELLED_GAME_IDS,
    SUPERSEDED_GAME_REPLACEMENTS,
    select_canonical_game_rows,
)

logger = logging.getLogger(__name__)

MIN_GAMES_FOR_FINAL_SEASON = 100
_POSTSEASON = "postseason"
_CLOSURE_SEASON_TYPES = frozenset({"regular", _POSTSEASON})
_IGNORED_SEASON_TYPES = frozenset({"allstar", "exhibition"})


@dataclass(frozen=True)
class SeasonResolutions:
    """Reviewed terminal and replacement event identities.

    Callers may pass a small fixture-specific value to the pure classifier.
    Production uses the reviewed registry in :mod:`game_identity`.
    """

    cancelled_game_ids: frozenset[int]
    superseded_game_replacements: Mapping[int, int]


DEFAULT_RESOLUTIONS = SeasonResolutions(
    cancelled_game_ids=CANCELLED_GAME_IDS,
    superseded_game_replacements=SUPERSEDED_GAME_REPLACEMENTS,
)


@dataclass(frozen=True)
class SeasonLifecycle:
    """Pure classification result with enough detail for logs and tests."""

    is_final: bool
    reason: str
    scheduled_games: int
    completed_games: int
    unresolved_game_ids: tuple[int, ...] = ()


def season_close_floor(season: int) -> datetime:
    """Earliest instant a season may be final.

    February 1 covers the ordinary bowl/CFP tail.  The 2020 season uses July 1
    because pandemic-disrupted schedules continued into spring 2021.  This is
    only an eligibility floor; passing it never turns an unresolved row into a
    cancellation.
    """

    month = 7 if season == 2020 else 2
    return datetime.combine(date(season + 1, month, 1), time.min, tzinfo=UTC)


def _as_utc(value: datetime | date | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return datetime.combine(value, time.max, tzinfo=UTC)


def _canonical_rows(
    rows: list[dict], resolutions: SeasonResolutions
) -> tuple[list[dict], str | None]:
    """Apply reviewed identities, returning a reconciliation error as state."""

    try:
        return (
            select_canonical_game_rows(
                rows,
                cancelled_game_ids=resolutions.cancelled_game_ids,
                superseded_game_replacements=resolutions.superseded_game_replacements,
            ),
            None,
        )
    except ValueError as exc:
        return rows, str(exc)


def classify_season(
    rows: Iterable[Mapping],
    season: int,
    *,
    as_of: datetime | date | None = None,
    resolutions: SeasonResolutions = DEFAULT_RESOLUTIONS,
) -> SeasonLifecycle:
    """Classify a fully fetched schedule without database or network access.

    ``rows`` must contain game_id, season, season_type, start_date, completed,
    home_team, away_team, home_points, and away_points.  Missing dates, missing
    scores on completed games, future games, and incomplete games all remain
    unresolved.  A row's age is never evidence that it was cancelled.
    """

    normalized = [dict(row) for row in rows]
    canonical, reconciliation_error = _canonical_rows(normalized, resolutions)
    if reconciliation_error:
        return SeasonLifecycle(False, reconciliation_error, len(normalized), 0)

    mixed_season_ids = [int(row["game_id"]) for row in canonical if row.get("season") != season]
    if mixed_season_ids:
        ids = tuple(sorted(set(mixed_season_ids)))
        return SeasonLifecycle(
            False,
            f"game(s) do not belong to season {season}: {', '.join(map(str, ids[:10]))}",
            len(canonical),
            0,
            ids,
        )

    closure_rows: list[dict] = []
    unknown_type_ids: list[int] = []
    for row in canonical:
        season_type = str(row.get("season_type") or "").lower()
        if season_type in _CLOSURE_SEASON_TYPES:
            closure_rows.append(row)
        elif season_type not in _IGNORED_SEASON_TYPES:
            unknown_type_ids.append(int(row["game_id"]))

    scheduled = len(closure_rows)
    completed = sum(bool(row.get("completed")) for row in closure_rows)
    if unknown_type_ids:
        ids = tuple(sorted(set(unknown_type_ids)))
        return SeasonLifecycle(
            False,
            f"unknown season type for game(s) {', '.join(map(str, ids[:10]))}",
            scheduled,
            completed,
            ids,
        )
    if scheduled < MIN_GAMES_FOR_FINAL_SEASON:
        return SeasonLifecycle(
            False,
            f"schedule has only {scheduled} canonical games",
            scheduled,
            completed,
        )

    present_types = {str(row.get("season_type") or "").lower() for row in closure_rows}
    missing_types = _CLOSURE_SEASON_TYPES - present_types
    if missing_types:
        return SeasonLifecycle(
            False,
            f"schedule has no {' or '.join(sorted(missing_types))} games",
            scheduled,
            completed,
        )

    observed_at = _as_utc(as_of)
    close_floor = season_close_floor(season)
    if observed_at < close_floor:
        return SeasonLifecycle(
            False,
            f"calendar floor {close_floor.date().isoformat()} has not passed",
            scheduled,
            completed,
        )

    unresolved: list[int] = []
    for row in closure_rows:
        game_id = int(row["game_id"])
        start_date = row.get("start_date")
        if start_date is None:
            unresolved.append(game_id)
            continue
        kickoff = _as_utc(start_date)
        if kickoff > observed_at:
            unresolved.append(game_id)
            continue
        if not row.get("completed"):
            unresolved.append(game_id)
            continue
        if row.get("home_points") is None or row.get("away_points") is None:
            unresolved.append(game_id)

    if unresolved:
        ids = tuple(sorted(set(unresolved)))
        sample = ", ".join(map(str, ids[:10]))
        return SeasonLifecycle(
            False,
            f"{len(ids)} canonical game(s) remain unresolved: {sample}",
            scheduled,
            completed,
            ids,
        )
    return SeasonLifecycle(True, "all canonical games are resolved", scheduled, completed)


_SEASON_GAMES_QUERY = """
    SELECT id AS game_id,
           season,
           season_type,
           start_date,
           completed,
           home_id,
           home_team,
           home_points,
           away_id,
           away_team,
           away_points
    FROM core.games
    WHERE season = %s
"""


def season_is_final(conn, season: int, *, as_of: datetime | date | None = None) -> bool:
    """Return whether ``season`` is conservatively safe to treat as final."""

    with conn.cursor() as cur:
        cur.execute(_SEASON_GAMES_QUERY, (season,))
        columns = [description[0] for description in cur.description]
        rows = [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]
    lifecycle = classify_season(rows, season, as_of=as_of)
    if not lifecycle.is_final:
        logger.info("Season %d is not final: %s", season, lifecycle.reason)
    return lifecycle.is_final
