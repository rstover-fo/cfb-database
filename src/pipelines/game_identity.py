"""Reviewed provider event resolutions; raw warehouse rows remain unchanged."""

import re
from collections.abc import Mapping

# Original ESPN event is postponed with no stats/drives; its replacement was played.
# https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=401866625
# https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=401917058
# https://catamountsports.com/news/2026/9/5/football-catamounts-camels-postponed-until-sunday.aspx
SUPERSEDED_GAME_REPLACEMENTS = {401866625: 401917058}
# Add only after reviewing provider/official evidence; absence is not cancellation.
CANCELLED_GAME_IDS: frozenset[int] = frozenset()


def eligible_game_sql(id_column: str = "g.id") -> str:
    """Exclude reviewed non-contests from modeled inputs, including partial fetches.

    This only filters identities; callers retain their season/result predicates.
    A missing replacement must never make a known postponed original a result.
    Schedule/closure callers additionally validate the replacement pair below.
    Column names are developer-supplied identifiers, never user SQL fragments.
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*(?:\.[A-Za-z_][A-Za-z_0-9]*)?", id_column):
        raise ValueError("invalid game ID column")
    excluded = sorted(set(SUPERSEDED_GAME_REPLACEMENTS) | set(CANCELLED_GAME_IDS))
    return f"{id_column} NOT IN ({', '.join(str(i) for i in excluded)})" if excluded else "TRUE"


def select_canonical_game_rows(
    rows: list[dict],
    *,
    cancelled_game_ids: frozenset[int] = CANCELLED_GAME_IDS,
    superseded_game_replacements: Mapping[int, int] = SUPERSEDED_GAME_REPLACEMENTS,
) -> list[dict]:
    """Resolve a complete fetched schedule without changing source rows.

    The replacement must exist and match season/home/away identity. Missing
    identity fields do not count as agreement. Repeated matchups are retained.
    """
    rows_by_id = {row["game_id"]: row for row in rows}
    for original_id, replacement_id in superseded_game_replacements.items():
        original = rows_by_id.get(original_id)
        if original is None:
            continue
        replacement = rows_by_id.get(replacement_id)
        if replacement is None:
            raise ValueError(
                f"Superseded game {original_id} is present without replacement "
                f"{replacement_id}; refusing an incomplete schedule"
            )
        mismatches = [
            field
            for field in ("season", "home_team", "away_team")
            if original.get(field) is None or original.get(field) != replacement.get(field)
        ]
        if mismatches:
            raise ValueError(
                f"Superseded game {original_id} and replacement {replacement_id} disagree "
                f"on {', '.join(mismatches)}; refusing to suppress either event"
            )
    excluded = set(superseded_game_replacements) | set(cancelled_game_ids)
    return [row for row in rows if row["game_id"] not in excluded]
