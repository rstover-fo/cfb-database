"""Narrow, verified corrections for CFBD ``/games`` result records.

Corrections apply only after the provider event identity has been checked.  They
never mutate the fetched response, so the shared games cache continues to hold
the exact raw CFBD payload used by dependent resources.
"""

from collections.abc import Mapping

# These are source-result corrections, not general team/date matching rules.
# Taylor 63, Defiance 12: https://taylor2023.prestosports.com/sports/fball/2024-25/bios/loveless_carter_825c
# Sul Ross 0, Angelo State 62: https://srlobos.com/sports/football/stats/2025/angelo-state/boxscore/3842
VERIFIED_GAME_RESULTS: dict[int, dict[str, int]] = {
    401677463: {
        "season": 2024,
        "homeId": 620,
        "awayId": 190,
        "homePoints": 63,
        "awayPoints": 12,
    },
    401773541: {
        "season": 2025,
        "homeId": 2834,
        "awayId": 2025,
        "homePoints": 0,
        "awayPoints": 62,
    },
}


def validate_verified_result_source_season(
    game: Mapping[str, object], requested_season: int
) -> None:
    """Reject a reviewed event whose supplied season conflicts with its request.

    CFBD normally omits ``season`` from ``/games?year=...`` payloads.  If it
    supplies one for a reviewed correction, preserve that evidence long enough
    to validate it before the resource adds its requested partition season.
    """
    if game.get("id") not in VERIFIED_GAME_RESULTS:
        return
    source_season = game.get("season")
    if source_season is not None and source_season != requested_season:
        raise ValueError(
            f"Verified result correction for game {game.get('id')} has source "
            f"season={source_season!r}, requested season={requested_season}"
        )


def apply_verified_result_correction(game: Mapping[str, object]) -> dict[str, object]:
    """Return a corrected copy for an exactly identified reviewed game.

    A known event ID with a mismatching season or team identity is an upstream
    identity change and must stop the load rather than transfer a score to a
    different contest.  Existing non-null scores must already agree with the
    reviewed result; only missing scores are filled.
    """
    correction = VERIFIED_GAME_RESULTS.get(game.get("id"))
    if correction is None:
        return dict(game)

    identity_fields = ("season", "homeId", "awayId")
    mismatches = [field for field in identity_fields if game.get(field) != correction[field]]
    if mismatches:
        raise ValueError(
            f"Verified result correction for game {game.get('id')} has mismatching "
            f"{', '.join(mismatches)}"
        )

    for field in ("homePoints", "awayPoints"):
        existing = game.get(field)
        if existing is not None and existing != correction[field]:
            raise ValueError(
                f"Verified result correction for game {game.get('id')} conflicts with "
                f"existing {field}={existing!r}"
            )

    corrected = dict(game)
    corrected["homePoints"] = correction["homePoints"]
    corrected["awayPoints"] = correction["awayPoints"]
    corrected["completed"] = True
    return corrected
