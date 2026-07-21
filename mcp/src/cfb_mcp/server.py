"""cfb_mcp FastMCP server: 8 read-only tools over the cfb-database warehouse.

Every tool is a thin, LLM-facing wrapper around one or more PostgREST calls
(see cfb_mcp.postgrest). Tools only touch objects in the public surface
defined by docs/SCHEMA_CONTRACT.md in cfb-database: ``api.*`` views and a
fixed allowlist of ``public`` RPCs (Contract Rule 4). There is deliberately
no dynamic-SQL / arbitrary-query tool -- see mcp/README.md's "SQL tool
deferral" section for why.

Every successful JSON response includes a ``_source`` field (or one per
sub-object) naming the exact view/RPC the data came from, so the calling
model can qualify its answer ("per api.leaderboard_teams...").
"""

from __future__ import annotations

import json
from enum import StrEnum
from typing import Annotated, Any

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from cfb_mcp.postgrest import DEFAULT_ROW_CAP, PostgrestClient, PostgrestError, eq, gte

mcp = FastMCP("cfb_mcp")

# All eight tools are read-only, non-destructive, idempotent, and talk to an
# external service -- same annotation set for every one of them.
READ_ONLY_ANNOTATIONS = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": True,
    "openWorldHint": True,
}


def _dump(payload: Any) -> str:
    return json.dumps(payload, indent=2, default=str)


def _wrap(source: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Attach a _source tag and row count to a result set."""
    return {"_source": source, "count": len(rows), "rows": rows}


class SplitType(StrEnum):
    """Which situational-splits RPC to call for `situational_splits`."""

    HOME_AWAY = "home_away"
    CONFERENCE = "conference"
    RED_ZONE = "red_zone"
    DOWN_DISTANCE = "down_distance"
    FIELD_POSITION = "field_position"


_SPLIT_RPCS: dict[SplitType, str] = {
    SplitType.HOME_AWAY: "get_home_away_splits",
    SplitType.CONFERENCE: "get_conference_splits",
    SplitType.RED_ZONE: "get_red_zone_splits",
    SplitType.DOWN_DISTANCE: "get_down_distance_splits",
    SplitType.FIELD_POSITION: "get_field_position_splits",
}


class LeaderboardMetric(StrEnum):
    """Ranking metric for `get_leaderboard`."""

    WINS = "wins"
    PPG = "ppg"
    SCORING_DEFENSE = "scoring_defense"
    EPA = "epa"
    SP_RATING = "sp_rating"
    WEPA = "wepa"


# Column to order by within api.leaderboard_teams for each non-wepa metric.
# leaderboard_teams pre-computes these *_rank columns; wepa is handled
# separately against api.team_wepa_season (see get_leaderboard).
_LEADERBOARD_ORDER: dict[LeaderboardMetric, str] = {
    LeaderboardMetric.WINS: "wins_rank.asc",
    LeaderboardMetric.PPG: "ppg_rank.asc",
    LeaderboardMetric.SCORING_DEFENSE: "defense_ppg_rank.asc",
    LeaderboardMetric.EPA: "epa_rank.asc",
    LeaderboardMetric.SP_RATING: "sp_rank.asc",
}


class PollSeasonType(StrEnum):
    """season_type filter for `get_rankings` -- see api.poll_rankings.

    CFBD reports the final postseason poll as week=1, which collides with
    the regular-season week-1 poll's week number. season_type is the only
    thing that disambiguates them.
    """

    REGULAR = "regular"
    POSTSEASON = "postseason"


# ---------------------------------------------------------------------
# 1. query_team
# ---------------------------------------------------------------------


@mcp.tool(name="query_team", annotations={"title": "Query Team", **READ_ONLY_ANNOTATIONS})
async def query_team(
    team: Annotated[
        str,
        Field(
            description=(
                "Exact school name as used by CFBD, e.g. 'Oklahoma', 'Ohio State', "
                "'Texas A&M'. This is an exact, case-sensitive match, not a fuzzy search -- "
                "if unsure of the exact spelling, try get_leaderboard or query_games first "
                "to confirm it."
            )
        ),
    ],
) -> str:
    """Get a team's current-season snapshot plus its full multi-season history.

    When to use: any question about a single team -- "how good is Oklahoma this
    year", "show Oklahoma's history since 2014", ratings/EPA trends over time.

    Combines two sources in one call:
      - api.team_detail: current-season snapshot (record, SP+/Elo/FPI ratings,
        EPA/success rate/explosiveness, recruiting rank). At most one row.
      - api.team_history: one row per season the team has data for (record,
        ratings, EPA), ordered season DESC. Up to 100 rows.

    Caveats:
      - Team names must match CFBD's convention exactly (case-sensitive).
        "oklahoma" or "OU" will not match "Oklahoma".
      - api.team_detail only includes FBS-classification teams; an FCS/other
        team will return an empty team_detail but may still have team_history
        rows from a season it was FBS (rare) or no rows at all.

    Returns: JSON with "team_detail" and "team_history" keys, each shaped as
    {"_source": "<view>", "count": int, "rows": [...]}. If nothing at all is
    found, returns a plain "No team found..." string instead.
    """
    client = PostgrestClient()
    try:
        detail_rows = await client.select(
            "team_detail", {"school": eq(team)}, profile="api", limit=1
        )
        history_rows = await client.select(
            "team_history",
            {"team": eq(team), "order": "season.desc"},
            profile="api",
            limit=DEFAULT_ROW_CAP,
        )
    except PostgrestError as e:
        return e.message

    if not detail_rows and not history_rows:
        return (
            f"No team found matching '{team}'. Team names are case-sensitive exact matches "
            "(e.g. 'Oklahoma', not 'oklahoma' or 'OU')."
        )

    return _dump(
        {
            "team_detail": _wrap("api.team_detail", detail_rows),
            "team_history": _wrap("api.team_history", history_rows),
        }
    )


# ---------------------------------------------------------------------
# 2. query_games
# ---------------------------------------------------------------------


@mcp.tool(name="query_games", annotations={"title": "Query Games", **READ_ONLY_ANNOTATIONS})
async def query_games(
    season: Annotated[
        int | None,
        Field(default=None, description="Season year, e.g. 2024. Strongly recommended."),
    ] = None,
    week: Annotated[
        int | None,
        Field(
            default=None,
            description="Week number within the season (regular season "
            "roughly 1-15; bowls/playoff weeks follow CFBD's season_type/week scheme).",
        ),
    ] = None,
    team: Annotated[
        str | None,
        Field(
            default=None,
            description="Exact school name. Matches games where this team "
            "played either home or away.",
        ),
    ] = None,
    min_excitement: Annotated[
        float | None,
        Field(
            default=None,
            description="Minimum excitement_index (CFBD's game-excitement "
            "score, roughly 0-10; >6 is generally a thrilling finish). Use to find close "
            "or dramatic games.",
        ),
    ] = None,
    limit: Annotated[
        int,
        Field(
            default=DEFAULT_ROW_CAP,
            ge=1,
            le=DEFAULT_ROW_CAP,
            description="Max rows to "
            "return. Hard-capped at 100 server-side regardless of this value.",
        ),
    ] = DEFAULT_ROW_CAP,
) -> str:
    """Search games by season, week, team, and/or minimum excitement.

    When to use: "what happened in Oklahoma's week 5 game", "show close games
    in the 2023 season", "list Oklahoma's 2024 schedule".

    Backed by api.game_detail: teams, scores, winner, betting lines
    (spread/over-under and whether they hit), EPA, pregame win probability,
    venue, attendance, excitement_index. Results are ordered by start_date
    descending (most recent first).

    Caveats:
      - All filters are optional but combine with AND (min_excitement is a
        floor, not a range). Calling with no filters at all returns the 100
        most recent games across all of CFBD history -- always pass at least
        `season` or `team` for a useful result.
      - `team` matches home OR away, so pass one team, not both (use
        query_matchup for head-to-head).
      - Results are capped at 100 rows; a full season across all FBS teams
        is ~800 games, so pair `season` with `team` or `week` to stay under
        the cap.
      - Uncompleted/future games have NULL scores, winner, and EPA.

    Returns: JSON {"_source": "api.game_detail", "count": int, "rows": [...]},
    or a plain "No games found..." string if the filters match nothing.
    """
    params: dict[str, Any] = {}
    if season is not None:
        params["season"] = eq(season)
    if week is not None:
        params["week"] = eq(week)
    if team is not None:
        # Double-quote the value: ( ) , are structural in PostgREST logic-tree
        # syntax, so names like "Miami (OH)" would otherwise corrupt the filter.
        quoted = team.replace('"', '""')
        params["or"] = f'(home_team.eq."{quoted}",away_team.eq."{quoted}")'
    if min_excitement is not None:
        params["excitement_index"] = gte(min_excitement)
    params["order"] = "start_date.desc"

    client = PostgrestClient()
    try:
        rows = await client.select("game_detail", params, profile="api", limit=limit)
    except PostgrestError as e:
        return e.message

    if not rows:
        return "No games found matching the given filters."
    return _dump(_wrap("api.game_detail", rows))


# ---------------------------------------------------------------------
# 3. query_matchup
# ---------------------------------------------------------------------


@mcp.tool(
    name="query_matchup",
    annotations={"title": "Query Head-to-Head Matchup", **READ_ONLY_ANNOTATIONS},
)
async def query_matchup(
    team_a: Annotated[str, Field(description="First team's exact school name.")],
    team_b: Annotated[
        str,
        Field(
            description="Second team's exact school name. Order relative to team_a doesn't "
            "matter -- results are identical either way."
        ),
    ],
) -> str:
    """Get head-to-head history and current-season comparison between two teams.

    When to use: "Oklahoma vs Texas all-time record", "how do these two teams
    compare this season" (rivalry games, bowl previews, etc).

    Backed by api.matchup, which stores one row per unordered team pair
    (internally keyed alphabetically: team1 < team2). This tool normalizes
    team_a/team_b alphabetically before querying, so callers never need to
    know or care about that internal ordering.

    Returns all-time record (total games, wins for each side, ties, first/
    last meeting), a JSON array of recent results (2014+), and each team's
    current-season record/SP+ rank/EPA for context.

    Returns: JSON {"_source": "api.matchup", "count": int, "rows": [...]}
    (rows has 0 or 1 entries), or a plain "No matchup history found..."
    string if the teams have never played or a name is misspelled.
    """
    # NOTE: Python code-point sort assumed to match the DB collation used by the
    # view's LEAST/GREATEST pair ordering; holds for current CFBD ASCII names.
    lo, hi = sorted((team_a, team_b))
    client = PostgrestClient()
    try:
        rows = await client.select(
            "matchup", {"team1": eq(lo), "team2": eq(hi)}, profile="api", limit=1
        )
    except PostgrestError as e:
        return e.message

    if not rows:
        return (
            f"No matchup history found between '{team_a}' and '{team_b}'. These teams may "
            "have never played each other (FBS-era games only), or a team name may be "
            "misspelled."
        )
    return _dump(_wrap("api.matchup", rows))


# ---------------------------------------------------------------------
# 4. get_rankings
# ---------------------------------------------------------------------


@mcp.tool(name="get_rankings", annotations={"title": "Get Poll Rankings", **READ_ONLY_ANNOTATIONS})
async def get_rankings(
    season: Annotated[int, Field(description="Season year, e.g. 2024.")],
    week: Annotated[
        int | None,
        Field(
            default=None,
            description="Week number. Omit to get every week of the season "
            "(subject to the 100-row cap, so prefer pairing with `poll`).",
        ),
    ] = None,
    poll: Annotated[
        str | None,
        Field(
            default=None,
            description="Exact poll name, e.g. 'AP Top 25', 'Coaches Poll', "
            "'Playoff Committee Rankings'. Omit to get all polls for the given week(s).",
        ),
    ] = None,
    season_type: Annotated[
        PollSeasonType,
        Field(
            default=PollSeasonType.REGULAR,
            description="'regular' (default) for weekly "
            "in-season polls, or 'postseason' for the final poll of the season. CFBD "
            "reports the final poll as week=1, the same week number as the regular-season "
            "week-1 poll -- season_type is what tells them apart.",
        ),
    ] = PollSeasonType.REGULAR,
    limit: Annotated[
        int, Field(default=DEFAULT_ROW_CAP, ge=1, le=DEFAULT_ROW_CAP)
    ] = DEFAULT_ROW_CAP,
) -> str:
    """Get weekly or final poll rankings (AP Top 25, Coaches Poll, CFP committee, etc).

    When to use: "who was #1 in the AP poll in week 8 of 2024", "show the
    final CFP rankings for 2023", "was Oklahoma ranked in week 3".

    Backed by api.poll_rankings. Rows are ordered week, poll, rank ascending.

    Caveats (important for correct interpretation):
      - Tied teams share the same rank value, and the next rank is skipped
        (e.g. two teams at #11 means no #12 that week) -- do not assume rank
        values are contiguous or that one row per rank exists.
      - To get the END-OF-SEASON final poll, set season_type='postseason'
        (week is reported as 1, identical to the regular-season week-1 poll's
        week number -- season_type is the only disambiguator).
      - Omitting both `week` and `poll` for a full season can return a lot of
        rows (many weeks x several polls x ~25 teams); the 100-row cap may
        truncate results, so prefer narrowing with `poll` and/or `week`.

    Returns: JSON {"_source": "api.poll_rankings", "count": int, "rows": [...]},
    or a plain "No rankings found..." string if nothing matches.
    """
    params: dict[str, Any] = {"season": eq(season), "season_type": eq(season_type.value)}
    if week is not None:
        params["week"] = eq(week)
    if poll is not None:
        params["poll"] = eq(poll)
    params["order"] = "week.asc,poll.asc,rank.asc"

    client = PostgrestClient()
    try:
        rows = await client.select("poll_rankings", params, profile="api", limit=limit)
    except PostgrestError as e:
        return e.message

    if not rows:
        return (
            f"No rankings found for season={season}, season_type={season_type.value} with "
            "the given filters."
        )
    return _dump(_wrap("api.poll_rankings", rows))


# ---------------------------------------------------------------------
# 5. get_leaderboard
# ---------------------------------------------------------------------


@mcp.tool(
    name="get_leaderboard", annotations={"title": "Get Team Leaderboard", **READ_ONLY_ANNOTATIONS}
)
async def get_leaderboard(
    season: Annotated[int, Field(description="Season year, e.g. 2024.")],
    metric: Annotated[
        LeaderboardMetric,
        Field(
            description=(
                "Ranking metric: 'wins' (most wins), 'ppg' (points per game), "
                "'scoring_defense' (fewest points allowed per game), 'epa' (EPA/play), "
                "'sp_rating' (best SP+ rank), or 'wepa' (opponent-adjusted EPA -- pulled "
                "from a different, more advanced view, api.team_wepa_season)."
            )
        ),
    ],
    limit: Annotated[
        int,
        Field(
            default=DEFAULT_ROW_CAP,
            ge=1,
            le=DEFAULT_ROW_CAP,
            description="Max rows. "
            "Capped at 100; there are ~130 FBS teams so a full-season list may be "
            "truncated -- lower this or treat results as top-N, not exhaustive.",
        ),
    ] = DEFAULT_ROW_CAP,
) -> str:
    """Get a ranked leaderboard of teams for a season by a chosen metric.

    When to use: "top 10 teams by EPA in 2024", "best scoring defense last
    season", "who led the country in wins".

    All metrics except 'wepa' are served from api.leaderboard_teams, which
    pre-computes rank columns (wins_rank, ppg_rank, defense_ppg_rank,
    epa_rank) via SQL window functions -- ties are possible. 'wepa' (opponent-
    adjusted EPA) is served from the separate api.team_wepa_season view,
    which has its own epa_rank column.

    Returns: JSON {"_source": "<view>", "count": int, "rows": [...]}, ordered
    best-to-worst for the chosen metric, or a plain "No leaderboard data
    found..." string if the season has no data.
    """
    client = PostgrestClient()
    try:
        if metric == LeaderboardMetric.WEPA:
            rows = await client.select(
                "team_wepa_season",
                {"season": eq(season), "order": "epa_rank.asc"},
                profile="api",
                limit=limit,
            )
            source = "api.team_wepa_season"
        else:
            rows = await client.select(
                "leaderboard_teams",
                {"season": eq(season), "order": _LEADERBOARD_ORDER[metric]},
                profile="api",
                limit=limit,
            )
            source = "api.leaderboard_teams"
    except PostgrestError as e:
        return e.message

    if not rows:
        return f"No leaderboard data found for season={season}."
    return _dump(_wrap(source, rows))


# ---------------------------------------------------------------------
# 6. situational_splits
# ---------------------------------------------------------------------


@mcp.tool(
    name="situational_splits",
    annotations={"title": "Get Situational Splits", **READ_ONLY_ANNOTATIONS},
)
async def situational_splits(
    team: Annotated[str, Field(description="Exact school name.")],
    season: Annotated[int, Field(description="Season year, e.g. 2024.")],
    split_type: Annotated[
        SplitType,
        Field(
            description=(
                "Which breakdown to compute: 'home_away' (home vs away performance), "
                "'conference' (conference vs non-conference opponents), 'red_zone' "
                "(trips inside the opponent 20: TD/FG/turnover rates), 'down_distance' "
                "(success rate/EPA by down and distance bucket), or 'field_position' "
                "(EPA/success rate by field-position zone)."
            )
        ),
    ],
) -> str:
    """Get a team's situational performance splits for a season.

    When to use: "how does Oklahoma perform on 3rd down", "home vs away splits
    for Oklahoma in 2023", "red zone efficiency", "conference vs non-conference
    performance".

    Fans out to one of five public RPCs based on split_type:
    get_home_away_splits, get_conference_splits, get_red_zone_splits,
    get_down_distance_splits, get_field_position_splits -- each called as
    (p_team=team, p_season=season). All five exclude garbage-time plays.

    Caveats:
      - Play-by-play data (needed for down_distance/field_position/red_zone/
        home_away/conference splits) is available from the 2014 season on;
        earlier seasons will return empty or partial results.
      - Each RPC returns a small breakdown table (rows keyed by side
        offense/defense, or by zone/bucket) -- this is not row-capped since
        results are inherently small.

    Returns: JSON {"_source": "public.<rpc_name>", "count": int, "rows": [...]},
    or a plain "No <split_type> splits found..." string if the team/season
    has no matching plays.
    """
    fn = _SPLIT_RPCS[split_type]
    client = PostgrestClient()
    try:
        rows = await client.rpc(fn, {"p_team": team, "p_season": season}, profile="public")
    except PostgrestError as e:
        return e.message

    if not rows:
        return (
            f"No {split_type.value} splits found for '{team}' in {season}. Check the team "
            "name and that the season has play-by-play data (2014+)."
        )
    return _dump(_wrap(f"public.{fn}", rows))


# ---------------------------------------------------------------------
# 7. search_players
# ---------------------------------------------------------------------


@mcp.tool(name="search_players", annotations={"title": "Search Players", **READ_ONLY_ANNOTATIONS})
async def search_players(
    query: Annotated[
        str,
        Field(
            description=(
                "Player name to search, full or partial, typo-tolerant (trigram similarity "
                "match). E.g. 'Caleb Williams', 'Bijan', or a misspelling like 'Calib "
                "Williams'."
            )
        ),
    ],
    team: Annotated[
        str | None, Field(default=None, description="Restrict search to an exact school name.")
    ] = None,
    season: Annotated[
        int | None, Field(default=None, description="Restrict search to a season year.")
    ] = None,
    limit: Annotated[
        int,
        Field(
            default=25,
            ge=1,
            le=DEFAULT_ROW_CAP,
            description="Max search results (default 25, hard-capped at 100).",
        ),
    ] = 25,
) -> str:
    """Search for a player by name, then fetch full detail for the best match.

    When to use: "find Caleb Williams' stats", "search for a player named
    Bijan on Texas" -- anytime the caller has a name but not an exact
    player_id.

    Two-step workflow, both via public RPCs:
      1. get_player_search(p_query, p_team, p_season, p_limit) -- fuzzy name
         match via pg_trgm, ranked by similarity_score descending.
      2. get_player_detail(p_player_id, p_season) is then called automatically
         for the single top-ranked hit, returning full bio/recruiting/season
         stats/PPA/WEPA/PAAR for that player.

    Caveats:
      - If multiple players share a similar name, only the top hit gets full
        detail -- inspect the "search" rows to see other candidates and call
        search_players again with a more specific query/team/season if the
        top hit isn't the right one.
      - If `season` is omitted, get_player_detail returns that player's most
        recent season on record, which may not be the season implied by the
        query.

    Returns: JSON with "search" ({"_source": "public.get_player_search", ...})
    and "top_hit_detail" ({"_source": "public.get_player_detail", ...}) keys.
    If the detail lookup itself fails, returns "search" plus a
    "top_hit_detail_error" string instead of discarding the search results.
    Returns a plain "No players found..." string if the search itself is empty.
    """
    client = PostgrestClient()
    args: dict[str, Any] = {"p_query": query, "p_limit": limit}
    if team is not None:
        args["p_team"] = team
    if season is not None:
        args["p_season"] = season

    try:
        results = await client.rpc("get_player_search", args, profile="public")
    except PostgrestError as e:
        return e.message

    if not results:
        return f"No players found matching '{query}'."

    top = results[0]
    detail_args: dict[str, Any] = {"p_player_id": top["player_id"]}
    if season is not None:
        detail_args["p_season"] = season

    try:
        detail_rows = await client.rpc("get_player_detail", detail_args, profile="public")
    except PostgrestError as e:
        return _dump(
            {
                "search": _wrap("public.get_player_search", results),
                "top_hit_detail_error": e.message,
            }
        )

    return _dump(
        {
            "search": _wrap("public.get_player_search", results),
            "top_hit_detail": _wrap("public.get_player_detail", detail_rows),
        }
    )


# ---------------------------------------------------------------------
# 8. get_data_freshness
# ---------------------------------------------------------------------


@mcp.tool(
    name="get_data_freshness", annotations={"title": "Get Data Freshness", **READ_ONLY_ANNOTATIONS}
)
async def get_data_freshness() -> str:
    """Get freshness/staleness status for all tracked warehouse tables.

    When to use: before answering questions about very recent games/stats,
    to qualify how current the data is -- e.g. "as of the last refresh (X
    days ago), ...". Also useful if a query returns unexpectedly few/no rows
    for the current week, to check whether the pipeline has run yet.

    Takes no arguments. Backed by the public.get_data_freshness() RPC, which
    reports row_count, expected_refresh_frequency, days_since_activity, and
    is_stale for each of ~23 tracked tables, ordered stale-first.

    Returns: JSON {"_source": "public.get_data_freshness", "count": int,
    "rows": [...]}.
    """
    client = PostgrestClient()
    try:
        rows = await client.rpc("get_data_freshness", {}, profile="public")
    except PostgrestError as e:
        return e.message
    return _dump(_wrap("public.get_data_freshness", rows))
