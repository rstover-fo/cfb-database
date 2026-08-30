"""Parsers for the NCAA (stats.ncaa.org) bundle (B6a), sourced from
sportsdataverse-data's ``ncaa_mfb_*`` GitHub release tags -- one parquet per
season per dataset, same per-season-file shape as the `sdv_*` sources in
``sportsdataverse.py`` (``url_template`` + ``fallback_latest=True`` in the
registry). All seven datasets were downloaded (2025 + 2013, the earliest
published season) and inspected directly with pyarrow; findings below are
from that inspection, not API docs.

Datasets (tag -> table; all in the new, deliberately ungranted ``ncaa``
Postgres schema -- migration 053):
- ncaa_mfb_schedule      -> ncaa.schedule      (PK season, ncaa_contest_id, ncaa_team_id)
- ncaa_mfb_teams         -> ncaa.teams         (PK season, ncaa_team_id)
- ncaa_mfb_rosters       -> ncaa.rosters       (PK season, ncaa_team_id, ncaa_player_id)
- ncaa_mfb_linescore     -> ncaa.linescores    (PK season, ncaa_contest_id, team_name, period)
- ncaa_mfb_player_stats  -> ncaa.player_stats  (PK season, ncaa_contest_id, ncaa_team_id,
                                                 player_number, category, row_seq)
- ncaa_mfb_team_stats    -> ncaa.team_stats    (PK season, ncaa_contest_id, category, stat,
                                                 period, row_seq)
- ncaa_mfb_pbp           -> ncaa.pbp           (PK season, ncaa_contest_id, drive_number,
                                                 play_number)

Season coverage: 2013-2025 for every tag at write time (stats.ncaa.org has no
football data before 2013). 2026 is not yet published on any tag (confirmed
2026-08-29) -- every registry entry uses ``fallback_latest=True`` so `--due`
runs fall back to 2025 until it lands, self-healing per the framework
described in ``flat_files.py``'s module docstring.

pbp size call: the 2025-season ``ncaa_mfb_pbp`` parquet is ~11.8MB (measured
via a real download, not the release page's aggregate "388.6MB total across
all 13 seasons x 3 formats" figure) -- far under the ~200MB single-file scope
threshold, so pbp is INCLUDED, not commented out.

**ID-space isolation is this schema's reason to exist.** stats.ncaa.org's own
contest/team/player ids are a namespace completely disjoint from CFBD/ESPN's
-- there is no verified crosswalk (unlike the sdv_* xwalk/ratings sources in
``sportsdataverse.py``, whose numeric ids were verified equal to ESPN's).
Every bare, file-native id column that could otherwise invite an accidental
join is renamed with an ``ncaa_`` prefix here (``contest_id`` ->
``ncaa_contest_id``, ``team_id`` -> ``ncaa_team_id``, ``player_id`` ->
``ncaa_player_id``, ``opponent_id`` -> ``ncaa_opponent_id``). Name columns
(``team_name``, ``player_name``, ...) are left as-is -- they cannot
accidentally numeric-join.

**``ncaa_team_id`` and ``ncaa_player_id`` are RE-ISSUED EVERY SEASON --
verified, not assumed.** Comparing the 2013 and 2025 ``ncaa_mfb_teams``
files by team name: Alabama's team_id is 62682 in 2013 and 606070 in 2025 (a
matching pattern holds for every team checked: Ohio St. 62757 -> 606012,
Georgia 62717 -> 606105, Michigan 62739 -> 605993, Air Force 62793 ->
606051). The same holds for players: cross-referencing 82 players who
appear on both Alabama's 2024 and 2025 ``ncaa_mfb_rosters`` by name, EVERY
one carries a different ``player_id`` between the two seasons (e.g. Vito
Perri: 8626995 in 2024, 9240713 in 2025). Consequence: ``(season,
ncaa_team_id)`` and ``(season, ncaa_player_id)`` are the only safe join
keys within this schema -- never join across seasons on the bare id alone,
and never assume a stable "this is the same team/player" identity without
going through team_name/player_name (themselves imperfect: school
abbreviations and player-name spelling can still vary).

**Bridge column kept for the future FCS-join path:** ``espn_game_id``
appears (as a nullable numeric-string column) in ncaa_mfb_schedule,
ncaa_mfb_linescore, and ncaa_mfb_pbp -- kept verbatim (cast to bigint, no
rename needed since the name is already self-namespaced) as the join key a
future crosswalk to core.games would use. None of the other four datasets
(teams, rosters, player_stats, team_stats) carry any ESPN/CFBD id at all.
Notably **ncaa_mfb_player_stats has no player id column whatsoever** --
only jersey ``number`` (renamed ``player_number`` below) and ``name`` --
so there is no clean join from player_stats to ncaa.rosters either; only a
fragile (team, jersey-number-that-game, name) heuristic, not implemented
here.

**stats.ncaa.org data-quality quirks handled defensively:**
- Numeric stat values ship as strings with thousand-separator commas in rare
  high-magnitude cases (e.g. a tiny-sample-size game's ``pass_eff`` of
  ``"1,043.20"``) -- ``_to_float()`` strips commas before casting.
- ``ncaa_mfb_player_stats``' per-category player box score is itself a
  concatenation of several physical stats.ncaa.org sub-tables: the same
  (contest, team, jersey number, category) key can legitimately repeat with
  disjoint populated columns (e.g. category="other" holds both a "total
  offense" sub-row -- ``yds``/``plays`` -- and a "kickoff returns" sub-row --
  ``ko_ret``/``ko_ret_yds``/... -- for the same player, verified: Ethan Loss,
  jersey 19, contest 6386278). Team-total rows carry a null jersey number
  (``name`` is literally "TEAM" or the school name) -- coalesced to the
  sentinel ``"TEAM"`` in ``player_number`` rather than dropped, since these
  are real team-aggregate data, not garbage (~20% of all rows in the 2025
  file). Both effects mean (contest, team, player_number, category) is NOT
  unique -- a synthetic ``row_seq`` (0-based, per-key encounter order) is
  added to make the PK deterministic without dropping any row.
- ``ncaa_mfb_team_stats`` has a confirmed upstream data-entry quirk in
  overtime games: an "1stOT"-style label leaks into the ``stat`` column
  instead of (or in addition to) ``period``, producing genuine duplicate
  (contest, category, stat, period) keys (verified: contest 6386336, a
  Temple @ Tulsa OT game, has 6 rows for category="Rushing", stat="1stOT",
  period="total" with different values). Same fix: a per-key ``row_seq``.
- Schema drift across seasons: 2013's ``ncaa_mfb_player_stats`` file has
  ONLY the 8 identity columns (no stat columns at all -- every row's
  category is "other" with nothing else populated); the ~40 stat columns
  only exist in later, richer seasons. Parsers here use ``row.get(col)``
  throughout rather than assuming a fixed column set, so a sparser
  historical season loads the identity columns and leaves the rest NULL
  (dlt evolves the pre-created migration-053 columns for whichever season's
  richer file loads later -- same coexistence note as migrations 041/052).
- Two rows in the 2025 ``ncaa_mfb_schedule`` file have a null
  ``contest_id`` (two Week 0 games that were canceled and never got a
  stats.ncaa.org box score) -- dropped with a log count, same
  null-PK-drop-and-count pattern as nflverse.py/sportsdataverse.py.

Read with ``pyarrow.parquet.read_table(io.BytesIO(raw))`` -- pandas is not a
dependency, per the nflverse.py/sportsdataverse.py precedent.
"""

import io
import logging
from collections.abc import Iterator
from datetime import date, datetime

import pyarrow.parquet

from ..flat_files import ParseContext, ParserStructureError

logger = logging.getLogger(__name__)

# Jersey number sentinel for player_stats' null-number "team total" rows
# (~20% of rows in the 2025 file) -- see the module docstring.
TEAM_ROW_SENTINEL = "TEAM"

# Every stat value column observed in the richest (2025) ncaa_mfb_player_stats
# file, coerced defensively with _to_float(); older/sparser seasons simply
# lack some or all of these keys (row.get() handles that safely).
PLAYER_STAT_COLUMNS = (
    "rush_attempts",
    "rush_yds_gained",
    "rush_yds_lost",
    "yds_rush",
    "rush_tds",
    "rush_long",
    "pass_attempts",
    "completions",
    "pass_yards",
    "interceptions",
    "pass_tds",
    "pass_eff",
    "yds_per_completion",
    "pct",
    "long_pass",
    "rec",
    "receiving_yards",
    "yards_per_reception",
    "rec_td",
    "long_rec",
    "yds",
    "plays",
    "pbu",
    "int",
    "intyds",
    "int_ret_tds",
    "pdef",
    "ko_ret",
    "ko_ret_yds",
    "kick_ret_tds",
    "long_kor",
    "sacks",
    "solo_tack",
    "asst_tack",
    "tackles",
    "fgm",
    "fga",
    "fg_blocks_allowed",
    "punt_ret",
    "punt_ret_yds",
    "punt_ret_tds",
    "long_pr",
)


def _blank_to_none(value):
    """Treat an empty or whitespace-only string as a missing value, same as
    None. stats.ncaa.org's parquet exports ship ``''`` for some missing
    numeric-ish fields (confirmed: ``ncaa_mfb_rosters_2025``'s ``height``
    column has blank-string rows, which crashed ``float('')`` in
    ``_height_inches`` -- ledger row ``ncaa_rosters:2025``) rather than a
    proper null. Non-string values (including already-None) pass through
    unchanged.
    """
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


def _to_float(value) -> float | None:
    """Coerce a stats.ncaa.org numeric value to float, tolerating None and
    blank strings.

    Defensive against the observed comma-thousands-separator quirk (e.g.
    ``"1,043.20"``) -- strips commas from strings before casting.
    """
    value = _blank_to_none(value)
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace(",", "")
    return float(value)


def _to_int(value) -> int | None:
    """Coerce a stats.ncaa.org numeric-string id column to int, tolerating
    None and blank strings."""
    value = _blank_to_none(value)
    if value is None:
        return None
    return int(value)


def _height_inches(value: str | None) -> float | None:
    """ "6-0" (feet-inches, stats.ncaa.org's roster height format) -> 72.0
    inches. Tolerates None and blank strings (see ``_blank_to_none``)."""
    value = _blank_to_none(value)
    if value is None:
        return None
    if isinstance(value, str) and "-" in value:
        feet, _, inches = value.partition("-")
        return float(feet) * 12 + float(inches)
    return float(value)


def _parse_mmddyyyy(value: str | None) -> date | None:
    """ "08/29/2025" -> date(2025, 8, 29)."""
    if value is None:
        return None
    return datetime.strptime(value, "%m/%d/%Y").date()


def _parse_game_date(value: str | None) -> date | None:
    """The date portion of "MM/DD/YYYY HH:MM AM/PM" (ncaa_mfb_linescore's
    ``game_date``). The time-of-day is local-venue time with no timezone
    information in the source -- dropped rather than fabricating a
    timezone-aware timestamp; only the date is kept.
    """
    if value is None:
        return None
    date_part = value.split(" ", 1)[0]
    return datetime.strptime(date_part, "%m/%d/%Y").date()


def _assign_row_seq(rows: list[dict], key_fields: tuple[str, ...]) -> None:
    """Assign a 0-based ``row_seq`` to each row, counting occurrences of
    ``key_fields`` in first-seen order. Mutates ``rows`` in place.

    Exists because stats.ncaa.org's per-category player/team box scores are
    themselves concatenations of several physical sub-tables that can repeat
    the same key with disjoint populated columns (confirmed real cases in
    ncaa_mfb_player_stats and ncaa_mfb_team_stats -- see the module
    docstring). Without this, dlt's merge write disposition can fail
    outright on an in-batch duplicate primary key (Postgres: "ON CONFLICT
    DO UPDATE command cannot affect row a second time"), or silently keep
    only one of several genuinely distinct rows.
    """
    counters: dict[tuple, int] = {}
    for row in rows:
        key = tuple(row.get(f) for f in key_fields)
        seq = counters.get(key, 0)
        row["row_seq"] = seq
        counters[key] = seq + 1


def _require_columns(dataset: str, schema_names: set[str], required: set[str]) -> None:
    missing = required - schema_names
    if missing:
        raise ParserStructureError(f"{dataset}: missing column(s): {sorted(missing)}")


def parse_schedule(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse ncaa_mfb_schedule_{season}.parquet into ncaa.schedule rows.

    Real columns (14): team_id, team_name, date, opponent_id, opponent,
    result, outcome, team_score, opponent_score, contest_id, attendance,
    academic_year, espn_game_id, season. One row per team per contest (a
    completed game has exactly 2 rows sharing one contest_id).

    PK: (season, ncaa_contest_id, ncaa_team_id) -- verified 0 duplicates
    across 3314 rows in the 2025 file. 2 rows in the 2025 file have a null
    contest_id (two Week 0 games canceled before a box score existed) --
    dropped with a log count. ``date`` -> ``game_date`` (parsed
    "MM/DD/YYYY"). ``team_id``/``opponent_id``/``contest_id`` are
    stats.ncaa.org's own ids, re-issued every season (see module docstring)
    -- renamed with an ``ncaa_`` prefix. ``espn_game_id`` is a bridge column
    to ESPN/CFBD, kept as-is (~2% null in the 2025 file; entirely absent as
    a column in the 2013 file -- schema drift, not a parser concern).
    ``team_name``/``opponent`` carry embedded season-end annotations (e.g.
    "Kennesaw St. Owls (10-4) *Myrtle Beach Bowl") -- kept raw, not cleaned.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)
    _require_columns("ncaa_schedule", schema_names, {"contest_id", "team_id", "season"})

    rows = table.to_pylist()
    dropped_count = 0
    out = []

    for row in rows:
        if (
            _blank_to_none(row.get("contest_id")) is None
            or _blank_to_none(row.get("team_id")) is None
            or row.get("season") is None
        ):
            dropped_count += 1
            continue

        row["ncaa_contest_id"] = _to_int(row.pop("contest_id"))
        row["ncaa_team_id"] = _to_int(row.pop("team_id"))
        row["ncaa_opponent_id"] = _to_int(row.pop("opponent_id", None))
        row["game_date"] = _parse_mmddyyyy(row.pop("date", None))
        if row.get("espn_game_id") is not None:
            row["espn_game_id"] = _to_int(row["espn_game_id"])

        out.append(row)

    if dropped_count > 0:
        logger.info(f"ncaa_schedule: dropped {dropped_count} row(s) with null PK column(s)")

    yield from out


def parse_teams(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse ncaa_mfb_teams_{season}.parquet into ncaa.teams rows.

    Real columns (5): team_id, team_name, academic_year, division, season.

    PK: (season, ncaa_team_id) -- verified 0 duplicates across 265 rows in
    the 2025 file. ``division`` is stats.ncaa.org's own raw numeric division
    code, kept as-is (not decoded to "FBS"/"FCS" text): observed values are
    11 (136 teams in 2025, 128 in 2013) and 12 (129 teams in 2025, 124 in
    2013) -- counts consistent with FBS/FCS membership across those years,
    but this is an inference from team counts, not a documented NCAA code
    table, so it is not asserted as fact in the schema.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)
    _require_columns("ncaa_teams", schema_names, {"team_id", "season"})

    rows = table.to_pylist()
    dropped_count = 0
    out = []

    for row in rows:
        if _blank_to_none(row.get("team_id")) is None or row.get("season") is None:
            dropped_count += 1
            continue

        row["ncaa_team_id"] = _to_int(row.pop("team_id"))
        out.append(row)

    if dropped_count > 0:
        logger.info(f"ncaa_teams: dropped {dropped_count} row(s) with null PK column(s)")

    yield from out


def parse_rosters(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse ncaa_mfb_rosters_{season}.parquet into ncaa.rosters rows.

    Real columns (16): team_id, team_name, player_id, player_name, jersey,
    statcrew_jersey, player_class, position, height, weight, hometown,
    high_school, games_played, games_started, academic_year, season.

    PK: (season, ncaa_team_id, ncaa_player_id) -- verified 0 duplicates
    across 29206 rows in the 2025 file. ``height`` ("6-0" feet-inches) ->
    ``height_inches`` (float). ``ncaa_player_id`` is re-issued every season,
    same as ``ncaa_team_id`` -- verified by cross-referencing 82 players
    common to Alabama's 2024 and 2025 rosters by name: every one carries a
    different id between the two seasons (see module docstring).

    ``height`` ships blank (``""``) for some rows (confirmed live:
    ``ncaa_mfb_rosters_2025``) rather than null -- ``_height_inches`` treats
    a blank string as missing and yields ``None``, not a crash.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)
    _require_columns("ncaa_rosters", schema_names, {"team_id", "player_id", "season"})

    rows = table.to_pylist()
    dropped_count = 0
    out = []

    for row in rows:
        if (
            _blank_to_none(row.get("team_id")) is None
            or _blank_to_none(row.get("player_id")) is None
            or row.get("season") is None
        ):
            dropped_count += 1
            continue

        row["ncaa_team_id"] = _to_int(row.pop("team_id"))
        row["ncaa_player_id"] = _to_int(row.pop("player_id"))
        row["height_inches"] = _height_inches(row.pop("height", None))
        out.append(row)

    if dropped_count > 0:
        logger.info(f"ncaa_rosters: dropped {dropped_count} row(s) with null PK column(s)")

    yield from out


def parse_linescores(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse ncaa_mfb_linescore_{season}.parquet into ncaa.linescores rows.

    Real columns (11): contest_id, team, home_away, period, points, final,
    game_date, venue, attendance, espn_game_id, season. No team-id column at
    all -- only a team name (``team``, renamed ``team_name``).

    PK: (season, ncaa_contest_id, team_name, period) -- verified 0
    duplicates across 13680 rows in the 2025 file. Safe despite ``team_name``
    being a name, not an id: a single contest's two team names never collide
    with EACH OTHER within that same contest_id, which is all uniqueness
    here requires. ``period`` values observed: "1"-"4" plus overtime labels
    "1OT".."5OT". ``final`` (the game's total final score, repeated on every
    period row) -> ``final_score`` (avoids ambiguity with "the final
    period"). ``game_date`` carries a time-of-day with no timezone -- only
    the date portion is kept (see ``_parse_game_date``).
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)
    _require_columns("ncaa_linescores", schema_names, {"contest_id", "team", "period", "season"})

    rows = table.to_pylist()
    dropped_count = 0
    out = []

    for row in rows:
        if (
            _blank_to_none(row.get("contest_id")) is None
            or row.get("team") is None
            or row.get("period") is None
            or row.get("season") is None
        ):
            dropped_count += 1
            continue

        row["ncaa_contest_id"] = _to_int(row.pop("contest_id"))
        row["team_name"] = row.pop("team")
        row["final_score"] = row.pop("final", None)
        row["game_date"] = _parse_game_date(row.pop("game_date", None))
        if row.get("espn_game_id") is not None:
            row["espn_game_id"] = _to_int(row["espn_game_id"])

        out.append(row)

    if dropped_count > 0:
        logger.info(f"ncaa_linescores: dropped {dropped_count} row(s) with null PK column(s)")

    yield from out


def parse_player_stats(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse ncaa_mfb_player_stats_{season}.parquet into ncaa.player_stats rows.

    Real columns in the 2025 file: 8 identity columns (contest_id, team_id,
    number, name, position, category, espn_game_id, season) plus 42 stat
    value columns (``PLAYER_STAT_COLUMNS``), ALL shipped as strings (coerced
    here via ``_to_float``, which also strips the observed comma-thousands
    quirk). The 2013 file has ONLY the 8 identity columns -- see module
    docstring's schema-drift note.

    PK: (season, ncaa_contest_id, ncaa_team_id, player_number, category,
    row_seq). Real duplicates confirmed at (contest, team, number, category)
    grain -- see module docstring -- so ``row_seq`` (0-based, per-key
    encounter order) is required to make the key unique; it is NOT a
    meaningful sort order on its own. ``number`` (jersey) is null for ~20%
    of rows (team-total rows, ``name`` = "TEAM" or the school name) --
    coalesced to the sentinel ``player_number = "TEAM"`` rather than
    dropped, since these carry real team-aggregate data. There is no player
    id column in this file at all (only jersey number + name) -- this table
    cannot be joined to ncaa.rosters by a stable id.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)
    _require_columns(
        "ncaa_player_stats", schema_names, {"contest_id", "team_id", "category", "season"}
    )

    rows = table.to_pylist()
    dropped_count = 0
    out = []

    for row in rows:
        if (
            _blank_to_none(row.get("contest_id")) is None
            or _blank_to_none(row.get("team_id")) is None
            or row.get("category") is None
            or row.get("season") is None
        ):
            dropped_count += 1
            continue

        row["ncaa_contest_id"] = _to_int(row.pop("contest_id"))
        row["ncaa_team_id"] = _to_int(row.pop("team_id"))
        number = row.pop("number", None)
        row["player_number"] = TEAM_ROW_SENTINEL if number is None else number
        if row.get("espn_game_id") is not None:
            row["espn_game_id"] = _to_int(row["espn_game_id"])

        for col in PLAYER_STAT_COLUMNS:
            if col in row and row[col] is not None:
                row[col] = _to_float(row[col])

        out.append(row)

    if dropped_count > 0:
        logger.info(f"ncaa_player_stats: dropped {dropped_count} row(s) with null PK column(s)")

    _assign_row_seq(out, ("ncaa_contest_id", "ncaa_team_id", "player_number", "category"))

    yield from out


def parse_team_stats(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse ncaa_mfb_team_stats_{season}.parquet into ncaa.team_stats rows.

    Real columns (10): contest_id, category, stat, period, away_team,
    away_value, home_team, home_value, espn_game_id, season. Long format --
    each row is one named stat for one period of one game, with both teams'
    values on the same row. ``away_value``/``home_value`` ship as strings
    (coerced via ``_to_float``; the observed comma-thousands quirk -- a
    "1,051.60" Pass Eff value -- confirmed here too).

    PK: (season, ncaa_contest_id, category, stat, period, row_seq).
    Confirmed upstream data-entry quirk in overtime games: an "1stOT"-style
    value leaks into the ``stat`` column, producing real duplicate (contest,
    category, stat, period) keys (verified: contest 6386336, Temple @ Tulsa,
    6 rows for category="Rushing"/stat="1stOT"/period="total" with different
    values) -- ``row_seq`` (0-based, per-key encounter order) makes the key
    deterministic without dropping any row.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)
    _require_columns(
        "ncaa_team_stats", schema_names, {"contest_id", "category", "stat", "period", "season"}
    )

    rows = table.to_pylist()
    dropped_count = 0
    out = []

    for row in rows:
        if (
            _blank_to_none(row.get("contest_id")) is None
            or row.get("category") is None
            or row.get("stat") is None
            or row.get("period") is None
            or row.get("season") is None
        ):
            dropped_count += 1
            continue

        row["ncaa_contest_id"] = _to_int(row.pop("contest_id"))
        if row.get("away_value") is not None:
            row["away_value"] = _to_float(row["away_value"])
        if row.get("home_value") is not None:
            row["home_value"] = _to_float(row["home_value"])
        if row.get("espn_game_id") is not None:
            row["espn_game_id"] = _to_int(row["espn_game_id"])

        out.append(row)

    if dropped_count > 0:
        logger.info(f"ncaa_team_stats: dropped {dropped_count} row(s) with null PK column(s)")

    _assign_row_seq(out, ("ncaa_contest_id", "category", "stat", "period"))

    yield from out


def parse_pbp(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse ncaa_mfb_pbp_{season}.parquet into ncaa.pbp rows.

    Real columns (51): contest_id, drive_number, play_number, offense,
    drive_result, drive_scored, down, distance, yard_line, yard_line_side,
    yard_line_number, play_type, clock, yards_gained, formation, passer,
    rusher, receiver, kicker, punter, returner, run_direction, qb_scramble,
    pass_complete, pass_depth, pass_direction, tackler_1, tackler_2,
    kick_yards, return_yards, punt_yards, fg_distance, fg_made,
    is_first_down, is_touchdown, is_safety, is_fumble, is_turnover,
    turnover_type, out_of_bounds, no_play, fair_catch, penalty_flag,
    penalty_team, penalty_type, penalty_player, penalty_yards,
    end_yard_line, play_text, espn_game_id, season. Numeric/boolean columns
    are already natively typed in the parquet (int64/bool) -- no
    string-numeric coercion needed here, unlike player_stats/team_stats.

    PK: (season, ncaa_contest_id, drive_number, play_number) -- verified 0
    duplicates and 0 nulls across 353879 rows in the 2025 file. ~11.8MB for
    the 2025 file (measured via direct download) -- well under the ~200MB
    single-file scope threshold, so this dataset is included.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)
    _require_columns(
        "ncaa_pbp", schema_names, {"contest_id", "drive_number", "play_number", "season"}
    )

    rows = table.to_pylist()
    dropped_count = 0
    out = []

    for row in rows:
        if (
            _blank_to_none(row.get("contest_id")) is None
            or row.get("drive_number") is None
            or row.get("play_number") is None
            or row.get("season") is None
        ):
            dropped_count += 1
            continue

        row["ncaa_contest_id"] = _to_int(row.pop("contest_id"))
        if row.get("espn_game_id") is not None:
            row["espn_game_id"] = _to_int(row["espn_game_id"])

        out.append(row)

    if dropped_count > 0:
        logger.info(f"ncaa_pbp: dropped {dropped_count} row(s) with null PK column(s)")

    yield from out
