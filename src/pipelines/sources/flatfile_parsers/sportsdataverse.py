"""Parsers for sportsdataverse-data parquet releases (B2 items 1-2).

Sources (weekly, per-season GitHub release assets -- see the deviation note
below):
- cfb_teams_crosswalk_{season}.parquet    -> ref.team_id_xwalk (PK season, norm_key)
- cfb_schedule_crosswalk_{season}.parquet -> ref.game_id_xwalk
  (PK season, matchup_key, yahoo_date)
- cfb_fpi_weekly_{season}.parquet         -> ratings.fpi_weekly
  (PK season, season_type, week, team_id)
- cfb_ratings_weekly_{season}.parquet     -> ratings.external_weekly
  (PK season, through_week, team_id)

**Deviation from the nflverse pattern these parsers otherwise clone:** nflverse's
combine/draft_picks releases are genuinely cumulative -- one URL, all history,
forever. sportsdataverse's `cfb_crosswalk`/`cfb_fpi_weekly`/`cfb_ratings_weekly`
releases are NOT cumulative: cfbfastR-cfb-data publishes one asset per season
(`{dataset}_{season}.parquet`), confirmed by probing the real GitHub release
assets (docs in the T-task report; no live API access was available to enumerate
them, so this was confirmed by direct-download probing of
github.com/sportsdataverse/sportsdataverse-data/releases/download/<tag>/<asset>).
`FlatFileSpec.fetch_url` is a single fixed string with no season templating, so
the registry entries below are pinned to the 2025 asset (the latest published
at write time -- 2026's had not appeared yet). **This URL will need a manual
bump to `_2026` once cfbfastR-cfb-data publishes it** (their cron runs through
the season); this is a known follow-up, not a bug. Until bumped, these sources
will keep re-fetching 2025 (harmless -- hash-skip no-ops after the first load)
and will NOT pick up 2026 automatically.

**No player/roster crosswalk exists.** The task anticipated a
`cfb_rosters_crosswalk` (or similarly-named) asset for CFBD<->ESPN player
identity. It does not exist: the real `cfb_crosswalk` release tag contains
exactly two datasets -- `cfb_teams_crosswalk` and `cfb_schedule_crosswalk` --
confirmed both by the release's own description text and by exhaustively
probing dozens of plausible asset-name variants (player/roster/athlete x
singular/plural x with/without season suffix), all 404. There is therefore no
`parse_player_xwalk` function here and no `sdv_player_xwalk` registry entry.
See the task report for the fuller finding, including where CFBD<->ESPN player
identity likely already lives (the `espn_cfb_rosters` dataset LEFT-joins CFBD
fields onto ESPN's `athlete_id` directly -- the two providers appear to share
one numeric athlete-id namespace already, so a separate crosswalk table may be
unnecessary for players).

**ID-namespace assumption (report this loudly to whoever reviews joins):**
neither `cfb_teams_crosswalk` nor `cfb_schedule_crosswalk` carries a CFBD id at
all -- both are pure ESPN/Fox/Yahoo crosswalks. The `team_id` in `cfb_fpi_weekly`
and (once cast) `cfb_ratings_weekly` matches `espn_team_id` values in
`cfb_teams_crosswalk` for the same team (verified: Alabama = 333 in both), and
CFBD's own numeric team/game ids are widely documented to equal ESPN's ids
(CFBD scrapes ESPN team/game pages). On that basis these four tables join to
`ref.teams`/`core.games` **directly on the numeric id column, with no
team-name crosswalk (`uses_xwalk=False` throughout)**. This equivalence was
verified against the live warehouse on 2026-08-29: ref.teams.id 333=Alabama /
2483=Oregon match ESPN ids, 2024 core.games ids are ESPN event ids (incl.
401632103), and ESPN-style athlete id 5083552 exists in core.roster.

Read with ``pyarrow.parquet.read_table(io.BytesIO(raw))`` -- pandas is not a
dependency, per nflverse.py's precedent. Column names/types captured from the
real downloaded files are documented per-function below.
"""

import io
import logging
from collections.abc import Iterator
from datetime import date, datetime

import pyarrow.parquet

from ..flat_files import ParseContext, ParserStructureError

logger = logging.getLogger(__name__)


def _parse_iso_date(value: str | None) -> date | None:
    """Parse a "YYYY-MM-DD" string to a date, passing through None."""
    if value is None:
        return None
    return date.fromisoformat(value)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    """ISO-8601 string (e.g. "2025-12-14T09:00Z") -> datetime, passing through None.

    Python 3.11+'s ``datetime.fromisoformat`` accepts the trailing "Z" directly.
    """
    if value is None:
        return None
    return datetime.fromisoformat(value)


def parse_team_xwalk(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse cfb_teams_crosswalk_{season}.parquet into ref.team_id_xwalk rows.

    Real columns (11, none named "season" -- the file is one-per-season and
    carries no season column of its own; ``ctx.season`` is stamped onto every
    row):
        norm_key: large_string (normalized "school mascot" key; 0 nulls, the
            only always-present unique-ish column)
        espn_team_id: int64 (73/828 null in the 2025 file -- fox/yahoo-only
            matches with no ESPN counterpart)
        espn_team, espn_abbreviation: large_string
        fox_team_id, fox_team, fox_abbreviation: large_string
        yahoo_team_id, yahoo_team, yahoo_abbreviation: large_string
        matched_sources: large_string (e.g. "espn+fox+yahoo", "fox", "yahoo")

    PK: (season, norm_key). ``espn_team_id`` cannot be the PK -- see the null
    rate above; rows with a null id are exactly the fox/yahoo-only matches
    the crosswalk exists to carry, dropping them would defeat the source's
    purpose. ``norm_key`` is not perfectly unique either -- the 2025 file has
    one real collision ("roosevelt lakers", two distinct small colleges with
    different espn_team_id values, espn_team_id 599 and 127991) -- this is an
    upstream data-quality edge in sportsdataverse's own matching, not a
    parser bug; it is left as a documented, accepted collision (same class of
    issue as ref.teams' 35 duplicate school names).
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)

    required = {"norm_key"}
    missing = required - schema_names
    if missing:
        raise ParserStructureError(f"sdv_team_xwalk: missing column(s): {sorted(missing)}")
    if ctx.season is None:
        raise ParserStructureError(
            "sdv_team_xwalk: ParseContext.season is required (file carries no season column)"
        )

    rows = table.to_pylist()
    dropped_count = 0

    for row in rows:
        if row.get("norm_key") is None:
            dropped_count += 1
            continue

        row["season"] = ctx.season

        if row.get("espn_team_id") is not None:
            row["espn_team_id"] = int(row["espn_team_id"])

        yield row

    if dropped_count > 0:
        logger.info(f"sdv_team_xwalk: dropped {dropped_count} row(s) with null PK column(s)")


def parse_game_xwalk(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse cfb_schedule_crosswalk_{season}.parquet into ref.game_id_xwalk rows.

    Real columns (11, no season column -- ``ctx.season`` stamped onto every row):
        matchup_key: large_string ("away norm|home norm" pair; 0 nulls, but
            NOT unique alone -- rematches, e.g. conference title-game
            rematches of the regular-season pairing, repeat the same key)
        espn_game_id: int64 (755/1689 null in the 2025 file -- yahoo-only
            matches)
        fox_game_id: large_string (755/1689 null, same rows as espn_game_id)
        yahoo_game_id, yahoo_global_game_id: large_string (0 nulls -- yahoo is
            the one source matched-source-of-record for every row)
        home_team, away_team: large_string (full mascot names, e.g. "Ohio
            State Buckeyes" -- ESPN-style, not CFBD's bare-school spelling)
        espn_date, fox_date: large_string "YYYY-MM-DD" (755 null, same rows)
        yahoo_date: large_string "YYYY-MM-DD" (0 nulls)
        matched_sources: large_string

    PK: (season, matchup_key, yahoo_date) -- yahoo_date disambiguates the
    rematch case above (verified 0 collisions in the 2025 file) and is never
    null, unlike espn_game_id/espn_date.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)

    required = {"matchup_key", "yahoo_date"}
    missing = required - schema_names
    if missing:
        raise ParserStructureError(f"sdv_game_xwalk: missing column(s): {sorted(missing)}")
    if ctx.season is None:
        raise ParserStructureError(
            "sdv_game_xwalk: ParseContext.season is required (file carries no season column)"
        )

    rows = table.to_pylist()
    dropped_count = 0

    for row in rows:
        if row.get("matchup_key") is None or row.get("yahoo_date") is None:
            dropped_count += 1
            continue

        row["season"] = ctx.season

        if row.get("espn_game_id") is not None:
            row["espn_game_id"] = int(row["espn_game_id"])
        row["yahoo_date"] = _parse_iso_date(row["yahoo_date"])
        row["espn_date"] = _parse_iso_date(row.get("espn_date"))
        row["fox_date"] = _parse_iso_date(row.get("fox_date"))

        yield row

    if dropped_count > 0:
        logger.info(f"sdv_game_xwalk: dropped {dropped_count} row(s) with null PK column(s)")


def parse_fpi_weekly(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse cfb_fpi_weekly_{season}.parquet into ratings.fpi_weekly rows.

    Real columns (51; season/season_type/week/team_id all present in-file --
    used as-is, ``ctx.season`` is NOT consulted): season, season_type (2 =
    regular, 3 = postseason -- CFBD's postseason-week-restart convention
    applies here too, hence season_type in the PK), week, team_id (int64,
    0 nulls -- this is ESPN's team_id, which equals ref.teams.id; verified
    against the live warehouse 2026-08-29, 333=Alabama), last_updated (ISO
    datetime string,
    kept and parsed to a real timestamp per the "keep the updated column"
    convention), run_date_time_key (int64, opaque YYYYMMDDHHMMSS-shaped
    snapshot key), snapshot_out_of_sequence / snapshot_is_contemporaneous
    (bool), plus ~40 double-precision FPI/efficiency/rank metric columns
    (fpi, fpirank, projectedw, projectedl, ..., totefficiencyrank).
    ``projectedt`` is an all-null Arrow ``null`` column in the 2025 file (no
    team has ever had a non-null value) -- passed through as None; the
    migration types it double precision on the assumption it would hold a
    number like projectedw/projectedl if ESPN ever populates it.

    PK: (season, season_type, week, team_id) -- verified 0 duplicate keys
    across 2312 rows in the 2025 file.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)

    required = {"season", "season_type", "week", "team_id"}
    missing = required - schema_names
    if missing:
        raise ParserStructureError(f"sdv_fpi_weekly: missing column(s): {sorted(missing)}")

    double_cols = [
        "fpi",
        "fpirank",
        "projectedw",
        "projectedl",
        "projectedt",
        "projectedwpctrank",
        "probwinout",
        "probwinconf",
        "sosremainingrank",
        "accomplishment",
        "accomplishmentrank",
        "adjwins",
        "adjlosses",
        "adjwinpctrank",
        "gamecontrol",
        "gamecontrolrank",
        "adjavgingamewp",
        "adjavgingamewprank",
        "avgingamewp",
        "avgingamewprank",
        "avgsosrank",
        "topsosrank",
        "epaoffense",
        "epadefense",
        "epaspecialteams",
        "probwindiv",
        "probmakeplayoffs",
        "probmaketitlegame",
        "numwins",
        "numlosses",
        "numties",
        "probwintitle",
        "rankchange7days",
        "prob6wins",
        "rank",
        "offefficiency",
        "offefficiencyrank",
        "defefficiency",
        "defefficiencyrank",
        "stefficiency",
        "stefficiencyrank",
        "totefficiency",
        "totefficiencyrank",
    ]

    rows = table.to_pylist()
    dropped_count = 0

    for row in rows:
        if (
            row.get("season") is None
            or row.get("season_type") is None
            or row.get("week") is None
            or row.get("team_id") is None
        ):
            dropped_count += 1
            continue

        row["season"] = int(row["season"])
        row["season_type"] = int(row["season_type"])
        row["week"] = int(row["week"])
        row["team_id"] = int(row["team_id"])

        if row.get("run_date_time_key") is not None:
            row["run_date_time_key"] = int(row["run_date_time_key"])

        row["last_updated"] = _parse_iso_datetime(row.get("last_updated"))

        for flag in ("snapshot_out_of_sequence", "snapshot_is_contemporaneous"):
            if row.get(flag) is not None:
                row[flag] = bool(row[flag])

        for col in double_cols:
            if row.get(col) is not None:
                row[col] = float(row[col])

        yield row

    if dropped_count > 0:
        logger.info(f"sdv_fpi_weekly: dropped {dropped_count} row(s) with null PK column(s)")


def parse_ratings_weekly(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse cfb_ratings_weekly_{season}.parquet into ratings.external_weekly rows.

    Real columns (16; season/through_week/team_id all present in-file --
    ``ctx.season`` is NOT consulted): season (int64), team_id (large_string
    holding a plain integer, e.g. "333" -- coerced to int here so it lines up
    with fpi_weekly's int64 team_id and ref.teams.id), adj_off_epa,
    adj_def_epa, adj_st_epa, adj_net, fei_off, fei_def, fei_net, off_pace,
    net_z (double), games, off_rank, def_rank, net_rank (int), through_week
    (int32). No system/label column -- this file is a single external
    system's (adjusted-EPA + FEI) wide-format weekly snapshot, not a
    long-format multi-system table; a second external system would need
    either a new table or a reshape, not a bolt-on column here.

    PK: (season, through_week, team_id) -- verified 0 duplicate keys, 0 null
    values in any column, across 2124 rows in the 2025 file.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)

    required = {"season", "through_week", "team_id"}
    missing = required - schema_names
    if missing:
        raise ParserStructureError(f"sdv_ratings_weekly: missing column(s): {sorted(missing)}")

    double_cols = [
        "adj_off_epa",
        "adj_def_epa",
        "adj_st_epa",
        "adj_net",
        "fei_off",
        "fei_def",
        "fei_net",
        "off_pace",
        "net_z",
    ]
    int_cols = ["games", "off_rank", "def_rank", "net_rank"]

    rows = table.to_pylist()
    dropped_count = 0

    for row in rows:
        if (
            row.get("season") is None
            or row.get("through_week") is None
            or row.get("team_id") is None
        ):
            dropped_count += 1
            continue

        row["season"] = int(row["season"])
        row["through_week"] = int(row["through_week"])
        row["team_id"] = int(row["team_id"])

        for col in int_cols:
            if row.get(col) is not None:
                row[col] = int(row[col])
        for col in double_cols:
            if row.get(col) is not None:
                row[col] = float(row[col])

        yield row

    if dropped_count > 0:
        logger.info(f"sdv_ratings_weekly: dropped {dropped_count} row(s) with null PK column(s)")
