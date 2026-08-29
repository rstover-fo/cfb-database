"""Parsers for the ESPN player-grain bundle (B6b), sourced from
sportsdataverse-data's ``espn_cfb_adv_*``/``espn_cfb_play_participants``
GitHub release tags -- one parquet per season per dataset, same
``url_template`` + ``fallback_latest=True`` mechanism as the ncaa_mfb_*/sdv_*
sources (see ``flat_files.py``'s "Per-season multi-file sources" docstring).

Datasets (tag -> table; all in the existing, already-granted ``stats``
Postgres schema -- migration 055, per the schema-architect verdict that
ESPN's numeric ids ARE CFBD's ids, so no separate ``espn`` schema is needed):
- espn_cfb_adv_passing            -> stats.espn_player_passing
  (PK season, game_id, pos_team_id, passer_player_name)
- espn_cfb_adv_rushing            -> stats.espn_player_rushing
  (PK season, game_id, pos_team_id, rusher_player_name)
- espn_cfb_adv_receiving          -> stats.espn_player_receiving
  (PK season, game_id, pos_team_id, receiver_player_name)
- espn_cfb_adv_defensive_players  -> stats.espn_player_defense
  (PK season, game_id, def_pos_team_id, player_name)
- espn_cfb_play_participants      -> stats.espn_play_participants
  (PK season, game_id, play_id)

**Exact asset URLs were NOT guessed** -- the task's candidate tag names
(``cfb_adv_passing``, ``espn_cfb_adv_passing``, ``cfb_passing``, ...) were
resolved by downloading the real ``sportsdataverse`` PyPI package
(0.1.3) and reading its generated ``sportsdataverse/cfb/cfb_loaders.py``
source, which embeds the literal release URL every ``load_cfb_*`` function
fetches (e.g. ``.../releases/download/espn_cfb_adv_passing/
adv_passing_{season}.parquet``). Every URL below was then verified with a
real HTTP download (not merely a HEAD/existence probe) before being used to
build a fixture. ``sportsdataverse-data``'s own ``data-raw/tags.txt`` /
``db_catalog.json`` (checked out from the data repo's git history, since
those files are catalog/codegen inputs, not release assets) independently
confirmed every tag name used here.

**``espn_pbp_2002_2003`` (core.espn_plays_2002_2003) is DROPPED from this
unit -- reported loudly per the task's instruction, not silently
substituted.** The task's premise (a 2002+2003 pre-CFBD gap-fill) does not
hold: ``load_cfb_pbp``'s own docstring declares "seasons: an int or iterable
of seasons (>= 2004)", and this was verified live -- ``play_by_play_2004.
parquet`` downloads fine (25.4MB) while ``play_by_play_2002.parquet`` and
``play_by_play_2003.parquet`` both 404 under the real
``espn_cfb_pbp`` release tag. cfbfastR's own documentation (2004+) was
right; the 2002-03 claim traced to a brainstorm doc was wrong. No
substitute season or dataset was invented in its place -- there is simply no
sixth table in this migration.

**No athlete-id column exists in any of the four ``adv_*`` tables --
reported loudly, per the task's "report if any file lacks an athlete id"
instruction.** Every one of ``espn_cfb_adv_passing``/``adv_rushing``/
``adv_receiving``/``adv_defensive_players`` identifies a player ONLY by a
free-text ``{role}_player_name`` / ``player_name`` column (ESPN's advanced
box-score text scrape, per ``cfb_advanced_stats.py``'s docstring in the
sportsdataverse package -- a different code path than the live
per-game-API-scraping ``espn_cfb_play_participants`` wrapper the package
also ships, which DOES carry ids but is not what these four release
datasets are). Consequently the id-namespace COMMENT ON / join-to-
``core.roster.id`` guidance in migration 055 applies ONLY to
``stats.espn_play_participants``'s ``{type}_player_id`` columns (verified
clean numeric strings, cast to bigint here) -- the four ``adv_*`` tables
carry a free-text name column instead and cannot be joined to
``core.roster`` by id at all. None of the five tables carries a
``position`` column either (also reported loudly, per the same
instruction) -- ``adv_passing``/``adv_rushing``/``adv_receiving`` are
already position-scoped by their very name (passer/rusher/receiver), and
neither ``adv_defensive_players`` nor ``play_participants`` ships one.
``espn_play_participants`` also carries no team id/name column at all
(only ``game_id``/``play_id``) -- team attribution would require a
separate join back to a pbp table by ``(game_id, play_id)``, not
implemented here.

**Team id IS the verified-equal ESPN/CFBD numeric id** (``pos_team_id`` /
``def_pos_team_id`` in the four ``adv_*`` tables) -- same equivalence
already established for the ``sdv_*``/``ncaa_mfb_*`` bundles (schema.py's
2026-08-29 warehouse check: ref.teams.id 333=Alabama). ``game_id`` is
likewise ESPN's event id, equal to ``core.games.id``.

**Season coverage, verified live (not from docs):**
- ``espn_cfb_adv_passing``/``adv_rushing``/``adv_receiving``/
  ``adv_defensive_players``: 2004+ (``SeasonNotFoundError`` for season < 2004
  in the package's own loader). ``min_season=2004`` in the registry.
- ``espn_cfb_play_participants``: 2014+ (same pattern, min season 2014 in
  the loader). ``min_season=2014`` in the registry.
- 2026 is not yet published on any of the five tags (confirmed 2026-08-29,
  same lag as every other sportsdataverse-data bundle this quarter) --
  every registry entry uses ``fallback_latest=True``.

**Schema drift, confirmed by downloading and diffing a 2025 file against an
early (2005, or 2014 for play_participants) file for every dataset:**
- ``adv_passing``/``adv_rushing``/``adv_receiving``: byte-identical column
  sets across 2005 and 2025 -- no drift.
- ``adv_defensive_players``: 2005 has 12 columns; 2025 adds
  ``interceptions``/``interceptions_yards`` (14 columns). Handled with
  ``row.get()`` throughout, same as every other parser in this bundle --
  the migration's columns are the UNION (2025's richer set), dlt fills
  NULL for seasons that predate a column's introduction.
- ``play_participants``: 2014 has 16 participant "types" (68 columns); 2025
  adds a ``fumbler`` type (72 columns, 17 types). ``PARTICIPANT_TYPES``
  below is the 2025 (richer) set; ``row.get()`` throughout means an older
  season simply yields ``None`` for the missing type's four columns.

**Player-identity null handling (NULL-never-0 does not apply to a name
column, so a coalesce-to-sentinel is used instead, mirroring
``ncaa.py``'s ``TEAM_ROW_SENTINEL`` precedent):** ``rusher_player_name``
is null in 78/10206 rows of the 2025 file, ``receiver_player_name`` in
1749/18529 -- both are genuine unattributed-player rows (ESPN's text-scrape
regex couldn't resolve a name for that rush/target; the row still carries
real yardage/EPA, e.g. a real -15-yard rush with a null name is not a
"team total" placeholder the way NCAA's null-jersey-number rows are). Every
null-name row was verified unique per ``(game_id, pos_team_id)`` in the
2025 file (0 collisions), so coalescing to ``UNATTRIBUTED_PLAYER`` keeps
the primary key deterministic without a ``row_seq`` escape hatch.
``passer_player_name`` (passing) and ``player_name`` (defensive_players)
have zero nulls in every file inspected -- no sentinel needed there.

**A real ESPN scrape-artifact name, left as-is (not cleaned):** one row in
the ``adv_defensive_players`` 2025 file carries
``player_name = "by #23 L.Johnson-Burrell at SAC12, End Of Play"`` -- a
play-text fragment that leaked into the name field for an interception
return, not a normal "First Last" name. Kept verbatim, like
``ncaa.schedule.team_name``'s embedded season-end annotations -- this
parser does not attempt free-text cleanup.

**``{type}_player_names`` / ``{type}_player_ids`` (the "list" column
family in ``play_participants``) are Python ``repr()`` string literals,
NOT JSON or a Postgres array** -- e.g. ``"['Ben Barnes']"`` or ``"[]"``,
verified against the real file. Kept as opaque ``text`` here rather than
parsed: the scalar ``{type}_player_name``/``{type}_player_id`` family
already carries the common case (first occurrence), and this repo's
flat-file framework has no precedent for a list/array-typed merge column
(dlt would otherwise want to spin up a child table, which the pre-created
migration-055 schema does not provide for). A caller that needs every
participant of a given type (e.g. both players on a split sack) must
``ast.literal_eval`` this column client-side; that transform is out of
scope here and is called out on the column comment in migration 055.

Read with ``pyarrow.parquet.read_table(io.BytesIO(raw))`` -- pandas is not
a dependency, per the ncaa.py/sportsdataverse.py precedent.
"""

import io
import logging
from collections.abc import Iterator

import pyarrow.parquet

from ..flat_files import ParseContext, ParserStructureError

logger = logging.getLogger(__name__)

# Sentinel for adv_rushing/adv_receiving's null-name "unattributed" rows (a
# real rush/target ESPN's text scrape couldn't resolve to a player -- see
# module docstring). Distinct from ncaa.py's TEAM_ROW_SENTINEL: these rows
# are NOT team-aggregate rows, just individually-unattributed plays.
UNATTRIBUTED_PLAYER = "UNATTRIBUTED"

# Every participant "type" ESPN ships in the richest (2025) play_participants
# file (17 types; 2014's file has 16 -- missing "fumbler". See module
# docstring). Drives which {type}_player_name/_id/_names/_ids columns this
# parser looks for via row.get() (safe against the older, sparser season).
PARTICIPANT_TYPES = (
    "kicker",
    "tackler",
    "returner",
    "rusher",
    "passer",
    "receiver",
    "punter",
    "assisted_by",
    "penalized",
    "scorer",
    "pat_scorer",
    "sacked_by",
    "pass_defender",
    "recoverer",
    "fumbler",
    "forced_by",
    "pat_passer",
)

# Raw parquet column -> target snake_case column, per dataset. Applied
# BEFORE the row reaches dlt so the loaded column name is exactly what this
# module (and migration 055) says it is, rather than depending on dlt's
# naming-convention normalizer to land on the same name independently (the
# repo's other flat-file parsers only need this because their source
# columns are already snake_case; ESPN's advanced-stat exports ship
# Title_Case/camelCase abbreviations that a bare pass-through would leave
# ambiguous).
_PASSING_RENAME = {
    "Comp": "comp",
    "Att": "att",
    "xComp": "x_comp",
    "Yds": "yds",
    "Pass_TD": "pass_td",
    "Int": "int",
    "YPA": "ypa",
    "EPA": "epa",
    "EPA_per_Play": "epa_per_play",
    "WPA": "wpa",
    "SR": "sr",
    "Sck": "sck",
    "CompPct": "comp_pct",
    "xCompPct": "x_comp_pct",
    "CPOE": "cpoe",
}

_RUSHING_RENAME = {
    "Car": "car",
    "Yds": "yds",
    "Rush_TD": "rush_td",
    "YPC": "ypc",
    "EPA": "epa",
    "EPA_per_Play": "epa_per_play",
    "WPA": "wpa",
    "SR": "sr",
    "Fum": "fum",
    "Fum_Lost": "fum_lost",
}

_RECEIVING_RENAME = {
    "Rec": "rec",
    "Tar": "tar",
    "Yds": "yds",
    "Rec_TD": "rec_td",
    "YPT": "ypt",
    "EPA": "epa",
    "EPA_per_Play": "epa_per_play",
    "WPA": "wpa",
    "SR": "sr",
    "Fum": "fum",
    "Fum_Lost": "fum_lost",
}


def _apply_rename(row: dict, rename: dict[str, str]) -> None:
    """Pop each raw key present in ``row`` and reinsert it under its target name."""
    for raw, target in rename.items():
        if raw in row:
            row[target] = row.pop(raw)


def _require_columns(dataset: str, schema_names: set[str], required: set[str]) -> None:
    missing = required - schema_names
    if missing:
        raise ParserStructureError(f"{dataset}: missing column(s): {sorted(missing)}")


def parse_player_passing(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse adv_passing_{season}.parquet into stats.espn_player_passing rows.

    Real columns (32; byte-identical across the 2005 and 2025 files
    inspected -- no schema drift): pos_team_id, pos_team,
    passer_player_name, Comp, Att, xComp, Yds, Pass_TD, Int, YPA, EPA,
    EPA_per_Play, WPA, SR, Sck, CompPct, xCompPct, CPOE, qbr_epa, sack_epa,
    pass_epa, rush_epa, pen_epa, spread, era0, era1, era2, era3, exp_qbr,
    game_id, season, week. ``era0``..``era3`` are one-hot rule-era flags
    (opaque upstream, passed through as-is). NO athlete id column at all --
    see module docstring.

    PK: (season, game_id, pos_team_id, passer_player_name) -- verified 0
    duplicate keys and 0 null passer_player_name across both the 2005
    (2354 rows) and 2025 (1969 rows) files.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)
    _require_columns(
        "espn_player_passing", schema_names, {"game_id", "pos_team_id", "passer_player_name"}
    )

    rows = table.to_pylist()
    dropped_count = 0
    out = []

    for row in rows:
        if (
            row.get("game_id") is None
            or row.get("pos_team_id") is None
            or row.get("passer_player_name") is None
        ):
            dropped_count += 1
            continue

        _apply_rename(row, _PASSING_RENAME)
        out.append(row)

    if dropped_count > 0:
        logger.info(f"espn_player_passing: dropped {dropped_count} row(s) with null PK column(s)")

    yield from out


def parse_player_rushing(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse adv_rushing_{season}.parquet into stats.espn_player_rushing rows.

    Real columns (16; byte-identical across 2005/2025): pos_team_id,
    pos_team, rusher_player_name, Car, Yds, Rush_TD, YPC, EPA, EPA_per_Play,
    WPA, SR, Fum, Fum_Lost, game_id, season, week. NO athlete id column.

    PK: (season, game_id, pos_team_id, rusher_player_name).
    ``rusher_player_name`` is null for a real subset of rows (78/10206 in
    the 2025 file, 11/6406 in 2005) -- genuine unattributed-rusher plays
    (real yardage/EPA, ESPN's text scrape just couldn't resolve a name),
    verified unique per (game_id, pos_team_id) with 0 collisions -- coalesced
    to ``UNATTRIBUTED_PLAYER`` rather than dropped, since the row carries
    real play data.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)
    _require_columns("espn_player_rushing", schema_names, {"game_id", "pos_team_id"})

    rows = table.to_pylist()
    dropped_count = 0
    out = []

    for row in rows:
        if row.get("game_id") is None or row.get("pos_team_id") is None:
            dropped_count += 1
            continue

        if row.get("rusher_player_name") is None:
            row["rusher_player_name"] = UNATTRIBUTED_PLAYER

        _apply_rename(row, _RUSHING_RENAME)
        out.append(row)

    if dropped_count > 0:
        logger.info(f"espn_player_rushing: dropped {dropped_count} row(s) with null PK column(s)")

    yield from out


def parse_player_receiving(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse adv_receiving_{season}.parquet into stats.espn_player_receiving rows.

    Real columns (17; byte-identical across 2005/2025): pos_team_id,
    pos_team, receiver_player_name, Rec, Tar, Yds, Rec_TD, YPT, EPA,
    EPA_per_Play, WPA, SR, Fum, Fum_Lost, game_id, season, week. NO athlete
    id column.

    PK: (season, game_id, pos_team_id, receiver_player_name).
    ``receiver_player_name`` is null for a real subset of rows (1749/18529
    in the 2025 file) -- same unattributed-target pattern as
    ``parse_player_rushing``, verified unique per (game_id, pos_team_id)
    with 0 collisions -- coalesced to ``UNATTRIBUTED_PLAYER``.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)
    _require_columns("espn_player_receiving", schema_names, {"game_id", "pos_team_id"})

    rows = table.to_pylist()
    dropped_count = 0
    out = []

    for row in rows:
        if row.get("game_id") is None or row.get("pos_team_id") is None:
            dropped_count += 1
            continue

        if row.get("receiver_player_name") is None:
            row["receiver_player_name"] = UNATTRIBUTED_PLAYER

        _apply_rename(row, _RECEIVING_RENAME)
        out.append(row)

    if dropped_count > 0:
        logger.info(f"espn_player_receiving: dropped {dropped_count} row(s) with null PK column(s)")

    yield from out


def parse_player_defense(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse adv_defensive_players_{season}.parquet into stats.espn_player_defense rows.

    Real columns: 12 in the 2005 file (def_pos_team_id, def_pos_team,
    player_name, sacks, sacks_yards, pass_breakups, forced_fumbles,
    fumble_recoveries, fumble_recoveries_yards, game_id, season, week), 14
    in 2025 (adds interceptions, interceptions_yards -- confirmed schema
    drift, handled via row.get()). Already snake_case -- no rename map
    needed. NO athlete id column; NO position column.

    Every count column (sacks, interceptions, forced_fumbles, ...) is
    NULL, not 0, when that category wasn't reported for the player in that
    game (verified: e.g. a player with sacks=0 but interceptions=None) --
    per this repo's NULL-never-0 rule, nulls are passed through as-is, not
    coalesced to zero.

    One real ESPN scrape-artifact observed (kept verbatim, not cleaned):
    a play-text fragment leaking into player_name for an interception
    return -- see module docstring.

    PK: (season, game_id, def_pos_team_id, player_name) -- verified 0
    duplicate keys and 0 null player_name across both the 2005 (4046 rows)
    and 2025 (8544 rows) files.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)
    _require_columns(
        "espn_player_defense", schema_names, {"game_id", "def_pos_team_id", "player_name"}
    )

    rows = table.to_pylist()
    dropped_count = 0
    out = []

    for row in rows:
        if (
            row.get("game_id") is None
            or row.get("def_pos_team_id") is None
            or row.get("player_name") is None
        ):
            dropped_count += 1
            continue

        out.append(row)

    if dropped_count > 0:
        logger.info(f"espn_player_defense: dropped {dropped_count} row(s) with null PK column(s)")

    yield from out


def parse_play_participants(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse play_participants_{season}.parquet into stats.espn_play_participants rows.

    Real columns: game_id, play_id, season, week, plus four columns per
    entry in PARTICIPANT_TYPES ({type}_player_name, {type}_player_id,
    {type}_player_names, {type}_player_ids) -- 72 columns total in the 2025
    file (17 types), 68 in 2014 (16 types -- missing "fumbler", handled via
    row.get()). NO team id/name column at all -- team attribution would
    require joining back to a pbp table by (game_id, play_id), not done
    here.

    ``{type}_player_id`` (the scalar family) is the shared CFBD/ESPN
    athlete-id namespace -- verified clean numeric strings (0 parse
    failures across 154653 rows in the 2025 file) -- cast to bigint here.
    ``{type}_player_names``/``{type}_player_ids`` (the list family) are
    Python repr() string literals (e.g. "['Ben Barnes']"), NOT JSON or a
    Postgres array -- kept as opaque text, not parsed (see module
    docstring).

    PK: (season, game_id, play_id) -- verified 0 duplicate keys and 0
    nulls in game_id/play_id/season/week across 154653 rows in the 2025
    file.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))
    schema_names = set(table.column_names)
    _require_columns("espn_play_participants", schema_names, {"game_id", "play_id"})

    rows = table.to_pylist()
    dropped_count = 0
    out = []

    for row in rows:
        if row.get("game_id") is None or row.get("play_id") is None:
            dropped_count += 1
            continue

        for ptype in PARTICIPANT_TYPES:
            id_col = f"{ptype}_player_id"
            if row.get(id_col) is not None:
                row[id_col] = int(row[id_col])

        out.append(row)

    if dropped_count > 0:
        logger.info(
            f"espn_play_participants: dropped {dropped_count} row(s) with null PK column(s)"
        )

    yield from out
