"""Parsers for nflverse-data parquet releases (T5).

Sources (annual, stable GitHub release URLs):
- combine.parquet   -> draft.combine          (PK season, player_name, pos)
- draft_picks.parquet -> draft.nflverse_draft_picks (PK season, round, pick)

Column names/types are captured verbatim in tests/fixtures/flatfiles/FINDINGS.md;
map them 1:1 (no renames beyond snake_case normalization) with light defensive
munging only: coerce numerics, pass nulls through, drop rows missing a primary
key component (log-worthy but expected for ancient combine rows). Read with
``pyarrow.parquet.read_table(io.BytesIO(raw))`` -- pandas is intentionally not
a dependency. Raise ParserStructureError if an expected PK column is absent
from the file's schema.
"""

import io
import logging
from collections.abc import Iterator

import pyarrow.parquet

from ..flat_files import ParseContext, ParserStructureError

logger = logging.getLogger(__name__)


def parse_combine(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse combine.parquet into draft.combine rows.

    PK columns: season, player_name, pos
    Rows missing any PK column are dropped with a log message.
    Numerics are coerced defensively: ints for season, floats for measurables.
    Null values pass through.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))

    # Verify PK columns exist in schema
    pk_columns = {"season", "player_name", "pos"}
    schema_names = set(table.column_names)
    missing = pk_columns - schema_names
    if missing:
        raise ParserStructureError(
            f"nflverse_combine: missing PK column(s): {sorted(missing)}"
        )

    rows = table.to_pylist()
    dropped_count = 0

    for row in rows:
        # Check for null PK columns
        if row.get("season") is None or row.get("player_name") is None or row.get("pos") is None:
            dropped_count += 1
            continue

        # Coerce numerics defensively
        if row.get("season") is not None:
            row["season"] = int(row["season"])

        if row.get("draft_year") is not None:
            row["draft_year"] = int(row["draft_year"])

        if row.get("draft_round") is not None:
            row["draft_round"] = int(row["draft_round"])

        if row.get("draft_ovr") is not None:
            row["draft_ovr"] = int(row["draft_ovr"])

        if row.get("wt") is not None:
            row["wt"] = int(row["wt"])

        # Coerce measurables to float
        for measurable in ["ht", "forty", "vertical", "cone", "shuttle"]:
            if row.get(measurable) is not None:
                row[measurable] = float(row[measurable])

        if row.get("bench") is not None:
            row["bench"] = int(row["bench"])

        if row.get("broad_jump") is not None:
            row["broad_jump"] = int(row["broad_jump"])

        yield row

    if dropped_count > 0:
        logger.info(
            f"nflverse_combine: dropped {dropped_count} row(s) with null PK column(s)"
        )


def parse_draft_picks(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse draft_picks.parquet into draft.nflverse_draft_picks rows.

    PK columns: season, round, pick
    Rows missing any PK column are dropped with a log message.
    Numerics are coerced defensively: ints for season/round/pick, floats for stats.
    Null values pass through.
    Extra columns in the file pass through untouched.
    """
    table = pyarrow.parquet.read_table(io.BytesIO(raw))

    # Verify PK columns exist in schema
    pk_columns = {"season", "round", "pick"}
    schema_names = set(table.column_names)
    missing = pk_columns - schema_names
    if missing:
        raise ParserStructureError(
            f"nflverse_draft_picks: missing PK column(s): {sorted(missing)}"
        )

    rows = table.to_pylist()
    dropped_count = 0

    for row in rows:
        # Check for null PK columns
        if row.get("season") is None or row.get("round") is None or row.get("pick") is None:
            dropped_count += 1
            continue

        # Coerce PK columns to int
        row["season"] = int(row["season"])
        row["round"] = int(row["round"])
        row["pick"] = int(row["pick"])

        # Coerce age to int if present
        if row.get("age") is not None:
            row["age"] = int(row["age"])

        # Coerce hof to bool if present
        if row.get("hof") is not None:
            row["hof"] = bool(row["hof"])

        # Coerce stat/numeric columns to float (most are already float in parquet)
        # but we ensure consistency for defensive munging
        for stat_col in [
            "to", "allpro", "probowls", "seasons_started", "w_av", "car_av", "dr_av",
            "games", "pass_completions", "pass_attempts", "pass_yards", "pass_tds",
            "pass_ints", "rush_atts", "rush_yards", "rush_tds", "receptions",
            "rec_yards", "rec_tds", "def_solo_tackles", "def_ints", "def_sacks"
        ]:
            if row.get(stat_col) is not None:
                row[stat_col] = float(row[stat_col])

        yield row

    if dropped_count > 0:
        logger.info(
            f"nflverse_draft_picks: dropped {dropped_count} row(s) with null PK column(s)"
        )
