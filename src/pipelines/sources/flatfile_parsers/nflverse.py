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

from collections.abc import Iterator

from ..flat_files import ParseContext


def parse_combine(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse combine.parquet into draft.combine rows."""
    raise NotImplementedError("T5 implements nflverse.parse_combine")


def parse_draft_picks(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse draft_picks.parquet into draft.nflverse_draft_picks rows."""
    raise NotImplementedError("T5 implements nflverse.parse_draft_picks")
