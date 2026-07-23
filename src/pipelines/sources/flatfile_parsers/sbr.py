"""Parser for sportsbookreviewsonline NCAAF historical odds Excel files (T6).

Format (per-season .xlsx, two consecutive rows per game -- visitor row then
home row; verify against tests/fixtures/flatfiles/FINDINGS.md): columns
typically Date (MMDD int), Rot, VH (V/H/N), Team, quarter scores, Final, Open,
Close, ML, 2H. Odd cell values: "NL" (no line), "pk"/"PK" (pick'em = 0),
occasional whitespace/case noise. The Open/Close columns interleave spread and
total (the larger value of the pair is the total, the smaller the spread --
standard SBR convention; confirm in FINDINGS.md).

Required behavior:
- Pair rows strictly (V then H, or two N rows for neutral); raise
  ``ParserStructureError`` with row context when pairing breaks.
- Season inference: from ``ctx.file_name`` (e.g. "ncaa football 2013-14.xlsx")
  or ``ctx.season``; game dates spanning Aug-Dec belong to the season year,
  Jan dates to season year + 1 calendar (build game_date accordingly).
- Yield one row per game matching betting.sbr_historical columns, with
  home_team/away_team carrying the SOURCE spellings (the framework resolves
  via crosswalk and preserves originals to *_source -- keep_source_names=True).
- Normalize "NL" -> None, "pk" -> 0.0; coerce numerics defensively; fail loud
  on per-year format drift rather than guessing.
"""

from collections.abc import Iterator

from ..flat_files import ParseContext


def parse(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse an SBR season Excel file into betting.sbr_historical rows."""
    raise NotImplementedError("T6 implements sbr.parse")
