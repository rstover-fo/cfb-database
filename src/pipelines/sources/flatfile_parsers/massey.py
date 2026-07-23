"""Parser for the Massey College Football Ranking Comparison CSV (T4).

Source: https://masseyratings.com/cf/compare.csv -- three sections:
1. Preamble: title line, "Thru games of <weekday>, <Month> <D>, <YYYY>" line,
   "compiled by ..." line, blank lines.
2. Systems legend: one line per rating system -- code, name, URL, trailing
   stats numbers (~86 systems; count varies week to week -- read dynamically).
3. Team matrix: one line per team with per-system ranks in columns mapped to
   the legend order, plus consensus columns (mean/median/stdev/rank).
Exact column mapping documented in tests/fixtures/flatfiles/FINDINGS.md.

Required behavior:
- Parse the "Thru games of" date; if ``season_for_date(thru_date)`` is not the
  current season (``season_for_date(ctx.snapshot_date)``), raise
  ``StaleSnapshotError`` -- Massey serves last season's final snapshot all
  offseason and those bytes must not be loaded as a new snapshot.
- Yield one composite row per team (season, snapshot_date, team,
  composite_rank, rating_mean, rating_median, rating_stdev, n_systems) and one
  child row per (team, system) tagged ``{"_table": "massey_system_ratings"}``
  with (season, snapshot_date, team, system_code, system_rank).
- ``season`` comes from the thru-date (``season_for_date``), ``snapshot_date``
  from ``ctx.snapshot_date``.
- Fail loud (``ParserStructureError``) on any structural surprise: missing
  section, matrix width not matching the legend, unparseable consensus block.
"""

from collections.abc import Iterator

from ..flat_files import ParseContext


def parse(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse the Massey comparison CSV into composite + per-system rows."""
    raise NotImplementedError("T4 implements massey.parse")
