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
  current season (``season_for_date(ctx.snapshot_date)``, or ``ctx.season``
  when the caller supplied one), raise ``StaleSnapshotError`` -- Massey serves
  last season's final snapshot all offseason and those bytes must not be
  loaded as a new snapshot.
- Yield one composite row per team (season, snapshot_date, team,
  composite_rank, rating_mean, rating_median, rating_stdev, n_systems) and one
  child row per (team, system) tagged ``{"_table": "massey_system_ratings"}``
  with (season, snapshot_date, team, system_code, system_rank).
- ``season`` comes from the thru-date (``season_for_date``), ``snapshot_date``
  from ``ctx.snapshot_date``.
- Fail loud (``ParserStructureError``) on any structural surprise: missing
  section, matrix width not matching the legend, unparseable consensus block.

Parsing notes (see FINDINGS.md for the full writeup):
- Lines are comma-delimited but *not* CSV-quoted -- plain ``str.split(",")``
  per line is used throughout, never the ``csv`` module's quote handling
  (fields rely on not containing commas; padding is via literal spaces).
- The header row and every team-matrix row end in a trailing comma (an empty
  final field after splitting) -- this is verified and stripped. Legend lines
  do *not* have this trailing comma; only the header/matrix rows do.
- System codes may be left-padded (e.g. ``" AP"`` for 2-char codes) --
  compared after stripping both sides.
- Consensus columns (``Rank, Mean, Trimmed, Median, StDev``) sit at fixed
  offsets 3-7 right after the three descriptive columns (``Team, Conf, WL``);
  system columns start at offset 8 and run for however many systems the
  legend declares (verified against the header, never assumed).
"""

import re
from collections.abc import Iterator
from datetime import date, datetime

from ..flat_files import (
    TABLE_KEY,
    ParseContext,
    ParserStructureError,
    StaleSnapshotError,
    season_for_date,
)

_THRU_DATE_RE = re.compile(r"Thru games of\s+\w+,\s+(\w+)\s+(\d{1,2}),\s+(\d{4})")

_FIXED_COLUMNS = ("Team", "Conf", "WL", "Rank", "Mean", "Trimmed", "Median", "StDev")
_COL_TEAM, _COL_CONF, _COL_WL, _COL_RANK, _COL_MEAN, _COL_TRIMMED, _COL_MEDIAN, _COL_STDEV = range(
    len(_FIXED_COLUMNS)
)

CHILD_TABLE = "massey_system_ratings"


def _parse_thru_date(lines: list[str]) -> date:
    if len(lines) < 2:
        raise ParserStructureError(
            "massey: file has fewer than 2 lines -- missing 'Thru games of' date line"
        )
    thru_line = lines[1]
    match = _THRU_DATE_RE.search(thru_line)
    if not match:
        raise ParserStructureError(
            f"massey line 2: could not find 'Thru games of <date>' pattern in {thru_line!r}"
        )
    month_name, day, year = match.groups()
    try:
        return datetime.strptime(f"{month_name} {day} {year}", "%B %d %Y").date()
    except ValueError as e:
        raise ParserStructureError(
            f"massey line 2: unparseable thru-date {thru_line!r}"
        ) from e


def _skip_blank(lines: list[str], idx: int) -> int:
    while idx < len(lines) and lines[idx].strip() == "":
        idx += 1
    return idx


def _consume_nonblank(lines: list[str], idx: int) -> tuple[int, int]:
    """Return (start, end) of a contiguous run of non-blank lines starting at idx."""
    start = idx
    while idx < len(lines) and lines[idx].strip() != "":
        idx += 1
    return start, idx


def _parse_legend(lines: list[str], idx: int) -> tuple[list[str], int]:
    idx = _skip_blank(lines, idx)
    if idx >= len(lines):
        raise ParserStructureError("massey: no systems legend section found after preamble")
    start, end = _consume_nonblank(lines, idx)
    system_codes: list[str] = []
    for line_no0 in range(start, end):
        line = lines[line_no0]
        fields = line.split(",")
        if len(fields) < 2:
            raise ParserStructureError(
                f"massey line {line_no0 + 1}: legend row missing comma-delimited fields: {line!r}"
            )
        code = fields[0].strip()
        if not code:
            raise ParserStructureError(
                f"massey line {line_no0 + 1}: legend row missing system code: {line!r}"
            )
        system_codes.append(code)
    if not system_codes:
        raise ParserStructureError("massey: systems legend section is empty")
    return system_codes, end


def _split_trailing_comma_row(line: str, line_no: int, what: str) -> list[str]:
    """Split a header/matrix row and strip its guaranteed trailing empty field."""
    fields = line.split(",")
    if len(fields) < 2 or fields[-1].strip() != "":
        raise ParserStructureError(
            f"massey line {line_no}: {what} missing expected trailing comma: {line!r}"
        )
    return fields[:-1]


def _parse_header(lines: list[str], idx: int) -> tuple[list[str], int, int]:
    idx = _skip_blank(lines, idx)
    if idx >= len(lines):
        raise ParserStructureError("massey: no team-matrix header row found")
    header_line_no = idx + 1
    fields = _split_trailing_comma_row(lines[idx], header_line_no, "header row")
    stripped = [f.strip() for f in fields]
    if tuple(stripped[: len(_FIXED_COLUMNS)]) != _FIXED_COLUMNS:
        raise ParserStructureError(
            f"massey line {header_line_no}: unexpected fixed columns "
            f"{stripped[: len(_FIXED_COLUMNS)]!r}, expected {list(_FIXED_COLUMNS)!r}"
        )
    header_system_codes = stripped[len(_FIXED_COLUMNS) :]
    return header_system_codes, header_line_no, idx + 1


def parse(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    """Parse the Massey comparison CSV into composite + per-system rows."""
    text = raw.decode("utf-8")
    lines = text.splitlines()

    thru_date = _parse_thru_date(lines)
    season = season_for_date(thru_date)
    expected_season = ctx.season if ctx.season is not None else season_for_date(ctx.snapshot_date)
    if season != expected_season:
        raise StaleSnapshotError(
            f"massey: file thru-date {thru_date} (season {season}) is not the expected "
            f"season {expected_season} (ctx.snapshot_date={ctx.snapshot_date}, "
            f"ctx.season={ctx.season})"
        )

    system_codes, idx = _parse_legend(lines, 3)
    header_system_codes, header_line_no, idx = _parse_header(lines, idx)
    if header_system_codes != system_codes:
        raise ParserStructureError(
            f"massey line {header_line_no}: header system columns "
            f"({len(header_system_codes)}) do not match legend systems "
            f"({len(system_codes)}): header={header_system_codes!r} legend={system_codes!r}"
        )
    n_systems_total = len(system_codes)
    expected_field_count = len(_FIXED_COLUMNS) + n_systems_total

    idx = _skip_blank(lines, idx)
    matrix_start, matrix_end = _consume_nonblank(lines, idx)
    if matrix_start >= matrix_end:
        raise ParserStructureError("massey: no team rows found in matrix section")

    teams_parsed = 0
    for line_no0 in range(matrix_start, matrix_end):
        line = lines[line_no0]
        line_no = line_no0 + 1
        fields = _split_trailing_comma_row(line, line_no, "team-matrix row")
        if len(fields) != expected_field_count:
            raise ParserStructureError(
                f"massey line {line_no}: row has {len(fields)} fields, expected "
                f"{expected_field_count} ({len(_FIXED_COLUMNS)} fixed + {n_systems_total} "
                f"systems): {line!r}"
            )

        team = fields[_COL_TEAM].strip()
        if not team:
            raise ParserStructureError(f"massey line {line_no}: empty team name: {line!r}")

        try:
            composite_rank = int(fields[_COL_RANK].strip())
            rating_mean = float(fields[_COL_MEAN].strip())
            rating_median = float(fields[_COL_MEDIAN].strip())
            rating_stdev = float(fields[_COL_STDEV].strip())
        except ValueError as e:
            raise ParserStructureError(
                f"massey line {line_no}: unparseable consensus block for {team!r}: {line!r}"
            ) from e

        system_values = fields[len(_FIXED_COLUMNS) :]
        child_rows = []
        n_systems = 0
        for system_code, raw_val in zip(system_codes, system_values):
            val = raw_val.strip()
            if val == "":
                continue
            try:
                system_rank = int(val)
            except ValueError as e:
                raise ParserStructureError(
                    f"massey line {line_no}: unparseable system rank for {team!r}/"
                    f"{system_code}: {raw_val!r}"
                ) from e
            n_systems += 1
            child_rows.append(
                {
                    TABLE_KEY: CHILD_TABLE,
                    "season": season,
                    "snapshot_date": ctx.snapshot_date,
                    "team": team,
                    "system_code": system_code,
                    "system_rank": system_rank,
                }
            )

        teams_parsed += 1
        yield {
            "season": season,
            "snapshot_date": ctx.snapshot_date,
            "team": team,
            "composite_rank": composite_rank,
            "rating_mean": rating_mean,
            "rating_median": rating_median,
            "rating_stdev": rating_stdev,
            "n_systems": n_systems,
        }
        yield from child_rows

    if teams_parsed == 0:
        raise ParserStructureError("massey: zero teams parsed from matrix section")
