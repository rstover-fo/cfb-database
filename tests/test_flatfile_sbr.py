"""Tests for the SBR historical odds Excel parser (T6).

Tests parse() against tests/fixtures/flatfiles/sbr_sample_synthetic.xlsx (5 games,
10 rows, real values copied from the live 2022-23 NCAAF odds table -- see
FINDINGS.md section 4) plus inline-constructed workbooks (via openpyxl) for
structural edge cases: broken pairing, bad header, NL/pk cells, the January
date year-rollover, and season inference from the filename.
"""

import io
from datetime import date

import pytest

pytest.importorskip("openpyxl", reason="flatfiles extra not installed")

from openpyxl import Workbook

from src.pipelines.sources.flat_files import ParseContext, ParserStructureError
from src.pipelines.sources.flatfile_parsers import sbr

FIXTURE_PATH = "tests/fixtures/flatfiles/sbr_sample_synthetic.xlsx"

HEADER = [
    "Date",
    "Rot",
    "VH",
    "Team",
    "1st",
    "2nd",
    "3rd",
    "4th",
    "Final",
    "Open",
    "Close",
    "ML",
    "2H",
]


def _load_fixture_bytes() -> bytes:
    with open(FIXTURE_PATH, "rb") as f:
        return f.read()


def _build_workbook(rows: list[list], header: list[str] | None = None) -> bytes:
    """Build a minimal .xlsx in memory from a header + data rows."""
    wb = Workbook()
    ws = wb.active
    ws.append(header if header is not None else HEADER)
    for row in rows:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _row(date_val, rot, vh, team, final, open_, close, ml, two_h):
    """Build a full data row with placeholder quarter scores (1st-4th unused by parser)."""
    return [date_val, rot, vh, team, 0, 0, 0, 0, final, open_, close, ml, two_h]


# ---------------------------------------------------------------------------
# Fixture-based tests
# ---------------------------------------------------------------------------


class TestFixture:
    def test_game_count(self):
        """Fixture has 5 games across 10 data rows."""
        ctx = ParseContext(
            source="sbr", snapshot_date=date(2026, 7, 23), season=2022, file_name="fixture.xlsx"
        )
        rows = list(sbr.parse(_load_fixture_bytes(), ctx))
        assert len(rows) == 5

    def test_neutral_site_pair(self):
        """Rot 299/300 (Northwestern @ Nebraska) is a neutral-site 'N'/'N' pair; first N is
        visitor by SBR convention, so Northwestern=away, Nebraska=home, neutral_site=True.

        Hand-computed spread/total from the fixture's raw Open cells (visitor=55,
        home=10): smaller=10 sits on the home row -> home (Nebraska) favored ->
        spread_open = -10 (home-perspective); total_open = larger = 55.
        Close (visitor=52, home=11.5): smaller=11.5 on home row -> spread_close = -11.5;
        total_close = 52.
        """
        ctx = ParseContext(
            source="sbr", snapshot_date=date(2026, 7, 23), season=2022, file_name="fixture.xlsx"
        )
        rows = list(sbr.parse(_load_fixture_bytes(), ctx))
        game = rows[0]
        assert game["neutral_site"] is True
        assert game["away_team"] == "Northwestern"
        assert game["home_team"] == "Nebraska"
        assert game["away_rot"] == 299
        assert game["home_rot"] == 300
        assert game["spread_open"] == -10.0
        assert game["total_open"] == 55.0
        assert game["spread_close"] == -11.5
        assert game["total_close"] == 52.0
        assert game["away_ml"] == 395
        assert game["home_ml"] == 500
        # 2H (visitor=28, home=7): smaller=7 on home row -> spread_2h = -7; total_2h = 28.
        assert game["spread_2h"] == -7.0
        assert game["total_2h"] == 28.0

    def test_non_neutral_pair_visitor_favored(self):
        """Rot 301/302 (Charlotte @ FloridaAtlantic): V/H pair, non-neutral.

        Open (visitor=57.5, home=5.5): smaller=5.5 on home row -> home favored ->
        spread_open = -5.5; total_open = 57.5.
        """
        ctx = ParseContext(
            source="sbr", snapshot_date=date(2026, 7, 23), season=2022, file_name="fixture.xlsx"
        )
        rows = list(sbr.parse(_load_fixture_bytes(), ctx))
        game = rows[1]
        assert game["neutral_site"] is False
        assert game["away_team"] == "Charlotte"
        assert game["home_team"] == "FloridaAtlantic"
        assert game["spread_open"] == -5.5
        assert game["total_open"] == 57.5

    def test_visitor_favored_positive_home_spread(self):
        """Rot 303/304 (Nevada @ NewMexicoState): visitor holds the smaller Open value
        (16.5 vs. home's 57) -> visitor (Nevada) favored -> home spread is POSITIVE
        (home is the underdog): spread_open = +16.5; total_open = 57.
        """
        ctx = ParseContext(
            source="sbr", snapshot_date=date(2026, 7, 23), season=2022, file_name="fixture.xlsx"
        )
        rows = list(sbr.parse(_load_fixture_bytes(), ctx))
        game = rows[2]
        assert game["away_team"] == "Nevada"
        assert game["home_team"] == "NewMexicoState"
        assert game["spread_open"] == 16.5
        assert game["total_open"] == 57.0
        # Close (visitor=7.5, home=48): smaller=7.5 on visitor row -> spread_close = +7.5.
        assert game["spread_close"] == 7.5
        assert game["total_close"] == 48.0

    def test_game_date_and_season(self):
        """All fixture rows are Date=827 (Aug 27) -> season year, season=2022."""
        ctx = ParseContext(
            source="sbr", snapshot_date=date(2026, 7, 23), season=2022, file_name="fixture.xlsx"
        )
        rows = list(sbr.parse(_load_fixture_bytes(), ctx))
        for game in rows:
            assert game["season"] == 2022
            assert game["game_date"] == date(2022, 8, 27)

    def test_final_scores(self):
        """Rot 299/300: Northwestern 31, Nebraska 28 (from Final column)."""
        ctx = ParseContext(
            source="sbr", snapshot_date=date(2026, 7, 23), season=2022, file_name="fixture.xlsx"
        )
        rows = list(sbr.parse(_load_fixture_bytes(), ctx))
        game = rows[0]
        assert game["away_final"] == 31
        assert game["home_final"] == 28


# ---------------------------------------------------------------------------
# Header validation
# ---------------------------------------------------------------------------


class TestHeaderValidation:
    def test_missing_column_raises(self):
        bad_header = [c for c in HEADER if c != "ML"]
        raw = _build_workbook([], header=bad_header)
        ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
        with pytest.raises(ParserStructureError, match="missing"):
            list(sbr.parse(raw, ctx))

    def test_extra_column_raises(self):
        bad_header = [*HEADER, "Unexpected"]
        raw = _build_workbook([], header=bad_header)
        ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
        with pytest.raises(ParserStructureError, match="extra"):
            list(sbr.parse(raw, ctx))

    def test_empty_workbook_raises(self):
        raw = _build_workbook([], header=[])
        ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
        with pytest.raises(ParserStructureError):
            list(sbr.parse(raw, ctx))


# ---------------------------------------------------------------------------
# Row pairing
# ---------------------------------------------------------------------------


class TestRowPairing:
    def test_broken_pairing_v_v_raises(self):
        rows = [
            _row(901, 1, "V", "TeamA", 10, 3.0, 44.0, -150, 2.0),
            _row(901, 2, "V", "TeamB", 20, 3.5, 45.0, 130, 2.5),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
        with pytest.raises(ParserStructureError, match="bad row pairing"):
            list(sbr.parse(raw, ctx))

    def test_odd_row_count_raises(self):
        rows = [
            _row(901, 1, "V", "TeamA", 10, 3.0, 44.0, -150, 2.0),
            _row(901, 2, "H", "TeamB", 20, 3.5, 45.0, 130, 2.5),
            _row(902, 3, "V", "TeamC", 14, 6.0, 50.0, 110, 3.0),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
        with pytest.raises(ParserStructureError, match="unpaired"):
            list(sbr.parse(raw, ctx))

    def test_pairing_error_names_row_numbers(self):
        rows = [
            _row(901, 1, "H", "TeamA", 10, 3.0, 44.0, -150, 2.0),
            _row(901, 2, "V", "TeamB", 20, 3.5, 45.0, 130, 2.5),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
        with pytest.raises(ParserStructureError, match="rows 2-3"):
            list(sbr.parse(raw, ctx))


# ---------------------------------------------------------------------------
# NL / pk sentinel handling
# ---------------------------------------------------------------------------


class TestSentinels:
    def test_nl_becomes_none_on_both_sides(self):
        rows = [
            _row(901, 1, "V", "TeamA", 10, "NL", "NL", "NL", "NL"),
            _row(901, 2, "H", "TeamB", 7, "NL", "NL", "NL", "NL"),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
        game = next(iter(sbr.parse(raw, ctx)))
        assert game["spread_open"] is None
        assert game["total_open"] is None
        assert game["spread_close"] is None
        assert game["total_close"] is None
        assert game["away_ml"] is None
        assert game["home_ml"] is None
        assert game["spread_2h"] is None
        assert game["total_2h"] is None

    def test_nl_on_one_side_only_raises(self):
        rows = [
            _row(901, 1, "V", "TeamA", 10, "pk", 44.0, -150, 2.0),
            _row(901, 2, "H", "TeamB", 7, "NL", 45.0, 130, 2.5),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
        with pytest.raises(ParserStructureError, match="NL on only one side"):
            list(sbr.parse(raw, ctx))

    def test_pk_becomes_zero_favorite_side(self):
        """'pk' on the visitor row's Open cell means visitor's line is 0 (favorite
        side); since 0 < the home total, it's correctly picked as the smaller/spread
        value, and since it sits on the visitor row, home spread comes out positive.
        """
        rows = [
            _row(901, 1, "V", "TeamA", 10, "pk", 44.0, -150, 2.0),
            _row(901, 2, "H", "TeamB", 7, 41.5, 45.0, 130, 2.5),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
        game = next(iter(sbr.parse(raw, ctx)))
        assert game["spread_open"] == 0.0
        assert game["total_open"] == 41.5

    def test_pk_case_variants(self):
        for token in ("pk", "PK", "p", "P"):
            rows = [
                _row(901, 1, "V", "TeamA", 10, token, 44.0, -150, 2.0),
                _row(901, 2, "H", "TeamB", 7, 41.5, 45.0, 130, 2.5),
            ]
            raw = _build_workbook(rows)
            ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
            game = next(iter(sbr.parse(raw, ctx)))
            assert game["spread_open"] == 0.0

    def test_non_numeric_junk_raises(self):
        rows = [
            _row(901, 1, "V", "TeamA", 10, "banana", 44.0, -150, 2.0),
            _row(901, 2, "H", "TeamB", 7, 41.5, 45.0, 130, 2.5),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
        with pytest.raises(ParserStructureError, match="not numeric"):
            list(sbr.parse(raw, ctx))

    def test_ml_nl_becomes_none(self):
        rows = [
            _row(901, 1, "V", "TeamA", 10, 3.0, 44.0, "NL", 2.0),
            _row(901, 2, "H", "TeamB", 7, 3.5, 45.0, 130, 2.5),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
        game = next(iter(sbr.parse(raw, ctx)))
        assert game["away_ml"] is None
        assert game["home_ml"] == 130


# ---------------------------------------------------------------------------
# Date / season handling
# ---------------------------------------------------------------------------


class TestDateAndSeason:
    def test_january_date_rolls_to_next_calendar_year(self):
        """Date=102 (Jan 2) with season=2022 -> game_date year is 2023."""
        rows = [
            _row(102, 1, "V", "TeamA", 10, 3.0, 44.0, -150, 2.0),
            _row(102, 2, "H", "TeamB", 7, 3.5, 45.0, 130, 2.5),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
        game = next(iter(sbr.parse(raw, ctx)))
        assert game["game_date"] == date(2023, 1, 2)
        assert game["season"] == 2022

    def test_december_date_stays_in_season_year(self):
        rows = [
            _row(1231, 1, "V", "TeamA", 10, 3.0, 44.0, -150, 2.0),
            _row(1231, 2, "H", "TeamB", 7, 3.5, 45.0, 130, 2.5),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
        game = next(iter(sbr.parse(raw, ctx)))
        assert game["game_date"] == date(2022, 12, 31)

    def test_season_from_filename_dash_format(self):
        rows = [
            _row(903, 1, "V", "TeamA", 10, 3.0, 44.0, -150, 2.0),
            _row(903, 2, "H", "TeamB", 7, 3.5, 45.0, 130, 2.5),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(
            source="sbr",
            snapshot_date=date(2026, 7, 23),
            season=None,
            file_name="ncaa football 2013-14.xlsx",
        )
        game = next(iter(sbr.parse(raw, ctx)))
        assert game["season"] == 2013

    def test_season_from_filename_compact_format(self):
        rows = [
            _row(903, 1, "V", "TeamA", 10, 3.0, 44.0, -150, 2.0),
            _row(903, 2, "H", "TeamB", 7, 3.5, 45.0, 130, 2.5),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(
            source="sbr",
            snapshot_date=date(2026, 7, 23),
            season=None,
            file_name="ncaafootball2013.xlsx",
        )
        game = next(iter(sbr.parse(raw, ctx)))
        assert game["season"] == 2013

    def test_missing_season_raises(self):
        rows = [
            _row(903, 1, "V", "TeamA", 10, 3.0, 44.0, -150, 2.0),
            _row(903, 2, "H", "TeamB", 7, 3.5, 45.0, 130, 2.5),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(
            source="sbr",
            snapshot_date=date(2026, 7, 23),
            season=None,
            file_name="no-year-here.xlsx",
        )
        with pytest.raises(ParserStructureError, match="no season available"):
            list(sbr.parse(raw, ctx))

    def test_ctx_season_overrides_filename(self):
        rows = [
            _row(903, 1, "V", "TeamA", 10, 3.0, 44.0, -150, 2.0),
            _row(903, 2, "H", "TeamB", 7, 3.5, 45.0, 130, 2.5),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(
            source="sbr",
            snapshot_date=date(2026, 7, 23),
            season=1999,
            file_name="ncaa football 2013-14.xlsx",
        )
        game = next(iter(sbr.parse(raw, ctx)))
        assert game["season"] == 1999


# ---------------------------------------------------------------------------
# Neutral-site handling (inline-constructed, in addition to the fixture pair)
# ---------------------------------------------------------------------------


class TestNeutralSite:
    def test_two_n_rows_pair_first_as_visitor(self):
        rows = [
            _row(903, 10, "N", "TeamA", 10, 3.0, 44.0, -150, 2.0),
            _row(903, 11, "N", "TeamB", 7, 3.5, 45.0, 130, 2.5),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
        game = next(iter(sbr.parse(raw, ctx)))
        assert game["neutral_site"] is True
        assert game["away_team"] == "TeamA"
        assert game["home_team"] == "TeamB"

    def test_n_then_v_is_invalid_pairing(self):
        rows = [
            _row(903, 10, "N", "TeamA", 10, 3.0, 44.0, -150, 2.0),
            _row(903, 11, "V", "TeamB", 7, 3.5, 45.0, 130, 2.5),
        ]
        raw = _build_workbook(rows)
        ctx = ParseContext(source="sbr", snapshot_date=date(2026, 7, 23), season=2022)
        with pytest.raises(ParserStructureError, match="bad row pairing"):
            list(sbr.parse(raw, ctx))
