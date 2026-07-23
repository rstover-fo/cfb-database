"""Tests for the Massey composite CSV parser (T4).

Uses tests/fixtures/flatfiles/massey_compare_sample.csv (10 systems x 10
teams, trimmed per FINDINGS.md from a live fetch dated "Thru games of
Monday, January 9, 2023" -- season 2022) plus small inline-built bad inputs
for the structural-error paths.
"""

from datetime import date

import pytest

from src.pipelines.sources.flat_files import (
    ParseContext,
    ParserStructureError,
    StaleSnapshotError,
)
from src.pipelines.sources.flatfile_parsers import massey

FIXTURE_PATH = "tests/fixtures/flatfiles/massey_compare_sample.csv"

# Same-season snapshot date for the fixture's Jan 9, 2023 thru-date (season 2022:
# Jan 2023 falls in the Aug2022-Jan2023 season window).
ALIGNED_SNAPSHOT = date(2023, 1, 15)
# A snapshot date that lands in a different CFB season (season 2023).
STALE_SNAPSHOT = date(2023, 9, 1)

# Preamble/legend/header shared by the hand-built bad-input fixtures below.
_GOOD_PREAMBLE = (
    "College Football Ranking Comparison\n"
    "Thru games of Monday, January 9, 2023\n"
    "compiled by: Kenneth Massey (http://www.masseyratings.com) Sat May  6 05:05:42 2023\n"
    "\n"
    "\n"
)
_GOOD_LEGEND = (
    "AND, Anderson                 , http://example.com/and,  985,  917,  224,  327\n"
    " AP, Associated Press         , http://example.com/ap,  887,  887,  255,  255\n"
)
_GOOD_HEADER = "Team, Conf, WL, Rank, Mean, Trimmed, Median, StDev, AND, AP,\n"
_GOOD_ROW = "Georgia          ,SEC,  15-0,   1,   1.00,   1.00,   1.0,   0.00,   1,   1,\n"


def _good_body(header: str = _GOOD_HEADER, row: str = _GOOD_ROW) -> str:
    """Preamble + legend + blank + header + blank + row, mirroring the real file layout."""
    return _GOOD_PREAMBLE + _GOOD_LEGEND + "\n" + header + "\n" + row


def _good_ctx(snapshot_date: date = ALIGNED_SNAPSHOT) -> ParseContext:
    return ParseContext(source="massey", snapshot_date=snapshot_date)


def _read_fixture() -> bytes:
    with open(FIXTURE_PATH, "rb") as f:
        return f.read()


class TestFixtureParsing:
    def test_team_row_count(self):
        rows = list(massey.parse(_read_fixture(), _good_ctx()))
        composite_rows = [r for r in rows if "composite_rank" in r]
        assert len(composite_rows) == 10

    def test_season_derived_from_thru_date(self):
        """A January thru-date belongs to the previous calendar year's season."""
        rows = list(massey.parse(_read_fixture(), _good_ctx()))
        composite_rows = [r for r in rows if "composite_rank" in r]
        assert all(r["season"] == 2022 for r in composite_rows)

    def test_snapshot_date_passthrough(self):
        rows = list(massey.parse(_read_fixture(), _good_ctx()))
        composite_rows = [r for r in rows if "composite_rank" in r]
        assert all(r["snapshot_date"] == ALIGNED_SNAPSHOT for r in composite_rows)

    def test_louisville_composite_hand_computed(self):
        """Spot-check Louisville's composite row against hand-read fixture values.

        Fixture row: Louisville,ACC,8-5,22,26.39,25.77,25.0,6.12,
        27,   ,23,22,22,18,27,31,24,26, -- AND=27, AP=blank, ARG=23, BAS=22,
        BBT=22, BEG=18, BIH=27, BIL=31, BMC=24, BRN=26 -> 9 non-blank systems.
        """
        rows = list(massey.parse(_read_fixture(), _good_ctx()))
        composite_rows = {r["team"]: r for r in rows if "composite_rank" in r}
        louisville = composite_rows["Louisville"]
        assert louisville["composite_rank"] == 22
        assert louisville["rating_mean"] == pytest.approx(26.39)
        assert louisville["rating_median"] == pytest.approx(25.0)
        assert louisville["rating_stdev"] == pytest.approx(6.12)
        assert louisville["n_systems"] == 9

    def test_fully_ranked_team_n_systems(self):
        """Georgia is ranked by all 10 systems in the trimmed fixture."""
        rows = list(massey.parse(_read_fixture(), _good_ctx()))
        composite_rows = {r["team"]: r for r in rows if "composite_rank" in r}
        georgia = composite_rows["Georgia"]
        assert georgia["composite_rank"] == 1
        assert georgia["rating_mean"] == pytest.approx(1.00)
        assert georgia["rating_median"] == pytest.approx(1.0)
        assert georgia["rating_stdev"] == pytest.approx(0.00)
        assert georgia["n_systems"] == 10

    def test_child_row_count(self):
        """5 teams fully ranked (10 systems) + 5 teams missing only AP (9 systems)."""
        rows = list(massey.parse(_read_fixture(), _good_ctx()))
        child_rows = [r for r in rows if r.get("_table") == "massey_system_ratings"]
        assert len(child_rows) == 5 * 10 + 5 * 9

    def test_child_row_all_tagged_correctly(self):
        rows = list(massey.parse(_read_fixture(), _good_ctx()))
        child_rows = [r for r in rows if r.get("_table") == "massey_system_ratings"]
        for row in child_rows:
            assert row["season"] == 2022
            assert row["snapshot_date"] == ALIGNED_SNAPSHOT
            assert set(row) == {
                "_table",
                "season",
                "snapshot_date",
                "team",
                "system_code",
                "system_rank",
            }

    def test_specific_child_row_triple(self):
        """Louisville/BAS system rank is 22 per the fixture's matrix row."""
        rows = list(massey.parse(_read_fixture(), _good_ctx()))
        child_rows = [r for r in rows if r.get("_table") == "massey_system_ratings"]
        match = [r for r in child_rows if r["team"] == "Louisville" and r["system_code"] == "BAS"]
        assert len(match) == 1
        assert match[0]["system_rank"] == 22

    def test_ap_blank_cell_no_child_row(self):
        """AP is blank for Louisville (unranked outside AP Top 25) -- no child row."""
        rows = list(massey.parse(_read_fixture(), _good_ctx()))
        child_rows = [r for r in rows if r.get("_table") == "massey_system_ratings"]
        ap_rows = [r for r in child_rows if r["team"] == "Louisville" and r["system_code"] == "AP"]
        assert ap_rows == []

    def test_ap_blank_across_all_unranked_teams(self):
        """Per FINDINGS.md, Louisville/Oklahoma St/Utah St/Akron/Massachusetts all
        have a blank AP cell; the fully-ranked top-5 teams do have an AP child row."""
        rows = list(massey.parse(_read_fixture(), _good_ctx()))
        child_rows = [r for r in rows if r.get("_table") == "massey_system_ratings"]
        ap_teams = {r["team"] for r in child_rows if r["system_code"] == "AP"}
        assert ap_teams == {"Georgia", "Michigan", "Ohio St", "Alabama", "Tennessee"}

    def test_composite_row_field_set(self):
        rows = list(massey.parse(_read_fixture(), _good_ctx()))
        composite_rows = [r for r in rows if "composite_rank" in r]
        for row in composite_rows:
            assert "_table" not in row
            assert set(row) == {
                "season",
                "snapshot_date",
                "team",
                "composite_rank",
                "rating_mean",
                "rating_median",
                "rating_stdev",
                "n_systems",
            }


class TestStaleSnapshot:
    def test_raises_when_seasons_differ(self):
        with pytest.raises(StaleSnapshotError, match=r"2023-01-09") as exc_info:
            list(massey.parse(_read_fixture(), _good_ctx(snapshot_date=STALE_SNAPSHOT)))
        assert "2023-09-01" in str(exc_info.value)

    def test_not_raised_when_seasons_align(self):
        # Should not raise -- both the thru-date and snapshot_date fall in season 2022.
        rows = list(massey.parse(_read_fixture(), _good_ctx(snapshot_date=ALIGNED_SNAPSHOT)))
        assert len(rows) > 0

    def test_ctx_season_overrides_snapshot_date_side(self):
        """ctx.season=2022 lines up with the file even though snapshot_date is season 2023."""
        ctx = ParseContext(source="massey", snapshot_date=STALE_SNAPSHOT, season=2022)
        rows = list(massey.parse(_read_fixture(), ctx))
        assert len(rows) > 0

    def test_ctx_season_mismatch_still_raises(self):
        """ctx.season=2021 does not match the file's season 2022, even with an aligned
        snapshot_date."""
        ctx = ParseContext(source="massey", snapshot_date=ALIGNED_SNAPSHOT, season=2021)
        with pytest.raises(StaleSnapshotError):
            list(massey.parse(_read_fixture(), ctx))


class TestStructuralErrors:
    def test_missing_thru_date_line(self):
        raw = b"College Football Ranking Comparison\nnot a date line at all\n"
        with pytest.raises(ParserStructureError):
            list(massey.parse(raw, _good_ctx()))

    def test_unparseable_thru_date(self):
        raw = (
            b"College Football Ranking Comparison\n"
            b"Thru games of Monday, Smarch 9, 2023\n"
            b"compiled by: someone\n"
        )
        with pytest.raises(ParserStructureError):
            list(massey.parse(raw, _good_ctx()))

    def test_legend_header_column_count_mismatch(self):
        """Header declares 3 system columns but the legend only lists 2."""
        bad_header = "Team, Conf, WL, Rank, Mean, Trimmed, Median, StDev, AND, AP, ARG,\n"
        raw = _good_body(header=bad_header).encode()
        with pytest.raises(ParserStructureError, match="header system columns"):
            list(massey.parse(raw, _good_ctx()))

    def test_legend_header_column_order_mismatch(self):
        """Header lists systems in a different order than the legend."""
        bad_header = "Team, Conf, WL, Rank, Mean, Trimmed, Median, StDev, AP, AND,\n"
        raw = _good_body(header=bad_header).encode()
        with pytest.raises(ParserStructureError, match="header system columns"):
            list(massey.parse(raw, _good_ctx()))

    def test_matrix_row_width_mismatch(self):
        """Data row has only 1 system column value instead of the declared 2."""
        bad_row = "Georgia          ,SEC,  15-0,   1,   1.00,   1.00,   1.0,   0.00,   1,\n"
        raw = _good_body(row=bad_row).encode()
        with pytest.raises(ParserStructureError, match="row has"):
            list(massey.parse(raw, _good_ctx()))

    def test_unparseable_consensus_block(self):
        """Rank field is non-numeric."""
        bad_row = "Georgia          ,SEC,  15-0, N/A,   1.00,   1.00,   1.0,   0.00,   1,   1,\n"
        raw = _good_body(row=bad_row).encode()
        with pytest.raises(ParserStructureError, match="consensus block"):
            list(massey.parse(raw, _good_ctx()))

    def test_unparseable_system_rank(self):
        """A system column cell has garbage instead of an integer rank or blank."""
        bad_row = "Georgia          ,SEC,  15-0,   1,   1.00,   1.00,   1.0,   0.00, xx,   1,\n"
        raw = _good_body(row=bad_row).encode()
        with pytest.raises(ParserStructureError, match="unparseable system rank"):
            list(massey.parse(raw, _good_ctx()))

    def test_zero_teams_parsed(self):
        """Valid preamble/legend/header but no team rows at all."""
        raw = (_GOOD_PREAMBLE + _GOOD_LEGEND + "\n" + _GOOD_HEADER).encode()
        with pytest.raises(ParserStructureError, match="no team rows"):
            list(massey.parse(raw, _good_ctx()))

    def test_missing_legend_section(self):
        """No legend lines between the preamble and the header."""
        raw = (_GOOD_PREAMBLE + _GOOD_HEADER + "\n" + _GOOD_ROW).encode()
        with pytest.raises(ParserStructureError):
            list(massey.parse(raw, _good_ctx()))

    def test_missing_trailing_comma_on_header(self):
        """Header row missing its guaranteed trailing empty column."""
        bad_header = "Team, Conf, WL, Rank, Mean, Trimmed, Median, StDev, AND, AP\n"
        raw = _good_body(header=bad_header).encode()
        with pytest.raises(ParserStructureError, match="trailing comma"):
            list(massey.parse(raw, _good_ctx()))

    def test_good_inline_fixture_parses_cleanly(self):
        """Sanity check: the hand-built 'good' pieces above actually parse without error,
        proving the bad-input tests fail for the intended reason and not a typo."""
        raw = _good_body().encode()
        rows = list(massey.parse(raw, _good_ctx()))
        composite = [r for r in rows if "composite_rank" in r]
        assert len(composite) == 1
        assert composite[0]["team"] == "Georgia"
        assert composite[0]["n_systems"] == 2
