"""Tests for the NCAA (stats.ncaa.org) bundle parsers (B6a).

Tests parse_schedule/parse_teams/parse_rosters/parse_linescores/
parse_player_stats/parse_team_stats/parse_pbp against small fixtures sliced
from the real downloaded ncaa_mfb_* release files
(tests/fixtures/flatfiles/ncaa_*_sample.parquet), plus synthetic structural
edge cases constructed inline with pyarrow, mirroring
test_flatfile_sportsdataverse.py's conventions.
"""

import io
import logging
from datetime import date

import pytest

pytest.importorskip("pyarrow", reason="flatfiles extra not installed")

import pyarrow as pa
import pyarrow.parquet

from src.pipelines.sources.flat_files import ParseContext, ParserStructureError
from src.pipelines.sources.flatfile_parsers import ncaa

FIXTURE_DIR = "tests/fixtures/flatfiles"


def _read_fixture(name: str) -> bytes:
    with open(f"{FIXTURE_DIR}/{name}", "rb") as f:
        return f.read()


def _ctx(source: str, season: int = 2025) -> ParseContext:
    return ParseContext(source=source, snapshot_date=date(2026, 8, 29), season=season)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


class TestBlankToNone:
    def test_passes_through_none(self):
        assert ncaa._blank_to_none(None) is None

    def test_empty_string_becomes_none(self):
        assert ncaa._blank_to_none("") is None

    def test_whitespace_only_string_becomes_none(self):
        assert ncaa._blank_to_none("   ") is None

    def test_non_blank_string_passes_through(self):
        assert ncaa._blank_to_none("6-0") == "6-0"

    def test_non_string_passes_through(self):
        assert ncaa._blank_to_none(7) == 7


class TestToFloat:
    def test_passes_through_none(self):
        assert ncaa._to_float(None) is None

    def test_casts_plain_numeric_string(self):
        assert ncaa._to_float("130.23") == 130.23

    def test_strips_thousands_comma(self):
        assert ncaa._to_float("1,043.20") == 1043.20

    def test_casts_native_number(self):
        assert ncaa._to_float(7) == 7.0

    def test_empty_string_returns_none(self):
        """Live crash: ncaa_mfb_rosters_2025 shipped '' for some numeric-ish
        fields -- float('') raises ValueError without this guard."""
        assert ncaa._to_float("") is None

    def test_whitespace_only_string_returns_none(self):
        assert ncaa._to_float("   ") is None


class TestToInt:
    def test_passes_through_none(self):
        assert ncaa._to_int(None) is None

    def test_casts_numeric_string(self):
        assert ncaa._to_int("42") == 42

    def test_casts_native_number(self):
        assert ncaa._to_int(42) == 42

    def test_empty_string_returns_none(self):
        assert ncaa._to_int("") is None

    def test_whitespace_only_string_returns_none(self):
        assert ncaa._to_int("  ") is None


class TestHeightInches:
    def test_passes_through_none(self):
        assert ncaa._height_inches(None) is None

    def test_parses_feet_inches(self):
        assert ncaa._height_inches("6-0") == 72.0
        assert ncaa._height_inches("5-11") == 71.0

    def test_empty_string_returns_none(self):
        """Exact live failure: ledger row ncaa_rosters:2025 --
        `could not convert string to float: ''` from ``float('')`` on a
        blank height value."""
        assert ncaa._height_inches("") is None

    def test_whitespace_only_string_returns_none(self):
        assert ncaa._height_inches("   ") is None


class TestParseDates:
    def test_parse_mmddyyyy(self):
        assert ncaa._parse_mmddyyyy("08/29/2025") == date(2025, 8, 29)

    def test_parse_mmddyyyy_none(self):
        assert ncaa._parse_mmddyyyy(None) is None

    def test_parse_game_date_drops_time_component(self):
        assert ncaa._parse_game_date("10/25/2025 03:30 PM") == date(2025, 10, 25)

    def test_parse_game_date_none(self):
        assert ncaa._parse_game_date(None) is None


class TestAssignRowSeq:
    def test_distinct_keys_all_get_zero(self):
        rows = [{"k": "a"}, {"k": "b"}, {"k": "c"}]
        ncaa._assign_row_seq(rows, ("k",))
        assert [r["row_seq"] for r in rows] == [0, 0, 0]

    def test_repeated_key_increments(self):
        rows = [{"k": "a"}, {"k": "a"}, {"k": "a"}, {"k": "b"}]
        ncaa._assign_row_seq(rows, ("k",))
        assert [r["row_seq"] for r in rows] == [0, 1, 2, 0]


# ---------------------------------------------------------------------------
# parse_schedule
# ---------------------------------------------------------------------------


class TestParseSchedule:
    def test_fixture_row_shape(self):
        raw = _read_fixture("ncaa_schedule_sample.parquet")
        rows = list(ncaa.parse_schedule(raw, _ctx("ncaa_schedule")))
        # 4 rows in the fixture: 2 normal (a completed contest, both teams)
        # + 2 canceled (null contest_id) -- the canceled pair is dropped.
        assert len(rows) == 2
        row = next(r for r in rows if r["ncaa_team_id"] == 605986)
        assert row["ncaa_contest_id"] == 6405615
        assert row["ncaa_opponent_id"] == 606059
        assert row["game_date"] == date(2025, 8, 29)
        assert row["espn_game_id"] == 401754374
        assert isinstance(row["espn_game_id"], int)
        assert "contest_id" not in row
        assert "team_id" not in row
        assert "opponent_id" not in row
        assert "date" not in row

    def test_canceled_rows_dropped_with_log(self, caplog):
        raw = _read_fixture("ncaa_schedule_sample.parquet")
        with caplog.at_level(logging.INFO):
            rows = list(ncaa.parse_schedule(raw, _ctx("ncaa_schedule")))
        assert len(rows) == 2
        assert "dropped 2" in caplog.text

    def test_missing_contest_id_column_raises_structure_error(self):
        schema = pa.schema([("team_id", pa.string()), ("season", pa.int64())])
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table({"team_id": ["1"], "season": [2025]}, schema=schema), buf
        )
        with pytest.raises(ParserStructureError, match="missing column.*contest_id"):
            list(ncaa.parse_schedule(buf.getvalue(), _ctx("ncaa_schedule")))

    def test_blank_contest_id_dropped_as_null_pk_component(self, caplog):
        schema = pa.schema(
            [
                ("team_id", pa.string()),
                ("contest_id", pa.string()),
                ("season", pa.int64()),
            ]
        )
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table({"team_id": ["1"], "contest_id": [""], "season": [2025]}, schema=schema),
            buf,
        )
        with caplog.at_level(logging.INFO):
            rows = list(ncaa.parse_schedule(buf.getvalue(), _ctx("ncaa_schedule")))
        assert rows == []
        assert "dropped 1" in caplog.text

    def test_blank_espn_game_id_parses_to_none_no_exception(self):
        """espn_game_id is not a PK component -- blank is a null value, kept
        (not dropped), same _to_int() idiom as everywhere else."""
        schema = pa.schema(
            [
                ("team_id", pa.string()),
                ("contest_id", pa.string()),
                ("espn_game_id", pa.string()),
                ("season", pa.int64()),
            ]
        )
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table(
                {
                    "team_id": ["1"],
                    "contest_id": ["100"],
                    "espn_game_id": [""],
                    "season": [2025],
                },
                schema=schema,
            ),
            buf,
        )
        rows = list(ncaa.parse_schedule(buf.getvalue(), _ctx("ncaa_schedule")))
        assert len(rows) == 1
        assert rows[0]["espn_game_id"] is None


# ---------------------------------------------------------------------------
# parse_teams
# ---------------------------------------------------------------------------


class TestParseTeams:
    def test_fixture_row_shape(self):
        raw = _read_fixture("ncaa_teams_sample.parquet")
        rows = list(ncaa.parse_teams(raw, _ctx("ncaa_teams")))
        assert len(rows) == 6
        divisions = {r["division"] for r in rows}
        assert divisions == {11, 12}
        row = next(r for r in rows if r["team_name"] == "Air Force")
        assert row["ncaa_team_id"] == 606051
        assert "team_id" not in row

    def test_missing_team_id_column_raises_structure_error(self):
        schema = pa.schema([("season", pa.int64())])
        buf = io.BytesIO()
        pyarrow.parquet.write_table(pa.table({"season": [2025]}, schema=schema), buf)
        with pytest.raises(ParserStructureError, match="missing column.*team_id"):
            list(ncaa.parse_teams(buf.getvalue(), _ctx("ncaa_teams")))


# ---------------------------------------------------------------------------
# parse_rosters
# ---------------------------------------------------------------------------


class TestParseRosters:
    def test_fixture_row_shape(self):
        raw = _read_fixture("ncaa_rosters_sample.parquet")
        rows = list(ncaa.parse_rosters(raw, _ctx("ncaa_rosters")))
        assert len(rows) == 6
        hallum = next(r for r in rows if r["player_name"] == "Tyler Hallum")
        assert hallum["ncaa_team_id"] == 605986
        assert hallum["ncaa_player_id"] == 9275620
        assert hallum["height_inches"] == 72.0
        assert "team_id" not in hallum
        assert "player_id" not in hallum
        assert "height" not in hallum

    def test_height_variants_parsed(self):
        raw = _read_fixture("ncaa_rosters_sample.parquet")
        rows = list(ncaa.parse_rosters(raw, _ctx("ncaa_rosters")))
        williams = next(r for r in rows if r["player_name"] == "Britton Williams")
        assert williams["height_inches"] == 71.0  # "5-11"

    def test_missing_player_id_column_raises_structure_error(self):
        schema = pa.schema([("team_id", pa.string()), ("season", pa.int64())])
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table({"team_id": ["1"], "season": [2025]}, schema=schema), buf
        )
        with pytest.raises(ParserStructureError, match="missing column.*player_id"):
            list(ncaa.parse_rosters(buf.getvalue(), _ctx("ncaa_rosters")))

    def test_empty_string_height_parses_to_none_no_exception(self):
        """Reproduces the live ncaa_rosters:2025 failure
        (`could not convert string to float: ''`) -- the checked-in fixture
        slice happens not to contain a blank height row, so this is built
        in-test. A blank height is a null value, not a dropped row (height
        is not a PK component)."""
        schema = pa.schema(
            [
                ("team_id", pa.string()),
                ("player_id", pa.string()),
                ("player_name", pa.string()),
                ("height", pa.string()),
                ("season", pa.int64()),
            ]
        )
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table(
                {
                    "team_id": ["605986"],
                    "player_id": ["9999999"],
                    "player_name": ["Blank Height Player"],
                    "height": [""],
                    "season": [2025],
                },
                schema=schema,
            ),
            buf,
        )
        rows = list(ncaa.parse_rosters(buf.getvalue(), _ctx("ncaa_rosters")))
        assert len(rows) == 1
        assert rows[0]["height_inches"] is None

    def test_whitespace_only_height_parses_to_none_no_exception(self):
        schema = pa.schema(
            [
                ("team_id", pa.string()),
                ("player_id", pa.string()),
                ("player_name", pa.string()),
                ("height", pa.string()),
                ("season", pa.int64()),
            ]
        )
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table(
                {
                    "team_id": ["605986"],
                    "player_id": ["9999998"],
                    "player_name": ["Whitespace Height Player"],
                    "height": ["   "],
                    "season": [2025],
                },
                schema=schema,
            ),
            buf,
        )
        rows = list(ncaa.parse_rosters(buf.getvalue(), _ctx("ncaa_rosters")))
        assert len(rows) == 1
        assert rows[0]["height_inches"] is None

    def test_blank_team_id_dropped_as_null_pk_component(self, caplog):
        """A blank string is a null value -- but team_id is a PK component,
        so it follows the existing null-PK-drop convention (drop + log
        count) rather than loading with a null ncaa_team_id."""
        schema = pa.schema(
            [
                ("team_id", pa.string()),
                ("player_id", pa.string()),
                ("season", pa.int64()),
            ]
        )
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table({"team_id": [""], "player_id": ["1"], "season": [2025]}, schema=schema),
            buf,
        )
        with caplog.at_level(logging.INFO):
            rows = list(ncaa.parse_rosters(buf.getvalue(), _ctx("ncaa_rosters")))
        assert rows == []
        assert "dropped 1" in caplog.text


# ---------------------------------------------------------------------------
# parse_linescores
# ---------------------------------------------------------------------------


class TestParseLinescores:
    def test_fixture_row_shape(self):
        raw = _read_fixture("ncaa_linescores_sample.parquet")
        rows = list(ncaa.parse_linescores(raw, _ctx("ncaa_linescores")))
        assert len(rows) == 10
        row = next(r for r in rows if r["team_name"] == "Temple" and r["period"] == "1")
        assert row["ncaa_contest_id"] == 6386336
        assert row["final_score"] == 38
        assert row["game_date"] == date(2025, 10, 25)
        assert isinstance(row["game_date"], date)
        assert "contest_id" not in row
        assert "team" not in row
        assert "final" not in row

    def test_overtime_period_present(self):
        raw = _read_fixture("ncaa_linescores_sample.parquet")
        rows = list(ncaa.parse_linescores(raw, _ctx("ncaa_linescores")))
        periods = {r["period"] for r in rows}
        assert "1OT" in periods

    def test_missing_team_column_raises_structure_error(self):
        schema = pa.schema([("contest_id", pa.string()), ("season", pa.int64())])
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table({"contest_id": ["1"], "season": [2025]}, schema=schema), buf
        )
        with pytest.raises(ParserStructureError, match="missing column"):
            list(ncaa.parse_linescores(buf.getvalue(), _ctx("ncaa_linescores")))


# ---------------------------------------------------------------------------
# parse_player_stats
# ---------------------------------------------------------------------------


class TestParsePlayerStats:
    def test_fixture_row_shape(self):
        raw = _read_fixture("ncaa_player_stats_sample.parquet")
        rows = list(ncaa.parse_player_stats(raw, _ctx("ncaa_player_stats")))
        assert len(rows) == 6

    def test_comma_thousands_value_parsed(self):
        raw = _read_fixture("ncaa_player_stats_sample.parquet")
        rows = list(ncaa.parse_player_stats(raw, _ctx("ncaa_player_stats")))
        row = next(r for r in rows if r["name"] == "RJ Garcia II")
        assert row["pass_eff"] == 1043.20
        assert isinstance(row["pass_eff"], float)

    def test_duplicate_subtable_rows_disambiguated_by_row_seq(self):
        """Ethan Loss has two category='other' rows (a real upstream quirk --
        one sub-table's columns, then another's) sharing every PK component
        except row_seq."""
        raw = _read_fixture("ncaa_player_stats_sample.parquet")
        rows = list(ncaa.parse_player_stats(raw, _ctx("ncaa_player_stats")))
        loss_rows = [r for r in rows if r["name"] == "Ethan Loss"]
        assert len(loss_rows) == 2
        assert {r["row_seq"] for r in loss_rows} == {0, 1}
        assert all(r["player_number"] == "19" for r in loss_rows)
        assert all(r["category"] == "other" for r in loss_rows)
        # disjoint populated stat columns across the two sub-rows
        by_seq = {r["row_seq"]: r for r in loss_rows}
        assert by_seq[0]["yds"] == 27.0
        assert by_seq[0]["ko_ret"] is None
        assert by_seq[1]["ko_ret"] == 2.0
        assert by_seq[1]["yds"] is None

    def test_null_jersey_number_coalesced_to_team_sentinel(self):
        """Team-total rows (null jersey number) are kept, not dropped, with
        player_number coalesced to the 'TEAM' sentinel."""
        raw = _read_fixture("ncaa_player_stats_sample.parquet")
        rows = list(ncaa.parse_player_stats(raw, _ctx("ncaa_player_stats")))
        team_rows = [r for r in rows if r["player_number"] == ncaa.TEAM_ROW_SENTINEL]
        assert len(team_rows) == 2
        assert {r["row_seq"] for r in team_rows} == {0, 1}
        names = {r["name"] for r in team_rows}
        assert names == {"TEAM", "Butler"}

    def test_espn_game_id_cast_to_int(self):
        raw = _read_fixture("ncaa_player_stats_sample.parquet")
        rows = list(ncaa.parse_player_stats(raw, _ctx("ncaa_player_stats")))
        assert all(isinstance(r["espn_game_id"], int) for r in rows)

    def test_missing_category_column_raises_structure_error(self):
        schema = pa.schema(
            [
                ("contest_id", pa.string()),
                ("team_id", pa.string()),
                ("season", pa.int64()),
            ]
        )
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table({"contest_id": ["1"], "team_id": ["1"], "season": [2025]}, schema=schema), buf
        )
        with pytest.raises(ParserStructureError, match="missing column.*category"):
            list(ncaa.parse_player_stats(buf.getvalue(), _ctx("ncaa_player_stats")))

    def test_empty_string_stat_value_parses_to_none_no_exception(self):
        """Same _to_float() idiom as parse_rosters' height -- hardened
        identically even though the live run loaded this parser's data
        fine (no blank rows in that day's file)."""
        schema = pa.schema(
            [
                ("contest_id", pa.string()),
                ("team_id", pa.string()),
                ("category", pa.string()),
                ("pass_eff", pa.string()),
                ("season", pa.int64()),
            ]
        )
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table(
                {
                    "contest_id": ["1"],
                    "team_id": ["1"],
                    "category": ["passing"],
                    "pass_eff": [""],
                    "season": [2025],
                },
                schema=schema,
            ),
            buf,
        )
        rows = list(ncaa.parse_player_stats(buf.getvalue(), _ctx("ncaa_player_stats")))
        assert len(rows) == 1
        assert rows[0]["pass_eff"] is None

    def test_blank_contest_id_dropped_as_null_pk_component(self, caplog):
        schema = pa.schema(
            [
                ("contest_id", pa.string()),
                ("team_id", pa.string()),
                ("category", pa.string()),
                ("season", pa.int64()),
            ]
        )
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table(
                {"contest_id": [""], "team_id": ["1"], "category": ["passing"], "season": [2025]},
                schema=schema,
            ),
            buf,
        )
        with caplog.at_level(logging.INFO):
            rows = list(ncaa.parse_player_stats(buf.getvalue(), _ctx("ncaa_player_stats")))
        assert rows == []
        assert "dropped 1" in caplog.text


# ---------------------------------------------------------------------------
# parse_team_stats
# ---------------------------------------------------------------------------


class TestParseTeamStats:
    def test_fixture_row_shape(self):
        raw = _read_fixture("ncaa_team_stats_sample.parquet")
        rows = list(ncaa.parse_team_stats(raw, _ctx("ncaa_team_stats")))
        assert len(rows) == 12

    def test_comma_thousands_value_parsed(self):
        raw = _read_fixture("ncaa_team_stats_sample.parquet")
        rows = list(ncaa.parse_team_stats(raw, _ctx("ncaa_team_stats")))
        row = next(r for r in rows if r["ncaa_contest_id"] == 6386509)
        assert row["away_value"] == 1051.60
        assert isinstance(row["away_value"], float)

    def test_overtime_quirk_disambiguated_by_row_seq(self):
        """The confirmed '1stOT' upstream quirk: 6 genuine duplicate
        (contest, category, stat, period) rows, distinguished only by
        row_seq."""
        raw = _read_fixture("ncaa_team_stats_sample.parquet")
        rows = list(ncaa.parse_team_stats(raw, _ctx("ncaa_team_stats")))
        quirk_rows = [r for r in rows if r["stat"] == "1stOT"]
        assert len(quirk_rows) == 6
        assert {r["row_seq"] for r in quirk_rows} == set(range(6))
        assert all(r["ncaa_contest_id"] == 6386336 for r in quirk_rows)
        assert all(r["category"] == "Rushing" for r in quirk_rows)
        assert all(r["period"] == "total" for r in quirk_rows)

    def test_normal_rows_get_row_seq_zero(self):
        raw = _read_fixture("ncaa_team_stats_sample.parquet")
        rows = list(ncaa.parse_team_stats(raw, _ctx("ncaa_team_stats")))
        normal = [r for r in rows if r["ncaa_contest_id"] == 6386278]
        assert len(normal) == 5
        assert all(r["row_seq"] == 0 for r in normal)

    def test_missing_stat_column_raises_structure_error(self):
        schema = pa.schema(
            [
                ("contest_id", pa.string()),
                ("category", pa.string()),
                ("period", pa.string()),
                ("season", pa.int64()),
            ]
        )
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table(
                {"contest_id": ["1"], "category": ["x"], "period": ["total"], "season": [2025]},
                schema=schema,
            ),
            buf,
        )
        with pytest.raises(ParserStructureError, match="missing column.*stat"):
            list(ncaa.parse_team_stats(buf.getvalue(), _ctx("ncaa_team_stats")))

    def test_empty_string_value_parses_to_none_no_exception(self):
        schema = pa.schema(
            [
                ("contest_id", pa.string()),
                ("category", pa.string()),
                ("stat", pa.string()),
                ("period", pa.string()),
                ("away_value", pa.string()),
                ("season", pa.int64()),
            ]
        )
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table(
                {
                    "contest_id": ["1"],
                    "category": ["Rushing"],
                    "stat": ["Yards"],
                    "period": ["total"],
                    "away_value": [""],
                    "season": [2025],
                },
                schema=schema,
            ),
            buf,
        )
        rows = list(ncaa.parse_team_stats(buf.getvalue(), _ctx("ncaa_team_stats")))
        assert len(rows) == 1
        assert rows[0]["away_value"] is None


# ---------------------------------------------------------------------------
# parse_pbp
# ---------------------------------------------------------------------------


class TestParsePbp:
    def test_fixture_row_shape(self):
        raw = _read_fixture("ncaa_pbp_sample.parquet")
        rows = list(ncaa.parse_pbp(raw, _ctx("ncaa_pbp")))
        assert len(rows) == 7
        first = rows[0]
        assert first["ncaa_contest_id"] == 6386278
        assert first["drive_number"] == 1
        assert first["play_number"] == 1
        assert first["play_type"] == "kickoff"
        assert isinstance(first["drive_scored"], bool)
        assert "contest_id" not in first

    def test_espn_game_id_cast_to_int(self):
        raw = _read_fixture("ncaa_pbp_sample.parquet")
        rows = list(ncaa.parse_pbp(raw, _ctx("ncaa_pbp")))
        assert all(isinstance(r["espn_game_id"], int) for r in rows)

    def test_missing_drive_number_column_raises_structure_error(self):
        schema = pa.schema(
            [
                ("contest_id", pa.string()),
                ("play_number", pa.int64()),
                ("season", pa.int64()),
            ]
        )
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table({"contest_id": ["1"], "play_number": [1], "season": [2025]}, schema=schema),
            buf,
        )
        with pytest.raises(ParserStructureError, match="missing column.*drive_number"):
            list(ncaa.parse_pbp(buf.getvalue(), _ctx("ncaa_pbp")))


# ---------------------------------------------------------------------------
# Registry-level sanity checks (no network)
# ---------------------------------------------------------------------------


class TestRegistry:
    NCAA_SOURCES = (
        "ncaa_schedule",
        "ncaa_teams",
        "ncaa_rosters",
        "ncaa_linescores",
        "ncaa_player_stats",
        "ncaa_team_stats",
        "ncaa_pbp",
    )

    def test_registry_entries_present(self):
        from src.pipelines.sources.flat_files import REGISTRY

        for name in self.NCAA_SOURCES:
            assert name in REGISTRY
            spec = REGISTRY[name]
            assert spec.kind == "dlt"
            assert spec.write_disposition == "merge"
            assert spec.schema == "ncaa"
            assert spec.uses_xwalk is False

    def test_resolve_parser_for_each_entry(self):
        from src.pipelines.sources.flat_files import REGISTRY, resolve_parser

        for name in self.NCAA_SOURCES:
            resolve_parser(REGISTRY[name].parser)
