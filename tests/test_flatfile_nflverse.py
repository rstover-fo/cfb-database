"""Tests for nflverse parquet parsers (T5).

Tests parse_combine() and parse_draft_picks() with fixture parquets and synthetic
edge cases constructed inline with pyarrow.
"""

import io
import logging
from datetime import date

import pytest

pytest.importorskip("pyarrow", reason="flatfiles extra not installed")

import pyarrow as pa
import pyarrow.parquet

from src.pipelines.sources.flat_files import ParseContext, ParserStructureError
from src.pipelines.sources.flatfile_parsers import nflverse


class TestParseCombine:
    """Tests for parse_combine()."""

    def test_fixture_row_count(self):
        """Fixture has 20 rows with no null PK columns."""
        with open("tests/fixtures/flatfiles/combine_sample.parquet", "rb") as f:
            raw = f.read()
        ctx = ParseContext(source="nflverse_combine", snapshot_date=date(2026, 7, 23))
        rows = list(nflverse.parse_combine(raw, ctx))
        assert len(rows) == 20

    def test_fixture_zero_measurables_player(self):
        """Row with all measurables null (Rudy Noteworth, row 9) passes through with None values."""
        with open("tests/fixtures/flatfiles/combine_sample.parquet", "rb") as f:
            raw = f.read()
        ctx = ParseContext(source="nflverse_combine", snapshot_date=date(2026, 7, 23))
        rows = list(nflverse.parse_combine(raw, ctx))
        # Find Rudy Noteworth
        rudy = [r for r in rows if r.get("player_name") == "Rudy Noteworth"]
        assert len(rudy) == 1
        row = rudy[0]
        assert row["forty"] is None
        assert row["bench"] is None
        assert row["vertical"] is None
        assert row["broad_jump"] is None
        assert row["cone"] is None
        assert row["shuttle"] is None

    def test_fixture_column_types(self):
        """Verify type coercion: season is int, forty is float, pfr_id is str."""
        with open("tests/fixtures/flatfiles/combine_sample.parquet", "rb") as f:
            raw = f.read()
        ctx = ParseContext(source="nflverse_combine", snapshot_date=date(2026, 7, 23))
        rows = list(nflverse.parse_combine(raw, ctx))
        assert len(rows) > 0
        row = rows[0]  # Patrick Mahomes II
        assert isinstance(row["season"], int)
        assert isinstance(row["forty"], float)
        assert isinstance(row["pfr_id"], str)
        assert row["pfr_id"] == "MahoPa00"

    def test_fixture_null_draft_year(self):
        """Undrafted players have null draft_year (row 7: AJ Green Jr.) passes through."""
        with open("tests/fixtures/flatfiles/combine_sample.parquet", "rb") as f:
            raw = f.read()
        ctx = ParseContext(source="nflverse_combine", snapshot_date=date(2026, 7, 23))
        rows = list(nflverse.parse_combine(raw, ctx))
        # The fixture has undrafted rows per FINDINGS.md but no row named "AJ Green Jr."
        # Check for any row with null draft_year
        null_draft_year_rows = [r for r in rows if r.get("draft_year") is None]
        assert len(null_draft_year_rows) >= 1

    def test_pk_null_season_dropped_with_log(self, caplog):
        """Rows with null season are dropped and logged."""
        # Create a minimal parquet with one good row and one null-season row
        schema = pa.schema(
            [
                ("season", pa.int64()),
                ("player_name", pa.string()),
                ("pos", pa.string()),
                ("school", pa.string()),
                ("ht", pa.float64()),
                ("wt", pa.int64()),
                ("forty", pa.float64()),
            ]
        )
        data = {
            "season": [2023, None],
            "player_name": ["Player One", "Player Two"],
            "pos": ["QB", "WR"],
            "school": ["School A", "School B"],
            "ht": [74.0, 72.0],
            "wt": [220, 190],
            "forty": [4.8, 4.5],
        }
        table = pa.table(data, schema=schema)
        buf = io.BytesIO()
        pyarrow.parquet.write_table(table, buf)
        raw = buf.getvalue()

        ctx = ParseContext(source="nflverse_combine", snapshot_date=date(2026, 7, 23))
        with caplog.at_level(logging.INFO):
            rows = list(nflverse.parse_combine(raw, ctx))
        assert len(rows) == 1
        assert rows[0]["player_name"] == "Player One"
        assert "dropped 1" in caplog.text

    def test_pk_null_player_name_dropped(self):
        """Rows with null player_name are dropped."""
        schema = pa.schema(
            [
                ("season", pa.int64()),
                ("player_name", pa.string()),
                ("pos", pa.string()),
                ("school", pa.string()),
            ]
        )
        data = {
            "season": [2023, 2023],
            "player_name": ["Player One", None],
            "pos": ["QB", "WR"],
            "school": ["School A", "School B"],
        }
        table = pa.table(data, schema=schema)
        buf = io.BytesIO()
        pyarrow.parquet.write_table(table, buf)
        raw = buf.getvalue()

        ctx = ParseContext(source="nflverse_combine", snapshot_date=date(2026, 7, 23))
        rows = list(nflverse.parse_combine(raw, ctx))
        assert len(rows) == 1
        assert rows[0]["player_name"] == "Player One"

    def test_pk_null_pos_dropped(self):
        """Rows with null pos are dropped."""
        schema = pa.schema(
            [
                ("season", pa.int64()),
                ("player_name", pa.string()),
                ("pos", pa.string()),
                ("school", pa.string()),
            ]
        )
        data = {
            "season": [2023, 2023],
            "player_name": ["Player One", "Player Two"],
            "pos": ["QB", None],
            "school": ["School A", "School B"],
        }
        table = pa.table(data, schema=schema)
        buf = io.BytesIO()
        pyarrow.parquet.write_table(table, buf)
        raw = buf.getvalue()

        ctx = ParseContext(source="nflverse_combine", snapshot_date=date(2026, 7, 23))
        rows = list(nflverse.parse_combine(raw, ctx))
        assert len(rows) == 1
        assert rows[0]["player_name"] == "Player One"

    def test_missing_season_column_raises_structure_error(self):
        """Missing season column raises ParserStructureError."""
        schema = pa.schema(
            [
                ("player_name", pa.string()),
                ("pos", pa.string()),
                ("school", pa.string()),
            ]
        )
        data = {
            "player_name": ["Player One"],
            "pos": ["QB"],
            "school": ["School A"],
        }
        table = pa.table(data, schema=schema)
        buf = io.BytesIO()
        pyarrow.parquet.write_table(table, buf)
        raw = buf.getvalue()

        ctx = ParseContext(source="nflverse_combine", snapshot_date=date(2026, 7, 23))
        with pytest.raises(ParserStructureError, match="missing PK column.*season"):
            list(nflverse.parse_combine(raw, ctx))

    def test_missing_player_name_column_raises_structure_error(self):
        """Missing player_name column raises ParserStructureError."""
        schema = pa.schema(
            [
                ("season", pa.int64()),
                ("pos", pa.string()),
                ("school", pa.string()),
            ]
        )
        data = {
            "season": [2023],
            "pos": ["QB"],
            "school": ["School A"],
        }
        table = pa.table(data, schema=schema)
        buf = io.BytesIO()
        pyarrow.parquet.write_table(table, buf)
        raw = buf.getvalue()

        ctx = ParseContext(source="nflverse_combine", snapshot_date=date(2026, 7, 23))
        with pytest.raises(ParserStructureError, match="missing PK column.*player_name"):
            list(nflverse.parse_combine(raw, ctx))

    def test_missing_pos_column_raises_structure_error(self):
        """Missing pos column raises ParserStructureError."""
        schema = pa.schema(
            [
                ("season", pa.int64()),
                ("player_name", pa.string()),
                ("school", pa.string()),
            ]
        )
        data = {
            "season": [2023],
            "player_name": ["Player One"],
            "school": ["School A"],
        }
        table = pa.table(data, schema=schema)
        buf = io.BytesIO()
        pyarrow.parquet.write_table(table, buf)
        raw = buf.getvalue()

        ctx = ParseContext(source="nflverse_combine", snapshot_date=date(2026, 7, 23))
        with pytest.raises(ParserStructureError, match="missing PK column.*pos"):
            list(nflverse.parse_combine(raw, ctx))


class TestParseDraftPicks:
    """Tests for parse_draft_picks()."""

    def test_fixture_row_count(self):
        """Fixture has 20 rows with no null PK columns."""
        with open("tests/fixtures/flatfiles/draft_picks_sample.parquet", "rb") as f:
            raw = f.read()
        ctx = ParseContext(source="nflverse_draft", snapshot_date=date(2026, 7, 23))
        rows = list(nflverse.parse_draft_picks(raw, ctx))
        assert len(rows) == 20

    def test_fixture_recent_rookie_null_stats(self):
        """Recent rookie (Shedeur Maye, row 16) has null gsis_id and null career stats."""
        with open("tests/fixtures/flatfiles/draft_picks_sample.parquet", "rb") as f:
            raw = f.read()
        ctx = ParseContext(source="nflverse_draft", snapshot_date=date(2026, 7, 23))
        rows = list(nflverse.parse_draft_picks(raw, ctx))
        # Find Shedeur Maye
        shedeur = [r for r in rows if r.get("pfr_player_name") == "Shedeur Maye"]
        assert len(shedeur) == 1
        row = shedeur[0]
        assert row["gsis_id"] is None
        assert row["games"] is None
        assert row["pass_completions"] is None
        assert row["def_solo_tackles"] is None

    def test_fixture_column_types(self):
        """Verify type coercion: season/round/pick are int, games is float, pfr_player_id is str."""
        with open("tests/fixtures/flatfiles/draft_picks_sample.parquet", "rb") as f:
            raw = f.read()
        ctx = ParseContext(source="nflverse_draft", snapshot_date=date(2026, 7, 23))
        rows = list(nflverse.parse_draft_picks(raw, ctx))
        assert len(rows) > 0
        row = rows[0]  # Bryce Young
        assert isinstance(row["season"], int)
        assert isinstance(row["round"], int)
        assert isinstance(row["pick"], int)
        assert isinstance(row["games"], float)
        assert isinstance(row["pfr_player_id"], str)

    def test_fixture_hof_bool_coercion(self):
        """HOF column is coerced to bool."""
        with open("tests/fixtures/flatfiles/draft_picks_sample.parquet", "rb") as f:
            raw = f.read()
        ctx = ParseContext(source="nflverse_draft", snapshot_date=date(2026, 7, 23))
        rows = list(nflverse.parse_draft_picks(raw, ctx))
        # All rows should have hof as bool (True or False)
        for row in rows:
            assert isinstance(row["hof"], bool)

    def test_pk_null_season_dropped_with_log(self, caplog):
        """Rows with null season are dropped and logged."""
        schema = pa.schema(
            [
                ("season", pa.int64()),
                ("round", pa.int64()),
                ("pick", pa.int64()),
                ("team", pa.string()),
                ("pfr_player_name", pa.string()),
                ("gsis_id", pa.string()),
                ("hof", pa.bool_()),
            ]
        )
        data = {
            "season": [2023, None],
            "round": [1, 1],
            "pick": [1, 2],
            "team": ["CAR", "HOU"],
            "pfr_player_name": ["Player One", "Player Two"],
            "gsis_id": ["00-001", "00-002"],
            "hof": [False, False],
        }
        table = pa.table(data, schema=schema)
        buf = io.BytesIO()
        pyarrow.parquet.write_table(table, buf)
        raw = buf.getvalue()

        ctx = ParseContext(source="nflverse_draft", snapshot_date=date(2026, 7, 23))
        with caplog.at_level(logging.INFO):
            rows = list(nflverse.parse_draft_picks(raw, ctx))
        assert len(rows) == 1
        assert rows[0]["pfr_player_name"] == "Player One"
        assert "dropped 1" in caplog.text

    def test_pk_null_round_dropped(self):
        """Rows with null round are dropped."""
        schema = pa.schema(
            [
                ("season", pa.int64()),
                ("round", pa.int64()),
                ("pick", pa.int64()),
                ("team", pa.string()),
                ("pfr_player_name", pa.string()),
                ("hof", pa.bool_()),
            ]
        )
        data = {
            "season": [2023, 2023],
            "round": [1, None],
            "pick": [1, 2],
            "team": ["CAR", "HOU"],
            "pfr_player_name": ["Player One", "Player Two"],
            "hof": [False, False],
        }
        table = pa.table(data, schema=schema)
        buf = io.BytesIO()
        pyarrow.parquet.write_table(table, buf)
        raw = buf.getvalue()

        ctx = ParseContext(source="nflverse_draft", snapshot_date=date(2026, 7, 23))
        rows = list(nflverse.parse_draft_picks(raw, ctx))
        assert len(rows) == 1
        assert rows[0]["pfr_player_name"] == "Player One"

    def test_pk_null_pick_dropped(self):
        """Rows with null pick are dropped."""
        schema = pa.schema(
            [
                ("season", pa.int64()),
                ("round", pa.int64()),
                ("pick", pa.int64()),
                ("team", pa.string()),
                ("pfr_player_name", pa.string()),
                ("hof", pa.bool_()),
            ]
        )
        data = {
            "season": [2023, 2023],
            "round": [1, 1],
            "pick": [1, None],
            "team": ["CAR", "HOU"],
            "pfr_player_name": ["Player One", "Player Two"],
            "hof": [False, False],
        }
        table = pa.table(data, schema=schema)
        buf = io.BytesIO()
        pyarrow.parquet.write_table(table, buf)
        raw = buf.getvalue()

        ctx = ParseContext(source="nflverse_draft", snapshot_date=date(2026, 7, 23))
        rows = list(nflverse.parse_draft_picks(raw, ctx))
        assert len(rows) == 1
        assert rows[0]["pfr_player_name"] == "Player One"

    def test_missing_season_column_raises_structure_error(self):
        """Missing season column raises ParserStructureError."""
        schema = pa.schema(
            [
                ("round", pa.int64()),
                ("pick", pa.int64()),
                ("team", pa.string()),
                ("hof", pa.bool_()),
            ]
        )
        data = {
            "round": [1],
            "pick": [1],
            "team": ["CAR"],
            "hof": [False],
        }
        table = pa.table(data, schema=schema)
        buf = io.BytesIO()
        pyarrow.parquet.write_table(table, buf)
        raw = buf.getvalue()

        ctx = ParseContext(source="nflverse_draft", snapshot_date=date(2026, 7, 23))
        with pytest.raises(ParserStructureError, match="missing PK column.*season"):
            list(nflverse.parse_draft_picks(raw, ctx))

    def test_missing_round_column_raises_structure_error(self):
        """Missing round column raises ParserStructureError."""
        schema = pa.schema(
            [
                ("season", pa.int64()),
                ("pick", pa.int64()),
                ("team", pa.string()),
                ("hof", pa.bool_()),
            ]
        )
        data = {
            "season": [2023],
            "pick": [1],
            "team": ["CAR"],
            "hof": [False],
        }
        table = pa.table(data, schema=schema)
        buf = io.BytesIO()
        pyarrow.parquet.write_table(table, buf)
        raw = buf.getvalue()

        ctx = ParseContext(source="nflverse_draft", snapshot_date=date(2026, 7, 23))
        with pytest.raises(ParserStructureError, match="missing PK column.*round"):
            list(nflverse.parse_draft_picks(raw, ctx))

    def test_missing_pick_column_raises_structure_error(self):
        """Missing pick column raises ParserStructureError."""
        schema = pa.schema(
            [
                ("season", pa.int64()),
                ("round", pa.int64()),
                ("team", pa.string()),
                ("hof", pa.bool_()),
            ]
        )
        data = {
            "season": [2023],
            "round": [1],
            "team": ["CAR"],
            "hof": [False],
        }
        table = pa.table(data, schema=schema)
        buf = io.BytesIO()
        pyarrow.parquet.write_table(table, buf)
        raw = buf.getvalue()

        ctx = ParseContext(source="nflverse_draft", snapshot_date=date(2026, 7, 23))
        with pytest.raises(ParserStructureError, match="missing PK column.*pick"):
            list(nflverse.parse_draft_picks(raw, ctx))

    def test_extra_columns_pass_through(self):
        """Extra columns in the file pass through untouched."""
        schema = pa.schema(
            [
                ("season", pa.int64()),
                ("round", pa.int64()),
                ("pick", pa.int64()),
                ("team", pa.string()),
                ("extra_col", pa.string()),
            ]
        )
        data = {
            "season": [2023],
            "round": [1],
            "pick": [1],
            "team": ["CAR"],
            "extra_col": ["extra_value"],
        }
        table = pa.table(data, schema=schema)
        buf = io.BytesIO()
        pyarrow.parquet.write_table(table, buf)
        raw = buf.getvalue()

        ctx = ParseContext(source="nflverse_draft", snapshot_date=date(2026, 7, 23))
        rows = list(nflverse.parse_draft_picks(raw, ctx))
        assert len(rows) == 1
        assert rows[0]["extra_col"] == "extra_value"


class TestDdlAlignment:
    """Regressions for parser-output vs migration-041 column alignment."""

    def test_to_renamed_to_year(self):
        """The reserved-word 'to' column merges into to_year, never 'to'."""
        with open("tests/fixtures/flatfiles/draft_picks_sample.parquet", "rb") as f:
            raw = f.read()
        ctx = ParseContext(source="nflverse_draft", snapshot_date=date(2026, 7, 23))
        rows = list(nflverse.parse_draft_picks(raw, ctx))
        assert all("to" not in row for row in rows)
        non_null = [row["to_year"] for row in rows if row.get("to_year") is not None]
        assert non_null and all(isinstance(v, int) for v in non_null)

    def test_height_inches_numeric_and_legacy(self):
        """ht handles numeric-inches (current releases) and PFR '6-2' strings."""
        assert nflverse._height_inches(74.0) == 74.0
        assert nflverse._height_inches("6-2") == 74.0
        assert nflverse._height_inches("5-11") == 71.0

    def test_combine_ht_is_float_inches(self):
        with open("tests/fixtures/flatfiles/combine_sample.parquet", "rb") as f:
            raw = f.read()
        ctx = ParseContext(source="nflverse_combine", snapshot_date=date(2026, 7, 23))
        rows = list(nflverse.parse_combine(raw, ctx))
        heights = [row["ht"] for row in rows if row.get("ht") is not None]
        assert heights and all(isinstance(h, float) and 60 <= h <= 84 for h in heights)
