"""Tests for sportsdataverse-data parquet parsers (B2 items 1-2).

Tests parse_team_xwalk(), parse_game_xwalk(), parse_fpi_weekly(), and
parse_ratings_weekly() against small fixtures sliced from the real downloaded
files (tests/fixtures/flatfiles/sdv_*_sample.parquet) plus synthetic
edge cases constructed inline with pyarrow, mirroring
test_flatfile_nflverse.py's conventions.
"""

import io
import logging
from datetime import date, datetime

import pytest

pytest.importorskip("pyarrow", reason="flatfiles extra not installed")

import pyarrow as pa
import pyarrow.parquet

from src.pipelines.sources.flat_files import ParseContext, ParserStructureError
from src.pipelines.sources.flatfile_parsers import sportsdataverse

FIXTURE_DIR = "tests/fixtures/flatfiles"


def _read_fixture(name: str) -> bytes:
    with open(f"{FIXTURE_DIR}/{name}", "rb") as f:
        return f.read()


class TestParseTeamXwalk:
    """Tests for parse_team_xwalk()."""

    def test_fixture_row_shape(self):
        raw = _read_fixture("sdv_team_xwalk_sample.parquet")
        ctx = ParseContext(source="sdv_team_xwalk", snapshot_date=date(2026, 8, 29), season=2025)
        rows = list(sportsdataverse.parse_team_xwalk(raw, ctx))
        assert len(rows) == 6
        row = next(r for r in rows if r["norm_key"] == "alabama crimson tide")
        assert row["season"] == 2025
        assert row["espn_team_id"] == 333
        assert isinstance(row["espn_team_id"], int)
        assert row["espn_team"] == "Alabama Crimson Tide"
        assert row["matched_sources"] == "espn+fox+yahoo"
        assert row["xwalk_key"] == "alabama crimson tide#333"

    def test_fixture_null_espn_team_id_passes_through(self):
        """A fox/yahoo-only row (no ESPN match) keeps espn_team_id as None, not dropped."""
        raw = _read_fixture("sdv_team_xwalk_sample.parquet")
        ctx = ParseContext(source="sdv_team_xwalk", snapshot_date=date(2026, 8, 29), season=2025)
        rows = list(sportsdataverse.parse_team_xwalk(raw, ctx))
        null_rows = [r for r in rows if r["espn_team_id"] is None]
        assert len(null_rows) == 1
        assert null_rows[0]["norm_key"] == "penn quakers"
        assert null_rows[0]["fox_team_id"] is not None
        # tiebreak falls through to fox_team_id when espn_team_id is null
        assert null_rows[0]["xwalk_key"] == f"penn quakers#{null_rows[0]['fox_team_id']}"

    def test_fixture_duplicate_norm_key_both_kept(self):
        """The known upstream norm_key collision (roosevelt lakers) is not deduped here --
        that is dlt's merge behavior, not the parser's job. xwalk_key (the actual PK
        component, not norm_key) must be distinct for the two colliding rows, or dlt
        merge would silently drop one."""
        raw = _read_fixture("sdv_team_xwalk_sample.parquet")
        ctx = ParseContext(source="sdv_team_xwalk", snapshot_date=date(2026, 8, 29), season=2025)
        rows = list(sportsdataverse.parse_team_xwalk(raw, ctx))
        roosevelt = [r for r in rows if r["norm_key"] == "roosevelt lakers"]
        assert len(roosevelt) == 2
        assert {r["espn_team_id"] for r in roosevelt} == {599, 127991}
        assert len({r["xwalk_key"] for r in roosevelt}) == 2

    def test_pk_null_norm_key_dropped_with_log(self, caplog):
        schema = pa.schema(
            [
                ("norm_key", pa.string()),
                ("espn_team_id", pa.int64()),
            ]
        )
        data = {"norm_key": ["alabama", None], "espn_team_id": [333, 999]}
        buf = io.BytesIO()
        pyarrow.parquet.write_table(pa.table(data, schema=schema), buf)

        ctx = ParseContext(source="sdv_team_xwalk", snapshot_date=date(2026, 8, 29), season=2025)
        with caplog.at_level(logging.INFO):
            rows = list(sportsdataverse.parse_team_xwalk(buf.getvalue(), ctx))
        assert len(rows) == 1
        assert rows[0]["norm_key"] == "alabama"
        assert "dropped 1" in caplog.text

    def test_missing_norm_key_column_raises_structure_error(self):
        schema = pa.schema([("espn_team_id", pa.int64())])
        buf = io.BytesIO()
        pyarrow.parquet.write_table(pa.table({"espn_team_id": [333]}, schema=schema), buf)

        ctx = ParseContext(source="sdv_team_xwalk", snapshot_date=date(2026, 8, 29), season=2025)
        with pytest.raises(ParserStructureError, match="missing column.*norm_key"):
            list(sportsdataverse.parse_team_xwalk(buf.getvalue(), ctx))

    def test_missing_season_in_context_raises_structure_error(self):
        raw = _read_fixture("sdv_team_xwalk_sample.parquet")
        ctx = ParseContext(source="sdv_team_xwalk", snapshot_date=date(2026, 8, 29), season=None)
        with pytest.raises(ParserStructureError, match="season is required"):
            list(sportsdataverse.parse_team_xwalk(raw, ctx))


class TestParseGameXwalk:
    """Tests for parse_game_xwalk()."""

    def test_fixture_row_shape(self):
        raw = _read_fixture("sdv_game_xwalk_sample.parquet")
        ctx = ParseContext(source="sdv_game_xwalk", snapshot_date=date(2026, 8, 29), season=2025)
        rows = list(sportsdataverse.parse_game_xwalk(raw, ctx))
        assert len(rows) == 4
        row = next(r for r in rows if r["espn_game_id"] == 401756846)
        assert row["season"] == 2025
        assert isinstance(row["espn_game_id"], int)
        assert row["yahoo_date"] == date(2025, 8, 23)
        assert row["espn_date"] == date(2025, 8, 23)

    def test_fixture_rematch_disambiguated_by_yahoo_date(self):
        """Same matchup_key twice (regular season + rematch) yields two distinct
        (season, matchup_key, yahoo_date) keys."""
        raw = _read_fixture("sdv_game_xwalk_sample.parquet")
        ctx = ParseContext(source="sdv_game_xwalk", snapshot_date=date(2026, 8, 29), season=2025)
        rows = list(sportsdataverse.parse_game_xwalk(raw, ctx))
        rematch = [r for r in rows if r["matchup_key"] == "alabama crimson tide|georgia bulldogs"]
        assert len(rematch) == 2
        dates = {r["yahoo_date"] for r in rematch}
        assert dates == {date(2025, 9, 27), date(2025, 12, 6)}

    def test_fixture_null_espn_game_id_passes_through(self):
        """A yahoo-only row (no ESPN/Fox match) keeps espn_game_id/espn_date as None."""
        raw = _read_fixture("sdv_game_xwalk_sample.parquet")
        ctx = ParseContext(source="sdv_game_xwalk", snapshot_date=date(2026, 8, 29), season=2025)
        rows = list(sportsdataverse.parse_game_xwalk(raw, ctx))
        null_rows = [r for r in rows if r["espn_game_id"] is None]
        assert len(null_rows) == 1
        assert null_rows[0]["espn_date"] is None
        assert null_rows[0]["fox_date"] is None
        assert null_rows[0]["yahoo_date"] is not None

    def test_pk_null_matchup_key_dropped_with_log(self, caplog):
        schema = pa.schema(
            [
                ("matchup_key", pa.string()),
                ("yahoo_date", pa.string()),
                ("espn_game_id", pa.int64()),
            ]
        )
        data = {
            "matchup_key": ["a|b", None],
            "yahoo_date": ["2025-08-23", "2025-08-24"],
            "espn_game_id": [1, 2],
        }
        buf = io.BytesIO()
        pyarrow.parquet.write_table(pa.table(data, schema=schema), buf)

        ctx = ParseContext(source="sdv_game_xwalk", snapshot_date=date(2026, 8, 29), season=2025)
        with caplog.at_level(logging.INFO):
            rows = list(sportsdataverse.parse_game_xwalk(buf.getvalue(), ctx))
        assert len(rows) == 1
        assert rows[0]["matchup_key"] == "a|b"
        assert "dropped 1" in caplog.text

    def test_pk_null_yahoo_date_dropped(self):
        schema = pa.schema(
            [
                ("matchup_key", pa.string()),
                ("yahoo_date", pa.string()),
                ("espn_game_id", pa.int64()),
            ]
        )
        data = {
            "matchup_key": ["a|b", "c|d"],
            "yahoo_date": ["2025-08-23", None],
            "espn_game_id": [1, 2],
        }
        buf = io.BytesIO()
        pyarrow.parquet.write_table(pa.table(data, schema=schema), buf)

        ctx = ParseContext(source="sdv_game_xwalk", snapshot_date=date(2026, 8, 29), season=2025)
        rows = list(sportsdataverse.parse_game_xwalk(buf.getvalue(), ctx))
        assert len(rows) == 1
        assert rows[0]["matchup_key"] == "a|b"

    def test_missing_matchup_key_column_raises_structure_error(self):
        schema = pa.schema([("yahoo_date", pa.string())])
        buf = io.BytesIO()
        pyarrow.parquet.write_table(pa.table({"yahoo_date": ["2025-08-23"]}, schema=schema), buf)

        ctx = ParseContext(source="sdv_game_xwalk", snapshot_date=date(2026, 8, 29), season=2025)
        with pytest.raises(ParserStructureError, match="missing column.*matchup_key"):
            list(sportsdataverse.parse_game_xwalk(buf.getvalue(), ctx))

    def test_missing_yahoo_date_column_raises_structure_error(self):
        schema = pa.schema([("matchup_key", pa.string())])
        buf = io.BytesIO()
        pyarrow.parquet.write_table(pa.table({"matchup_key": ["a|b"]}, schema=schema), buf)

        ctx = ParseContext(source="sdv_game_xwalk", snapshot_date=date(2026, 8, 29), season=2025)
        with pytest.raises(ParserStructureError, match="missing column.*yahoo_date"):
            list(sportsdataverse.parse_game_xwalk(buf.getvalue(), ctx))

    def test_missing_season_in_context_raises_structure_error(self):
        raw = _read_fixture("sdv_game_xwalk_sample.parquet")
        ctx = ParseContext(source="sdv_game_xwalk", snapshot_date=date(2026, 8, 29), season=None)
        with pytest.raises(ParserStructureError, match="season is required"):
            list(sportsdataverse.parse_game_xwalk(raw, ctx))


class TestParseFpiWeekly:
    """Tests for parse_fpi_weekly()."""

    def test_fixture_row_shape(self):
        raw = _read_fixture("sdv_fpi_weekly_sample.parquet")
        ctx = ParseContext(source="sdv_fpi_weekly", snapshot_date=date(2026, 8, 29))
        rows = list(sportsdataverse.parse_fpi_weekly(raw, ctx))
        assert len(rows) == 5
        row = next(r for r in rows if r["team_id"] == 333 and r["week"] == 1)
        assert row["season"] == 2025
        assert row["season_type"] == 2
        assert isinstance(row["team_id"], int)
        assert isinstance(row["fpi"], float)
        assert isinstance(row["last_updated"], datetime)

    def test_fixture_projectedt_always_null(self):
        raw = _read_fixture("sdv_fpi_weekly_sample.parquet")
        ctx = ParseContext(source="sdv_fpi_weekly", snapshot_date=date(2026, 8, 29))
        rows = list(sportsdataverse.parse_fpi_weekly(raw, ctx))
        assert all(r["projectedt"] is None for r in rows)

    def test_fixture_sosremainingrank_null_passes_through(self):
        raw = _read_fixture("sdv_fpi_weekly_sample.parquet")
        ctx = ParseContext(source="sdv_fpi_weekly", snapshot_date=date(2026, 8, 29))
        rows = list(sportsdataverse.parse_fpi_weekly(raw, ctx))
        null_sos = [r for r in rows if r["sosremainingrank"] is None]
        assert len(null_sos) >= 1

    def test_fixture_postseason_season_type(self):
        """season_type=3 (postseason) rows are present and week restarts at 1."""
        raw = _read_fixture("sdv_fpi_weekly_sample.parquet")
        ctx = ParseContext(source="sdv_fpi_weekly", snapshot_date=date(2026, 8, 29))
        rows = list(sportsdataverse.parse_fpi_weekly(raw, ctx))
        postseason = [r for r in rows if r["season_type"] == 3]
        assert len(postseason) == 1
        assert postseason[0]["week"] == 1

    def test_pk_null_team_id_dropped_with_log(self, caplog):
        schema = pa.schema(
            [
                ("season", pa.int64()),
                ("season_type", pa.int64()),
                ("week", pa.int64()),
                ("team_id", pa.int64()),
                ("fpi", pa.float64()),
            ]
        )
        data = {
            "season": [2025, 2025],
            "season_type": [2, 2],
            "week": [1, 1],
            "team_id": [333, None],
            "fpi": [20.25, 5.0],
        }
        buf = io.BytesIO()
        pyarrow.parquet.write_table(pa.table(data, schema=schema), buf)

        ctx = ParseContext(source="sdv_fpi_weekly", snapshot_date=date(2026, 8, 29))
        with caplog.at_level(logging.INFO):
            rows = list(sportsdataverse.parse_fpi_weekly(buf.getvalue(), ctx))
        assert len(rows) == 1
        assert rows[0]["team_id"] == 333
        assert "dropped 1" in caplog.text

    @pytest.mark.parametrize("missing_col", ["season", "season_type", "week", "team_id"])
    def test_missing_pk_column_raises_structure_error(self, missing_col):
        all_cols = {
            "season": pa.int64(),
            "season_type": pa.int64(),
            "week": pa.int64(),
            "team_id": pa.int64(),
        }
        del all_cols[missing_col]
        schema = pa.schema(list(all_cols.items()))
        data = {name: [1] for name in all_cols}
        buf = io.BytesIO()
        pyarrow.parquet.write_table(pa.table(data, schema=schema), buf)

        ctx = ParseContext(source="sdv_fpi_weekly", snapshot_date=date(2026, 8, 29))
        with pytest.raises(ParserStructureError, match=f"missing column.*{missing_col}"):
            list(sportsdataverse.parse_fpi_weekly(buf.getvalue(), ctx))


class TestParseRatingsWeekly:
    """Tests for parse_ratings_weekly()."""

    def test_fixture_row_shape(self):
        raw = _read_fixture("sdv_ratings_weekly_sample.parquet")
        ctx = ParseContext(source="sdv_ratings_weekly", snapshot_date=date(2026, 8, 29))
        rows = list(sportsdataverse.parse_ratings_weekly(raw, ctx))
        assert len(rows) == 6
        row = next(r for r in rows if r["team_id"] == 333)
        assert row["season"] == 2025
        assert row["through_week"] == 1
        assert isinstance(row["team_id"], int)
        assert isinstance(row["adj_off_epa"], float)
        assert isinstance(row["games"], int)

    def test_team_id_coerced_from_string(self):
        """Source ships team_id as a string; parser must cast to int."""
        raw = _read_fixture("sdv_ratings_weekly_sample.parquet")
        ctx = ParseContext(source="sdv_ratings_weekly", snapshot_date=date(2026, 8, 29))
        rows = list(sportsdataverse.parse_ratings_weekly(raw, ctx))
        assert all(isinstance(r["team_id"], int) for r in rows)

    def test_pk_null_team_id_dropped_with_log(self, caplog):
        schema = pa.schema(
            [
                ("season", pa.int64()),
                ("through_week", pa.int64()),
                ("team_id", pa.string()),
                ("adj_net", pa.float64()),
            ]
        )
        data = {
            "season": [2025, 2025],
            "through_week": [1, 1],
            "team_id": ["333", None],
            "adj_net": [-0.2, 0.1],
        }
        buf = io.BytesIO()
        pyarrow.parquet.write_table(pa.table(data, schema=schema), buf)

        ctx = ParseContext(source="sdv_ratings_weekly", snapshot_date=date(2026, 8, 29))
        with caplog.at_level(logging.INFO):
            rows = list(sportsdataverse.parse_ratings_weekly(buf.getvalue(), ctx))
        assert len(rows) == 1
        assert rows[0]["team_id"] == 333
        assert "dropped 1" in caplog.text

    @pytest.mark.parametrize("missing_col", ["season", "through_week", "team_id"])
    def test_missing_pk_column_raises_structure_error(self, missing_col):
        all_cols = {
            "season": pa.int64(),
            "through_week": pa.int64(),
            "team_id": pa.string(),
        }
        del all_cols[missing_col]
        schema = pa.schema(list(all_cols.items()))
        data = {name: (["333"] if name == "team_id" else [1]) for name in all_cols}
        buf = io.BytesIO()
        pyarrow.parquet.write_table(pa.table(data, schema=schema), buf)

        ctx = ParseContext(source="sdv_ratings_weekly", snapshot_date=date(2026, 8, 29))
        with pytest.raises(ParserStructureError, match=f"missing column.*{missing_col}"):
            list(sportsdataverse.parse_ratings_weekly(buf.getvalue(), ctx))


class TestRegistry:
    """Registry-level sanity checks (no network)."""

    def test_registry_entries_present(self):
        from src.pipelines.sources.flat_files import REGISTRY

        for name in ("sdv_team_xwalk", "sdv_game_xwalk", "sdv_fpi_weekly", "sdv_ratings_weekly"):
            assert name in REGISTRY
            spec = REGISTRY[name]
            assert spec.kind == "dlt"
            assert spec.write_disposition == "merge"
            assert spec.cadence == "weekly"
            # Migrated to the url_template mechanism in B6a -- no more pinned
            # fetch_url, and fallback_latest handles an unpublished current season.
            assert spec.fetch_url is None
            assert spec.url_template is not None and spec.url_template.startswith("https://")
            assert "{season}" in spec.url_template
            assert spec.fallback_latest is True
            assert spec.uses_xwalk is False

        assert REGISTRY["sdv_fpi_weekly"].table == "espn_fpi_weekly"
        assert REGISTRY["sdv_ratings_weekly"].table == "sdv_ratings_weekly"
        assert REGISTRY["sdv_team_xwalk"].primary_key == ("season", "xwalk_key")

    def test_no_player_xwalk_registry_entry(self):
        """Deliberate: no cfb_rosters_crosswalk-shaped asset exists upstream."""
        from src.pipelines.sources.flat_files import REGISTRY

        assert "sdv_player_xwalk" not in REGISTRY

    def test_resolve_parser_for_each_entry(self):
        from src.pipelines.sources.flat_files import REGISTRY, resolve_parser

        for name in ("sdv_team_xwalk", "sdv_game_xwalk", "sdv_fpi_weekly", "sdv_ratings_weekly"):
            resolve_parser(REGISTRY[name].parser)


class TestUrlTemplateMigration:
    """The four sdv_* specs produce the same URLs today (season resolves to
    2025's asset, the latest actually published) as the pre-B6a pinned
    fetch_url strings, and a 2026 URL once that season is requested/published."""

    EXPECTED_2025 = {
        "sdv_team_xwalk": (
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_crosswalk/cfb_teams_crosswalk_2025.parquet"
        ),
        "sdv_game_xwalk": (
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_crosswalk/cfb_schedule_crosswalk_2025.parquet"
        ),
        "sdv_fpi_weekly": (
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_fpi_weekly/cfb_fpi_weekly_2025.parquet"
        ),
        "sdv_ratings_weekly": (
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_ratings_weekly/cfb_ratings_weekly_2025.parquet"
        ),
    }

    def test_resolves_same_2025_url_as_the_old_pinned_fetch_url(self):
        from src.pipelines.sources.flat_files import REGISTRY, resolve_fetch_url

        for name, expected in self.EXPECTED_2025.items():
            assert resolve_fetch_url(REGISTRY[name], 2025) == expected

    def test_resolves_2026_url_when_that_season_is_requested(self):
        from src.pipelines.sources.flat_files import REGISTRY, resolve_fetch_url

        for name, expected_2025 in self.EXPECTED_2025.items():
            expected_2026 = expected_2025.replace("_2025.parquet", "_2026.parquet")
            assert resolve_fetch_url(REGISTRY[name], 2026) == expected_2026
