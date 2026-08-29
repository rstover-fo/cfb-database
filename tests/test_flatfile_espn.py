"""Tests for the ESPN player-grain bundle parsers (B6b).

Tests parse_player_passing/parse_player_rushing/parse_player_receiving/
parse_player_defense/parse_play_participants against small fixtures sliced
from the real downloaded espn_cfb_adv_*/espn_cfb_play_participants release
files (tests/fixtures/flatfiles/espn_*_sample.parquet), plus synthetic
structural edge cases constructed inline with pyarrow, mirroring
test_flatfile_ncaa.py's conventions.
"""

import io
from datetime import date

import pytest

pytest.importorskip("pyarrow", reason="flatfiles extra not installed")

import pyarrow as pa
import pyarrow.parquet

from src.pipelines.sources.flat_files import ParseContext, ParserStructureError
from src.pipelines.sources.flatfile_parsers import espn

FIXTURE_DIR = "tests/fixtures/flatfiles"


def _read_fixture(name: str) -> bytes:
    with open(f"{FIXTURE_DIR}/{name}", "rb") as f:
        return f.read()


def _ctx(source: str, season: int = 2025) -> ParseContext:
    return ParseContext(source=source, snapshot_date=date(2026, 8, 29), season=season)


# ---------------------------------------------------------------------------
# parse_player_passing
# ---------------------------------------------------------------------------


class TestParsePlayerPassing:
    def test_fixture_row_shape(self):
        raw = _read_fixture("espn_adv_passing_sample.parquet")
        rows = list(espn.parse_player_passing(raw, _ctx("espn_player_passing")))
        assert len(rows) == 8
        row = next(r for r in rows if r["passer_player_name"] == "Cole Payton")
        assert row["game_id"] == 401767509
        assert row["pos_team_id"] == 2449
        assert row["season"] == 2025
        # renamed Title_Case source columns land under the migration's names
        assert row["comp"] == 11
        assert row["att"] == 17
        assert row["comp_pct"] == pytest.approx(0.6470588235294118)
        assert row["epa_per_play"] == 0.44
        assert row["x_comp"] == pytest.approx(9.863967210054398)
        assert row["x_comp_pct"] == pytest.approx(0.5802333652973175)
        assert row["pass_td"] == 1
        # raw Title_Case keys must not survive the rename
        for raw_key in ("Comp", "Att", "CompPct", "EPA_per_Play", "xComp", "xCompPct", "Pass_TD"):
            assert raw_key not in row

    def test_no_athlete_id_column(self):
        """This dataset carries NO athlete id at all -- only a free-text name."""
        raw = _read_fixture("espn_adv_passing_sample.parquet")
        rows = list(espn.parse_player_passing(raw, _ctx("espn_player_passing")))
        for row in rows:
            assert "passer_player_id" not in row
            assert "athlete_id" not in row

    def test_passer_player_name_never_null(self):
        raw = _read_fixture("espn_adv_passing_sample.parquet")
        rows = list(espn.parse_player_passing(raw, _ctx("espn_player_passing")))
        assert all(r["passer_player_name"] is not None for r in rows)

    def test_missing_passer_player_name_column_raises_structure_error(self):
        schema = pa.schema([("game_id", pa.int64()), ("pos_team_id", pa.int64())])
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table({"game_id": [1], "pos_team_id": [1]}, schema=schema), buf
        )
        with pytest.raises(ParserStructureError, match="missing column.*passer_player_name"):
            list(espn.parse_player_passing(buf.getvalue(), _ctx("espn_player_passing")))


# ---------------------------------------------------------------------------
# parse_player_rushing
# ---------------------------------------------------------------------------


class TestParsePlayerRushing:
    def test_fixture_row_shape(self):
        raw = _read_fixture("espn_adv_rushing_sample.parquet")
        rows = list(espn.parse_player_rushing(raw, _ctx("espn_player_rushing")))
        assert len(rows) == 8
        row = next(r for r in rows if r["rusher_player_name"] == "Barika Kpeenu")
        assert row["car"] == 12
        assert row["yds"] == 79.0
        assert "Car" not in row
        assert "Yds" not in row

    def test_null_name_coalesced_to_sentinel(self):
        """A real unattributed-rusher row (nonzero yardage, no resolvable name)
        is coalesced to UNATTRIBUTED_PLAYER, not dropped."""
        raw = _read_fixture("espn_adv_rushing_sample.parquet")
        rows = list(espn.parse_player_rushing(raw, _ctx("espn_player_rushing")))
        unattributed = [r for r in rows if r["rusher_player_name"] == espn.UNATTRIBUTED_PLAYER]
        assert len(unattributed) == 2
        # these carry real play data, not sentinel/placeholder stats
        assert all(r["car"] == 1 for r in unattributed)
        assert all(r["yds"] is not None for r in unattributed)

    def test_no_athlete_id_column(self):
        raw = _read_fixture("espn_adv_rushing_sample.parquet")
        rows = list(espn.parse_player_rushing(raw, _ctx("espn_player_rushing")))
        for row in rows:
            assert "rusher_player_id" not in row

    def test_missing_pos_team_id_column_raises_structure_error(self):
        schema = pa.schema([("game_id", pa.int64())])
        buf = io.BytesIO()
        pyarrow.parquet.write_table(pa.table({"game_id": [1]}, schema=schema), buf)
        with pytest.raises(ParserStructureError, match="missing column.*pos_team_id"):
            list(espn.parse_player_rushing(buf.getvalue(), _ctx("espn_player_rushing")))


# ---------------------------------------------------------------------------
# parse_player_receiving
# ---------------------------------------------------------------------------


class TestParsePlayerReceiving:
    def test_fixture_row_shape(self):
        raw = _read_fixture("espn_adv_receiving_sample.parquet")
        rows = list(espn.parse_player_receiving(raw, _ctx("espn_player_receiving")))
        assert len(rows) == 8
        row = next(r for r in rows if r["receiver_player_name"] == "Bryce Lance")
        assert row["rec"] == 4
        assert row["tar"] == 6
        assert "Rec" not in row
        assert "Tar" not in row

    def test_null_name_coalesced_to_sentinel(self):
        raw = _read_fixture("espn_adv_receiving_sample.parquet")
        rows = list(espn.parse_player_receiving(raw, _ctx("espn_player_receiving")))
        unattributed = [r for r in rows if r["receiver_player_name"] == espn.UNATTRIBUTED_PLAYER]
        assert len(unattributed) == 2

    def test_no_athlete_id_column(self):
        raw = _read_fixture("espn_adv_receiving_sample.parquet")
        rows = list(espn.parse_player_receiving(raw, _ctx("espn_player_receiving")))
        for row in rows:
            assert "receiver_player_id" not in row

    def test_missing_pos_team_id_column_raises_structure_error(self):
        schema = pa.schema([("game_id", pa.int64())])
        buf = io.BytesIO()
        pyarrow.parquet.write_table(pa.table({"game_id": [1]}, schema=schema), buf)
        with pytest.raises(ParserStructureError, match="missing column.*pos_team_id"):
            list(espn.parse_player_receiving(buf.getvalue(), _ctx("espn_player_receiving")))


# ---------------------------------------------------------------------------
# parse_player_defense
# ---------------------------------------------------------------------------


class TestParsePlayerDefense:
    def test_fixture_row_shape(self):
        raw = _read_fixture("espn_adv_defensive_players_sample.parquet")
        rows = list(espn.parse_player_defense(raw, _ctx("espn_player_defense")))
        assert len(rows) == 6
        row = next(r for r in rows if r["player_name"] == "Jack Iuliano")
        assert row["sacks"] == 1
        assert row["game_id"] is not None
        assert row["def_pos_team_id"] is not None

    def test_null_stat_columns_stay_null_not_zero(self):
        """NULL-never-0: an unreported stat category stays NULL."""
        raw = _read_fixture("espn_adv_defensive_players_sample.parquet")
        rows = list(espn.parse_player_defense(raw, _ctx("espn_player_defense")))
        row = next(r for r in rows if r["player_name"] == "Jack Iuliano")
        assert row["interceptions"] is None
        assert row["forced_fumbles"] is None

    def test_scrape_artifact_name_kept_verbatim(self):
        """A confirmed real ESPN scrape artifact: play-text leaking into
        player_name. Not cleaned by this parser."""
        raw = _read_fixture("espn_adv_defensive_players_sample.parquet")
        rows = list(espn.parse_player_defense(raw, _ctx("espn_player_defense")))
        names = {r["player_name"] for r in rows}
        assert any("End Of Play" in n for n in names)

    def test_player_name_never_null(self):
        raw = _read_fixture("espn_adv_defensive_players_sample.parquet")
        rows = list(espn.parse_player_defense(raw, _ctx("espn_player_defense")))
        assert all(r["player_name"] is not None for r in rows)

    def test_no_athlete_id_column(self):
        raw = _read_fixture("espn_adv_defensive_players_sample.parquet")
        rows = list(espn.parse_player_defense(raw, _ctx("espn_player_defense")))
        for row in rows:
            assert "player_id" not in row
            assert "athlete_id" not in row

    def test_missing_player_name_column_raises_structure_error(self):
        schema = pa.schema([("game_id", pa.int64()), ("def_pos_team_id", pa.int64())])
        buf = io.BytesIO()
        pyarrow.parquet.write_table(
            pa.table({"game_id": [1], "def_pos_team_id": [1]}, schema=schema), buf
        )
        with pytest.raises(ParserStructureError, match="missing column.*player_name"):
            list(espn.parse_player_defense(buf.getvalue(), _ctx("espn_player_defense")))


# ---------------------------------------------------------------------------
# parse_play_participants
# ---------------------------------------------------------------------------


class TestParsePlayParticipants:
    def test_fixture_row_shape(self):
        raw = _read_fixture("espn_play_participants_sample.parquet")
        rows = list(espn.parse_play_participants(raw, _ctx("espn_play_participants")))
        assert len(rows) == 7
        row = next(r for r in rows if r["play_id"] == 401767509101849903)
        assert row["game_id"] == 401767509
        assert row["kicker_player_name"] == "Ben Barnes"

    def test_player_id_cast_to_int(self):
        raw = _read_fixture("espn_play_participants_sample.parquet")
        rows = list(espn.parse_play_participants(raw, _ctx("espn_play_participants")))
        row = next(r for r in rows if r["play_id"] == 401767509101849903)
        assert row["kicker_player_id"] == 5095796
        assert isinstance(row["kicker_player_id"], int)
        assert isinstance(row["returner_player_id"], int)

    def test_no_team_id_or_position_column(self):
        """This dataset carries NO team id/name and NO position column at all."""
        raw = _read_fixture("espn_play_participants_sample.parquet")
        rows = list(espn.parse_play_participants(raw, _ctx("espn_play_participants")))
        for row in rows:
            assert "team_id" not in row
            assert "pos_team_id" not in row
            assert "position" not in row

    def test_list_family_kept_as_raw_text(self):
        """{type}_player_names/_ids are Python repr() string literals, not
        parsed into a real list/array here."""
        raw = _read_fixture("espn_play_participants_sample.parquet")
        rows = list(espn.parse_play_participants(raw, _ctx("espn_play_participants")))
        row = next(r for r in rows if r["play_id"] == 401767509101849903)
        assert row["kicker_player_names"] == "['Ben Barnes']"
        assert isinstance(row["kicker_player_names"], str)

    def test_missing_participant_type_in_older_season_is_none(self):
        """The 'fumbler' type was added upstream after 2014 -- a row lacking
        it entirely (schema drift) must not raise, just read as None."""
        raw = _read_fixture("espn_play_participants_sample.parquet")
        rows = list(espn.parse_play_participants(raw, _ctx("espn_play_participants")))
        no_fumble_row = next(r for r in rows if r["play_id"] == 401767509101849903)
        assert no_fumble_row.get("fumbler_player_id") is None

    def test_missing_play_id_column_raises_structure_error(self):
        schema = pa.schema([("game_id", pa.int64())])
        buf = io.BytesIO()
        pyarrow.parquet.write_table(pa.table({"game_id": [1]}, schema=schema), buf)
        with pytest.raises(ParserStructureError, match="missing column.*play_id"):
            list(espn.parse_play_participants(buf.getvalue(), _ctx("espn_play_participants")))


# ---------------------------------------------------------------------------
# Registry-level sanity checks (no network)
# ---------------------------------------------------------------------------


class TestRegistry:
    ESPN_SOURCES = (
        "espn_player_passing",
        "espn_player_rushing",
        "espn_player_receiving",
        "espn_player_defense",
        "espn_play_participants",
    )

    def test_registry_entries_present(self):
        from src.pipelines.sources.flat_files import REGISTRY

        for name in self.ESPN_SOURCES:
            assert name in REGISTRY
            spec = REGISTRY[name]
            assert spec.kind == "dlt"
            assert spec.write_disposition == "merge"
            assert spec.schema == "stats"
            assert spec.uses_xwalk is False
            assert spec.url_template is not None
            assert spec.fallback_latest is True

    def test_no_pbp_gap_fill_entry(self):
        """espn_pbp_2002_2003 was dropped -- the dataset does not exist
        (verified live: espn_cfb_pbp's own minimum season is 2004)."""
        from src.pipelines.sources.flat_files import REGISTRY

        assert "espn_pbp_2002_2003" not in REGISTRY
        assert not any(name.startswith("espn_pbp") for name in REGISTRY)

    def test_resolve_parser_for_each_entry(self):
        from src.pipelines.sources.flat_files import REGISTRY, resolve_parser

        for name in self.ESPN_SOURCES:
            resolve_parser(REGISTRY[name].parser)

    def test_min_seasons(self):
        from src.pipelines.sources.flat_files import REGISTRY

        assert REGISTRY["espn_player_passing"].min_season == 2004
        assert REGISTRY["espn_player_rushing"].min_season == 2004
        assert REGISTRY["espn_player_receiving"].min_season == 2004
        assert REGISTRY["espn_player_defense"].min_season == 2004
        assert REGISTRY["espn_play_participants"].min_season == 2014
