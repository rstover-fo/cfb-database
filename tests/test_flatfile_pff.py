"""Unit tests for the PFF Premium Stats manual-CSV lane: parser column
contracts, the season fingerprint guard, registry wiring, resolved-school
xwalk behavior, and the driver's --season requirement. Synthetic fixtures
only -- no real PFF rows, no DB, no network.
"""

from datetime import date

import pytest

import scripts.load_flat_files as load_flat_files
from src.pipelines.sources.flat_files import (
    REGISTRY,
    FlatFileSpec,
    ParseContext,
    ParserStructureError,
    SeasonFingerprintError,
    UnmappedNamesError,
    build_flat_file_source,
    ledger_key,
    resolve_parser,
)
from src.pipelines.sources.flatfile_parsers import pff
from src.pipelines.utils.team_xwalk import XwalkResolver, normalize_name

PFF_SOURCES = [
    "pff_passing_summary",
    "pff_receiving_summary",
    "pff_rushing_summary",
    "pff_offense_blocking",
    "pff_defense_summary",
]

EXPECTED_COLUMN_COUNTS = {
    "passing_summary": 44,
    "receiving_summary": 47,
    "rushing_summary": 47,
    "offense_blocking": 31,
    "defense_summary": 55,
}

IDENTITY_COLUMNS = (
    "player",
    "player_id",
    "position",
    "team_name",
    "franchise_id",
    "player_game_count",
)


def _teams(n: int, extra: tuple[str, ...] = ()) -> list[str]:
    """`n` distinct synthetic team names, with `extra` real marker names first."""
    teams = list(extra)
    teams += [f"TEAM {i:03d}" for i in range(n - len(teams))]
    assert len(teams) == n
    return teams


TEAMS_2023 = _teams(134)
TEAMS_2024 = _teams(134, ("KENNESAW",))
TEAMS_2025 = _teams(136, ("KENNESAW", "DELAWARE", "MO STATE"))


def _csv_bytes(
    family: str,
    teams: list[str],
    *,
    drop_column: str | None = None,
    add_column: str | None = None,
    override: dict[tuple[int, str], str] | None = None,
) -> bytes:
    """Synthetic one-row-per-team CSV honoring (or deliberately breaking)
    the family's column contract. `override` patches cell (row_index, col).
    """
    spec = pff.FAMILY_COLUMNS[family]
    cols = [c for c in spec if c != drop_column]
    if add_column:
        cols.append(add_column)

    lines = [",".join(cols)]
    for i, team in enumerate(teams):
        cells = []
        for col in cols:
            kind = spec.get(col, "text")
            if col == "player":
                value = f"Test Player{i}"
            elif col == "player_id":
                value = str(100000 + i)
            elif col == "franchise_id":
                value = str(5000 + i)
            elif col == "position":
                value = "QB"
            elif col == "team_name":
                value = team
            elif kind == "int":
                value = "3"
            elif kind == "float":
                value = "71.5"
            else:
                value = "x"
            if override and (i, col) in override:
                value = override[(i, col)]
            cells.append(value)
        lines.append(",".join(cells))
    return ("\n".join(lines) + "\n").encode("utf-8")


def _ctx(season: int | None, source: str = "pff_passing_summary") -> ParseContext:
    return ParseContext(source=source, snapshot_date=date(2026, 9, 4), season=season)


# ---------------------------------------------------------------------------
# Column contracts
# ---------------------------------------------------------------------------


class TestColumnContracts:
    def test_family_column_counts_match_validated_exports(self):
        assert {f: len(c) for f, c in pff.FAMILY_COLUMNS.items()} == EXPECTED_COLUMN_COUNTS

    @pytest.mark.parametrize("family", sorted(EXPECTED_COLUMN_COUNTS))
    def test_identity_columns_present_everywhere(self, family):
        for col in IDENTITY_COLUMNS:
            assert col in pff.FAMILY_COLUMNS[family], (family, col)

    def test_good_file_parses_with_season_injected_and_types_cast(self):
        raw = _csv_bytes("passing_summary", TEAMS_2023)
        rows = list(pff.parse_passing_summary(raw, _ctx(2023)))

        assert len(rows) == 134
        first = rows[0]
        assert first["season"] == 2023
        assert isinstance(first["player_id"], int)
        assert isinstance(first["franchise_id"], int)
        assert isinstance(first["attempts"], int)
        assert first["grades_pass"] == pytest.approx(71.5)
        assert first["player"] == "Test Player0"
        assert first["team_name"] == "TEAM 000"

    def test_empty_cell_becomes_none_never_zero(self):
        raw = _csv_bytes("passing_summary", TEAMS_2023, override={(0, "grades_run"): ""})
        rows = list(pff.parse_passing_summary(raw, _ctx(2023)))
        assert rows[0]["grades_run"] is None

    def test_scientific_notation_parses_as_float(self):
        raw = _csv_bytes("rushing_summary", TEAMS_2023, override={(0, "elusive_rating"): "1.0e3"})
        rows = list(pff.parse_rushing_summary(raw, _ctx(2023)))
        assert rows[0]["elusive_rating"] == pytest.approx(1000.0)

    def test_missing_column_fails_loud(self):
        raw = _csv_bytes("passing_summary", TEAMS_2023, drop_column="twp_rate")
        with pytest.raises(ParserStructureError, match="twp_rate"):
            list(pff.parse_passing_summary(raw, _ctx(2023)))

    def test_unexpected_column_fails_loud(self):
        raw = _csv_bytes("passing_summary", TEAMS_2023, add_column="mystery_stat")
        with pytest.raises(ParserStructureError, match="mystery_stat"):
            list(pff.parse_passing_summary(raw, _ctx(2023)))

    def test_wrong_family_header_fails_loud(self):
        raw = _csv_bytes("defense_summary", TEAMS_2023)
        with pytest.raises(ParserStructureError):
            list(pff.parse_passing_summary(raw, _ctx(2023)))

    def test_missing_season_fails_loud(self):
        raw = _csv_bytes("passing_summary", TEAMS_2023)
        with pytest.raises(ParserStructureError, match="season"):
            list(pff.parse_passing_summary(raw, _ctx(None)))

    def test_non_numeric_junk_in_int_column_fails_loud(self):
        raw = _csv_bytes("passing_summary", TEAMS_2023, override={(3, "attempts"): "lots"})
        with pytest.raises(ParserStructureError, match="attempts"):
            list(pff.parse_passing_summary(raw, _ctx(2023)))

    def test_empty_file_fails_loud(self):
        header_only = ",".join(pff.FAMILY_COLUMNS["passing_summary"]).encode() + b"\n"
        with pytest.raises(ParserStructureError, match="no data rows"):
            list(pff.parse_passing_summary(header_only, _ctx(2023)))


# ---------------------------------------------------------------------------
# Season fingerprint guard
# ---------------------------------------------------------------------------


class TestSeasonFingerprint:
    def test_2025_file_claimed_as_2023_fails(self):
        raw = _csv_bytes("passing_summary", TEAMS_2025)
        with pytest.raises(SeasonFingerprintError, match="DELAWARE"):
            list(pff.parse_passing_summary(raw, _ctx(2023)))

    def test_2025_file_claimed_as_2024_fails(self):
        with pytest.raises(SeasonFingerprintError):
            pff.verify_season_fingerprint(set(TEAMS_2025), 2024, "pff_passing_summary")

    def test_2023_file_claimed_as_2024_fails_on_missing_kennesaw(self):
        # The reversed swap: both seasons have 134 teams, so only the
        # marker's ABSENCE can catch a 2023 export labelled 2024.
        with pytest.raises(SeasonFingerprintError, match="KENNESAW is missing"):
            pff.verify_season_fingerprint(set(TEAMS_2023), 2024, "pff_passing_summary")

    def test_2024_file_claimed_as_2025_fails_on_missing_markers(self):
        teams = set(TEAMS_2024) | {"EXTRA A", "EXTRA B"}  # 136 teams, no 2025 markers
        with pytest.raises(SeasonFingerprintError, match="DELAWARE is missing"):
            pff.verify_season_fingerprint(teams, 2025, "pff_passing_summary")

    def test_missing_marker_is_not_checked_for_unvalidated_seasons(self):
        # 2027 is not in EXPECTED_TEAM_COUNTS: a marker could have left FBS,
        # so absence proves nothing there.
        teams = set(TEAMS_2023) | {"EXTRA A", "EXTRA B"}
        pff.verify_season_fingerprint(teams, 2027, "pff_passing_summary")

    def test_2024_file_claimed_as_2023_fails_on_kennesaw(self):
        with pytest.raises(SeasonFingerprintError, match="KENNESAW"):
            pff.verify_season_fingerprint(set(TEAMS_2024), 2023, "pff_passing_summary")

    @pytest.mark.parametrize(
        ("teams", "season"),
        [(TEAMS_2023, 2023), (TEAMS_2024, 2024), (TEAMS_2025, 2025)],
    )
    def test_consistent_claims_pass(self, teams, season):
        pff.verify_season_fingerprint(set(teams), season, "pff_passing_summary")

    def test_unknown_future_season_passes(self):
        # 2026 membership is unknown at authoring time: a 2025-shaped file
        # must not be a provable contradiction for a future season claim.
        pff.verify_season_fingerprint(set(TEAMS_2025), 2026, "pff_passing_summary")

    def test_past_season_without_validated_fingerprint_is_refused(self):
        # 2014-2022 backfills: the count band cannot tell those seasons
        # apart, so a season must be validated (EXPECTED_TEAM_COUNTS) first.
        teams = set(TEAMS_2023[:128])
        with pytest.raises(SeasonFingerprintError, match="no validated FBS fingerprint"):
            pff.verify_season_fingerprint(teams, 2018, "pff_passing_summary")

    def test_pre_2014_claim_fails(self):
        with pytest.raises(SeasonFingerprintError, match="2014"):
            pff.verify_season_fingerprint(set(TEAMS_2023), 2013, "pff_passing_summary")

    def test_known_season_with_wrong_team_count_fails(self):
        with pytest.raises(SeasonFingerprintError, match="134"):
            pff.verify_season_fingerprint(set(TEAMS_2023), 2025, "pff_passing_summary")

    def test_partial_export_fails_count_sanity_even_for_future_season(self):
        with pytest.raises(SeasonFingerprintError, match="distinct teams"):
            pff.verify_season_fingerprint(set(TEAMS_2025[:30]), 2027, "pff_passing_summary")


# ---------------------------------------------------------------------------
# Registry wiring
# ---------------------------------------------------------------------------


class TestRegistryWiring:
    @pytest.mark.parametrize("name", PFF_SOURCES)
    def test_spec_shape(self, name):
        spec = REGISTRY[name]
        family = name.removeprefix("pff_")
        assert spec.schema == "pff"
        assert spec.table == family
        assert spec.primary_key == ("player_id", "season")
        assert spec.cadence == "manual"
        assert spec.fetch_url is None and spec.url_template is None
        assert spec.requires_season is True
        assert spec.uses_xwalk is True
        assert spec.xwalk_fields == ("team_name",)
        assert spec.xwalk_resolved_field == "school"
        assert spec.xwalk_map == ("pff.team_map", "pff_team_name", "cfbd_school")
        assert spec.unmapped_fail_rate == 0.0
        assert callable(resolve_parser(spec.parser))

    @pytest.mark.parametrize("name", PFF_SOURCES)
    def test_ledger_key_is_per_season(self, name):
        assert ledger_key(REGISTRY[name], 2024) == f"{name}:2024"

    def test_non_seasoned_single_file_spec_unchanged(self):
        assert ledger_key(REGISTRY["sbr"], 2024) == "sbr"


# ---------------------------------------------------------------------------
# build_flat_file_source: resolved-school column
# ---------------------------------------------------------------------------


def _resolver_for(teams: list[str]) -> XwalkResolver:
    return XwalkResolver("pff", {normalize_name(t): f"School {t.title()}" for t in teams})


class TestResolvedSchoolColumn:
    def test_school_added_and_team_name_preserved(self):
        spec = REGISTRY["pff_passing_summary"]
        raw = _csv_bytes("passing_summary", TEAMS_2023)
        source = build_flat_file_source(spec, raw, _ctx(2023), _resolver_for(TEAMS_2023))

        rows = list(source.resources["passing_summary"])
        assert len(rows) == 134
        assert rows[0]["team_name"] == "TEAM 000"
        assert rows[0]["school"] == "School Team 000"
        assert source.resources["passing_summary"].write_disposition == "merge"

    def test_single_unmapped_team_fails_whole_load(self):
        spec = REGISTRY["pff_passing_summary"]
        raw = _csv_bytes("passing_summary", TEAMS_2023)
        resolver = _resolver_for(TEAMS_2023[:-1])  # last team unmapped
        with pytest.raises(UnmappedNamesError):
            build_flat_file_source(spec, raw, _ctx(2023), resolver)

    def test_resolved_field_without_map_leaves_legacy_behavior_alone(self):
        # A spec without xwalk_resolved_field keeps overwriting the field
        # in place (massey/sbr behavior) -- guarded here via spec defaults.
        legacy = FlatFileSpec(
            name="toy",
            parser="toy.parse",
            schema="s",
            table="t",
            primary_key=("id",),
            cadence="manual",
        )
        assert legacy.xwalk_resolved_field is None
        assert legacy.xwalk_map is None
        assert legacy.requires_season is False


# ---------------------------------------------------------------------------
# Driver: --season is mandatory for pff --file loads
# ---------------------------------------------------------------------------


class TestDriverSeasonRequirement:
    def test_file_without_season_is_rejected(self, capsys):
        with pytest.raises(SystemExit):
            load_flat_files.main(
                ["--source", "pff_passing_summary", "--file", "passing_summary_2024.csv"]
            )
        assert "--season" in capsys.readouterr().err

    def test_file_with_season_runs_the_source(self, monkeypatch):
        calls = {}

        def fake_run_source(spec, *, file_path, season, season_explicit, today):
            calls.update(
                spec=spec.name, file_path=file_path, season=season, explicit=season_explicit
            )
            return {
                "source": spec.name,
                "status": "loaded",
                "rows": 1,
                "sha": "x",
                "duration_s": 0.0,
                "error": None,
            }

        monkeypatch.setattr(load_flat_files, "run_source", fake_run_source)
        rc = load_flat_files.main(
            ["--source", "pff_passing_summary", "--season", "2024", "--file", "f.csv"]
        )
        assert rc == 0
        assert calls == {
            "spec": "pff_passing_summary",
            "file_path": "f.csv",
            "season": 2024,
            "explicit": True,
        }


class TestXwalkMapTableLoader:
    def test_rejects_non_identifier_parts(self):
        with pytest.raises(ValueError):
            XwalkResolver.load_map_table("pff", "pff.team_map; DROP", "a", "b")
        with pytest.raises(ValueError):
            XwalkResolver.load_map_table("pff", "team_map", "a", "b")  # no schema part
