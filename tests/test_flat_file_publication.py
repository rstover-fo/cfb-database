"""Unit coverage for generic receipt-backed flat-file publication."""

from __future__ import annotations

import hashlib
import io
import uuid
from dataclasses import replace
from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.pipelines.config.source_publication_assets import (
    SDV_FPI_PUBLICATION,
    SDV_GAME_XWALK_PUBLICATION,
    SDV_RATINGS_PUBLICATION,
    SDV_TEAM_XWALK_PUBLICATION,
    SOURCE_PUBLICATION_ASSETS,
)
from src.pipelines.sources.flat_files import REGISTRY
from src.pipelines.utils import flat_file_publication as publication
from src.pipelines.utils.file_fetcher import FetchedFile

RUN_ID = "11111111-1111-4111-8111-111111111111"
GENERATION_ID = "22222222-2222-4222-8222-222222222222"


def fpi_row(**overrides):
    row = {column: None for column in SDV_FPI_PUBLICATION.columns}
    row.update(
        season=2025,
        season_type=2,
        week=0,
        team_id=333,
        last_updated=datetime(2025, 8, 1, tzinfo=UTC),
        run_date_time_key=20250801000000,
        snapshot_out_of_sequence=False,
        fpi=1.25,
        snapshot_is_contemporaneous=True,
    )
    row.update(overrides)
    return row


def team_row(**overrides):
    row = {
        "season": 2025,
        "norm_key": "example owls",
        "xwalk_key": "example owls#None",
        "espn_team_id": None,
        "espn_team": None,
        "espn_abbreviation": None,
        "fox_team_id": None,
        "fox_team": None,
        "fox_abbreviation": None,
        "yahoo_team_id": None,
        "yahoo_team": None,
        "yahoo_abbreviation": None,
        "matched_sources": "fox",
    }
    row.update(overrides)
    return row


def game_row(**overrides):
    row = {
        "season": 2025,
        "matchup_key": "away|home",
        "yahoo_date": date(2025, 9, 1),
        "espn_game_id": None,
        "fox_game_id": None,
        "yahoo_game_id": "123",
        "yahoo_global_game_id": "global-123",
        "home_team": "Home Owls",
        "away_team": "Away Bears",
        "espn_date": None,
        "fox_date": None,
        "matched_sources": "yahoo",
    }
    row.update(overrides)
    return row


def plan(asset, season=2025):
    return {
        "protocol": "sdv-season-file-v1",
        "source_name": asset.source_name,
        "asset_key": asset.asset_key,
        "coverage_key": f"season:{season}",
        "season": season,
        "expected_generation_id": None,
        "parser_contract": asset.parser_contract,
    }


def install_ids(monkeypatch):
    values = iter((uuid.UUID(RUN_ID), uuid.UUID(GENERATION_ID)))
    monkeypatch.setattr(publication.uuid, "uuid4", lambda: next(values))


def test_assets_enroll_all_sources_with_stable_season_evidence():
    assert tuple(SOURCE_PUBLICATION_ASSETS) == (
        "sdv_ratings_weekly",
        "sdv_fpi_weekly",
        "sdv_team_xwalk",
        "sdv_game_xwalk",
    )
    assert SDV_RATINGS_PUBLICATION.season_basis("local_file") == "artifact_field"
    assert SDV_FPI_PUBLICATION.season_basis("registered_url") == "artifact_field"
    assert SDV_TEAM_XWALK_PUBLICATION.season_basis("registered_url") == ("registered_artifact_name")
    assert SDV_GAME_XWALK_PUBLICATION.season_basis("local_file") == "caller_declared"
    with pytest.raises(ValueError, match="artifact origin"):
        SDV_FPI_PUBLICATION.season_basis("unknown")


@pytest.mark.parametrize(
    "source,asset",
    [
        ("sdv_fpi_weekly", SDV_FPI_PUBLICATION),
        ("sdv_team_xwalk", SDV_TEAM_XWALK_PUBLICATION),
        ("sdv_game_xwalk", SDV_GAME_XWALK_PUBLICATION),
    ],
)
def test_only_exact_canonical_specs_are_accepted(source, asset):
    assert publication._asset_for(REGISTRY[source], 2025) is asset
    with pytest.raises(ValueError, match="canonical"):
        publication._asset_for(replace(REGISTRY[source]), 2025)


@pytest.mark.parametrize(
    "spec,season",
    [
        (replace(REGISTRY["sdv_fpi_weekly"]), 2025),
        (REGISTRY["sdv_game_xwalk"], 2201),
        (REGISTRY["massey"], 2025),
    ],
)
def test_run_returns_failed_result_when_generic_preflight_rejects(monkeypatch, spec, season):
    install_ids(monkeypatch)
    monkeypatch.setattr(
        publication,
        "get_db_url",
        lambda: pytest.fail("preflight must run before database configuration"),
    )
    result = publication.run_source_publication(spec, season=season)
    assert result["source"] == spec.name
    assert result["status"] == "failed"
    assert result["run_id"] == RUN_ID
    assert result["generation_id"] == GENERATION_ID


def test_plan_requires_exact_source_protocol_and_generation():
    expected = plan(SDV_FPI_PUBLICATION)
    assert publication._validated_plan(expected, SDV_FPI_PUBLICATION, 2025) == expected
    with pytest.raises(publication.SourcePublicationError, match="invalid source plan"):
        publication._validated_plan({**expected, "extra": True}, SDV_FPI_PUBLICATION, 2025)
    with pytest.raises(publication.SourcePublicationError, match="mismatched"):
        publication._validated_plan(
            {**expected, "source_name": "sdv_game_xwalk"}, SDV_FPI_PUBLICATION, 2025
        )
    with pytest.raises(publication.SourcePublicationError, match="invalid expected"):
        publication._validated_plan(
            {**expected, "expected_generation_id": "bad"}, SDV_FPI_PUBLICATION, 2025
        )


def test_team_rows_preserve_all_null_provider_ids_and_verify_derived_key():
    publication._validate_parsed_rows([team_row()], 1, SDV_TEAM_XWALK_PUBLICATION, 2025)
    publication._validate_parsed_rows(
        [team_row(fox_team_id="fox-1", xwalk_key="example owls#fox-1")],
        1,
        SDV_TEAM_XWALK_PUBLICATION,
        2025,
    )
    with pytest.raises(publication.SourcePublicationError, match="derived xwalk_key"):
        publication._validate_parsed_rows(
            [team_row(xwalk_key="wrong")], 1, SDV_TEAM_XWALK_PUBLICATION, 2025
        )


@pytest.mark.parametrize(
    "row,message",
    [
        (fpi_row(last_updated=datetime(2025, 8, 1)), "timezone-aware"),
        (fpi_row(season_type=1), "season_type"),
        (fpi_row(week=-1), "nonnegative"),
        (fpi_row(team_id=0), "positive"),
    ],
)
def test_fpi_domain_and_timezone_validation(row, message):
    with pytest.raises(publication.SourcePublicationError, match=message):
        publication._validate_parsed_rows([row], 1, SDV_FPI_PUBLICATION, 2025)


def _parquet_bytes(row):
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist([row]), buffer)
    return buffer.getvalue()


@pytest.mark.parametrize("added_column", ["season", "xwalk_key"])
def test_team_raw_schema_rejects_conflicting_parser_derived_columns(added_column):
    raw_row = team_row()
    raw_row.pop("season")
    raw_row.pop("xwalk_key")
    raw_row[added_column] = 2024 if added_column == "season" else "upstream#key"
    with pytest.raises(publication.SourcePublicationError, match="raw columns"):
        publication._raw_row_count(_parquet_bytes(raw_row), SDV_TEAM_XWALK_PUBLICATION, 2025)


def test_raw_schema_rejects_duplicate_column_names(monkeypatch):
    class Schema:
        names = [*SDV_GAME_XWALK_PUBLICATION.columns[1:], "matchup_key"]

    class Parquet:
        schema_arrow = Schema()

    monkeypatch.setattr(publication.pyarrow.parquet, "ParquetFile", lambda stream: Parquet())
    with pytest.raises(publication.SourcePublicationError, match="raw columns"):
        publication._raw_row_count(b"unused", SDV_GAME_XWALK_PUBLICATION, 2025)


@pytest.mark.parametrize(
    "column,value,message",
    [
        ("season_type", 2.5, "non-integer"),
        ("snapshot_out_of_sequence", "false", "non-boolean"),
    ],
)
def test_fpi_raw_values_are_checked_before_lossy_parser_coercion(column, value, message):
    raw = fpi_row()
    raw["last_updated"] = "2025-08-01T00:00:00Z"
    raw[column] = value
    with pytest.raises(publication.SourcePublicationError, match=message):
        publication._raw_row_count(_parquet_bytes(raw), SDV_FPI_PUBLICATION, 2025)


def test_fpi_raw_season_must_match_selected_season():
    raw = fpi_row(season=2024)
    raw["last_updated"] = "2024-08-01T00:00:00Z"
    with pytest.raises(publication.SourcePublicationError, match="raw season"):
        publication._raw_row_count(_parquet_bytes(raw), SDV_FPI_PUBLICATION, 2025)


@pytest.mark.parametrize(
    "rows,count,asset,message",
    [
        ([], 0, SDV_FPI_PUBLICATION, "nonempty"),
        ([fpi_row()], 2, SDV_FPI_PUBLICATION, "dropped or added"),
        ([fpi_row(season=2024)], 1, SDV_FPI_PUBLICATION, "requested season"),
        ([fpi_row(team_id=None)], 1, SDV_FPI_PUBLICATION, "null primary-key"),
        ([fpi_row(), fpi_row()], 2, SDV_FPI_PUBLICATION, "duplicate primary key"),
        ([fpi_row(fpi=float("nan"))], 1, SDV_FPI_PUBLICATION, "not finite"),
        ([fpi_row(season_type=2.5)], 1, SDV_FPI_PUBLICATION, "not an integer"),
        ([game_row(yahoo_date="2025-09-01")], 1, SDV_GAME_XWALK_PUBLICATION, "not a date"),
    ],
)
def test_snapshot_validation_rejects_incomplete_or_unsafe_rows(rows, count, asset, message):
    with pytest.raises(publication.SourcePublicationError, match=message):
        publication._validate_parsed_rows(rows, count, asset, 2025)


def test_snapshot_contract_rejects_missing_and_extra_columns():
    missing = game_row()
    missing.pop("fox_date")
    with pytest.raises(publication.SourcePublicationError, match="12-column"):
        publication._validate_parsed_rows([missing], 1, SDV_GAME_XWALK_PUBLICATION, 2025)
    with pytest.raises(publication.SourcePublicationError, match="12-column"):
        publication._validate_parsed_rows(
            [game_row(unexpected=True)], 1, SDV_GAME_XWALK_PUBLICATION, 2025
        )


def test_explicit_hints_cover_each_business_column_and_nullable_ids():
    fpi = {hint["name"]: hint for hint in publication._column_hints(SDV_FPI_PUBLICATION)}
    team = {hint["name"]: hint for hint in publication._column_hints(SDV_TEAM_XWALK_PUBLICATION)}
    game = {hint["name"]: hint for hint in publication._column_hints(SDV_GAME_XWALK_PUBLICATION)}
    assert (len(fpi), len(team), len(game)) == (51, 13, 12)
    assert fpi["last_updated"]["data_type"] == "timestamp"
    assert fpi["snapshot_is_contemporaneous"]["data_type"] == "bool"
    assert team["espn_team_id"] == {
        "name": "espn_team_id",
        "data_type": "bigint",
        "nullable": True,
    }
    assert team["norm_key"]["nullable"] is False
    assert game["yahoo_date"]["data_type"] == "date"
    assert game["espn_game_id"]["nullable"] is True


def test_json_encoding_preserves_dates_timestamps_and_booleans():
    encoded = publication._json_dumps(
        {
            "date": date(2025, 9, 1),
            "timestamp": datetime(2025, 9, 1, tzinfo=UTC),
            "flag": False,
        }
    )
    assert '"date": "2025-09-01"' in encoded
    assert '"timestamp": "2025-09-01T00:00:00+00:00"' in encoded
    assert '"flag": false' in encoded


@pytest.mark.parametrize(
    "source,row,origin,basis",
    [
        ("sdv_fpi_weekly", fpi_row(), "local_file", "artifact_field"),
        ("sdv_team_xwalk", team_row(), "local_file", "caller_declared"),
        (
            "sdv_game_xwalk",
            game_row(),
            "registered_url",
            "registered_artifact_name",
        ),
    ],
)
def test_success_lifecycle_stages_evidence_and_cleans_up(
    monkeypatch, tmp_path, source, row, origin, basis
):
    install_ids(monkeypatch)
    raw = b"parquet fixture"
    sha = hashlib.sha256(raw).hexdigest()
    path = tmp_path / f"{source}.parquet"
    path.write_bytes(raw)
    events = []
    monkeypatch.setattr(publication, "get_db_url", lambda: "postgres://fixture")
    monkeypatch.setattr(publication, "_get_plan", lambda dsn, asset, season: plan(asset))

    def start(dsn, run_id, selected_plan, state):
        state.started = True
        events.append(("start", selected_plan["source_name"]))

    monkeypatch.setattr(publication, "_start_run", start)
    source_url = str(path) if origin == "local_file" else f"https://example/{source}_2025.parquet"
    monkeypatch.setattr(
        publication,
        "fetch_file",
        lambda target: FetchedFile(content=raw, sha256=sha, source_url=source_url),
    )
    monkeypatch.setattr(publication, "_raw_row_count", lambda raw, asset, season: 1)
    monkeypatch.setattr(publication, "resolve_parser", lambda ref: lambda raw, ctx: [row])

    def stage(dsn, spec, asset, raw, ctx, run_id, count, artifact_origin, season_basis):
        events.append(("stage", artifact_origin, season_basis))
        return publication._StageResult(
            table=publication._stage_table_name(source, run_id),
            rows=[{**row, "_dlt_id": "row", "_dlt_load_id": "load"}],
            evidence={
                "stage_schema": publication.STAGE_SCHEMA,
                "parser_contract": asset.parser_contract,
                "dlt_load_ids": ["load"],
                "source_rows": 1,
                "artifact_origin": artifact_origin,
                "season_basis": season_basis,
            },
        )

    monkeypatch.setattr(publication, "_stage_rows", stage)

    def publish(dsn, run_id, generation_id, fetched_sha, staged, state):
        events.append(("publish", staged.evidence))
        return generation_id, False, 1

    monkeypatch.setattr(publication, "_publish", publish)
    monkeypatch.setattr(
        publication,
        "_drop_stage_table",
        lambda dsn, table: events.append(("drop", table)),
    )

    result = publication.run_source_publication(
        REGISTRY[source],
        file_path=str(path) if origin == "local_file" else None,
        season=2025,
    )

    assert result["status"] == "loaded"
    assert result["source"] == source
    assert result["rows"] == 1
    assert result["sha"] == sha
    assert result["run_id"] == RUN_ID
    assert result["generation_id"] == GENERATION_ID
    assert [event[0] for event in events] == ["start", "stage", "publish", "drop"]
    assert events[1] == ("stage", origin, basis)
    assert events[2][1]["season_basis"] == basis


def test_generic_entrypoint_delegates_legacy_ratings(monkeypatch):
    expected = {"source": "sdv_ratings_weekly", "status": "loaded"}
    from src.pipelines.utils import sdv_ratings_publication

    monkeypatch.setattr(
        sdv_ratings_publication,
        "run_sdv_ratings_publication",
        lambda spec, file_path, season: expected,
    )
    assert (
        publication.run_source_publication(
            REGISTRY["sdv_ratings_weekly"], file_path=None, season=2025
        )
        is expected
    )


def test_failure_after_start_records_failed_generation(monkeypatch, tmp_path):
    install_ids(monkeypatch)
    path = tmp_path / "team.parquet"
    path.write_bytes(b"bad")
    failures = []
    monkeypatch.setattr(publication, "get_db_url", lambda: "postgres://fixture")
    monkeypatch.setattr(
        publication,
        "_get_plan",
        lambda dsn, asset, season: plan(SDV_TEAM_XWALK_PUBLICATION),
    )

    def start(dsn, run_id, selected_plan, state):
        state.started = True

    monkeypatch.setattr(publication, "_start_run", start)
    monkeypatch.setattr(
        publication,
        "fetch_file",
        lambda target: FetchedFile(content=b"bad", sha256="0" * 64, source_url=str(path)),
    )
    monkeypatch.setattr(
        publication,
        "_fail_run",
        lambda dsn, run_id, generation_id, outcome, state: failures.append(outcome),
    )
    result = publication.run_source_publication(
        REGISTRY["sdv_team_xwalk"], file_path=str(path), season=2025
    )
    assert result["status"] == "failed"
    assert "SHA-256" in result["error"]
    assert failures == ["failed"]
