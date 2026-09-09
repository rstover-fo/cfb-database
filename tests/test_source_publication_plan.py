"""Source coverage contracts and the opt-in flat-file CLI boundary."""

import sys
from types import SimpleNamespace

import pytest

from scripts import load_flat_files
from src.pipelines.config.source_publication_assets import (
    SDV_RATINGS_PUBLICATION,
    SOURCE_PUBLICATION_ASSETS,
)
from src.pipelines.sources.flat_files import REGISTRY
from src.pipelines.utils.refresh_plan import REFRESH_GRAPH

ENROLLED_SOURCES = (
    "sdv_ratings_weekly",
    "sdv_fpi_weekly",
    "sdv_team_xwalk",
    "sdv_game_xwalk",
)


def test_source_contract_matches_loader_and_declared_dependencies():
    contract = SDV_RATINGS_PUBLICATION
    spec = REGISTRY[contract.source_name]
    assert set(SOURCE_PUBLICATION_ASSETS) == set(ENROLLED_SOURCES)
    assert contract.asset_key == f"{spec.schema}.{spec.table}"
    assert contract.primary_key == spec.primary_key
    assert contract.cadence == spec.cadence
    assert len(contract.columns) == len(set(contract.columns)) == 16
    assert set(contract.primary_key) <= set(contract.columns)
    assert contract.coverage_key(2025) == "season:2025"
    assert REFRESH_GRAPH.plan(changed=[contract.asset_key]).views == ("marts.epa_crossvalidation",)


@pytest.mark.parametrize("name", ENROLLED_SOURCES)
def test_enrolled_sources_preserve_registry_identity_and_scope(name):
    contract = SOURCE_PUBLICATION_ASSETS[name]
    spec = REGISTRY[name]
    assert contract.source_name == name
    assert contract.asset_key == f"{spec.schema}.{spec.table}"
    assert contract.primary_key == spec.primary_key
    assert contract.cadence == spec.cadence
    assert len(contract.columns) == len(set(contract.columns))
    assert set(contract.primary_key) <= set(contract.columns)
    assert contract.coverage_key(2025) == "season:2025"


@pytest.mark.parametrize("season", [None, True, "2025", 2025.0, 1868, 2201])
def test_invalid_source_coverage_is_rejected(season):
    with pytest.raises(ValueError, match="explicit season"):
        SDV_RATINGS_PUBLICATION.coverage_key(season)


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--due", "--season", "2025"],
        ["--source", "massey", "--season", "2025"],
        ["--source", "sdv_ratings_weekly"],
        ["--source", "sdv_ratings_weekly", "--season", "2201"],
        ["--source", "sdv_ratings_weekly", "--source", "massey", "--season", "2025"],
        ["--source", "sdv_ratings_weekly", "--source", "sdv_ratings_weekly", "--season", "2025"],
        [
            "--source",
            "sdv_ratings_weekly",
            "--source",
            "sdv_fpi_weekly",
            "--season",
            "2025",
            "--file",
            "fixture.parquet",
        ],
    ],
)
def test_receipt_cli_rejects_unsupported_plans_before_work(monkeypatch, args):
    def forbidden(*_args, **_kwargs):
        pytest.fail("unsupported receipt plans must not plan, fetch or query the ledger")

    monkeypatch.setattr(load_flat_files, "_planned_sources", forbidden)
    monkeypatch.setattr(load_flat_files, "fetch_file", forbidden)
    monkeypatch.setattr(load_flat_files, "last_checked", forbidden)
    with pytest.raises(SystemExit) as error:
        load_flat_files.main(["--require-receipts", *args])
    assert error.value.code == 2


def test_receipt_dry_run_is_offline_and_does_not_refresh_descendants(monkeypatch, capsys):
    def forbidden(*_args, **_kwargs):
        pytest.fail("receipt dry-run must be offline")

    monkeypatch.setattr(load_flat_files, "last_checked", forbidden)
    monkeypatch.setattr(load_flat_files, "run_source", forbidden)
    assert (
        load_flat_files.main(
            [
                "--require-receipts",
                "--source",
                "sdv_ratings_weekly",
                "--season",
                "2025",
                "--dry-run",
            ]
        )
        == 0
    )
    output = capsys.readouterr().out
    assert "ratings.sdv_ratings_weekly/season:2025" in output
    assert "not refreshed" in output
    assert "Live generation and schema validation is not run" in output


@pytest.mark.parametrize(
    "status,exit_code",
    [("loaded", 0), ("failed", 1), ("deferred", 1), ("not_published", 1), ("blocked", 1)],
)
def test_receipt_cli_routes_exact_scope_and_fails_closed(monkeypatch, capsys, status, exit_code):
    calls = []

    def adapter(spec, **kwargs):
        calls.append((spec, kwargs))
        return {
            "source": spec.name,
            "status": status,
            "rows": 2,
            "sha": None,
            "duration_s": 0.1,
            "error": None if status == "loaded" else "Selected artifact cannot be published",
            "run_id": "run-fixture",
            "generation_id": "generation-fixture",
        }

    monkeypatch.setitem(
        sys.modules,
        "src.pipelines.utils.flat_file_publication",
        SimpleNamespace(run_source_publication=adapter),
    )
    assert (
        load_flat_files.main(
            [
                "--require-receipts",
                "--source",
                "sdv_ratings_weekly",
                "--season",
                "2025",
                "--file",
                "fixture.parquet",
            ]
        )
        == exit_code
    )
    assert calls == [
        (REGISTRY["sdv_ratings_weekly"], {"season": 2025, "file_path": "fixture.parquet"})
    ]
    output = capsys.readouterr()
    assert "sdv_ratings_weekly" in output.out
    if exit_code:
        assert "Selected artifact cannot be published" in output.err
        assert "run_id=run-fixture" in output.err
        assert "generation_id=generation-fixture" in output.err
        assert status in output.err
    else:
        assert output.err == ""


def test_batch_dry_run_labels_source_season_provenance_without_querying(monkeypatch, capsys):
    def forbidden(*_args, **_kwargs):
        pytest.fail("batch dry-run must not invoke publication or fetch")

    monkeypatch.setitem(
        sys.modules,
        "src.pipelines.utils.flat_file_publication",
        SimpleNamespace(run_source_publication=forbidden),
    )
    monkeypatch.setattr(load_flat_files, "fetch_file", forbidden)
    monkeypatch.setattr(load_flat_files, "last_checked", forbidden)
    args = ["--require-receipts", "--season", "2025", "--dry-run"]
    for name in ENROLLED_SOURCES:
        args.extend(["--source", name])

    assert load_flat_files.main(args) == 0
    output = capsys.readouterr().out
    for contract in SOURCE_PUBLICATION_ASSETS.values():
        assert f"{contract.asset_key}/season:2025" in output
    assert output.count("Season basis: artifact_field") == 2
    assert output.count("Season basis: registered_artifact_name") == 2
    assert "not registered in the SQL refresh graph" in output
    assert "separate operation and publication transaction" in output


def test_crosswalk_local_dry_run_labels_caller_declared_season(capsys):
    assert (
        load_flat_files.main(
            [
                "--require-receipts",
                "--source",
                "sdv_team_xwalk",
                "--season",
                "2025",
                "--file",
                "arbitrary-name.parquet",
                "--dry-run",
            ]
        )
        == 0
    )
    assert "Season basis: caller_declared" in capsys.readouterr().out


def test_batch_reports_each_result_and_continues_after_source_failure(monkeypatch, capsys):
    names = ("sdv_fpi_weekly", "sdv_team_xwalk", "sdv_game_xwalk")
    calls = []

    def adapter(spec, **kwargs):
        calls.append((spec.name, kwargs))
        failed = spec.name == "sdv_team_xwalk"
        return {
            "source": spec.name,
            "status": "failed" if failed else "loaded",
            "rows": 0 if failed else 2,
            "sha": None,
            "duration_s": 0.1,
            "error": "invalid crosswalk" if failed else None,
            "run_id": f"run-{spec.name}",
            "generation_id": f"generation-{spec.name}",
        }

    monkeypatch.setitem(
        sys.modules,
        "src.pipelines.utils.flat_file_publication",
        SimpleNamespace(run_source_publication=adapter),
    )
    args = ["--require-receipts", "--season", "2025"]
    for name in names:
        args.extend(["--source", name])

    assert load_flat_files.main(args) == 1
    assert calls == [(name, {"file_path": None, "season": 2025}) for name in names]
    output = capsys.readouterr()
    for name in names:
        assert name in output.out
    assert "invalid crosswalk" in output.err
    assert "run-sdv_team_xwalk" in output.err
    assert "generation-sdv_team_xwalk" in output.err
    assert "run-sdv_game_xwalk" not in output.err
