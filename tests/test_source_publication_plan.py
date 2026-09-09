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


def test_source_contract_matches_loader_and_declared_dependencies():
    contract = SDV_RATINGS_PUBLICATION
    spec = REGISTRY[contract.source_name]
    assert set(SOURCE_PUBLICATION_ASSETS) == {spec.name}
    assert contract.asset_key == f"{spec.schema}.{spec.table}"
    assert contract.primary_key == spec.primary_key
    assert contract.cadence == spec.cadence
    assert len(contract.columns) == len(set(contract.columns)) == 16
    assert set(contract.primary_key) <= set(contract.columns)
    assert contract.coverage_key(2025) == "season:2025"
    assert REFRESH_GRAPH.plan(changed=[contract.asset_key]).views == ("marts.epa_crossvalidation",)


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
        ["--source", "sdv_ratings_weekly", "--source", "sdv_fpi_weekly", "--season", "2025"],
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
    "status,exit_code", [("loaded", 0), ("failed", 1), ("deferred", 1), ("blocked", 1)]
)
def test_receipt_cli_routes_exact_scope_and_fails_closed(monkeypatch, status, exit_code):
    calls = []

    def adapter(spec, **kwargs):
        calls.append((spec, kwargs))
        return {"source": spec.name, "status": status, "rows": 2, "sha": None, "duration_s": 0.1}

    monkeypatch.setitem(
        sys.modules,
        "src.pipelines.utils.sdv_ratings_publication",
        SimpleNamespace(run_sdv_ratings_publication=adapter),
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
