"""Exercise scheduled SDV routing and legacy fallback ownership."""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from scripts import load_flat_files
from tests.test_f10_flat_file_workflow import run_steps, workflow

ROOT = Path(__file__).resolve().parents[1]
ACTIVATIONS = {
    "SDV_RATINGS_RECEIPT_SEASON": "2026",
    "SDV_TEAM_XWALK_RECEIPT_SEASON": "2025",
    "SDV_GAME_XWALK_RECEIPT_SEASON": "2025",
}


@pytest.mark.parametrize("seasons", ["", "2025 2026"])
def test_active_scopes_exclude_fallback_and_preserve_fpi(tmp_path, seasons):
    status, calls = run_steps(tmp_path, "", seasons, activation="2026", sdv_activations=ACTIVATIONS)
    assert status == 0
    excluded = [
        "sdv_fpi_weekly:2026",
        "sdv_ratings_weekly:2026",
        "sdv_team_xwalk:2025",
        "sdv_team_xwalk:2026",
        "sdv_game_xwalk:2025",
        "sdv_game_xwalk:2026",
    ]
    planned_seasons = seasons.split() if seasons else [None]
    assert len(calls) == len(planned_seasons) + 1
    for call, season in zip(calls, planned_seasons, strict=False):
        prefix = ["scripts/load_flat_files.py", "--due"]
        if season is not None:
            prefix += ["--season", season]
        expected = prefix + [arg for pair in excluded for arg in ("--exclude-source-season", pair)]
        assert call == expected


@pytest.mark.parametrize("season", [None, 2025])
def test_receipted_crosswalk_never_reaches_legacy_fetch_or_fallback(tmp_path, monkeypatch, season):
    """Execute workflow argv through the real loader planner, before any fetch."""
    status, calls = run_steps(
        tmp_path,
        "",
        str(season) if season else "",
        sdv_activations={"SDV_TEAM_XWALK_RECEIPT_SEASON": "2025"},
    )
    assert status == 0
    registry = {
        name: load_flat_files.REGISTRY[name]
        for name in ("sdv_team_xwalk", "sdv_game_xwalk", "sdv_ratings_weekly")
    }
    monkeypatch.setattr(load_flat_files, "REGISTRY", registry)
    monkeypatch.setattr(load_flat_files, "season_for_date", lambda today: 2026)
    monkeypatch.setattr(load_flat_files, "is_due", lambda *args: True)
    monkeypatch.setattr(load_flat_files, "_cadence_last_checked", lambda *args: None)
    executed = []

    def legacy_run(spec, **kwargs):
        if spec.name == "sdv_team_xwalk":
            pytest.fail("receipted crosswalk reached the legacy path that can fall back to 2025")
        executed.append((spec.name, kwargs["season"], kwargs["season_explicit"]))
        return {"source": spec.name, "status": "skipped", "rows": 0, "duration_s": 0.0}

    monkeypatch.setattr(load_flat_files, "run_source", legacy_run)
    assert load_flat_files.main(calls[0][1:]) == 0
    assert executed == [
        ("sdv_game_xwalk", season or 2026, season is not None),
        ("sdv_ratings_weekly", season or 2026, season is not None),
    ]


@pytest.mark.parametrize("variable", list(ACTIVATIONS))
@pytest.mark.parametrize("bad_value", ["2027", "02025", "2025 ", "anything"])
def test_bad_new_activation_stops_legacy_writes(tmp_path, variable, bad_value):
    status, calls = run_steps(tmp_path, "", "", sdv_activations={variable: bad_value})
    assert status == 2
    assert calls == [["scripts/refresh_marts.py", "--views", "marts.epa_crossvalidation"]]


def test_explicit_legacy_source_is_still_an_intentional_manual_bypass(tmp_path):
    status, calls = run_steps(tmp_path, "sdv_team_xwalk", "2025", sdv_activations=ACTIVATIONS)
    assert status == 0
    assert calls[0] == [
        "scripts/load_flat_files.py",
        "--source",
        "sdv_team_xwalk",
        "--season",
        "2025",
    ]


def test_canary_refresh_handles_possible_ratings_commit_without_hiding_failure():
    steps = workflow()["jobs"]["load"]["steps"]
    refresh = next(step for step in steps if step.get("name") == "Refresh canary rating consumers")
    assert refresh["if"] == (
        "${{ !cancelled() && steps.receipt_canary.outputs.publication_source "
        "== 'sdv_ratings_weekly' "
        "&& steps.receipt_canary.outputs.publication_attempted == 'true' "
        "&& (steps.receipt_canary.outcome == 'success' "
        "|| steps.receipt_canary.outcome == 'failure') }}"
    )
    assert refresh["run"] == "python scripts/refresh_marts.py --views marts.epa_crossvalidation"
    assert not refresh.get("continue-on-error")


def scheduled_workflow():
    return yaml.safe_load((ROOT / ".github/workflows/sdv-source-receipts.yml").read_text())


@pytest.mark.parametrize("config", [workflow, scheduled_workflow])
def test_receipt_workflow_actions_use_immutable_revisions(config):
    references = [
        step["uses"]
        for job in config()["jobs"].values()
        for step in job.get("steps", [])
        if "uses" in step
    ]
    assert references
    assert all(re.fullmatch(r"actions/[a-z-]+@[0-9a-f]{40}", ref) for ref in references)


def test_schedule_uses_shared_lock_order_and_scopes_credentials_to_operations():
    config = scheduled_workflow()
    assert config[True]["schedule"] == [{"cron": "29 10 * * 1"}]
    assert config["permissions"] == {"contents": "read"}
    assert "daily-season-load" in config["concurrency"]["group"]
    assert "sdv-sources-inactive-" in config["concurrency"]["group"]
    assert config["concurrency"]["queue"] == "max"
    assert config["concurrency"]["cancel-in-progress"] is False
    job = config["jobs"]["publish"]
    assert job["concurrency"] == {
        "group": "flat-file-load",
        "queue": "max",
        "cancel-in-progress": False,
    }
    assert "SUPABASE_DB_URL" not in config["env"]
    assert "env" not in job
    for variable in ACTIVATIONS:
        assert f"vars.{variable} != ''" in job["if"]
    for step in job["steps"]:
        operational = step.get("name") in {
            "Publish and verify SDV receipts",
            "Refresh external-rating consumers",
        }
        assert ("SUPABASE_DB_URL" in step.get("env", {})) == operational
    refresh = job["steps"][-1]
    assert refresh["if"] == (
        "${{ !cancelled() && steps.publish_sdv.outputs.ratings_publication_attempted == 'true' "
        "&& (steps.publish_sdv.outcome == 'success' || steps.publish_sdv.outcome == 'failure') }}"
    )
    assert all(not step.get("continue-on-error") for step in job["steps"])


def test_scheduled_command_passes_project_reference_as_one_argument(tmp_path):
    command = next(
        step["run"]
        for step in scheduled_workflow()["jobs"]["publish"]["steps"]
        if step.get("id") == "publish_sdv"
    )
    fake_python = tmp_path / "python"
    fake_python.write_text(
        f"#!{sys.executable}\nimport json, sys\nprint(json.dumps(sys.argv[1:]))\n"
    )
    fake_python.chmod(0o755)
    project = "reviewed-project; touch never"
    result = subprocess.run(
        ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", command],
        env={
            **os.environ,
            "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
            "EXPECTED_PROJECT_REF": project,
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert json.loads(result.stdout) == [
        "scripts/run_scheduled_sdv_receipts.py",
        "--expected-project-ref",
        project,
    ]
    assert "${{" not in command
