"""Exercise flat-file workflow commands without provider or database access."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/flat-files.yml"


def workflow():
    return yaml.safe_load(WORKFLOW.read_text())


def test_flat_file_workflow_is_reusable_and_keeps_manual_backfills():
    triggers = workflow()[True]  # PyYAML's YAML 1.1 spelling of `on`.
    assert "schedule" not in triggers
    assert set(triggers) == {"workflow_call", "workflow_dispatch"}
    for name in ("source", "seasons"):
        assert triggers["workflow_call"]["inputs"][name]["type"] == "string"
        assert triggers["workflow_call"]["inputs"][name]["default"] == ""
        assert triggers["workflow_dispatch"]["inputs"][name]["required"] is False
    assert triggers["workflow_call"]["secrets"]["SUPABASE_DB_URL"]["required"] is True
    # The caller owns daily-season-load for its entire run; reusing that group
    # here would make the called workflow wait for its own parent to finish.
    assert workflow()["concurrency"]["group"] == "flat-file-load"


@pytest.mark.parametrize(
    "sources,seasons,expected",
    [
        ("", "", [["--due"]]),
        ("sdv_fpi_weekly", "", [["--source", "sdv_fpi_weekly"]]),
        ("", "2024 2025", [["--due", "--season", "2024"], ["--due", "--season", "2025"]]),
        (
            "sdv_fpi_weekly sdv_ratings_weekly",
            "2024 2025",
            [
                ["--source", "sdv_fpi_weekly", "--source", "sdv_ratings_weekly", "--season", year]
                for year in ("2024", "2025")
            ],
        ),
    ],
)
def test_imports_finish_before_refresh(tmp_path, sources, seasons, expected):
    result, calls = run_steps(tmp_path, sources, seasons)
    assert result == 0
    assert calls == [["scripts/load_flat_files.py", *args] for args in expected] + [
        ["scripts/refresh_marts.py", "--views", "marts.epa_crossvalidation"]
    ]


@pytest.mark.parametrize(
    "failed_script", ["scripts/load_flat_files.py", "scripts/refresh_marts.py"]
)
def test_workflow_propagates_failures(tmp_path, failed_script):
    result, calls = run_steps(tmp_path, "sdv_fpi_weekly", "2024 2025", failed_script)
    assert result == 17
    if failed_script == "scripts/load_flat_files.py":
        assert calls == [
            ["scripts/load_flat_files.py", "--source", "sdv_fpi_weekly", "--season", "2024"]
        ]
    else:
        assert len(calls) == 3
        assert calls[-1][0] == "scripts/refresh_marts.py"


def run_steps(tmp_path, sources, seasons, failed_script=""):
    """Run the actual success-path shell steps with a logging command stub."""
    calls_path = tmp_path / "calls.jsonl"
    fake_python = tmp_path / "python"
    fake_python.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "with open(os.environ['TEST_CALLS'], 'a') as log:\n"
        "    log.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "sys.exit(17 if sys.argv[1] == os.environ.get('TEST_FAIL_SCRIPT') else 0)\n"
    )
    fake_python.chmod(0o755)
    steps = workflow()["jobs"]["load"]["steps"]
    selected = [
        step
        for step in steps
        if step.get("name") in {"Load flat-file sources", "Refresh external-rating consumers"}
    ]
    assert [step["name"] for step in selected] == [
        "Load flat-file sources",
        "Refresh external-rating consumers",
    ]
    # GitHub's implicit success() must guard both, so a failed import cannot
    # refresh its consumers or make the reusable job report success.
    assert all("if" not in step and not step.get("continue-on-error") for step in selected)
    env = {
        **os.environ,
        "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
        "SOURCE_INPUT": sources,
        "SEASONS_INPUT": seasons,
        "TEST_CALLS": str(calls_path),
        "TEST_FAIL_SCRIPT": failed_script,
    }
    status = 0
    for step in selected:
        result = subprocess.run(
            ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", step["run"]],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        status = result.returncode
        if status:
            break
    return status, [json.loads(line) for line in calls_path.read_text().splitlines()]
