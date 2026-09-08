"""Regression coverage for the immediate F10 workflow dependencies."""

from __future__ import annotations

import os
import pathlib
import subprocess

import yaml

ROOT = pathlib.Path(__file__).parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


def _workflow(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text())


def _step(workflow: dict, job: str, name: str) -> dict:
    return next(step for step in workflow["jobs"][job]["steps"] if step.get("name") == name)


def _fake_python(tmp_path: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    call_log = tmp_path / "python-calls.log"
    executable = bin_dir / "python"
    executable.write_text(
        "#!/usr/bin/env bash\n"
        'printf \'%s\\n\' "$*" >> "$PYTHON_CALL_LOG"\n'
        'if [[ -n "$FAIL_PYTHON_CALL" && "$*" == *"$FAIL_PYTHON_CALL"* ]]; then\n'
        "  exit 23\n"
        "fi\n"
    )
    executable.chmod(0o755)
    return bin_dir, call_log


def _run_script(
    script: str,
    tmp_path: pathlib.Path,
    *,
    seasons: str = "",
    sources: str,
    fail_call: str = "",
    player_overview_max: str = "",
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    bin_dir, call_log = _fake_python(tmp_path)
    env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "PYTHON_CALL_LOG": str(call_log),
        "FAIL_PYTHON_CALL": fail_call,
        "SEASONS_INPUT": seasons,
        "SOURCES_INPUT": sources,
        "PLAYER_OVERVIEW_MAX_PER_RUN": player_overview_max,
    }
    result = subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    calls = call_log.read_text().splitlines() if call_log.exists() else []
    return result, calls


def test_daily_verification_waits_for_load_and_flat_files() -> None:
    workflow = _workflow("daily-load.yml")

    assert workflow["jobs"]["flat_files"] == {
        "name": "Load flat-file sources",
        "needs": "load",
        "uses": "./.github/workflows/flat-files.yml",
        "secrets": "inherit",
    }
    assert "needs" not in workflow["jobs"]["load"]
    assert workflow["jobs"]["verify"]["needs"] == "flat_files"
    assert workflow["jobs"]["recaps"]["needs"] == "verify"
    assert _step(workflow, "verify", "Verify load")["env"] == {
        "SEASON_INPUT": "${{ inputs.season }}"
    }


def test_daily_refreshes_crossvalidation_after_adjusted_epa() -> None:
    workflow = _workflow("daily-load.yml")
    steps = workflow["jobs"]["load"]["steps"]
    refresh_index = next(
        index
        for index, step in enumerate(steps)
        if step.get("name") == "Refresh Tier 2 + Tier 3 marts"
    )
    refresh = steps[refresh_index]["run"]
    views = refresh.split("--views ", 1)[1].split(",")

    assert views.index("marts.epa_crossvalidation") == views.index("marts.team_adjusted_epa") + 1
    adjusted_epa_index = next(
        index for index, step in enumerate(steps) if step.get("name") == "Refit adjusted EPA"
    )
    assert adjusted_epa_index < refresh_index
    assert not any(step.get("name") == "Verify load" for step in steps)


def test_coach_tenure_pipeline_refreshes_after_successful_load(tmp_path: pathlib.Path) -> None:
    workflow = _workflow("backfill-sources.yml")
    script = _step(workflow, "backfill", "Run pipeline source")["run"]

    result, calls = _run_script(
        script,
        tmp_path,
        seasons="2024 2025",
        sources="coach_tenures",
    )

    assert result.returncode == 0, result.stderr
    assert calls == [
        "-m src.pipelines.run --source coach_tenures --years 2024 2025",
        "scripts/refresh_marts.py --views marts.coach_tenures",
    ]


def test_failed_coach_tenure_load_blocks_refresh(tmp_path: pathlib.Path) -> None:
    workflow = _workflow("backfill-sources.yml")
    script = _step(workflow, "backfill", "Run pipeline source")["run"]

    result, calls = _run_script(
        script,
        tmp_path,
        seasons="2025",
        sources="coach_tenures",
        fail_call="src.pipelines.run",
    )

    assert result.returncode == 23
    assert calls == ["-m src.pipelines.run --source coach_tenures --years 2025"]


def test_metrics_pipeline_preserves_bare_invocation_without_refresh(
    tmp_path: pathlib.Path,
) -> None:
    workflow = _workflow("backfill-sources.yml")
    script = _step(workflow, "backfill", "Run pipeline source")["run"]

    result, calls = _run_script(script, tmp_path, sources="metrics_ppa_predicted")

    assert result.returncode == 0, result.stderr
    assert calls == ["-m src.pipelines.run --source metrics_ppa_predicted"]


def test_load_season_keeps_selected_sources_and_year_loop(tmp_path: pathlib.Path) -> None:
    workflow = _workflow("backfill-sources.yml")
    script = _step(workflow, "backfill", "Load seasons")["run"]

    result, calls = _run_script(
        script,
        tmp_path,
        seasons="2022 2023",
        sources="ratings,stats:player_returning",
        player_overview_max="9000",
    )

    assert result.returncode == 0, result.stderr
    assert calls == [
        "scripts/load_season.py --season 2022 "
        "--sources ratings,stats:player_returning --skip-refresh",
        "scripts/load_season.py --season 2023 "
        "--sources ratings,stats:player_returning --skip-refresh",
    ]
    refresh = _step(workflow, "backfill", "Refresh marts")
    assert refresh["if"] == "${{ inputs.runner != 'pipeline_run' }}"
    assert refresh["run"] == "python scripts/refresh_marts.py"
