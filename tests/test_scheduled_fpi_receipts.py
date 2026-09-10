"""Bounded tests for the activated scheduled FPI receipt publisher."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from datetime import date
from pathlib import Path

import psycopg2
import pytest
import yaml

from scripts import run_scheduled_fpi_receipts as scheduled

PROJECT_REF = "ibobsbwlewpqslkqbrjd"
RUN_ID = "1c74bd2a-c85f-4a28-bd5f-b5473ed30779"
GENERATION_ID = "78699d90-51a8-4729-b41c-8232ed7c98f0"
SHA256 = "a" * 64
WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/sdv-fpi-receipts.yml"
WORKFLOWS = WORKFLOW.parent


def args() -> argparse.Namespace:
    return argparse.Namespace(season=2026, expected_project_ref=PROJECT_REF)


def loaded_result() -> dict:
    return {
        "status": "loaded",
        "source": scheduled.SOURCE,
        "rows": 276,
        "sha": SHA256,
        "duration_s": 4.5,
        "run_id": RUN_ID,
        "generation_id": GENERATION_ID,
    }


def workflow():
    return yaml.safe_load(WORKFLOW.read_text())


def named_workflow(name: str):
    return yaml.safe_load((WORKFLOWS / name).read_text())


@pytest.mark.parametrize("season", ["2025", "2027", "2026 2027"])
def test_cli_accepts_only_the_explicitly_activated_2026_season(season):
    with pytest.raises(SystemExit, match="2"):
        scheduled.build_parser().parse_args(
            ["--season", season, "--expected-project-ref", PROJECT_REF]
        )


def test_future_season_boundary_is_a_noop_before_database_or_provider(monkeypatch, tmp_path):
    outputs = tmp_path / "outputs"
    monkeypatch.setenv("GITHUB_OUTPUT", str(outputs))
    monkeypatch.setattr(
        scheduled.publication,
        "get_db_url",
        lambda: pytest.fail("expired activation must not resolve a database"),
    )
    monkeypatch.setattr(
        scheduled.canary,
        "_inspect_target",
        lambda *values: pytest.fail("expired activation must not inspect a database"),
    )
    monkeypatch.setattr(
        scheduled.publication,
        "run_source_publication",
        lambda *values, **kwargs: pytest.fail("expired activation must not fetch or publish"),
    )

    payload, status = scheduled.run_scheduled(args(), today=date(2027, 8, 1))

    assert status == 0
    assert payload == {
        "status": "skipped",
        "source": "sdv_fpi_weekly",
        "season": 2026,
        "run_id": None,
        "generation_id": None,
        "rows": 0,
        "sha": None,
        "duration_s": 0.0,
        "reason": "activated season is no longer current",
    }
    assert outputs.read_text().splitlines() == [
        "active_season=2026",
        "publication_attempted=false",
    ]


def test_active_run_checks_preconditions_pins_one_publication_and_verifies_result(
    monkeypatch, tmp_path
):
    outputs = tmp_path / "outputs"
    monkeypatch.setenv("GITHUB_OUTPUT", str(outputs))
    events = []
    monkeypatch.setattr(scheduled.publication, "get_db_url", lambda: "resolved-dsn")

    def inspect(dsn, project, source, season):
        events.append(("inspect", dsn, project, source, season))
        return {
            "can_set_role": True,
            "assumed_role": scheduled.canary.PUBLISHER_ROLE,
        }, {"source_name": source}

    monkeypatch.setattr(scheduled.canary, "_inspect_target", inspect)
    monkeypatch.setattr(
        scheduled,
        "_require_policy",
        lambda dsn, season: events.append(("policy", dsn, season)),
    )

    def publish(spec, **kwargs):
        events.append(
            (
                "publish",
                spec is scheduled.REGISTRY[scheduled.SOURCE],
                kwargs,
                scheduled.publication.get_db_url(),
            )
        )
        return loaded_result()

    monkeypatch.setattr(scheduled.publication, "run_source_publication", publish)
    monkeypatch.setattr(
        scheduled,
        "_verify_private_receipt",
        lambda dsn, season, result: events.append(
            ("receipt", dsn, season, result["generation_id"], result["sha"])
        ),
    )
    monkeypatch.setattr(
        scheduled,
        "_verify_public_freshness",
        lambda dsn, season, result: events.append(
            ("freshness", dsn, season, result["generation_id"], result["rows"])
        ),
    )

    payload, status = scheduled.run_scheduled(args(), today=date(2026, 9, 10))

    assert status == 0
    assert payload == {
        "status": "loaded",
        "source": "sdv_fpi_weekly",
        "season": 2026,
        "run_id": RUN_ID,
        "generation_id": GENERATION_ID,
        "rows": 276,
        "sha": SHA256,
        "duration_s": 4.5,
    }
    assert events == [
        ("inspect", "resolved-dsn", PROJECT_REF, "sdv_fpi_weekly", 2026),
        ("policy", "resolved-dsn", 2026),
        ("publish", True, {"season": 2026}, "resolved-dsn"),
        ("receipt", "resolved-dsn", 2026, GENERATION_ID, SHA256),
        ("freshness", "resolved-dsn", 2026, GENERATION_ID, 276),
    ]
    assert scheduled.publication.get_db_url() == "resolved-dsn"
    assert outputs.read_text().splitlines()[-1] == "publication_attempted=true"


@pytest.mark.parametrize("failure", ["role", "policy"])
def test_precondition_failure_stops_before_publication(monkeypatch, failure):
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.setattr(scheduled.publication, "get_db_url", lambda: "resolved-dsn")
    identity = {
        "can_set_role": failure != "role",
        "assumed_role": scheduled.canary.PUBLISHER_ROLE if failure != "role" else None,
    }
    monkeypatch.setattr(scheduled.canary, "_inspect_target", lambda *values: (identity, {}))

    def require_policy(*values):
        if failure == "policy":
            raise scheduled.ScheduledFpiError("policy missing")

    monkeypatch.setattr(scheduled, "_require_policy", require_policy)
    monkeypatch.setattr(
        scheduled.publication,
        "run_source_publication",
        lambda *values, **kwargs: pytest.fail("precondition failure must not publish"),
    )

    with pytest.raises(scheduled.ScheduledFpiError):
        scheduled.run_scheduled(args(), today=date(2026, 9, 10))


@pytest.mark.parametrize(
    ("policy_rows", "accepted"), [([(True,)], True), ([], False), ([(False,)], False)]
)
def test_policy_preflight_requires_one_exact_36_hour_row(policy_rows, accepted):
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *unused):
            return False

        def execute(self, statement, parameters=None):
            if parameters is not None:
                assert parameters == ("ratings.espn_fpi_weekly", "season:2026")
                assert "interval '36 hours'" in statement

        def fetchall(self):
            return policy_rows

    class Connection:
        def set_session(self, **options):
            assert options == {"readonly": True, "autocommit": False}

        def cursor(self):
            return Cursor()

        def rollback(self):
            pass

        def close(self):
            pass

    if accepted:
        scheduled._require_policy("dsn", 2026, connector=lambda *values, **options: Connection())
    else:
        with pytest.raises(scheduled.ScheduledFpiError, match="exactly 36 hours"):
            scheduled._require_policy(
                "dsn", 2026, connector=lambda *values, **options: Connection()
            )


@pytest.mark.parametrize("publication_status", ["failed", "not_published", "blocked"])
def test_failed_deferred_or_uncertain_publication_is_not_retried_or_postverified(
    monkeypatch, tmp_path, publication_status
):
    outputs = tmp_path / "outputs"
    monkeypatch.setenv("GITHUB_OUTPUT", str(outputs))
    monkeypatch.setattr(scheduled.publication, "get_db_url", lambda: "resolved-dsn")
    monkeypatch.setattr(
        scheduled.canary,
        "_inspect_target",
        lambda *values: (
            {"can_set_role": True, "assumed_role": scheduled.canary.PUBLISHER_ROLE},
            {},
        ),
    )
    monkeypatch.setattr(scheduled, "_require_policy", lambda *values: None)
    calls = []
    failed = {
        **loaded_result(),
        "status": publication_status,
        "rows": 0,
        "sha": None,
        "duration_s": 1.0,
    }

    def publish(*values, **kwargs):
        calls.append((values, kwargs))
        return failed

    monkeypatch.setattr(scheduled.publication, "run_source_publication", publish)
    monkeypatch.setattr(
        scheduled,
        "_verify_private_receipt",
        lambda *values: pytest.fail("failed publication must not be postverified"),
    )
    monkeypatch.setattr(
        scheduled,
        "_verify_public_freshness",
        lambda *values: pytest.fail("failed publication must not be postverified"),
    )

    payload, status = scheduled.run_scheduled(args(), today=date(2026, 9, 10))

    assert status == 1
    assert payload["status"] == publication_status
    assert payload["error_category"] == "source_publication_failed"
    assert len(calls) == 1
    assert outputs.read_text().splitlines()[-1] == "publication_attempted=true"


def freshness_row(**overrides):
    row = {
        "source_name": "sdv_fpi_weekly",
        "asset_key": "ratings.espn_fpi_weekly",
        "season": 2026,
        "coverage_key": "season:2026",
        "generation_id": GENERATION_ID,
        "published_at": "2026-09-10T15:00:00+00:00",
        "age_seconds": 2,
        "expected_refresh_interval": "1 day 12:00:00",
        "is_stale": False,
        "publication_state": "current",
        "current_outcome": "succeeded",
        "is_complete": True,
        "source_rows": 276,
        "published_rows": 276,
        "artifact_origin": "registered_url",
        "season_basis": "artifact_field",
        "latest_outcome": "succeeded",
        "latest_recorded_at": "2026-09-10T15:00:00+00:00",
        "last_failure_outcome": None,
        "last_failure_at": None,
        "last_failure_category": None,
    }
    row.update(overrides)
    return row


def test_postpublication_checks_exact_private_and_both_public_evidence(monkeypatch):
    private = (
        RUN_ID,
        GENERATION_ID,
        SHA256,
        "succeeded",
        {
            "complete": True,
            "season": 2026,
            "source_rows": 276,
            "published_rows": 276,
        },
        {"artifact_origin": "registered_url", "season_basis": "artifact_field"},
        {"published_rows": 276},
        GENERATION_ID,
    )

    class Cursor:
        def __init__(self, result):
            self.result = result

        def __enter__(self):
            return self

        def __exit__(self, *unused):
            return False

        def execute(self, statement, parameters=None):
            pass

        def fetchall(self):
            return [self.result]

    class Connection:
        def __init__(self, result):
            self.result = result

        def set_session(self, **options):
            assert options == {"readonly": True, "autocommit": False}

        def cursor(self):
            return Cursor(self.result)

        def rollback(self):
            pass

        def close(self):
            pass

    scheduled._verify_private_receipt(
        "dsn", 2026, loaded_result(), connector=lambda *args, **kwargs: Connection(private)
    )
    roles = []

    def read_public(dsn, role, season):
        roles.append((dsn, role, season))
        return freshness_row(), True

    monkeypatch.setattr(scheduled, "_read_public_freshness", read_public)
    scheduled._verify_public_freshness("dsn", 2026, loaded_result())
    assert roles == [("dsn", "anon", 2026), ("dsn", "authenticated", 2026)]


@pytest.mark.parametrize(
    "change",
    [
        {"generation_id": str(uuid.uuid4())},
        {"published_rows": 275},
        {"artifact_origin": "local_file"},
        {"latest_outcome": "failed"},
        {"is_stale": True},
    ],
)
def test_public_postcheck_rejects_mismatched_or_stale_evidence(monkeypatch, change):
    monkeypatch.setattr(
        scheduled,
        "_read_public_freshness",
        lambda *values: (freshness_row(**change), True),
    )
    with pytest.raises(scheduled.ScheduledFpiError):
        scheduled._verify_public_freshness("dsn", 2026, loaded_result())


def test_main_never_prints_unexpected_exception_details(monkeypatch, capsys):
    monkeypatch.setattr(
        scheduled,
        "run_scheduled",
        lambda selected: (_ for _ in ()).throw(
            RuntimeError("postgresql://user:secret@private-host/database")
        ),
    )

    status = scheduled.main(["--season", "2026", "--expected-project-ref", PROJECT_REF])
    output = capsys.readouterr().out

    assert status == 1
    assert json.loads(output)["error"] == "unexpected failure (RuntimeError)"
    assert "secret" not in output
    assert "private-host" not in output


@pytest.mark.parametrize(
    ("failed_check", "error", "expected_error"),
    [
        (
            "private",
            scheduled.ScheduledFpiError("committed publication receipt does not match"),
            "committed publication receipt does not match",
        ),
        (
            "public",
            psycopg2.OperationalError(
                "password=do-not-print postgresql://user:secret@private-host/database"
            ),
            "post-publication verification failed (type=OperationalError, code=none)",
        ),
        (
            "public",
            RuntimeError("postgresql://user:secret@private-host/database"),
            "unexpected verification failure (RuntimeError)",
        ),
    ],
)
def test_main_retains_loaded_evidence_when_postpublication_verification_fails(
    monkeypatch, tmp_path, capsys, failed_check, error, expected_error
):
    outputs = tmp_path / "outputs"
    monkeypatch.setenv("GITHUB_OUTPUT", str(outputs))
    monkeypatch.setattr(scheduled, "season_for_date", lambda selected_date: 2026)
    monkeypatch.setattr(scheduled.publication, "get_db_url", lambda: "resolved-dsn")
    monkeypatch.setattr(
        scheduled.canary,
        "_inspect_target",
        lambda *values: (
            {"can_set_role": True, "assumed_role": scheduled.canary.PUBLISHER_ROLE},
            {},
        ),
    )
    monkeypatch.setattr(scheduled, "_require_policy", lambda *values: None)
    calls = []

    def publish(*values, **kwargs):
        calls.append((values, kwargs))
        return loaded_result()

    def private_check(*values):
        if failed_check == "private":
            raise error

    def public_check(*values):
        if failed_check == "public":
            raise error

    monkeypatch.setattr(scheduled.publication, "run_source_publication", publish)
    monkeypatch.setattr(scheduled, "_verify_private_receipt", private_check)
    monkeypatch.setattr(scheduled, "_verify_public_freshness", public_check)

    status = scheduled.main(["--season", "2026", "--expected-project-ref", PROJECT_REF])
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert status == 1
    assert payload == {
        "status": "verification_failed",
        "source": "sdv_fpi_weekly",
        "season": 2026,
        "run_id": RUN_ID,
        "generation_id": GENERATION_ID,
        "rows": 276,
        "sha": SHA256,
        "duration_s": 4.5,
        "phase": "post_publication_verification",
        "publication_status": "loaded",
        "error": expected_error,
    }
    assert len(calls) == 1
    assert outputs.read_text().splitlines()[-1] == "publication_attempted=true"
    assert "do-not-print" not in output
    assert "secret" not in output
    assert "private-host" not in output


def test_main_prepublication_failure_has_no_publication_evidence(monkeypatch, capsys):
    monkeypatch.setattr(scheduled, "season_for_date", lambda selected_date: 2026)
    monkeypatch.setattr(scheduled.publication, "get_db_url", lambda: "resolved-dsn")
    monkeypatch.setattr(
        scheduled.canary,
        "_inspect_target",
        lambda *values: (_ for _ in ()).throw(
            scheduled.canary.CanaryError("database migration ledger did not match")
        ),
    )
    monkeypatch.setattr(
        scheduled.publication,
        "run_source_publication",
        lambda *values, **kwargs: pytest.fail("prepublication failure must not publish"),
    )

    status = scheduled.main(["--season", "2026", "--expected-project-ref", PROJECT_REF])
    payload = json.loads(capsys.readouterr().out)

    assert status == 1
    assert payload["status"] == "failed"
    assert payload["run_id"] is None
    assert payload["generation_id"] is None
    assert payload["sha"] is None
    assert payload["rows"] == 0
    assert "publication_status" not in payload
    assert "phase" not in payload


def test_scheduled_workflow_is_daily_bounded_and_default_off():
    config = workflow()
    triggers = config[True]
    assert triggers == {"schedule": [{"cron": "17 10 * * *"}], "workflow_dispatch": None}
    assert config["concurrency"] == {
        "group": "${{ vars.SDV_FPI_RECEIPT_SEASON == '2026' && 'daily-season-load' || "
        "format('sdv-fpi-inactive-{0}', github.run_id) }}",
        "queue": "max",
        "cancel-in-progress": False,
    }
    assert config["permissions"] == {"contents": "read"}
    job = config["jobs"]["publish"]
    assert job["if"] == "${{ vars.SDV_FPI_RECEIPT_SEASON == '2026' }}"
    assert job["timeout-minutes"] == 30
    assert job["concurrency"] == {
        "group": "flat-file-load",
        "queue": "max",
        "cancel-in-progress": False,
    }
    assert "SUPABASE_DB_URL" not in config["env"]
    assert config["env"]["SDV_FPI_RECEIPT_SEASON"] == ("${{ vars.SDV_FPI_RECEIPT_SEASON }}")
    steps = {step.get("name"): step for step in job["steps"]}
    assert "Set dlt destination credentials" not in steps
    assert steps["Publish and verify FPI receipt"]["id"] == "publish_fpi"
    assert steps["Refresh external-rating consumers"]["if"] == (
        "${{ !cancelled() && steps.publish_fpi.outputs.publication_attempted == 'true' && "
        "(steps.publish_fpi.outcome == 'success' || steps.publish_fpi.outcome == 'failure') }}"
    )
    assert "${{" not in steps["Publish and verify FPI receipt"]["run"]


def test_database_secret_is_scoped_only_to_operational_steps():
    config = workflow()
    steps = config["jobs"]["publish"]["steps"]
    secret_reference = "${{ secrets.SUPABASE_DB_URL }}"
    credential_steps = [
        step.get("name")
        for step in steps
        if step.get("env", {}).get("SUPABASE_DB_URL") == secret_reference
    ]

    assert credential_steps == [
        "Publish and verify FPI receipt",
        "Refresh external-rating consumers",
    ]
    for step in steps:
        if step.get("name") not in credential_steps:
            assert secret_reference not in str(step)
    assert "SUPABASE_DB_URL" not in config["env"]
    assert "SUPABASE_DB_URL" not in config["jobs"]["publish"].get("env", {})
    assert "DESTINATION__POSTGRES__CREDENTIALS" not in WORKFLOW.read_text()


def test_every_shared_concurrency_holder_preserves_all_pending_runs():
    expected_daily_groups = {
        "daily-load.yml": "daily-season-load",
        "historical-refresh.yml": "daily-season-load",
        "backfill-sources.yml": "daily-season-load",
        "deploy-schema.yml": (
            "${{ inputs.compute_script == 'recover_season_projections' && "
            "'daily-season-load' || 'deploy-schema' }}"
        ),
    }
    for name, expected_group in expected_daily_groups.items():
        assert named_workflow(name)["concurrency"] == {
            "group": expected_group,
            "queue": "max",
            "cancel-in-progress": False,
        }

    assert named_workflow("flat-files.yml")["concurrency"] == {
        "group": "flat-file-load",
        "queue": "max",
        "cancel-in-progress": False,
    }


def test_fpi_and_daily_acquire_shared_groups_in_the_same_order():
    daily = named_workflow("daily-load.yml")
    flat_files = named_workflow("flat-files.yml")
    fpi = workflow()

    assert daily["concurrency"]["group"] == "daily-season-load"
    assert daily["jobs"]["flat_files"]["needs"] == "load"
    assert daily["jobs"]["flat_files"]["uses"] == "./.github/workflows/flat-files.yml"
    assert flat_files["concurrency"]["group"] == "flat-file-load"
    assert "'daily-season-load'" in fpi["concurrency"]["group"]
    assert fpi["jobs"]["publish"]["concurrency"]["group"] == "flat-file-load"


@pytest.mark.parametrize(
    ("failed_script", "attempted", "expected_status", "expected_count"),
    [
        ("", True, 0, 2),
        ("scripts/run_scheduled_fpi_receipts.py", True, 17, 2),
        ("scripts/refresh_marts.py", True, 17, 2),
        ("", False, 0, 1),
    ],
)
def test_scheduled_workflow_routes_arguments_refreshes_attempts_and_propagates_failure(
    tmp_path, failed_script, attempted, expected_status, expected_count
):
    calls_path = tmp_path / "calls.jsonl"
    outputs = tmp_path / "outputs"
    fake_python = tmp_path / "python"
    fake_python.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "with open(os.environ['TEST_CALLS'], 'a') as log:\n"
        "    log.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "if sys.argv[1] == 'scripts/run_scheduled_fpi_receipts.py':\n"
        "    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:\n"
        "        output.write('publication_attempted=' + os.environ['TEST_ATTEMPTED'] + '\\n')\n"
        "sys.exit(17 if sys.argv[1] == os.environ.get('TEST_FAIL_SCRIPT') else 0)\n"
    )
    fake_python.chmod(0o755)
    steps = workflow()["jobs"]["publish"]["steps"]
    publish = next(step for step in steps if step.get("id") == "publish_fpi")
    refresh = next(
        step for step in steps if step.get("name") == "Refresh external-rating consumers"
    )
    env = {
        **os.environ,
        "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
        "TEST_CALLS": str(calls_path),
        "TEST_FAIL_SCRIPT": failed_script,
        "TEST_ATTEMPTED": str(attempted).lower(),
        "GITHUB_OUTPUT": str(outputs),
        "SDV_FPI_RECEIPT_SEASON": "2026",
        "EXPECTED_PROJECT_REF": PROJECT_REF,
    }
    first = subprocess.run(
        ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", publish["run"]],
        env=env,
        check=False,
    )
    status = first.returncode
    if attempted:
        second = subprocess.run(
            ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", refresh["run"]],
            env=env,
            check=False,
        )
        status = status or second.returncode
    calls = [json.loads(line) for line in calls_path.read_text().splitlines()]
    assert status == expected_status
    assert len(calls) == expected_count
    assert calls[0] == [
        "scripts/run_scheduled_fpi_receipts.py",
        "--season",
        "2026",
        "--expected-project-ref",
        PROJECT_REF,
    ]
    if attempted:
        assert calls[1] == [
            "scripts/refresh_marts.py",
            "--views",
            "marts.epa_crossvalidation",
        ]
