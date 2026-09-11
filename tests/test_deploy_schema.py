"""Unit tests for deploy_schema's pure plan-building (no DB, no subprocess)."""

import json
import sys
from types import SimpleNamespace

import pytest

import scripts.deploy_schema as deploy_schema
from scripts.deploy_schema import (
    COMPUTE_SCRIPTS,
    MANAGED_ACTION_MODES,
    PRODUCTION_MANIFEST,
    VALID_ACTIONS,
    BackfillSpec,
    ComputeSpec,
    Plan,
    load_manifest,
    plan_from_cli,
    plan_from_manifest,
    validate_plan,
)


class TestValidActions:
    def test_recovery_manifest_rejected_but_dispatch_plan_allowed(self):
        with pytest.raises(ValueError, match="requires workflow_dispatch"):
            plan_from_manifest(
                {
                    "action": "compute",
                    "compute": {"script": "recover_season_projections", "args": ["--execute"]},
                }
            )
        plan = plan_from_cli(
            action="compute", compute_script="recover_season_projections", compute_args="--execute"
        )
        assert plan.compute.args == ["--execute"]

    def test_expected_actions(self):
        assert MANAGED_ACTION_MODES == {
            "managed_plan": "plan",
            "managed_upgrade": "upgrade",
            "managed_status": "status",
        }
        assert PRODUCTION_MANIFEST == "src/schemas/production-manifest.json"
        assert VALID_ACTIONS == {
            "presence_check",
            "apply",
            "backfill",
            "compute",
            "managed_plan",
            "managed_upgrade",
            "managed_status",
        }


class TestComputeScripts:
    def test_expected_allowlist(self):
        assert COMPUTE_SCRIPTS == {
            "adopt_warehouse_catalog",
            "export_warehouse_catalog",
            "probe_projection_schedule",
            "verify_load",
            "recover_season_projections",
            "check_backtest",
            "compute_house_elo",
            "compute_adjusted_epa",
            "compute_predictions",
            # TEMPORARY (P3.2 Lane B) -- see deploy_schema.py's comment.
            "probe_metrics_wp",
            "compute_adjusted_epa_week",
            # Drive-chain EP model P1 (2026-08-08 plan).
            "compute_drive_chain",
            "build_features",
            "train_model",
            "score_fitted",
            "tune_params",
            "calibrate_live_wp",
            "poll_scoreboard",
            # Phase 4 season projections. Runnable through the deploy path so
            # the compute chain does not depend on a healthy ingest run.
            "simulate_season",
            # Section 4.3 preseason backtest -- read-only, reports to stdout.
            "backtest_preseason",
            # Section 2.5 feature screen -- read-only, reports to stdout.
            "screen_preseason_features",
        }


class TestPlanFromManifestPresenceCheck:
    def test_minimal_presence_check(self):
        plan = plan_from_manifest({"action": "presence_check"})
        assert plan.action == "presence_check"
        assert plan.strict is False
        assert plan.files == []
        assert plan.backfill is None
        assert plan.compute is None

    def test_strict_flag_passthrough(self):
        plan = plan_from_manifest({"action": "presence_check", "strict": True})
        assert plan.strict is True


class TestPlanFromManifestApply:
    def test_legacy_mart_selectors_are_rejected(self):
        manifest = {
            "action": "apply",
            "marts_from": "029",
            "files": ["src/schemas/api/019_x.sql", "src/schemas/functions/y.sql"],
            "refresh": True,
        }
        with pytest.raises(ValueError, match="legacy mart selectors"):
            plan_from_manifest(manifest)

    def test_apply_defaults(self):
        plan = plan_from_manifest({"action": "apply"})
        assert plan.marts_from is None
        assert plan.marts_only is None
        assert plan.files == []
        assert plan.refresh is False


class TestPlanFromManifestBackfill:
    def test_backfill_fields(self):
        manifest = {
            "action": "backfill",
            "backfill": {"start": 2014, "end": 2025, "sources": "stats,betting"},
        }
        plan = plan_from_manifest(manifest)
        assert plan.action == "backfill"
        assert plan.backfill == BackfillSpec(start=2014, end=2025, sources="stats,betting")

    def test_backfill_missing_block_rejected(self):
        with pytest.raises(ValueError, match="backfill"):
            plan_from_manifest({"action": "backfill"})

    def test_backfill_start_after_end_rejected(self):
        manifest = {
            "action": "backfill",
            "backfill": {"start": 2025, "end": 2014, "sources": "stats"},
        }
        with pytest.raises(ValueError, match="after"):
            plan_from_manifest(manifest)

    def test_backfill_missing_end_rejected(self):
        manifest = {"action": "backfill", "backfill": {"start": 2014, "sources": "stats"}}
        with pytest.raises(ValueError):
            plan_from_manifest(manifest)

    def test_backfill_equal_start_end_allowed(self):
        manifest = {
            "action": "backfill",
            "backfill": {"start": 2020, "end": 2020, "sources": "stats"},
        }
        plan = plan_from_manifest(manifest)
        assert plan.backfill.start == plan.backfill.end == 2020


class TestPlanFromManifestCompute:
    def test_compute_fields(self):
        manifest = {
            "action": "compute",
            "compute": {"script": "compute_house_elo", "args": ["--full"]},
        }
        plan = plan_from_manifest(manifest)
        assert plan.action == "compute"
        assert plan.compute == ComputeSpec(script="compute_house_elo", args=["--full"])

    def test_compute_args_optional(self):
        manifest = {"action": "compute", "compute": {"script": "compute_adjusted_epa"}}
        plan = plan_from_manifest(manifest)
        assert plan.compute == ComputeSpec(script="compute_adjusted_epa", args=[])

    def test_compute_refresh_passthrough(self):
        manifest = {
            "action": "compute",
            "compute": {"script": "compute_predictions"},
            "refresh": True,
        }
        plan = plan_from_manifest(manifest)
        assert plan.refresh is True

    @pytest.mark.parametrize(
        "extra", [{"refresh": True}, {"refresh_views": ["marts.epa_crossvalidation"]}]
    )
    def test_verification_cannot_refresh_marts(self, extra):
        with pytest.raises(ValueError, match="verify_load is read-only"):
            plan_from_manifest({"action": "compute", "compute": {"script": "verify_load"}, **extra})

    @pytest.mark.parametrize("exit_code", [0, 1])
    def test_verification_runs_only_verifier_and_preserves_outcome(self, monkeypatch, exit_code):
        from scripts import deploy_schema

        calls = []

        def run(cmd, label):
            calls.append((cmd, label))
            return exit_code

        monkeypatch.setattr(deploy_schema, "run_cmd", run)
        plan = plan_from_cli(
            action="compute", compute_script="verify_load", compute_args="--season,2026"
        )

        assert deploy_schema.run_compute(plan) == exit_code
        assert calls == [
            (
                [
                    deploy_schema.sys.executable,
                    str(deploy_schema.SCRIPTS_DIR / "verify_load.py"),
                    "--season",
                    "2026",
                ],
                "compute verify_load",
            )
        ]

    def test_compute_missing_block_rejected(self):
        with pytest.raises(ValueError, match="compute block"):
            plan_from_manifest({"action": "compute"})

    def test_compute_unknown_script_rejected(self):
        manifest = {"action": "compute", "compute": {"script": "compute_something_else"}}
        with pytest.raises(ValueError, match="compute_house_elo"):
            plan_from_manifest(manifest)

    def test_refresh_views_populated(self):
        manifest = {
            "action": "compute",
            "compute": {"script": "compute_adjusted_epa_week"},
            "refresh": True,
            "refresh_views": ["marts.team_week_features", "marts.adjusted_epa_week"],
        }
        plan = plan_from_manifest(manifest)
        assert plan.refresh_views == ["marts.team_week_features", "marts.adjusted_epa_week"]

    def test_refresh_views_defaults_empty(self):
        manifest = {"action": "compute", "compute": {"script": "compute_house_elo"}}
        plan = plan_from_manifest(manifest)
        assert plan.refresh_views == []


class TestBadAction:
    def test_unknown_action_rejected(self):
        with pytest.raises(ValueError, match="invalid action"):
            plan_from_manifest({"action": "delete_everything"})

    def test_missing_action_rejected(self):
        with pytest.raises(ValueError):
            plan_from_manifest({})

    def test_validate_plan_directly(self):
        plan = Plan(action="not_a_real_action")
        with pytest.raises(ValueError, match="invalid action"):
            validate_plan(plan)

    def test_compute_missing_block_rejected_directly(self):
        plan = Plan(action="compute")
        with pytest.raises(ValueError, match="compute block"):
            validate_plan(plan)

    def test_compute_unknown_script_rejected_directly(self):
        plan = Plan(action="compute", compute=ComputeSpec(script="not_allowlisted"))
        with pytest.raises(ValueError, match="invalid compute script") as exc_info:
            validate_plan(plan)
        # Message lists the allowlist so a bad script name is self-explanatory in CI logs.
        for script in COMPUTE_SCRIPTS:
            assert script in str(exc_info.value)

    def test_compute_allowlisted_script_accepted_directly(self):
        plan = Plan(action="compute", compute=ComputeSpec(script="compute_house_elo"))
        validate_plan(plan)  # does not raise

    def test_refresh_views_without_dot_rejected_directly(self):
        plan = Plan(action="presence_check", refresh_views=["marts_team_week_features"])
        with pytest.raises(ValueError, match="refresh_views"):
            validate_plan(plan)

    def test_refresh_views_with_dot_accepted_directly(self):
        plan = Plan(action="presence_check", refresh_views=["marts.team_week_features"])
        validate_plan(plan)  # does not raise


class TestPlanFromCli:
    def test_presence_check_minimal(self):
        plan = plan_from_cli(action="presence_check")
        assert plan.action == "presence_check"
        assert plan.strict is False
        assert plan.compute is None

    def test_presence_check_strict(self):
        plan = plan_from_cli(action="presence_check", strict=True)
        assert plan.strict is True

    def test_apply_fields_mapped_without_legacy_mart_selector(self):
        plan = plan_from_cli(
            action="apply",
            files="src/schemas/api/019_x.sql, src/schemas/functions/y.sql",
            refresh=True,
        )
        assert plan.action == "apply"
        # Comma-separated CLI string is split and whitespace-stripped.
        assert plan.files == ["src/schemas/api/019_x.sql", "src/schemas/functions/y.sql"]
        assert plan.refresh is True

    def test_apply_no_files_gives_empty_list(self):
        plan = plan_from_cli(action="apply")
        assert plan.files == []

    def test_files_blank_entries_dropped(self):
        plan = plan_from_cli(action="apply", files="a.sql,,b.sql,")
        assert plan.files == ["a.sql", "b.sql"]

    def test_backfill_flags_mapped(self):
        plan = plan_from_cli(
            action="backfill",
            backfill_start=2014,
            backfill_end=2025,
            sources="stats,betting",
        )
        assert plan.backfill == BackfillSpec(start=2014, end=2025, sources="stats,betting")

    def test_backfill_start_after_end_rejected(self):
        with pytest.raises(ValueError, match="after"):
            plan_from_cli(
                action="backfill", backfill_start=2025, backfill_end=2014, sources="stats"
            )

    def test_backfill_missing_bounds_rejected(self):
        with pytest.raises(ValueError):
            plan_from_cli(action="backfill", sources="stats")

    def test_bad_action_rejected(self):
        with pytest.raises(ValueError, match="invalid action"):
            plan_from_cli(action="not_a_real_action")

    def test_compute_flags_mapped(self):
        plan = plan_from_cli(
            action="compute",
            compute_script="compute_house_elo",
            compute_args="--full, --season 2024",
        )
        assert plan.action == "compute"
        # Comma-separated CLI string is split and whitespace-stripped, like files/sources.
        assert plan.compute == ComputeSpec(
            script="compute_house_elo", args=["--full", "--season 2024"]
        )

    def test_compute_no_args_gives_empty_list(self):
        plan = plan_from_cli(action="compute", compute_script="compute_predictions")
        assert plan.compute == ComputeSpec(script="compute_predictions", args=[])

    def test_compute_args_blank_entries_dropped(self):
        plan = plan_from_cli(
            action="compute", compute_script="compute_adjusted_epa", compute_args="a,,b,"
        )
        assert plan.compute.args == ["a", "b"]

    def test_compute_unknown_script_rejected(self):
        with pytest.raises(ValueError, match="invalid compute script"):
            plan_from_cli(action="compute", compute_script="not_allowlisted")

    def test_compute_missing_script_rejected(self):
        with pytest.raises(ValueError, match="compute block"):
            plan_from_cli(action="compute")

    def test_compute_args_without_script_rejected(self):
        with pytest.raises(ValueError, match="--compute-script"):
            plan_from_cli(action="compute", compute_args="--full")

    def test_compute_refresh_flag_mapped(self):
        plan = plan_from_cli(action="compute", compute_script="compute_house_elo", refresh=True)
        assert plan.refresh is True

    def test_refresh_views_flag_mapped(self):
        plan = plan_from_cli(
            action="compute",
            compute_script="compute_adjusted_epa_week",
            refresh_views="marts.team_week_features, marts.adjusted_epa_week",
        )
        # Comma-separated CLI string is split and whitespace-stripped, like files/sources.
        assert plan.refresh_views == ["marts.team_week_features", "marts.adjusted_epa_week"]

    def test_refresh_views_no_flag_gives_empty_list(self):
        plan = plan_from_cli(action="compute", compute_script="compute_house_elo")
        assert plan.refresh_views == []

    def test_refresh_views_blank_entries_dropped(self):
        plan = plan_from_cli(action="apply", refresh_views="marts.a,,marts.b,")
        assert plan.refresh_views == ["marts.a", "marts.b"]

    def test_refresh_views_without_dot_rejected(self):
        with pytest.raises(ValueError, match="refresh_views"):
            plan_from_cli(action="presence_check", refresh_views="bad_view_name")


class TestLoadManifest:
    def test_reads_json_file(self, tmp_path):
        manifest_path = tmp_path / "deploy-manifest.json"
        manifest_path.write_text(json.dumps({"action": "presence_check"}))
        manifest = load_manifest(str(manifest_path))
        assert manifest == {"action": "presence_check"}


class TestMartReleasePlans:
    def test_manifest_maps_release_and_read_only_plan(self):
        plan = plan_from_manifest(
            {
                "action": "apply",
                "mart_release": "src/schemas/mart-releases/example.json",
                "plan": True,
            }
        )
        assert plan.mart_release == "src/schemas/mart-releases/example.json"
        assert plan.plan is True

    def test_cli_maps_release_execution_by_default(self):
        plan = plan_from_cli(action="apply", mart_release="release.json")
        assert plan.mart_release == "release.json"
        assert plan.plan is False

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("marts_from", "029"),
            ("marts_only", "011"),
            ("files", ["src/schemas/api/example.sql"]),
            ("refresh", True),
            ("refresh_views", ["marts.example"]),
            ("backfill", BackfillSpec(start=2025, end=2025, sources="stats")),
            ("compute", ComputeSpec(script="compute_house_elo")),
        ],
    )
    def test_release_rejects_legacy_deploy_fields(self, field, value):
        plan = Plan(action="apply", mart_release="release.json")
        setattr(plan, field, value)
        with pytest.raises(ValueError, match=field):
            validate_plan(plan)

    def test_plan_requires_release(self):
        with pytest.raises(ValueError, match="requires a mart_release"):
            plan_from_cli(action="apply", plan=True)

    def test_manifest_plan_must_be_boolean(self):
        with pytest.raises(ValueError, match="plan must be a boolean"):
            plan_from_manifest({"action": "apply", "mart_release": "release.json", "plan": "false"})

    def test_release_requires_apply_action(self):
        with pytest.raises(ValueError, match="only for the apply action"):
            plan_from_cli(action="presence_check", mart_release="release.json")

    @pytest.mark.parametrize("selector", [{"marts_from": "029"}, {"marts_only": "011"}])
    def test_real_apply_rejects_legacy_mart_selectors(self, selector):
        with pytest.raises(ValueError, match="dependency-complete"):
            plan_from_cli(action="apply", **selector)

    @pytest.mark.parametrize(
        "files",
        [
            "src/schemas/marts/001_example.sql",
            "src/schemas/api/001_ok.sql,src/schemas/marts/002_blocked.sql",
            "src/schemas/api/../marts/003_traversal.sql",
        ],
    )
    def test_apply_rejects_mart_file_in_any_position_or_spelling(self, files):
        with pytest.raises(ValueError, match="per-file migration route"):
            plan_from_cli(action="apply", files=files)

    def test_release_execute_dispatches_exactly_one_atomic_runner(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            deploy_schema,
            "run_cmd",
            lambda cmd, label: calls.append((cmd, label)) or 0,
        )

        rc = deploy_schema.execute_plan(
            Plan(action="apply", mart_release="src/schemas/mart-releases/example.json")
        )

        assert rc == 0
        assert calls == [
            (
                [
                    sys.executable,
                    str(deploy_schema.RUN_MARTS),
                    "--release",
                    "src/schemas/mart-releases/example.json",
                ],
                "run_marts --release src/schemas/mart-releases/example.json",
            )
        ]

    def test_release_plan_dispatches_read_only_flag(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            deploy_schema,
            "run_cmd",
            lambda cmd, label: calls.append(cmd) or 0,
        )

        rc = deploy_schema.execute_plan(
            Plan(action="apply", mart_release="release.json", plan=True)
        )

        assert rc == 0
        assert calls == [
            [
                sys.executable,
                str(deploy_schema.RUN_MARTS),
                "--release",
                "release.json",
                "--plan",
            ]
        ]


class TestManagedProductionActions:
    @pytest.mark.parametrize(
        ("action", "mode"),
        sorted(MANAGED_ACTION_MODES.items()),
    )
    def test_cli_maps_explicit_managed_action(self, action, mode):
        plan = plan_from_cli(action=action)

        assert plan.action == action
        assert MANAGED_ACTION_MODES[plan.action] == mode

    @pytest.mark.parametrize("action", sorted(MANAGED_ACTION_MODES))
    def test_push_manifest_rejects_managed_actions(self, action):
        with pytest.raises(ValueError, match="requires workflow_dispatch"):
            plan_from_manifest({"action": action})

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("mart_release", "release.json"),
            ("plan", True),
            ("marts_from", "029"),
            ("marts_only", "011"),
            ("files", ["src/schemas/api/example.sql"]),
            ("refresh", True),
            ("refresh_views", ["marts.example"]),
            ("backfill", BackfillSpec(start=2026, end=2026, sources="stats")),
            ("strict", True),
            ("compute", ComputeSpec(script="compute_house_elo")),
        ],
    )
    def test_managed_action_rejects_legacy_deploy_fields(self, field, value):
        plan = Plan(action="managed_plan")
        setattr(plan, field, value)

        with pytest.raises(ValueError, match=field):
            validate_plan(plan)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"mart_release": "release.json"},
            {"plan": True},
            {"marts_from": "029"},
            {"marts_only": "011"},
            {"files": "src/schemas/api/example.sql"},
            {"refresh": True},
            {"refresh_views": "marts.example"},
            {"backfill_start": 2026},
            {"backfill_end": 2026},
            {"sources": "stats"},
            {"strict": True},
            {"compute_script": "compute_house_elo"},
            {"compute_args": "--full"},
        ],
    )
    def test_managed_cli_rejects_each_incompatible_input(self, kwargs):
        with pytest.raises(ValueError):
            plan_from_cli(action="managed_status", **kwargs)

    @pytest.mark.parametrize(
        ("action", "mode"),
        sorted(MANAGED_ACTION_MODES.items()),
    )
    def test_dispatches_fixed_manifest_and_managed_environment(self, monkeypatch, action, mode):
        secret = "postgresql://deploy-user:sensitive-password@db.example:5432/postgres"
        monkeypatch.setenv("WAREHOUSE_DB_URL", secret)
        monkeypatch.setenv("SUPABASE_DB_URL", secret)
        monkeypatch.setenv("DATABASE_URL", secret)
        monkeypatch.setenv("DESTINATION__POSTGRES__CREDENTIALS", secret)
        calls = []

        def fake_run_cmd(cmd, label, *, env=None):
            calls.append((cmd, label, env))
            return 0

        monkeypatch.setattr(deploy_schema, "run_cmd", fake_run_cmd)

        assert deploy_schema.execute_plan(Plan(action=action)) == 0
        assert len(calls) == 1
        cmd, label, env = calls[0]
        assert cmd == [
            sys.executable,
            str(deploy_schema.BOOTSTRAP_WAREHOUSE),
            mode,
            "--manifest",
            PRODUCTION_MANIFEST,
        ]
        assert label == f"managed production {mode}"
        assert env["WAREHOUSE_DB_URL"] == secret
        assert "SUPABASE_DB_URL" not in env
        assert "DATABASE_URL" not in env
        assert "DESTINATION__POSTGRES__CREDENTIALS" not in env
        assert secret not in " ".join(cmd)
        assert secret not in label

    def test_managed_runner_failure_is_returned_and_not_retried(self, monkeypatch):
        calls = []

        def fake_run_cmd(cmd, label, *, env=None):
            calls.append((cmd, label, env))
            return 23

        monkeypatch.setattr(deploy_schema, "run_cmd", fake_run_cmd)

        assert deploy_schema.execute_plan(Plan(action="managed_upgrade")) == 23
        assert len(calls) == 1

    def test_managed_secret_is_not_logged(self, monkeypatch, caplog):
        secret = "postgresql://deploy-user:sensitive-password@db.example:5432/postgres"
        caplog.set_level("INFO", logger=deploy_schema.__name__)
        monkeypatch.setenv("WAREHOUSE_DB_URL", secret)
        monkeypatch.setattr(
            deploy_schema.subprocess,
            "run",
            lambda cmd, env=None: SimpleNamespace(returncode=1),
        )

        assert deploy_schema.execute_plan(Plan(action="managed_status")) == 1
        assert secret not in caplog.text
        assert "sensitive-password" not in caplog.text

    def test_manifest_and_cli_flags_are_mutually_exclusive(self, capsys):
        with pytest.raises(SystemExit) as exc_info:
            deploy_schema.main(["--manifest", "deploy-manifest.json", "--action", "managed_status"])

        assert exc_info.value.code == 2
        assert "--manifest cannot be combined" in capsys.readouterr().err


def test_workflow_scopes_managed_credentials_and_observes_upgrade_status():
    workflow = (deploy_schema.REPO_ROOT / ".github/workflows/deploy-schema.yml").read_text()
    workflow_env = workflow.split("env:\n", 1)[1].split("jobs:\n", 1)[0]
    managed_step = workflow.split("      - name: Run managed production action\n", 1)[1].split(
        "      - name: Observe managed status after upgrade attempt\n", 1
    )[0]

    for action in MANAGED_ACTION_MODES:
        assert f"          - {action}\n" in workflow
    assert "id: managed_deploy" in workflow
    for input_name in (
        "MART_RELEASE_INPUT",
        "PLAN_INPUT",
        "FILES_INPUT",
        "REFRESH_INPUT",
        "BACKFILL_START_INPUT",
        "BACKFILL_END_INPUT",
        "SOURCES_INPUT",
        "COMPUTE_SCRIPT_INPUT",
        "COMPUTE_ARGS_INPUT",
    ):
        assert input_name in managed_step
    assert 'python scripts/deploy_schema.py "${ARGS[@]}"' in managed_step
    assert "inputs.action == 'managed_upgrade'" in workflow
    assert "steps.managed_deploy.outcome != 'skipped'" in workflow
    assert "run: python scripts/deploy_schema.py --action managed_status" in workflow
    assert "WAREHOUSE_DB_URL: ${{ secrets.SUPABASE_DB_URL }}" in workflow
    assert "SUPABASE_DB_URL:" not in workflow_env
    assert "WAREHOUSE_DB_URL:" not in workflow_env
    assert "          SUPABASE_DB_URL:" not in managed_step
    assert "run: python scripts/deploy_schema.py --action ${{ inputs.action }}" not in workflow
