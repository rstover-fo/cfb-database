"""Unit tests for deploy_schema's pure plan-building (no DB, no subprocess)."""

import json

import pytest

from scripts.deploy_schema import (
    COMPUTE_SCRIPTS,
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
    def test_expected_actions(self):
        assert VALID_ACTIONS == {"presence_check", "apply", "backfill", "compute"}


class TestComputeScripts:
    def test_expected_allowlist(self):
        assert COMPUTE_SCRIPTS == {
            "check_backtest",
            "compute_house_elo",
            "compute_adjusted_epa",
            "compute_predictions",
            # TEMPORARY (P3.2 Lane B) -- see deploy_schema.py's comment.
            "probe_metrics_wp",
            "compute_adjusted_epa_week",
            "build_features",
            "train_model",
            "score_fitted",
            "tune_params",
            "calibrate_live_wp",
            "poll_scoreboard",
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
    def test_apply_fields(self):
        manifest = {
            "action": "apply",
            "marts_from": "029",
            "marts_only": "011",
            "files": ["src/schemas/api/019_x.sql", "src/schemas/functions/y.sql"],
            "refresh": True,
        }
        plan = plan_from_manifest(manifest)
        assert plan.action == "apply"
        assert plan.marts_from == "029"
        assert plan.marts_only == "011"
        assert plan.files == ["src/schemas/api/019_x.sql", "src/schemas/functions/y.sql"]
        assert plan.refresh is True

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

    def test_apply_flags_mapped(self):
        plan = plan_from_cli(
            action="apply",
            marts_from="029",
            marts_only="011",
            files="src/schemas/api/019_x.sql, src/schemas/functions/y.sql",
            refresh=True,
        )
        assert plan.action == "apply"
        assert plan.marts_from == "029"
        assert plan.marts_only == "011"
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
