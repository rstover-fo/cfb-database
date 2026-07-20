"""Unit tests for deploy_schema's pure plan-building (no DB, no subprocess)."""

import json

import pytest

from scripts.deploy_schema import (
    VALID_ACTIONS,
    BackfillSpec,
    Plan,
    load_manifest,
    plan_from_cli,
    plan_from_manifest,
    validate_plan,
)


class TestValidActions:
    def test_expected_actions(self):
        assert VALID_ACTIONS == {"presence_check", "apply", "backfill"}


class TestPlanFromManifestPresenceCheck:
    def test_minimal_presence_check(self):
        plan = plan_from_manifest({"action": "presence_check"})
        assert plan.action == "presence_check"
        assert plan.strict is False
        assert plan.files == []
        assert plan.backfill is None

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


class TestPlanFromCli:
    def test_presence_check_minimal(self):
        plan = plan_from_cli(action="presence_check")
        assert plan.action == "presence_check"
        assert plan.strict is False

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


class TestLoadManifest:
    def test_reads_json_file(self, tmp_path):
        manifest_path = tmp_path / "deploy-manifest.json"
        manifest_path.write_text(json.dumps({"action": "presence_check"}))
        manifest = load_manifest(str(manifest_path))
        assert manifest == {"action": "presence_check"}
