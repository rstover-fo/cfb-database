#!/usr/bin/env python3
"""Driver for push-triggered schema deploys (.github/workflows/deploy-schema.yml).

Reads a plan from either a JSON manifest (pushed to a `deploy/**` branch as
`deploy-manifest.json`, the mechanism `docs/plans/2026-07-19-tier1-analytics-unlock-plan.md`
describes) or CLI flags (workflow_dispatch, for human-triggered runs), then
executes it by shelling out to the existing driver scripts. Managed production
schema actions are workflow-dispatch-only and always use the repository's fixed
production manifest.

Manifest schema:
    {
      "action": "presence_check" | "apply" | "backfill" | "compute",
      "mart_release": "src/schemas/mart-releases/example.json",
      "plan": false,
      "files": ["src/schemas/api/019_x.sql", ...],
      "refresh": true,
      "refresh_views": ["marts.team_week_features", "marts.adjusted_epa_week"],
      "backfill": {"start": 2014, "end": 2025, "sources": "stats,betting"},
      "compute": {"script": "compute_house_elo", "args": ["--full"]}
    }

All fields besides "action" are optional. A mart release is exclusive: it
cannot be combined with legacy mart selectors, per-file SQL, refresh, backfill,
or compute fields.

Usage:
    python scripts/deploy_schema.py --manifest deploy-manifest.json
    python scripts/deploy_schema.py --action presence_check --strict
    python scripts/deploy_schema.py --action apply \\
        --mart-release src/schemas/mart-releases/example.json --plan
    python scripts/deploy_schema.py --action apply \\
        --files src/schemas/functions/get_player_game_log.sql --refresh
    python scripts/deploy_schema.py --action backfill \\
        --backfill-start 2014 --backfill-end 2025 --sources stats,betting
    python scripts/deploy_schema.py --action managed_plan
    python scripts/deploy_schema.py --action managed_upgrade
    python scripts/deploy_schema.py --action managed_status

Plan-building (plan_from_manifest / plan_from_cli / validate_plan) is pure --
no subprocesses, no DB -- so it can be unit tested directly; execute_plan is
the only part that shells out.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCRIPTS_DIR = Path(__file__).parent
REPO_ROOT = SCRIPTS_DIR.parent.resolve()
MARTS_DIR = (REPO_ROOT / "src" / "schemas" / "marts").resolve()
RUN_MARTS = SCRIPTS_DIR / "run_marts.py"
RUN_MIGRATIONS = SCRIPTS_DIR / "run_migrations.py"
REFRESH_MARTS = SCRIPTS_DIR / "refresh_marts.py"
LOAD_SEASON = SCRIPTS_DIR / "load_season.py"
CHECK_PRESENCE = SCRIPTS_DIR / "check_presence.py"
BOOTSTRAP_WAREHOUSE = SCRIPTS_DIR / "bootstrap_warehouse.py"
PRODUCTION_MANIFEST = "src/schemas/production-manifest.json"

MANAGED_ACTION_MODES = {
    "managed_plan": "plan",
    "managed_upgrade": "upgrade",
    "managed_status": "status",
}
VALID_ACTIONS = {"presence_check", "apply", "backfill", "compute", *MANAGED_ACTION_MODES}

# Allowlist of compute scripts the "compute" action may run (scripts/<name>.py).
# These land in later Tier 2 + Tier 3 phases -- membership is checked, not file
# existence, so this action can be wired up before the scripts themselves exist.
COMPUTE_SCRIPTS = {
    "adopt_warehouse_catalog",
    "export_warehouse_catalog",
    # Read-only one-request schedule probe for the September 2026 recovery.
    "probe_projection_schedule",
    # Read-only post-load checks without replaying ingestion or mart refreshes.
    "verify_load",
    "recover_season_projections",
    "compute_house_elo",
    "compute_adjusted_epa",
    "compute_predictions",
    "check_backtest",
    # TEMPORARY (P3.2 Lane B): read-only field-shape probe for /metrics/wp,
    # scripts/probe_metrics_wp.py. Remove this entry once the win-probability
    # deploy sequence (docs/pipeline-manifest.md row 47) is done -- it exists
    # only so the probe can run through the same deploy/compute mechanism as
    # everything else instead of a one-off workflow.
    "probe_metrics_wp",
    "compute_adjusted_epa_week",
    # Drive-chain EP model (docs/plans/2026-08-08-drive-chain-ep-model-plan.md,
    # P1): league transition matrices + Goldner-basis EP per era.
    "compute_drive_chain",
    "build_features",
    "train_model",
    "score_fitted",
    "tune_params",
    "calibrate_live_wp",
    "poll_scoreboard",
    # Phase 4 of the preseason-outlook plan. Needed here, not just in
    # daily-load.yml, because the compute chain must be runnable independently
    # of ingest -- a rate-limited season load should not be able to block
    # season projections, which touch no API at all.
    "simulate_season",
    # Section 4.3 preseason backtest. Read-only -- re-scores history from
    # week-1 feature vectors and reports to stdout; writes no rows.
    "backtest_preseason",
    # Section 2.5 candidate-feature screen. Read-only -- partial correlations
    # and column discovery, reports to stdout. Previously runnable only via an
    # interactive MCP session, which made the gate depend on a connector being
    # authorized rather than on the deploy path everything else uses.
    "screen_preseason_features",
}

# Marts refreshed after a compute run when plan.refresh is set. One home for
# this list so deploy_schema.py and any caller agree on the Tier 2 mart names.
TIER2_MART_VIEWS = [
    "marts.house_elo",
    "marts.house_elo_game",
    "marts.team_adjusted_epa",
    "marts.scored_matchup_edges",
    "marts.prediction_accuracy",
]

# Marts refreshed after the Tier 3 feature-building / adjusted-EPA-week
# computes, when a plan supplies them explicitly via refresh_views.
TIER3_MART_VIEWS = [
    "marts.team_week_features",
    "marts.adjusted_epa_week",
]


@dataclass
class BackfillSpec:
    start: int | None
    end: int | None
    sources: str = ""


@dataclass
class ComputeSpec:
    script: str
    args: list[str] = field(default_factory=list)


@dataclass
class Plan:
    action: str
    mart_release: str | None = None
    plan: bool = False
    marts_from: str | None = None
    marts_only: str | None = None
    files: list[str] = field(default_factory=list)
    refresh: bool = False
    refresh_views: list[str] = field(default_factory=list)
    backfill: BackfillSpec | None = None
    strict: bool = False
    compute: ComputeSpec | None = None


# --------------------------------------------------------------------------
# Plan building (pure -- no subprocess, no DB)
# --------------------------------------------------------------------------


def validate_plan(plan: Plan) -> None:
    """Raise ValueError if the plan is not executable. Called by both builders."""
    if plan.action not in VALID_ACTIONS:
        raise ValueError(f"invalid action {plan.action!r}; must be one of {sorted(VALID_ACTIONS)}")

    if plan.action in MANAGED_ACTION_MODES:
        conflicts = []
        if plan.mart_release:
            conflicts.append("mart_release")
        if plan.plan:
            conflicts.append("plan")
        if plan.marts_from:
            conflicts.append("marts_from")
        if plan.marts_only:
            conflicts.append("marts_only")
        if plan.files:
            conflicts.append("files")
        if plan.refresh:
            conflicts.append("refresh")
        if plan.refresh_views:
            conflicts.append("refresh_views")
        if plan.backfill is not None:
            conflicts.append("backfill")
        if plan.strict:
            conflicts.append("strict")
        if plan.compute is not None:
            conflicts.append("compute")
        if conflicts:
            raise ValueError(
                f"{plan.action} uses the fixed production manifest and cannot be combined "
                "with legacy deploy fields: " + ", ".join(conflicts)
            )

    if plan.action == "backfill":
        if plan.backfill is None:
            raise ValueError("backfill action requires a backfill start/end/sources block")
        if plan.backfill.start is None or plan.backfill.end is None:
            raise ValueError("backfill action requires both a start and an end season")
        if plan.backfill.start > plan.backfill.end:
            raise ValueError(
                f"backfill start season ({plan.backfill.start}) is after "
                f"end season ({plan.backfill.end})"
            )

    if plan.action == "compute":
        if plan.compute is None:
            raise ValueError("compute action requires a compute block")
        if plan.compute.script not in COMPUTE_SCRIPTS:
            raise ValueError(
                f"invalid compute script {plan.compute.script!r}; "
                f"must be one of {sorted(COMPUTE_SCRIPTS)}"
            )
        if plan.compute.script == "verify_load" and (plan.refresh or plan.refresh_views):
            raise ValueError("verify_load is read-only and cannot request mart refreshes")

    if not isinstance(plan.plan, bool):
        raise ValueError("plan must be a boolean")
    if plan.mart_release is not None and (
        not isinstance(plan.mart_release, str) or not plan.mart_release.strip()
    ):
        raise ValueError("mart_release must be a nonempty manifest path")

    if plan.plan and plan.mart_release is None:
        raise ValueError("--plan requires a mart_release manifest")

    if plan.mart_release is not None:
        if plan.action != "apply":
            raise ValueError("mart_release is valid only for the apply action")
        conflicts = []
        if plan.marts_from:
            conflicts.append("marts_from")
        if plan.marts_only:
            conflicts.append("marts_only")
        if plan.files:
            conflicts.append("files")
        if plan.refresh:
            conflicts.append("refresh")
        if plan.refresh_views:
            conflicts.append("refresh_views")
        if plan.backfill is not None:
            conflicts.append("backfill")
        if plan.compute is not None:
            conflicts.append("compute")
        if plan.strict:
            conflicts.append("strict")
        if conflicts:
            raise ValueError(
                "mart_release cannot be combined with legacy deploy fields: " + ", ".join(conflicts)
            )

    if plan.action == "apply" and (plan.marts_from or plan.marts_only):
        raise ValueError(
            "legacy mart selectors cannot execute real changes; "
            "use mart_release with a dependency-complete release manifest"
        )

    for sql_file in plan.files:
        candidate = Path(sql_file)
        if not candidate.is_absolute():
            candidate = REPO_ROOT / candidate
        resolved = candidate.resolve()
        if resolved == MARTS_DIR or MARTS_DIR in resolved.parents:
            raise ValueError(
                f"mart SQL file {sql_file!r} cannot use the per-file migration route; "
                "use mart_release with a dependency-complete release manifest"
            )

    if plan.refresh_views:
        for view in plan.refresh_views:
            if view.count(".") != 1:
                raise ValueError(f"invalid refresh_views entry {view!r}; expected 'schema.view'")


def load_manifest(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def plan_from_manifest(manifest: dict) -> Plan:
    """Build and validate a Plan from a parsed deploy-manifest.json dict."""
    raw_release_plan = manifest.get("plan", False)
    if not isinstance(raw_release_plan, bool):
        raise ValueError("plan must be a boolean")

    backfill = None
    bf = manifest.get("backfill")
    if bf:
        backfill = BackfillSpec(
            start=int(bf["start"]) if bf.get("start") is not None else None,
            end=int(bf["end"]) if bf.get("end") is not None else None,
            sources=str(bf.get("sources", "")),
        )

    compute = None
    cp = manifest.get("compute")
    if cp:
        compute = ComputeSpec(
            script=cp.get("script"),
            args=list(cp.get("args") or []),
        )

    plan = Plan(
        action=manifest.get("action"),
        mart_release=manifest.get("mart_release"),
        plan=raw_release_plan,
        marts_from=manifest.get("marts_from"),
        marts_only=manifest.get("marts_only"),
        files=list(manifest.get("files") or []),
        refresh=bool(manifest.get("refresh", False)),
        refresh_views=list(manifest.get("refresh_views") or []),
        backfill=backfill,
        strict=bool(manifest.get("strict", False)),
        compute=compute,
    )
    validate_plan(plan)
    if plan.action in MANAGED_ACTION_MODES:
        raise ValueError(
            f"{plan.action} requires workflow_dispatch; managed production actions "
            "cannot run from deploy-manifest.json"
        )
    if plan.compute and plan.compute.script == "recover_season_projections":
        raise ValueError(
            "recover_season_projections requires workflow_dispatch with compute_script; "
            "manifest execution does not share the daily ingestion concurrency group"
        )
    return plan


def plan_from_cli(
    *,
    action: str,
    mart_release: str | None = None,
    plan: bool = False,
    marts_from: str | None = None,
    marts_only: str | None = None,
    files: str | None = None,
    refresh: bool = False,
    refresh_views: str | None = None,
    backfill_start: int | None = None,
    backfill_end: int | None = None,
    sources: str | None = None,
    strict: bool = False,
    compute_script: str | None = None,
    compute_args: str | None = None,
) -> Plan:
    """Build and validate a Plan from workflow_dispatch-style CLI flags.

    `files`, `sources`, `refresh_views`, and `compute_args` are comma-separated
    strings (workflow_dispatch inputs are plain text), matching the manifest's
    list/string fields once parsed.
    """
    file_list = [f.strip() for f in files.split(",") if f.strip()] if files else []
    refresh_views_list = (
        [v.strip() for v in refresh_views.split(",") if v.strip()] if refresh_views else []
    )

    backfill = None
    if backfill_start is not None or backfill_end is not None or sources:
        backfill = BackfillSpec(
            start=int(backfill_start) if backfill_start is not None else None,
            end=int(backfill_end) if backfill_end is not None else None,
            sources=sources or "",
        )

    if compute_args and compute_script is None:
        raise ValueError("--compute-args requires --compute-script")

    compute = None
    if compute_script is not None:
        arg_list = [a.strip() for a in compute_args.split(",") if a.strip()] if compute_args else []
        compute = ComputeSpec(script=compute_script, args=arg_list)

    plan = Plan(
        action=action,
        mart_release=mart_release,
        plan=plan,
        marts_from=marts_from,
        marts_only=marts_only,
        files=file_list,
        refresh=refresh,
        refresh_views=refresh_views_list,
        backfill=backfill,
        strict=strict,
        compute=compute,
    )
    validate_plan(plan)
    return plan


# --------------------------------------------------------------------------
# Execution (subprocess dispatch to the existing driver scripts)
# --------------------------------------------------------------------------


def run_cmd(cmd: list[str], label: str, *, env: dict[str, str] | None = None) -> int:
    """Run a subprocess, inheriting stdout/stderr so CI logs show it live."""
    logger.info(f"--- {label}: {' '.join(cmd)} ---")
    proc = subprocess.run(cmd, env=env)
    logger.info(f"--- {label} exit={proc.returncode} ---")
    return proc.returncode


def run_presence_check(plan: Plan) -> int:
    cmd = [sys.executable, str(CHECK_PRESENCE)]
    if plan.strict:
        cmd.append("--strict")
    return run_cmd(cmd, "check_presence")


def run_apply(plan: Plan) -> int:
    validate_plan(plan)
    if plan.mart_release:
        cmd = [sys.executable, str(RUN_MARTS), "--release", plan.mart_release]
        if plan.plan:
            cmd.append("--plan")
        return run_cmd(cmd, f"run_marts --release {plan.mart_release}")

    for sql_file in plan.files:
        rc = run_cmd(
            [sys.executable, str(RUN_MIGRATIONS), "--file", sql_file],
            f"run_migrations --file {sql_file}",
        )
        if rc:
            return rc

    if plan.refresh:
        rc = run_cmd(
            [sys.executable, str(REFRESH_MARTS), "--schema", "marts"],
            "refresh_marts --schema marts",
        )
        if rc:
            return rc

    logger.info("apply plan completed successfully")
    return 0


def run_backfill(plan: Plan) -> int:
    b = plan.backfill
    # validate_plan guarantees start/end are set for a backfill action.
    assert b is not None and b.start is not None and b.end is not None
    logger.info(f"backfill seasons {b.start}..{b.end} sources={b.sources!r}")

    for season in range(b.start, b.end + 1):
        start_t = time.monotonic()
        rc = run_cmd(
            [
                sys.executable,
                str(LOAD_SEASON),
                "--season",
                str(season),
                "--sources",
                b.sources,
                "--skip-refresh",
            ],
            f"load_season {season}",
        )
        elapsed = time.monotonic() - start_t
        logger.info(f"season {season} finished in {elapsed:.1f}s (exit={rc})")
        if rc:
            logger.error(f"backfill stopped at season {season} (exit {rc})")
            return rc

    rc = run_cmd([sys.executable, str(REFRESH_MARTS)], "refresh_marts")
    if rc:
        return rc

    return run_cmd([sys.executable, str(CHECK_PRESENCE)], "check_presence")


def run_compute(plan: Plan) -> int:
    c = plan.compute
    # validate_plan guarantees a compute block with an allowlisted script.
    assert c is not None
    rc = run_cmd(
        [sys.executable, str(SCRIPTS_DIR / f"{c.script}.py"), *c.args],
        f"compute {c.script}",
    )
    if rc:
        return rc

    if plan.refresh:
        if plan.refresh_views:
            views, label = plan.refresh_views, "refresh_marts --views (custom)"
        else:
            views, label = TIER2_MART_VIEWS, "refresh_marts --views (tier2)"
        rc = run_cmd(
            [sys.executable, str(REFRESH_MARTS), "--views", ",".join(views)],
            label,
        )
        if rc:
            return rc

    logger.info("compute plan completed successfully")
    return 0


def run_managed_production(plan: Plan) -> int:
    """Dispatch one managed action against the fixed production manifest."""
    validate_plan(plan)
    mode = MANAGED_ACTION_MODES[plan.action]

    # bootstrap_warehouse.py intentionally accepts WAREHOUSE_DB_URL only. Keep
    # legacy/dlt connection aliases out of its subprocess even when a caller's
    # ambient environment contains them; the workflow supplies WAREHOUSE_DB_URL
    # only on managed steps.
    managed_env = os.environ.copy()
    for variable in (
        "SUPABASE_DB_URL",
        "DATABASE_URL",
        "DESTINATION__POSTGRES__CREDENTIALS",
    ):
        managed_env.pop(variable, None)

    return run_cmd(
        [
            sys.executable,
            str(BOOTSTRAP_WAREHOUSE),
            mode,
            "--manifest",
            PRODUCTION_MANIFEST,
        ],
        f"managed production {mode}",
        env=managed_env,
    )


def execute_plan(plan: Plan) -> int:
    logger.info(f"executing plan: {plan}")
    if plan.action == "presence_check":
        return run_presence_check(plan)
    if plan.action == "apply":
        return run_apply(plan)
    if plan.action == "backfill":
        return run_backfill(plan)
    if plan.action == "compute":
        return run_compute(plan)
    if plan.action in MANAGED_ACTION_MODES:
        return run_managed_production(plan)
    raise ValueError(f"unknown action: {plan.action}")  # unreachable after validate_plan


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Schema deploy driver")
    parser.add_argument("--manifest", help="Path to a JSON deploy manifest (see module docstring)")
    parser.add_argument("--action", choices=sorted(VALID_ACTIONS), help="Action to run")
    parser.add_argument(
        "--mart-release",
        dest="mart_release",
        help="Dependency-complete mart release manifest (apply only)",
    )
    parser.add_argument(
        "--plan",
        action="store_true",
        help="Preview --mart-release through a read-only transaction",
    )
    parser.add_argument(
        "--marts-from",
        dest="marts_from",
        help="unsupported legacy selector; use --mart-release",
    )
    parser.add_argument(
        "--marts-only",
        dest="marts_only",
        help="unsupported legacy selector; use --mart-release",
    )
    parser.add_argument("--files", help="Comma-separated SQL files to apply via run_migrations.py")
    parser.add_argument("--refresh", action="store_true", help="Refresh marts schema after apply")
    parser.add_argument(
        "--refresh-views",
        dest="refresh_views",
        help="Comma-separated marts.<view> names to refresh after compute (overrides tier2)",
    )
    parser.add_argument(
        "--backfill-start", dest="backfill_start", type=int, help="First backfill season"
    )
    parser.add_argument(
        "--backfill-end", dest="backfill_end", type=int, help="Last backfill season"
    )
    parser.add_argument("--sources", help="Comma-separated sources for load_season.py --sources")
    parser.add_argument(
        "--strict", action="store_true", help="Pass --strict through to check_presence.py"
    )
    parser.add_argument(
        "--compute-script",
        dest="compute_script",
        help="Compute script to run, e.g. compute_house_elo (see COMPUTE_SCRIPTS)",
    )
    parser.add_argument(
        "--compute-args",
        dest="compute_args",
        help="Comma-separated args passed through to the compute script",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.manifest:
        conflicting_flags = []
        for field_name in (
            "action",
            "mart_release",
            "marts_from",
            "marts_only",
            "files",
            "refresh_views",
            "backfill_start",
            "backfill_end",
            "sources",
            "compute_script",
            "compute_args",
        ):
            if getattr(args, field_name) is not None:
                conflicting_flags.append("--" + field_name.replace("_", "-"))
        for enabled, flag in (
            (args.plan, "--plan"),
            (args.refresh, "--refresh"),
            (args.strict, "--strict"),
        ):
            if enabled:
                conflicting_flags.append(flag)
        if conflicting_flags:
            parser.error(
                "--manifest cannot be combined with CLI plan flags: " + ", ".join(conflicting_flags)
            )
        manifest = load_manifest(args.manifest)
        plan = plan_from_manifest(manifest)
    else:
        if not args.action:
            parser.error("--action is required when --manifest is not given")
        plan = plan_from_cli(
            action=args.action,
            mart_release=args.mart_release,
            plan=args.plan,
            marts_from=args.marts_from,
            marts_only=args.marts_only,
            files=args.files,
            refresh=args.refresh,
            refresh_views=args.refresh_views,
            backfill_start=args.backfill_start,
            backfill_end=args.backfill_end,
            sources=args.sources,
            strict=args.strict,
            compute_script=args.compute_script,
            compute_args=args.compute_args,
        )

    sys.exit(execute_plan(plan))


if __name__ == "__main__":
    main()
