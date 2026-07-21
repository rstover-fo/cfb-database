#!/usr/bin/env python3
"""Driver for push-triggered schema deploys (.github/workflows/deploy-schema.yml).

Reads a plan from either a JSON manifest (pushed to a `deploy/**` branch as
`deploy-manifest.json`, the mechanism `docs/plans/2026-07-19-tier1-analytics-unlock-plan.md`
describes) or CLI flags (workflow_dispatch, for human-triggered runs), then
executes it by shelling out to the existing driver scripts: run_marts.py,
run_migrations.py, refresh_marts.py, load_season.py, and check_presence.py.

Manifest schema:
    {
      "action": "presence_check" | "apply" | "backfill" | "compute",
      "marts_from": "029",
      "marts_only": "011",
      "files": ["src/schemas/api/019_x.sql", ...],
      "refresh": true,
      "backfill": {"start": 2014, "end": 2025, "sources": "stats,betting"},
      "compute": {"script": "compute_house_elo", "args": ["--full"]}
    }

All fields besides "action" are optional and only consulted by the action
that uses them (e.g. an "apply" manifest never looks at "backfill").

Usage:
    python scripts/deploy_schema.py --manifest deploy-manifest.json
    python scripts/deploy_schema.py --action presence_check --strict
    python scripts/deploy_schema.py --action apply --marts-only 011 \\
        --files src/schemas/functions/get_player_game_log.sql --refresh
    python scripts/deploy_schema.py --action backfill \\
        --backfill-start 2014 --backfill-end 2025 --sources stats,betting

Plan-building (plan_from_manifest / plan_from_cli / validate_plan) is pure --
no subprocesses, no DB -- so it can be unit tested directly; execute_plan is
the only part that shells out.
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCRIPTS_DIR = Path(__file__).parent
RUN_MARTS = SCRIPTS_DIR / "run_marts.py"
RUN_MIGRATIONS = SCRIPTS_DIR / "run_migrations.py"
REFRESH_MARTS = SCRIPTS_DIR / "refresh_marts.py"
LOAD_SEASON = SCRIPTS_DIR / "load_season.py"
CHECK_PRESENCE = SCRIPTS_DIR / "check_presence.py"

VALID_ACTIONS = {"presence_check", "apply", "backfill", "compute"}

# Allowlist of compute scripts the "compute" action may run (scripts/<name>.py).
# These land in later Tier 2 phases -- membership is checked, not file existence,
# so this action can be wired up before the scripts themselves exist.
COMPUTE_SCRIPTS = {
    "compute_house_elo",
    "compute_adjusted_epa",
    "compute_predictions",
    "check_backtest",
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
    marts_from: str | None = None
    marts_only: str | None = None
    files: list[str] = field(default_factory=list)
    refresh: bool = False
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


def load_manifest(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def plan_from_manifest(manifest: dict) -> Plan:
    """Build and validate a Plan from a parsed deploy-manifest.json dict."""
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
        marts_from=manifest.get("marts_from"),
        marts_only=manifest.get("marts_only"),
        files=list(manifest.get("files") or []),
        refresh=bool(manifest.get("refresh", False)),
        backfill=backfill,
        strict=bool(manifest.get("strict", False)),
        compute=compute,
    )
    validate_plan(plan)
    return plan


def plan_from_cli(
    *,
    action: str,
    marts_from: str | None = None,
    marts_only: str | None = None,
    files: str | None = None,
    refresh: bool = False,
    backfill_start: int | None = None,
    backfill_end: int | None = None,
    sources: str | None = None,
    strict: bool = False,
    compute_script: str | None = None,
    compute_args: str | None = None,
) -> Plan:
    """Build and validate a Plan from workflow_dispatch-style CLI flags.

    `files`, `sources`, and `compute_args` are comma-separated strings
    (workflow_dispatch inputs are plain text), matching the manifest's
    list/string fields once parsed.
    """
    file_list = [f.strip() for f in files.split(",") if f.strip()] if files else []

    backfill = None
    if backfill_start is not None or backfill_end is not None or sources:
        backfill = BackfillSpec(
            start=int(backfill_start) if backfill_start is not None else None,
            end=int(backfill_end) if backfill_end is not None else None,
            sources=sources or "",
        )

    compute = None
    if compute_script is not None:
        arg_list = [a.strip() for a in compute_args.split(",") if a.strip()] if compute_args else []
        compute = ComputeSpec(script=compute_script, args=arg_list)

    plan = Plan(
        action=action,
        marts_from=marts_from,
        marts_only=marts_only,
        files=file_list,
        refresh=refresh,
        backfill=backfill,
        strict=strict,
        compute=compute,
    )
    validate_plan(plan)
    return plan


# --------------------------------------------------------------------------
# Execution (subprocess dispatch to the existing driver scripts)
# --------------------------------------------------------------------------


def run_cmd(cmd: list[str], label: str) -> int:
    """Run a subprocess, inheriting stdout/stderr so CI logs show it live."""
    logger.info(f"--- {label}: {' '.join(cmd)} ---")
    proc = subprocess.run(cmd)
    logger.info(f"--- {label} exit={proc.returncode} ---")
    return proc.returncode


def run_presence_check(plan: Plan) -> int:
    cmd = [sys.executable, str(CHECK_PRESENCE)]
    if plan.strict:
        cmd.append("--strict")
    return run_cmd(cmd, "check_presence")


def run_apply(plan: Plan) -> int:
    # --only takes precedence over --from, mirroring run_marts.py's own logic.
    if plan.marts_only:
        rc = run_cmd(
            [sys.executable, str(RUN_MARTS), "--only", plan.marts_only],
            f"run_marts --only {plan.marts_only}",
        )
        if rc:
            return rc
    elif plan.marts_from:
        rc = run_cmd(
            [sys.executable, str(RUN_MARTS), "--from", plan.marts_from],
            f"run_marts --from {plan.marts_from}",
        )
        if rc:
            return rc

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
        rc = run_cmd(
            [sys.executable, str(REFRESH_MARTS), "--views", ",".join(TIER2_MART_VIEWS)],
            "refresh_marts --views (tier2)",
        )
        if rc:
            return rc

    logger.info("compute plan completed successfully")
    return 0


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
    raise ValueError(f"unknown action: {plan.action}")  # unreachable after validate_plan


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Schema deploy driver")
    parser.add_argument("--manifest", help="Path to a JSON deploy manifest (see module docstring)")
    parser.add_argument("--action", choices=sorted(VALID_ACTIONS), help="Action to run")
    parser.add_argument("--marts-from", dest="marts_from", help="run_marts.py --from value")
    parser.add_argument("--marts-only", dest="marts_only", help="run_marts.py --only value")
    parser.add_argument("--files", help="Comma-separated SQL files to apply via run_migrations.py")
    parser.add_argument("--refresh", action="store_true", help="Refresh marts schema after apply")
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
        manifest = load_manifest(args.manifest)
        plan = plan_from_manifest(manifest)
    else:
        if not args.action:
            parser.error("--action is required when --manifest is not given")
        plan = plan_from_cli(
            action=args.action,
            marts_from=args.marts_from,
            marts_only=args.marts_only,
            files=args.files,
            refresh=args.refresh,
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
