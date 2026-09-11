#!/usr/bin/env python3
"""Publish and verify the explicitly activated remaining SDV receipts."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2 import sql

from scripts import run_source_receipt_canary as canary
from scripts.verify_load import SOURCE_FRESHNESS_FIELDS, _source_row_contract_error
from src.pipelines.config.source_publication_assets import (
    SOURCE_PUBLICATION_ASSETS,
    SourcePublicationAsset,
)
from src.pipelines.sources.flat_files import REGISTRY, season_for_date
from src.pipelines.utils import flat_file_publication as publication
from src.pipelines.utils import sdv_ratings_publication

OPERATING_SEASON = 2026
PUBLIC_ROLES = ("anon", "authenticated")
SHA256_RE = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class Activation:
    source: str
    variable: str
    season: int

    @property
    def asset(self) -> SourcePublicationAsset:
        return SOURCE_PUBLICATION_ASSETS[self.source]


ACTIVATIONS = (
    Activation("sdv_ratings_weekly", "SDV_RATINGS_RECEIPT_SEASON", 2026),
    Activation("sdv_team_xwalk", "SDV_TEAM_XWALK_RECEIPT_SEASON", 2025),
    Activation("sdv_game_xwalk", "SDV_GAME_XWALK_RECEIPT_SEASON", 2025),
)


class ScheduledSdvError(RuntimeError):
    """A bounded scheduled-publication failure safe to report."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish activated SDV ratings and crosswalk receipts"
    )
    parser.add_argument(
        "--expected-project-ref",
        type=canary._project_ref,
        required=True,
        action=canary._StoreOnce,
    )
    return parser


def _append_workflow_outputs(**values: str) -> None:
    target = os.environ.get("GITHUB_OUTPUT")
    if not target:
        return
    with Path(target).open("a", encoding="utf-8") as output:
        for name, value in values.items():
            output.write(f"{name}={value}\n")


def _selected_activations(environ: Mapping[str, str]) -> list[Activation]:
    selected = []
    for activation in ACTIVATIONS:
        value = environ.get(activation.variable)
        if value in {None, ""}:
            continue
        if value != str(activation.season):
            raise ScheduledSdvError(
                f"{activation.variable} must be exactly {activation.season} when set"
            )
        selected.append(activation)
    return selected


def _database_failure(error: psycopg2.Error, operation: str) -> ScheduledSdvError:
    return ScheduledSdvError(
        f"{operation} failed (type={type(error).__name__}, code={error.pgcode or 'none'})"
    )


def _require_policy(
    dsn: str,
    activation: Activation,
    *,
    connector=psycopg2.connect,
) -> None:
    """Require one exact eight-day policy without modifying policy state."""
    try:
        conn = connector(dsn, connect_timeout=10, application_name="scheduled-sdv-policy")
        try:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '15s'")
                cur.execute("SET LOCAL lock_timeout = '5s'")
                cur.execute(
                    """
                    SELECT expected_refresh_interval = interval '8 days'
                    FROM meta.asset_freshness_policies
                    WHERE asset_key = %s AND coverage_key = %s
                    """,
                    (
                        activation.asset.asset_key,
                        activation.asset.coverage_key(activation.season),
                    ),
                )
                rows = cur.fetchall()
            conn.rollback()
        except BaseException:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()
    except psycopg2.Error as error:
        raise _database_failure(error, "freshness policy inspection") from None
    if rows != [(True,)]:
        raise ScheduledSdvError(
            f"{activation.source}/{activation.season} freshness policy must be exactly 8 days"
        )


def _require_publication_result(source: str, result: dict[str, Any]) -> None:
    if result.get("source") != source or result.get("status") not in {
        "loaded",
        "not_published",
        "blocked",
        "failed",
    }:
        raise ScheduledSdvError("publisher returned an invalid publication status")
    try:
        run_id = str(result.get("run_id"))
        generation_id = str(result.get("generation_id"))
        if str(uuid.UUID(run_id)) != run_id or str(uuid.UUID(generation_id)) != generation_id:
            raise ValueError
    except (TypeError, ValueError, AttributeError) as error:
        raise ScheduledSdvError("publisher returned invalid receipt identifiers") from error
    rows = result.get("rows")
    duration = result.get("duration_s")
    sha256 = result.get("sha")
    if (
        type(rows) is not int
        or rows < 0
        or isinstance(duration, bool)
        or not isinstance(duration, (int, float))
        or not math.isfinite(duration)
        or duration < 0
        or (sha256 is not None and SHA256_RE.fullmatch(str(sha256)) is None)
    ):
        raise ScheduledSdvError("publisher returned invalid publication evidence")
    if result.get("status") == "loaded" and (rows <= 0 or sha256 is None):
        raise ScheduledSdvError("publisher returned incomplete publication evidence")


def _verify_private_receipt(
    dsn: str,
    activation: Activation,
    result: dict[str, Any],
    *,
    connector=psycopg2.connect,
) -> None:
    """Match the returned operation and generation to committed receipt evidence."""
    try:
        conn = connector(dsn, connect_timeout=10, application_name="scheduled-sdv-receipt")
        try:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '15s'")
                cur.execute("SET LOCAL lock_timeout = '5s'")
                cur.execute(
                    """
                    SELECT receipt.operation_run_id::text,
                           receipt.generation_id::text,
                           receipt.source_watermark,
                           receipt.outcome,
                           receipt.coverage,
                           receipt.input_observations,
                           receipt.row_delta,
                           current_generation.generation_id::text
                    FROM meta.asset_receipts AS receipt
                    JOIN meta.asset_current_generations AS current_generation
                      ON current_generation.asset_key = receipt.asset_key
                     AND current_generation.coverage_key = receipt.coverage_key
                    WHERE receipt.asset_key = %s
                      AND receipt.coverage_key = %s
                      AND receipt.generation_id = %s::uuid
                    """,
                    (
                        activation.asset.asset_key,
                        activation.asset.coverage_key(activation.season),
                        result["generation_id"],
                    ),
                )
                rows = cur.fetchall()
            conn.rollback()
        except BaseException:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()
    except psycopg2.Error as error:
        raise _database_failure(error, "publication receipt verification") from None

    if len(rows) != 1 or len(rows[0]) != 8:
        raise ScheduledSdvError("committed publication receipt is missing or ambiguous")
    run_id, generation_id, sha256, outcome, coverage, inputs, delta, current = rows[0]
    expected_rows = result["rows"]
    if (
        run_id != result["run_id"]
        or generation_id != result["generation_id"]
        or current != result["generation_id"]
        or sha256 != result["sha"]
        or outcome != "succeeded"
        or type(coverage) is not dict
        or coverage.get("complete") is not True
        or coverage.get("scope") != "season"
        or coverage.get("season") != activation.season
        or coverage.get("mode") != "full_file_for_season"
        or coverage.get("source_rows") != expected_rows
        or coverage.get("published_rows") != expected_rows
        or type(inputs) is not dict
        or inputs.get("publisher") != "load_flat_files"
        or inputs.get("protocol") != activation.asset.protocol
        or inputs.get("parser_contract") != activation.asset.parser_contract
        or inputs.get("stage_schema") != "warehouse_source_stage"
        or inputs.get("artifact_origin") != "registered_url"
        or type(delta) is not dict
        or delta.get("published_rows") != expected_rows
    ):
        raise ScheduledSdvError("committed publication receipt does not match the result")
    if activation.source != "sdv_ratings_weekly" and (
        inputs.get("source_name") != activation.source
        or inputs.get("season_basis") != "registered_artifact_name"
    ):
        raise ScheduledSdvError("committed publication provenance does not match the source")


def _read_public_freshness(
    dsn: str,
    role: str,
    activation: Activation,
    *,
    connector=psycopg2.connect,
) -> tuple[dict[str, Any], bool]:
    try:
        conn = connector(dsn, connect_timeout=10, application_name="scheduled-sdv-freshness")
        try:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '15s'")
                cur.execute("SET LOCAL lock_timeout = '5s'")
                cur.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
                cur.execute("SELECT current_user::text")
                identity = cur.fetchone()
                if identity != (role,):
                    raise ScheduledSdvError(
                        f"freshness verification did not assume the {role} role"
                    )
                cur.execute(
                    """
                    SELECT to_jsonb(f),
                           f.expected_refresh_interval = interval '8 days'
                    FROM public.get_source_freshness(%s) AS f
                    WHERE f.source_name = %s
                    """,
                    (activation.season, activation.source),
                )
                rows = cur.fetchall()
            conn.rollback()
        except BaseException:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()
    except ScheduledSdvError:
        raise
    except psycopg2.Error as error:
        raise _database_failure(error, f"{role} freshness verification") from None
    if len(rows) != 1 or len(rows[0]) != 2 or type(rows[0][0]) is not dict:
        raise ScheduledSdvError(f"{role} freshness response is missing or ambiguous")
    return rows[0]


def _verify_public_freshness(
    dsn: str,
    activation: Activation,
    result: dict[str, Any],
) -> None:
    expected_basis = (
        "artifact_field"
        if activation.source == "sdv_ratings_weekly"
        else "registered_artifact_name"
    )
    for role in PUBLIC_ROLES:
        row, interval_matches = _read_public_freshness(dsn, role, activation)
        if frozenset(row) != SOURCE_FRESHNESS_FIELDS:
            raise ScheduledSdvError(f"{role} freshness response has an invalid shape")
        contract_error = _source_row_contract_error(row, activation.source)
        if contract_error is not None:
            raise ScheduledSdvError(f"{role} freshness response is invalid: {contract_error}")
        if (
            row["source_name"] != activation.source
            or row["asset_key"] != activation.asset.asset_key
            or row["season"] != activation.season
            or row["coverage_key"] != activation.asset.coverage_key(activation.season)
            or str(row["generation_id"]) != result["generation_id"]
            or row["publication_state"] != "current"
            or row["current_outcome"] != "succeeded"
            or row["is_complete"] is not True
            or row["source_rows"] != result["rows"]
            or row["published_rows"] != result["rows"]
            or row["artifact_origin"] != "registered_url"
            or row["season_basis"] != expected_basis
            or row["latest_outcome"] != "succeeded"
            or interval_matches is not True
            or row["is_stale"] is not False
        ):
            raise ScheduledSdvError(f"{role} freshness response does not match publication")


def _source_payload(
    status: str,
    activation: Activation,
    result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result = result or {}
    payload = {
        "status": status,
        "source": activation.source,
        "season": activation.season,
        "asset_key": activation.asset.asset_key,
        "coverage_key": activation.asset.coverage_key(activation.season),
        "run_id": result.get("run_id"),
        "generation_id": result.get("generation_id"),
        "rows": result.get("rows", 0),
        "sha": result.get("sha"),
        "duration_s": result.get("duration_s", 0.0),
    }
    if result.get("status") == "loaded":
        payload.update(
            artifact_origin="registered_url",
            season_basis=(
                "artifact_field"
                if activation.source == "sdv_ratings_weekly"
                else "registered_artifact_name"
            ),
        )
    return payload


def _safe_failure(
    activation: Activation,
    error: BaseException,
    result: dict[str, Any] | None = None,
    *,
    phase: str,
) -> dict[str, Any]:
    payload = _source_payload("verification_failed" if result else "failed", activation, result)
    payload["phase"] = phase
    if isinstance(error, (ScheduledSdvError, canary.CanaryError)):
        payload["error"] = str(error)[:240]
    elif isinstance(error, psycopg2.Error):
        payload["error"] = str(_database_failure(error, phase))
    elif isinstance(error, publication.SourcePublicationError):
        payload["error"] = "source publication validation failed"
    else:
        payload["error"] = f"unexpected failure ({type(error).__name__})"
    return payload


def _publish(activation: Activation) -> dict[str, Any]:
    if activation.source == "sdv_ratings_weekly":
        _append_workflow_outputs(ratings_publication_attempted="true")
        return sdv_ratings_publication.run_sdv_ratings_publication(
            REGISTRY[activation.source], season=activation.season
        )
    return publication.run_source_publication(REGISTRY[activation.source], season=activation.season)


def _run_one(dsn: str, project_ref: str, activation: Activation) -> tuple[dict[str, Any], bool]:
    try:
        identity, plan = canary._inspect_target(
            dsn,
            project_ref,
            activation.source,
            activation.season,
        )
        if (
            identity.get("can_set_role") is not True
            or identity.get("assumed_role") != canary.PUBLISHER_ROLE
            or plan is None
        ):
            raise ScheduledSdvError("bounded source publisher role is unavailable")
        _require_policy(dsn, activation)
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        return _safe_failure(activation, error, phase="preflight"), False

    try:
        result = _publish(activation)
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        return _safe_failure(activation, error, phase="publication"), False

    try:
        _require_publication_result(activation.source, result)
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        retained_result = result if isinstance(result, dict) else None
        return (
            _safe_failure(
                activation,
                error,
                retained_result,
                phase="publication_result_validation",
            ),
            False,
        )

    if result["status"] != "loaded":
        payload = _source_payload(str(result["status"]), activation, result)
        payload["phase"] = "publication"
        payload["error_category"] = "source_publication_failed"
        return payload, False

    try:
        _verify_private_receipt(dsn, activation, result)
        _verify_public_freshness(dsn, activation, result)
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        return (
            _safe_failure(
                activation,
                error,
                result,
                phase="post_publication_verification",
            ),
            False,
        )
    return _source_payload("loaded", activation, result), True


def run_scheduled(
    args: argparse.Namespace,
    *,
    today: date | None = None,
    environ: Mapping[str, str] | None = None,
) -> tuple[dict[str, Any], int]:
    _append_workflow_outputs(ratings_publication_attempted="false")
    selected = _selected_activations(os.environ if environ is None else environ)
    current_season = season_for_date(today or date.today())
    if current_season != OPERATING_SEASON:
        return {
            "status": "skipped",
            "operating_season": current_season,
            "reason": "remaining SDV receipt rollout is limited to operating season 2026",
            "sources": [],
        }, 0
    if not selected:
        return {
            "status": "inactive",
            "operating_season": current_season,
            "sources": [],
        }, 0

    dsn = publication.get_db_url()
    outcomes = []
    all_succeeded = True
    with canary.pinned_publication_connection(dsn):
        for activation in selected:
            outcome, succeeded = _run_one(dsn, args.expected_project_ref, activation)
            outcomes.append(outcome)
            all_succeeded = all_succeeded and succeeded
    return {
        "status": "loaded" if all_succeeded else "failed",
        "operating_season": current_season,
        "sources": outcomes,
    }, 0 if all_succeeded else 1


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        payload, status = run_scheduled(args)
    except ScheduledSdvError as error:
        payload = {"status": "failed", "error": str(error)[:240], "sources": []}
        status = 1
    except psycopg2.Error as error:
        payload = {
            "status": "failed",
            "error": str(_database_failure(error, "database operation")),
            "sources": [],
        }
        status = 1
    except Exception as error:
        payload = {
            "status": "failed",
            "error": f"unexpected failure ({type(error).__name__})",
            "sources": [],
        }
        status = 1
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
