#!/usr/bin/env python3
"""Publish and verify the explicitly activated 2026 SDV FPI receipt."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2 import sql

from scripts import run_source_receipt_canary as canary
from scripts.verify_load import SOURCE_FRESHNESS_FIELDS, _source_row_contract_error
from src.pipelines.config.source_publication_assets import SDV_FPI_PUBLICATION
from src.pipelines.sources.flat_files import REGISTRY, season_for_date
from src.pipelines.utils import flat_file_publication as publication

SOURCE = "sdv_fpi_weekly"
SEASON = 2026
PUBLIC_ROLES = ("anon", "authenticated")
SHA256_RE = re.compile(r"[0-9a-f]{64}")


class ScheduledFpiError(RuntimeError):
    """A bounded scheduled-publication failure safe to report."""


def _season(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be the activated season 2026") from error
    if parsed != SEASON:
        raise argparse.ArgumentTypeError("must be the activated season 2026")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish the activated 2026 SDV FPI receipt and verify freshness"
    )
    parser.add_argument("--season", type=_season, required=True)
    parser.add_argument("--expected-project-ref", type=canary._project_ref, required=True)
    return parser


def _append_workflow_outputs(**values: str) -> None:
    target = os.environ.get("GITHUB_OUTPUT")
    if not target:
        return
    with Path(target).open("a", encoding="utf-8") as output:
        for name, value in values.items():
            output.write(f"{name}={value}\n")


def _database_failure(error: psycopg2.Error, operation: str) -> ScheduledFpiError:
    return ScheduledFpiError(
        f"{operation} failed (type={type(error).__name__}, code={error.pgcode or 'none'})"
    )


def _require_policy(
    dsn: str,
    season: int,
    *,
    connector=psycopg2.connect,
) -> None:
    """Require the exact reviewed cadence without modifying policy state."""
    try:
        conn = connector(dsn, connect_timeout=10, application_name="scheduled-fpi-policy")
        try:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '15s'")
                cur.execute("SET LOCAL lock_timeout = '5s'")
                cur.execute(
                    """
                    SELECT expected_refresh_interval = interval '36 hours'
                    FROM meta.asset_freshness_policies
                    WHERE asset_key = %s AND coverage_key = %s
                    """,
                    (SDV_FPI_PUBLICATION.asset_key, SDV_FPI_PUBLICATION.coverage_key(season)),
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
        raise ScheduledFpiError("FPI/2026 freshness policy must be exactly 36 hours")


@contextmanager
def _pinned_publication_connection(dsn: str) -> Iterator[None]:
    original = publication.get_db_url
    publication.get_db_url = lambda: dsn
    try:
        yield
    finally:
        publication.get_db_url = original


def _require_publication_result(result: dict[str, Any]) -> None:
    if result.get("source") != SOURCE or result.get("status") not in {
        "loaded",
        "not_published",
        "blocked",
        "failed",
    }:
        raise ScheduledFpiError("publisher returned an invalid publication status")
    try:
        run_id = str(result.get("run_id"))
        generation_id = str(result.get("generation_id"))
        if str(uuid.UUID(run_id)) != run_id or str(uuid.UUID(generation_id)) != generation_id:
            raise ValueError
    except (TypeError, ValueError, AttributeError) as error:
        raise ScheduledFpiError("publisher returned invalid receipt identifiers") from error
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
        raise ScheduledFpiError("publisher returned invalid publication evidence")
    if result.get("status") == "loaded" and (rows <= 0 or sha256 is None):
        raise ScheduledFpiError("publisher returned incomplete publication evidence")


def _verify_private_receipt(
    dsn: str,
    season: int,
    result: dict[str, Any],
    *,
    connector=psycopg2.connect,
) -> None:
    """Match the returned identifiers, row count, and SHA to committed evidence."""
    try:
        conn = connector(dsn, connect_timeout=10, application_name="scheduled-fpi-receipt")
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
                        SDV_FPI_PUBLICATION.asset_key,
                        SDV_FPI_PUBLICATION.coverage_key(season),
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
        raise ScheduledFpiError("committed publication receipt is missing or ambiguous")
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
        or coverage.get("season") != season
        or coverage.get("source_rows") != expected_rows
        or coverage.get("published_rows") != expected_rows
        or type(inputs) is not dict
        or inputs.get("artifact_origin") != "registered_url"
        or inputs.get("season_basis") != "artifact_field"
        or type(delta) is not dict
        or delta.get("published_rows") != expected_rows
    ):
        raise ScheduledFpiError("committed publication receipt does not match the result")


def _read_public_freshness(
    dsn: str,
    role: str,
    season: int,
    *,
    connector=psycopg2.connect,
) -> tuple[dict[str, Any], bool]:
    try:
        conn = connector(dsn, connect_timeout=10, application_name="scheduled-fpi-freshness")
        try:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '15s'")
                cur.execute("SET LOCAL lock_timeout = '5s'")
                cur.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
                cur.execute("SELECT current_user::text")
                identity = cur.fetchone()
                if identity != (role,):
                    raise ScheduledFpiError(
                        f"freshness verification did not assume the {role} role"
                    )
                cur.execute(
                    """
                    SELECT to_jsonb(f),
                           f.expected_refresh_interval = interval '36 hours'
                    FROM public.get_source_freshness(%s) AS f
                    WHERE f.source_name = %s
                    """,
                    (season, SOURCE),
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
    except ScheduledFpiError:
        raise
    except psycopg2.Error as error:
        raise _database_failure(error, f"{role} freshness verification") from None
    if len(rows) != 1 or len(rows[0]) != 2 or type(rows[0][0]) is not dict:
        raise ScheduledFpiError(f"{role} freshness response is missing or ambiguous")
    return rows[0]


def _verify_public_freshness(dsn: str, season: int, result: dict[str, Any]) -> None:
    for role in PUBLIC_ROLES:
        row, interval_matches = _read_public_freshness(dsn, role, season)
        if frozenset(row) != SOURCE_FRESHNESS_FIELDS:
            raise ScheduledFpiError(f"{role} freshness response has an invalid shape")
        contract_error = _source_row_contract_error(row, SOURCE)
        if contract_error is not None:
            raise ScheduledFpiError(f"{role} freshness response is invalid: {contract_error}")
        if (
            row["source_name"] != SOURCE
            or row["asset_key"] != SDV_FPI_PUBLICATION.asset_key
            or row["season"] != season
            or row["coverage_key"] != SDV_FPI_PUBLICATION.coverage_key(season)
            or str(row["generation_id"]) != result["generation_id"]
            or row["publication_state"] != "current"
            or row["current_outcome"] != "succeeded"
            or row["is_complete"] is not True
            or row["source_rows"] != result["rows"]
            or row["published_rows"] != result["rows"]
            or row["artifact_origin"] != "registered_url"
            or row["season_basis"] != "artifact_field"
            or row["latest_outcome"] != "succeeded"
            or interval_matches is not True
            or row["is_stale"] is not False
        ):
            raise ScheduledFpiError(f"{role} freshness response does not match publication")


def _payload(status: str, season: int, result: dict[str, Any] | None = None) -> dict[str, Any]:
    result = result or {}
    return {
        "status": status,
        "source": SOURCE,
        "season": season,
        "run_id": result.get("run_id"),
        "generation_id": result.get("generation_id"),
        "rows": result.get("rows", 0),
        "sha": result.get("sha"),
        "duration_s": result.get("duration_s", 0.0),
    }


def _postpublication_failure(
    season: int,
    result: dict[str, Any],
    error: Exception,
) -> tuple[dict[str, Any], int]:
    payload = _payload("verification_failed", season, result)
    payload.update(
        phase="post_publication_verification",
        publication_status="loaded",
    )
    if isinstance(error, ScheduledFpiError):
        payload["error"] = str(error)[:240]
    elif isinstance(error, psycopg2.Error):
        payload["error"] = str(_database_failure(error, "post-publication verification"))
    else:
        payload["error"] = f"unexpected verification failure ({type(error).__name__})"
    return payload, 1


def run_scheduled(
    args: argparse.Namespace,
    *,
    today: date | None = None,
) -> tuple[dict[str, Any], int]:
    _append_workflow_outputs(active_season=str(args.season), publication_attempted="false")
    current_season = season_for_date(today or date.today())
    if current_season != SEASON:
        payload = _payload("skipped", args.season)
        payload["reason"] = "activated season is no longer current"
        return payload, 0

    dsn = publication.get_db_url()
    identity, plan = canary._inspect_target(dsn, args.expected_project_ref, SOURCE, args.season)
    if (
        identity.get("can_set_role") is not True
        or identity.get("assumed_role") != canary.PUBLISHER_ROLE
        or plan is None
    ):
        raise ScheduledFpiError("bounded source publisher role is unavailable")
    _require_policy(dsn, args.season)

    _append_workflow_outputs(publication_attempted="true")
    with _pinned_publication_connection(dsn):
        result = publication.run_source_publication(REGISTRY[SOURCE], season=args.season)
    _require_publication_result(result)
    if result.get("status") != "loaded":
        payload = _payload(str(result.get("status") or "failed"), args.season, result)
        payload["error_category"] = "source_publication_failed"
        return payload, 1

    try:
        _verify_private_receipt(dsn, args.season, result)
        _verify_public_freshness(dsn, args.season, result)
    except Exception as error:
        return _postpublication_failure(args.season, result, error)
    return _payload("loaded", args.season, result), 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        payload, status = run_scheduled(args)
    except (ScheduledFpiError, canary.CanaryError) as error:
        payload = _payload("failed", args.season)
        payload["error"] = str(error)[:240]
        status = 1
    except publication.SourcePublicationError:
        payload = _payload("failed", args.season)
        payload["error"] = "source publication validation failed"
        status = 1
    except psycopg2.Error as error:
        payload = _payload("failed", args.season)
        payload["error"] = str(_database_failure(error, "database operation"))
        status = 1
    except Exception as error:
        payload = _payload("failed", args.season)
        payload["error"] = f"unexpected failure ({type(error).__name__})"
        status = 1
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
