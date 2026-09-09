"""Receipt-backed publication adapter for the SDV weekly ratings season file.

The dlt load is deliberately confined to ``warehouse_source_stage``.  The
bounded SQL publisher is the only code that changes the public target: its
transaction replaces the selected season and records the load ledger row,
publication receipt, and current-generation pointer together.
"""

from __future__ import annotations

import hashlib
import io
import logging
import math
import os
import tempfile
import time
import uuid
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any

import dlt
import httpx
import psycopg2
import pyarrow.parquet
from dlt.destinations import postgres
from psycopg2 import sql
from psycopg2.extras import Json

from src.pipelines.config.source_publication_assets import SDV_RATINGS_PUBLICATION
from src.pipelines.sources.flat_files import (
    REGISTRY,
    FlatFileSpec,
    ParseContext,
    build_flat_file_source,
    resolve_fetch_url,
    resolve_parser,
)
from src.pipelines.utils.file_fetcher import fetch_file
from src.pipelines.utils.load_ledger import get_db_url

logger = logging.getLogger(__name__)

STAGE_SCHEMA = "warehouse_source_stage"
MAX_SOURCE_ROWS = 100_000
ERROR_MESSAGE_LIMIT = 500
_ROLE = "warehouse_source_publisher"
_EXPECTED_PLAN_KEYS = frozenset(
    {
        "protocol",
        "asset_key",
        "coverage_key",
        "season",
        "expected_generation_id",
        "parser_contract",
    }
)
_INT_COLUMNS = frozenset(
    {"season", "through_week", "team_id", "games", "off_rank", "def_rank", "net_rank"}
)
_FLOAT_COLUMNS = frozenset(SDV_RATINGS_PUBLICATION.columns) - _INT_COLUMNS
_STAGE_SYSTEM_COLUMNS = ("_dlt_id", "_dlt_load_id")
_EXPECTED_STAGE_COLUMNS = frozenset((*SDV_RATINGS_PUBLICATION.columns, *_STAGE_SYSTEM_COLUMNS))


class SourcePublicationError(RuntimeError):
    """A known pre-commit publication failure."""


class UncertainCommitError(RuntimeError):
    """The client cannot determine whether a mutating RPC committed."""


class PostCommitError(RuntimeError):
    """A non-transactional error occurred after a confirmed RPC commit."""


@dataclass(frozen=True)
class _StageResult:
    table: str
    rows: list[dict[str, Any]]
    evidence: dict[str, Any]


@dataclass
class _MutationState:
    start_pending: bool = False
    started: bool = False
    publish_pending: bool = False
    published: bool = False
    failure_pending: bool = False
    failure_recorded: bool = False

    def set_pending(self, phase: str) -> None:
        setattr(self, f"{phase}_pending", True)

    def set_committed(self, phase: str) -> None:
        committed = {"start": "started", "publish": "published", "failure": "failure_recorded"}
        setattr(self, committed[phase], True)
        # Keep the conservative pending state until the confirmed state is set.
        # An asynchronous interrupt between these assignments can therefore
        # never make a successful commit look safe to relabel as failed.
        setattr(self, f"{phase}_pending", False)


def _base_result(run_id: str, generation_id: str) -> dict[str, Any]:
    return {
        "source": SDV_RATINGS_PUBLICATION.source_name,
        "status": "failed",
        "rows": 0,
        "sha": None,
        "duration_s": 0.0,
        "error": None,
        "unmapped": None,
        "gaps": None,
        "run_id": run_id,
        "generation_id": generation_id,
    }


def _validate_supported_spec(spec: FlatFileSpec, season: int) -> None:
    canonical = REGISTRY[SDV_RATINGS_PUBLICATION.source_name]
    if spec is not canonical:
        raise ValueError(
            "receipt publication requires the canonical sdv_ratings_weekly registry spec"
        )
    if (
        spec.schema != "ratings"
        or spec.table != "sdv_ratings_weekly"
        or spec.parser != "sportsdataverse.parse_ratings_weekly"
        or tuple(spec.primary_key) != SDV_RATINGS_PUBLICATION.primary_key
        or spec.write_disposition != "merge"
        or spec.kind != "dlt"
    ):
        raise ValueError("sdv_ratings_weekly registry contract does not match the publisher")
    SDV_RATINGS_PUBLICATION.coverage_key(season)


def _validated_plan(value: Any, season: int) -> dict[str, Any]:
    if not isinstance(value, dict) or frozenset(value) != _EXPECTED_PLAN_KEYS:
        raise SourcePublicationError("publisher returned an invalid SDV ratings plan")
    expected = {
        "protocol": SDV_RATINGS_PUBLICATION.protocol,
        "asset_key": SDV_RATINGS_PUBLICATION.asset_key,
        "coverage_key": SDV_RATINGS_PUBLICATION.coverage_key(season),
        "season": season,
        "parser_contract": SDV_RATINGS_PUBLICATION.parser_contract,
    }
    if any(value.get(key) != expected_value for key, expected_value in expected.items()):
        raise SourcePublicationError("publisher returned a mismatched SDV ratings plan")
    generation = value["expected_generation_id"]
    if generation is not None:
        try:
            if str(uuid.UUID(str(generation))) != str(generation):
                raise ValueError
        except (TypeError, ValueError, AttributeError) as error:
            raise SourcePublicationError(
                "publisher plan has an invalid expected generation"
            ) from error
    return value


def _connect(dsn: str):
    return psycopg2.connect(dsn, connect_timeout=10)


def _rpc(
    dsn: str,
    statement: str,
    args: tuple[Any, ...],
    *,
    mutating: bool,
    state: _MutationState | None = None,
    phase: str | None = None,
) -> tuple[Any, ...] | None:
    if mutating != (state is not None and phase in {"start", "publish", "failure"}):
        raise ValueError("mutating RPCs require an explicit mutation phase and state")
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(_ROLE)))
            cur.execute("SET LOCAL statement_timeout = '60s'")
            cur.execute(statement, args)
            row = cur.fetchone() if cur.description else None
    except BaseException:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        raise

    if mutating:
        assert state is not None and phase is not None
        state.set_pending(phase)
    try:
        conn.commit()
    except BaseException as error:
        try:
            conn.close()
        except Exception:
            pass
        if mutating:
            raise UncertainCommitError(
                "source publication RPC commit outcome is uncertain"
            ) from error
        raise
    if mutating:
        state.set_committed(phase)
    try:
        conn.close()
    except BaseException as error:
        if mutating:
            raise PostCommitError("source publication RPC committed before close failed") from error
        raise
    return row


def _get_plan(dsn: str, season: int) -> dict[str, Any]:
    row = _rpc(
        dsn,
        "SELECT warehouse_source.get_sdv_ratings_plan(%s)",
        (season,),
        mutating=False,
    )
    if row is None:
        raise SourcePublicationError("publisher returned no SDV ratings plan")
    return _validated_plan(row[0], season)


def _start_run(dsn: str, run_id: str, plan: dict[str, Any], state: _MutationState) -> None:
    row = _rpc(
        dsn,
        "SELECT warehouse_source.start_sdv_ratings_load(%s,%s)",
        (run_id, Json(plan)),
        mutating=True,
        state=state,
        phase="start",
    )
    if row is None or str(row[0]) != run_id:
        # The RPC commit is confirmed, but its effect cannot safely be labeled.
        raise UncertainCommitError("publisher committed an unrecognized operation result")


def _publish(
    dsn: str,
    run_id: str,
    generation_id: str,
    sha256: str,
    stage: _StageResult,
    state: _MutationState,
) -> tuple[str, bool, int]:
    row = _rpc(
        dsn,
        "SELECT * FROM warehouse_source.publish_sdv_ratings_load(%s,%s,%s,%s,%s)",
        (run_id, generation_id, sha256, Json(stage.rows), Json(stage.evidence)),
        mutating=True,
        state=state,
        phase="publish",
    )
    if row is None or len(row) != 3:
        raise UncertainCommitError("publisher committed an unrecognized publication result")
    returned_generation, replayed, published_rows = row
    if (
        str(returned_generation) != generation_id
        or type(replayed) is not bool
        or type(published_rows) is not int
        or published_rows != len(stage.rows)
    ):
        raise UncertainCommitError("publisher committed a mismatched publication result")
    return str(returned_generation), replayed, published_rows


def _fail_run(
    dsn: str,
    run_id: str,
    generation_id: str,
    outcome: str,
    state: _MutationState,
) -> None:
    row = _rpc(
        dsn,
        "SELECT warehouse_source.fail_sdv_ratings_load(%s,%s,%s)",
        (run_id, generation_id, outcome),
        mutating=True,
        state=state,
        phase="failure",
    )
    if row is None or str(row[0]) != generation_id:
        raise SourcePublicationError("publisher returned a mismatched failure generation id")


def _raw_row_count(raw: bytes) -> int:
    try:
        return pyarrow.parquet.ParquetFile(io.BytesIO(raw)).metadata.num_rows
    except Exception as error:
        raise SourcePublicationError("sdv_ratings_weekly is not a readable parquet file") from error


def _validate_parsed_rows(rows: list[dict[str, Any]], raw_count: int, season: int) -> None:
    if raw_count == 0 or not rows:
        raise SourcePublicationError("sdv_ratings_weekly season file must be nonempty")
    if raw_count > MAX_SOURCE_ROWS or len(rows) > MAX_SOURCE_ROWS:
        raise SourcePublicationError(f"sdv_ratings_weekly exceeds {MAX_SOURCE_ROWS} rows")
    if len(rows) != raw_count:
        raise SourcePublicationError("sdv_ratings_weekly parser dropped or added source rows")

    expected_columns = frozenset(SDV_RATINGS_PUBLICATION.columns)
    keys: set[tuple[Any, ...]] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or frozenset(row) != expected_columns:
            raise SourcePublicationError(
                f"sdv_ratings_weekly row {index} does not match the 16-column parser contract"
            )
        if row["season"] != season:
            raise SourcePublicationError(
                "sdv_ratings_weekly row season does not match requested season"
            )
        key = tuple(row[column] for column in SDV_RATINGS_PUBLICATION.primary_key)
        if any(value is None for value in key):
            raise SourcePublicationError("sdv_ratings_weekly contains a null primary-key value")
        if key in keys:
            raise SourcePublicationError("sdv_ratings_weekly contains a duplicate primary key")
        keys.add(key)
        for column in _INT_COLUMNS:
            value = row[column]
            if value is not None and (type(value) is not int):
                raise SourcePublicationError(
                    f"sdv_ratings_weekly column {column} is not an integer"
                )
        for column in _FLOAT_COLUMNS:
            value = row[column]
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
            ):
                raise SourcePublicationError(f"sdv_ratings_weekly column {column} is not finite")


def _column_hints() -> list[dict[str, Any]]:
    return [
        {
            "name": name,
            "data_type": "bigint" if name in _INT_COLUMNS else "double",
            "nullable": name not in SDV_RATINGS_PUBLICATION.primary_key,
        }
        for name in SDV_RATINGS_PUBLICATION.columns
    ]


def _require_stage_schema(dsn: str) -> None:
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    stage.nspowner = routines.nspowner AS owners_match,
                    pg_catalog.has_schema_privilege(
                        current_user, stage.oid, 'USAGE,CREATE'
                    ) AS actor_can_stage,
                    NOT EXISTS (
                        SELECT 1 FROM pg_catalog.aclexplode(stage.nspacl) acl
                        WHERE acl.grantee NOT IN (
                            stage.nspowner,
                            (SELECT oid FROM pg_catalog.pg_roles
                             WHERE rolname = current_user)
                        )
                    ) AS grants_are_bounded,
                    pg_catalog.to_regnamespace('warehouse_source_stage_staging') IS NULL
                        AS no_secondary_stage
                FROM pg_catalog.pg_namespace stage
                JOIN pg_catalog.pg_namespace routines ON routines.nspname = 'warehouse_source'
                WHERE stage.nspname = %s
                """,
                (STAGE_SCHEMA,),
            )
            row = cur.fetchone()
        conn.rollback()
    finally:
        conn.close()
    if row is None or row != (True, True, True, True):
        raise SourcePublicationError("managed warehouse_source_stage boundary is unavailable")


def _read_stage(dsn: str, table: str, expected_rows: int, artifact_origin: str) -> _StageResult:
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname
                FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = pg_catalog.to_regclass(%s)
                  AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
                """,
                (f"{STAGE_SCHEMA}.{table}",),
            )
            stage_columns = {name for (name,) in cur.fetchall()}
            if stage_columns != _EXPECTED_STAGE_COLUMNS:
                raise SourcePublicationError(
                    "normalized staging columns violate the parser contract"
                )

            selected = [
                sql.Identifier(name)
                for name in (*SDV_RATINGS_PUBLICATION.columns, *_STAGE_SYSTEM_COLUMNS)
            ]
            order = [sql.Identifier(name) for name in SDV_RATINGS_PUBLICATION.primary_key]
            cur.execute(
                sql.SQL("SELECT {} FROM {}.{} ORDER BY {}").format(
                    sql.SQL(",").join(selected),
                    sql.Identifier(STAGE_SCHEMA),
                    sql.Identifier(table),
                    sql.SQL(",").join(order),
                )
            )
            names = [description.name for description in cur.description]
            rows = [dict(zip(names, values, strict=True)) for values in cur.fetchall()]
            if len(rows) != expected_rows:
                raise SourcePublicationError(
                    "dlt staging row count does not match parsed source rows"
                )
            load_ids = sorted({row["_dlt_load_id"] for row in rows})
            if not load_ids or any(
                not isinstance(load_id, str) or not load_id for load_id in load_ids
            ):
                raise SourcePublicationError("dlt staging rows do not carry actual load ids")
            if any(not isinstance(row["_dlt_id"], str) or not row["_dlt_id"] for row in rows):
                raise SourcePublicationError("dlt staging rows do not carry actual row ids")
            cur.execute(
                sql.SQL(
                    "SELECT load_id FROM {}._dlt_loads "
                    "WHERE load_id = ANY(%s) AND status = 0 ORDER BY load_id"
                ).format(sql.Identifier(STAGE_SCHEMA)),
                (load_ids,),
            )
            recorded_load_ids = [load_id for (load_id,) in cur.fetchall()]
            if recorded_load_ids != load_ids:
                raise SourcePublicationError("dlt staging load evidence is incomplete")
        conn.rollback()
    finally:
        conn.close()

    evidence = {
        "stage_schema": STAGE_SCHEMA,
        "parser_contract": SDV_RATINGS_PUBLICATION.parser_contract,
        "dlt_load_ids": load_ids,
        "source_rows": len(rows),
        "artifact_origin": artifact_origin,
    }
    return _StageResult(table=table, rows=rows, evidence=evidence)


def _stage_table_name(run_id: str) -> str:
    return f"sdv_ratings_{uuid.UUID(run_id).hex}"


def _stage_rows(
    dsn: str,
    spec: FlatFileSpec,
    raw: bytes,
    ctx: ParseContext,
    run_id: str,
    expected_rows: int,
    artifact_origin: str,
) -> _StageResult:
    _require_stage_schema(dsn)
    run_hex = uuid.UUID(run_id).hex
    table = _stage_table_name(run_id)
    # The table is unique to this run, so append is sufficient. dlt merge would
    # create a second ``warehouse_source_stage_staging`` schema outside the
    # migration-owned private boundary.
    stage_spec = replace(spec, table=table, write_disposition="append")
    source = build_flat_file_source(stage_spec, raw, ctx, None)
    resource = source.resources[table]
    resource.apply_hints(
        columns=_column_hints(),
        schema_contract={"tables": "evolve", "columns": "freeze", "data_type": "freeze"},
    )

    pipeline_name = f"sdv_ratings_publication_{run_hex}"
    with tempfile.TemporaryDirectory(prefix="sdv-ratings-dlt-") as pipelines_dir:
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            pipelines_dir=pipelines_dir,
            destination=postgres(credentials=dsn),
            dataset_name=STAGE_SCHEMA,
        )
        load_info = pipeline.run(source)
        load_info.raise_on_failed_jobs()
        normalized_count = pipeline.last_trace.last_normalize_info.row_counts.get(table, 0)
    if normalized_count != expected_rows:
        raise SourcePublicationError("dlt normalized row count does not match parsed source rows")
    return _read_stage(dsn, table, expected_rows, artifact_origin)


def _drop_stage_table(dsn: str, table: str) -> None:
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    sql.Identifier(STAGE_SCHEMA), sql.Identifier(table)
                )
            )
        conn.commit()
    except BaseException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def _known_error_text(error: BaseException, run_id: str, generation_id: str) -> str:
    if isinstance(error, SourcePublicationError):
        detail = str(error)
    elif isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 404:
        detail = "sdv_ratings_weekly season file is not published"
    else:
        detail = "sdv_ratings_weekly source publication failed"
    return f"{detail}; run_id={run_id}; generation_id={generation_id}"[:ERROR_MESSAGE_LIMIT]


def run_sdv_ratings_publication(
    spec: FlatFileSpec,
    *,
    file_path: str | None = None,
    season: int,
) -> dict[str, Any]:
    """Publish one complete SDV ratings season through the bounded SQL RPCs."""

    started_at = time.monotonic()
    run_id = str(uuid.uuid4())
    generation_id = str(uuid.uuid4())
    result = _base_result(run_id, generation_id)
    stage_table: str | None = None
    state = _MutationState()
    terminal_confirmed = False

    try:
        _validate_supported_spec(spec, season)
        dsn = get_db_url()
        plan = _get_plan(dsn, season)
        _start_run(dsn, run_id, plan, state)

        if file_path is not None:
            local_path = Path(file_path)
            if "://" in file_path or not local_path.is_file():
                raise SourcePublicationError("receipt file override must be an existing local file")
            target = file_path
            artifact_origin = "local_file"
        else:
            target = resolve_fetch_url(spec, season)
            artifact_origin = "registered_url"
        if not target:
            raise SourcePublicationError("sdv_ratings_weekly has no fetch target")
        fetched = fetch_file(target)
        if fetched.sha256 != hashlib.sha256(fetched.content).hexdigest():
            raise SourcePublicationError("fetched SDV ratings SHA-256 does not match its bytes")
        result["sha"] = fetched.sha256
        ctx = ParseContext(
            source=spec.name,
            snapshot_date=date.today(),
            season=season,
            source_url=fetched.source_url,
            file_name=os.path.basename(fetched.source_url),
        )
        raw_count = _raw_row_count(fetched.content)
        if raw_count > MAX_SOURCE_ROWS:
            raise SourcePublicationError(f"sdv_ratings_weekly exceeds {MAX_SOURCE_ROWS} rows")
        parser = resolve_parser(spec.parser)
        parsed_rows = list(parser(fetched.content, ctx))
        _validate_parsed_rows(parsed_rows, raw_count, season)

        # Set before dlt starts so a known stage failure or interrupt can clean
        # up a table created before pipeline.run returned to us.
        stage_table = _stage_table_name(run_id)
        stage = _stage_rows(
            dsn,
            spec,
            fetched.content,
            ctx,
            run_id,
            len(parsed_rows),
            artifact_origin,
        )
        if stage.table != stage_table:
            raise SourcePublicationError("dlt returned a mismatched staging table")
        _, _, published_rows = _publish(dsn, run_id, generation_id, fetched.sha256, stage, state)
        terminal_confirmed = True
        result.update(status="loaded", rows=published_rows)
        return result
    except UncertainCommitError as error:
        # Never retry or write a contradictory failure after an uncertain commit.
        terminal_confirmed = state.published
        logger.error(
            "SDV ratings commit/result is uncertain; run_id=%s generation_id=%s",
            run_id,
            generation_id,
        )
        if isinstance(error.__cause__, (KeyboardInterrupt, SystemExit)):
            raise error.__cause__
        result["error"] = (f"{error}; run_id={run_id}; generation_id={generation_id}")[
            :ERROR_MESSAGE_LIMIT
        ]
        logger.error(result["error"])
        return result
    except BaseException as error:
        interrupt = (
            error
            if isinstance(error, (KeyboardInterrupt, SystemExit))
            else error.__cause__
            if isinstance(error.__cause__, (KeyboardInterrupt, SystemExit))
            else None
        )
        deferred = isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 404
        blocked = isinstance(error, psycopg2.Error) and error.pgcode in {"40001", "55000"}
        outcome = "deferred" if deferred else "blocked" if blocked else "failed"
        result["status"] = "not_published" if deferred else outcome
        result["error"] = _known_error_text(error, run_id, generation_id)
        if state.published:
            # The publish commit is authoritative even if connection close or
            # local response handling was interrupted afterward.
            terminal_confirmed = True
        elif state.started and not state.publish_pending:
            try:
                _fail_run(dsn, run_id, generation_id, outcome, state)
                terminal_confirmed = True
            except UncertainCommitError as failure_error:
                logger.error(
                    "SDV ratings failure commit outcome is uncertain; run_id=%s generation_id=%s",
                    run_id,
                    generation_id,
                )
                if isinstance(failure_error.__cause__, (KeyboardInterrupt, SystemExit)):
                    raise failure_error.__cause__
            except PostCommitError as failure_error:
                terminal_confirmed = state.failure_recorded
                logger.error(
                    "SDV ratings failure committed before connection close failed; "
                    "run_id=%s generation_id=%s",
                    run_id,
                    generation_id,
                )
                if isinstance(failure_error.__cause__, (KeyboardInterrupt, SystemExit)):
                    raise failure_error.__cause__
            except Exception:
                terminal_confirmed = state.failure_recorded
                logger.error(
                    "SDV ratings failure receipt was not confirmed; run_id=%s generation_id=%s",
                    run_id,
                    generation_id,
                )
        if interrupt is not None:
            raise interrupt
        return result
    finally:
        if stage_table is not None and (
            terminal_confirmed or state.published or state.failure_recorded
        ):
            try:
                _drop_stage_table(dsn, stage_table)
            except Exception:
                logger.warning(
                    "Could not drop terminal SDV ratings stage table %s; run_id=%s",
                    stage_table,
                    run_id,
                )
        result["duration_s"] = time.monotonic() - started_at


__all__ = ["run_sdv_ratings_publication"]
