"""Receipt-backed publication for enrolled SportsDataverse season files.

Each run normalizes one complete file into an isolated dlt staging table.  A
bounded owner-rights RPC then replaces exactly the selected season and commits
the load ledger, receipt, and generation pointer in the same transaction.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import math
import os
import tempfile
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import Any

import dlt
import httpx
import psycopg2
import pyarrow.parquet
from dlt.destinations import postgres
from psycopg2 import sql
from psycopg2.extras import Json

from src.pipelines.config.source_publication_assets import (
    SDV_FPI_PUBLICATION,
    SDV_GAME_XWALK_PUBLICATION,
    SDV_TEAM_XWALK_PUBLICATION,
    SOURCE_PUBLICATION_ASSETS,
    SourcePublicationAsset,
)
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
_SUPPORTED_SOURCES = frozenset(
    {
        SDV_FPI_PUBLICATION.source_name,
        SDV_TEAM_XWALK_PUBLICATION.source_name,
        SDV_GAME_XWALK_PUBLICATION.source_name,
    }
)
_EXPECTED_PLAN_KEYS = frozenset(
    {
        "protocol",
        "source_name",
        "asset_key",
        "coverage_key",
        "season",
        "expected_generation_id",
        "parser_contract",
    }
)
_STAGE_SYSTEM_COLUMNS = ("_dlt_id", "_dlt_load_id")

_FPI_INTS = frozenset({"season", "season_type", "week", "team_id", "run_date_time_key"})
_FPI_BOOLS = frozenset({"snapshot_out_of_sequence", "snapshot_is_contemporaneous"})
_FPI_TIMESTAMPS = frozenset({"last_updated"})
_FPI_DOUBLES = frozenset(SDV_FPI_PUBLICATION.columns) - _FPI_INTS - _FPI_BOOLS - _FPI_TIMESTAMPS
_TEAM_INTS = frozenset({"season", "espn_team_id"})
_TEAM_TEXT = frozenset(SDV_TEAM_XWALK_PUBLICATION.columns) - _TEAM_INTS
_GAME_INTS = frozenset({"season", "espn_game_id"})
_GAME_DATES = frozenset({"yahoo_date", "espn_date", "fox_date"})
_GAME_TEXT = frozenset(SDV_GAME_XWALK_PUBLICATION.columns) - _GAME_INTS - _GAME_DATES
_RAW_COLUMNS = {
    "sdv_fpi_weekly": frozenset(SDV_FPI_PUBLICATION.columns),
    "sdv_team_xwalk": frozenset(SDV_TEAM_XWALK_PUBLICATION.columns) - {"season", "xwalk_key"},
    "sdv_game_xwalk": frozenset(SDV_GAME_XWALK_PUBLICATION.columns) - {"season"},
}

_PARSER_REFS = {
    "sdv_fpi_weekly": "sportsdataverse.parse_fpi_weekly",
    "sdv_team_xwalk": "sportsdataverse.parse_team_xwalk",
    "sdv_game_xwalk": "sportsdataverse.parse_game_xwalk",
}
_TARGETS = {
    "sdv_fpi_weekly": ("ratings", "espn_fpi_weekly"),
    "sdv_team_xwalk": ("ref", "team_id_xwalk"),
    "sdv_game_xwalk": ("ref", "game_id_xwalk"),
}


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
        setattr(self, f"{phase}_pending", False)


@dataclass
class _RunContext:
    stage_table: str | None = None
    terminal_confirmed: bool = False
    source_sha: str | None = None


@dataclass(frozen=True)
class _LifecycleHooks:
    source_name: str
    new_id: Callable[[], Any]
    preflight: Callable[[FlatFileSpec, int], None]
    get_db_url: Callable[[], str]
    get_plan: Callable[[str, int], dict[str, Any]]
    start_run: Callable[[str, str, dict[str, Any], _MutationState], None]
    work: Callable[
        [
            str,
            FlatFileSpec,
            str | None,
            int,
            str,
            str,
            _MutationState,
            _RunContext,
        ],
        tuple[str, int],
    ]
    fail_run: Callable[[str, str, str, str, _MutationState], None]
    drop_stage: Callable[[str, str], None]
    known_error_text: Callable[[BaseException, str, str], str]


def _asset_for(spec: FlatFileSpec, season: int) -> SourcePublicationAsset:
    if spec.name not in _SUPPORTED_SOURCES:
        raise ValueError(f"{spec.name} is not enrolled in generic source publication")
    canonical = REGISTRY[spec.name]
    if spec is not canonical:
        raise ValueError("receipt publication requires the canonical registry spec")
    asset = SOURCE_PUBLICATION_ASSETS[spec.name]
    schema, table = _TARGETS[spec.name]
    if (
        spec.schema != schema
        or spec.table != table
        or spec.parser != _PARSER_REFS[spec.name]
        or tuple(spec.primary_key) != asset.primary_key
        or spec.write_disposition != "merge"
        or spec.kind != "dlt"
    ):
        raise ValueError(f"{spec.name} registry contract does not match the publisher")
    asset.coverage_key(season)
    return asset


def _base_result(source_name: str, run_id: str, generation_id: str) -> dict[str, Any]:
    return {
        "source": source_name,
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


def _validated_plan(value: Any, asset: SourcePublicationAsset, season: int) -> dict[str, Any]:
    if not isinstance(value, dict) or frozenset(value) != _EXPECTED_PLAN_KEYS:
        raise SourcePublicationError("publisher returned an invalid source plan")
    expected = {
        "protocol": asset.protocol,
        "source_name": asset.source_name,
        "asset_key": asset.asset_key,
        "coverage_key": asset.coverage_key(season),
        "season": season,
        "parser_contract": asset.parser_contract,
    }
    if any(value.get(key) != expected_value for key, expected_value in expected.items()):
        raise SourcePublicationError("publisher returned a mismatched source plan")
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


def _execute_rpc(
    connector: Callable[[str], Any],
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
    conn = connector(dsn)
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
            if isinstance(error, Exception):
                logger.warning(
                    "Source publication RPC committed but connection close failed; "
                    "phase=%s error_type=%s",
                    phase,
                    type(error).__name__,
                )
            else:
                raise PostCommitError(
                    "source publication RPC committed before close was interrupted"
                ) from error
        else:
            raise
    return row


def _rpc(
    dsn: str,
    statement: str,
    args: tuple[Any, ...],
    *,
    mutating: bool,
    state: _MutationState | None = None,
    phase: str | None = None,
) -> tuple[Any, ...] | None:
    return _execute_rpc(
        _connect,
        dsn,
        statement,
        args,
        mutating=mutating,
        state=state,
        phase=phase,
    )


def _get_plan(dsn: str, asset: SourcePublicationAsset, season: int) -> dict[str, Any]:
    row = _rpc(
        dsn,
        "SELECT warehouse_source_batch.get_source_plan(%s,%s)",
        (asset.source_name, season),
        mutating=False,
    )
    if row is None:
        raise SourcePublicationError("publisher returned no source plan")
    return _validated_plan(row[0], asset, season)


def _start_run(dsn: str, run_id: str, plan: dict[str, Any], state: _MutationState) -> None:
    row = _rpc(
        dsn,
        "SELECT warehouse_source_batch.start_source_load(%s,%s)",
        (run_id, Json(plan)),
        mutating=True,
        state=state,
        phase="start",
    )
    if row is None or str(row[0]) != run_id:
        raise UncertainCommitError("publisher committed an unrecognized operation result")


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def _json_dumps(value: Any) -> str:
    return json.dumps(value, default=_json_default, allow_nan=False)


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
        "SELECT * FROM warehouse_source_batch.publish_source_load(%s,%s,%s,%s,%s)",
        (
            run_id,
            generation_id,
            sha256,
            Json(stage.rows, dumps=_json_dumps),
            Json(stage.evidence),
        ),
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
        "SELECT warehouse_source_batch.fail_source_load(%s,%s,%s)",
        (run_id, generation_id, outcome),
        mutating=True,
        state=state,
        phase="failure",
    )
    if row is None or str(row[0]) != generation_id:
        raise SourcePublicationError("publisher returned a mismatched failure generation id")


def _is_lossless_bigint(value: Any) -> bool:
    return value is None or type(value) is int and -(1 << 63) <= value < (1 << 63)


def _is_lossless_double(value: Any) -> bool:
    if value is None:
        return True
    if type(value) not in (int, float):
        return False
    try:
        converted = float(value)
    except OverflowError:
        return False
    return math.isfinite(converted) and converted == value


def _raw_row_count(raw: bytes, asset: SourcePublicationAsset, season: int) -> int:
    try:
        parquet_file = pyarrow.parquet.ParquetFile(io.BytesIO(raw))
        names = parquet_file.schema_arrow.names
        if len(names) != len(set(names)) or frozenset(names) != _RAW_COLUMNS[asset.source_name]:
            raise SourcePublicationError(
                f"{asset.source_name} raw columns violate the parser contract"
            )
        row_count = parquet_file.metadata.num_rows
        if row_count > MAX_SOURCE_ROWS:
            raise SourcePublicationError(f"{asset.source_name} exceeds {MAX_SOURCE_ROWS} rows")
        integer_columns, double_columns, boolean_columns, _, _, _ = _type_sets(asset)
        raw_integer_columns = sorted(integer_columns.intersection(names))
        raw_double_columns = sorted(double_columns.intersection(names))
        raw_boolean_columns = sorted(boolean_columns.intersection(names))
        checked_columns = [*raw_integer_columns, *raw_double_columns, *raw_boolean_columns]
        for batch in parquet_file.iter_batches(batch_size=4_096, columns=checked_columns):
            values_by_name = batch.to_pydict()
            for column in raw_integer_columns:
                if any(not _is_lossless_bigint(value) for value in values_by_name[column]):
                    raise SourcePublicationError(
                        f"{asset.source_name} raw column {column} has a non-integer value"
                    )
            for column in raw_double_columns:
                if any(not _is_lossless_double(value) for value in values_by_name[column]):
                    raise SourcePublicationError(
                        f"{asset.source_name} raw column {column} is not a lossless finite number"
                    )
            for column in raw_boolean_columns:
                if any(
                    value is not None and type(value) is not bool
                    for value in values_by_name[column]
                ):
                    raise SourcePublicationError(
                        f"{asset.source_name} raw column {column} has a non-boolean value"
                    )
            if asset is SDV_FPI_PUBLICATION and any(
                value is not None and value != season for value in values_by_name["season"]
            ):
                raise SourcePublicationError(
                    "sdv_fpi_weekly raw season does not match requested season"
                )
    except Exception as error:
        if isinstance(error, SourcePublicationError):
            raise
        raise SourcePublicationError(
            f"{asset.source_name} is not a readable parquet file"
        ) from error
    return row_count


def _type_sets(asset: SourcePublicationAsset) -> tuple[set[str] | frozenset[str], ...]:
    if asset is SDV_FPI_PUBLICATION:
        return _FPI_INTS, _FPI_DOUBLES, _FPI_BOOLS, _FPI_TIMESTAMPS, frozenset(), frozenset()
    if asset is SDV_TEAM_XWALK_PUBLICATION:
        return _TEAM_INTS, frozenset(), frozenset(), frozenset(), frozenset(), _TEAM_TEXT
    return _GAME_INTS, frozenset(), frozenset(), frozenset(), _GAME_DATES, _GAME_TEXT


def _validate_scalar(asset: SourcePublicationAsset, column: str, value: Any) -> None:
    if value is None:
        return
    ints, doubles, bools, timestamps, dates, texts = _type_sets(asset)
    if column in ints and type(value) is not int:
        raise SourcePublicationError(f"{asset.source_name} column {column} is not an integer")
    if column in doubles and (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        raise SourcePublicationError(f"{asset.source_name} column {column} is not finite")
    if column in bools and type(value) is not bool:
        raise SourcePublicationError(f"{asset.source_name} column {column} is not boolean")
    if column in timestamps and not isinstance(value, datetime):
        raise SourcePublicationError(f"{asset.source_name} column {column} is not a timestamp")
    if column in timestamps and value.tzinfo is None:
        raise SourcePublicationError(f"{asset.source_name} column {column} is not timezone-aware")
    if column in dates and type(value) is not date:
        raise SourcePublicationError(f"{asset.source_name} column {column} is not a date")
    if column in texts and type(value) is not str:
        raise SourcePublicationError(f"{asset.source_name} column {column} is not text")


def _validate_parsed_rows(
    rows: list[dict[str, Any]], raw_count: int, asset: SourcePublicationAsset, season: int
) -> None:
    if raw_count == 0 or not rows:
        raise SourcePublicationError(f"{asset.source_name} season file must be nonempty")
    if raw_count > MAX_SOURCE_ROWS or len(rows) > MAX_SOURCE_ROWS:
        raise SourcePublicationError(f"{asset.source_name} exceeds {MAX_SOURCE_ROWS} rows")
    if len(rows) != raw_count:
        raise SourcePublicationError(f"{asset.source_name} parser dropped or added source rows")

    expected_columns = frozenset(asset.columns)
    keys: set[tuple[Any, ...]] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or frozenset(row) != expected_columns:
            raise SourcePublicationError(
                f"{asset.source_name} row {index} does not match the "
                f"{len(asset.columns)}-column parser contract"
            )
        if row["season"] != season:
            raise SourcePublicationError(
                f"{asset.source_name} row season does not match requested season"
            )
        key = tuple(row[column] for column in asset.primary_key)
        if any(value is None for value in key):
            raise SourcePublicationError(f"{asset.source_name} contains a null primary-key value")
        if key in keys:
            raise SourcePublicationError(f"{asset.source_name} contains a duplicate primary key")
        keys.add(key)
        for column, value in row.items():
            _validate_scalar(asset, column, value)

        if asset is SDV_FPI_PUBLICATION:
            if row["season_type"] not in {2, 3}:
                raise SourcePublicationError("sdv_fpi_weekly season_type must be 2 or 3")
            if row["week"] < 0:
                raise SourcePublicationError("sdv_fpi_weekly week must be nonnegative")
            if row["team_id"] <= 0:
                raise SourcePublicationError("sdv_fpi_weekly team_id must be positive")

        if asset is SDV_TEAM_XWALK_PUBLICATION:
            tiebreak = row["espn_team_id"]
            if tiebreak is None:
                tiebreak = row["fox_team_id"]
            if tiebreak is None:
                tiebreak = row["yahoo_team_id"]
            if row["xwalk_key"] != f"{row['norm_key']}#{tiebreak}":
                raise SourcePublicationError("sdv_team_xwalk contains an invalid derived xwalk_key")


def _column_hints(asset: SourcePublicationAsset) -> list[dict[str, Any]]:
    ints, doubles, bools, timestamps, dates, _ = _type_sets(asset)
    hints = []
    for name in asset.columns:
        data_type = (
            "bigint"
            if name in ints
            else "double"
            if name in doubles
            else "bool"
            if name in bools
            else "timestamp"
            if name in timestamps
            else "date"
            if name in dates
            else "text"
        )
        required = set(asset.primary_key)
        if asset is SDV_TEAM_XWALK_PUBLICATION:
            required.add("norm_key")
        hints.append({"name": name, "data_type": data_type, "nullable": name not in required})
    return hints


def _require_stage_schema(dsn: str) -> None:
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    stage.nspowner = routines.nspowner,
                    pg_catalog.has_schema_privilege(current_user, stage.oid, 'USAGE,CREATE'),
                    NOT EXISTS (
                        SELECT 1 FROM pg_catalog.aclexplode(stage.nspacl) acl
                        WHERE acl.grantee NOT IN (
                            stage.nspowner,
                            (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user)
                        )
                    ),
                    pg_catalog.to_regnamespace('warehouse_source_stage_staging') IS NULL
                FROM pg_catalog.pg_namespace stage
                JOIN pg_catalog.pg_namespace routines
                  ON routines.nspname = 'warehouse_source_batch'
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


def _read_stage(
    dsn: str,
    table: str,
    expected_rows: int,
    asset: SourcePublicationAsset,
    artifact_origin: str,
    season_basis: str,
) -> _StageResult:
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
            expected_columns = frozenset((*asset.columns, *_STAGE_SYSTEM_COLUMNS))
            if stage_columns != expected_columns:
                raise SourcePublicationError(
                    "normalized staging columns violate the parser contract"
                )
            selected = [sql.Identifier(name) for name in (*asset.columns, *_STAGE_SYSTEM_COLUMNS)]
            order = [sql.Identifier(name) for name in asset.primary_key]
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
            if not load_ids or any(not isinstance(value, str) or not value for value in load_ids):
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
            if [load_id for (load_id,) in cur.fetchall()] != load_ids:
                raise SourcePublicationError("dlt staging load evidence is incomplete")
        conn.rollback()
    finally:
        conn.close()
    return _StageResult(
        table=table,
        rows=rows,
        evidence={
            "stage_schema": STAGE_SCHEMA,
            "parser_contract": asset.parser_contract,
            "dlt_load_ids": load_ids,
            "source_rows": len(rows),
            "artifact_origin": artifact_origin,
            "season_basis": season_basis,
        },
    )


def _stage_table_name(source_name: str, run_id: str) -> str:
    short_name = source_name.removeprefix("sdv_").removesuffix("_weekly")
    return f"sdv_{short_name}_{uuid.UUID(run_id).hex}"


def _stage_rows(
    dsn: str,
    spec: FlatFileSpec,
    asset: SourcePublicationAsset,
    raw: bytes,
    ctx: ParseContext,
    run_id: str,
    expected_rows: int,
    artifact_origin: str,
    season_basis: str,
) -> _StageResult:
    _require_stage_schema(dsn)
    table = _stage_table_name(asset.source_name, run_id)
    stage_spec = replace(spec, table=table, write_disposition="append")
    source = build_flat_file_source(stage_spec, raw, ctx, None)
    source.resources[table].apply_hints(
        columns=_column_hints(asset),
        schema_contract={"tables": "evolve", "columns": "freeze", "data_type": "freeze"},
    )
    run_hex = uuid.UUID(run_id).hex
    with tempfile.TemporaryDirectory(prefix="sdv-source-dlt-") as pipelines_dir:
        pipeline = dlt.pipeline(
            pipeline_name=f"sdv_source_publication_{run_hex}",
            pipelines_dir=pipelines_dir,
            destination=postgres(credentials=dsn),
            dataset_name=STAGE_SCHEMA,
        )
        load_info = pipeline.run(source)
        load_info.raise_on_failed_jobs()
        normalized_count = pipeline.last_trace.last_normalize_info.row_counts.get(table, 0)
    if normalized_count != expected_rows:
        raise SourcePublicationError("dlt normalized row count does not match parsed source rows")
    return _read_stage(dsn, table, expected_rows, asset, artifact_origin, season_basis)


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


def _known_error_text(
    error: BaseException, source_name: str, run_id: str, generation_id: str
) -> str:
    if isinstance(error, SourcePublicationError):
        detail = str(error)
    elif isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 404:
        detail = f"{source_name} season file is not published"
    else:
        detail = f"{source_name} source publication failed"
    return f"{detail}; run_id={run_id}; generation_id={generation_id}"[:ERROR_MESSAGE_LIMIT]


def _generic_work(
    dsn: str,
    spec: FlatFileSpec,
    file_path: str | None,
    season: int,
    run_id: str,
    generation_id: str,
    state: _MutationState,
    context: _RunContext,
) -> tuple[str, int]:
    asset = SOURCE_PUBLICATION_ASSETS[spec.name]
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
        raise SourcePublicationError(f"{asset.source_name} has no fetch target")
    fetched = fetch_file(target)
    if fetched.sha256 != hashlib.sha256(fetched.content).hexdigest():
        raise SourcePublicationError("fetched source SHA-256 does not match its bytes")
    context.source_sha = fetched.sha256
    season_basis = asset.season_basis(artifact_origin)
    ctx = ParseContext(
        source=spec.name,
        snapshot_date=date.today(),
        season=season,
        source_url=fetched.source_url,
        file_name=os.path.basename(fetched.source_url),
    )
    raw_count = _raw_row_count(fetched.content, asset, season)
    parsed_rows = list(resolve_parser(spec.parser)(fetched.content, ctx))
    _validate_parsed_rows(parsed_rows, raw_count, asset, season)
    context.stage_table = _stage_table_name(asset.source_name, run_id)
    stage = _stage_rows(
        dsn,
        spec,
        asset,
        fetched.content,
        ctx,
        run_id,
        len(parsed_rows),
        artifact_origin,
        season_basis,
    )
    if stage.table != context.stage_table:
        raise SourcePublicationError("dlt returned a mismatched staging table")
    _, _, published_rows = _publish(dsn, run_id, generation_id, fetched.sha256, stage, state)
    return fetched.sha256, published_rows


def _run_publication_lifecycle(
    spec: FlatFileSpec,
    *,
    file_path: str | None,
    season: int,
    hooks: _LifecycleHooks,
) -> dict[str, Any]:
    """Run shared start/work/terminal-failure/cleanup transaction semantics."""
    started_at = time.monotonic()
    run_id = str(hooks.new_id())
    generation_id = str(hooks.new_id())
    result = _base_result(hooks.source_name, run_id, generation_id)
    state = _MutationState()
    context = _RunContext()
    dsn = ""
    try:
        hooks.preflight(spec, season)
        dsn = hooks.get_db_url()
        plan = hooks.get_plan(dsn, season)
        hooks.start_run(dsn, run_id, plan, state)
        sha256, published_rows = hooks.work(
            dsn,
            spec,
            file_path,
            season,
            run_id,
            generation_id,
            state,
            context,
        )
        context.terminal_confirmed = True
        result.update(status="loaded", rows=published_rows, sha=sha256)
        return result
    except UncertainCommitError as error:
        context.terminal_confirmed = state.published
        result["error"] = (f"{error}; run_id={run_id}; generation_id={generation_id}")[
            :ERROR_MESSAGE_LIMIT
        ]
        if isinstance(error.__cause__, (KeyboardInterrupt, SystemExit)):
            raise error.__cause__
        logger.error(result["error"])
        return result
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            interrupt = error
        elif isinstance(error.__cause__, (KeyboardInterrupt, SystemExit)):
            interrupt = error.__cause__
        else:
            interrupt = None
        deferred = isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 404
        blocked = isinstance(error, psycopg2.Error) and error.pgcode in {"40001", "55000"}
        outcome = "deferred" if deferred else "blocked" if blocked else "failed"
        result["status"] = "not_published" if deferred else outcome
        result["error"] = hooks.known_error_text(error, run_id, generation_id)
        if state.published:
            context.terminal_confirmed = True
        elif state.started and not state.publish_pending:
            try:
                hooks.fail_run(dsn, run_id, generation_id, outcome, state)
                context.terminal_confirmed = True
            except (UncertainCommitError, PostCommitError) as failure_error:
                context.terminal_confirmed = state.failure_recorded
                logger.error(
                    "Source failure receipt was not confirmed; source=%s "
                    "run_id=%s generation_id=%s",
                    hooks.source_name,
                    run_id,
                    generation_id,
                )
                if isinstance(failure_error.__cause__, (KeyboardInterrupt, SystemExit)):
                    raise failure_error.__cause__
            except Exception:
                context.terminal_confirmed = state.failure_recorded
                logger.error(
                    "Source failure receipt was not confirmed; source=%s "
                    "run_id=%s generation_id=%s",
                    hooks.source_name,
                    run_id,
                    generation_id,
                )
        if interrupt is not None:
            raise interrupt
        return result
    finally:
        if (
            context.stage_table is not None
            and dsn
            and (context.terminal_confirmed or state.published or state.failure_recorded)
        ):
            try:
                hooks.drop_stage(dsn, context.stage_table)
            except Exception:
                logger.warning(
                    "Could not drop terminal stage table %s; run_id=%s",
                    context.stage_table,
                    run_id,
                )
        result["duration_s"] = time.monotonic() - started_at
        if context.source_sha is not None:
            result["sha"] = context.source_sha


def run_source_publication(
    spec: FlatFileSpec,
    *,
    file_path: str | None = None,
    season: int,
) -> dict[str, Any]:
    """Publish one complete enrolled SportsDataverse season file."""
    if spec.name == "sdv_ratings_weekly":
        from src.pipelines.utils.sdv_ratings_publication import (
            run_sdv_ratings_publication,
        )

        return run_sdv_ratings_publication(spec, file_path=file_path, season=season)

    hooks = _LifecycleHooks(
        source_name=spec.name,
        new_id=uuid.uuid4,
        preflight=lambda selected_spec, selected_season: _asset_for(selected_spec, selected_season),
        get_db_url=get_db_url,
        get_plan=lambda dsn, selected_season: _get_plan(
            dsn, SOURCE_PUBLICATION_ASSETS[spec.name], selected_season
        ),
        start_run=_start_run,
        work=_generic_work,
        fail_run=_fail_run,
        drop_stage=_drop_stage_table,
        known_error_text=lambda error, run_id, generation_id: _known_error_text(
            error, spec.name, run_id, generation_id
        ),
    )
    return _run_publication_lifecycle(spec, file_path=file_path, season=season, hooks=hooks)


__all__ = ["run_source_publication"]
