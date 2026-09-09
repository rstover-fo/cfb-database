"""Unit coverage for the receipt-backed SDV ratings publication adapter."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import replace

import httpx
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.pipelines.config.source_publication_assets import SDV_RATINGS_PUBLICATION
from src.pipelines.sources.flat_files import REGISTRY
from src.pipelines.utils import sdv_ratings_publication as publication
from src.pipelines.utils.file_fetcher import FetchedFile

RUN_ID = "11111111-1111-4111-8111-111111111111"
GENERATION_ID = "22222222-2222-4222-8222-222222222222"


def source_row(**overrides):
    row = {
        "season": 2025,
        "through_week": 1,
        "team_id": 333,
        "adj_off_epa": 0.1,
        "adj_def_epa": -0.2,
        "adj_st_epa": None,
        "adj_net": 0.3,
        "fei_off": None,
        "fei_def": None,
        "fei_net": None,
        "off_pace": 2.5,
        "net_z": 1.1,
        "games": 1,
        "off_rank": 2,
        "def_rank": 3,
        "net_rank": 1,
    }
    row.update(overrides)
    return row


def raw_parquet(**overrides):
    row = source_row(team_id="333")
    row.update(overrides)
    buffer = publication.io.BytesIO()
    pq.write_table(pa.Table.from_pylist([row]), buffer)
    return buffer.getvalue()


def plan(season=2025):
    return {
        "protocol": "sdv-ratings-season-v1",
        "asset_key": "ratings.sdv_ratings_weekly",
        "coverage_key": f"season:{season}",
        "season": season,
        "expected_generation_id": None,
        "parser_contract": "sdv-ratings-v1",
    }


def staged(run_id=RUN_ID, *, origin="local_file"):
    row = {
        **source_row(),
        "_dlt_id": "row-id",
        "_dlt_load_id": "load-id",
    }
    return publication._StageResult(
        table=publication._stage_table_name(run_id),
        rows=[row],
        evidence={
            "stage_schema": "warehouse_source_stage",
            "parser_contract": "sdv-ratings-v1",
            "dlt_load_ids": ["load-id"],
            "source_rows": 1,
            "artifact_origin": origin,
        },
    )


def install_ids(monkeypatch):
    values = iter((uuid.UUID(RUN_ID), uuid.UUID(GENERATION_ID)))
    monkeypatch.setattr(publication.uuid, "uuid4", lambda: next(values))


def install_success_fakes(monkeypatch, tmp_path, events):
    install_ids(monkeypatch)
    raw = b"fixture parquet bytes"
    sha = hashlib.sha256(raw).hexdigest()
    path = tmp_path / "ratings.parquet"
    path.write_bytes(raw)

    monkeypatch.setattr(publication, "get_db_url", lambda: "postgres://fixture")
    monkeypatch.setattr(publication, "_get_plan", lambda dsn, season: plan(season))

    def start(dsn, run_id, selected_plan, state):
        state.started = True
        events.append(("start", run_id, selected_plan))

    def fetch(target):
        events.append(("fetch", target))
        return FetchedFile(content=raw, sha256=sha, source_url=str(path))

    monkeypatch.setattr(publication, "_start_run", start)
    monkeypatch.setattr(publication, "fetch_file", fetch)
    monkeypatch.setattr(publication, "resolve_parser", lambda ref: lambda raw, ctx: [source_row()])
    monkeypatch.setattr(publication, "_raw_row_count", lambda raw: 1)

    def stage(dsn, spec, content, ctx, run_id, expected_rows, artifact_origin):
        events.append(("stage", artifact_origin, run_id, expected_rows))
        return staged(run_id, origin=artifact_origin)

    monkeypatch.setattr(publication, "_stage_rows", stage)
    monkeypatch.setattr(
        publication,
        "_publish",
        lambda dsn, run_id, generation_id, fetched_sha, stage_result, state: (
            generation_id,
            False,
            len(stage_result.rows),
        ),
    )
    monkeypatch.setattr(
        publication,
        "_drop_stage_table",
        lambda dsn, table: events.append(("drop", table)),
    )
    return path, sha


def test_success_starts_before_fetch_and_drops_only_run_stage(monkeypatch, tmp_path):
    events = []
    path, sha = install_success_fakes(monkeypatch, tmp_path, events)

    result = publication.run_sdv_ratings_publication(
        REGISTRY["sdv_ratings_weekly"], file_path=str(path), season=2025
    )

    assert result == {
        "source": "sdv_ratings_weekly",
        "status": "loaded",
        "rows": 1,
        "sha": sha,
        "duration_s": result["duration_s"],
        "error": None,
        "unmapped": None,
        "gaps": None,
        "run_id": RUN_ID,
        "generation_id": GENERATION_ID,
    }
    assert result["duration_s"] >= 0
    assert [event[0] for event in events] == ["start", "fetch", "stage", "drop"]
    assert events[2][1] == "local_file"
    assert events[3][1] == f"sdv_ratings_{uuid.UUID(RUN_ID).hex}"


def test_registered_fetch_has_registered_origin(monkeypatch, tmp_path):
    events = []
    install_success_fakes(monkeypatch, tmp_path, events)

    result = publication.run_sdv_ratings_publication(REGISTRY["sdv_ratings_weekly"], season=2025)

    assert result["status"] == "loaded"
    assert events[1][1].endswith("cfb_ratings_weekly_2025.parquet")
    assert events[2][1] == "registered_url"


def test_only_canonical_registry_spec_is_supported():
    with pytest.raises(ValueError, match="canonical"):
        publication._validate_supported_spec(replace(REGISTRY["sdv_ratings_weekly"]), 2025)
    with pytest.raises(ValueError, match="1869 to 2200"):
        publication._validate_supported_spec(REGISTRY["sdv_ratings_weekly"], 2201)


def test_run_preserves_failed_result_contract_for_invalid_spec(monkeypatch):
    install_ids(monkeypatch)
    monkeypatch.setattr(
        publication,
        "get_db_url",
        lambda: pytest.fail("preflight must run before database configuration"),
    )
    result = publication.run_sdv_ratings_publication(
        replace(REGISTRY["sdv_ratings_weekly"]), season=2025
    )
    assert result["status"] == "failed"
    assert "source publication failed" in result["error"]
    assert result["run_id"] == RUN_ID
    assert result["generation_id"] == GENERATION_ID


def test_plan_requires_exact_protocol_and_keys():
    assert publication._validated_plan(plan(), 2025) == plan()
    with pytest.raises(publication.SourcePublicationError, match="invalid.*plan"):
        publication._validated_plan({**plan(), "extra": True}, 2025)
    with pytest.raises(publication.SourcePublicationError, match="mismatched"):
        publication._validated_plan({**plan(), "coverage_key": "season:2024"}, 2025)
    with pytest.raises(publication.SourcePublicationError, match="invalid expected"):
        publication._validated_plan({**plan(), "expected_generation_id": "not-a-uuid"}, 2025)


@pytest.mark.parametrize(
    "rows,raw_count,message",
    [
        ([], 0, "nonempty"),
        ([source_row()], 2, "dropped or added"),
        ([source_row(season=2024)], 1, "season does not match"),
        ([source_row(team_id=None)], 1, "null primary-key"),
        ([source_row(), source_row()], 2, "duplicate primary key"),
        ([source_row(adj_net=float("nan"))], 1, "not finite"),
        ([source_row(adj_net=float("inf"))], 1, "not finite"),
        ([source_row(games=1.5)], 1, "not an integer"),
    ],
)
def test_source_snapshot_validation_rejects_incomplete_or_unsafe_rows(rows, raw_count, message):
    with pytest.raises(publication.SourcePublicationError, match=message):
        publication._validate_parsed_rows(rows, raw_count, 2025)


def test_source_snapshot_requires_all_and_only_known_columns():
    missing = source_row()
    missing.pop("fei_net")
    with pytest.raises(publication.SourcePublicationError, match="16-column"):
        publication._validate_parsed_rows([missing], 1, 2025)
    with pytest.raises(publication.SourcePublicationError, match="16-column"):
        publication._validate_parsed_rows([source_row(surprise=1)], 1, 2025)


def test_column_hints_type_all_optional_null_columns():
    hints = {hint["name"]: hint for hint in publication._column_hints()}
    assert tuple(hints) == SDV_RATINGS_PUBLICATION.columns
    assert hints["season"] == {"name": "season", "data_type": "bigint", "nullable": False}
    assert hints["adj_st_epa"] == {
        "name": "adj_st_epa",
        "data_type": "double",
        "nullable": True,
    }
    assert len(hints) == 16


def test_stage_uses_append_with_unique_table_and_explicit_hints(monkeypatch):
    captured = {}

    class Resource:
        def apply_hints(self, **hints):
            captured["hints"] = hints

    class Source:
        resources = {publication._stage_table_name(RUN_ID): Resource()}

    class LoadInfo:
        def raise_on_failed_jobs(self):
            captured["checked_failures"] = True

    class NormalizeInfo:
        row_counts = {publication._stage_table_name(RUN_ID): 1}

    class Trace:
        last_normalize_info = NormalizeInfo()

    class Pipeline:
        last_trace = Trace()

        def run(self, source):
            return LoadInfo()

    monkeypatch.setattr(publication, "_require_stage_schema", lambda dsn: None)

    def build(spec, raw, ctx, resolver):
        captured["spec"] = spec
        return Source()

    monkeypatch.setattr(publication, "build_flat_file_source", build)
    monkeypatch.setattr(
        publication.dlt,
        "pipeline",
        lambda **kwargs: captured.update(pipeline=kwargs) or Pipeline(),
    )
    monkeypatch.setattr(publication, "postgres", lambda credentials: ("postgres", credentials))
    expected = staged(origin="local_file")
    monkeypatch.setattr(publication, "_read_stage", lambda *args: expected)

    result = publication._stage_rows(
        "postgres://fixture",
        REGISTRY["sdv_ratings_weekly"],
        b"raw",
        object(),
        RUN_ID,
        1,
        "local_file",
    )

    assert result is expected
    assert captured["spec"].write_disposition == "append"
    assert captured["spec"].table == publication._stage_table_name(RUN_ID)
    assert captured["hints"]["schema_contract"] == {
        "tables": "evolve",
        "columns": "freeze",
        "data_type": "freeze",
    }
    assert len(captured["hints"]["columns"]) == 16
    assert captured["pipeline"]["dataset_name"] == "warehouse_source_stage"
    assert captured["pipeline"]["destination"] == ("postgres", "postgres://fixture")
    assert captured["checked_failures"] is True


def test_file_override_must_be_an_existing_local_file(monkeypatch):
    install_ids(monkeypatch)
    failures = []
    monkeypatch.setattr(publication, "get_db_url", lambda: "postgres://fixture")
    monkeypatch.setattr(publication, "_get_plan", lambda dsn, season: plan())

    def start(dsn, run_id, selected_plan, state):
        state.started = True

    monkeypatch.setattr(publication, "_start_run", start)
    monkeypatch.setattr(
        publication,
        "_fail_run",
        lambda dsn, run_id, generation_id, outcome, state: failures.append(outcome),
    )

    result = publication.run_sdv_ratings_publication(
        REGISTRY["sdv_ratings_weekly"],
        file_path="https://untrusted.example/ratings.parquet",
        season=2025,
    )

    assert result["status"] == "failed"
    assert "existing local file" in result["error"]
    assert failures == ["failed"]


def test_fetched_hash_must_match_bytes(monkeypatch, tmp_path):
    events = []
    path, _ = install_success_fakes(monkeypatch, tmp_path, events)
    failures = []
    monkeypatch.setattr(
        publication,
        "fetch_file",
        lambda target: FetchedFile(content=b"actual", sha256="0" * 64, source_url=str(path)),
    )
    monkeypatch.setattr(
        publication,
        "_fail_run",
        lambda dsn, run_id, generation_id, outcome, state: failures.append(outcome),
    )

    result = publication.run_sdv_ratings_publication(
        REGISTRY["sdv_ratings_weekly"], file_path=str(path), season=2025
    )

    assert result["status"] == "failed"
    assert "SHA-256" in result["error"]
    assert failures == ["failed"]
    assert not any(event[0] in {"stage", "drop"} for event in events)


def test_oversized_parquet_is_rejected_before_parser_materialization(monkeypatch, tmp_path):
    events = []
    path, _ = install_success_fakes(monkeypatch, tmp_path, events)
    failures = []
    monkeypatch.setattr(publication, "_raw_row_count", lambda raw: 100_001)
    monkeypatch.setattr(
        publication,
        "resolve_parser",
        lambda ref: pytest.fail("oversized input must be bounded before parser materialization"),
    )
    monkeypatch.setattr(
        publication,
        "_fail_run",
        lambda dsn, run_id, generation_id, outcome, state: failures.append(outcome),
    )

    result = publication.run_sdv_ratings_publication(
        REGISTRY["sdv_ratings_weekly"], file_path=str(path), season=2025
    )

    assert result["status"] == "failed"
    assert "100000" in result["error"]
    assert failures == ["failed"]


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("season", 2025.5),
        ("through_week", 1.5),
        ("team_id", 333.5),
        ("games", 1.5),
        ("off_rank", 2.5),
        ("def_rank", 3.5),
        ("net_rank", 1.5),
    ],
)
def test_raw_parquet_fractional_integer_columns_are_rejected_before_parser(
    monkeypatch, tmp_path, column, value
):
    real_raw_row_count = publication._raw_row_count
    events = []
    path, _ = install_success_fakes(monkeypatch, tmp_path, events)
    raw = raw_parquet(**{column: value})
    path.write_bytes(raw)
    failures = []
    monkeypatch.setattr(publication, "_raw_row_count", real_raw_row_count)
    monkeypatch.setattr(
        publication,
        "fetch_file",
        lambda target: FetchedFile(
            content=raw,
            sha256=hashlib.sha256(raw).hexdigest(),
            source_url=str(path),
        ),
    )
    monkeypatch.setattr(
        publication,
        "resolve_parser",
        lambda ref: pytest.fail("fractional raw integers must be rejected before parsing"),
    )
    monkeypatch.setattr(
        publication,
        "_fail_run",
        lambda dsn, run_id, generation_id, outcome, state: failures.append(outcome),
    )

    result = publication.run_sdv_ratings_publication(
        REGISTRY["sdv_ratings_weekly"], file_path=str(path), season=2025
    )

    assert result["status"] == "failed"
    assert column in result["error"]
    assert "non-integer" in result["error"]
    assert failures == ["failed"]
    assert not any(event[0] == "stage" for event in events)


def test_raw_parquet_accepts_lossless_string_float_and_null_integer_values():
    raw = raw_parquet(season=2025.0, through_week=1.0, games=None)

    assert publication._raw_row_count(raw) == 1


def test_404_is_deferred_and_never_staged(monkeypatch, tmp_path):
    events = []
    install_success_fakes(monkeypatch, tmp_path, events)
    failures = []
    request = httpx.Request("GET", "https://example.test/ratings.parquet")
    response = httpx.Response(404, request=request)
    monkeypatch.setattr(
        publication,
        "fetch_file",
        lambda target: (_ for _ in ()).throw(
            httpx.HTTPStatusError("404", request=request, response=response)
        ),
    )
    monkeypatch.setattr(
        publication,
        "_fail_run",
        lambda dsn, run_id, generation_id, outcome, state: failures.append(outcome),
    )

    result = publication.run_sdv_ratings_publication(REGISTRY["sdv_ratings_weekly"], season=2025)

    assert result["status"] == "not_published"
    assert failures == ["deferred"]
    assert not any(event[0] in {"stage", "drop"} for event in events)


def test_uncertain_publish_is_not_retried_failed_or_cleaned(monkeypatch, tmp_path):
    events = []
    path, _ = install_success_fakes(monkeypatch, tmp_path, events)
    monkeypatch.setattr(
        publication,
        "_publish",
        lambda *args: (_ for _ in ()).throw(
            publication.UncertainCommitError("publication commit outcome is uncertain")
        ),
    )
    monkeypatch.setattr(
        publication,
        "_fail_run",
        lambda *args: pytest.fail("uncertain publication must not record failure"),
    )

    result = publication.run_sdv_ratings_publication(
        REGISTRY["sdv_ratings_weekly"], file_path=str(path), season=2025
    )

    assert result["status"] == "failed"
    assert "uncertain" in result["error"]
    assert not any(event[0] == "drop" for event in events)


def test_malformed_committed_publish_response_is_not_relabeled_failed(monkeypatch, tmp_path):
    events = []
    path, _ = install_success_fakes(monkeypatch, tmp_path, events)

    def malformed_publish(dsn, run_id, generation_id, sha, stage_result, state):
        state.published = True
        raise publication.UncertainCommitError("committed an unrecognized publication result")

    monkeypatch.setattr(publication, "_publish", malformed_publish)
    monkeypatch.setattr(
        publication,
        "_fail_run",
        lambda *args: pytest.fail("committed publication must not record failure"),
    )

    result = publication.run_sdv_ratings_publication(
        REGISTRY["sdv_ratings_weekly"], file_path=str(path), season=2025
    )

    assert result["status"] == "failed"
    assert events[-1] == ("drop", publication._stage_table_name(RUN_ID))


class _BlockedSqlError(psycopg2.Error):
    @property
    def pgcode(self):
        return "40001"


def test_cas_readiness_error_records_blocked(monkeypatch, tmp_path):
    events = []
    path, _ = install_success_fakes(monkeypatch, tmp_path, events)
    failures = []
    monkeypatch.setattr(
        publication,
        "_publish",
        lambda *args: (_ for _ in ()).throw(_BlockedSqlError("replan")),
    )
    monkeypatch.setattr(
        publication,
        "_fail_run",
        lambda dsn, run_id, generation_id, outcome, state: failures.append(outcome),
    )

    result = publication.run_sdv_ratings_publication(
        REGISTRY["sdv_ratings_weekly"], file_path=str(path), season=2025
    )

    assert result["status"] == "blocked"
    assert failures == ["blocked"]
    assert events[-1] == ("drop", publication._stage_table_name(RUN_ID))


def test_known_stage_failure_records_failure_and_drops_run_table(monkeypatch, tmp_path):
    events = []
    path, sha = install_success_fakes(monkeypatch, tmp_path, events)
    failures = []
    monkeypatch.setattr(
        publication,
        "_stage_rows",
        lambda *args: (_ for _ in ()).throw(publication.SourcePublicationError("stage failed")),
    )
    monkeypatch.setattr(
        publication,
        "_fail_run",
        lambda dsn, run_id, generation_id, outcome, state: failures.append(outcome),
    )

    result = publication.run_sdv_ratings_publication(
        REGISTRY["sdv_ratings_weekly"], file_path=str(path), season=2025
    )

    assert result["status"] == "failed"
    assert result["sha"] == sha
    assert failures == ["failed"]
    assert events[-1] == ("drop", publication._stage_table_name(RUN_ID))


def test_committed_failure_with_malformed_response_still_drops_stage(monkeypatch, tmp_path):
    events = []
    path, _ = install_success_fakes(monkeypatch, tmp_path, events)
    monkeypatch.setattr(
        publication,
        "_stage_rows",
        lambda *args: (_ for _ in ()).throw(publication.SourcePublicationError("stage failed")),
    )

    def malformed_failure(dsn, run_id, generation_id, outcome, state):
        state.failure_recorded = True
        raise publication.SourcePublicationError("mismatched failure response")

    monkeypatch.setattr(publication, "_fail_run", malformed_failure)

    result = publication.run_sdv_ratings_publication(
        REGISTRY["sdv_ratings_weekly"], file_path=str(path), season=2025
    )

    assert result["status"] == "failed"
    assert events[-1] == ("drop", publication._stage_table_name(RUN_ID))


def test_keyboard_interrupt_records_failure_cleans_stage_and_rethrows(monkeypatch, tmp_path):
    events = []
    path, _ = install_success_fakes(monkeypatch, tmp_path, events)
    failures = []
    monkeypatch.setattr(
        publication,
        "_stage_rows",
        lambda *args: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    monkeypatch.setattr(
        publication,
        "_fail_run",
        lambda dsn, run_id, generation_id, outcome, state: failures.append(outcome),
    )

    with pytest.raises(KeyboardInterrupt):
        publication.run_sdv_ratings_publication(
            REGISTRY["sdv_ratings_weekly"], file_path=str(path), season=2025
        )

    assert failures == ["failed"]
    assert events[-1] == ("drop", publication._stage_table_name(RUN_ID))


class _CommitFailureConnection:
    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    @property
    def description(self):
        return None

    def execute(self, statement, args=()):
        return None

    def commit(self):
        raise OSError("lost commit response")

    def rollback(self):
        pytest.fail("uncertain mutating commit must not be relabeled by rollback")

    def close(self):
        return None


def test_mutating_rpc_converts_commit_failure_to_uncertain(monkeypatch):
    monkeypatch.setattr(publication, "_connect", lambda dsn: _CommitFailureConnection())

    with pytest.raises(publication.UncertainCommitError):
        publication._rpc(
            "postgres://fixture",
            "SELECT do_work()",
            (),
            mutating=True,
            state=publication._MutationState(),
            phase="publish",
        )


def test_keyboard_interrupt_during_mutating_commit_is_not_swallowed(monkeypatch, tmp_path):
    events = []
    path, _ = install_success_fakes(monkeypatch, tmp_path, events)
    interrupt = KeyboardInterrupt()

    def interrupted_publish(*args):
        raise publication.UncertainCommitError("uncertain") from interrupt

    monkeypatch.setattr(publication, "_publish", interrupted_publish)

    with pytest.raises(KeyboardInterrupt):
        publication.run_sdv_ratings_publication(
            REGISTRY["sdv_ratings_weekly"], file_path=str(path), season=2025
        )

    assert not any(event[0] == "drop" for event in events)


class _PostCommitCloseInterruptConnection(_CommitFailureConnection):
    def __init__(self):
        self.closed = False

    def commit(self):
        return None

    def close(self):
        self.closed = True
        raise KeyboardInterrupt()


def test_publish_close_interrupt_preserves_confirmed_commit_state(monkeypatch):
    conn = _PostCommitCloseInterruptConnection()
    monkeypatch.setattr(publication, "_connect", lambda dsn: conn)
    state = publication._MutationState()

    with pytest.raises(publication.PostCommitError) as raised:
        publication._rpc(
            "postgres://fixture",
            "SELECT do_work()",
            (),
            mutating=True,
            state=state,
            phase="publish",
        )

    assert isinstance(raised.value.__cause__, KeyboardInterrupt)
    assert state.published is True
    assert state.publish_pending is False
    assert conn.closed is True


class _CommittedRowCloseFailureConnection(_CommitFailureConnection):
    def __init__(self, row):
        self.row = row

    @property
    def description(self):
        return (object(),)

    def fetchone(self):
        return self.row

    def commit(self):
        return None

    def close(self):
        raise OSError("socket close failed")


def test_confirmed_publish_close_error_returns_loaded_rows(monkeypatch, tmp_path, caplog):
    real_publish = publication._publish
    events = []
    path, _ = install_success_fakes(monkeypatch, tmp_path, events)
    monkeypatch.setattr(publication, "_publish", real_publish)
    monkeypatch.setattr(
        publication,
        "_connect",
        lambda dsn: _CommittedRowCloseFailureConnection((GENERATION_ID, False, 1)),
    )

    with caplog.at_level("WARNING"):
        result = publication.run_sdv_ratings_publication(
            REGISTRY["sdv_ratings_weekly"], file_path=str(path), season=2025
        )

    assert result["status"] == "loaded"
    assert result["rows"] == 1
    assert result["error"] is None
    assert events[-1] == ("drop", publication._stage_table_name(RUN_ID))
    assert "committed but connection close failed" in caplog.text


def test_malformed_publish_response_after_close_error_is_not_success(monkeypatch, tmp_path):
    real_publish = publication._publish
    events = []
    path, _ = install_success_fakes(monkeypatch, tmp_path, events)
    monkeypatch.setattr(publication, "_publish", real_publish)
    monkeypatch.setattr(
        publication,
        "_connect",
        lambda dsn: _CommittedRowCloseFailureConnection((GENERATION_ID, False, 2)),
    )

    result = publication.run_sdv_ratings_publication(
        REGISTRY["sdv_ratings_weekly"], file_path=str(path), season=2025
    )

    assert result["status"] == "failed"
    assert result["rows"] == 0
    assert "mismatched publication result" in result["error"]
    assert events[-1] == ("drop", publication._stage_table_name(RUN_ID))
