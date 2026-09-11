"""Unit coverage for the explicit SDV FPI receipt canary."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path

import httpx
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import run_source_receipt_canary as canary
from src.pipelines.utils.file_fetcher import FetchedFile

PROJECT_REF = "ibobsbwlewpqslkqbrjd"
EXPECTED_SHA = "a" * 64


def args(action="preflight"):
    return argparse.Namespace(
        action=action,
        source="sdv_fpi_weekly",
        season=2026,
        expected_sha256=EXPECTED_SHA,
        expected_project_ref=PROJECT_REF,
    )


def cli_args(action):
    return [
        action,
        "--source",
        "sdv_fpi_weekly",
        "--season",
        "2026",
        "--expected-sha256",
        EXPECTED_SHA,
        "--expected-project-ref",
        PROJECT_REF,
    ]


def source_plan():
    return {
        "protocol": "sdv-season-file-v1",
        "source_name": "sdv_fpi_weekly",
        "asset_key": "ratings.espn_fpi_weekly",
        "coverage_key": "season:2026",
        "season": 2026,
        "expected_generation_id": None,
        "parser_contract": "sdv-fpi-v1",
    }


def ratings_plan():
    return {
        "protocol": "sdv-ratings-season-v1",
        "asset_key": "ratings.sdv_ratings_weekly",
        "coverage_key": "season:2026",
        "season": 2026,
        "expected_generation_id": None,
        "parser_contract": "sdv-ratings-v1",
    }


class FakeCursor:
    def __init__(self, *, can_set_role, migrations=None):
        self.can_set_role = can_set_role
        self.migrations = migrations
        self.statement = ""
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        return False

    def execute(self, statement, parameters=None):
        self.statement = str(statement)
        self.calls.append((self.statement, parameters))

    def fetchone(self):
        if "current_database()" in self.statement:
            return ("postgres", "runtime_login", "runtime_login", self.can_set_role)
        if "SELECT current_user::text" in self.statement:
            return (canary.PUBLISHER_ROLE,)
        if "get_source_plan" in self.statement:
            return (source_plan(),)
        if "get_sdv_ratings_plan" in self.statement:
            return (ratings_plan(),)
        raise AssertionError(f"unexpected fetchone for {self.statement}")

    def fetchall(self):
        if "warehouse_control.schema_migrations" in self.statement:
            return list(self.migrations or canary._expected_migrations())
        raise AssertionError(f"unexpected fetchall for {self.statement}")


class FakeConnection:
    def __init__(self, *, can_set_role, migrations=None):
        self.cursor_object = FakeCursor(can_set_role=can_set_role, migrations=migrations)
        self.session_calls = []
        self.rollbacks = 0
        self.commits = 0
        self.closed = False

    def set_session(self, **kwargs):
        self.session_calls.append(kwargs)

    def cursor(self):
        return self.cursor_object

    def rollback(self):
        self.rollbacks += 1

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


@pytest.mark.parametrize(
    "dsn,evidence",
    [
        (
            f"postgresql://postgres:secret@db.{PROJECT_REF}.supabase.co/postgres",
            ["direct_host_project_ref"],
        ),
        (
            f"postgresql://postgres.{PROJECT_REF}:secret@aws-0-us-east-1.pooler.supabase.com/postgres",
            ["pooler_user_project_ref"],
        ),
    ],
)
def test_read_only_inspection_verifies_runtime_target_and_bounded_plan(dsn, evidence):
    connection = FakeConnection(can_set_role=True)
    connect_calls = []

    def connect(*values, **options):
        connect_calls.append((values, options))
        return connection

    identity, plan = canary._inspect_target(
        dsn,
        PROJECT_REF,
        "sdv_fpi_weekly",
        2026,
        connector=connect,
    )

    assert connect_calls == [
        ((dsn,), {"connect_timeout": 10, "application_name": "source-receipt-canary"})
    ]
    assert connection.session_calls == [{"readonly": True, "autocommit": False}]
    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert connection.closed is True
    assert identity["project_ref_evidence"] == evidence
    assert identity["database"] == "postgres"
    assert identity["current_user"] == identity["session_user"] == "runtime_login"
    assert identity["can_set_role"] is True
    assert identity["assumed_role"] == canary.PUBLISHER_ROLE
    assert identity["activation_required"] is False
    assert identity["migration_ledger"]["verified"] is True
    assert [entry["id"] for entry in identity["migration_ledger"]["entries"]] == [
        row[0] for row in canary._expected_migrations()
    ]
    assert plan == source_plan()
    statements = [statement for statement, _ in connection.cursor_object.calls]
    assert any("SET LOCAL ROLE" in statement for statement in statements)
    assert any("get_source_plan" in statement for statement in statements)
    ledger_statement, ledger_parameters = next(
        call
        for call in connection.cursor_object.calls
        if "warehouse_control.schema_migrations" in call[0]
    )
    assert "WHERE" not in ledger_statement
    assert ledger_parameters is None


def test_preflight_accepts_unactivated_login_without_attempting_set_role():
    dsn = f"postgresql://postgres.{PROJECT_REF}:secret@aws-0-us-east-1.pooler.supabase.com/postgres"
    connection = FakeConnection(can_set_role=False)
    identity, plan = canary._inspect_target(
        dsn,
        PROJECT_REF,
        "sdv_fpi_weekly",
        2026,
        connector=lambda *args, **kwargs: connection,
    )

    assert plan is None
    assert identity["activation_required"] is True
    assert connection.commits == 0
    statements = [statement for statement, _ in connection.cursor_object.calls]
    assert not any("SET LOCAL ROLE" in statement for statement in statements)
    assert not any("get_source_plan" in statement for statement in statements)


def test_ratings_inspection_uses_dedicated_plan_rpc_and_validator():
    dsn = f"postgresql://postgres.{PROJECT_REF}:secret@aws.pooler.supabase.com/postgres"
    connection = FakeConnection(can_set_role=True)

    _, plan = canary._inspect_target(
        dsn,
        PROJECT_REF,
        "sdv_ratings_weekly",
        2026,
        connector=lambda *args, **kwargs: connection,
    )

    assert plan == ratings_plan()
    plan_calls = [
        call for call in connection.cursor_object.calls if "get_sdv_ratings_plan" in call[0]
    ]
    assert plan_calls == [("SELECT warehouse_source.get_sdv_ratings_plan(%s)", (2026,))]
    assert not any(
        "warehouse_source_batch.get_source_plan" in statement
        for statement, _ in connection.cursor_object.calls
    )


@pytest.mark.parametrize(
    "dsn,message",
    [
        (
            f"postgresql://postgres.{PROJECT_REF}:secret@db."
            "aaaaaaaaaaaaaaaaaaaa.supabase.co/postgres",
            "different Supabase project",
        ),
        (
            f"postgresql://postgres.aaaaaaaaaaaaaaaaaaaa:secret@db.{PROJECT_REF}."
            "supabase.co/postgres",
            "host and username identify different Supabase projects",
        ),
        (
            f"postgresql://postgres.{PROJECT_REF}:secret@pooler.example/postgres",
            "does not identify the expected Supabase project",
        ),
    ],
)
def test_project_identity_rejects_conflicting_or_unrecognized_dsn(dsn, message):
    with pytest.raises(canary.CanaryError, match=message):
        canary._inspect_target(
            dsn,
            PROJECT_REF,
            "sdv_fpi_weekly",
            2026,
            connector=lambda *args, **kwargs: pytest.fail("invalid target must not connect"),
        )


def test_project_identity_rejects_explicit_hostaddr_before_connect():
    dsn = (
        f"postgresql://postgres.{PROJECT_REF}:secret@"
        "aws-0-us-east-1.pooler.supabase.com/postgres?hostaddr=192.0.2.10"
    )
    with pytest.raises(canary.CanaryError, match="forbidden routing parameters: hostaddr"):
        canary._inspect_target(
            dsn,
            PROJECT_REF,
            "sdv_fpi_weekly",
            2026,
            connector=lambda *args, **kwargs: pytest.fail("hostaddr must fail before connect"),
        )


@pytest.mark.parametrize("variable", ["PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE"])
def test_project_identity_rejects_ambient_libpq_routing_before_connect(monkeypatch, variable):
    dsn = f"postgresql://postgres.{PROJECT_REF}:secret@aws-0-us-east-1.pooler.supabase.com/postgres"
    monkeypatch.setenv(variable, "untrusted-routing-value")
    with pytest.raises(canary.CanaryError, match=variable):
        canary._inspect_target(
            dsn,
            PROJECT_REF,
            "sdv_fpi_weekly",
            2026,
            connector=lambda *args, **kwargs: pytest.fail(
                "ambient routing must fail before connect"
            ),
        )


def test_target_inspection_rejects_unexpected_applied_migration():
    dsn = f"postgresql://postgres.{PROJECT_REF}:secret@aws-0-us-east-1.pooler.supabase.com/postgres"
    migrations = (
        *canary._expected_migrations(),
        ("warehouse.future.074", "future.sql", "immutable", "f" * 64, 10),
    )
    connection = FakeConnection(can_set_role=False, migrations=migrations)

    with pytest.raises(canary.CanaryError, match=r"unexpected=warehouse\.future\.074"):
        canary._inspect_target(
            dsn,
            PROJECT_REF,
            "sdv_fpi_weekly",
            2026,
            connector=lambda *args, **kwargs: connection,
        )


def test_connection_errors_report_only_type_and_code():
    dsn = f"postgresql://postgres.{PROJECT_REF}:secret@aws-0-us-east-1.pooler.supabase.com/postgres"

    def fail_connect(*args, **kwargs):
        raise psycopg2.OperationalError("password=do-not-print host=private-host")

    with pytest.raises(canary.CanaryError) as raised:
        canary._inspect_target(
            dsn,
            PROJECT_REF,
            "sdv_fpi_weekly",
            2026,
            connector=fail_connect,
        )

    assert str(raised.value) == ("database inspection failed (type=OperationalError, code=none)")


def test_preflight_validates_artifact_without_publication(monkeypatch):
    identity = {"can_set_role": False, "activation_required": True}
    artifact = canary.Artifact(b"pinned", EXPECTED_SHA, 2, (0, 1))
    monkeypatch.setattr(canary.publication, "get_db_url", lambda: "dsn")
    monkeypatch.setattr(canary, "_inspect_target", lambda *values: (identity, None))
    monkeypatch.setattr(canary, "_download_and_validate", lambda *values: artifact)
    monkeypatch.setattr(
        canary,
        "_publish_pinned",
        lambda *values: pytest.fail("preflight must never publish"),
    )

    payload, status = canary.run_canary(args())

    assert status == 0
    assert payload["status"] == "activation_required"
    assert payload["artifact"] == {"sha256": EXPECTED_SHA, "rows": 2, "weeks": [0, 1]}


def test_publish_requires_activation_before_fetch_or_publication(monkeypatch):
    monkeypatch.setattr(canary.publication, "get_db_url", lambda: "dsn")
    monkeypatch.setattr(
        canary,
        "_inspect_target",
        lambda *values: ({"can_set_role": False, "activation_required": True}, None),
    )
    monkeypatch.setattr(
        canary,
        "_download_and_validate",
        lambda *values: pytest.fail("inactive publisher must not fetch"),
    )
    monkeypatch.setattr(
        canary,
        "_publish_pinned",
        lambda *values: pytest.fail("inactive publisher must not publish"),
    )

    with pytest.raises(canary.CanaryError, match="activation is required"):
        canary.run_canary(args("publish"))


def test_wrong_sha_stops_before_parser_and_publication(monkeypatch):
    calls = []
    raw = b"changed upstream bytes"
    actual_sha = hashlib.sha256(raw).hexdigest()

    def fetch(url, *, timeout, retries):
        calls.append((url, timeout, retries))
        return FetchedFile(raw, actual_sha, url)

    monkeypatch.setattr(canary, "fetch_file", fetch)
    monkeypatch.setattr(
        canary,
        "resolve_parser",
        lambda parser: pytest.fail("wrong SHA must not parse"),
    )
    with pytest.raises(canary.CanaryError, match="artifact SHA-256 mismatch"):
        canary._download_and_validate("sdv_fpi_weekly", 2026, EXPECTED_SHA)

    assert calls == [
        (
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_fpi_weekly/cfb_fpi_weekly_2026.parquet",
            30,
            1,
        )
    ]


@pytest.mark.parametrize("action", ["preflight", "publish"])
@pytest.mark.parametrize(
    "http_status,expected",
    [
        (
            404,
            {
                "status": "not_published",
                "outcome": "deferred",
                "error_category": "http_status",
                "http_status": 404,
            },
        ),
        (
            503,
            {
                "status": "failed",
                "outcome": "failed",
                "error_category": "http_status",
                "http_status": 503,
            },
        ),
    ],
)
def test_http_fetch_failures_are_sanitized_json_before_publication(
    monkeypatch, capsys, action, http_status, expected
):
    calls = []
    request = httpx.Request("GET", "https://user:secret@example.test/artifact?token=private")
    response = httpx.Response(http_status, request=request)
    failure = httpx.HTTPStatusError(
        f"unsafe request detail token=private status={http_status}",
        request=request,
        response=response,
    )
    monkeypatch.setattr(canary.publication, "get_db_url", lambda: "dsn")
    monkeypatch.setattr(
        canary,
        "_inspect_target",
        lambda *values: ({"can_set_role": True, "activation_required": False}, source_plan()),
    )

    def fetch(url, *, timeout, retries):
        calls.append((url, timeout, retries))
        raise failure

    monkeypatch.setattr(canary, "fetch_file", fetch)
    monkeypatch.setattr(
        canary,
        "_publish_pinned",
        lambda *values: pytest.fail("fetch failure must not start publication or records"),
    )

    status = canary.main(cli_args(action))
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert status == 1
    assert payload == {
        "action": action,
        "season": 2026,
        "source": "sdv_fpi_weekly",
        **expected,
    }
    assert "secret" not in output
    assert "private" not in output
    assert calls == [
        (
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_fpi_weekly/cfb_fpi_weekly_2026.parquet",
            30,
            1,
        )
    ]


@pytest.mark.parametrize("action", ["preflight", "publish"])
@pytest.mark.parametrize(
    "failure_type,transport_category",
    [(httpx.ConnectError, "network"), (httpx.ReadTimeout, "timeout")],
)
def test_transport_fetch_failures_are_sanitized_json_before_publication(
    monkeypatch, capsys, action, failure_type, transport_category
):
    calls = []
    request = httpx.Request("GET", "https://user:secret@example.test/artifact?token=private")
    failure = failure_type("unsafe transport detail token=private", request=request)
    monkeypatch.setattr(canary.publication, "get_db_url", lambda: "dsn")
    monkeypatch.setattr(
        canary,
        "_inspect_target",
        lambda *values: ({"can_set_role": True, "activation_required": False}, source_plan()),
    )

    def fetch(url, *, timeout, retries):
        calls.append((url, timeout, retries))
        raise failure

    monkeypatch.setattr(canary, "fetch_file", fetch)
    monkeypatch.setattr(
        canary,
        "_publish_pinned",
        lambda *values: pytest.fail("fetch failure must not start publication or records"),
    )

    status = canary.main(cli_args(action))
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert status == 1
    assert payload == {
        "action": action,
        "error_category": "transport",
        "outcome": "failed",
        "season": 2026,
        "source": "sdv_fpi_weekly",
        "status": "failed",
        "transport_category": transport_category,
    }
    assert "secret" not in output
    assert "private" not in output
    assert len(calls) == 1
    assert calls[0][1:] == (30, 1)


def test_main_preserves_unexpected_exception_visibility(monkeypatch):
    monkeypatch.setattr(
        canary,
        "run_canary",
        lambda selected: (_ for _ in ()).throw(RuntimeError("unexpected implementation bug")),
    )
    with pytest.raises(RuntimeError, match="unexpected implementation bug"):
        canary.main(cli_args("preflight"))


def test_main_does_not_misclassify_http_error_outside_artifact_fetch(monkeypatch):
    request = httpx.Request("GET", "https://example.test/unrelated")
    failure = httpx.HTTPStatusError(
        "unexpected later HTTP error",
        request=request,
        response=httpx.Response(502, request=request),
    )
    monkeypatch.setattr(
        canary,
        "run_canary",
        lambda selected: (_ for _ in ()).throw(failure),
    )
    with pytest.raises(httpx.HTTPStatusError, match="unexpected later HTTP error"):
        canary.main(cli_args("publish"))


def test_main_sanitizes_ratings_publication_validation_error(monkeypatch, capsys):
    monkeypatch.setattr(
        canary,
        "run_canary",
        lambda selected: (_ for _ in ()).throw(
            canary.sdv_ratings_publication.SourcePublicationError(
                "postgresql://user:secret@private-host/database"
            )
        ),
    )
    argv = cli_args("publish")
    argv[2] = "sdv_ratings_weekly"

    assert canary.main(argv) == 1
    output = capsys.readouterr().out
    assert json.loads(output)["error"] == "SDV ratings source publication validation failed"
    assert "secret" not in output
    assert "private-host" not in output


def _fpi_parquet_bytes() -> bytes:
    fixture = Path(__file__).parent / "fixtures/flatfiles/sdv_fpi_weekly_sample.parquet"
    schema = pq.read_schema(fixture)
    rows = []
    for week, team_id in ((0, 333), (1, 2483)):
        row = {name: None for name in schema.names}
        row.update(
            season=2026,
            season_type=2,
            week=week,
            team_id=team_id,
            last_updated="2026-08-20T12:30:00Z",
            run_date_time_key=20260820123000 + week,
            snapshot_out_of_sequence=False,
            snapshot_is_contemporaneous=True,
            fpi=18.25 - week,
        )
        rows.append(row)
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), buffer)
    return buffer.getvalue()


def test_download_uses_canonical_parser_and_reports_rows_and_weeks(monkeypatch):
    raw = _fpi_parquet_bytes()
    sha = hashlib.sha256(raw).hexdigest()
    seen = []

    def fetch(url, *, timeout, retries):
        seen.append((url, timeout, retries))
        return FetchedFile(raw, sha, url)

    monkeypatch.setattr(canary, "fetch_file", fetch)
    artifact = canary._download_and_validate("sdv_fpi_weekly", 2026, sha)

    assert artifact.content == raw
    assert artifact.sha256 == sha
    assert artifact.rows == 2
    assert artifact.weeks == (0, 1)
    assert seen[0][1:] == (30, 1)


@pytest.mark.parametrize(
    "source,summary_keys",
    [
        ("sdv_ratings_weekly", {"through_weeks", "teams"}),
        ("sdv_team_xwalk", {"xwalk_keys", "espn_team_ids"}),
        ("sdv_game_xwalk", {"matchup_date_keys", "espn_game_ids"}),
    ],
)
def test_download_uses_source_parser_and_source_specific_summary(monkeypatch, source, summary_keys):
    fixture = Path(__file__).parent / f"fixtures/flatfiles/{source}_sample.parquet"
    raw = fixture.read_bytes()
    sha = hashlib.sha256(raw).hexdigest()

    def fetch(url, *, timeout, retries):
        return FetchedFile(raw, sha, url)

    monkeypatch.setattr(canary, "fetch_file", fetch)
    artifact = canary._download_and_validate(source, 2025, sha)
    evidence = artifact.evidence()

    assert artifact.rows == pq.read_metadata(fixture).num_rows
    assert summary_keys <= evidence.keys()
    assert "weeks" not in evidence
    if source.endswith("xwalk"):
        assert evidence["publication_artifact_origin"] == "local_file"
        assert evidence["season_basis"] == "caller_declared"
        assert evidence["registered_url"].endswith("_2025.parquet")
        assert evidence["registered_file_name"].endswith("_2025.parquet")


def test_publish_pins_verified_bytes_uses_same_dsn_and_cleans_up(monkeypatch):
    artifact = canary.Artifact(b"exact reviewed bytes", EXPECTED_SHA, 2, (0, 1))
    observed = {}
    original_resolver = canary.publication.get_db_url

    def publish(spec, *, file_path, season):
        path = Path(file_path)
        observed.update(
            source=spec.name,
            season=season,
            bytes=path.read_bytes(),
            path=path,
            dsn=canary.publication.get_db_url(),
        )
        return {
            "status": "loaded",
            "sha": EXPECTED_SHA,
            "rows": 2,
            "run_id": "run",
            "generation_id": "generation",
            "duration_s": 1.25,
        }

    monkeypatch.setattr(canary.publication, "run_source_publication", publish)
    result = canary._publish_pinned("inspected-dsn", "sdv_fpi_weekly", 2026, artifact)

    assert result["status"] == "loaded"
    assert observed == {
        "source": "sdv_fpi_weekly",
        "season": 2026,
        "bytes": b"exact reviewed bytes",
        "path": observed["path"],
        "dsn": "inspected-dsn",
    }
    assert observed["path"].exists() is False
    assert canary.publication.get_db_url is original_resolver


def test_ratings_publish_pins_both_resolvers_and_restores_after_failure(monkeypatch):
    artifact = canary.Artifact(b"exact reviewed bytes", EXPECTED_SHA, 2)
    original_batch = canary.publication.get_db_url
    original_ratings = canary.sdv_ratings_publication.get_db_url
    observed = {}

    def publish(spec, *, file_path, season):
        observed.update(
            source=spec.name,
            batch_dsn=canary.publication.get_db_url(),
            ratings_dsn=canary.sdv_ratings_publication.get_db_url(),
            bytes=Path(file_path).read_bytes(),
        )
        raise canary.sdv_ratings_publication.SourcePublicationError("bounded failure")

    monkeypatch.setattr(
        canary.sdv_ratings_publication,
        "get_db_url",
        lambda: "adversarial-ambient-ratings-dsn",
    )
    adversarial_resolver = canary.sdv_ratings_publication.get_db_url
    monkeypatch.setattr(
        canary.sdv_ratings_publication,
        "run_sdv_ratings_publication",
        publish,
    )

    with pytest.raises(canary.publication.SourcePublicationError, match="bounded failure"):
        canary._publish_pinned("inspected-dsn", "sdv_ratings_weekly", 2026, artifact)

    assert observed == {
        "source": "sdv_ratings_weekly",
        "batch_dsn": "inspected-dsn",
        "ratings_dsn": "inspected-dsn",
        "bytes": b"exact reviewed bytes",
    }
    assert canary.publication.get_db_url is original_batch
    assert canary.sdv_ratings_publication.get_db_url is adversarial_resolver
    assert original_ratings is not adversarial_resolver


def test_publish_output_marks_attempt_immediately_before_publication(monkeypatch, tmp_path):
    output = tmp_path / "github-output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setattr(canary.publication, "get_db_url", lambda: "dsn")
    monkeypatch.setattr(
        canary,
        "_inspect_target",
        lambda *values: ({"can_set_role": True, "activation_required": False}, source_plan()),
    )
    monkeypatch.setattr(
        canary,
        "_download_and_validate",
        lambda *values: canary.Artifact(b"pinned", EXPECTED_SHA, 2, (0, 1)),
    )

    def publish(*values):
        assert output.read_text().splitlines() == [
            "publication_attempted=false",
            "publication_source=sdv_fpi_weekly",
            "publication_attempted=true",
        ]
        return {
            "status": "loaded",
            "sha": EXPECTED_SHA,
            "rows": 2,
            "run_id": "run",
            "generation_id": "generation",
        }

    monkeypatch.setattr(canary, "_publish_pinned", publish)

    assert canary.main(cli_args("publish")) == 0


def test_failed_or_uncertain_publication_is_not_retried(monkeypatch):
    artifact = canary.Artifact(b"pinned", EXPECTED_SHA, 2, (0, 1))
    calls = []
    monkeypatch.setattr(canary.publication, "get_db_url", lambda: "dsn")
    monkeypatch.setattr(
        canary,
        "_inspect_target",
        lambda *values: ({"can_set_role": True, "activation_required": False}, source_plan()),
    )
    monkeypatch.setattr(canary, "_download_and_validate", lambda *values: artifact)

    def publish(*values):
        calls.append(values)
        return {
            "status": "failed",
            "error": "source publication RPC commit outcome is uncertain",
            "run_id": "run",
            "generation_id": "generation",
            "sha": EXPECTED_SHA,
            "rows": 0,
            "duration_s": 0.5,
        }

    monkeypatch.setattr(canary, "_publish_pinned", publish)
    payload, status = canary.run_canary(args("publish"))

    assert status == 1
    assert payload["status"] == "failed"
    assert payload["publication"]["run_id"] == "run"
    assert len(calls) == 1


@pytest.mark.parametrize(
    "argv",
    [
        [
            "preflight",
            "--source",
            "sdv_fpi_weekly",
            "--source",
            "sdv_ratings_weekly",
            "--season",
            "2026",
            "--expected-sha256",
            EXPECTED_SHA,
            "--expected-project-ref",
            PROJECT_REF,
        ],
        [
            "preflight",
            "--source",
            "sdv_fpi_weekly",
            "--season",
            "2026 2025",
            "--expected-sha256",
            EXPECTED_SHA,
            "--expected-project-ref",
            PROJECT_REF,
        ],
        [
            "publish",
            "--source",
            "sdv_fpi_weekly",
            "--season",
            "2026",
            "--expected-sha256",
            EXPECTED_SHA.upper(),
            "--expected-project-ref",
            PROJECT_REF,
        ],
    ],
)
def test_cli_rejects_any_source_set_season_set_or_unpinned_sha(argv):
    with pytest.raises(SystemExit, match="2"):
        canary.build_parser().parse_args(argv)


@pytest.mark.parametrize("source", canary.SOURCES)
def test_cli_accepts_each_enrolled_source_with_one_explicit_scope(source):
    parsed = canary.build_parser().parse_args(
        [
            "preflight",
            "--source",
            source,
            "--season",
            "2026",
            "--expected-sha256",
            EXPECTED_SHA,
            "--expected-project-ref",
            PROJECT_REF,
        ]
    )
    assert parsed.source == source
