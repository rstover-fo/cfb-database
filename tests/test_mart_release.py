"""Unit tests for the bounded dependency-aware mart release engine."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.mart_release import (
    ReleaseBlockedError,
    ReleaseExecutionError,
    ReleaseManifestError,
    execute_release,
    load_release,
    plan_release,
)


def _write_release(
    root: Path,
    sql: str,
    *,
    restores: list[dict[str, str]] | None = None,
    consumers: list[str] | None = None,
    validations: list[dict[str, object]] | None = None,
):
    root.mkdir(parents=True, exist_ok=True)
    sql_path = root / "release.sql"
    sql_path.write_bytes(sql.encode())
    manifest_path = root / "release.json"
    payload = {
        "version": 1,
        "files": [
            {
                "path": "release.sql",
                "sha256": hashlib.sha256(sql_path.read_bytes()).hexdigest(),
            }
        ],
        "roots": ["marts.parent"],
        "restores": restores
        or [
            {"kind": "relation", "identity": "marts.parent"},
            {"kind": "relation", "identity": "api.consumer"},
        ],
        "validations": validations
        or [
            {
                "name": "anon consumer",
                "role": "anon",
                "query": "SELECT true",
                "covers": [],
            }
        ],
    }
    if consumers is not None:
        payload["consumers"] = consumers
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    return load_release(manifest_path, root)


def _snapshot_rows(
    *,
    changed_definition: bool = False,
    parent_populated: bool = True,
    function_consumer: bool = False,
):
    # kind, identity, object type, owner, mutable implementation definition, then the fourteen
    # contract fields selected by _SNAPSHOT_SQL.
    parent_definition = "SELECT 2" if changed_definition else "SELECT 1"
    rows = [
        (
            "relation",
            "api.consumer",
            "v",
            "owner",
            "SELECT * FROM marts.parent",
            "v",
            "true",
            "",
            "d",
            "false",
            "false",
            "",
            "1:id:integer:false:::data::",
            "",
            "anon:true:false:false",
            "",
            "",
            "",
            "",
        ),
        (
            "relation",
            "marts.parent",
            "m",
            "owner",
            parent_definition,
            "m",
            str(parent_populated).lower(),
            "",
            "d",
            "false",
            "false",
            "",
            "1:id:integer:false:::data::",
            "parent_id=CREATE UNIQUE INDEX parent_id ON marts.parent USING btree (id)",
            "anon:false:false:false",
            "",
            "",
            "",
            "",
        ),
    ]
    if function_consumer:
        rows.append(
            (
                "function",
                "public.dynamic_consumer(integer)",
                "function",
                "owner",
                "CREATE FUNCTION public.dynamic_consumer(integer) RETURNS boolean ...",
                "f",
                "false",
                "",
                "v",
                "u",
                "false",
                "false",
                "boolean",
                "integer",
                "",
                "anon:true",
                "",
                "",
                "",
            )
        )
    return rows


def _acl_rows():
    return [
        ("relation", "api.consumer", "owner", "owner", "owner", "SELECT", False),
        ("relation", "api.consumer", "owner", "owner", "anon", "SELECT", False),
        ("relation", "marts.parent", "owner", "owner", "owner", "SELECT", False),
    ]


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.results = []
        self.closed = False

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        self.conn.executions.append((normalized, params))
        if "mart_release:roots" in sql:
            self.results = [(42, "marts.parent", "m")]
        elif "mart_release:closure" in sql:
            self.results = list(self.conn.closure)
        elif "mart_release:snapshot" in sql:
            self.results = _snapshot_rows(
                changed_definition=self.conn.release_ran,
                parent_populated=not (self.conn.release_ran and self.conn.unpopulate_after_release),
                function_consumer=self.conn.function_consumer,
            )
        elif "mart_release:acl" in sql:
            self.results = _acl_rows()
        elif sql == "SELECT true":
            self.results = [(self.conn.validation_result,)]
        else:
            self.results = []
        if "RELEASE BODY" in sql:
            self.conn.release_ran = True
        if "INJECT FAILURE" in sql:
            raise ReleaseExecutionError("injected failure")

    def fetchall(self):
        rows, self.results = self.results, []
        return rows

    def fetchone(self):
        return self.results.pop(0) if self.results else None

    def close(self):
        self.closed = True


class FakeConnection:
    autocommit = False

    def __init__(
        self,
        *,
        closure=None,
        unpopulate_after_release: bool = False,
        function_consumer: bool = False,
        validation_result=True,
    ):
        self.closure = closure or [
            ("object", "relation", "api.consumer", None, "v"),
            ("object", "relation", "marts.parent", None, "m"),
        ]
        self.executions = []
        self.commits = 0
        self.rollbacks = 0
        self.release_ran = False
        self.unpopulate_after_release = unpopulate_after_release
        self.function_consumer = function_consumer
        self.validation_result = validation_result
        self.cursor_instance = None

    def get_transaction_status(self):
        return 0

    def cursor(self):
        self.cursor_instance = FakeCursor(self)
        return self.cursor_instance

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_load_release_hashes_exact_bytes_and_preserves_file_order(tmp_path: Path):
    release = _write_release(tmp_path, "SELECT 1;\r\n")

    assert release.files[0].sha256 == hashlib.sha256(b"SELECT 1;\r\n").hexdigest()
    assert release.files[0].path == "release.sql"
    assert release.roots == ("marts.parent",)
    assert release.consumers == ()


def test_load_release_rejects_hash_mismatch_and_nontransactional_sql(tmp_path: Path):
    _write_release(tmp_path / "hash", "SELECT 1;")
    manifest_path = tmp_path / "hash" / "release.json"
    payload = json.loads(manifest_path.read_text())
    payload["files"][0]["sha256"] = "0" * 64
    manifest_path.write_text(json.dumps(payload))
    with pytest.raises(ReleaseManifestError, match="checksum mismatch"):
        load_release(manifest_path, tmp_path / "hash")

    with pytest.raises(ReleaseManifestError, match="nontransactional"):
        _write_release(tmp_path / "ddl", "CREATE INDEX CONCURRENTLY x ON y(z);")


@pytest.mark.parametrize(
    "sql",
    [
        'SET "standard_conforming_strings" = off;',
        "RESET ALL;",
        "SET statement_timeout = '30min';",
    ],
)
def test_load_release_rejects_top_level_guc_changes(tmp_path: Path, sql: str):
    with pytest.raises(ReleaseManifestError, match="unsafe SQL"):
        _write_release(tmp_path, sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT set_config('statement_timeout', '0', false);",
        "SELECT pg_catalog.\"set_config\"('lock_timeout', '0', false);",
        "SELECT \"pg_catalog\".\"set_config\"('statement_timeout', '0', false);",
    ],
)
def test_load_release_rejects_direct_set_config_calls(tmp_path: Path, sql: str):
    with pytest.raises(ReleaseManifestError, match="may not call set_config"):
        _write_release(tmp_path, sql)


def test_set_config_text_in_inert_regions_is_allowed(tmp_path: Path):
    release = _write_release(
        tmp_path,
        "SELECT 'set_config('';' AS text; /* set_config(...) */",
    )
    assert release.files[0].sql.startswith("SELECT")


def test_unicode_escaped_configuration_call_is_rejected(tmp_path: Path):
    with pytest.raises(ReleaseManifestError, match="Unicode-escaped identifiers"):
        _write_release(
            tmp_path,
            r"""SELECT pg_catalog.U&"set\005fconfig"('statement_timeout', '0', false);""",
        )


def test_plan_fails_closed_on_unsupported_catalog_objects(tmp_path: Path):
    release = _write_release(tmp_path, "SELECT 1;")
    conn = FakeConnection(
        closure=[
            ("object", "relation", "api.consumer", None, "v"),
            ("object", "relation", "marts.parent", None, "m"),
            ("unsupported", "pg_class", "73", None, "catalog class"),
        ]
    )

    plan = plan_release(conn, release)

    assert not plan.valid
    assert "unsupported downstream catalog object" in " ".join(plan.blockers)


def test_declared_dynamic_consumer_must_exist_and_have_validation_coverage(tmp_path: Path):
    consumer = "public.dynamic_consumer(integer)"
    validation = {
        "name": "dynamic consumer",
        "role": "anon",
        "query": "SELECT true",
        "covers": [consumer],
    }
    release = _write_release(
        tmp_path / "existing",
        "SELECT 1;",
        consumers=[consumer],
        validations=[validation],
    )
    assert plan_release(FakeConnection(function_consumer=True), release).valid

    missing = plan_release(FakeConnection(), release)
    assert not missing.valid
    assert "consumer functions do not exist" in " ".join(missing.blockers)

    uncovered = _write_release(
        tmp_path / "uncovered",
        "SELECT 1;",
        consumers=[consumer],
        validations=[{**validation, "covers": []}],
    )
    plan = plan_release(FakeConnection(function_consumer=True), uncovered)
    assert not plan.valid
    assert "require caller-role validations" in " ".join(plan.blockers)


def test_plan_is_read_only_and_reports_incomplete_live_closure(tmp_path: Path):
    release = _write_release(
        tmp_path,
        "SELECT 1;",
        restores=[{"kind": "relation", "identity": "marts.parent"}],
    )
    conn = FakeConnection()

    plan = plan_release(conn, release)

    assert not plan.valid
    assert "api.consumer" in " ".join(plan.blockers)
    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert any(
        sql.startswith("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        for sql, _ in conn.executions
    )


def test_execute_rejects_incomplete_closure_before_release_sql(tmp_path: Path):
    release = _write_release(
        tmp_path,
        "/* RELEASE BODY */ SELECT 1;",
        restores=[{"kind": "relation", "identity": "marts.parent"}],
    )
    conn = FakeConnection()

    with pytest.raises(ReleaseBlockedError):
        execute_release(conn, release)

    assert not conn.release_ran
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_execute_rolls_back_file_failure(tmp_path: Path):
    release = _write_release(tmp_path, "/* RELEASE BODY INJECT FAILURE */ SELECT 1;")
    conn = FakeConnection()

    with pytest.raises(ReleaseExecutionError, match="injected"):
        execute_release(conn, release)

    assert conn.release_ran
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_execute_allows_definition_change_with_same_contract_and_rolls_back_validation_writes(
    tmp_path: Path,
):
    release = _write_release(tmp_path, "/* RELEASE BODY */ SELECT 1;")
    conn = FakeConnection()

    plan = execute_release(conn, release)

    assert plan.valid
    assert conn.commits == 1
    assert conn.rollbacks == 0
    queries = [sql for sql, _ in conn.executions]
    assert "ROLLBACK TO SAVEPOINT mart_release_validation" in queries
    assert "RELEASE SAVEPOINT mart_release_validation" in queries


def test_execute_rejects_recreated_unpopulated_materialized_view(tmp_path: Path):
    release = _write_release(tmp_path, "/* RELEASE BODY */ SELECT 1;")
    conn = FakeConnection(unpopulate_after_release=True)

    with pytest.raises(ReleaseExecutionError, match="changed an existing object contract"):
        execute_release(conn, release)

    assert conn.commits == 0
    assert conn.rollbacks == 1


@pytest.mark.parametrize("result", [1, 1.0, "true", None])
def test_validation_requires_literal_boolean_true(tmp_path: Path, result):
    release = _write_release(tmp_path, "/* RELEASE BODY */ SELECT 1;")
    conn = FakeConnection(validation_result=result)

    with pytest.raises(ReleaseExecutionError, match="exactly one true boolean"):
        execute_release(conn, release)

    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_validation_rejects_direct_set_config_call(tmp_path: Path):
    with pytest.raises(ReleaseManifestError, match="may not call set_config"):
        _write_release(
            tmp_path,
            "SELECT 1;",
            validations=[
                {
                    "name": "timeout bypass",
                    "role": "anon",
                    "query": "SELECT pg_catalog.set_config('statement_timeout', '0', false)",
                    "covers": [],
                }
            ],
        )
