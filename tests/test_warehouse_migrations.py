"""Unit tests for the bounded warehouse migration ledger (no live database)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.warehouse_migrations import (
    ADVISORY_LOCK_KEYS,
    PROJECT_SCHEMAS,
    ManifestError,
    MigrationStateError,
    apply_manifest,
    load_manifest,
)


class FakeCursor:
    def __init__(self, conn: FakeConnection):
        self.conn = conn
        self.results: list[tuple[object, ...]] = []
        self.closed = False

    def execute(self, sql: str, params=None) -> None:
        normalized = " ".join(sql.split())
        self.conn.executions.append((normalized, params))
        if self.conn.fail_on and self.conn.fail_on in sql:
            raise RuntimeError("injected migration failure")
        if "to_regclass('warehouse_control.schema_migrations')" in sql:
            self.results = [(self.conn.ledger_installed,)]
        elif "FROM pg_namespace" in sql:
            self.results = [(schema,) for schema in self.conn.project_schemas]
        elif "AS public_objects" in sql:
            self.results = [(name,) for name in self.conn.public_objects]
        elif "FROM warehouse_control.schema_migrations" in sql and normalized.startswith("SELECT"):
            self.results = list(self.conn.applied)
        else:
            self.results = []

    def fetchone(self):
        return self.results[0] if self.results else None

    def fetchall(self):
        return self.results

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(
        self,
        *,
        ledger_installed: bool = False,
        project_schemas: tuple[str, ...] = (),
        public_objects: tuple[str, ...] = (),
        applied: tuple[tuple[object, ...], ...] = (),
        fail_on: str | None = None,
        transaction_status: int = 0,
        autocommit: bool = False,
    ):
        self.ledger_installed = ledger_installed
        self.project_schemas = project_schemas
        self.public_objects = public_objects
        self.applied = applied
        self.fail_on = fail_on
        self.transaction_status = transaction_status
        self.autocommit = autocommit
        self.executions: list[tuple[str, object]] = []
        self.commits = 0
        self.rollbacks = 0
        self.last_cursor: FakeCursor | None = None

    def get_transaction_status(self) -> int:
        return self.transaction_status

    def cursor(self) -> FakeCursor:
        self.last_cursor = FakeCursor(self)
        return self.last_cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def _write_manifest(
    root: Path,
    entries: list[dict[str, str]],
    *,
    name: str = "manifest.json",
) -> Path:
    path = root / name
    path.write_text(json.dumps({"version": 1, "migrations": entries}), encoding="utf-8")
    return path


def _manifest(root: Path, *, second_kind: str = "repeatable"):
    (root / "sql").mkdir()
    (root / "sql" / "001.sql").write_bytes(b"CREATE TABLE one(id int);\r\n")
    (root / "sql" / "002.sql").write_text("CREATE VIEW two AS SELECT 2;\n", encoding="utf-8")
    path = _write_manifest(
        root,
        [
            {"id": "v1", "path": "sql/001.sql", "kind": "immutable"},
            {"id": "v2", "path": "sql/002.sql", "kind": second_kind},
        ],
    )
    return load_manifest(path, root)


def _queries(conn: FakeConnection) -> list[str]:
    return [sql for sql, _ in conn.executions]


def test_load_manifest_hashes_exact_bytes_and_preserves_order(tmp_path: Path):
    manifest = _manifest(tmp_path)

    assert [migration.id for migration in manifest.migrations] == ["v1", "v2"]
    assert (
        manifest.migrations[0].checksum
        == hashlib.sha256(b"CREATE TABLE one(id int);\r\n").hexdigest()
    )
    assert manifest.migrations[0].path == "sql/001.sql"


def test_bootstrap_schema_guard_includes_live_sources_and_all_staging_variants():
    bases = {
        "analytics",
        "api",
        "betting",
        "core",
        "draft",
        "features",
        "live",
        "marts",
        "meta",
        "metrics",
        "ncaa",
        "pff",
        "predictions",
        "public",
        "ratings",
        "raw",
        "recruiting",
        "ref",
        "scouting",
        "stats",
    }
    guarded = set(PROJECT_SCHEMAS)
    assert bases - {"public"} <= guarded
    assert {f"{base}_staging" for base in bases} <= guarded


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda entries: entries.append(entries[0].copy()), "duplicate migration id"),
        (
            lambda entries: entries.append(
                {"id": "other", "path": entries[0]["path"], "kind": "immutable"}
            ),
            "duplicate migration path",
        ),
        (lambda entries: entries[0].update(kind="mutable"), "invalid kind"),
        (lambda entries: entries[0].update(extra="field"), "keys must be exactly"),
    ],
)
def test_load_manifest_rejects_invalid_entries(tmp_path: Path, mutation, message: str):
    (tmp_path / "one.sql").write_text("SELECT 1;", encoding="utf-8")
    entries = [{"id": "v1", "path": "one.sql", "kind": "immutable"}]
    mutation(entries)
    manifest_path = _write_manifest(tmp_path, entries)

    with pytest.raises(ManifestError, match=message):
        load_manifest(manifest_path, tmp_path)


@pytest.mark.parametrize(
    "bad_path",
    ["../outside.sql", "/tmp/outside.sql", "dir\\file.sql", "dir//file.sql", "dir/./file.sql"],
)
def test_load_manifest_rejects_traversal_and_non_posix_paths(tmp_path: Path, bad_path: str):
    manifest_path = _write_manifest(
        tmp_path,
        [{"id": "v1", "path": bad_path, "kind": "immutable"}],
    )

    with pytest.raises(ManifestError, match="path"):
        load_manifest(manifest_path, tmp_path)


def test_load_manifest_rejects_duplicate_json_keys(tmp_path: Path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text('{"version":1,"version":1,"migrations":[]}', encoding="utf-8")

    with pytest.raises(ManifestError, match="duplicate JSON key"):
        load_manifest(manifest_path, tmp_path)


def test_load_manifest_rejects_two_symlink_paths_to_same_file(tmp_path: Path):
    (tmp_path / "real.sql").write_text("SELECT 1;", encoding="utf-8")
    (tmp_path / "alias.sql").symlink_to(tmp_path / "real.sql")
    manifest_path = _write_manifest(
        tmp_path,
        [
            {"id": "v1", "path": "real.sql", "kind": "immutable"},
            {"id": "v2", "path": "alias.sql", "kind": "immutable"},
        ],
    )

    with pytest.raises(ManifestError, match="duplicate resolved migration path"):
        load_manifest(manifest_path, tmp_path)


@pytest.mark.parametrize(
    "control",
    [
        "BEGIN; SELECT 1;",
        "COMMIT;",
        "START TRANSACTION;",
        "ROLLBACK TO SAVEPOINT x;",
        "PREPARE TRANSACTION 'x';",
        "SET TRANSACTION READ ONLY;",
    ],
)
def test_load_manifest_rejects_top_level_transaction_control(tmp_path: Path, control: str):
    (tmp_path / "one.sql").write_text(control, encoding="utf-8")
    manifest_path = _write_manifest(
        tmp_path,
        [{"id": "v1", "path": "one.sql", "kind": "immutable"}],
    )

    with pytest.raises(ManifestError, match="transaction control"):
        load_manifest(manifest_path, tmp_path)


def test_transaction_words_inside_comments_strings_and_do_blocks_are_allowed(tmp_path: Path):
    sql = """
    -- COMMIT;
    SELECT 'ROLLBACK', $$ BEGIN COMMIT; END $$;
    DO $body$ BEGIN RAISE NOTICE 'COMMIT'; END $body$;
    /* START TRANSACTION; /* nested COMMIT; */ */
    """
    (tmp_path / "one.sql").write_text(sql, encoding="utf-8")
    manifest_path = _write_manifest(
        tmp_path,
        [{"id": "v1", "path": "one.sql", "kind": "immutable"}],
    )

    assert load_manifest(manifest_path, tmp_path).migrations[0].sql == sql


def test_ordinary_backslash_does_not_hide_later_transaction_control(tmp_path: Path):
    # With the standard-conforming strings setting pinned by apply_manifest, the backslash is a
    # literal and the first quote closes the string. COMMIT must therefore be seen as top-level.
    (tmp_path / "one.sql").write_text(
        "SELECT 'C:\\'; COMMIT; CREATE TABLE public.after_commit(id int);",
        encoding="utf-8",
    )
    manifest_path = _write_manifest(
        tmp_path,
        [{"id": "v1", "path": "one.sql", "kind": "immutable"}],
    )

    with pytest.raises(ManifestError, match="transaction control"):
        load_manifest(manifest_path, tmp_path)


def test_escape_string_backslash_quote_is_scanned_correctly(tmp_path: Path):
    (tmp_path / "one.sql").write_text("SELECT E'C:\\'drive'; SELECT 1;", encoding="utf-8")
    manifest_path = _write_manifest(
        tmp_path,
        [{"id": "v1", "path": "one.sql", "kind": "immutable"}],
    )

    assert load_manifest(manifest_path, tmp_path).migrations[0].id == "v1"


def test_existing_064_do_blocks_pass_transaction_scanner(tmp_path: Path):
    repo_root = Path(__file__).parents[1]
    manifest_path = _write_manifest(
        tmp_path,
        [
            {
                "id": "v2",
                "path": "src/schemas/migrations/064_immutable_training_fits.sql",
                "kind": "immutable",
            }
        ],
    )

    loaded = load_manifest(manifest_path, repo_root)
    assert loaded.migrations[0].checksum


def test_bootstrap_dry_run_is_locked_read_only_and_does_not_create_ledger(tmp_path: Path):
    manifest = _manifest(tmp_path)
    conn = FakeConnection()

    plan = apply_manifest(conn, manifest, mode="bootstrap", dry_run=True)

    assert plan.valid
    assert [step.action for step in plan.steps] == ["apply", "apply"]
    assert _queries(conn)[0] == "BEGIN READ ONLY"
    assert conn.executions[2][1] == ADVISORY_LOCK_KEYS
    assert not any(sql.startswith("CREATE SCHEMA warehouse_control") for sql in _queries(conn))
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_bootstrap_rejects_existing_project_schema_without_writes(tmp_path: Path):
    manifest = _manifest(tmp_path)
    conn = FakeConnection(project_schemas=("analytics",))

    with pytest.raises(MigrationStateError, match="analytics"):
        apply_manifest(conn, manifest, mode="bootstrap")

    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert not any("CREATE TABLE one" in sql for sql in _queries(conn))


def test_bootstrap_rejects_non_extension_public_objects(tmp_path: Path):
    manifest = _manifest(tmp_path)
    conn = FakeConnection(public_objects=("public.legacy_table", "public.legacy_fn()"))

    with pytest.raises(MigrationStateError, match="public.legacy_table"):
        apply_manifest(conn, manifest, mode="bootstrap")

    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_bootstrap_creates_private_ledger_and_applies_all_in_one_transaction(tmp_path: Path):
    manifest = _manifest(tmp_path)
    conn = FakeConnection()

    plan = apply_manifest(conn, manifest, mode="bootstrap")

    queries = _queries(conn)
    assert plan.valid
    assert queries[0] == "BEGIN"
    assert "CREATE SCHEMA warehouse_control" in "\n".join(queries)
    assert "REVOKE ALL ON SCHEMA warehouse_control FROM PUBLIC" in "\n".join(queries)
    assert queries.index("CREATE TABLE one(id int);") < next(
        index
        for index, query in enumerate(queries)
        if query.startswith("INSERT INTO warehouse_control.schema_migrations")
    )
    assert any(
        query.startswith("INSERT INTO warehouse_control.repeatable_migration_executions")
        for query in queries
    )
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert conn.last_cursor and conn.last_cursor.closed


def test_bootstrap_is_rerunnable_when_ledger_is_already_complete(tmp_path: Path):
    manifest = _manifest(tmp_path)
    applied = tuple(
        (migration.id, migration.path, migration.kind, migration.checksum, order)
        for order, migration in enumerate(manifest.migrations, start=1)
    )
    conn = FakeConnection(
        ledger_installed=True,
        project_schemas=("warehouse_control", "analytics"),
        applied=applied,
    )

    plan = apply_manifest(conn, manifest, mode="bootstrap")

    assert plan.valid
    assert [step.action for step in plan.steps] == ["skip", "skip"]
    assert not any("FROM pg_namespace" in sql for sql in _queries(conn))
    assert conn.commits == 1


def test_upgrade_refuses_unledgered_database(tmp_path: Path):
    manifest = _manifest(tmp_path)
    conn = FakeConnection()

    with pytest.raises(MigrationStateError, match="not adopted"):
        apply_manifest(conn, manifest, mode="upgrade")

    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_unchanged_migrations_skip_and_changed_repeatable_reapplies(tmp_path: Path):
    manifest = _manifest(tmp_path)
    first, second = manifest.migrations
    conn = FakeConnection(
        ledger_installed=True,
        applied=(
            (first.id, first.path, first.kind, first.checksum, 1),
            (second.id, second.path, second.kind, "0" * 64, 2),
        ),
    )

    plan = apply_manifest(conn, manifest, mode="upgrade")

    assert [step.action for step in plan.steps] == ["skip", "reapply"]
    assert not any("CREATE TABLE one" in query for query in _queries(conn))
    assert any("CREATE VIEW two" in query for query in _queries(conn))
    assert any(
        query.startswith("UPDATE warehouse_control.schema_migrations") for query in _queries(conn)
    )
    assert any("repeatable_migration_executions" in query for query in _queries(conn))
    assert conn.commits == 1


def test_forward_immutables_run_before_changed_repeatables(tmp_path: Path):
    (tmp_path / "sql").mkdir()
    (tmp_path / "sql" / "001.sql").write_text("CREATE TABLE source(a int);", encoding="utf-8")
    (tmp_path / "sql" / "002.sql").write_text(
        "CREATE OR REPLACE VIEW current_source AS SELECT a, b FROM source;",
        encoding="utf-8",
    )
    (tmp_path / "sql" / "003.sql").write_text(
        "ALTER TABLE source ADD COLUMN b int;", encoding="utf-8"
    )
    manifest_path = _write_manifest(
        tmp_path,
        [
            {"id": "base", "path": "sql/001.sql", "kind": "immutable"},
            {"id": "view", "path": "sql/002.sql", "kind": "repeatable"},
            {"id": "column", "path": "sql/003.sql", "kind": "immutable"},
        ],
    )
    manifest = load_manifest(manifest_path, tmp_path)
    base, view, _ = manifest.migrations
    conn = FakeConnection(
        ledger_installed=True,
        applied=(
            (base.id, base.path, base.kind, base.checksum, 1),
            (view.id, view.path, view.kind, "0" * 64, 2),
        ),
    )

    apply_manifest(conn, manifest, mode="upgrade")

    queries = _queries(conn)
    assert queries.index("ALTER TABLE source ADD COLUMN b int;") < queries.index(
        "CREATE OR REPLACE VIEW current_source AS SELECT a, b FROM source;"
    )


def test_changed_immutable_is_visible_in_plan_and_rejected_on_execute(tmp_path: Path):
    manifest = _manifest(tmp_path)
    first = manifest.migrations[0]
    applied = ((first.id, first.path, first.kind, "0" * 64, 1),)

    dry_conn = FakeConnection(ledger_installed=True, applied=applied)
    plan = apply_manifest(dry_conn, manifest, mode="plan")
    assert not plan.valid
    assert "checksum changed" in plan.diagnostics[0]
    assert dry_conn.commits == 0
    assert dry_conn.rollbacks == 1

    write_conn = FakeConnection(ledger_installed=True, applied=applied)
    with pytest.raises(MigrationStateError) as exc_info:
        apply_manifest(write_conn, manifest, mode="upgrade")
    assert exc_info.value.plan == plan.__class__(
        mode="upgrade",
        target=None,
        ledger_installed=plan.ledger_installed,
        steps=plan.steps,
        diagnostics=plan.diagnostics,
    )
    assert write_conn.commits == 0


@pytest.mark.parametrize(
    ("applied", "message"),
    [
        (("v2", "sql/002.sql", "repeatable", "x", 1), "prefix changed"),
        (("v1", "sql/001.sql", "immutable", "x", 2), "order gap"),
        (("v1", "renamed.sql", "immutable", "x", 1), "path changed"),
    ],
)
def test_applied_prefix_edits_and_gaps_are_rejected(
    tmp_path: Path, applied: tuple[object, ...], message: str
):
    manifest = _manifest(tmp_path)
    conn = FakeConnection(ledger_installed=True, applied=(applied,))

    plan = apply_manifest(conn, manifest, mode="status")

    assert not plan.valid
    assert any(message in diagnostic for diagnostic in plan.diagnostics)


def test_removed_applied_migration_is_rejected(tmp_path: Path):
    manifest = _manifest(tmp_path)
    first, second = manifest.migrations
    conn = FakeConnection(
        ledger_installed=True,
        applied=(
            (first.id, first.path, first.kind, first.checksum, 1),
            (second.id, second.path, second.kind, second.checksum, 2),
            ("v3", "sql/003.sql", "immutable", "f" * 64, 3),
        ),
    )

    plan = apply_manifest(conn, manifest, mode="status")

    assert not plan.valid
    assert "removed" in plan.diagnostics[0]


def test_target_applies_inclusive_prefix_and_rejects_backwards_target(tmp_path: Path):
    manifest = _manifest(tmp_path)
    bootstrap_conn = FakeConnection()

    plan = apply_manifest(bootstrap_conn, manifest, mode="bootstrap", target="v1")

    assert [step.action for step in plan.steps] == ["apply", "deferred"]
    assert not any("CREATE VIEW two" in query for query in _queries(bootstrap_conn))

    first, second = manifest.migrations
    applied = (
        (first.id, first.path, first.kind, first.checksum, 1),
        (second.id, second.path, second.kind, second.checksum, 2),
    )
    backwards_conn = FakeConnection(ledger_installed=True, applied=applied)
    backwards = apply_manifest(backwards_conn, manifest, mode="plan", target="v1")
    assert not backwards.valid
    assert any("behind" in diagnostic for diagnostic in backwards.diagnostics)


def test_migration_failure_rolls_back_entire_batch(tmp_path: Path):
    manifest = _manifest(tmp_path)
    conn = FakeConnection(fail_on="CREATE VIEW two")

    with pytest.raises(RuntimeError, match="injected"):
        apply_manifest(conn, manifest, mode="bootstrap")

    assert any("CREATE TABLE one" in query for query in _queries(conn))
    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert conn.last_cursor and conn.last_cursor.closed


def test_connection_must_be_idle_before_engine_takes_transaction_ownership(tmp_path: Path):
    manifest = _manifest(tmp_path)
    conn = FakeConnection(transaction_status=2)

    with pytest.raises(RuntimeError, match="must not already be in a transaction"):
        apply_manifest(conn, manifest, mode="plan")

    assert conn.executions == []
    assert conn.rollbacks == 0


def test_autocommit_connection_is_rejected_before_database_work(tmp_path: Path):
    manifest = _manifest(tmp_path)
    conn = FakeConnection(autocommit=True)

    with pytest.raises(RuntimeError, match="autocommit disabled"):
        apply_manifest(conn, manifest, mode="plan")

    assert conn.executions == []
