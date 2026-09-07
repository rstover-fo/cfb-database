"""Bounded, transactional warehouse migration ledger.

The module deliberately contains no credential discovery or command-line entry point.  Callers
must supply an existing PostgreSQL connection and an explicit operation mode.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal

MigrationKind = Literal["immutable", "repeatable"]
MigrationMode = Literal["bootstrap", "upgrade", "status", "plan"]
MigrationAction = Literal["apply", "reapply", "skip", "deferred"]

VALID_KINDS = frozenset({"immutable", "repeatable"})
VALID_MODES = frozenset({"bootstrap", "upgrade", "status", "plan"})

# A repository-specific, stable two-key advisory lock.  Transaction-scoped locking means a
# rollback releases it together with every DDL and ledger change from the attempted batch.
ADVISORY_LOCK_KEYS = (1_128_677_364, 70_654)

# Supabase installations have several service-owned schemas.  Bootstrap only guards namespaces
# owned by this repository, including a partially-created control schema.
PROJECT_SCHEMAS = (
    "analytics",
    "analytics_staging",
    "api",
    "api_staging",
    "betting",
    "betting_staging",
    "core",
    "core_staging",
    "draft",
    "draft_staging",
    "features",
    "features_staging",
    "live",
    "live_staging",
    "marts",
    "marts_staging",
    "meta",
    "meta_staging",
    "metrics",
    "metrics_staging",
    "ncaa",
    "ncaa_staging",
    "pff",
    "pff_staging",
    "predictions",
    "predictions_staging",
    "public_staging",
    "raw",
    "raw_staging",
    "rp",
    "rp_staging",
    "ratings",
    "ratings_staging",
    "recruiting",
    "recruiting_staging",
    "ref",
    "ref_staging",
    "scouting",
    "scouting_staging",
    "stats",
    "stats_staging",
    "tracking",
    "tracking_staging",
    "warehouse_control",
)

_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")

_CREATE_LEDGER_SQL = """
CREATE SCHEMA warehouse_control;
REVOKE ALL ON SCHEMA warehouse_control FROM PUBLIC;

CREATE TABLE warehouse_control.schema_migrations (
    migration_id text PRIMARY KEY,
    path text NOT NULL UNIQUE,
    kind text NOT NULL CHECK (kind IN ('immutable', 'repeatable')),
    checksum text NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
    manifest_order integer NOT NULL UNIQUE CHECK (manifest_order > 0),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE warehouse_control.repeatable_migration_executions (
    execution_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    migration_id text NOT NULL
        REFERENCES warehouse_control.schema_migrations (migration_id),
    checksum text NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
    executed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

REVOKE ALL ON warehouse_control.schema_migrations FROM PUBLIC;
REVOKE ALL ON warehouse_control.repeatable_migration_executions FROM PUBLIC;

-- Host-level default privileges can grant named roles access independently of
-- PUBLIC. Remove inherited ACL entries only from the new private ledger objects;
-- preserve the owner and leave the host's defaults and other schemas untouched.
DO $ledger_acl$
DECLARE entry record; recipient text;
BEGIN
    FOR entry IN
        SELECT DISTINCT 'SCHEMA' AS object_kind, format('%I', n.nspname) AS object_name,
            acl.grantee
        FROM pg_catalog.pg_namespace n
        CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) acl
        WHERE n.nspname='warehouse_control' AND acl.grantee<>n.nspowner
        UNION
        SELECT DISTINCT CASE WHEN c.relkind='S' THEN 'SEQUENCE' ELSE 'TABLE' END,
            format('%I.%I', n.nspname, c.relname), acl.grantee
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) acl
        WHERE n.nspname='warehouse_control' AND c.relkind IN ('r','p','S')
            AND acl.grantee<>c.relowner
    LOOP
        recipient := CASE WHEN entry.grantee=0 THEN 'PUBLIC'
            ELSE format('%I', pg_catalog.pg_get_userbyid(entry.grantee)) END;
        EXECUTE format('REVOKE ALL ON %s %s FROM %s',
            entry.object_kind, entry.object_name, recipient);
    END LOOP;
END
$ledger_acl$;
"""


class ManifestError(ValueError):
    """The manifest or one of its SQL files is unsafe or malformed."""


class MigrationStateError(RuntimeError):
    """The database ledger cannot be reconciled with the requested manifest."""

    def __init__(self, plan: MigrationPlan):
        self.plan = plan
        super().__init__("; ".join(plan.diagnostics) or "invalid migration state")


@dataclass(frozen=True)
class Migration:
    """A fully validated migration with an exact-byte checksum."""

    id: str
    path: str
    kind: MigrationKind
    checksum: str
    absolute_path: Path
    sql: str


@dataclass(frozen=True)
class Manifest:
    """Versioned, ordered migration manifest."""

    version: int
    migrations: tuple[Migration, ...]
    source_path: Path
    root: Path


@dataclass(frozen=True)
class MigrationStep:
    """One migration's disposition in a plan."""

    id: str
    path: str
    kind: MigrationKind
    checksum: str
    order: int
    action: MigrationAction


@dataclass(frozen=True)
class MigrationPlan:
    """A read-only description of work and any state that blocks execution."""

    mode: MigrationMode
    target: str | None
    ledger_installed: bool
    steps: tuple[MigrationStep, ...]
    diagnostics: tuple[str, ...] = ()

    @property
    def valid(self) -> bool:
        return not self.diagnostics

    @property
    def pending(self) -> tuple[MigrationStep, ...]:
        return tuple(step for step in self.steps if step.action in {"apply", "reapply"})


@dataclass(frozen=True)
class _AppliedMigration:
    id: str
    path: str
    kind: str
    checksum: str
    order: int


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ManifestError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _load_json(path: Path) -> object:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ManifestError(f"cannot read manifest {path}: {exc}") from exc
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_json_keys)
    except UnicodeDecodeError as exc:
        raise ManifestError(f"manifest is not UTF-8: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ManifestError(f"invalid JSON manifest {path}: {exc}") from exc


def _resolve_migration_path(value: object, root: Path) -> tuple[str, Path]:
    if not isinstance(value, str) or not value:
        raise ManifestError("migration path must be a non-empty string")
    if "\\" in value:
        raise ManifestError(f"migration path must use repository-relative POSIX syntax: {value!r}")
    relative = PurePosixPath(value)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise ManifestError(
            f"migration path must not be absolute or traverse directories: {value!r}"
        )
    if value != relative.as_posix():
        raise ManifestError(f"migration path must use canonical POSIX syntax: {value!r}")
    if relative.suffix.lower() != ".sql":
        raise ManifestError(f"migration path must name a .sql file: {value!r}")

    candidate = root.joinpath(*relative.parts)
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise ManifestError(
            f"migration path is missing or outside repository root: {value!r}"
        ) from exc
    if not resolved.is_file():
        raise ManifestError(f"migration path is not a file: {value!r}")
    return value, resolved


def load_manifest(path: str | Path, root: str | Path) -> Manifest:
    """Load, strictly validate, and checksum a version-1 JSON manifest.

    Migration paths use POSIX separators, are relative to ``root``, and may not resolve through a
    symlink outside it. SQL is decoded as UTF-8 only after hashing its exact on-disk bytes.
    """

    try:
        root_path = Path(root).resolve(strict=True)
    except OSError as exc:
        raise ManifestError(f"cannot resolve repository root {root}: {exc}") from exc
    if not root_path.is_dir():
        raise ManifestError(f"repository root is not a directory: {root_path}")
    manifest_path = Path(path).resolve(strict=False)
    payload = _load_json(manifest_path)
    if not isinstance(payload, dict):
        raise ManifestError("manifest must be a JSON object")
    if set(payload) != {"version", "migrations"}:
        raise ManifestError("manifest keys must be exactly: version, migrations")
    if type(payload["version"]) is not int or payload["version"] != 1:
        raise ManifestError("manifest version must be integer 1")
    entries = payload["migrations"]
    if not isinstance(entries, list) or not entries:
        raise ManifestError("manifest migrations must be a non-empty list")

    seen_ids: set[str] = set()
    seen_paths: set[str] = set()
    seen_files: set[Path] = set()
    migrations: list[Migration] = []
    for index, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict) or set(entry) != {"id", "path", "kind"}:
            raise ManifestError(f"migration {index} keys must be exactly: id, path, kind")
        migration_id = entry["id"]
        if not isinstance(migration_id, str) or not _ID_RE.fullmatch(migration_id):
            raise ManifestError(f"migration {index} has invalid id: {migration_id!r}")
        if migration_id in seen_ids:
            raise ManifestError(f"duplicate migration id: {migration_id}")
        kind = entry["kind"]
        if not isinstance(kind, str) or kind not in VALID_KINDS:
            raise ManifestError(f"migration {migration_id} has invalid kind: {kind!r}")
        relative_path, absolute_path = _resolve_migration_path(entry["path"], root_path)
        if relative_path in seen_paths:
            raise ManifestError(f"duplicate migration path: {relative_path}")
        if absolute_path in seen_files:
            raise ManifestError(f"duplicate resolved migration path: {relative_path}")

        sql_bytes = absolute_path.read_bytes()
        try:
            sql = sql_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ManifestError(f"migration is not UTF-8: {relative_path}") from exc
        _validate_no_transaction_control(sql, relative_path)
        migrations.append(
            Migration(
                id=migration_id,
                path=relative_path,
                kind=kind,
                checksum=hashlib.sha256(sql_bytes).hexdigest(),
                absolute_path=absolute_path,
                sql=sql,
            )
        )
        seen_ids.add(migration_id)
        seen_paths.add(relative_path)
        seen_files.add(absolute_path)

    return Manifest(
        version=1,
        migrations=tuple(migrations),
        source_path=manifest_path,
        root=root_path,
    )


def _statement_words(sql: str) -> list[list[str]]:
    """Return unquoted leading words for each SQL statement.

    PostgreSQL comments, quoted strings/identifiers, and dollar-quoted bodies are skipped.  That
    keeps transaction-looking text inside comments, function bodies, and ``DO`` blocks inert.
    """

    statements: list[list[str]] = []
    words: list[str] = []
    index = 0
    length = len(sql)
    block_depth = 0
    while index < length:
        if block_depth:
            if sql.startswith("/*", index):
                block_depth += 1
                index += 2
            elif sql.startswith("*/", index):
                block_depth -= 1
                index += 2
            else:
                index += 1
            continue
        if sql.startswith("--", index):
            newline = sql.find("\n", index + 2)
            index = length if newline < 0 else newline + 1
            continue
        if sql.startswith("/*", index):
            block_depth = 1
            index += 2
            continue

        char = sql[index]
        if char in {"'", '"'}:
            quote = char
            escape_string = (
                quote == "'"
                and index > 0
                and sql[index - 1] in {"e", "E"}
                and (index < 2 or not (sql[index - 2].isalnum() or sql[index - 2] in {"_", "$"}))
            )
            index += 1
            while index < length:
                if sql[index] == quote:
                    if index + 1 < length and sql[index + 1] == quote:
                        index += 2
                        continue
                    index += 1
                    break
                if escape_string and sql[index] == "\\" and index + 1 < length:
                    index += 2
                else:
                    index += 1
            continue
        if char == "$":
            tag_match = re.match(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$", sql[index:])
            if tag_match:
                tag = tag_match.group(0)
                end = sql.find(tag, index + len(tag))
                index = length if end < 0 else end + len(tag)
                continue
        if char == ";":
            if words:
                statements.append(words)
            words = []
            index += 1
            continue
        if char.isalpha() or char == "_":
            end = index + 1
            while end < length and (sql[end].isalnum() or sql[end] in {"_", "$"}):
                end += 1
            words.append(sql[index:end].upper())
            index = end
            continue
        index += 1
    if words:
        statements.append(words)
    return statements


def _validate_no_transaction_control(sql: str, path: str) -> None:
    forbidden_first = {"ABORT", "BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT"}
    for words in _statement_words(sql):
        if not words:
            continue
        forbidden = words[0] in forbidden_first
        forbidden = forbidden or words[:2] in (["START", "TRANSACTION"], ["SET", "TRANSACTION"])
        forbidden = forbidden or words[:2] in (
            ["PREPARE", "TRANSACTION"],
            ["RELEASE", "SAVEPOINT"],
        )
        if forbidden:
            raise ManifestError(
                f"migration {path!r} contains top-level transaction control: {' '.join(words[:2])}"
            )


def _ensure_idle_connection(conn: object) -> None:
    if getattr(conn, "autocommit", False) is True:
        raise RuntimeError("migration connection must have autocommit disabled")
    get_status = getattr(conn, "get_transaction_status", None)
    if callable(get_status):
        status = get_status()
        # psycopg2 and psycopg 3 both use zero for IDLE.  Avoid interpreting arbitrary mocks.
        if isinstance(status, int) and status != 0:
            raise RuntimeError("migration connection must not already be in a transaction")


def _execute(cur: object, sql: str, params: object = None) -> None:
    if params is None:
        cur.execute(sql)
    else:
        cur.execute(sql, params)


def _ledger_installed(cur: object) -> bool:
    _execute(
        cur,
        "SELECT to_regclass('warehouse_control.schema_migrations') IS NOT NULL",
    )
    row = cur.fetchone()
    return bool(row and row[0])


def _existing_project_schemas(cur: object) -> tuple[str, ...]:
    _execute(
        cur,
        "SELECT nspname FROM pg_namespace WHERE nspname = ANY(%s) ORDER BY nspname",
        (list(PROJECT_SCHEMAS),),
    )
    return tuple(row[0] for row in cur.fetchall())


def _existing_public_objects(cur: object) -> tuple[str, ...]:
    """Find repository-like objects in ``public``, excluding extension-owned objects."""

    _execute(
        cur,
        """
        SELECT object_identity
        FROM (
            SELECT format('%I.%I', namespace.nspname, relation.relname) AS object_identity
            FROM pg_class AS relation
            JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'public'
              AND relation.relkind IN ('r', 'p', 'v', 'm')
              AND NOT EXISTS (
                  SELECT 1
                  FROM pg_depend AS dependency
                  WHERE dependency.classid = 'pg_class'::regclass
                    AND dependency.objid = relation.oid
                    AND dependency.deptype = 'e'
              )
            UNION ALL
            SELECT format(
                '%I.%I(%s)',
                namespace.nspname,
                procedure.proname,
                pg_get_function_identity_arguments(procedure.oid)
            ) AS object_identity
            FROM pg_proc AS procedure
            JOIN pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
            WHERE namespace.nspname = 'public'
              AND NOT EXISTS (
                  SELECT 1
                  FROM pg_depend AS dependency
                  WHERE dependency.classid = 'pg_proc'::regclass
                    AND dependency.objid = procedure.oid
                    AND dependency.deptype = 'e'
              )
        ) AS public_objects
        ORDER BY object_identity
        """,
    )
    return tuple(row[0] for row in cur.fetchall())


def _read_applied(cur: object) -> tuple[_AppliedMigration, ...]:
    _execute(
        cur,
        """
        SELECT migration_id, path, kind, checksum, manifest_order
        FROM warehouse_control.schema_migrations
        ORDER BY manifest_order
        """,
    )
    return tuple(_AppliedMigration(*row) for row in cur.fetchall())


def _target_index(manifest: Manifest, target: str | None) -> int:
    if target is None:
        return len(manifest.migrations)
    for index, migration in enumerate(manifest.migrations, start=1):
        if migration.id == target:
            return index
    raise ValueError(f"target migration is not present in manifest: {target}")


def _build_plan(
    manifest: Manifest,
    *,
    mode: MigrationMode,
    target: str | None,
    ledger_installed: bool,
    applied: tuple[_AppliedMigration, ...],
    bootstrap_schemas: tuple[str, ...] = (),
    bootstrap_public_objects: tuple[str, ...] = (),
) -> MigrationPlan:
    target_index = _target_index(manifest, target)
    diagnostics: list[str] = []
    if mode == "bootstrap":
        if not ledger_installed and bootstrap_schemas:
            diagnostics.append(
                "bootstrap requires no existing project schemas; found: "
                + ", ".join(bootstrap_schemas)
            )
        if not ledger_installed and bootstrap_public_objects:
            diagnostics.append(
                "bootstrap requires no existing user objects in public; found: "
                + ", ".join(bootstrap_public_objects)
            )
    elif not ledger_installed:
        diagnostics.append(
            f"{mode} requires an installed migration ledger; unledgered databases are not adopted"
        )

    if len(applied) > len(manifest.migrations):
        diagnostics.append("applied migrations were removed from the manifest")

    comparable = min(len(applied), len(manifest.migrations))
    actions: list[MigrationAction] = ["skip"] * comparable
    for index in range(comparable):
        recorded = applied[index]
        current = manifest.migrations[index]
        expected_order = index + 1
        if recorded.order != expected_order:
            diagnostics.append(
                f"ledger order gap at {recorded.id}: expected {expected_order}, "
                f"found {recorded.order}"
            )
        if recorded.id != current.id:
            diagnostics.append(
                f"applied manifest prefix changed at order {expected_order}: "
                f"ledger has {recorded.id}, manifest has {current.id}"
            )
        if recorded.path != current.path:
            diagnostics.append(f"applied migration {recorded.id} path changed")
        if recorded.kind != current.kind:
            diagnostics.append(f"applied migration {recorded.id} kind changed")
        if recorded.checksum != current.checksum:
            if recorded.kind == current.kind == "repeatable" and recorded.id == current.id:
                actions[index] = "reapply"
            else:
                diagnostics.append(f"applied immutable migration {recorded.id} checksum changed")

    if target_index < len(applied):
        diagnostics.append(
            f"target {target!r} is behind {len(applied)} already-applied migration(s)"
        )

    steps: list[MigrationStep] = []
    for index, migration in enumerate(manifest.migrations, start=1):
        if index <= comparable:
            action = actions[index - 1]
        elif index <= target_index:
            action = "apply"
        else:
            action = "deferred"
        steps.append(
            MigrationStep(
                id=migration.id,
                path=migration.path,
                kind=migration.kind,
                checksum=migration.checksum,
                order=index,
                action=action,
            )
        )
    return MigrationPlan(
        mode=mode,
        target=target,
        ledger_installed=ledger_installed,
        steps=tuple(steps),
        diagnostics=tuple(dict.fromkeys(diagnostics)),
    )


def _record_initial(cur: object, migration: Migration, order: int) -> None:
    _execute(
        cur,
        """
        INSERT INTO warehouse_control.schema_migrations
            (migration_id, path, kind, checksum, manifest_order)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (migration.id, migration.path, migration.kind, migration.checksum, order),
    )
    if migration.kind == "repeatable":
        _record_repeatable_execution(cur, migration)


def _record_repeatable_execution(cur: object, migration: Migration) -> None:
    _execute(
        cur,
        """
        INSERT INTO warehouse_control.repeatable_migration_executions
            (migration_id, checksum)
        VALUES (%s, %s)
        """,
        (migration.id, migration.checksum),
    )


def _record_repeatable_update(cur: object, migration: Migration) -> None:
    _execute(
        cur,
        """
        UPDATE warehouse_control.schema_migrations
        SET checksum = %s, applied_at = clock_timestamp()
        WHERE migration_id = %s
        """,
        (migration.checksum, migration.id),
    )
    _record_repeatable_execution(cur, migration)


def apply_manifest(
    conn: object,
    manifest: Manifest,
    *,
    mode: MigrationMode,
    target: str | None = None,
    dry_run: bool = False,
) -> MigrationPlan:
    """Plan or atomically apply a manifest against ``conn``.

    ``mode`` is always explicit. ``status`` and ``plan`` are read-only, as is any call with
    ``dry_run=True``. ``bootstrap`` creates the private ledger only on a database without project
    schemas. ``upgrade`` requires an existing ledger and never adopts an unledgered database.
    """

    if mode not in VALID_MODES:
        raise ValueError(f"invalid migration mode {mode!r}; expected one of {sorted(VALID_MODES)}")
    _target_index(manifest, target)
    for migration in manifest.migrations:
        _validate_no_transaction_control(migration.sql, migration.path)

    read_only = dry_run or mode in {"status", "plan"}
    _ensure_idle_connection(conn)
    cur = conn.cursor()
    try:
        _execute(cur, "BEGIN READ ONLY" if read_only else "BEGIN")
        _execute(cur, "SET LOCAL standard_conforming_strings = on")
        _execute(cur, "SELECT pg_advisory_xact_lock(%s, %s)", ADVISORY_LOCK_KEYS)
        installed = _ledger_installed(cur)
        bootstrap_schemas: tuple[str, ...] = ()
        bootstrap_public_objects: tuple[str, ...] = ()
        if mode == "bootstrap" and not installed:
            bootstrap_schemas = _existing_project_schemas(cur)
            bootstrap_public_objects = _existing_public_objects(cur)
        applied = _read_applied(cur) if installed else ()
        plan = _build_plan(
            manifest,
            mode=mode,
            target=target,
            ledger_installed=installed,
            applied=applied,
            bootstrap_schemas=bootstrap_schemas,
            bootstrap_public_objects=bootstrap_public_objects,
        )

        if read_only:
            conn.rollback()
            return plan
        if not plan.valid:
            raise MigrationStateError(plan)
        if mode == "bootstrap" and not installed:
            _execute(cur, _CREATE_LEDGER_SQL)

        migrations_by_id = {migration.id: migration for migration in manifest.migrations}
        # Versioned DDL establishes prerequisites before any repeatable definition is refreshed.
        # Each group retains manifest order.
        pending = sorted(plan.pending, key=lambda step: (step.kind == "repeatable", step.order))
        for step in pending:
            migration = migrations_by_id[step.id]
            _execute(cur, migration.sql)
            if step.action == "apply":
                _record_initial(cur, migration, step.order)
            else:
                _record_repeatable_update(cur, migration)
        conn.commit()
        return plan
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
