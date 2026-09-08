"""Dependency-complete, transactional mart releases.

Version 1 deliberately supports only contract-preserving releases.  A manifest names every
existing relation or function in the PostgreSQL downstream dependency closure.  Planning is
read-only; execution repeats the plan under the warehouse migration advisory lock before any
manifest SQL is run.

PostgreSQL records parsed dependencies for views and SQL functions.  PL/pgSQL bodies can contain
textual relation references which are not catalog dependencies, so affected PL/pgSQL functions
also require role-scoped validation queries declared by the manifest.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal

from scripts.warehouse_migrations import (
    ADVISORY_LOCK_KEYS,
    PROJECT_SCHEMAS,
    _statement_words,
    _validate_no_transaction_control,
)

ObjectKind = Literal["relation", "function"]

LOCK_TIMEOUT = "10s"
STATEMENT_TIMEOUT = "15min"

_IDENTIFIER_RE = re.compile(r"[a-z_][a-z0-9_$]*\Z")
_RELATION_RE = re.compile(r"[a-z_][a-z0-9_$]*\.[a-z_][a-z0-9_$]*\Z")
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


class ReleaseManifestError(ValueError):
    """The release manifest or one of its files is malformed or unsafe."""


class ReleaseExecutionError(RuntimeError):
    """A release failed a validation or post-execution invariant."""


class ReleaseBlockedError(RuntimeError):
    """The live dependency closure does not permit this release."""

    def __init__(self, plan: ReleasePlan):
        self.plan = plan
        super().__init__("; ".join(plan.blockers) or "mart release is blocked")


@dataclass(frozen=True, order=True)
class ObjectIdentity:
    kind: ObjectKind
    identity: str


@dataclass(frozen=True)
class ReleaseFile:
    path: str
    sha256: str
    absolute_path: Path
    sql: str


@dataclass(frozen=True)
class ValidationQuery:
    name: str
    role: str
    query: str
    covers: tuple[str, ...]


@dataclass(frozen=True)
class ReleaseManifest:
    version: int
    files: tuple[ReleaseFile, ...]
    roots: tuple[str, ...]
    restores: tuple[ObjectIdentity, ...]
    validations: tuple[ValidationQuery, ...]
    source_path: Path
    root: Path


@dataclass(frozen=True)
class ReleasePlan:
    files: tuple[ReleaseFile, ...]
    roots: tuple[str, ...]
    declared_restores: tuple[ObjectIdentity, ...]
    live_closure: tuple[ObjectIdentity, ...]
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    @property
    def valid(self) -> bool:
        return not self.blockers


@dataclass(frozen=True)
class _CatalogSnapshot:
    identity: ObjectIdentity
    object_type: str
    definition: str
    contract: tuple[object, ...]
    owner: str
    acl: tuple[tuple[str, str, str, bool], ...]


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ReleaseManifestError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _load_json(path: Path) -> object:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ReleaseManifestError(f"cannot read release manifest {path}: {exc}") from exc
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_json_keys)
    except UnicodeDecodeError as exc:
        raise ReleaseManifestError(f"release manifest is not UTF-8: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ReleaseManifestError(f"invalid JSON release manifest {path}: {exc}") from exc


def _resolve_sql_path(value: object, root: Path) -> tuple[str, Path]:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ReleaseManifestError("release file path must be repository-relative POSIX syntax")
    relative = PurePosixPath(value)
    if (
        relative.is_absolute()
        or value != relative.as_posix()
        or any(part in {"", ".", ".."} for part in relative.parts)
        or relative.suffix.lower() != ".sql"
    ):
        raise ReleaseManifestError(f"unsafe or non-canonical release file path: {value!r}")
    try:
        resolved = root.joinpath(*relative.parts).resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise ReleaseManifestError(
            f"release file is missing or outside repository: {value!r}"
        ) from exc
    if not resolved.is_file():
        raise ReleaseManifestError(f"release path is not a file: {value!r}")
    return value, resolved


def _validate_release_sql(sql: str, path: str) -> None:
    _validate_no_transaction_control(sql, path)
    for words in _statement_words(sql):
        if not words:
            continue
        forbidden = words[0] in {"CALL", "VACUUM"}
        forbidden = forbidden or words[:2] in (
            ["ALTER", "SYSTEM"],
            ["CREATE", "DATABASE"],
            ["CREATE", "TABLESPACE"],
            ["DROP", "DATABASE"],
            ["DROP", "TABLESPACE"],
        )
        # A file-level GUC change can weaken the parser assumptions or remove the bounded
        # timeouts for later statements in the same protocol message. Function-clause SETs are
        # part of a CREATE statement and therefore remain supported.
        forbidden = forbidden or words[0] in {"SET", "RESET"}
        forbidden = forbidden or (
            "CONCURRENTLY" in words and words[0] in {"CREATE", "DROP", "REINDEX", "REFRESH"}
        )
        if forbidden:
            raise ReleaseManifestError(
                f"release file {path!r} contains nontransactional or unsafe SQL: "
                f"{' '.join(words[:4])}"
            )


def _require_exact_keys(value: object, keys: set[str], context: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ReleaseManifestError(f"{context} keys must be exactly: {', '.join(sorted(keys))}")
    return value


def load_release(path: str | Path, root: str | Path) -> ReleaseManifest:
    """Load and strictly validate a version-1 release manifest and its exact-byte hashes."""

    try:
        root_path = Path(root).resolve(strict=True)
    except OSError as exc:
        raise ReleaseManifestError(f"cannot resolve repository root {root}: {exc}") from exc
    if not root_path.is_dir():
        raise ReleaseManifestError(f"repository root is not a directory: {root_path}")
    source_path = Path(path).resolve(strict=False)
    payload = _require_exact_keys(
        _load_json(source_path),
        {"version", "files", "roots", "restores", "validations"},
        "release manifest",
    )
    if type(payload["version"]) is not int or payload["version"] != 1:
        raise ReleaseManifestError("release manifest version must be integer 1")

    entries = payload["files"]
    if not isinstance(entries, list) or not entries:
        raise ReleaseManifestError("release manifest files must be a non-empty list")
    files: list[ReleaseFile] = []
    seen_paths: set[str] = set()
    seen_resolved: set[Path] = set()
    for index, raw_entry in enumerate(entries, start=1):
        entry = _require_exact_keys(raw_entry, {"path", "sha256"}, f"release file {index}")
        relative, absolute = _resolve_sql_path(entry["path"], root_path)
        checksum = entry["sha256"]
        if not isinstance(checksum, str) or not _SHA256_RE.fullmatch(checksum):
            raise ReleaseManifestError(f"release file {relative!r} has invalid sha256")
        if relative in seen_paths or absolute in seen_resolved:
            raise ReleaseManifestError(f"duplicate release file: {relative}")
        sql_bytes = absolute.read_bytes()
        actual = hashlib.sha256(sql_bytes).hexdigest()
        if actual != checksum:
            raise ReleaseManifestError(
                f"release file checksum mismatch for {relative}: expected {checksum}, "
                f"found {actual}"
            )
        try:
            sql = sql_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ReleaseManifestError(f"release file is not UTF-8: {relative}") from exc
        _validate_release_sql(sql, relative)
        files.append(ReleaseFile(relative, checksum, absolute, sql))
        seen_paths.add(relative)
        seen_resolved.add(absolute)

    roots_raw = payload["roots"]
    if not isinstance(roots_raw, list) or not roots_raw:
        raise ReleaseManifestError("release roots must be a non-empty list")
    roots: list[str] = []
    for value in roots_raw:
        if not isinstance(value, str) or not _RELATION_RE.fullmatch(value):
            raise ReleaseManifestError(f"root must be a fully qualified relation name: {value!r}")
        if value in roots:
            raise ReleaseManifestError(f"duplicate release root: {value}")
        roots.append(value)

    restores_raw = payload["restores"]
    if not isinstance(restores_raw, list) or not restores_raw:
        raise ReleaseManifestError("release restores must be a non-empty list")
    restores: list[ObjectIdentity] = []
    for index, raw_restore in enumerate(restores_raw, start=1):
        restore = _require_exact_keys(raw_restore, {"kind", "identity"}, f"restore {index}")
        kind = restore["kind"]
        identity = restore["identity"]
        if kind not in {"relation", "function"} or not isinstance(identity, str) or not identity:
            raise ReleaseManifestError(f"invalid restore {index}: {raw_restore!r}")
        if kind == "relation" and not _RELATION_RE.fullmatch(identity):
            raise ReleaseManifestError(f"invalid relation restore identity: {identity!r}")
        item = ObjectIdentity(kind, identity)
        if item in restores:
            raise ReleaseManifestError(f"duplicate restore identity: {kind} {identity}")
        restores.append(item)
    missing_roots = set(roots) - {item.identity for item in restores if item.kind == "relation"}
    if missing_roots:
        raise ReleaseManifestError(
            f"release roots must also be declared as restores: {', '.join(sorted(missing_roots))}"
        )

    validations_raw = payload["validations"]
    if not isinstance(validations_raw, list):
        raise ReleaseManifestError("release validations must be a list")
    validations: list[ValidationQuery] = []
    names: set[str] = set()
    for index, raw_validation in enumerate(validations_raw, start=1):
        validation = _require_exact_keys(
            raw_validation, {"name", "role", "query", "covers"}, f"validation {index}"
        )
        name, role, query, covers = (
            validation["name"],
            validation["role"],
            validation["query"],
            validation["covers"],
        )
        if not isinstance(name, str) or not name or name in names:
            raise ReleaseManifestError(f"validation {index} has missing or duplicate name")
        if not isinstance(role, str) or not _IDENTIFIER_RE.fullmatch(role):
            raise ReleaseManifestError(f"validation {name!r} has invalid role")
        if not isinstance(query, str) or not query.strip():
            raise ReleaseManifestError(f"validation {name!r} has an empty query")
        statements = _statement_words(query)
        if len(statements) != 1 or statements[0][0] != "SELECT":
            raise ReleaseManifestError(f"validation {name!r} must be one SELECT assertion")
        _validate_no_transaction_control(query, f"validation {name}")
        if not isinstance(covers, list) or any(
            not isinstance(item, str) or not item for item in covers
        ):
            raise ReleaseManifestError(f"validation {name!r} covers must be a list of identities")
        validations.append(ValidationQuery(name, role, query, tuple(covers)))
        names.add(name)

    return ReleaseManifest(
        1,
        tuple(files),
        tuple(roots),
        tuple(restores),
        tuple(validations),
        source_path,
        root_path,
    )


def _ensure_idle_connection(conn: object) -> None:
    if getattr(conn, "autocommit", False) is True:
        raise RuntimeError("mart release connection must have autocommit disabled")
    get_status = getattr(conn, "get_transaction_status", None)
    if callable(get_status):
        status = get_status()
        if isinstance(status, int) and status != 0:
            raise RuntimeError("mart release connection must not already be in a transaction")


def _execute(cur: object, sql: str, params: object = None) -> None:
    if params is None:
        cur.execute(sql)
    else:
        cur.execute(sql, params)


def _start_transaction(cur: object, *, read_only: bool) -> None:
    _execute(cur, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY" if read_only else "BEGIN")
    _execute(cur, "SET LOCAL standard_conforming_strings = on")
    _execute(cur, "SET LOCAL lock_timeout = %s", (LOCK_TIMEOUT,))
    _execute(cur, "SET LOCAL statement_timeout = %s", (STATEMENT_TIMEOUT,))
    _execute(cur, "SELECT pg_advisory_xact_lock(%s, %s)", ADVISORY_LOCK_KEYS)


def _resolve_roots(cur: object, roots: tuple[str, ...]) -> tuple[tuple[int, str], ...]:
    _execute(
        cur,
        """
        /* mart_release:roots */
        SELECT c.oid, format('%%I.%%I', n.nspname, c.relname), c.relkind
        FROM unnest(%s::text[]) WITH ORDINALITY AS requested(identity, position)
        LEFT JOIN pg_catalog.pg_class c ON c.oid = pg_catalog.to_regclass(requested.identity)
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        ORDER BY requested.position
        """,
        (list(roots),),
    )
    rows = cur.fetchall()
    resolved: list[tuple[int, str]] = []
    for requested, row in zip(roots, rows, strict=True):
        oid, identity, relkind = row
        if oid is None:
            raise ReleaseExecutionError(f"release root does not exist: {requested}")
        if relkind not in {"v", "m"}:
            raise ReleaseExecutionError(
                f"release root has unsupported relation kind {relkind!r}: {requested}"
            )
        resolved.append((int(oid), str(identity)))
    return tuple(resolved)


_CLOSURE_SQL = """
WITH RECURSIVE dependency_graph(classid, objid, objsubid) AS (
    SELECT 'pg_catalog.pg_class'::pg_catalog.regclass::oid, root_oid, 0
    FROM unnest(%s::oid[]) AS roots(root_oid)
    UNION
    SELECT
        CASE WHEN dependency.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass
             THEN 'pg_catalog.pg_class'::pg_catalog.regclass::oid
             ELSE dependency.classid END,
        CASE WHEN dependency.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass
             THEN rewrite.ev_class
             ELSE dependency.objid END,
        CASE WHEN dependency.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass
             THEN 0 ELSE dependency.objsubid END
    FROM dependency_graph AS upstream
    JOIN pg_catalog.pg_depend AS dependency
      ON dependency.refclassid = upstream.classid
     AND dependency.refobjid = upstream.objid
     AND (upstream.objsubid = 0 OR dependency.refobjsubid = upstream.objsubid)
    LEFT JOIN pg_catalog.pg_rewrite AS rewrite
      ON dependency.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass
     AND rewrite.oid = dependency.objid
    WHERE dependency.deptype <> 'p'
), visible AS (
    SELECT 'object'::text AS category, 'relation'::text AS kind,
           format('%%I.%%I', namespace.nspname, relation.relname) AS identity,
           NULL::text AS language, relation.relkind::text AS detail
    FROM dependency_graph AS graph
    JOIN pg_catalog.pg_class AS relation
      ON graph.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND relation.oid = graph.objid
    JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    WHERE relation.relkind IN ('v', 'm')
    UNION
    SELECT 'object', 'function',
           format('%%I.%%I(%%s)', namespace.nspname, procedure.proname,
                  pg_catalog.pg_get_function_identity_arguments(procedure.oid)),
           language.lanname,
           procedure.prokind::text
    FROM dependency_graph AS graph
    JOIN pg_catalog.pg_proc AS procedure
      ON graph.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass AND procedure.oid = graph.objid
    JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
    JOIN pg_catalog.pg_language AS language ON language.oid = procedure.prolang
), unsupported AS (
    SELECT DISTINCT 'unsupported'::text, graph.classid::pg_catalog.regclass::text,
           graph.objid::text, NULL::text, 'catalog class'::text
    FROM dependency_graph AS graph
    LEFT JOIN pg_catalog.pg_class AS relation
      ON graph.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND relation.oid = graph.objid
    LEFT JOIN pg_catalog.pg_type AS type_info
      ON graph.classid = 'pg_catalog.pg_type'::pg_catalog.regclass
     AND type_info.oid = graph.objid
    WHERE (graph.classid = 'pg_catalog.pg_type'::pg_catalog.regclass
           AND NOT EXISTS (
               SELECT 1 FROM dependency_graph AS relation_graph
               WHERE relation_graph.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                 AND relation_graph.objid = type_info.typrelid
           )
           AND NOT (type_info.typelem <> 0 AND EXISTS (
               SELECT 1 FROM dependency_graph AS element_graph
               WHERE element_graph.classid = 'pg_catalog.pg_type'::pg_catalog.regclass
                 AND element_graph.objid = type_info.typelem
           )))
       OR graph.classid NOT IN (
              'pg_catalog.pg_class'::pg_catalog.regclass,
              'pg_catalog.pg_proc'::pg_catalog.regclass,
              'pg_catalog.pg_type'::pg_catalog.regclass,
              'pg_catalog.pg_constraint'::pg_catalog.regclass,
              'pg_catalog.pg_attrdef'::pg_catalog.regclass,
              'pg_catalog.pg_trigger'::pg_catalog.regclass,
              'pg_catalog.pg_rewrite'::pg_catalog.regclass
          )
       OR (graph.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
           AND relation.relkind NOT IN ('v','m','i','I','t'))
)
SELECT * FROM visible
UNION ALL
SELECT * FROM unsupported
ORDER BY 1, 2, 3
"""


def _read_closure(
    cur: object, root_oids: tuple[int, ...]
) -> tuple[tuple[ObjectIdentity, str | None, str], tuple[str, ...]]:
    _execute(cur, "/* mart_release:closure */\n" + _CLOSURE_SQL, (list(root_oids),))
    objects: list[tuple[ObjectIdentity, str | None, str]] = []
    unsupported: list[str] = []
    for category, kind, identity, language, detail in cur.fetchall():
        if category == "unsupported":
            unsupported.append(f"unsupported downstream catalog object {kind} OID {identity}")
        else:
            objects.append((ObjectIdentity(kind, identity), language, detail))
    return tuple(objects), tuple(unsupported)


_SNAPSHOT_SQL = """
/* mart_release:snapshot */
SELECT 'relation'::text,
       format('%%I.%%I', namespace.nspname, relation.relname),
       relation.relkind::text,
       pg_catalog.pg_get_userbyid(relation.relowner),
       COALESCE(pg_catalog.pg_get_viewdef(relation.oid, false), ''),
       relation.relkind::text,
       COALESCE(relation.reloptions::text, ''),
       relation.relreplident::text,
       relation.relrowsecurity::text,
       relation.relforcerowsecurity::text,
       COALESCE(pg_catalog.obj_description(relation.oid, 'pg_class'), ''),
       COALESCE((
           SELECT string_agg(format('%%s:%%I:%%s:%%s:%%s:%%s:%%s:%%s:%%s', attribute.attnum,
                     attribute.attname, pg_catalog.format_type(attribute.atttypid,
                     attribute.atttypmod), attribute.attnotnull, attribute.attidentity,
                     attribute.attgenerated, COALESCE(pg_catalog.pg_get_expr(definition.adbin,
                     definition.adrelid), ''), COALESCE(attribute.attacl::text, ''),
                     COALESCE(pg_catalog.col_description(attribute.attrelid,
                     attribute.attnum), '')), '|' ORDER BY attribute.attnum)
           FROM pg_catalog.pg_attribute AS attribute
           LEFT JOIN pg_catalog.pg_attrdef AS definition
             ON definition.adrelid = attribute.attrelid
            AND definition.adnum = attribute.attnum
           WHERE attribute.attrelid = relation.oid
             AND attribute.attnum > 0 AND NOT attribute.attisdropped
       ), ''),
       COALESCE((
           SELECT string_agg(format('%%I=%%s', index_relation.relname,
                                    pg_catalog.pg_get_indexdef(index_relation.oid)),
                             '|' ORDER BY index_relation.relname)
           FROM pg_catalog.pg_index AS index_info
           JOIN pg_catalog.pg_class AS index_relation ON index_relation.oid = index_info.indexrelid
           WHERE index_info.indrelid = relation.oid
       ), ''),
       COALESCE((
           SELECT string_agg(format('%%I:%%s:%%s:%%s', role.rolname,
                     pg_catalog.has_table_privilege(role.oid, relation.oid, 'SELECT'),
                     pg_catalog.has_table_privilege(role.oid, relation.oid, 'INSERT,UPDATE,DELETE'),
                     pg_catalog.has_table_privilege(role.oid, relation.oid,
                                                    'TRUNCATE,REFERENCES,TRIGGER')),
                     '|' ORDER BY role.rolname)
           FROM pg_catalog.pg_roles AS role
           WHERE NOT role.rolsuper AND role.rolname !~ '^pg_'
       ), ''),
       COALESCE((SELECT string_agg(pg_catalog.pg_get_constraintdef(constraint_info.oid),
                                  '|' ORDER BY constraint_info.conname)
                 FROM pg_catalog.pg_constraint AS constraint_info
                 WHERE constraint_info.conrelid = relation.oid), ''),
       COALESCE((SELECT string_agg(trigger_info.tgenabled::text || ':' ||
                                  pg_catalog.pg_get_triggerdef(trigger_info.oid),
                                  '|' ORDER BY trigger_info.tgname)
                 FROM pg_catalog.pg_trigger AS trigger_info
                 WHERE trigger_info.tgrelid = relation.oid
                   AND NOT trigger_info.tgisinternal), ''),
       COALESCE((SELECT string_agg(pg_catalog.concat_ws(':', policy_info.polname,
                                      policy_info.polcmd, policy_info.polpermissive::text,
                                      policy_info.polroles::text,
                                      pg_catalog.pg_get_expr(policy_info.polqual,
                                                             policy_info.polrelid),
                                      pg_catalog.pg_get_expr(policy_info.polwithcheck,
                                                             policy_info.polrelid)),
                                  '|' ORDER BY policy_info.polname)
                 FROM pg_catalog.pg_policy AS policy_info
                 WHERE policy_info.polrelid = relation.oid), ''),
       COALESCE((SELECT string_agg(rule_info.ev_enabled::text || ':' ||
                                  pg_catalog.pg_get_ruledef(rule_info.oid),
                                  '|' ORDER BY rule_info.rulename)
                 FROM pg_catalog.pg_rewrite AS rule_info
                 WHERE rule_info.ev_class = relation.oid
                   AND rule_info.rulename <> '_RETURN'), '')
FROM pg_catalog.pg_class AS relation
JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = ANY(%s)
  AND relation.relkind IN ('r','p','v','m','f','S')
  AND NOT EXISTS (
      SELECT 1 FROM pg_catalog.pg_depend AS extension_dependency
      WHERE extension_dependency.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
        AND extension_dependency.objid = relation.oid
        AND extension_dependency.deptype = 'e'
  )
UNION ALL
SELECT 'function',
       format('%%I.%%I(%%s)', namespace.nspname, procedure.proname,
              pg_catalog.pg_get_function_identity_arguments(procedure.oid)),
       CASE procedure.prokind WHEN 'p' THEN 'procedure' ELSE 'function' END,
       pg_catalog.pg_get_userbyid(procedure.proowner),
       pg_catalog.pg_get_functiondef(procedure.oid),
       procedure.prokind::text,
       procedure.prosecdef::text,
       COALESCE(procedure.proconfig::text, ''),
       procedure.provolatile::text,
       procedure.proparallel::text,
       procedure.proleakproof::text,
       procedure.proisstrict::text,
       pg_catalog.pg_get_function_result(procedure.oid),
       pg_catalog.pg_get_function_arguments(procedure.oid),
       COALESCE(pg_catalog.obj_description(procedure.oid, 'pg_proc'), ''),
       COALESCE((
           SELECT string_agg(format('%%I:%%s', role.rolname,
                     pg_catalog.has_function_privilege(role.oid, procedure.oid, 'EXECUTE')),
                     '|' ORDER BY role.rolname)
           FROM pg_catalog.pg_roles AS role
           WHERE NOT role.rolsuper AND role.rolname !~ '^pg_'
       ), ''),
       ''::text,
       ''::text
FROM pg_catalog.pg_proc AS procedure
JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
WHERE namespace.nspname = ANY(%s)
  AND procedure.prokind IN ('f', 'p')
  AND NOT EXISTS (
      SELECT 1 FROM pg_catalog.pg_depend AS extension_dependency
      WHERE extension_dependency.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
        AND extension_dependency.objid = procedure.oid
        AND extension_dependency.deptype = 'e'
  )
ORDER BY 1, 2
"""


_ACL_SQL = """
/* mart_release:acl */
SELECT 'relation'::text,
       format('%%I.%%I', namespace.nspname, relation.relname),
       pg_catalog.pg_get_userbyid(relation.relowner),
       COALESCE(pg_catalog.pg_get_userbyid(acl.grantor), ''),
       CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE pg_catalog.pg_get_userbyid(acl.grantee) END,
       acl.privilege_type,
       acl.is_grantable
FROM pg_catalog.pg_class AS relation
JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
LEFT JOIN LATERAL pg_catalog.aclexplode(
    COALESCE(relation.relacl, pg_catalog.acldefault(
        CASE WHEN relation.relkind = 'S' THEN 's'::"char" ELSE 'r'::"char" END,
        relation.relowner))) AS acl ON true
WHERE namespace.nspname = ANY(%s)
  AND relation.relkind IN ('r','p','v','m','f','S')
  AND NOT EXISTS (
      SELECT 1 FROM pg_catalog.pg_depend AS extension_dependency
      WHERE extension_dependency.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
        AND extension_dependency.objid = relation.oid
        AND extension_dependency.deptype = 'e'
  )
UNION ALL
SELECT 'function',
       format('%%I.%%I(%%s)', namespace.nspname, procedure.proname,
              pg_catalog.pg_get_function_identity_arguments(procedure.oid)),
       pg_catalog.pg_get_userbyid(procedure.proowner),
       COALESCE(pg_catalog.pg_get_userbyid(acl.grantor), ''),
       CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE pg_catalog.pg_get_userbyid(acl.grantee) END,
       acl.privilege_type,
       acl.is_grantable
FROM pg_catalog.pg_proc AS procedure
JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
LEFT JOIN LATERAL pg_catalog.aclexplode(
    COALESCE(procedure.proacl, pg_catalog.acldefault('f', procedure.proowner))) AS acl ON true
WHERE namespace.nspname = ANY(%s)
  AND procedure.prokind IN ('f', 'p')
  AND NOT EXISTS (
      SELECT 1 FROM pg_catalog.pg_depend AS extension_dependency
      WHERE extension_dependency.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
        AND extension_dependency.objid = procedure.oid
        AND extension_dependency.deptype = 'e'
  )
ORDER BY 1, 2, 5, 6, 7
"""


def _read_snapshot(cur: object) -> dict[ObjectIdentity, _CatalogSnapshot]:
    schemas = [*PROJECT_SCHEMAS, "public"]
    _execute(cur, _SNAPSHOT_SQL, (schemas, schemas))
    base_rows = cur.fetchall()
    _execute(cur, _ACL_SQL, (schemas, schemas))
    acl_by_identity: dict[ObjectIdentity, list[tuple[str, str, str, bool]]] = {}
    owner_by_identity: dict[ObjectIdentity, str] = {}
    for kind, identity, owner, grantor, grantee, privilege, grantable in cur.fetchall():
        item = ObjectIdentity(kind, identity)
        owner_by_identity[item] = owner
        if privilege is not None:
            acl_by_identity.setdefault(item, []).append(
                (str(grantor), str(grantee), str(privilege), bool(grantable))
            )
    snapshots: dict[ObjectIdentity, _CatalogSnapshot] = {}
    for kind, identity, object_type, owner, definition, *contract in base_rows:
        item = ObjectIdentity(kind, identity)
        snapshots[item] = _CatalogSnapshot(
            item,
            object_type,
            definition,
            tuple(contract),
            owner_by_identity.get(item, owner),
            tuple(acl_by_identity.get(item, ())),
        )
    return snapshots


def _build_plan(
    cur: object, manifest: ReleaseManifest
) -> tuple[ReleasePlan, dict[ObjectIdentity, _CatalogSnapshot]]:
    resolved = _resolve_roots(cur, manifest.roots)
    closure_rows, unsupported = _read_closure(cur, tuple(row[0] for row in resolved))
    live = tuple(sorted({row[0] for row in closure_rows}))
    declared = set(manifest.restores)
    actual = set(live)
    blockers = list(unsupported)
    missing = actual - declared
    extra = declared - actual
    if missing:
        blockers.append(
            "manifest omits live downstream closure objects: "
            + ", ".join(f"{item.kind} {item.identity}" for item in sorted(missing))
        )
    if extra:
        blockers.append(
            "manifest declares restore objects outside the live closure: "
            + ", ".join(f"{item.kind} {item.identity}" for item in sorted(extra))
        )
    plpgsql = {row[0].identity for row in closure_rows if row[1] == "plpgsql"}
    covered = {identity for validation in manifest.validations for identity in validation.covers}
    missing_validation = plpgsql - covered
    unknown_coverage = covered - {item.identity for item in actual if item.kind == "function"}
    if missing_validation:
        blockers.append(
            "affected PL/pgSQL functions require caller-role validations: "
            + ", ".join(sorted(missing_validation))
        )
    if unknown_coverage:
        blockers.append(
            "validation covers identities outside the live function closure: "
            + ", ".join(sorted(unknown_coverage))
        )
    snapshot = _read_snapshot(cur)
    unsnapshotted = actual - set(snapshot)
    if unsnapshotted:
        blockers.append(
            "live closure contains objects outside supported project/public namespaces: "
            + ", ".join(f"{item.kind} {item.identity}" for item in sorted(unsnapshotted))
        )
    warnings = (
        "PostgreSQL catalog dependencies do not include dynamic SQL or textual PL/pgSQL "
        "references; declared caller-role validations cover affected PL/pgSQL functions.",
        "Version 1 permits contract-preserving releases only and does not write the F06 "
        "warehouse migration ledger.",
    )
    return (
        ReleasePlan(
            manifest.files,
            tuple(identity for _, identity in resolved),
            manifest.restores,
            live,
            tuple(blockers),
            warnings,
        ),
        snapshot,
    )


def plan_release(conn: object, manifest: ReleaseManifest) -> ReleasePlan:
    """Return a read-only dependency and blocker preview, leaving no transaction open."""

    _ensure_idle_connection(conn)
    cur = conn.cursor()
    try:
        _start_transaction(cur, read_only=True)
        plan, _ = _build_plan(cur, manifest)
        conn.rollback()
        return plan
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _restore_security(
    cur: object,
    before: dict[ObjectIdentity, _CatalogSnapshot],
    affected: set[ObjectIdentity],
) -> None:
    current = _read_snapshot(cur)
    for identity in sorted(affected):
        original = before[identity]
        if identity not in current:
            raise ReleaseExecutionError(
                f"manifest did not recreate {identity.kind} {identity.identity}"
            )
        owner = _quote_ident(original.owner)
        if identity.kind == "relation":
            alter_kind = {
                "r": "TABLE",
                "p": "TABLE",
                "v": "VIEW",
                "m": "MATERIALIZED VIEW",
                "f": "FOREIGN TABLE",
                "S": "SEQUENCE",
            }[original.object_type]
            _execute(cur, f"ALTER {alter_kind} {identity.identity} OWNER TO {owner}")
            target = (
                f"TABLE {identity.identity}"
                if original.object_type != "S"
                else f"SEQUENCE {identity.identity}"
            )
        else:
            alter_kind = "PROCEDURE" if original.object_type == "procedure" else "FUNCTION"
            _execute(cur, f"ALTER {alter_kind} {identity.identity} OWNER TO {owner}")
            target = f"{alter_kind} {identity.identity}"

        old_grantees = {entry[1] for entry in original.acl}
        new_grantees = {entry[1] for entry in current[identity].acl}
        for grantee in sorted((old_grantees | new_grantees) - {original.owner}):
            recipient = "PUBLIC" if grantee == "PUBLIC" else _quote_ident(grantee)
            _execute(cur, f"REVOKE ALL PRIVILEGES ON {target} FROM {recipient}")
        for grantor, grantee, privilege, grantable in original.acl:
            if grantee == original.owner:
                continue
            _execute(cur, f"SET LOCAL ROLE {_quote_ident(grantor)}")
            recipient = "PUBLIC" if grantee == "PUBLIC" else _quote_ident(grantee)
            suffix = " WITH GRANT OPTION" if grantable else ""
            _execute(cur, f"GRANT {privilege} ON {target} TO {recipient}{suffix}")
            _execute(cur, "RESET ROLE")


def _run_validations(cur: object, validations: tuple[ValidationQuery, ...]) -> None:
    for validation in validations:
        _execute(cur, "SAVEPOINT mart_release_validation")
        try:
            _execute(cur, "SET LOCAL standard_conforming_strings = on")
            _execute(cur, f"SET LOCAL ROLE {_quote_ident(validation.role)}")
            _execute(cur, validation.query)
            row = cur.fetchone()
            if row != (True,):
                raise ReleaseExecutionError(
                    f"release validation {validation.name!r} must return exactly one true boolean"
                )
            if cur.fetchone() is not None:
                raise ReleaseExecutionError(
                    f"release validation {validation.name!r} returned more than one row"
                )
        except Exception:
            _execute(cur, "ROLLBACK TO SAVEPOINT mart_release_validation")
            _execute(cur, "RELEASE SAVEPOINT mart_release_validation")
            raise
        _execute(cur, "ROLLBACK TO SAVEPOINT mart_release_validation")
        _execute(cur, "RELEASE SAVEPOINT mart_release_validation")


def _verify_post_snapshot(
    before: dict[ObjectIdentity, _CatalogSnapshot],
    after: dict[ObjectIdentity, _CatalogSnapshot],
    affected: set[ObjectIdentity],
) -> None:
    lost = set(before) - set(after)
    if lost:
        raise ReleaseExecutionError(
            "release removed undeclared project objects: "
            + ", ".join(f"{item.kind} {item.identity}" for item in sorted(lost))
        )
    added = set(after) - set(before)
    undeclared_added = added - affected
    if undeclared_added:
        raise ReleaseExecutionError(
            "release added objects outside its declared closure: "
            + ", ".join(f"{item.kind} {item.identity}" for item in sorted(undeclared_added))
        )
    changed_outside = {
        item for item in set(before) & set(after) - affected if before[item] != after[item]
    }
    if changed_outside:
        raise ReleaseExecutionError(
            "release changed project objects outside its declared closure: "
            + ", ".join(f"{item.kind} {item.identity}" for item in sorted(changed_outside))
        )
    changed_contract = {
        item
        for item in affected
        if item in before
        and item in after
        and (
            before[item].object_type != after[item].object_type
            or before[item].contract != after[item].contract
            or before[item].owner != after[item].owner
            or before[item].acl != after[item].acl
        )
    }
    if changed_contract:
        raise ReleaseExecutionError(
            "release changed an existing object contract; version 1 has no reviewed "
            "contract-change escape hatch: "
            + ", ".join(f"{item.kind} {item.identity}" for item in sorted(changed_contract))
        )


def execute_release(conn: object, manifest: ReleaseManifest) -> ReleasePlan:
    """Atomically execute a valid release and its validations, or roll everything back."""

    for release_file in manifest.files:
        sql_bytes = release_file.absolute_path.read_bytes()
        actual = hashlib.sha256(sql_bytes).hexdigest()
        if actual != release_file.sha256:
            raise ReleaseManifestError(
                f"release file changed after manifest load: {release_file.path}"
            )
        _validate_release_sql(release_file.sql, release_file.path)
    _ensure_idle_connection(conn)
    cur = conn.cursor()
    try:
        _start_transaction(cur, read_only=False)
        plan, before = _build_plan(cur, manifest)
        if not plan.valid:
            raise ReleaseBlockedError(plan)
        affected = set(plan.live_closure)
        for release_file in manifest.files:
            _execute(cur, "SET LOCAL standard_conforming_strings = on")
            _execute(cur, release_file.sql)
        _restore_security(cur, before, affected)
        _run_validations(cur, manifest.validations)
        after = _read_snapshot(cur)
        _verify_post_snapshot(before, after, affected)
        conn.commit()
        return plan
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
