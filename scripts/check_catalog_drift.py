"""Report where the live warehouse catalog differs from the managed build.

The expected side is a disposable database built from
``src/schemas/warehouse-manifest.json`` (``WAREHOUSE_DB_URL``, loopback only).
The production side is ``SUPABASE_DB_URL``. Both are read through read-only
transactions; nothing is written to either database.

Usage:
    python scripts/check_catalog_drift.py --report drift.json --summary drift.md
    python scripts/check_catalog_drift.py --enforce

Differences fall into three classes:

* ``ingest_evolved`` -- what dlt adds on its own during a load: a nullable
  column without a default on a table it owns, or a new table it owns whose
  access matches its sibling tables. Reported, never a failure.
* ``accepted`` -- listed in ``src/schemas/drift-accepted.json`` with a reason.
* ``drift`` -- everything else.

Enforcement is on when the accepted file says ``"enforce": true`` or
``--enforce`` is passed. It exits 1 on any drift and on any accepted entry
that no longer applies. Otherwise the command reports and exits 0. A
connection or query failure exits 2 in either mode: an unchecked catalog is
not a clean one.

Run logs and artifacts of this repository are public. Unless
``--include-sensitive-details`` is passed, differences in grants, owners,
policies, default privileges and relation options are reported as counts per
schema only, with no object, role, privilege or fingerprint, and production's
deparsed SQL (functions, views, indexes, constraints, triggers, column
defaults) is replaced by its length and digest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from urllib.parse import urlsplit

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.bootstrap_warehouse import _redact_error, connect_database  # noqa: E402
from scripts.export_warehouse_catalog import PROJECT_SCHEMAS  # noqa: E402

PRODUCTION_ENV = "SUPABASE_DB_URL"
EXPECTED_ENV = "WAREHOUSE_DB_URL"
DEFAULT_ACCEPTED = REPO_ROOT / "src" / "schemas" / "drift-accepted.json"
LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
# Schemas the managed ledger creates beside the project schemas.
MANAGED_SCHEMA_PREFIX = "warehouse_"
ALL_SCHEMAS = "(all schemas)"
STATEMENT_TIMEOUT = "120s"
# Wait this long for a lock, then retry later, rather than queue behind a
# long mart refresh or a deploy.
LOCK_TIMEOUT = "5s"
CAPTURE_ATTEMPTS = 3
RETRY_SECONDS = 20
# Kinds that describe who can reach what.
ACCESS_KINDS = frozenset({"grant", "owner", "policy", "default_acl", "relation_options"})
# What dlt itself creates along with a table. Anything else found on a new
# table (a trigger, a policy) was put there by hand.
DLT_BUILT_KINDS = frozenset({"column", "column_order", "index", "constraint"})
TABLE_SHAPES = frozenset({"table", "partitioned table"})
DLT_STATE_TABLES = frozenset({"_dlt_loads", "_dlt_version", "_dlt_pipeline_state"})
# Kinds whose value is SQL text deparsed from the catalog. Production's copy
# may embed a literal this repository has never contained: a token in a
# trigger argument, a column default, a function body.
DEPARSED_KINDS = frozenset({"function", "definition", "index", "constraint", "trigger"})
COLUMN_DEFAULT = "; default "
SUMMARY_KEY_LIMIT = 100
UNREVIEWED_REASON = "TODO"

MISSING = "missing_in_production"
UNEXPECTED = "unexpected_in_production"
CHANGED = "changed"


class DriftCheckError(RuntimeError):
    """The comparison could not be completed or its configuration is unsafe."""


@dataclass(frozen=True)
class Item:
    """One catalog fact: a relation, a column, a grant, a definition."""

    kind: str
    key: str
    value: str
    schema: str
    relation: str | None = None


@dataclass(frozen=True)
class Difference:
    kind: str
    key: str
    status: str
    fingerprint: str
    schema: str
    relation: str | None
    expected: str | None
    production: str | None


Inventory = dict[tuple[str, str], Item]

_NOT_EXTENSION_MEMBER = (
    "NOT EXISTS (SELECT 1 FROM pg_depend ext WHERE ext.classid = '{catalog}'::regclass "
    "AND ext.objid = {oid} AND ext.deptype = 'e')"
)

_SCHEMAS_SQL = """
SELECT nspname FROM pg_namespace
WHERE nspname = ANY(%s) OR nspname LIKE %s ESCAPE '\\'
ORDER BY nspname
"""

_NAMESPACES_SQL = """
SELECT n.nspname, pg_get_userbyid(n.nspowner)
FROM pg_namespace n WHERE n.nspname = ANY(%s)
"""

_RELATIONS_SQL = f"""
SELECT n.nspname, c.relname, c.relkind, c.relpersistence,
       c.relrowsecurity, c.relforcerowsecurity,
       COALESCE(array_to_string(c.reloptions, ','), ''),
       CASE WHEN c.relispartition THEN pg_get_expr(c.relpartbound, c.oid) END,
       CASE WHEN c.relkind = 'p' THEN pg_get_partkeydef(c.oid) END,
       pg_get_userbyid(c.relowner),
       (SELECT inh.inhparent::regclass::text FROM pg_inherits inh
        WHERE inh.inhrelid = c.oid AND c.relispartition)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
  AND {_NOT_EXTENSION_MEMBER.format(catalog="pg_class", oid="c.oid")}
"""

# pg_get_viewdef locks each view and every relation it reads. It is the only
# statement here that locks user relations, so it runs last and alone, and its
# locks are released as soon as it returns.
_DEFINITIONS_SQL = f"""
SELECT n.nspname, c.relname, pg_get_viewdef(c.oid, true)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY(%s) AND c.relkind IN ('v', 'm')
  AND {_NOT_EXTENSION_MEMBER.format(catalog="pg_class", oid="c.oid")}
"""

_COLUMNS_SQL = f"""
SELECT n.nspname, c.relname, a.attname, format_type(a.atttypid, a.atttypmod),
       a.attnotnull, pg_get_expr(d.adbin, d.adrelid), a.attidentity, a.attgenerated,
       a.attnum
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
  AND a.attnum > 0 AND NOT a.attisdropped
  AND {_NOT_EXTENSION_MEMBER.format(catalog="pg_class", oid="c.oid")}
"""

_INDEXES_SQL = """
SELECT n.nspname, t.relname, i.relname, pg_get_indexdef(i.oid), ix.indisvalid
FROM pg_index ix
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_class t ON t.oid = ix.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = ANY(%s)
"""

_CONSTRAINTS_SQL = """
SELECT n.nspname, c.relname, con.conname, pg_get_constraintdef(con.oid, true)
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY(%s)
"""

_TRIGGERS_SQL = """
SELECT n.nspname, c.relname, t.tgname, pg_get_triggerdef(t.oid, true), t.tgenabled
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY(%s) AND NOT t.tgisinternal
"""

# Event triggers belong to the database, not a schema. Ours are the ones that
# call a function in a project schema; the platform's own are out of scope.
_EVENT_TRIGGERS_SQL = """
SELECT e.evtname, e.evtevent, e.evtenabled, n.nspname,
       format('%%s.%%s(%%s)', n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)),
       COALESCE(array_to_string(e.evttags, ', '), ''), pg_get_userbyid(e.evtowner)
FROM pg_event_trigger e
JOIN pg_proc p ON p.oid = e.evtfoid
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = ANY(%(schemas)s)
"""

_FUNCTIONS_SQL = f"""
SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), p.prokind,
       CASE WHEN p.prokind IN ('f', 'p') THEN pg_get_functiondef(p.oid) END,
       pg_get_function_result(p.oid), pg_get_userbyid(p.proowner)
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = ANY(%s)
  AND {_NOT_EXTENSION_MEMBER.format(catalog="pg_proc", oid="p.oid")}
"""

_POLICIES_SQL = """
SELECT schemaname, tablename, policyname, permissive, roles::text, cmd, qual, with_check
FROM pg_policies WHERE schemaname = ANY(%s)
"""

_ENUMS_SQL = f"""
SELECT n.nspname, t.typname, array_agg(e.enumlabel::text ORDER BY e.enumsortorder)
FROM pg_type t
JOIN pg_enum e ON e.enumtypid = t.oid
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = ANY(%s)
  AND {_NOT_EXTENSION_MEMBER.format(catalog="pg_type", oid="t.oid")}
GROUP BY 1, 2
"""

_EXTENSIONS_SQL = """
SELECT e.extname, n.nspname
FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
WHERE n.nspname = ANY(%s)
"""

# Explicit ACLs and the built-in defaults are folded together so a NULL acl and
# its spelled-out equivalent compare equal.
_GRANTS_SQL = f"""
WITH objects AS (
    SELECT 'schema' AS objtype, n.nspname::text AS schema, n.nspname::text AS objname,
           NULL::text AS relation, COALESCE(n.nspacl, acldefault('n', n.nspowner)) AS acl
    FROM pg_namespace n WHERE n.nspname = ANY(%(schemas)s)
    UNION ALL
    SELECT 'relation', n.nspname, format('%%s.%%s', n.nspname, c.relname),
           format('%%s.%%s', n.nspname, c.relname),
           COALESCE(c.relacl, acldefault(
               CASE WHEN c.relkind = 'S' THEN 's' ELSE 'r' END::"char", c.relowner))
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = ANY(%(schemas)s) AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
      AND {_NOT_EXTENSION_MEMBER.format(catalog="pg_class", oid="c.oid")}
    UNION ALL
    SELECT 'column', n.nspname, format('%%s.%%s.%%s', n.nspname, c.relname, a.attname),
           format('%%s.%%s', n.nspname, c.relname), a.attacl
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = ANY(%(schemas)s) AND a.attacl IS NOT NULL
      AND a.attnum > 0 AND NOT a.attisdropped
    UNION ALL
    SELECT 'function', n.nspname,
           format('%%s.%%s(%%s)', n.nspname, p.proname,
                  pg_get_function_identity_arguments(p.oid)),
           NULL, COALESCE(p.proacl, acldefault('f', p.proowner))
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = ANY(%(schemas)s)
      AND {_NOT_EXTENSION_MEMBER.format(catalog="pg_proc", oid="p.oid")}
)
SELECT o.objtype, o.schema, o.objname, o.relation, COALESCE(r.rolname, 'PUBLIC'),
       a.privilege_type, a.is_grantable
FROM objects o
CROSS JOIN LATERAL aclexplode(o.acl) a
LEFT JOIN pg_roles r ON r.oid = a.grantee
"""

# defaclnamespace = 0 is a default for every schema, including ours.
_DEFAULT_ACLS_SQL = """
SELECT pg_get_userbyid(d.defaclrole), n.nspname, d.defaclobjtype,
       CASE WHEN a.privilege_type IS NOT NULL THEN COALESCE(r.rolname, 'PUBLIC') END,
       a.privilege_type, a.is_grantable
FROM pg_default_acl d
LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace
LEFT JOIN LATERAL aclexplode(d.defaclacl) a ON true
LEFT JOIN pg_roles r ON r.oid = a.grantee
WHERE d.defaclnamespace = 0 OR n.nspname = ANY(%s)
"""

_RELKIND_NAMES = {
    "r": "table",
    "p": "partitioned table",
    "v": "view",
    "m": "materialized view",
    "f": "foreign table",
    "S": "sequence",
}


def _fetch(cur, statement: str, params) -> list[tuple]:
    cur.execute(statement, params)
    return cur.fetchall()


def _privileges(rows: list[tuple[str, bool]]) -> str:
    return ", ".join(sorted(f"{name}{'*' if grantable else ''}" for name, grantable in rows))


def _begin(cur) -> None:
    """Open the read-only transaction with limits that end with it."""
    cur.execute("SELECT set_config('statement_timeout', %s, true)", (STATEMENT_TIMEOUT,))
    cur.execute("SELECT set_config('lock_timeout', %s, true)", (LOCK_TIMEOUT,))
    # An empty search_path makes every deparsed name schema-qualified, so the
    # two databases render identical definitions identically.
    cur.execute("SELECT set_config('search_path', '', true)")


def _read_catalog(cur, items: Inventory) -> dict[str, object]:
    """Everything except view definitions. Takes no lock on any user relation."""

    def add(kind, key, value, schema, relation=None) -> None:
        items[(kind, key)] = Item(kind, key, "" if value is None else str(value), schema, relation)

    cur.execute("SHOW server_version")
    server_version = cur.fetchone()[0]
    pattern = MANAGED_SCHEMA_PREFIX.replace("_", "\\_") + "%"
    scope = [row[0] for row in _fetch(cur, _SCHEMAS_SQL, (sorted(PROJECT_SCHEMAS), pattern))]
    if not scope:
        raise DriftCheckError("no project schemas found; refusing to compare nothing")

    for schema, owner in _fetch(cur, _NAMESPACES_SQL, (scope,)):
        add("schema", schema, "present", schema)
        add("owner", f"schema {schema}", owner, schema)

    for row in _fetch(cur, _RELATIONS_SQL, (scope,)):
        schema, name, relkind, persistence, rls, force_rls, options, bound, partition_key = row[:9]
        relation = f"{schema}.{name}"
        shape = [_RELKIND_NAMES[relkind]]
        if persistence != "p":
            shape.append(f"persistence={persistence}")
        if partition_key:
            shape.append(f"partition by {partition_key}")
        if bound:
            shape += [f"partition of {row[10]}", f"bound {bound}"]
        add("relation", relation, "; ".join(shape), schema, relation)
        add(
            "relation_options",
            relation,
            f"row_security={rls}; force_row_security={force_rls}; options={options}",
            schema,
            relation,
        )
        add("owner", f"relation {relation}", row[9], schema, relation)

    positions: dict[tuple[str, str], list[tuple[int, str]]] = {}
    for row in _fetch(cur, _COLUMNS_SQL, (scope,)):
        schema, name, column, type_name, not_null, default, identity, generated, position = row
        relation = f"{schema}.{name}"
        positions.setdefault((schema, relation), []).append((position, column))
        parts = [type_name]
        if not_null:
            parts.append("not null")
        if default is not None:
            parts.append(f"default {default}")
        if identity:
            parts.append(f"identity={identity}")
        if generated:
            parts.append(f"generated={generated}")
        # A bare type means nullable with no default: the only column dlt adds.
        add("column", f"{relation}.{column}", "; ".join(parts), schema, relation)
    for (schema, relation), columns in positions.items():
        ordered = [column for _, column in sorted(columns)]
        add("column_order", relation, json.dumps(ordered), schema, relation)

    for schema, table, index, definition, valid in _fetch(cur, _INDEXES_SQL, (scope,)):
        value = definition if valid else f"{definition} [INVALID]"
        add("index", f"{schema}.{index}", value, schema, f"{schema}.{table}")

    for schema, table, name, definition in _fetch(cur, _CONSTRAINTS_SQL, (scope,)):
        relation = f"{schema}.{table}"
        add("constraint", f"{relation}.{name}", definition, schema, relation)

    for schema, table, name, definition, enabled in _fetch(cur, _TRIGGERS_SQL, (scope,)):
        relation = f"{schema}.{table}"
        value = f"{definition} [enabled={enabled}]"
        add("trigger", f"{relation}.{name}", value, schema, relation)

    for row in _fetch(cur, _EVENT_TRIGGERS_SQL, {"schemas": scope}):
        name, event, enabled, schema, function, tags, owner = row
        value = f"on {event} when tag in ({tags}) execute {function} [enabled={enabled}]"
        add("event_trigger", name, value, schema)
        add("owner", f"event trigger {name}", owner, schema)

    for row in _fetch(cur, _FUNCTIONS_SQL, (scope,)):
        schema, name, arguments, prokind, definition, result, owner = row
        signature = f"{schema}.{name}({arguments})"
        add("function", signature, definition or f"prokind={prokind} returns {result}", schema)
        add("owner", f"function {signature}", owner, schema)

    for row in _fetch(cur, _POLICIES_SQL, (scope,)):
        schema, table, name, permissive, roles, command, using, check = row
        relation = f"{schema}.{table}"
        value = f"{permissive} {command} to {roles} using ({using}) check ({check})"
        add("policy", f"{relation}.{name}", value, schema, relation)

    for schema, name, labels in _fetch(cur, _ENUMS_SQL, (scope,)):
        add("enum", f"{schema}.{name}", json.dumps(labels), schema)

    for name, schema in _fetch(cur, _EXTENSIONS_SQL, (scope,)):
        add("extension", name, f"schema {schema}", schema)

    grants: dict[tuple, list[tuple[str, bool]]] = {}
    for row in _fetch(cur, _GRANTS_SQL, {"schemas": scope}):
        objtype, schema, objname, relation, grantee, privilege, grantable = row
        identity = (f"{objtype} {objname} -> {grantee}", schema, relation)
        grants.setdefault(identity, []).append((privilege, grantable))
    for (key, schema, relation), privileges in grants.items():
        add("grant", key, _privileges(privileges), schema, relation)

    defaults: dict[tuple, list[tuple[str, bool]]] = {}
    for role, schema, objtype, grantee, privilege, grantable in _fetch(
        cur, _DEFAULT_ACLS_SQL, (scope,)
    ):
        where = f"schema {schema}" if schema else "all schemas"
        subject = f"{objtype} in {where} created by {role}"
        # A defaults entry that grants nothing still replaces the built-in defaults.
        add("default_acl", subject, "defined", schema or ALL_SCHEMAS)
        if grantee is not None:
            identity = (f"{subject} -> {grantee}", schema or ALL_SCHEMAS)
            defaults.setdefault(identity, []).append((privilege, grantable))
    for (key, schema), privileges in defaults.items():
        add("default_acl", key, _privileges(privileges), schema)

    return {"server_version": server_version, "schemas": scope}


def _read_definitions(cur, items: Inventory, scope: list[str]) -> None:
    for schema, name, definition in _fetch(cur, _DEFINITIONS_SQL, (scope,)):
        relation = f"{schema}.{name}"
        items[("definition", relation)] = Item(
            "definition", relation, definition or "", schema, relation
        )


def capture_inventory(conn) -> tuple[Inventory, dict[str, object]]:
    """Read every in-scope catalog fact from ``conn`` without writing or lingering.

    The catalog is read from one snapshot. View definitions follow in a second,
    short transaction so the locks they need are held only for that statement.
    """
    conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
    items: Inventory = {}
    try:
        with conn.cursor() as cur:
            _begin(cur)
            facts = _read_catalog(cur, items)
    finally:
        conn.rollback()
    try:
        with conn.cursor() as cur:
            _begin(cur)
            _read_definitions(cur, items, facts["schemas"])
    finally:
        conn.rollback()
    return items, facts | {"items": len(items)}


def _is_table(item: Item | None) -> bool:
    return item is not None and item.value.split(";", 1)[0] in TABLE_SHAPES


def dlt_managed_relations(inventory: Inventory) -> set[str]:
    """Tables dlt owns: its three state tables and tables keyed by its row id.

    dlt always declares ``_dlt_id`` NOT NULL. A view over such a table, or a
    ``CREATE TABLE AS`` copy of one, has the column without the constraint and
    is not dlt's.
    """
    managed: set[str] = set()
    for item in inventory.values():
        if item.kind == "column" and item.key.endswith("._dlt_id") and "; not null" in item.value:
            relation = item.relation
        elif item.kind == "relation" and item.key.split(".", 1)[1] in DLT_STATE_TABLES:
            relation = item.key
        else:
            continue
        if _is_table(inventory.get(("relation", relation))):
            managed.add(relation)
    return managed


def _by_relation(inventory: Inventory) -> dict[str, list[Item]]:
    grouped: dict[str, list[Item]] = {}
    for item in inventory.values():
        if item.relation is not None:
            grouped.setdefault(item.relation, []).append(item)
    return grouped


def _access_profile(relation: str, facts: list[Item]) -> tuple:
    """Who owns and can reach a relation, with the relation's own name removed."""
    return tuple(
        sorted(
            (item.kind, item.key.replace(relation, "", 1), item.value)
            for item in facts
            if item.kind in ACCESS_KINDS
        )
    )


def _fingerprint(kind: str, key: str, status: str, expected: str | None, production: str | None):
    payload = json.dumps([kind, key, status, expected, production], ensure_ascii=True)
    return hashlib.sha256(payload.encode("ascii")).hexdigest()[:16]


def diff_inventories(expected: Inventory, production: Inventory) -> list[Difference]:
    differences: list[Difference] = []
    for identity in sorted(expected.keys() | production.keys()):
        want, have = expected.get(identity), production.get(identity)
        if want is not None and have is not None and want.value == have.value:
            continue
        status = MISSING if have is None else UNEXPECTED if want is None else CHANGED
        kind, key = identity
        if kind == "column_order" and status == CHANGED and _same_order(want.value, have.value):
            continue
        want_value = want.value if want else None
        have_value = have.value if have else None
        differences.append(
            Difference(
                kind=kind,
                key=key,
                status=status,
                fingerprint=_fingerprint(kind, key, status, want_value, have_value),
                schema=(have or want).schema,
                relation=(have or want).relation,
                expected=want_value,
                production=have_value,
            )
        )
    return _fold_whole_relations(differences)


def _same_order(expected: str, production: str) -> bool:
    """Whether the columns both sides have appear in the same order.

    Added and missing columns are reported on their own; only a reordering of
    the shared ones is a difference in order.
    """
    want, have = json.loads(expected), json.loads(production)
    return [name for name in want if name in set(have)] == [
        name for name in have if name in set(want)
    ]


def _fold_whole_relations(differences: list[Difference]) -> list[Difference]:
    """Report a relation that exists on one side only as one difference.

    Its dependent facts are folded into a digest on that line, and the digest
    is part of the fingerprint, so an accepted extra table that later gains a
    column stops being accepted. Access facts of a relation found only in
    production are not folded: they stay separate differences, so they are
    classified, accepted and redacted on their own.
    """
    whole = {
        item.key: item.status
        for item in differences
        if item.kind == "relation" and item.status in (MISSING, UNEXPECTED)
    }
    dependents: dict[str, list[Difference]] = {}
    kept: list[Difference] = []
    for item in differences:
        folds = (
            item.kind != "relation"
            and whole.get(item.relation) == item.status
            and (item.status == MISSING or item.kind not in ACCESS_KINDS)
        )
        if folds:
            dependents.setdefault(item.relation, []).append(item)
        else:
            kept.append(item)
    folded: list[Difference] = []
    for item in kept:
        children = dependents.get(item.key) if item.kind == "relation" else None
        if children:
            facts = [
                [child.kind, child.key, child.expected, child.production] for child in children
            ]
            digest = hashlib.sha256(json.dumps(facts, ensure_ascii=True).encode("ascii"))
            note = f" [{len(children)} dependent facts, digest {digest.hexdigest()[:12]}]"
            expected = item.expected + note if item.expected is not None else None
            production = item.production + note if item.production is not None else None
            item = Difference(
                kind=item.kind,
                key=item.key,
                status=item.status,
                fingerprint=_fingerprint(item.kind, item.key, item.status, expected, production),
                schema=item.schema,
                relation=item.relation,
                expected=expected,
                production=production,
            )
        folded.append(item)
    return folded


def _column_parts(value: str) -> tuple[str, set[str]]:
    kind, *modifiers = value.split("; ")
    return kind, set(modifiers)


def _dlt_would_create(relation: str, production: Inventory, facts: list[Item], dlt, types) -> bool:
    """Whether a table found only in production is one dlt could have made.

    It must be a plain table, or a list partition of a dlt table. It may carry
    only columns, indexes, constraints and access facts. Each column must be of
    a type dlt already uses here with no modifier but NOT NULL, or, in a
    partition, be exactly the parent's column.
    """
    shape = production[("relation", relation)].value.split("; ")
    parent = None
    if shape != ["table"]:
        is_partition = (
            len(shape) == 3
            and shape[0] == "table"
            and shape[1].startswith("partition of ")
            and shape[2].startswith("bound FOR VALUES IN (")
        )
        parent = shape[1].removeprefix("partition of ") if is_partition else None
        if parent not in dlt:
            return False
    for item in facts:
        if item.kind not in DLT_BUILT_KINDS | ACCESS_KINDS | {"relation"}:
            return False
        if item.kind != "column":
            continue
        inherited = production.get(("column", f"{parent}.{item.key.rsplit('.', 1)[1]}"))
        if inherited is not None and inherited.value == item.value:
            continue
        kind, modifiers = _column_parts(item.value)
        if kind not in types or not modifiers <= {"not null"}:
            return False
    return True


def classify(
    differences: list[Difference], expected: Inventory, production: Inventory
) -> tuple[list[Difference], list[Difference]]:
    """Split differences into (drift, ingest_evolved).

    Only two additions count as dlt's own work, both in schemas where the
    managed build already has dlt tables:

    * on a dlt table both sides have, a nullable column without a default, of
      a type dlt already uses in the managed build;
    * a new table dlt could have made (see ``_dlt_would_create``) whose owner,
      grants and options match a dlt table of the same schema in the managed
      build. A new table that is reachable differently from its siblings is
      drift, and so is every access fact on it.

    A missing or altered dlt object is always drift.
    """
    expected_dlt = dlt_managed_relations(expected)
    dlt_schemas = {relation.split(".", 1)[0] for relation in expected_dlt}
    production_dlt = {
        relation
        for relation in dlt_managed_relations(production)
        if relation.split(".", 1)[0] in dlt_schemas
    }
    expected_facts, production_facts = _by_relation(expected), _by_relation(production)
    sibling_profiles: dict[str, set[tuple]] = {}
    dlt_types: set[str] = set()
    for relation in expected_dlt:
        facts = expected_facts[relation]
        profile = _access_profile(relation, facts)
        sibling_profiles.setdefault(relation.split(".", 1)[0], set()).add(profile)
        dlt_types |= {_column_parts(item.value)[0] for item in facts if item.kind == "column"}

    new_tables = set()
    for relation in production_dlt:
        if ("relation", relation) in expected:
            continue
        facts = production_facts[relation]
        siblings = sibling_profiles[relation.split(".", 1)[0]]
        if _access_profile(relation, facts) in siblings and _dlt_would_create(
            relation, production, facts, expected_dlt, dlt_types
        ):
            new_tables.add(relation)

    drift: list[Difference] = []
    evolved: list[Difference] = []
    for difference in differences:
        added_by_dlt = difference.status == UNEXPECTED and (
            difference.relation in new_tables
            or (
                difference.kind == "column"
                and difference.relation in production_dlt
                and difference.relation in expected_dlt
                and difference.production in dlt_types
            )
        )
        (evolved if added_by_dlt else drift).append(difference)
    return drift, evolved


def load_accepted(path: Path) -> tuple[bool, dict[str, dict[str, str]]]:
    """Return (enforce, accepted differences by fingerprint).

    The file must exist and must state ``enforce``: a renamed list or a
    dropped key would otherwise switch enforcement off without anyone
    deciding to.
    """
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DriftCheckError(f"{path.name} is not readable JSON: {exc}") from exc
    entries = document.get("accepted") if isinstance(document, dict) else None
    if not isinstance(entries, list):
        raise DriftCheckError(f"{path.name} must be an object with an 'accepted' list")
    # No default: a deleted or misspelled key must not switch enforcement off.
    enforce = document.get("enforce")
    if not isinstance(enforce, bool):
        raise DriftCheckError(f"{path.name} must state 'enforce' as true or false")
    accepted: dict[str, dict[str, str]] = {}
    for index, entry in enumerate(entries):
        required = ("fingerprint", "kind", "key", "status", "reason")
        if not isinstance(entry, dict) or any(
            not isinstance(entry.get(name), str) or not entry[name].strip() for name in required
        ):
            raise DriftCheckError(
                f"{path.name} entry {index} needs nonempty strings: " + ", ".join(required)
            )
        if entry["reason"].strip().casefold().startswith(UNREVIEWED_REASON.casefold()):
            raise DriftCheckError(
                f"{path.name} entry {index} ({entry['kind']} {entry['key']}) still has the "
                "placeholder reason; say why the difference is acceptable or remove it"
            )
        if entry["fingerprint"] in accepted:
            raise DriftCheckError(f"{path.name} repeats fingerprint {entry['fingerprint']}")
        accepted[entry["fingerprint"]] = entry
    return enforce, accepted


def propose_accepted(
    report: dict[str, object], accepted: dict[str, dict[str, str]], enforce: bool
) -> dict[str, object]:
    """The accepted list this report would need: what still applies, plus new drift.

    New entries carry a placeholder reason that ``load_accepted`` rejects, so a
    proposal cannot be committed without someone explaining each difference.
    Redacted access differences cannot be proposed; they are counted instead.
    """
    entries = []
    omitted = 0
    for section in ("accepted", "drift"):
        for entry in report[section]:
            if entry.get("redacted"):
                omitted += entry["count"]
                continue
            known = accepted.get(entry["fingerprint"])
            entries.append(
                {
                    "fingerprint": entry["fingerprint"],
                    "kind": entry["kind"],
                    "key": entry["key"],
                    "status": entry["status"],
                    "reason": known["reason"] if known else UNREVIEWED_REASON,
                }
            )
    proposal: dict[str, object] = {"version": 1, "enforce": enforce, "accepted": entries}
    if omitted:
        proposal["omitted_access_differences"] = omitted
    return proposal


def _is_deparsed(item: Difference) -> bool:
    """Whether production's value is SQL text rather than names, types and flags."""
    return item.kind in DEPARSED_KINDS or (
        item.kind == "column" and COLUMN_DEFAULT in item.production
    )


def _render(items: list[Difference], *, include_details: bool) -> list[dict[str, object]]:
    """Entries safe to publish unless details were asked for.

    Access differences are reduced to counts. Production's deparsed SQL (see
    ``DEPARSED_KINDS``, and any column with a default) is replaced by its
    length and digest.
    """
    rendered: list[dict[str, object]] = []
    hidden: dict[tuple[str, str, str], int] = {}
    for item in items:
        if item.kind in ACCESS_KINDS and not include_details:
            identity = (item.kind, item.schema, item.status)
            hidden[identity] = hidden.get(identity, 0) + 1
            continue
        production = item.production
        if production is not None and not include_details and _is_deparsed(item):
            digest = hashlib.sha256(production.encode("utf-8")).hexdigest()
            production = f"[withheld: {len(production)} characters, sha256 {digest}]"
        rendered.append(
            {
                "kind": item.kind,
                "key": item.key,
                "status": item.status,
                "fingerprint": item.fingerprint,
                "expected": item.expected,
                "production": production,
            }
        )
    for (kind, schema, status), count in sorted(hidden.items()):
        rendered.append(
            {"kind": kind, "schema": schema, "status": status, "count": count, "redacted": True}
        )
    return rendered


def build_report(
    expected: Inventory,
    production: Inventory,
    accepted: dict[str, dict[str, str]],
    *,
    expected_facts: dict[str, object],
    production_facts: dict[str, object],
    include_details: bool,
) -> dict[str, object]:
    drift, evolved = classify(diff_inventories(expected, production), expected, production)
    unaccepted = [item for item in drift if item.fingerprint not in accepted]
    known = [item for item in drift if item.fingerprint in accepted]
    for item in known:
        entry = accepted[item.fingerprint]
        if (entry["kind"], entry["key"], entry["status"]) != (item.kind, item.key, item.status):
            # Matching is by fingerprint, so a mislabelled entry would let a
            # reviewer approve one difference while reading about another.
            raise DriftCheckError(
                f"accepted entry {entry['kind']} {entry['key']} ({entry['status']}) is not "
                "what its fingerprint identifies; regenerate it from a report"
            )
    seen = {item.fingerprint for item in drift}
    stale = [entry for fingerprint, entry in sorted(accepted.items()) if fingerprint not in seen]
    return {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "expected": expected_facts,
        "production": production_facts,
        "details_included": include_details,
        "summary": {
            "drift": len(unaccepted),
            "accepted": len(known),
            "stale_accepted": len(stale),
            "ingest_evolved": len(evolved),
            "drift_by_kind": _count_by_kind(unaccepted),
            "ingest_evolved_by_kind": _count_by_kind(evolved),
        },
        "drift": _render(unaccepted, include_details=include_details),
        # Accepted entries are already published, by choice, in the accepted file.
        "accepted": [
            {
                "kind": item.kind,
                "key": item.key,
                "status": item.status,
                "fingerprint": item.fingerprint,
            }
            for item in known
        ],
        "stale_accepted": stale,
        "ingest_evolved": _render(evolved, include_details=include_details),
    }


def _count_by_kind(items: list[Difference]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        counts[item.kind] = counts.get(item.kind, 0) + 1
    return dict(sorted(counts.items()))


def render_summary(report: dict[str, object]) -> str:
    summary = report["summary"]
    lines = [
        "### Warehouse catalog drift",
        "",
        "| Class | Count |",
        "|---|---|",
        f"| Drift (not accepted) | {summary['drift']} |",
        f"| Accepted | {summary['accepted']} |",
        f"| Accepted entries that no longer apply | {summary['stale_accepted']} |",
        f"| Added by dlt during loads | {summary['ingest_evolved']} |",
        "",
        f"Production: PostgreSQL {report['production']['server_version']}, "
        f"{report['production']['items']} catalog facts. "
        f"Managed build: PostgreSQL {report['expected']['server_version']}, "
        f"{report['expected']['items']} catalog facts.",
    ]
    if not report["details_included"]:
        lines += [
            "",
            "Access differences are counted per schema, and production's deparsed SQL is withheld.",
        ]
    for title, section in (
        ("Drift", "drift"),
        ("Accepted entries that no longer apply", "stale_accepted"),
    ):
        entries = report[section]
        if not entries:
            continue
        lines += ["", f"#### {title}", ""]
        lines += ["| Kind | Object | Status | Fingerprint |", "|---|---|---|---|"]
        for entry in entries[:SUMMARY_KEY_LIMIT]:
            if entry.get("redacted"):
                what, fingerprint = f"{entry['count']} in schema {entry['schema']}", "withheld"
            else:
                what, fingerprint = f"`{entry['key']}`", f"`{entry['fingerprint']}`"
            lines.append(f"| {entry['kind']} | {what} | {entry['status']} | {fingerprint} |")
        if len(entries) > SUMMARY_KEY_LIMIT:
            more = len(entries) - SUMMARY_KEY_LIMIT
            lines.append(f"| ... | {more} more in the JSON report | | |")
    return "\n".join(lines) + "\n"


def _require_loopback(url: str) -> None:
    host = urlsplit(url).hostname
    if host not in LOOPBACK_HOSTS:
        raise DriftCheckError(
            f"{EXPECTED_ENV} must be a disposable loopback database; "
            "the managed build is never compared from a remote host"
        )


def _connect_production(url: str):
    import psycopg2

    return psycopg2.connect(url, connect_timeout=15)


def _safe_error(exc: Exception, url: str) -> str:
    """Exception text with the URL, its credentials and its host removed."""
    message = _redact_error(exc, url)
    try:
        host = urlsplit(url).hostname
    except ValueError:
        host = None
    if host:
        message = message.replace(host, "[REDACTED_HOST]")
    # libpq also prints the address it resolved the host to.
    message = re.sub(r"\(\d{1,3}(?:\.\d{1,3}){3}\)", "([REDACTED_ADDRESS])", message)
    return re.sub(r"\((?:[0-9A-Fa-f]{0,4}:){2,}[0-9A-Fa-f]{0,4}\)", "([REDACTED_ADDRESS])", message)


def _capture_with_retry(connect, sleep=time.sleep) -> tuple[Inventory, dict[str, object]]:
    """Capture on a fresh connection, retrying only when a lock was not available."""
    from psycopg2 import errors

    for attempt in range(1, CAPTURE_ATTEMPTS + 1):
        conn = connect()
        try:
            return capture_inventory(conn)
        except errors.LockNotAvailable:
            if attempt == CAPTURE_ATTEMPTS:
                raise
        finally:
            conn.close()
        sleep(RETRY_SECONDS)
    raise AssertionError("unreachable")


def run(args: argparse.Namespace) -> int:
    production_url = os.environ.get(PRODUCTION_ENV)
    expected_url = os.environ.get(EXPECTED_ENV)
    if not production_url or not expected_url:
        raise DriftCheckError(f"{PRODUCTION_ENV} and {EXPECTED_ENV} are both required")
    if production_url == expected_url:
        raise DriftCheckError("the two database URLs are identical; nothing to compare")
    _require_loopback(expected_url)
    file_enforces, accepted = load_accepted(args.accepted)
    enforce = args.enforce or file_enforces

    inventories = []
    for label, url, connect in (
        ("managed build", expected_url, connect_database),
        ("production", production_url, _connect_production),
    ):
        try:
            inventories.append(_capture_with_retry(partial(connect, url)))
        except DriftCheckError:
            raise
        except Exception as exc:
            raise DriftCheckError(
                f"could not read the {label} catalog: {_safe_error(exc, url)}"
            ) from None
    (expected, expected_facts), (production, production_facts) = inventories

    report = build_report(
        expected,
        production,
        accepted,
        expected_facts=expected_facts,
        production_facts=production_facts,
        include_details=args.include_sensitive_details,
    )
    if args.report:
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    summary_text = render_summary(report)
    if args.summary:
        with args.summary.open("a", encoding="utf-8") as stream:
            stream.write(summary_text)
    print(summary_text)
    if args.propose_accepted:
        proposal = propose_accepted(report, accepted, file_enforces)
        args.propose_accepted.write_text(json.dumps(proposal, indent=2) + "\n")

    summary = report["summary"]
    if enforce and (summary["drift"] or summary["stale_accepted"]):
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--accepted",
        type=Path,
        default=DEFAULT_ACCEPTED,
        help="Reviewed list of known differences (default: src/schemas/drift-accepted.json)",
    )
    parser.add_argument("--report", type=Path, help="Write the full JSON report here")
    parser.add_argument("--summary", type=Path, help="Append the Markdown summary here")
    parser.add_argument(
        "--enforce",
        action="store_true",
        help="Exit 1 on unaccepted drift or on accepted entries that no longer apply",
    )
    parser.add_argument(
        "--propose-accepted",
        type=Path,
        help="Write the accepted list this run would need, with placeholder reasons to fill in",
    )
    parser.add_argument(
        "--include-sensitive-details",
        action="store_true",
        help="Name objects and roles in access differences, and print production's deparsed SQL",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(args)
    except DriftCheckError as exc:
        print(f"catalog drift check failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
