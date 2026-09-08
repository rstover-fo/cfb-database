"""Catalog-safe audit and explicit repair for managed ``core.plays`` indexes.

The repair path is intentionally a maintenance operation.  PostgreSQL builds a
partitioned parent index and all missing leaf indexes non-concurrently, so writes
to ``core.plays`` are blocked for the duration of the authorized transaction.
Inspection is read only and is suitable for ingestion preflight checks.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict, dataclass

from psycopg2 import sql
from psycopg2.extensions import TRANSACTION_STATUS_IDLE

LOCK_TIMEOUT = "10s"
STATEMENT_TIMEOUT = "30min"
# Shared with play-partition maintenance so the two catalog operations serialize.
ADVISORY_LOCK_KEYS = (1_128_677_364, 80_008)


@dataclass(frozen=True)
class PlayIndexSpec:
    """Exact supported definition for one managed parent index."""

    columns: tuple[str, ...]
    unique: bool = False
    predicate: str | None = None
    method: str = "btree"


EXPECTED_PLAY_INDEXES: dict[str, PlayIndexSpec] = {
    "plays_dlt_id_unique": PlayIndexSpec(("_dlt_id", "season"), unique=True),
    "idx_plays_game_id": PlayIndexSpec(("game_id",)),
    "idx_plays_offense": PlayIndexSpec(("offense",)),
    "idx_plays_defense": PlayIndexSpec(("defense",)),
    "idx_plays_offense_season": PlayIndexSpec(("offense", "season")),
    "idx_plays_defense_season": PlayIndexSpec(("defense", "season")),
    "idx_plays_play_type": PlayIndexSpec(("play_type",)),
    "idx_plays_score_diff": PlayIndexSpec(("score_diff",)),
    "idx_plays_competitive": PlayIndexSpec(
        ("game_id", "period"),
        predicate="abs((offense_score - defense_score)) <= 28",
    ),
}


class PlayIndexStateError(RuntimeError):
    """The selected play-index catalog is unhealthy or unsafe to repair."""

    def __init__(self, message: str, report: dict[str, object] | None = None):
        super().__init__(message)
        self.report = report


def _selected_names(index_names: Iterable[str] | None) -> tuple[str, ...]:
    if index_names is None:
        return tuple(EXPECTED_PLAY_INDEXES)
    requested = tuple(index_names)
    unknown = sorted({name for name in requested if name not in EXPECTED_PLAY_INDEXES})
    if unknown:
        raise ValueError("unsupported core.plays index: " + ", ".join(unknown))
    requested_set = set(requested)
    return tuple(name for name in EXPECTED_PLAY_INDEXES if name in requested_set)


def _fetch_relations(cur: object, names: tuple[str, ...]) -> list[tuple[object, ...]]:
    cur.execute(
        """
        SELECT relation.oid, relation.relname, relation.relkind,
               relation.relispartition, pg_catalog.pg_get_userbyid(relation.relowner),
               partitioned.partstrat, partitioned.partnatts,
               ARRAY(
                   SELECT attribute.attname
                   FROM unnest(partitioned.partattrs::smallint[]) WITH ORDINALITY
                       AS partition_key(attnum, key_order)
                   LEFT JOIN pg_catalog.pg_attribute attribute
                     ON attribute.attrelid = relation.oid
                    AND attribute.attnum = partition_key.attnum
                   ORDER BY partition_key.key_order
               )
        FROM pg_catalog.pg_class relation
        JOIN pg_catalog.pg_namespace namespace
          ON namespace.oid = relation.relnamespace
        LEFT JOIN pg_catalog.pg_partitioned_table partitioned
          ON partitioned.partrelid = relation.oid
        WHERE namespace.nspname = %s AND relation.relname = ANY(%s)
        ORDER BY relation.relname
        """,
        ("core", ["plays", "plays_old", *names]),
    )
    return list(cur.fetchall())


def _fetch_leaf_partitions(cur: object, parent_oid: int) -> list[tuple[object, ...]]:
    cur.execute(
        """
        WITH RECURSIVE descendants(oid) AS (
            SELECT inheritance.inhrelid
            FROM pg_catalog.pg_inherits inheritance
            WHERE inheritance.inhparent = %s
          UNION ALL
            SELECT inheritance.inhrelid
            FROM pg_catalog.pg_inherits inheritance
            JOIN descendants parent ON parent.oid = inheritance.inhparent
        )
        SELECT child.oid, namespace.nspname, child.relname, child.relkind,
               child.relispartition,
               pg_catalog.pg_get_expr(child.relpartbound, child.oid)
        FROM descendants
        JOIN pg_catalog.pg_class child ON child.oid = descendants.oid
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid = child.relnamespace
        WHERE NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_inherits nested
            WHERE nested.inhparent = child.oid
        )
        ORDER BY namespace.nspname, child.relname
        """,
        (parent_oid,),
    )
    return list(cur.fetchall())


def _fetch_indexes(
    cur: object,
    table_oids: tuple[int, ...],
    named_index_oids: tuple[int, ...],
) -> list[tuple[object, ...]]:
    if not table_oids and not named_index_oids:
        return []
    cur.execute(
        """
        SELECT index_relation.oid, index_namespace.nspname, index_relation.relname,
               index_relation.relkind,
               pg_catalog.pg_get_userbyid(index_relation.relowner),
               indexed_table.oid, table_namespace.nspname, indexed_table.relname,
               indexed_table.relkind, pg_catalog.pg_get_userbyid(indexed_table.relowner),
               access_method.amname,
               catalog_index.indisunique, catalog_index.indisvalid,
               catalog_index.indisready, catalog_index.indislive,
               catalog_index.indisprimary, catalog_index.indisexclusion,
               catalog_index.indimmediate,
               catalog_index.indnullsnotdistinct,
               catalog_index.indnatts, catalog_index.indnkeyatts,
               pg_catalog.pg_get_expr(catalog_index.indexprs, catalog_index.indrelid),
               pg_catalog.pg_get_expr(catalog_index.indpred, catalog_index.indrelid),
               pg_catalog.pg_get_indexdef(index_relation.oid),
               ARRAY(
                   SELECT attribute.attname
                   FROM generate_series(0, catalog_index.indnkeyatts - 1)
                       AS key_position(position)
                   LEFT JOIN pg_catalog.pg_attribute attribute
                     ON attribute.attrelid = indexed_table.oid
                    AND attribute.attnum = catalog_index.indkey[position]
                   ORDER BY position
               ),
               ARRAY(
                   SELECT catalog_index.indcollation[position] = attribute.attcollation
                   FROM generate_series(0, catalog_index.indnkeyatts - 1)
                       AS key_position(position)
                   LEFT JOIN pg_catalog.pg_attribute attribute
                     ON attribute.attrelid = indexed_table.oid
                    AND attribute.attnum = catalog_index.indkey[position]
                   ORDER BY position
               ),
               ARRAY(
                   SELECT operator_class.opcdefault
                   FROM generate_series(0, catalog_index.indnkeyatts - 1)
                       AS key_position(position)
                   LEFT JOIN pg_catalog.pg_opclass operator_class
                     ON operator_class.oid = catalog_index.indclass[position]
                    AND operator_class.opcmethod = index_relation.relam
                   ORDER BY position
               ),
               ARRAY(
                   SELECT catalog_index.indoption[position]
                   FROM generate_series(0, catalog_index.indnkeyatts - 1)
                       AS key_position(position)
                   ORDER BY position
               ),
               ARRAY(
                   SELECT inheritance.inhparent
                   FROM pg_catalog.pg_inherits inheritance
                   WHERE inheritance.inhrelid = index_relation.oid
                   ORDER BY inheritance.inhparent
               ),
               ARRAY(
                   SELECT dependency_namespace.nspname || '.' || dependency_proc.proname
                   FROM pg_catalog.pg_depend dependency
                   JOIN pg_catalog.pg_proc dependency_proc
                     ON dependency.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                    AND dependency.refclassid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                    AND dependency.refobjid = dependency_proc.oid
                   JOIN pg_catalog.pg_namespace dependency_namespace
                     ON dependency_namespace.oid = dependency_proc.pronamespace
                   WHERE dependency.objid = index_relation.oid
                   ORDER BY 1
               ),
               ARRAY(
                   SELECT dependency_namespace.nspname || '.' || dependency_operator.oprname
                   FROM pg_catalog.pg_depend dependency
                   JOIN pg_catalog.pg_operator dependency_operator
                     ON dependency.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                    AND dependency.refclassid = 'pg_catalog.pg_operator'::pg_catalog.regclass
                    AND dependency.refobjid = dependency_operator.oid
                   JOIN pg_catalog.pg_namespace dependency_namespace
                     ON dependency_namespace.oid = dependency_operator.oprnamespace
                   WHERE dependency.objid = index_relation.oid
                   ORDER BY 1
               )
        FROM pg_catalog.pg_index catalog_index
        JOIN pg_catalog.pg_class index_relation
          ON index_relation.oid = catalog_index.indexrelid
        JOIN pg_catalog.pg_namespace index_namespace
          ON index_namespace.oid = index_relation.relnamespace
        JOIN pg_catalog.pg_class indexed_table
          ON indexed_table.oid = catalog_index.indrelid
        JOIN pg_catalog.pg_namespace table_namespace
          ON table_namespace.oid = indexed_table.relnamespace
        JOIN pg_catalog.pg_am access_method ON access_method.oid = index_relation.relam
        WHERE indexed_table.oid = ANY(%s) OR index_relation.oid = ANY(%s)
        ORDER BY index_namespace.nspname, index_relation.relname
        """,
        (list(table_oids), list(named_index_oids)),
    )
    return list(cur.fetchall())


def _relation_payload(row: tuple[object, ...]) -> dict[str, object]:
    oid, name, relkind, is_partition, owner, strategy, key_count, key_names = row
    return {
        "oid": int(oid),
        "schema": "core",
        "name": str(name),
        "relkind": str(relkind),
        "is_partition": bool(is_partition),
        "role_owner": str(owner),
        "partition_strategy": strategy,
        "partition_key_count": key_count,
        "partition_keys": list(key_names or []),
    }


def _index_payload(row: tuple[object, ...]) -> dict[str, object]:
    (
        oid,
        schema_name,
        name,
        relkind,
        role_owner,
        table_oid,
        table_schema,
        table_name,
        table_relkind,
        table_role_owner,
        method,
        unique,
        valid,
        ready,
        live,
        primary,
        exclusion,
        immediate,
        nulls_not_distinct,
        attribute_count,
        key_count,
        expressions,
        predicate,
        definition,
        keys,
        default_collations,
        default_operator_classes,
        options,
        parent_index_oids,
        function_dependencies,
        operator_dependencies,
    ) = row
    return {
        "oid": int(oid),
        "schema": str(schema_name),
        "name": str(name),
        "relkind": str(relkind),
        "role_owner": str(role_owner),
        "target": {
            "oid": int(table_oid),
            "schema": str(table_schema),
            "name": str(table_name),
            "relkind": str(table_relkind),
            "role_owner": str(table_role_owner),
        },
        "method": str(method),
        "unique": bool(unique),
        "valid": bool(valid),
        "ready": bool(ready),
        "live": bool(live),
        "primary": bool(primary),
        "exclusion": bool(exclusion),
        "immediate": bool(immediate),
        "nulls_not_distinct": bool(nulls_not_distinct),
        "attribute_count": int(attribute_count),
        "key_count": int(key_count),
        "expressions": expressions,
        "predicate": predicate,
        "definition": str(definition),
        "keys": list(keys or []),
        "default_collations": list(default_collations or []),
        "default_operator_classes": list(default_operator_classes or []),
        "options": list(options or []),
        "parent_index_oids": [int(value) for value in (parent_index_oids or [])],
        "function_dependencies": list(function_dependencies or []),
        "operator_dependencies": list(operator_dependencies or []),
    }


def _normalized_predicate(value: object) -> str | None:
    if value is None:
        return None
    normalized = "".join(str(value).split())
    # pg_get_expr qualifies pinned built-ins when a caller's search_path contains a
    # shadowing object.  Dependency checks below still reject the shadowing object.
    normalized = normalized.replace("pg_catalog.abs", "abs")
    normalized = normalized.replace("OPERATOR(pg_catalog.-)", "-")
    normalized = normalized.replace("OPERATOR(pg_catalog.<=)", "<=")
    while normalized.startswith("(") and normalized.endswith(")"):
        depth = 0
        encloses_all = True
        for offset, character in enumerate(normalized):
            if character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
                if depth == 0 and offset != len(normalized) - 1:
                    encloses_all = False
                    break
        if not encloses_all:
            break
        normalized = normalized[1:-1]
    return normalized


def _structure_problems(actual: dict[str, object], spec: PlayIndexSpec) -> list[str]:
    problems: list[str] = []
    if actual["method"] != spec.method:
        problems.append(f"access method is {actual['method']!r}, expected {spec.method!r}")
    if actual["unique"] is not spec.unique:
        problems.append(f"unique is {actual['unique']!r}, expected {spec.unique!r}")
    if tuple(actual["keys"]) != spec.columns:
        problems.append(f"keys are {actual['keys']!r}, expected {list(spec.columns)!r}")
    if actual["attribute_count"] != actual["key_count"]:
        problems.append("INCLUDE columns are not allowed")
    if actual["expressions"] is not None or any(key is None for key in actual["keys"]):
        problems.append("index expressions are not allowed")
    if _normalized_predicate(actual["predicate"]) != _normalized_predicate(spec.predicate):
        problems.append(f"predicate is {actual['predicate']!r}, expected {spec.predicate!r}")
    # Built-in pg_catalog functions/operators are pinned and do not produce pg_depend rows.
    # Any row here therefore identifies a shadowed or otherwise non-built-in dependency.
    expected_functions: list[str] = []
    expected_operators: list[str] = []
    if actual["function_dependencies"] != expected_functions:
        problems.append(
            "predicate function dependencies are "
            f"{actual['function_dependencies']!r}, expected {expected_functions!r}"
        )
    if actual["operator_dependencies"] != expected_operators:
        problems.append(
            "predicate operator dependencies are "
            f"{actual['operator_dependencies']!r}, expected {expected_operators!r}"
        )
    if not all(value is True for value in actual["default_collations"]):
        problems.append("non-default collation is not allowed")
    if not all(value is True for value in actual["default_operator_classes"]):
        problems.append("non-default operator class is not allowed")
    if any(value != 0 for value in actual["options"]):
        problems.append("non-default sort or NULL ordering is not allowed")
    if actual["nulls_not_distinct"]:
        problems.append("NULLS NOT DISTINCT is not allowed")
    if actual["primary"] or actual["exclusion"]:
        problems.append("primary/exclusion constraint indexes are not managed here")
    if not actual["immediate"]:
        problems.append("deferred uniqueness semantics are not allowed")
    return problems


def _readiness_problems(actual: dict[str, object]) -> list[str]:
    return [
        label
        for field, label in (
            ("valid", "index is not valid"),
            ("ready", "index is not ready"),
            ("live", "index is not live"),
        )
        if not actual[field]
    ]


def inspect_play_indexes(
    cur: object,
    index_names: Iterable[str] | None = None,
) -> dict[str, object]:
    """Return a JSON-serializable catalog report for selected managed indexes."""

    selected = _selected_names(index_names)
    relation_rows = _fetch_relations(cur, selected)
    relations = {str(row[1]): _relation_payload(row) for row in relation_rows}
    plays = relations.get("plays")
    old = relations.get("plays_old")
    catalog_issues: list[str] = []
    parent_oid: int | None = None
    if plays is None:
        catalog_issues.append("core.plays is missing")
    elif (
        plays["relkind"] != "p"
        or plays["partition_strategy"] != "l"
        or plays["partition_key_count"] != 1
        or plays["partition_keys"] != ["season"]
    ):
        catalog_issues.append("core.plays must be partitioned by exactly LIST (season)")
    else:
        parent_oid = int(plays["oid"])

    leaf_rows = _fetch_leaf_partitions(cur, parent_oid) if parent_oid is not None else []
    partitions: list[dict[str, object]] = []
    for oid, schema_name, name, relkind, is_partition, bound in leaf_rows:
        partition = {
            "oid": int(oid),
            "schema": str(schema_name),
            "name": str(name),
            "relkind": str(relkind),
            "is_partition": bool(is_partition),
            "bound": bound,
        }
        partitions.append(partition)
        if schema_name != "core" or relkind != "r" or not is_partition:
            catalog_issues.append(
                f"attached leaf {schema_name}.{name} is not an ordinary core partition"
            )

    table_oids = tuple(
        dict.fromkeys(
            [
                *([int(plays["oid"])] if plays is not None else []),
                *([int(old["oid"])] if old is not None else []),
                *(int(partition["oid"]) for partition in partitions),
            ]
        )
    )
    named_index_oids = tuple(
        int(relations[name]["oid"])
        for name in selected
        if name in relations and relations[name]["relkind"] in {"i", "I"}
    )
    all_indexes = [_index_payload(row) for row in _fetch_indexes(cur, table_oids, named_index_oids)]
    indexes_by_name = {str(index["name"]): index for index in all_indexes}
    partition_oids = {int(partition["oid"]): partition for partition in partitions}

    reports: dict[str, dict[str, object]] = {}
    issue_summary: dict[str, list[str]] = {}
    for name in selected:
        spec = EXPECTED_PLAY_INDEXES[name]
        expected = {"name": name, **asdict(spec)}
        expected["columns"] = list(spec.columns)
        relation = relations.get(name)
        actual = indexes_by_name.get(name)
        issue_codes: list[str] = []
        diagnostics: list[str] = []

        if relation is not None and relation["relkind"] not in {"i", "I"}:
            issue_codes.append("wrong_structure")
            diagnostics.append(f"core.{name} is relkind={relation['relkind']!r}, not an index")
        elif actual is None:
            issue_codes.append("missing")
            diagnostics.append(f"core.{name} is missing")
        else:
            target = actual["target"]
            if target["schema"] != "core" or target["name"] != "plays":
                issue_codes.append("wrong_owner")
                diagnostics.append(
                    f"core.{name} targets {target['schema']}.{target['name']}, expected core.plays"
                )
            structural = _structure_problems(actual, spec)
            if actual["relkind"] != "I" and target["name"] == "plays":
                structural.append(
                    f"parent index relkind is {actual['relkind']!r}, expected partitioned index"
                )
            if structural:
                issue_codes.append("wrong_structure")
                diagnostics.extend(structural)
            readiness = _readiness_problems(actual)
            if readiness:
                issue_codes.append("invalid")
                diagnostics.extend(readiness)

        coverage = {
            "expected_partition_count": len(partitions),
            "attached_partition_count": 0,
            "missing_partitions": [],
            "invalid_partitions": [],
            "wrong_structure_partitions": [],
            "unexpected_targets": [],
        }
        if actual is not None and actual["target"]["name"] == "plays":
            parent_index_oid = int(actual["oid"])
            attached = [
                index for index in all_indexes if parent_index_oid in index["parent_index_oids"]
            ]
            attached_by_target: dict[int, list[dict[str, object]]] = {}
            for child_index in attached:
                target_oid = int(child_index["target"]["oid"])
                attached_by_target.setdefault(target_oid, []).append(child_index)
                if target_oid not in partition_oids:
                    coverage["unexpected_targets"].append(child_index["target"])
            for partition_oid, partition in partition_oids.items():
                child_indexes = attached_by_target.get(partition_oid, [])
                if len(child_indexes) != 1:
                    coverage["missing_partitions"].append(partition["name"])
                    if len(child_indexes) > 1:
                        coverage["wrong_structure_partitions"].append(partition["name"])
                    continue
                child_index = child_indexes[0]
                coverage["attached_partition_count"] += 1
                child_structure = _structure_problems(child_index, spec)
                if child_structure or child_index["relkind"] != "i":
                    coverage["wrong_structure_partitions"].append(partition["name"])
                if _readiness_problems(child_index):
                    coverage["invalid_partitions"].append(partition["name"])
            if any(
                coverage[key]
                for key in (
                    "missing_partitions",
                    "invalid_partitions",
                    "wrong_structure_partitions",
                    "unexpected_targets",
                )
            ):
                issue_codes.append("missing_child_coverage")
                diagnostics.append("attached child-index coverage is incomplete or unhealthy")

        # A differently named equivalent live parent is never silently accepted or duplicated,
        # including when the canonical name still belongs to the retained old heap.
        if parent_oid is not None and (actual is None or actual["target"]["oid"] != parent_oid):
            equivalent = [
                index
                for index in all_indexes
                if index["target"]["oid"] == parent_oid
                and not _structure_problems(index, spec)
                and index["name"] != name
            ]
            if equivalent:
                if "wrong_structure" not in issue_codes:
                    issue_codes.append("wrong_structure")
                diagnostics.append(
                    "equivalent differently named parent index requires manual review: "
                    + ", ".join(sorted(str(index["name"]) for index in equivalent))
                )

        deduped_codes = list(dict.fromkeys(issue_codes))
        reports[name] = {
            "healthy": not deduped_codes and not catalog_issues,
            "expected": expected,
            "actual": actual,
            "issues": deduped_codes,
            "diagnostics": diagnostics,
            "child_coverage": coverage,
        }
        if deduped_codes:
            issue_summary[name] = deduped_codes

    return {
        "valid": not catalog_issues and not issue_summary,
        "selected_indexes": list(selected),
        "catalog_issues": catalog_issues,
        "plays": plays,
        "plays_old": old,
        "partition_count": len(partitions),
        "partitions": partitions,
        "issues": issue_summary,
        "indexes": reports,
    }


def validate_play_indexes(
    cur: object,
    index_names: Iterable[str] | None = None,
) -> dict[str, object]:
    """Return the report or raise when any selected index is unhealthy."""

    report = inspect_play_indexes(cur, index_names)
    if not report["valid"]:
        affected = ", ".join(report["issues"]) or "catalog"
        raise PlayIndexStateError(f"unhealthy core.plays indexes: {affected}", report)
    return report


def _ensure_idle_connection(conn: object) -> None:
    if getattr(conn, "autocommit", False):
        raise RuntimeError("play-index maintenance requires autocommit=False")
    get_status = getattr(conn, "get_transaction_status", None)
    if get_status is not None and get_status() != TRANSACTION_STATUS_IDLE:
        raise RuntimeError("play-index maintenance requires an idle connection")


def _repair_kind(name: str, report: dict[str, object]) -> str:
    if report["catalog_issues"]:
        raise PlayIndexStateError(
            "core.plays catalog is not safely repairable: " + "; ".join(report["catalog_issues"]),
            report,
        )
    index_report = report["indexes"][name]
    if index_report["healthy"]:
        return "healthy"
    issues = set(index_report["issues"])
    actual = index_report["actual"]
    if actual is None and issues == {"missing"}:
        return "missing"
    old = report["plays_old"]
    if (
        actual is not None
        and issues == {"wrong_owner"}
        and actual["target"]["schema"] == "core"
        and actual["target"]["name"] == "plays_old"
        and old is not None
        and old["relkind"] == "r"
        and not old["is_partition"]
    ):
        return "retained_old"
    raise PlayIndexStateError(
        f"core.{name} is not safely repairable: "
        + ", ".join(index_report["issues"])
        + (
            "; " + "; ".join(index_report["diagnostics"][:4]) if index_report["diagnostics"] else ""
        ),
        report,
    )


def _fetch_collisions(cur: object, names: list[str]) -> list[tuple[object, ...]]:
    if not names:
        return []
    cur.execute(
        """
        SELECT relation.relname, relation.relkind
        FROM pg_catalog.pg_class relation
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = %s AND relation.relname = ANY(%s)
        ORDER BY relation.relname
        """,
        ("core", names),
    )
    return list(cur.fetchall())


def _fetch_named_index_identity(cur: object, name: str) -> tuple[int, int] | None:
    cur.execute(
        """
        SELECT index_relation.oid, catalog_index.indrelid
        FROM pg_catalog.pg_class index_relation
        JOIN pg_catalog.pg_namespace namespace
          ON namespace.oid = index_relation.relnamespace
        JOIN pg_catalog.pg_index catalog_index
          ON catalog_index.indexrelid = index_relation.oid
        WHERE namespace.nspname = %s AND index_relation.relname = %s
        """,
        ("core", name),
    )
    rows = list(cur.fetchall())
    if not rows:
        return None
    if len(rows) != 1:
        raise PlayIndexStateError(f"expected exactly one core.{name} index")
    return int(rows[0][0]), int(rows[0][1])


def _create_parent_index(cur: object, name: str, spec: PlayIndexSpec) -> None:
    unique = sql.SQL("UNIQUE ") if spec.unique else sql.SQL("")
    predicate = (
        sql.SQL(
            " WHERE pg_catalog.abs((offense_score OPERATOR(pg_catalog.-) defense_score)) "
            "OPERATOR(pg_catalog.<=) 28"
        )
        if spec.predicate is not None
        else sql.SQL("")
    )
    cur.execute(
        sql.SQL("CREATE {}INDEX {} ON {}.{} USING {} ({}){}").format(
            unique,
            sql.Identifier(name),
            sql.Identifier("core"),
            sql.Identifier("plays"),
            sql.Identifier(spec.method),
            sql.SQL(", ").join(sql.Identifier(column) for column in spec.columns),
            predicate,
        )
    )


def repair_play_indexes(conn: object, index_names: Iterable[str]) -> dict[str, object]:
    """Atomically repair an explicit, nonempty selection of managed indexes.

    Only a wholly missing expected index or an exact valid copy still targeting a
    verified ``core.plays_old`` heap is repaired.  Every other state fails closed.
    The non-concurrent parent builds block writes during this maintenance window.
    """

    if index_names is None:
        raise ValueError("play-index repair requires an explicit index selection")
    selected = _selected_names(index_names)
    if not selected:
        raise ValueError("play-index repair requires at least one explicit --index selection")
    _ensure_idle_connection(conn)
    cur = conn.cursor()
    repaired: list[str] = []
    renamed: dict[str, str] = {}
    try:
        cur.execute("BEGIN ISOLATION LEVEL READ COMMITTED")
        cur.execute("SET LOCAL lock_timeout = %s", (LOCK_TIMEOUT,))
        cur.execute("SET LOCAL statement_timeout = %s", (STATEMENT_TIMEOUT,))
        cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", ADVISORY_LOCK_KEYS)
        # Locking without ONLY covers every current partition.  SHARE ROW EXCLUSIVE
        # prevents writes and competing catalog DDL while permitting readers.
        cur.execute("LOCK TABLE core.plays IN SHARE ROW EXCLUSIVE MODE")
        before = inspect_play_indexes(cur, selected)
        preliminary_kinds = {name: _repair_kind(name, before) for name in selected}
        if "retained_old" in preliminary_kinds.values():
            cur.execute("LOCK TABLE core.plays_old IN SHARE ROW EXCLUSIVE MODE")
            # Lock acquisition may have waited.  Repeat the complete preflight afterward.
            before = inspect_play_indexes(cur, selected)
        kinds = {name: _repair_kind(name, before) for name in selected}
        rename_targets = [
            f"plays_old_{name}" for name, kind in kinds.items() if kind == "retained_old"
        ]
        collisions = _fetch_collisions(cur, rename_targets)
        if collisions:
            details = ", ".join(f"core.{name} (relkind={kind})" for name, kind in collisions)
            raise PlayIndexStateError(
                f"retained-old index rename target already exists: {details}", before
            )

        # All selected states and rename targets are validated before the first DDL.
        for name in selected:
            kind = kinds[name]
            if kind == "healthy":
                continue
            if kind == "retained_old":
                renamed_name = f"plays_old_{name}"
                expected_index_oid = int(before["indexes"][name]["actual"]["oid"])
                expected_old_oid = int(before["plays_old"]["oid"])
                cur.execute(
                    sql.SQL("ALTER INDEX {}.{} RENAME TO {}").format(
                        sql.Identifier("core"),
                        sql.Identifier(name),
                        sql.Identifier(renamed_name),
                    )
                )
                renamed_identity = _fetch_named_index_identity(cur, renamed_name)
                if renamed_identity != (expected_index_oid, expected_old_oid):
                    raise PlayIndexStateError(
                        f"core.{name} changed identity during retained-old rename; "
                        "rolling back maintenance",
                        before,
                    )
                renamed[name] = renamed_name
            _create_parent_index(cur, name, EXPECTED_PLAY_INDEXES[name])
            repaired.append(name)

        after = validate_play_indexes(cur, selected)
        conn.commit()
        after["repaired_indexes"] = repaired
        after["renamed_old_indexes"] = renamed
        after["lock_timeout"] = LOCK_TIMEOUT
        after["statement_timeout"] = STATEMENT_TIMEOUT
        return after
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
