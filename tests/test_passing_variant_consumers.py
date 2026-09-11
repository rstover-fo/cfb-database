"""Static guards for maintained consumers of passing player-season variants.

The inventory is deliberately bounded to literal references in maintained SQL
definitions and SQL-bearing runtime Python. Historical baselines, adoptions,
migrations, and test fixtures are outside that boundary. Fully dynamic SQL and
external readers still require manual review.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any

import pytest
from pglast import parser

from src.pipelines.utils.variant_twins import REVIEWED_RAW_ONLY_VARIANT_TWINS

ROOT = Path(__file__).resolve().parents[1]
SOURCE_RELATION = "stats.passing_player_season"
SOURCE_SCHEMA, SOURCE_TABLE = SOURCE_RELATION.split(".")
SOURCE_REFERENCE = re.compile(
    r'(?<![A-Za-z0-9_])(?:"stats"|stats)\s*\.\s*'
    r'(?:"passing_player_season"|passing_player_season)(?![A-Za-z0-9_])',
    re.IGNORECASE,
)
MART_RELATION = "marts.passing_charting_player_season"
MART_PATH = Path("src/schemas/marts/045_passing_charting_player_season.sql")
API_PATH = Path("src/schemas/api/045_passing_charting_player_season.sql")

# These roots intentionally omit historical baselines, adoptions, migrations,
# and tests. Python roots are scanned because maintained SQL can live in string
# literals there as well as in .sql files.
MAINTAINED_ROOTS = (
    Path("src/schemas/marts"),
    Path("src/schemas/api"),
    Path("src/schemas/public"),
    Path("src/schemas/functions"),
    Path("scripts"),
    Path("src/pipelines"),
    Path("mcp"),
)

MAPPED_SOURCE_CONSUMERS = {MART_PATH}
REVIEWED_NON_METRIC_REFERENCES = {
    API_PATH: "comment documenting the API view's indirect source",
    Path("src/schemas/api/validation_expansion_views.sql"): (
        "validation error text; executable SQL reads the API view"
    ),
    Path("src/pipelines/config/refresh_assets.py"): "refresh dependency registration",
    Path("src/pipelines/utils/variant_twins.py"): "variant classification registry",
}

PASSING_API_COLUMNS = (
    "season",
    "player_id",
    "player",
    "team",
    "conference",
    "position",
    "attempts",
    "completions",
    "interceptions",
    "completion_rate",
    "total_air_yards",
    "average_depth_of_target",
    "air_yards_attempts_available",
    "total_yards_after_catch",
    "average_yards_after_catch",
    "yards_after_catch_attempts_available",
)

# Unique raw source columns read by mart 045. ``position`` comes from
# core.roster, while the YAC output deliberately reads both source twins.
PASSING_SOURCE_COLUMNS = frozenset(
    {
        "season",
        "player_id",
        "player",
        "team",
        "conference",
        "attempts",
        "completions",
        "interceptions",
        "completion_rate",
        "total_air_yards",
        "average_depth_of_target",
        "air_yards_attempts_available",
        "total_yards_after_catch",
        "average_yards_after_catch",
        "average_yards_after_catch__v_double",
        "yards_after_catch_attempts_available",
    }
)


def _walk(node: Any):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for value in node:
            yield from _walk(value)


def _parsed_statements(sql: str) -> list[dict[str, Any]]:
    return json.loads(parser.parse_sql_json(sql))["stmts"]


def _mart_query(sql: str) -> dict[str, Any]:
    matches = []
    for statement in _parsed_statements(sql):
        create = statement["stmt"].get("CreateTableAsStmt")
        if not create:
            continue
        relation = create["into"]["rel"]
        if (relation.get("schemaname"), relation["relname"]) == (
            "marts",
            "passing_charting_player_season",
        ):
            matches.append(create["query"]["SelectStmt"])
    assert len(matches) == 1, "expected exactly one mart 045 materialized-view query"
    return matches[0]


def _column_parts(column_ref: dict[str, Any]) -> tuple[str, ...]:
    parts = []
    for field in column_ref["fields"]:
        if "String" in field:
            parts.append(field["String"]["sval"])
        elif "A_Star" in field:
            parts.append("*")
        else:
            raise AssertionError(f"unhandled ColumnRef field: {field}")
    return tuple(parts)


def _source_aliases(query: dict[str, Any]) -> frozenset[str]:
    aliases = set()
    for node in _walk(query.get("fromClause", [])):
        relation = node.get("RangeVar")
        if not relation:
            continue
        if (relation.get("schemaname"), relation["relname"]) != (
            SOURCE_SCHEMA,
            SOURCE_TABLE,
        ):
            continue
        aliases.add(relation.get("alias", {}).get("aliasname", relation["relname"]))
    assert aliases, f"mapped mart no longer reads {SOURCE_RELATION}"
    return frozenset(aliases)


def _raw_source_columns(query: dict[str, Any]) -> frozenset[str]:
    aliases = _source_aliases(query)
    columns = set()
    for node in _walk(query):
        column_ref = node.get("ColumnRef")
        if not column_ref:
            continue
        parts = _column_parts(column_ref)
        if len(parts) >= 2 and parts[-2] in aliases and parts[-1] != "*":
            columns.add(parts[-1])
    return frozenset(columns)


def _raw_only_columns() -> frozenset[str]:
    twins = REVIEWED_RAW_ONLY_VARIANT_TWINS[SOURCE_RELATION]
    assert all(name.endswith("__v_double") for name in twins)
    bases = {name.removesuffix("__v_double") for name in twins}
    return twins | bases


def _assert_no_raw_only_reads(sql: str) -> None:
    query = _mart_query(sql)
    aliases = _source_aliases(query)
    forbidden = _raw_only_columns()
    reads = set()
    broad_reads = set()

    for node in _walk(query):
        column_ref = node.get("ColumnRef")
        if not column_ref:
            continue
        parts = _column_parts(column_ref)
        if parts == ("*",):
            broad_reads.add("bare *")
        elif len(parts) == 1 and parts[0] in aliases:
            broad_reads.add(f"whole row {parts[0]}")
        elif parts[-1] == "*" and len(parts) >= 2 and parts[-2] in aliases:
            broad_reads.add(".".join(parts))
        elif len(parts) == 1 and parts[0] in forbidden:
            reads.add(parts[0])
        elif len(parts) >= 2 and parts[-2] in aliases and parts[-1] in forbidden:
            reads.add(parts[-1])

    assert not broad_reads, (
        f"{MART_RELATION} has broad {SOURCE_RELATION} reads that bypass field review: "
        f"{sorted(broad_reads)}"
    )
    assert not reads, (
        f"{MART_RELATION} reads reviewed raw-only fields (base or twin): {sorted(reads)}"
    )


def _target_expression(query: dict[str, Any], target_name: str) -> dict[str, Any]:
    matches = []
    for item in query["targetList"]:
        target = item["ResTarget"]
        if target.get("name") == target_name:
            matches.append(target["val"])
    assert len(matches) == 1, f"expected one {target_name} output expression"
    return matches[0]


def _target_name(target: dict[str, Any]) -> str:
    if target.get("name"):
        return target["name"]
    value = target["val"]
    assert "ColumnRef" in value, "unnamed mart output must be a direct column"
    parts = _column_parts(value["ColumnRef"])
    assert parts[-1] != "*", "mart output contract cannot be inferred from a wildcard"
    return parts[-1]


def _assert_yac_fallback(sql: str) -> dict[str, Any]:
    query = _mart_query(sql)
    expression = _target_expression(query, "average_yards_after_catch")
    coalesce = expression.get("CoalesceExpr")
    assert coalesce, "YAC output must use executable COALESCE, not a comment token"
    args = coalesce["args"]
    assert len(args) == 2

    base = args[0].get("TypeCast", {}).get("arg", {}).get("ColumnRef")
    twin = args[1].get("ColumnRef")
    assert base and twin, "YAC must cast the base and fall back to the variant twin"
    type_name = args[0]["TypeCast"]["typeName"]["names"]
    assert tuple(part["String"]["sval"] for part in type_name) == (
        "pg_catalog",
        "float8",
    ), "YAC base must be cast to double precision"
    assert _column_parts(base)[-1] == "average_yards_after_catch"
    assert _column_parts(twin)[-1] == "average_yards_after_catch__v_double"
    aliases = _source_aliases(query)
    assert _column_parts(base)[-2] in aliases
    assert _column_parts(twin)[-2] in aliases
    return expression


def _evaluate_yac(expression: dict[str, Any], row: dict[str, float | int | None]):
    if "CoalesceExpr" in expression:
        for argument in expression["CoalesceExpr"]["args"]:
            value = _evaluate_yac(argument, row)
            if value is not None:
                return value
        return None
    if "TypeCast" in expression:
        return _evaluate_yac(expression["TypeCast"]["arg"], row)
    if "ColumnRef" in expression:
        return row[_column_parts(expression["ColumnRef"])[-1]]
    raise AssertionError(f"unsupported YAC expression node: {expression}")


def _maintained_reference_sources(
    overrides: dict[Path, str] | None = None,
) -> dict[Path, str]:
    overrides = overrides or {}
    sources = {}
    for root in MAINTAINED_ROOTS:
        for path in (ROOT / root).rglob("*"):
            if path.suffix not in {".py", ".sql"}:
                continue
            relative = path.relative_to(ROOT)
            source = overrides.get(relative, path.read_text())
            if SOURCE_REFERENCE.search(source):
                sources[relative] = source
    for path, source in overrides.items():
        if path not in sources and SOURCE_REFERENCE.search(source):
            sources[path] = source
    return sources


def _reference_lines(source: str) -> list[tuple[int, str]]:
    lines = source.splitlines()
    return [
        (source.count("\n", 0, match.start()) + 1, lines[source.count("\n", 0, match.start())])
        for match in SOURCE_REFERENCE.finditer(source)
    ]


def _assignment_value(module: ast.Module, name: str) -> ast.expr:
    matches = []
    for node in module.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            matches.append(node.value)
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == name
        ):
            matches.append(node.value)
    assert len(matches) == 1 and matches[0] is not None, f"expected one {name} assignment"
    return matches[0]


def _assert_refresh_registrations(source: str) -> None:
    module = ast.parse(source)
    external = _assignment_value(module, "EXTERNAL_RELATIONS")
    external_refs = [
        node
        for node in ast.walk(external)
        if isinstance(node, ast.Constant) and node.value == SOURCE_RELATION
    ]

    assets = _assignment_value(module, "REFRESH_ASSETS")
    asset_refs = []
    for node in ast.walk(assets):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        if node.func.id != "RefreshAsset" or len(node.args) < 2:
            continue
        if not isinstance(node.args[0], ast.Constant) or node.args[0].value != MART_RELATION:
            continue
        asset_refs.extend(
            child
            for child in ast.walk(node.args[1])
            if isinstance(child, ast.Constant) and child.value == SOURCE_RELATION
        )

    matching_strings = [
        node
        for node in ast.walk(module)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and SOURCE_REFERENCE.search(node.value)
    ]
    assert len(external_refs) == 1, "source must have one external-relation registration"
    assert len(asset_refs) == 1, "source must have one mart 045 dependency registration"
    assert {id(node) for node in matching_strings} == {
        id(external_refs[0]),
        id(asset_refs[0]),
    }, "all source references must remain exact refresh registrations"
    assert len(_reference_lines(source)) == 2


def _assert_variant_registrations(source: str) -> None:
    module = ast.parse(source)
    expected = _assignment_value(module, "EXPECTED_VARIANT_TWINS")
    reviewed = _assignment_value(module, "REVIEWED_RAW_ONLY_VARIANT_TWINS")

    def source_keys(mapping: ast.expr) -> list[ast.Constant]:
        assert isinstance(mapping, ast.Dict)
        return [
            key
            for key in mapping.keys
            if isinstance(key, ast.Constant) and key.value == SOURCE_RELATION
        ]

    expected_keys = source_keys(expected)
    reviewed_keys = source_keys(reviewed)
    assert len(expected_keys) == len(reviewed_keys) == 1

    doc_node = module.body[0]
    assert isinstance(doc_node, ast.Expr) and isinstance(doc_node.value, ast.Constant)
    assert SOURCE_REFERENCE.search(doc_node.value.value)
    references = _reference_lines(source)
    doc_refs = [line for line, _ in references if doc_node.lineno <= line <= doc_node.end_lineno]
    comment_refs = [line for line, text in references if text.lstrip().startswith("#")]
    key_lines = {expected_keys[0].lineno, reviewed_keys[0].lineno}
    assert len(doc_refs) == 1
    assert len(comment_refs) == 2
    assert {line for line, _ in references} - set(doc_refs) - set(comment_refs) == key_lines
    assert len(references) == 5


def _executable_sql_source_reads(source: str) -> int:
    count = 0
    for statement in _parsed_statements(source):
        for node in _walk(statement["stmt"]):
            relation = node.get("RangeVar")
            if not relation:
                continue
            if (
                relation.get("schemaname", "").casefold(),
                relation["relname"].casefold(),
            ) == (SOURCE_SCHEMA, SOURCE_TABLE):
                count += 1
    return count


def _assert_mapped_source_shape(source: str) -> None:
    references = _reference_lines(source)
    comments = [line for line, text in references if text.lstrip().startswith("--")]
    executable = [line for line, text in references if text.lstrip().upper().startswith("FROM ")]
    assert len(references) == 4, "mart 045 gained an additional raw-source reference"
    assert len(comments) == 3
    assert len(executable) == 1
    assert _executable_sql_source_reads(source) == 1
    _source_aliases(_mart_query(source))


def _assert_reviewed_non_metric_shape(path: Path, source: str) -> None:
    references = _reference_lines(source)
    if path == API_PATH:
        assert len(references) == 1 and references[0][1].lstrip().startswith("--")
        assert _executable_sql_source_reads(source) == 0
    elif path == Path("src/schemas/api/validation_expansion_views.sql"):
        assert len(references) == 1
        assert references[0][1].lstrip().startswith("RAISE EXCEPTION ")
        assert _executable_sql_source_reads(source) == 0
    elif path == Path("src/pipelines/config/refresh_assets.py"):
        _assert_refresh_registrations(source)
    elif path == Path("src/pipelines/utils/variant_twins.py"):
        _assert_variant_registrations(source)
    else:
        raise AssertionError(f"unhandled reviewed non-metric path: {path}")


def _assert_references_classified(sources: dict[Path, str]) -> None:
    classified = MAPPED_SOURCE_CONSUMERS | set(REVIEWED_NON_METRIC_REFERENCES)
    paths = set(sources)
    assert paths == classified, (
        f"literal {SOURCE_RELATION} references require consumer review; "
        f"unclassified={sorted(paths - classified)!r}, missing={sorted(classified - paths)!r}"
    )
    for path in MAPPED_SOURCE_CONSUMERS:
        _assert_mapped_source_shape(sources[path])
    for path in REVIEWED_NON_METRIC_REFERENCES:
        _assert_reviewed_non_metric_shape(path, sources[path])


def test_literal_consumer_inventory_is_exhaustive_within_maintained_roots():
    _assert_references_classified(_maintained_reference_sources())


@pytest.mark.parametrize(
    "relation",
    [
        "STATS.PASSING_PLAYER_SEASON",
        '"stats"."passing_player_season"',
        "stats . passing_player_season",
    ],
)
def test_new_unmapped_literal_consumer_requires_review(relation):
    mutated = _maintained_reference_sources(
        {
            Path("scripts/new_passing_consumer.py"): (
                f'sql = "SELECT pps.success_rate FROM {relation} pps"'
            )
        }
    )
    with pytest.raises(AssertionError, match="new_passing_consumer.py"):
        _assert_references_classified(mutated)


def test_allowlisted_sql_file_cannot_gain_a_raw_source_reader():
    path = Path("src/schemas/api/validation_expansion_views.sql")
    mutated = (ROOT / path).read_text() + (
        '\nSELECT pps.success_rate FROM "stats" . "passing_player_season" pps;\n'
    )
    with pytest.raises(AssertionError):
        _assert_references_classified(_maintained_reference_sources({path: mutated}))


def test_allowlisted_python_file_cannot_gain_a_raw_source_reader():
    path = Path("src/pipelines/config/refresh_assets.py")
    mutated = (ROOT / path).read_text() + (
        '\nSQL = "SELECT pps.success_rate FROM STATS . PASSING_PLAYER_SEASON pps"\n'
    )
    with pytest.raises(AssertionError):
        _assert_references_classified(_maintained_reference_sources({path: mutated}))


def test_mapped_file_cannot_gain_an_additional_raw_source_reader():
    mutated = (
        (ROOT / MART_PATH).read_text()
        + """
CREATE VIEW api.unreviewed_passing_metric AS
SELECT pps.success_rate
FROM stats.passing_player_season pps;
"""
    )
    with pytest.raises(AssertionError, match="additional raw-source reference"):
        _assert_references_classified(_maintained_reference_sources({MART_PATH: mutated}))


def test_mapped_query_cannot_gain_a_nested_raw_source_reader():
    sql = (ROOT / MART_PATH).read_text()
    mutated = sql.replace(
        "pps.completion_rate,",
        "pps.completion_rate + (SELECT x.success_rate "
        "FROM stats.passing_player_season x LIMIT 1) AS completion_rate,",
        1,
    )
    with pytest.raises(AssertionError, match="additional raw-source reference"):
        _assert_references_classified(_maintained_reference_sources({MART_PATH: mutated}))


@pytest.mark.parametrize(
    "opaque_sql",
    [
        """
        DO $$
        BEGIN
            PERFORM pps.success_rate FROM stats.passing_player_season pps;
        END $$;
        """,
        """
        CREATE FUNCTION api.unreviewed_passing_metric() RETURNS numeric
        LANGUAGE sql AS $$
            SELECT pps.success_rate FROM stats.passing_player_season pps LIMIT 1
        $$;
        """,
    ],
)
def test_mapped_file_cannot_hide_an_additional_reader_in_an_opaque_body(opaque_sql):
    mutated = (ROOT / MART_PATH).read_text() + opaque_sql
    with pytest.raises(AssertionError, match="additional raw-source reference"):
        _assert_references_classified(_maintained_reference_sources({MART_PATH: mutated}))


def test_mapped_mart_reads_only_the_reviewed_source_inventory():
    sql = (ROOT / MART_PATH).read_text()
    query = _mart_query(sql)
    assert _raw_source_columns(query) == PASSING_SOURCE_COLUMNS
    assert tuple(_target_name(item["ResTarget"]) for item in query["targetList"]) == (
        PASSING_API_COLUMNS
    )
    _assert_no_raw_only_reads(sql)


@pytest.mark.parametrize(
    "selection",
    [
        "pps.success_rate",
        "pps.success_rate__v_double",
        "pps.*",
        "*",
        "to_jsonb(pps)",
    ],
)
def test_raw_only_or_broad_source_selection_cannot_bypass_guard(selection):
    sql = (ROOT / MART_PATH).read_text()
    mutated = sql.replace("SELECT\n", f"SELECT\n    {selection},\n", 1)
    with pytest.raises(AssertionError):
        _assert_no_raw_only_reads(mutated)


def test_source_alias_is_discovered_from_executable_sql():
    sql = (ROOT / MART_PATH).read_text().replace("pps", "passing_stats")
    _assert_no_raw_only_reads(sql)
    assert _raw_source_columns(_mart_query(sql)) == PASSING_SOURCE_COLUMNS

    mutated = sql.replace("SELECT\n", "SELECT\n    passing_stats.success_rate,\n", 1)
    with pytest.raises(AssertionError, match="success_rate"):
        _assert_no_raw_only_reads(mutated)


def test_comments_do_not_count_as_raw_only_consumption():
    sql = (ROOT / MART_PATH).read_text() + "\n-- pps.success_rate__v_double\n"
    _assert_no_raw_only_reads(sql)


def test_yac_guard_requires_the_executable_fallback_expression():
    sql = (ROOT / MART_PATH).read_text()
    _assert_yac_fallback(sql)

    wrong_cast = sql.replace(
        "pps.average_yards_after_catch::double precision",
        "pps.average_yards_after_catch::numeric",
        1,
    )
    with pytest.raises(AssertionError, match="double precision"):
        _assert_yac_fallback(wrong_cast)

    mutated = sql.replace(
        "COALESCE(pps.average_yards_after_catch::double precision, "
        "pps.average_yards_after_catch__v_double)",
        "pps.average_yards_after_catch::double precision",
        1,
    )
    # The source file still contains twin names in comments, which must not
    # satisfy the executable-expression guard.
    assert "average_yards_after_catch__v_double" in mutated
    with pytest.raises(AssertionError, match="executable COALESCE"):
        _assert_yac_fallback(mutated)


@pytest.mark.parametrize(
    ("base", "twin", "expected"),
    [
        (4, None, 4),
        (None, 4.25, 4.25),
        (4, 9.5, 4),
        (0, 9.5, 0),
        (None, None, None),
    ],
)
def test_yac_fallback_preserves_base_twin_zero_and_null_semantics(base, twin, expected):
    expression = _assert_yac_fallback((ROOT / MART_PATH).read_text())
    row = {
        "average_yards_after_catch": base,
        "average_yards_after_catch__v_double": twin,
    }
    assert _evaluate_yac(expression, row) == expected


def test_api_definition_is_a_passthrough_of_the_guarded_mart_contract():
    view_queries = []
    for statement in _parsed_statements((ROOT / API_PATH).read_text()):
        view = statement["stmt"].get("ViewStmt")
        if not view:
            continue
        relation = view["view"]
        if (relation.get("schemaname"), relation["relname"]) == (
            "api",
            "passing_charting_player_season",
        ):
            view_queries.append(view["query"]["SelectStmt"])
    assert len(view_queries) == 1
    query = view_queries[0]
    relations = {
        f"{node['RangeVar'].get('schemaname')}.{node['RangeVar']['relname']}"
        for node in _walk(query.get("fromClause", []))
        if "RangeVar" in node
    }
    assert relations == {MART_RELATION}
    assert [
        _column_parts(item["ResTarget"]["val"]["ColumnRef"]) for item in query["targetList"]
    ] == [("*",)]
