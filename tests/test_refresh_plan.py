"""Registry drift, planning, and refresh failure propagation regressions."""

import hashlib
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pglast import parser

from scripts import refresh_marts as runner
from src.pipelines.config.refresh_assets import (
    EXTERNAL_RELATIONS,
    FUNCTION_DEFINITIONS,
    FUNCTION_RELATIONS,
    REFRESH_ASSETS,
    RefreshAsset,
)
from src.pipelines.utils.refresh_plan import REFRESH_GRAPH, RefreshGraph

ROOT = Path(__file__).resolve().parents[1]


def relations(node):
    if isinstance(node, dict):
        if "RangeVar" in node:
            relation = node["RangeVar"]
            if relation.get("schemaname"):
                yield relation["schemaname"] + "." + relation["relname"]
        for value in node.values():
            yield from relations(value)
    elif isinstance(node, list):
        for value in node:
            yield from relations(value)


# Explicit catalog-function allowlist: a newly introduced function requires
# review rather than silently hiding its relation dependencies.
BUILTIN_FUNCTIONS = set(
    """abs array_agg avg bool_or btrim corr count
jsonb_build_array lag length lower max min mode now percent_rank percentile_cont
pg_catalog.extract pg_catalog.substring pg_partition_root pg_postmaster_start_time
power rank replace round row_number split_part sqrt stddev stddev_samp sum unnest""".split()
)


def functions(node):
    if isinstance(node, dict):
        if "FuncCall" in node:
            yield ".".join(part["String"]["sval"] for part in node["FuncCall"]["funcname"])
        for value in node.values():
            yield from functions(value)
    elif isinstance(node, list):
        for value in node:
            yield from functions(value)


def test_reviewed_function_definitions_have_not_changed():
    assert FUNCTION_DEFINITIONS.keys() == FUNCTION_RELATIONS.keys()
    for path, digest in FUNCTION_DEFINITIONS.values():
        assert hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == digest


def test_registry_matches_all_canonical_postgres_materialized_views():
    found = {}
    paths = [ROOT / "src/schemas/013_analytics_views.sql"]
    paths.extend(sorted((ROOT / "src/schemas/marts").glob("*.sql")))
    for path in paths:
        for statement in json.loads(parser.parse_sql_json(path.read_text()))["stmts"]:
            create = statement["stmt"].get("CreateTableAsStmt")
            if not create or create["objtype"] != "OBJECT_MATVIEW":
                continue
            relation = create["into"]["rel"]
            name = relation["schemaname"] + "." + relation["relname"]
            assert name not in found
            dependencies = set(relations(create["query"]))
            for function in functions(create["query"]):
                assert function in BUILTIN_FUNCTIONS or function in FUNCTION_RELATIONS, function
                dependencies.update(FUNCTION_RELATIONS.get(function, ()))
            found[name] = (str(path.relative_to(ROOT)), dependencies)
    assert set(found) == {asset.name for asset in REFRESH_ASSETS}
    for asset in REFRESH_ASSETS:
        assert (asset.definition, set(asset.depends_on)) == found[asset.name]
        assert asset.coverage_grain == "whole_relation"
    assert EXTERNAL_RELATIONS == set.union(*(deps for _, deps in found.values())) - set(found)


def graph():
    # Intentionally reverse registration order to exercise actual topology.
    return RefreshGraph(
        [
            RefreshAsset("marts.leaf", ("marts.middle",), "test"),
            RefreshAsset("marts.middle", ("marts.parent",), "test"),
            RefreshAsset("marts.parent", ("core.input",), "test"),
            RefreshAsset("marts.independent", (), "test"),
        ],
        ["core.input", "core.unused"],
    )


def test_subset_preserves_transitive_order_and_deduplicates():
    assert graph().plan(views=["marts.leaf", "marts.parent", "marts.leaf"]).views == (
        "marts.parent",
        "marts.leaf",
    )


def test_changed_excludes_committed_root_and_selects_descendants():
    assert graph().plan(changed=["marts.parent"]).views == ("marts.middle", "marts.leaf")
    assert graph().plan(changed=["core.unused"]).views == ()
    assert graph().plan(changed=["core.input", "marts.middle"]).views == (
        "marts.parent",
        "marts.leaf",
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"views": []},
        {"changed": []},
        {"views": ["marts.missing"]},
        {"changed": ["core.missing"]},
        {"schema": "core"},
        {"views": ["marts.parent"], "changed": ["core.input"]},
    ],
)
def test_invalid_selection(kwargs):
    with pytest.raises(ValueError):
        graph().plan(**kwargs)


@pytest.mark.parametrize(
    "assets,external",
    [
        ([RefreshAsset("marts.a", (), "test")] * 2, []),
        ([RefreshAsset("marts.a", ("marts.b",), "test")], []),
        ([RefreshAsset("marts.a", ("marts.a",), "test")], []),
        ([RefreshAsset("marts.a", (), "test")], ["marts.a"]),
        ([RefreshAsset("a; DROP TABLE x", (), "test")], []),
    ],
)
def test_invalid_graph(assets, external):
    with pytest.raises(ValueError):
        RefreshGraph(assets, external)


def test_actual_committed_input_plans():
    assert REFRESH_GRAPH.plan(changed=["ratings.sdv_ratings_weekly"]).views == (
        "marts.epa_crossvalidation",
    )
    assert REFRESH_GRAPH.plan(changed=["analytics.adjusted_epa_build"]).views == (
        "marts.team_adjusted_epa",
        "marts.epa_crossvalidation",
    )
    assert REFRESH_GRAPH.plan(changed=["ref.coach_tenures"]).views == ("marts.coach_tenures",)
    assert set(REFRESH_GRAPH.plan(changed=["ref.eras"]).views) == {
        "marts.team_season_trajectory",
        "marts.conference_era_summary",
    }
    ordered = REFRESH_GRAPH.plan().views
    assert len(ordered) == 55
    for name in ordered:
        assert all(
            ordered.index(parent) < ordered.index(name)
            for parent in REFRESH_GRAPH.ancestors(name)
            if parent in ordered
        )


def test_failed_refresh_blocks_transitive_descendants_but_continues_independent(monkeypatch):
    import psycopg2

    connection = MagicMock()
    monkeypatch.setattr(runner, "REFRESH_GRAPH", graph())
    monkeypatch.setattr(runner, "get_db_url", lambda: "test")
    monkeypatch.setattr(psycopg2, "connect", lambda _: connection)
    calls = []

    def refresh(name, *args):
        calls.append(name)
        return name != "marts.parent"

    monkeypatch.setattr(runner, "refresh_view", refresh)
    assert runner.refresh_marts() == 3
    assert calls == ["marts.parent", "marts.independent"]
    connection.close.assert_called_once()


def test_refresh_failure_rolls_back_and_closes_cursor():
    connection = MagicMock()
    connection.cursor.return_value.execute.side_effect = RuntimeError("failed refresh")
    assert not runner.refresh_view("marts.parent", connection, True, False)
    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()
    connection.cursor.return_value.close.assert_called_once()


def test_dry_run_invalid_and_noop_never_resolve_database(monkeypatch, capsys):
    def forbidden():
        pytest.fail("must not resolve database")

    monkeypatch.setattr(runner, "get_db_url", forbidden)
    assert runner.refresh_marts(changed=["ratings.sdv_ratings_weekly"], dry_run=True) == 0
    assert (
        "REFRESH MATERIALIZED VIEW CONCURRENTLY marts.epa_crossvalidation"
        in capsys.readouterr().out
    )
    assert runner.refresh_marts(views=["marts.missing"]) == 1
    monkeypatch.setattr(runner, "REFRESH_GRAPH", graph())
    assert runner.refresh_marts(changed=["core.unused"]) == 0


@pytest.mark.parametrize("option", ["--views", "--changed"])
def test_cli_empty_selection_does_not_refresh_all(monkeypatch, option):
    monkeypatch.setattr("sys.argv", ["refresh_marts.py", option, "", "--dry-run"])
    with pytest.raises(SystemExit) as exit_info:
        runner.main()
    assert exit_info.value.code == 1
