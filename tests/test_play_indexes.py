"""Unit tests for the catalog-safe ``core.plays`` index lifecycle."""

from __future__ import annotations

import json

import pytest

import scripts.maintain_play_indexes as cli
from src.pipelines.utils import play_indexes
from src.pipelines.utils.play_indexes import (
    EXPECTED_PLAY_INDEXES,
    PlayIndexStateError,
    inspect_play_indexes,
    repair_play_indexes,
    validate_play_indexes,
)


def relation(
    oid: int,
    name: str,
    relkind: str,
    *,
    is_partition: bool = False,
    strategy=None,
    key_count=None,
    keys=(),
):
    return (oid, name, relkind, is_partition, "postgres", strategy, key_count, list(keys))


def index_row(
    oid: int,
    name: str,
    table_oid: int,
    table_name: str,
    columns: tuple[str, ...],
    *,
    unique: bool = False,
    predicate: str | None = None,
    relkind: str = "I",
    valid: bool = True,
    ready: bool = True,
    live: bool = True,
    immediate: bool = True,
    parent_oids=(),
    method: str = "btree",
    attribute_count: int | None = None,
    collations=None,
    operator_classes=None,
    options=None,
    function_dependencies=None,
    operator_dependencies=None,
):
    key_count = len(columns)
    return (
        oid,
        "core",
        name,
        relkind,
        "postgres",
        table_oid,
        "core",
        table_name,
        "p" if table_name == "plays" else "r",
        "postgres",
        method,
        unique,
        valid,
        ready,
        live,
        False,
        False,
        immediate,
        False,
        key_count if attribute_count is None else attribute_count,
        key_count,
        None,
        predicate,
        f"CREATE INDEX {name}",
        list(columns),
        [True] * key_count if collations is None else collations,
        [True] * key_count if operator_classes is None else operator_classes,
        [0] * key_count if options is None else options,
        list(parent_oids),
        function_dependencies or [],
        operator_dependencies or [],
    )


class CatalogCursor:
    def __init__(self, *, relations=(), leaves=(), indexes=(), collisions=()):
        self.relations = list(relations)
        self.leaves = list(leaves)
        self.indexes = list(indexes)
        self.collisions = list(collisions)
        self.rows = []
        self.executions = []
        self.closed = False

    def execute(self, statement, values=None):
        text = " ".join(str(statement).split())
        self.executions.append((text, values))
        if "pg_catalog.pg_partitioned_table" in text:
            self.rows = self.relations
        elif "WITH RECURSIVE descendants" in text:
            self.rows = self.leaves
        elif "FROM pg_catalog.pg_index catalog_index" in text:
            self.rows = self.indexes
        elif "SELECT relation.relname, relation.relkind" in text:
            self.rows = self.collisions
        elif "SQL('CREATE ')" in text or text.startswith(
            (
                "BEGIN",
                "SET LOCAL",
                "SELECT pg_advisory_xact_lock",
                "LOCK TABLE",
                "CREATE",
                "ALTER INDEX",
            )
        ):
            self.rows = []
        else:
            raise AssertionError(f"unexpected SQL: {text}")

    def fetchall(self):
        return list(self.rows)

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def healthy_catalog(name="idx_plays_game_id"):
    spec = EXPECTED_PLAY_INDEXES[name]
    parent_oid = 10
    partition_oid = 20
    parent_index_oid = 30
    return CatalogCursor(
        relations=[
            relation(parent_oid, "plays", "p", strategy="l", key_count=1, keys=("season",)),
            relation(parent_index_oid, name, "I"),
        ],
        leaves=[(partition_oid, "core", "plays_y2026", "r", True, "FOR VALUES IN (2026)")],
        indexes=[
            index_row(
                parent_index_oid,
                name,
                parent_oid,
                "plays",
                spec.columns,
                unique=spec.unique,
                predicate=spec.predicate,
            ),
            index_row(
                31,
                "plays_y2026_index",
                partition_oid,
                "plays_y2026",
                spec.columns,
                unique=spec.unique,
                predicate=spec.predicate,
                relkind="i",
                parent_oids=(parent_index_oid,),
            ),
        ],
    )


def test_expected_index_allowlist_is_exact_and_ordered():
    assert list(EXPECTED_PLAY_INDEXES) == [
        "plays_dlt_id_unique",
        "idx_plays_game_id",
        "idx_plays_offense",
        "idx_plays_defense",
        "idx_plays_offense_season",
        "idx_plays_defense_season",
        "idx_plays_play_type",
        "idx_plays_score_diff",
        "idx_plays_competitive",
    ]
    assert EXPECTED_PLAY_INDEXES["plays_dlt_id_unique"].unique
    assert EXPECTED_PLAY_INDEXES["plays_dlt_id_unique"].columns == ("_dlt_id", "season")
    assert EXPECTED_PLAY_INDEXES["idx_plays_competitive"].predicate == (
        "abs((offense_score - defense_score)) <= 28"
    )


def test_inspection_reports_complete_actual_definition_and_child_coverage():
    report = inspect_play_indexes(healthy_catalog(), ["idx_plays_game_id"])

    assert report["valid"] is True
    assert report["partition_count"] == 1
    actual = report["indexes"]["idx_plays_game_id"]["actual"]
    assert actual["target"]["name"] == "plays"
    assert actual["keys"] == ["game_id"]
    assert actual["valid"] and actual["ready"] and actual["live"]
    assert report["indexes"]["idx_plays_game_id"]["child_coverage"] == {
        "expected_partition_count": 1,
        "attached_partition_count": 1,
        "missing_partitions": [],
        "invalid_partitions": [],
        "wrong_structure_partitions": [],
        "unexpected_targets": [],
    }
    json.dumps(report)


@pytest.mark.parametrize(
    ("change", "issue"),
    [
        ({"valid": False}, "invalid"),
        ({"immediate": False}, "wrong_structure"),
        ({"method": "hash"}, "wrong_structure"),
        ({"attribute_count": 2}, "wrong_structure"),
        ({"collations": [False]}, "wrong_structure"),
        ({"operator_classes": [False]}, "wrong_structure"),
        ({"options": [1]}, "wrong_structure"),
    ],
)
def test_parent_structure_and_readiness_are_strict(change, issue):
    cur = healthy_catalog()
    kwargs = dict(change)
    spec = EXPECTED_PLAY_INDEXES["idx_plays_game_id"]
    cur.indexes[0] = index_row(
        30,
        "idx_plays_game_id",
        10,
        "plays",
        spec.columns,
        **kwargs,
    )

    report = inspect_play_indexes(cur, ["idx_plays_game_id"])

    assert issue in report["issues"]["idx_plays_game_id"]


def test_missing_or_invalid_child_is_reported_as_missing_child_coverage():
    cur = healthy_catalog()
    cur.indexes[1] = index_row(
        31,
        "plays_y2026_index",
        20,
        "plays_y2026",
        ("game_id",),
        relkind="i",
        valid=False,
        parent_oids=(30,),
    )

    report = inspect_play_indexes(cur, ["idx_plays_game_id"])

    assert report["issues"]["idx_plays_game_id"] == ["missing_child_coverage"]
    assert report["indexes"]["idx_plays_game_id"]["child_coverage"]["invalid_partitions"] == [
        "plays_y2026"
    ]
    invalid = healthy_catalog()
    invalid.indexes.pop()
    with pytest.raises(PlayIndexStateError) as raised:
        validate_play_indexes(invalid, ["idx_plays_game_id"])
    assert raised.value.report is not None


def test_builtin_predicate_qualification_is_accepted_but_shadow_dependency_is_not():
    name = "idx_plays_competitive"
    spec = EXPECTED_PLAY_INDEXES[name]
    cur = healthy_catalog(name)
    cur.indexes[0] = index_row(
        30,
        name,
        10,
        "plays",
        spec.columns,
        predicate=(
            "pg_catalog.abs((offense_score OPERATOR(pg_catalog.-) defense_score)) "
            "OPERATOR(pg_catalog.<=) 28"
        ),
    )

    assert inspect_play_indexes(cur, [name])["valid"] is True

    cur.indexes[0] = index_row(
        30,
        name,
        10,
        "plays",
        spec.columns,
        predicate=spec.predicate,
        function_dependencies=["public.abs"],
    )
    report = inspect_play_indexes(cur, [name])
    assert "wrong_structure" in report["issues"][name]


def test_canonical_index_on_unrelated_table_is_wrong_owner_not_missing():
    cur = CatalogCursor(
        relations=[
            relation(10, "plays", "p", strategy="l", key_count=1, keys=("season",)),
            relation(40, "idx_plays_game_id", "i"),
        ],
        indexes=[index_row(40, "idx_plays_game_id", 99, "unrelated", ("game_id",), relkind="i")],
    )

    report = inspect_play_indexes(cur, ["idx_plays_game_id"])

    assert "wrong_owner" in report["issues"]["idx_plays_game_id"]
    assert "missing" not in report["issues"]["idx_plays_game_id"]


def test_old_canonical_plus_equivalent_differently_named_live_parent_is_not_repairable():
    cur = CatalogCursor(
        relations=[
            relation(10, "plays", "p", strategy="l", key_count=1, keys=("season",)),
            relation(11, "plays_old", "r"),
            relation(40, "idx_plays_game_id", "i"),
        ],
        indexes=[
            index_row(40, "idx_plays_game_id", 11, "plays_old", ("game_id",), relkind="i"),
            index_row(41, "live_game_id_other_name", 10, "plays", ("game_id",)),
        ],
    )

    report = inspect_play_indexes(cur, ["idx_plays_game_id"])

    assert report["issues"]["idx_plays_game_id"] == ["wrong_owner", "wrong_structure"]
    assert "differently named" in " ".join(report["indexes"]["idx_plays_game_id"]["diagnostics"])


def test_unknown_selection_is_rejected_before_catalog_queries():
    cur = CatalogCursor()
    with pytest.raises(ValueError, match="unsupported"):
        inspect_play_indexes(cur, ["idx_plays_obsolete"])
    assert cur.executions == []


class Connection:
    def __init__(self, cursor, *, autocommit=False, status=0):
        self._cursor = cursor
        self.autocommit = autocommit
        self.status = status
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def get_transaction_status(self):
        return self.status

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def state_report(name, *, issues, actual=None, old=None, catalog_issues=()):
    return {
        "valid": not issues and not catalog_issues,
        "catalog_issues": list(catalog_issues),
        "issues": {name: list(issues)} if issues else {},
        "indexes": {
            name: {
                "healthy": not issues and not catalog_issues,
                "issues": list(issues),
                "actual": actual,
                "diagnostics": [],
            }
        },
        "plays_old": old,
    }


@pytest.mark.parametrize("selection", [None, []])
def test_repair_requires_nonempty_explicit_selection(selection):
    conn = Connection(CatalogCursor())
    with pytest.raises(ValueError, match="explicit|at least one"):
        repair_play_indexes(conn, selection)
    assert conn._cursor.executions == []


@pytest.mark.parametrize(("autocommit", "status"), [(True, 0), (False, 2)])
def test_repair_requires_dedicated_idle_transactional_connection(autocommit, status):
    conn = Connection(CatalogCursor(), autocommit=autocommit, status=status)
    with pytest.raises(RuntimeError, match="autocommit=False|idle connection"):
        repair_play_indexes(conn, ["idx_plays_game_id"])
    assert conn._cursor.executions == []


def test_missing_selected_index_is_built_and_postvalidated(monkeypatch):
    name = "idx_plays_game_id"
    before = state_report(name, issues=["missing"])
    after = state_report(name, issues=[])
    reports = iter((before, after))
    monkeypatch.setattr(play_indexes, "inspect_play_indexes", lambda *_args: next(reports))
    cur = CatalogCursor()
    conn = Connection(cur)

    result = repair_play_indexes(conn, [name])

    statements = [statement for statement, _ in cur.executions]
    assert statements[:5] == [
        "BEGIN ISOLATION LEVEL READ COMMITTED",
        "SET LOCAL lock_timeout = %s",
        "SET LOCAL statement_timeout = %s",
        "SELECT pg_advisory_xact_lock(%s, %s)",
        "LOCK TABLE core.plays IN SHARE ROW EXCLUSIVE MODE",
    ]
    assert any("CREATE" in statement and "INDEX" in statement for statement in statements)
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert result["repaired_indexes"] == [name]
    assert result["lock_timeout"] == "10s"
    assert result["statement_timeout"] == "30min"


def test_all_selected_states_are_validated_before_first_ddl(monkeypatch):
    selected = ["idx_plays_game_id", "idx_plays_offense"]
    report = {
        "valid": False,
        "catalog_issues": [],
        "issues": {
            "idx_plays_game_id": ["missing"],
            "idx_plays_offense": ["wrong_structure"],
        },
        "indexes": {
            "idx_plays_game_id": {"healthy": False, "issues": ["missing"], "actual": None},
            "idx_plays_offense": {
                "healthy": False,
                "issues": ["wrong_structure"],
                "actual": None,
                "diagnostics": [],
            },
        },
        "plays_old": None,
    }
    monkeypatch.setattr(play_indexes, "inspect_play_indexes", lambda *_args: report)
    cur = CatalogCursor()
    conn = Connection(cur)

    with pytest.raises(PlayIndexStateError, match="not safely repairable"):
        repair_play_indexes(conn, selected)

    assert not any(
        "CREATE" in statement or "ALTER INDEX" in statement for statement, _ in cur.executions
    )
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_retained_old_rename_collision_fails_before_ddl(monkeypatch):
    name = "idx_plays_game_id"
    actual = {"target": {"schema": "core", "name": "plays_old"}}
    old = {"relkind": "r", "is_partition": False}
    report = state_report(name, issues=["wrong_owner"], actual=actual, old=old)
    monkeypatch.setattr(play_indexes, "inspect_play_indexes", lambda *_args: report)
    cur = CatalogCursor(collisions=[(f"plays_old_{name}", "i")])
    conn = Connection(cur)

    with pytest.raises(PlayIndexStateError, match="rename target already exists"):
        repair_play_indexes(conn, [name])

    statements = [statement for statement, _ in cur.executions]
    assert "LOCK TABLE core.plays_old IN SHARE ROW EXCLUSIVE MODE" in statements
    assert not any(
        "ALTER INDEX" in statement or "CREATE INDEX" in statement for statement in statements
    )
    assert conn.rollbacks == 1


def test_inspect_cli_uses_read_only_snapshot_and_returns_nonzero_for_issues(monkeypatch, capsys):
    name = "idx_plays_game_id"
    report = state_report(name, issues=["missing"])
    cur = CatalogCursor()
    conn = Connection(cur)
    monkeypatch.setenv("WAREHOUSE_DB_URL", "postgresql://u:p@db.example:5432/warehouse")
    monkeypatch.setattr(cli, "validate_database_url", lambda _url: None)
    monkeypatch.setattr(cli.psycopg2, "connect", lambda _url: conn)
    monkeypatch.setattr(cli, "inspect_play_indexes", lambda *_args: report)

    assert cli.main([]) == 1

    payload = json.loads(capsys.readouterr().out)
    assert payload["apply"] is False
    assert payload["valid"] is False
    assert cur.executions[0][0] == "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
    assert conn.rollbacks == 1
    assert conn.closed


def test_apply_cli_requires_an_explicit_index_without_connecting(monkeypatch, capsys):
    monkeypatch.setenv("WAREHOUSE_DB_URL", "postgresql://u:p@db.example:5432/warehouse")
    monkeypatch.setattr(
        cli.psycopg2,
        "connect",
        lambda _url: pytest.fail("must not connect without an explicit repair selection"),
    )

    assert cli.main(["--apply"]) == 1

    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["type"] == "ValueError"
    assert "--apply requires" in payload["error"]["message"]
