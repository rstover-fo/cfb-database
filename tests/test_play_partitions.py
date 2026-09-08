"""Catalog and SQL tests for the bounded ``core.plays`` partition lifecycle."""

from __future__ import annotations

import json
import os
import uuid
from urllib.parse import urlparse

import psycopg2
import pytest
from psycopg2 import sql
from psycopg2.extensions import make_dsn

import scripts.maintain_play_partitions as cli
from src.pipelines.utils import partitions
from src.pipelines.utils.partitions import (
    PartitionStateError,
    PlayPartitionPlan,
    ensure_play_partitions,
    inspect_play_partitions,
)


class CatalogCursor:
    def __init__(
        self,
        *,
        parent=(42, "p", "l", 1, ["season"]),
        children=(),
        reserved=(),
    ):
        self.parent = parent
        self.children = list(children)
        self.reserved = list(reserved)
        self.rows = []
        self.executions = []

    def execute(self, statement, values=None):
        text = " ".join(str(statement).split())
        self.executions.append((text, values))
        if "pg_catalog.pg_partitioned_table" in text:
            self.rows = [] if self.parent is None else [self.parent]
        elif "pg_catalog.pg_inherits" in text:
            self.rows = self.children
        elif "relation.relname ~" in text:
            self.rows = self.reserved
        else:
            raise AssertionError(f"unexpected catalog query: {text}")

    def fetchall(self):
        return list(self.rows)


def child(year: int, *, name: str | None = None, bound: str | None = None, oid: int | None = None):
    relation_name = name or f"plays_y{year}"
    partition_bound = bound or f"FOR VALUES IN ('{year}')"
    return (oid or year, "core", relation_name, "r", True, partition_bound)


def reserved(year: int, *, oid: int | None = None, relkind: str = "r"):
    return (oid or year, f"plays_y{year}", relkind)


def test_inspection_reports_rolling_horizon_and_explicit_historical_years():
    rows = [child(2004), child(2026)]
    cur = CatalogCursor(children=rows, reserved=[reserved(2004), reserved(2026)])

    plan = inspect_play_partitions(cur, [2004], calendar_year=2026)

    assert plan.required_years == (2004, 2026, 2027)
    assert plan.existing_years == (2004, 2026)
    assert plan.missing_years == (2027,)


@pytest.mark.parametrize("year", [2003, 2028, True, 2026.0, "2026"])
def test_requested_years_are_bounded_and_strictly_typed(year):
    with pytest.raises(ValueError, match="year"):
        inspect_play_partitions(CatalogCursor(), [year], calendar_year=2026)


@pytest.mark.parametrize(
    ("parent", "message"),
    [
        (None, "exactly one"),
        ((42, "r", None, None, []), "LIST"),
        ((42, "p", "r", 1, ["season"]), "LIST"),
        ((42, "p", "l", 2, ["season", "game_id"]), "LIST"),
        ((42, "p", "l", 1, [None]), "LIST"),
    ],
)
def test_parent_must_be_list_partitioned_on_the_season_column(parent, message):
    with pytest.raises(PartitionStateError, match=message):
        inspect_play_partitions(CatalogCursor(parent=parent), calendar_year=2026)


@pytest.mark.parametrize(
    ("bad_child", "message"),
    [
        (child(2027, name="plays_future"), "unexpected attached child"),
        (child(2027, name="plays_y02027"), "canonical partition name"),
        (child(2027, bound="FOR VALUES IN ('2028')"), "expected 2027"),
        (child(2027, bound="FOR VALUES IN ('2027', '2028')"), "unsupported bound"),
        (child(2027, bound="DEFAULT"), "default partition"),
        ((2027, "other", "plays_y2027", "r", True, "FOR VALUES IN ('2027')"), "outside core"),
        ((2027, "core", "plays_y2027", "p", True, "FOR VALUES IN ('2027')"), "ordinary"),
        ((2027, "core", "plays_y2027", "r", False, "FOR VALUES IN ('2027')"), "ordinary"),
    ],
)
def test_every_attached_child_must_have_the_canonical_single_year_shape(bad_child, message):
    cur = CatalogCursor(children=[bad_child], reserved=[reserved(2027)])

    with pytest.raises(PartitionStateError, match=message):
        inspect_play_partitions(cur, calendar_year=2026)


def test_valid_precreated_future_partition_beyond_creation_horizon_is_accepted():
    rows = [child(2026), child(2027), child(2028)]
    cur = CatalogCursor(children=rows, reserved=[reserved(year) for year in (2026, 2027, 2028)])

    plan = inspect_play_partitions(cur, calendar_year=2026)

    assert plan.existing_years == (2026, 2027, 2028)
    assert not plan.missing_years


def test_unquoted_integral_catalog_bound_is_accepted():
    cur = CatalogCursor(
        children=[child(2026, bound="FOR VALUES IN (2026)")],
        reserved=[reserved(2026)],
    )

    assert inspect_play_partitions(cur, calendar_year=2026).existing_years == (2026,)


def test_same_name_relation_unattached_from_actual_parent_is_rejected():
    cur = CatalogCursor(children=[child(2026)], reserved=[reserved(2026), reserved(2027)])

    with pytest.raises(PartitionStateError, match="plays_y2027.*not attached"):
        inspect_play_partitions(cur, calendar_year=2026)


def test_reserved_catalog_query_matches_exact_partition_names_only():
    cur = CatalogCursor(children=[child(2026)], reserved=[reserved(2026)])

    inspect_play_partitions(cur, calendar_year=2026)

    reserved_query, values = cur.executions[-1]
    assert "relation.relname ~" in reserved_query
    assert values == ("core", r"^plays_y[0-9]+$")


class RecordingCursor:
    def __init__(self):
        self.executions = []
        self.closed = False

    def execute(self, statement, values=None):
        self.executions.append((str(statement), values))

    def close(self):
        self.closed = True


class RecordingConnection:
    autocommit = False

    def __init__(self):
        self.cursor_instance = RecordingCursor()
        self.commits = 0
        self.rollbacks = 0

    def get_transaction_status(self):
        return 0

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_create_validates_before_any_ddl_and_rolls_back_on_drift(monkeypatch):
    conn = RecordingConnection()

    def reject(*args, **kwargs):
        raise PartitionStateError("wrong bound")

    monkeypatch.setattr(partitions, "inspect_play_partitions", reject)

    with pytest.raises(PartitionStateError, match="wrong bound"):
        ensure_play_partitions(conn, [2027], create=True, calendar_year=2026)

    statements = [statement for statement, _ in conn.cursor_instance.executions]
    assert not any("CREATE TABLE" in statement for statement in statements)
    assert any("SHARE UPDATE EXCLUSIVE" in statement for statement in statements)
    assert ("SET LOCAL lock_timeout = %s", ("10s",)) in conn.cursor_instance.executions
    assert (
        "SELECT pg_advisory_xact_lock(%s, %s)",
        partitions.ADVISORY_LOCK_KEYS,
    ) in conn.cursor_instance.executions
    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert conn.cursor_instance.closed


def test_inspection_uses_read_only_transaction_and_rolls_it_back(monkeypatch):
    conn = RecordingConnection()
    expected = PlayPartitionPlan((2026, 2027), (2026,), (2027,))
    monkeypatch.setattr(partitions, "inspect_play_partitions", lambda *a, **k: expected)

    result = ensure_play_partitions(conn, create=False, calendar_year=2026)

    assert result == expected
    assert conn.cursor_instance.executions[0] == (
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
        None,
    )
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_autocommit_and_nonidle_connections_are_rejected_before_sql():
    conn = RecordingConnection()
    conn.autocommit = True
    with pytest.raises(RuntimeError, match="autocommit"):
        ensure_play_partitions(conn, calendar_year=2026)
    conn.autocommit = False
    conn.get_transaction_status = lambda: 2
    with pytest.raises(RuntimeError, match="idle"):
        ensure_play_partitions(conn, calendar_year=2026)
    assert conn.cursor_instance.executions == []


class ClosingConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_cli_defaults_to_inspection_and_resolves_dsn_through_load_ledger(monkeypatch, capsys):
    conn = ClosingConnection()
    captured = []
    monkeypatch.setattr(cli.load_ledger, "get_db_url", lambda: "postgresql://resolved")
    monkeypatch.setattr(cli.psycopg2, "connect", lambda dsn: captured.append(dsn) or conn)
    monkeypatch.setattr(
        cli,
        "ensure_play_partitions",
        lambda connection, years, *, create: PlayPartitionPlan((2026, 2027), (2004, 2026), (2027,)),
    )

    assert cli.main([]) == 0

    assert captured == ["postgresql://resolved"]
    assert conn.closed
    payload = json.loads(capsys.readouterr().out)
    assert payload["apply"] is False
    assert payload["missing_years"] == [2027]


def test_cli_apply_passes_explicit_years(monkeypatch, capsys):
    conn = ClosingConnection()
    calls = []
    monkeypatch.setattr(cli.load_ledger, "get_db_url", lambda: "postgresql://resolved")
    monkeypatch.setattr(cli.psycopg2, "connect", lambda dsn: conn)

    def ensure(connection, years, *, create):
        calls.append((connection, years, create))
        return PlayPartitionPlan((2004, 2026, 2027), (2004, 2026, 2027), (), (2027,))

    monkeypatch.setattr(cli, "ensure_play_partitions", ensure)

    assert cli.main(["--years", "2004", "--apply"]) == 0
    assert calls == [(conn, [2004], True)]
    assert json.loads(capsys.readouterr().out)["created_years"] == [2027]


@pytest.fixture
def partition_db():
    """Isolated database on the explicitly selected disposable F06 cluster."""

    dsn = os.environ.get("F06_TEST_DB_URL")
    if not dsn:
        if os.environ.get("F06_REQUIRE_DB") == "1":
            pytest.fail("F06_TEST_DB_URL is required for mandatory partition integration")
        pytest.skip("set F06_TEST_DB_URL to a disposable loopback PostgreSQL cluster")
    parsed = urlparse(dsn)
    if parsed.scheme not in {"postgres", "postgresql"} or parsed.hostname not in {
        "localhost",
        "127.0.0.1",
        "::1",
    }:
        pytest.fail("F06_TEST_DB_URL must select a disposable loopback PostgreSQL cluster")
    name = "f08_test_" + uuid.uuid4().hex
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name)))
    conn = psycopg2.connect(make_dsn(dsn, dbname=name))
    try:
        yield conn
    finally:
        conn.close()
        with admin.cursor() as cur:
            cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        admin.close()


def _create_play_parent(conn, *, indexed: bool = False):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA core")
        cur.execute("CREATE TABLE core.games (id bigint PRIMARY KEY, season bigint NOT NULL)")
        cur.execute(
            "CREATE TABLE core.plays (id bigint, game_id bigint, season bigint NOT NULL) "
            "PARTITION BY LIST (season)"
        )
        cur.execute("CREATE TABLE core.plays_y2004 PARTITION OF core.plays FOR VALUES IN (2004)")
        cur.execute("CREATE TABLE core.plays_y2026 PARTITION OF core.plays FOR VALUES IN (2026)")
        if indexed:
            cur.execute("CREATE INDEX idx_plays_game_id ON core.plays(game_id)")
    conn.commit()


def _relation_exists(conn, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"core.{name}",))
        return bool(cur.fetchone()[0])


def test_sql_rollover_late_history_idempotence_and_parent_indexes(partition_db):
    conn = partition_db
    _create_play_parent(conn, indexed=True)

    first = ensure_play_partitions(conn, [2004], create=True, calendar_year=2026)
    second = ensure_play_partitions(conn, [2004], create=True, calendar_year=2026)

    assert first.created_years == (2027,)
    assert not second.created_years
    with conn.cursor() as cur:
        cur.execute("INSERT INTO core.games VALUES (1, 2027)")
        cur.execute("INSERT INTO core.plays VALUES (1, 1, 2027), (2, 1, 2004)")
        cur.execute("SELECT tableoid::regclass::text, season FROM core.plays ORDER BY season DESC")
        assert cur.fetchall() == [("core.plays_y2027", 2027), ("core.plays_y2004", 2004)]
        cur.execute(
            """
            SELECT parent_index.relname, child_index.relname
            FROM pg_catalog.pg_inherits inheritance
            JOIN pg_catalog.pg_class child_index ON child_index.oid = inheritance.inhrelid
            JOIN pg_catalog.pg_class parent_index ON parent_index.oid = inheritance.inhparent
            WHERE parent_index.relname = 'idx_plays_game_id'
              AND child_index.relnamespace = 'core'::regnamespace
              AND child_index.relname LIKE 'plays_y2027%'
            """
        )
        assert len(cur.fetchall()) == 1
    conn.commit()


@pytest.mark.parametrize(
    "bad_ddl",
    [
        "CREATE TABLE core.plays_y2025 PARTITION OF core.plays FOR VALUES IN (2024)",
        "CREATE TABLE core.plays_y2027 (LIKE core.plays)",
        "CREATE TABLE core.plays_misc PARTITION OF core.plays DEFAULT",
    ],
)
def test_sql_catalog_drift_never_creates_any_missing_partition(partition_db, bad_ddl):
    conn = partition_db
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA core")
        cur.execute(
            "CREATE TABLE core.plays (id bigint, season bigint NOT NULL) PARTITION BY LIST (season)"
        )
        cur.execute(bad_ddl)
    conn.commit()

    with pytest.raises(PartitionStateError):
        ensure_play_partitions(conn, create=True, calendar_year=2026)

    assert not _relation_exists(conn, "plays_y2026")
    if "plays_y2027 (" not in bad_ddl:
        assert not _relation_exists(conn, "plays_y2027")


def test_sql_second_create_failure_rolls_back_first_partition(partition_db):
    conn = partition_db
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA core")
        cur.execute(
            "CREATE TABLE core.plays (id bigint, season bigint NOT NULL) PARTITION BY LIST (season)"
        )
    conn.commit()

    class FailSecondCreateCursor:
        def __init__(self, wrapped):
            self.wrapped = wrapped
            self.creates = 0

        def execute(self, statement, values=None):
            if "CREATE TABLE" in str(statement):
                self.creates += 1
                if self.creates == 2:
                    raise RuntimeError("injected second partition failure")
            return self.wrapped.execute(statement, values)

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

    class FailSecondCreateConnection:
        autocommit = False

        def __init__(self, wrapped):
            self.wrapped = wrapped
            self.cursor_instance = None

        def cursor(self):
            self.cursor_instance = FailSecondCreateCursor(self.wrapped.cursor())
            return self.cursor_instance

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

    failing = FailSecondCreateConnection(conn)
    with pytest.raises(RuntimeError, match="injected second partition failure"):
        ensure_play_partitions(failing, create=True, calendar_year=2026)

    assert failing.cursor_instance.creates == 2
    assert not _relation_exists(conn, "plays_y2026")
    assert not _relation_exists(conn, "plays_y2027")
