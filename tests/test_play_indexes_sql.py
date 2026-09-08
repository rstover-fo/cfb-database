"""Executed F09 regression tests for ``core.plays`` index ownership repair."""

from __future__ import annotations

from collections.abc import Iterable

import psycopg2
import pytest

from src.pipelines.utils.partitions import ensure_play_partitions
from src.pipelines.utils.play_indexes import (
    EXPECTED_PLAY_INDEXES,
    PlayIndexStateError,
    repair_play_indexes,
    validate_play_indexes,
)
from tests.test_warehouse_migrations_sql import warehouse_db as _warehouse_db_fixture  # noqa: F401

EXPECTED_DEFINITIONS = {
    "plays_dlt_id_unique": (True, ["_dlt_id", "season"], None),
    "idx_plays_game_id": (False, ["game_id"], None),
    "idx_plays_offense": (False, ["offense"], None),
    "idx_plays_defense": (False, ["defense"], None),
    "idx_plays_offense_season": (False, ["offense", "season"], None),
    "idx_plays_defense_season": (False, ["defense", "season"], None),
    "idx_plays_play_type": (False, ["play_type"], None),
    "idx_plays_score_diff": (False, ["score_diff"], None),
    "idx_plays_competitive": (
        False,
        ["game_id", "period"],
        "(abs((offense_score - defense_score)) <= 28)",
    ),
}

PLAY_COLUMNS = """
    _dlt_id text NOT NULL,
    season bigint NOT NULL,
    game_id bigint,
    offense text,
    defense text,
    play_type text,
    score_diff bigint,
    period bigint,
    offense_score bigint,
    defense_score bigint
"""

CREATE_EXPECTED_INDEXES = {
    "plays_dlt_id_unique": (
        "CREATE UNIQUE INDEX plays_dlt_id_unique ON core.plays(_dlt_id, season)"
    ),
    "idx_plays_game_id": "CREATE INDEX idx_plays_game_id ON core.plays(game_id)",
    "idx_plays_offense": "CREATE INDEX idx_plays_offense ON core.plays(offense)",
    "idx_plays_defense": "CREATE INDEX idx_plays_defense ON core.plays(defense)",
    "idx_plays_offense_season": (
        "CREATE INDEX idx_plays_offense_season ON core.plays(offense, season)"
    ),
    "idx_plays_defense_season": (
        "CREATE INDEX idx_plays_defense_season ON core.plays(defense, season)"
    ),
    "idx_plays_play_type": "CREATE INDEX idx_plays_play_type ON core.plays(play_type)",
    "idx_plays_score_diff": "CREATE INDEX idx_plays_score_diff ON core.plays(score_diff)",
    "idx_plays_competitive": (
        "CREATE INDEX idx_plays_competitive ON core.plays(game_id, period) "
        "WHERE abs(offense_score - defense_score) <= 28"
    ),
}

HISTORICALLY_COLLIDING_INDEXES = (
    "idx_plays_game_id",
    "idx_plays_offense",
    "idx_plays_defense",
    "idx_plays_play_type",
)


@pytest.fixture(name="warehouse_db")
def _play_index_warehouse_db(request):
    return request.getfixturevalue("_warehouse_db_fixture")


def _execute(conn, statement: str, values=None):
    with conn.cursor() as cur:
        cur.execute(statement, values)
        rows = cur.fetchall() if cur.description else None
    conn.commit()
    return rows


def _create_roles_and_schema(conn):
    _execute(
        conn,
        """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'anon') THEN
                CREATE ROLE anon NOLOGIN;
            END IF;
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'authenticated') THEN
                CREATE ROLE authenticated NOLOGIN;
            END IF;
        END $$;
        CREATE SCHEMA core;
        GRANT USAGE ON SCHEMA core TO anon, authenticated;
        """,
    )


def _create_partitioned_plays(conn, years: Iterable[int] = (2025, 2026)):
    _create_roles_and_schema(conn)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE core.plays ({PLAY_COLUMNS}) PARTITION BY LIST (season)")
        for year in years:
            cur.execute(
                f"CREATE TABLE core.plays_y{year} PARTITION OF core.plays FOR VALUES IN ({year})"
            )
    conn.commit()


def _create_historical_swap(conn):
    """Reproduce the old heap-to-partitioned-table index-name collision."""

    _create_roles_and_schema(conn)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE core.plays ({PLAY_COLUMNS})")
        cur.execute(
            """
            INSERT INTO core.plays VALUES
                ('old-a', 2025, 1, 'Alpha', 'Beta', 'Rush', 7, 1, 14, 7),
                ('old-b', 2026, 2, 'Gamma', 'Delta', 'Pass', -3, 2, 10, 13)
            """
        )
        for name in HISTORICALLY_COLLIDING_INDEXES:
            cur.execute(CREATE_EXPECTED_INDEXES[name])
        cur.execute(
            f"CREATE TABLE core.plays_partitioned ({PLAY_COLUMNS}) PARTITION BY LIST (season)"
        )
        for year in (2025, 2026):
            cur.execute(
                f"CREATE TABLE core.plays_y{year} PARTITION OF core.plays_partitioned "
                f"FOR VALUES IN ({year})"
            )
        cur.execute("INSERT INTO core.plays_partitioned SELECT * FROM core.plays")
        cur.execute("ALTER TABLE core.plays RENAME TO plays_old")
        cur.execute("ALTER TABLE core.plays_partitioned RENAME TO plays")
        for statement in CREATE_EXPECTED_INDEXES.values():
            # This is the historical bug: the schema-level old index name makes every
            # supposedly idempotent parent-index creation silently skip.
            cur.execute(
                statement.replace("CREATE ", "CREATE ", 1).replace(
                    " INDEX ", " INDEX IF NOT EXISTS ", 1
                )
            )
        cur.execute("GRANT SELECT ON core.plays TO anon, authenticated")
    conn.commit()


def _index_targets(conn, names=None):
    names = list(names or EXPECTED_DEFINITIONS)
    return dict(
        _execute(
            conn,
            """
            SELECT index_class.relname, table_class.relname
            FROM pg_catalog.pg_index index_state
            JOIN pg_catalog.pg_class index_class
              ON index_class.oid = index_state.indexrelid
            JOIN pg_catalog.pg_class table_class
              ON table_class.oid = index_state.indrelid
            WHERE index_class.relnamespace = 'core'::regnamespace
              AND index_class.relname = ANY(%s)
            ORDER BY index_class.relname
            """,
            (names,),
        )
    )


def _parent_index_definitions(conn):
    rows = _execute(
        conn,
        """
        SELECT index_class.relname,
               index_state.indisunique,
               ARRAY(
                   SELECT attribute.attname
                   FROM unnest(index_state.indkey::smallint[]) WITH ORDINALITY
                        AS key_column(attnum, position)
                   JOIN pg_catalog.pg_attribute attribute
                     ON attribute.attrelid = index_state.indrelid
                    AND attribute.attnum = key_column.attnum
                   WHERE key_column.position <= index_state.indnkeyatts
                   ORDER BY key_column.position
               ),
               pg_get_expr(index_state.indpred, index_state.indrelid),
               index_state.indisvalid,
               index_state.indisready
        FROM pg_catalog.pg_index index_state
        JOIN pg_catalog.pg_class index_class
          ON index_class.oid = index_state.indexrelid
        JOIN pg_catalog.pg_class table_class
          ON table_class.oid = index_state.indrelid
        WHERE index_class.relnamespace = 'core'::regnamespace
          AND table_class.relname = 'plays'
          AND index_class.relname = ANY(%s)
        ORDER BY index_class.relname
        """,
        (list(EXPECTED_DEFINITIONS),),
    )
    return {name: values for name, *values in rows}


def _attached_child_indexes(conn, *, child_name: str | None = None):
    child_filter = "AND child_table.relname = %s" if child_name else ""
    values = (
        (list(EXPECTED_DEFINITIONS), child_name) if child_name else (list(EXPECTED_DEFINITIONS),)
    )
    return _execute(
        conn,
        f"""
        SELECT parent_index.relname, child_table.relname,
               child_state.indisvalid, child_state.indisready
        FROM pg_catalog.pg_inherits inheritance
        JOIN pg_catalog.pg_class parent_index
          ON parent_index.oid = inheritance.inhparent
        JOIN pg_catalog.pg_index child_state
          ON child_state.indexrelid = inheritance.inhrelid
        JOIN pg_catalog.pg_class child_table
          ON child_table.oid = child_state.indrelid
        WHERE parent_index.relnamespace = 'core'::regnamespace
          AND parent_index.relname = ANY(%s)
          {child_filter}
        ORDER BY parent_index.relname, child_table.relname
        """,
        values,
    )


def _index_snapshot(conn):
    return _execute(
        conn,
        """
        SELECT index_class.oid, index_class.relname, table_class.relname,
               pg_get_indexdef(index_class.oid),
               index_state.indisvalid, index_state.indisready
        FROM pg_catalog.pg_index index_state
        JOIN pg_catalog.pg_class index_class
          ON index_class.oid = index_state.indexrelid
        JOIN pg_catalog.pg_class table_class
          ON table_class.oid = index_state.indrelid
        WHERE index_class.relnamespace = 'core'::regnamespace
        ORDER BY index_class.oid
        """,
    )


def _assert_caller_can_read(conn, role: str):
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL ROLE {role}")
            cur.execute("SELECT _dlt_id, season FROM core.plays ORDER BY season")
            assert cur.fetchall() == [("old-a", 2025), ("old-b", 2026)]
    finally:
        conn.rollback()


def test_historical_swap_repair_builds_exact_covered_indexes_and_is_idempotent(warehouse_db):
    conn, _ = warehouse_db
    _create_historical_swap(conn)
    names = list(EXPECTED_PLAY_INDEXES)

    expected_initial_targets = {
        name: "plays_old" if name in HISTORICALLY_COLLIDING_INDEXES else "plays" for name in names
    }
    assert _index_targets(conn, names) == expected_initial_targets
    with conn.cursor() as cur:
        with pytest.raises(PlayIndexStateError):
            validate_play_indexes(cur)
    conn.rollback()

    report = repair_play_indexes(conn, names)

    assert set(report["indexes"]) == set(names)
    assert _index_targets(conn, names) == {name: "plays" for name in names}
    definitions = _parent_index_definitions(conn)
    assert set(definitions) == set(EXPECTED_DEFINITIONS)
    for name, (unique, columns, predicate) in EXPECTED_DEFINITIONS.items():
        assert definitions[name] == [unique, columns, predicate, True, True]
    coverage = _attached_child_indexes(conn)
    assert len(coverage) == len(names) * 2
    assert {(child, valid, ready) for _, child, valid, ready in coverage} == {
        ("plays_y2025", True, True),
        ("plays_y2026", True, True),
    }
    assert _execute(conn, "SELECT * FROM core.plays_old ORDER BY season") == _execute(
        conn, "SELECT * FROM core.plays ORDER BY season"
    )
    renamed = [f"plays_old_{name}" for name in HISTORICALLY_COLLIDING_INDEXES]
    old_targets = _index_targets(conn, renamed)
    assert old_targets == {name: "plays_old" for name in renamed}
    _assert_caller_can_read(conn, "anon")
    _assert_caller_can_read(conn, "authenticated")

    before = _index_snapshot(conn)
    repair_play_indexes(conn, names)
    assert _index_snapshot(conn) == before


def test_explicit_selection_uses_builtin_predicate_under_shadowed_search_path(warehouse_db):
    conn, _ = warehouse_db
    _create_historical_swap(conn)
    _execute(
        conn,
        """
        DROP INDEX core.idx_plays_competitive;
        CREATE FUNCTION public.abs(bigint) RETURNS bigint
        LANGUAGE sql IMMUTABLE STRICT
        AS 'SELECT -1::bigint';
        SET search_path = public, pg_catalog;
        """,
    )

    repair_play_indexes(conn, ["idx_plays_competitive"])

    assert _index_targets(conn, ["idx_plays_competitive"]) == {"idx_plays_competitive": "plays"}
    assert _index_targets(conn, ["idx_plays_offense"]) == {"idx_plays_offense": "plays_old"}
    assert "pg_catalog.abs" in _parent_index_definitions(conn)["idx_plays_competitive"][2]
    assert _execute(
        conn,
        """
        SELECT count(*)
        FROM pg_catalog.pg_depend dependency
        WHERE dependency.objid = 'core.idx_plays_competitive'::regclass
          AND dependency.refobjid = 'public.abs(bigint)'::regprocedure
        """,
    ) == [(0,)]
    with conn.cursor() as cur:
        validate_play_indexes(cur, ["idx_plays_competitive"])
        with pytest.raises(PlayIndexStateError):
            validate_play_indexes(cur)
    conn.rollback()


def test_same_name_wrong_key_predicate_uniqueness_and_order_are_rejected(warehouse_db):
    conn, _ = warehouse_db
    _create_partitioned_plays(conn)
    _execute(
        conn,
        """
        CREATE FUNCTION public.abs(bigint) RETURNS bigint
        LANGUAGE sql IMMUTABLE STRICT
        AS 'SELECT -1::bigint';
        SET search_path = public, pg_catalog;
        """,
    )
    wrong_definitions = {
        "idx_plays_game_id": "CREATE INDEX idx_plays_game_id ON core.plays(defense)",
        "idx_plays_offense": (
            "CREATE INDEX idx_plays_offense ON core.plays(offense) WHERE offense IS NOT NULL"
        ),
        "idx_plays_defense": (
            "CREATE UNIQUE INDEX idx_plays_defense ON core.plays(defense, season)"
        ),
        "plays_dlt_id_unique": (
            "CREATE UNIQUE INDEX plays_dlt_id_unique ON core.plays(season, _dlt_id)"
        ),
        "idx_plays_score_diff": (
            "CREATE INDEX idx_plays_score_diff ON ONLY core.plays(score_diff)"
        ),
        "idx_plays_competitive": (
            "CREATE INDEX idx_plays_competitive ON core.plays(game_id, period) "
            "WHERE abs(offense_score - defense_score) <= 28"
        ),
    }
    with conn.cursor() as cur:
        for statement in wrong_definitions.values():
            cur.execute(statement)
    conn.commit()
    with conn.cursor() as cur:
        with pytest.raises(PlayIndexStateError):
            validate_play_indexes(cur, ["plays_dlt_id_unique"])
    conn.rollback()
    _execute(
        conn,
        """
        DROP INDEX core.plays_dlt_id_unique;
        ALTER TABLE core.plays
            ADD CONSTRAINT plays_dlt_id_unique
            UNIQUE (_dlt_id, season) DEFERRABLE INITIALLY DEFERRED
        """,
    )
    assert _execute(
        conn,
        """
        SELECT NOT index_state.indimmediate
        FROM pg_catalog.pg_index index_state
        WHERE index_state.indexrelid = 'core.plays_dlt_id_unique'::regclass
        """,
    ) == [(True,)]
    before = _index_snapshot(conn)

    for name in wrong_definitions:
        with conn.cursor() as cur:
            with pytest.raises(PlayIndexStateError):
                validate_play_indexes(cur, [name])
        conn.rollback()
    with pytest.raises(PlayIndexStateError):
        repair_play_indexes(conn, list(wrong_definitions))
    assert _index_snapshot(conn) == before
    assert not any(parent == "idx_plays_score_diff" for parent, *_ in _attached_child_indexes(conn))


def test_expected_name_on_unsupported_table_fails_before_index_mutation(warehouse_db):
    conn, _ = warehouse_db
    _create_partitioned_plays(conn)
    _execute(
        conn,
        "CREATE TABLE core.unrelated(id bigint); "
        "CREATE INDEX idx_plays_game_id ON core.unrelated(id)",
    )
    before = _index_snapshot(conn)

    with pytest.raises(PlayIndexStateError, match="unrelated|owner|target"):
        repair_play_indexes(conn, ["idx_plays_game_id"])

    assert _index_snapshot(conn) == before
    assert _index_targets(conn, ["idx_plays_game_id"]) == {"idx_plays_game_id": "unrelated"}


def test_retained_index_rename_collision_fails_before_any_rename(warehouse_db):
    conn, _ = warehouse_db
    _create_historical_swap(conn)
    _execute(conn, "CREATE INDEX plays_old_idx_plays_game_id ON core.plays_old(period)")
    before = _index_snapshot(conn)

    with pytest.raises(PlayIndexStateError, match="plays_old_idx_plays_game_id"):
        repair_play_indexes(conn, ["idx_plays_game_id"])

    assert _index_snapshot(conn) == before
    assert _index_targets(conn, ["idx_plays_game_id"]) == {"idx_plays_game_id": "plays_old"}


def test_equivalent_live_parent_index_blocks_duplicate_repair(warehouse_db):
    conn, _ = warehouse_db
    _create_historical_swap(conn)
    _execute(conn, "CREATE INDEX equivalent_game_id ON core.plays(game_id)")
    before = _index_snapshot(conn)

    with pytest.raises(PlayIndexStateError, match="equivalent|duplicate"):
        repair_play_indexes(conn, ["idx_plays_game_id"])

    assert _index_snapshot(conn) == before
    assert _index_targets(conn, ["idx_plays_game_id"]) == {"idx_plays_game_id": "plays_old"}


def test_injected_build_failure_rolls_back_old_index_rename(warehouse_db):
    conn, _ = warehouse_db
    _create_historical_swap(conn)
    before = _index_snapshot(conn)

    class FailingCursor:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        def execute(self, statement, values=None):
            rendered = str(statement)
            if "CREATE" in rendered and "INDEX" in rendered:
                raise RuntimeError("injected index build failure")
            return self.wrapped.execute(statement, values)

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

    class FailingConnection:
        autocommit = False

        def __init__(self, wrapped):
            self.wrapped = wrapped

        def cursor(self):
            return FailingCursor(self.wrapped.cursor())

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

    with pytest.raises(RuntimeError, match="injected index build failure"):
        repair_play_indexes(FailingConnection(conn), ["idx_plays_game_id"])

    assert _index_snapshot(conn) == before
    assert _index_targets(conn, ["idx_plays_game_id"]) == {"idx_plays_game_id": "plays_old"}


def test_index_name_swap_after_preflight_cannot_rename_the_wrong_index(warehouse_db):
    conn, target = warehouse_db
    _create_historical_swap(conn)
    _execute(
        conn,
        "CREATE TABLE core.unrelated(id bigint); CREATE INDEX race_unrelated ON core.unrelated(id)",
    )
    identities = dict(
        _execute(
            conn,
            """
            SELECT index_class.relname, index_class.oid
            FROM pg_catalog.pg_class index_class
            WHERE index_class.relnamespace = 'core'::regnamespace
              AND index_class.relname IN ('idx_plays_game_id', 'race_unrelated')
            """,
        )
    )
    contender = psycopg2.connect(target, options="-c lock_timeout=2s")

    class RacingCursor:
        def __init__(self, wrapped):
            self.wrapped = wrapped
            self.raced = False

        def execute(self, statement, values=None):
            rendered = str(statement)
            if not self.raced and "ALTER INDEX" in rendered:
                self.raced = True
                with contender.cursor() as race_cur:
                    race_cur.execute(
                        "ALTER INDEX core.idx_plays_game_id RENAME TO raced_old_game_id"
                    )
                    race_cur.execute("ALTER INDEX core.race_unrelated RENAME TO idx_plays_game_id")
                contender.commit()
            return self.wrapped.execute(statement, values)

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

    class RacingConnection:
        autocommit = False

        def __init__(self, wrapped):
            self.wrapped = wrapped
            self.cursor_instance = None

        def cursor(self):
            self.cursor_instance = RacingCursor(self.wrapped.cursor())
            return self.cursor_instance

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

    racing = RacingConnection(conn)
    try:
        with pytest.raises(PlayIndexStateError, match="changed|identity|race"):
            repair_play_indexes(racing, ["idx_plays_game_id"])
        assert racing.cursor_instance.raced
    finally:
        contender.close()

    after = dict(
        _execute(
            conn,
            """
            SELECT index_class.relname, index_class.oid
            FROM pg_catalog.pg_class index_class
            WHERE index_class.relnamespace = 'core'::regnamespace
              AND index_class.relname IN (
                  'idx_plays_game_id', 'raced_old_game_id',
                  'plays_old_idx_plays_game_id'
              )
            """,
        )
    )
    assert after == {
        "idx_plays_game_id": identities["race_unrelated"],
        "raced_old_game_id": identities["idx_plays_game_id"],
    }
    assert _index_targets(conn, ["idx_plays_game_id", "raced_old_game_id"]) == {
        "idx_plays_game_id": "unrelated",
        "raced_old_game_id": "plays_old",
    }


def test_duplicate_unique_keys_roll_back_rename_and_leave_data_intact(warehouse_db):
    conn, _ = warehouse_db
    _create_partitioned_plays(conn)
    _execute(
        conn,
        """
        CREATE TABLE core.plays_old (LIKE core.plays);
        INSERT INTO core.plays_old VALUES
            ('old', 2025, 1, 'Alpha', 'Beta', 'Rush', 7, 1, 14, 7);
        CREATE UNIQUE INDEX plays_dlt_id_unique
            ON core.plays_old(_dlt_id, season);
        INSERT INTO core.plays VALUES
            ('duplicate', 2026, 3, 'One', 'Two', 'Rush', 0, 1, 7, 7),
            ('duplicate', 2026, 4, 'Three', 'Four', 'Pass', 0, 2, 7, 7)
        """,
    )
    before = _index_snapshot(conn)

    with pytest.raises(
        psycopg2.errors.UniqueViolation,
        match="could not create unique index|duplicated|duplicate key",
    ):
        repair_play_indexes(conn, ["plays_dlt_id_unique"])

    assert _index_snapshot(conn) == before
    assert _index_targets(conn, ["plays_dlt_id_unique"]) == {"plays_dlt_id_unique": "plays_old"}
    assert _execute(
        conn,
        "SELECT _dlt_id, season, count(*) FROM core.plays "
        "GROUP BY _dlt_id, season HAVING count(*) > 1",
    ) == [("duplicate", 2026, 2)]


def test_f08_future_partition_inherits_every_repaired_index(warehouse_db):
    conn, _ = warehouse_db
    _create_historical_swap(conn)
    names = list(EXPECTED_PLAY_INDEXES)
    repair_play_indexes(conn, names)

    plan = ensure_play_partitions(conn, [2027], create=True, calendar_year=2026)

    assert plan.created_years == (2027,)
    coverage = _attached_child_indexes(conn, child_name="plays_y2027")
    assert len(coverage) == len(names)
    assert {parent for parent, _, _, _ in coverage} == set(names)
    assert all(valid and ready for _, _, valid, ready in coverage)
