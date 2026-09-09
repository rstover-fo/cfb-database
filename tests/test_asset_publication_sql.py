"""Executed F14 house-Elo publication receipts on isolated PostgreSQL 17."""

from __future__ import annotations

import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from time import monotonic, sleep

import psycopg2
import pytest

from src.pipelines.game_identity import CANCELLED_GAME_IDS, SUPERSEDED_GAME_REPLACEMENTS
from tests.test_warehouse_migrations_sql import query
from tests.test_warehouse_migrations_sql import warehouse_db as _warehouse_db  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = [
    ROOT / "src/schemas/migrations/066_operational_quota_ledger.sql",
    ROOT / "src/schemas/migrations/067_cfbd_transport_admission.sql",
    ROOT / "src/schemas/migrations/068_asset_publication_receipts.sql",
]
EXCLUDED_GAME_IDS = sorted(set(CANCELLED_GAME_IDS) | set(SUPERSEDED_GAME_REPLACEMENTS))

SETUP = """
DO $roles$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='anon') THEN
        CREATE ROLE anon NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='authenticated') THEN
        CREATE ROLE authenticated NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='analyst_ro') THEN
        CREATE ROLE analyst_ro NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='publication_bystander') THEN
        CREATE ROLE publication_bystander NOLOGIN;
    END IF;
END $roles$;
ALTER DEFAULT PRIVILEGES GRANT ALL ON SCHEMAS
    TO anon, authenticated, analyst_ro, publication_bystander;
ALTER DEFAULT PRIVILEGES GRANT ALL ON TABLES
    TO anon, authenticated, analyst_ro, publication_bystander;
ALTER DEFAULT PRIVILEGES GRANT ALL ON SEQUENCES
    TO anon, authenticated, analyst_ro, publication_bystander;
ALTER DEFAULT PRIVILEGES GRANT ALL ON FUNCTIONS
    TO anon, authenticated, analyst_ro, publication_bystander;
"""

HOUSE_ELO_OBJECTS = """
CREATE SCHEMA core;
CREATE SCHEMA analytics;
CREATE SCHEMA marts;

CREATE TABLE core.games (
    id bigint PRIMARY KEY,
    season bigint,
    week bigint,
    season_type varchar,
    start_date timestamptz,
    start_time_tbd boolean,
    completed boolean,
    neutral_site boolean,
    conference_game boolean,
    home_id bigint,
    home_team varchar,
    home_classification varchar,
    home_conference varchar,
    home_points bigint,
    away_id bigint,
    away_team varchar,
    away_points bigint,
    highlights varchar,
    _dlt_load_id varchar NOT NULL,
    _dlt_id varchar NOT NULL,
    attendance bigint,
    venue_id bigint,
    venue varchar,
    home_postgame_win_probability double precision,
    home_pregame_elo bigint,
    home_postgame_elo bigint,
    away_classification varchar,
    away_conference varchar,
    away_postgame_win_probability double precision,
    away_pregame_elo bigint,
    away_postgame_elo bigint,
    excitement_index double precision,
    notes varchar,
    playoff__competition varchar,
    playoff__format varchar,
    playoff__round varchar,
    playoff__round_name varchar,
    playoff__bracket_slot varchar,
    playoff__home_seed bigint,
    playoff__away_seed bigint,
    playoff__bowl_name varchar
);

INSERT INTO core.games (
    id, season, week, season_type, start_date, start_time_tbd, completed,
    neutral_site, conference_game, home_id, home_team, home_classification,
    home_conference, home_points, away_id, away_team, away_points,
    _dlt_load_id, _dlt_id, away_classification, away_conference,
    home_pregame_elo, away_pregame_elo
) VALUES
    (101, 2025, 1, 'regular', '2025-08-30 17:00:00+00', false, true,
     false, true, 1, 'Alpha', 'fbs', 'Test', 28, 2, 'Beta', 14,
     'load-1', 'game-101', 'fbs', 'Test', 1510, 1490),
    (102, 2025, 2, 'regular', '2025-09-06 17:00:00+00', false, false,
     false, true, 1, 'Alpha', 'fbs', 'Test', NULL, 3, 'Gamma', NULL,
     'load-1', 'game-102', 'fbs', 'Test', NULL, NULL);

CREATE TABLE analytics.house_elo_game (
    game_id bigint NOT NULL,
    season bigint NOT NULL,
    week bigint,
    season_type varchar,
    start_date timestamptz,
    neutral_site boolean,
    home_team varchar NOT NULL,
    away_team varchar NOT NULL,
    home_pregame_elo numeric(8,2),
    away_pregame_elo numeric(8,2),
    home_postgame_elo numeric(8,2),
    away_postgame_elo numeric(8,2),
    home_win_prob numeric(5,4),
    expected_home_margin numeric(6,2),
    actual_home_margin bigint,
    mov_multiplier numeric(6,3),
    cfbd_home_pregame_elo numeric(8,2),
    cfbd_away_pregame_elo numeric(8,2)
);
CREATE UNIQUE INDEX house_elo_game_key ON analytics.house_elo_game(game_id);

CREATE TABLE analytics.house_elo_current (
    team varchar NOT NULL,
    season bigint,
    rating numeric(8,2) NOT NULL,
    games_played bigint,
    last_game_id bigint,
    last_game_date timestamptz,
    low_confidence boolean,
    updated_at timestamptz
);
CREATE UNIQUE INDEX house_elo_current_key ON analytics.house_elo_current(team);

CREATE MATERIALIZED VIEW marts.house_elo_game AS
SELECT game_id, season, week, season_type, start_date, neutral_site,
    home_team, away_team, home_pregame_elo, away_pregame_elo,
    home_postgame_elo, away_postgame_elo, home_win_prob,
    expected_home_margin, actual_home_margin, mov_multiplier,
    cfbd_home_pregame_elo, cfbd_away_pregame_elo,
    expected_home_margin - actual_home_margin AS margin_error,
    abs(expected_home_margin - actual_home_margin) AS abs_margin_error
FROM analytics.house_elo_game;
CREATE UNIQUE INDEX house_elo_game_mart_key ON marts.house_elo_game(game_id);
"""


@pytest.fixture
def publication_db(request):
    conn, target = request.getfixturevalue("_warehouse_db")
    query(conn, SETUP)
    query(conn, MIGRATIONS[0].read_text())
    query(conn, MIGRATIONS[1].read_text())
    query(conn, HOUSE_ELO_OBJECTS)
    query(conn, MIGRATIONS[2].read_text())
    query(conn, MIGRATIONS[2].read_text())
    return conn, target


def publisher_query(conn, statement, values=None):
    with conn.cursor() as cur:
        cur.execute("SET LOCAL ROLE warehouse_publisher")
        cur.execute(statement, values)
        result = cur.fetchall() if cur.description else None
    conn.commit()
    return result


def start_run(conn, run_id=None):
    run_id = run_id or uuid.uuid4()
    publisher_query(
        conn,
        "SELECT warehouse_publication.start_house_elo_run(%s)",
        (str(run_id),),
    )
    return run_id


def state(conn):
    return publisher_query(
        conn,
        "SELECT * FROM warehouse_publication.get_house_elo_state(1869, 2025)",
    )[0]


def game_rows(*, postgame=1518.25):
    return [
        {
            "game_id": 101,
            "season": 2025,
            "week": 1,
            "season_type": "regular",
            "start_date": "2025-08-30T17:00:00+00:00",
            "neutral_site": False,
            "home_team": "Alpha",
            "away_team": "Beta",
            "home_pregame_elo": 1500,
            "away_pregame_elo": 1500,
            "home_postgame_elo": postgame,
            "away_postgame_elo": 3000 - postgame,
            "home_win_prob": 0.5925,
            "expected_home_margin": 2.95,
            "actual_home_margin": 14,
            "mov_multiplier": 1.322,
            "cfbd_home_pregame_elo": 1510,
            "cfbd_away_pregame_elo": 1490,
        }
    ]


def snapshot(*, alpha_rating=1518.25):
    return [
        {
            "team": "Alpha",
            "season": 2025,
            "rating": alpha_rating,
            "games_played": 1,
            "last_game_id": 101,
            "last_game_date": "2025-08-30T17:00:00+00:00",
            "low_confidence": True,
        },
        {
            "team": "Beta",
            "season": 2025,
            "rating": 3000 - alpha_rating,
            "games_played": 1,
            "last_game_id": 101,
            "last_game_date": "2025-08-30T17:00:00+00:00",
            "low_confidence": True,
        },
    ]


def publication_args(
    conn,
    *,
    run_id=None,
    source_generation=None,
    mart_generation=None,
    expected=None,
    digest=None,
    postgame=1518.25,
):
    expected = expected or state(conn)
    return (
        str(run_id or start_run(conn)),
        str(source_generation or uuid.uuid4()),
        str(mart_generation or uuid.uuid4()),
        str(expected[0]) if expected[0] else None,
        str(expected[1]) if expected[1] else None,
        digest or expected[2],
        json.dumps(game_rows(postgame=postgame)),
        json.dumps(snapshot(alpha_rating=postgame)),
        1869,
        2025,
        EXCLUDED_GAME_IDS,
    )


PUBLISH_SQL = """
SELECT * FROM warehouse_publication.publish_house_elo(
    %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s::bigint[]
)
"""


def publish(conn, args):
    return publisher_query(conn, PUBLISH_SQL, args)[0]


def current_snapshot(conn):
    return query(
        conn,
        """
        SELECT
            (SELECT array_agg((game_id, home_postgame_elo)::text ORDER BY game_id)
             FROM analytics.house_elo_game),
            (SELECT array_agg((game_id, home_postgame_elo)::text ORDER BY game_id)
             FROM marts.house_elo_game),
            (SELECT array_agg((team, rating)::text ORDER BY team)
             FROM analytics.house_elo_current),
            (SELECT array_agg((asset_key, generation_id)::text ORDER BY asset_key)
             FROM meta.asset_current_generations),
            (SELECT count(*) FROM meta.asset_receipts),
            (SELECT count(*) FROM meta.asset_receipt_inputs)
        """,
    )[0]


def visible_state_without_mart(conn):
    return query(
        conn,
        """
        SELECT
            (SELECT array_agg((game_id, home_postgame_elo)::text ORDER BY game_id)
             FROM analytics.house_elo_game),
            (SELECT array_agg((team, rating)::text ORDER BY team)
             FROM analytics.house_elo_current),
            (SELECT array_agg((asset_key, generation_id)::text ORDER BY asset_key)
             FROM meta.asset_current_generations),
            (SELECT count(*) FROM meta.asset_receipts),
            (SELECT count(*) FROM meta.asset_receipt_inputs)
        """,
    )[0]


def test_migration_is_idempotent_private_and_publisher_role_is_bounded(publication_db):
    conn, _ = publication_db
    assert query(
        conn,
        """
        SELECT rolcanlogin, rolinherit, rolsuper, rolcreatedb, rolcreaterole,
            rolreplication, rolbypassrls
        FROM pg_roles WHERE rolname='warehouse_publisher'
        """,
    ) == [(False, False, False, False, False, False, False)]

    for role in ("anon", "authenticated", "analyst_ro", "publication_bystander"):
        assert query(
            conn,
            """
            SELECT has_schema_privilege(%s, 'warehouse_publication', 'USAGE,CREATE'),
                has_table_privilege(%s, 'meta.asset_receipts',
                    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'),
                has_table_privilege(%s, 'meta.asset_current_generations',
                    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'),
                has_function_privilege(%s,
                    'warehouse_publication.get_house_elo_state(bigint,bigint)', 'EXECUTE')
            """,
            (role, role, role, role),
        ) == [(False, False, False, False)]

    assert query(
        conn,
        """
        SELECT has_schema_privilege('warehouse_publisher','warehouse_publication','USAGE'),
            has_schema_privilege('warehouse_publisher','warehouse_publication','CREATE'),
            has_table_privilege('warehouse_publisher','meta.asset_receipts','SELECT,INSERT'),
            has_function_privilege('warehouse_publisher',
                'warehouse_publication.publish_house_elo(uuid,uuid,uuid,uuid,uuid,text,jsonb,jsonb,bigint,bigint,bigint[])',
                'EXECUTE')
        """,
    ) == [(True, False, False, True)]
    for role, statement in (
        ("anon", "SELECT * FROM warehouse_publication.get_house_elo_state(1869,2025)"),
        ("warehouse_publisher", "INSERT INTO meta.asset_receipts DEFAULT VALUES"),
        ("warehouse_publisher", "DELETE FROM meta.asset_current_generations"),
    ):
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL ROLE {role}")
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cur.execute(statement)
        conn.rollback()


def test_publication_data_receipts_dependencies_and_pointers_commit_atomically(publication_db):
    conn, target = publication_db
    first = publication_args(conn)
    result = publish(conn, first)
    assert result[0:3] == (first[1], first[2], False)
    assert result[3:] == (1, 1)
    committed = current_snapshot(conn)
    assert committed[4:] == (2, 1)
    assert query(
        conn,
        """
        SELECT child.asset_key,parent.asset_key
        FROM meta.asset_receipt_inputs edge
        JOIN meta.asset_receipts child ON child.generation_id=edge.generation_id
        JOIN meta.asset_receipts parent ON parent.generation_id=edge.input_generation_id
        """,
    ) == [("marts.house_elo_game", "analytics.house_elo_game")]
    assert query(
        conn,
        """
        SELECT count(*)
        FROM meta.asset_current_generations pointer
        JOIN meta.asset_receipts receipt USING (asset_key,coverage_key,generation_id)
        WHERE receipt.outcome='succeeded'
          AND receipt.coverage->'complete'='true'::jsonb
        """,
    ) == [(2,)]
    committed_visible = visible_state_without_mart(conn)

    observer = psycopg2.connect(target)
    second = publication_args(conn, expected=state(conn), postgame=1524.0)
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL ROLE warehouse_publisher")
            cur.execute(PUBLISH_SQL, second)
            assert cur.fetchone()[2] is False
        # REFRESH holds AccessExclusive on the mart until transaction end, so
        # inspect the independently readable source, snapshot and metadata.
        assert visible_state_without_mart(observer) == committed_visible
        conn.rollback()
        assert current_snapshot(observer) == committed

        with conn.cursor() as cur:
            cur.execute("SET LOCAL ROLE warehouse_publisher")
            cur.execute(PUBLISH_SQL, second)
            cur.fetchone()
            with pytest.raises(psycopg2.errors.DivisionByZero):
                cur.execute("SELECT 1/0")
        conn.rollback()
        assert current_snapshot(observer) == committed
    finally:
        observer.close()


def test_python_full_publication_serializes_and_finishes_one_operation(publication_db):
    from scripts.compute_house_elo import run_full_published

    conn, _ = publication_db
    rows = run_full_published(conn)
    assert len(rows) == 1
    assert rows[0]["game_id"] == 101
    assert query(
        conn,
        "SELECT game_id FROM analytics.house_elo_game ORDER BY game_id",
    ) == [(101,)]
    assert query(
        conn,
        "SELECT game_id FROM marts.house_elo_game ORDER BY game_id",
    ) == [(101,)]
    assert query(
        conn,
        """
        SELECT operation_kind,outcome,error_summary
        FROM meta.operation_runs ORDER BY started_at
        """,
    ) == [("compute", "succeeded", None)]
    assert query(
        conn,
        """
        SELECT count(*), count(*) FILTER (WHERE outcome='succeeded')
        FROM meta.asset_receipts
        """,
    ) == [(2, 2)]
    assert query(conn, "SELECT count(*) FROM meta.asset_receipt_inputs") == [(1,)]
    assert query(conn, "SELECT count(*) FROM meta.asset_current_generations") == [(2,)]


def test_concurrent_compare_and_swap_has_one_winner_and_no_loser_receipts(publication_db):
    conn, target = publication_db
    expected = state(conn)
    args = [
        publication_args(conn, expected=expected, postgame=1518.0),
        publication_args(conn, expected=expected, postgame=1522.0),
    ]

    def contender(call_args):
        other = psycopg2.connect(target)
        try:
            try:
                return ("ok", publisher_query(other, PUBLISH_SQL, call_args)[0])
            except psycopg2.Error as exc:
                other.rollback()
                return ("error", str(exc))
        finally:
            other.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(contender, args))
    assert sorted(result[0] for result in results) == ["error", "ok"]
    assert "changed" in next(result[1] for result in results if result[0] == "error").lower()
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts") == [(2,)]
    assert query(conn, "SELECT count(*) FROM meta.asset_current_generations") == [(2,)]


def test_unfinished_schedule_change_changes_digest_and_rejects_stale_publication(publication_db):
    conn, _ = publication_db
    before = state(conn)
    args = publication_args(conn, expected=before)
    query(
        conn,
        """
        UPDATE core.games
        SET week=3, start_date='2025-09-13 19:30:00+00', start_time_tbd=true
        WHERE id=102
        """,
    )
    after = state(conn)
    assert after[2] != before[2]
    with pytest.raises(psycopg2.Error, match=r"core\.games changed|digest"):
        publish(conn, args)
    conn.rollback()
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts") == [(0,)]
    assert query(conn, "SELECT count(*) FROM meta.asset_current_generations") == [(0,)]


def test_committed_correction_while_publisher_waits_on_source_lock_is_detected(
    publication_db,
):
    conn, target = publication_db
    args = publication_args(conn)
    writer = psycopg2.connect(target)
    started = Event()
    pool = ThreadPoolExecutor(max_workers=1)

    def waiting_publisher():
        other = psycopg2.connect(target, application_name="receipt-digest-waiter")
        try:
            started.set()
            try:
                publisher_query(other, PUBLISH_SQL, args)
                return ("ok", "")
            except psycopg2.Error as exc:
                other.rollback()
                return ("error", str(exc))
        finally:
            other.close()

    try:
        with writer.cursor() as cur:
            cur.execute(
                """
                UPDATE core.games
                SET week=4, start_date='2025-09-20 20:00:00+00', start_time_tbd=true
                WHERE id=102
                """
            )
        future = pool.submit(waiting_publisher)
        assert started.wait(timeout=5)
        deadline = monotonic() + 5
        while monotonic() < deadline:
            blocked = query(
                conn,
                """
                SELECT wait_event_type='Lock'
                FROM pg_stat_activity
                WHERE application_name='receipt-digest-waiter'
                  AND state='active'
                """,
            )
            if blocked == [(True,)]:
                break
            sleep(0.02)
        else:
            pytest.fail("publisher never waited for the core.games table lock")
        writer.commit()
        status, detail = future.result(timeout=10)
        assert status == "error"
        assert "core.games" in detail.lower() and "changed" in detail.lower()
    finally:
        writer.rollback()
        writer.close()
        pool.shutdown(wait=True)
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts") == [(0,)]
    assert query(conn, "SELECT count(*) FROM meta.asset_current_generations") == [(0,)]


def test_expected_no_data_atomically_clears_outputs_and_advances_eligible_pointers(
    publication_db,
):
    conn, _ = publication_db
    publish(conn, publication_args(conn))
    query(conn, "UPDATE core.games SET completed=false, home_points=NULL, away_points=NULL")
    expected = state(conn)
    args = list(publication_args(conn, expected=expected))
    args[6] = "[]"
    args[7] = "[]"
    result = publish(conn, tuple(args))
    assert result[2:] == (False, 0, 0)
    assert query(conn, "SELECT count(*) FROM analytics.house_elo_game") == [(0,)]
    assert query(conn, "SELECT count(*) FROM analytics.house_elo_current") == [(0,)]
    assert query(conn, "SELECT count(*) FROM marts.house_elo_game") == [(0,)]
    assert query(
        conn,
        """
        SELECT receipt.outcome,receipt.coverage->>'complete'
        FROM meta.asset_current_generations pointer
        JOIN meta.asset_receipts receipt USING(asset_key,coverage_key,generation_id)
        ORDER BY receipt.asset_key
        """,
    ) == [("expected_no_data", "true"), ("expected_no_data", "true")]


@pytest.mark.parametrize("malformation", ["missing", "duplicate", "required_null"])
def test_malformed_or_incomplete_payload_cannot_publish(publication_db, malformation):
    conn, _ = publication_db
    args = list(publication_args(conn))
    rows = game_rows()
    if malformation == "missing":
        rows = []
    elif malformation == "duplicate":
        rows = rows * 2
    else:
        rows[0]["home_team"] = None
    args[6] = json.dumps(rows)
    with pytest.raises(psycopg2.Error):
        publish(conn, tuple(args))
    conn.rollback()
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts") == [(0,)]
    assert query(conn, "SELECT count(*) FROM meta.asset_current_generations") == [(0,)]
    assert query(conn, "SELECT count(*) FROM analytics.house_elo_game") == [(0,)]


def test_publisher_rejects_non_read_committed_transaction(publication_db):
    conn, _ = publication_db
    args = publication_args(conn)
    with conn.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        cur.execute("SET LOCAL ROLE warehouse_publisher")
        with pytest.raises(psycopg2.Error, match="READ COMMITTED|isolation"):
            cur.execute(PUBLISH_SQL, args)
    conn.rollback()
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts") == [(0,)]


def test_exact_replay_does_not_repoint_after_a_newer_publication(publication_db):
    conn, _ = publication_db
    first = publication_args(conn)
    first_result = publish(conn, first)
    second = publication_args(conn, expected=state(conn), postgame=1524.0)
    second_result = publish(conn, second)
    pointers = query(
        conn,
        "SELECT asset_key,generation_id FROM meta.asset_current_generations ORDER BY asset_key",
    )

    replay = publish(conn, first)
    assert replay[0:2] == first_result[0:2]
    assert replay[2] is True
    assert replay[3:] == (1, 1)
    assert (
        query(
            conn,
            "SELECT asset_key,generation_id FROM meta.asset_current_generations ORDER BY asset_key",
        )
        == pointers
    )
    assert pointers == [
        ("analytics.house_elo_game", second_result[0]),
        ("marts.house_elo_game", second_result[1]),
    ]
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts") == [(4,)]


def test_legacy_dml_invalidates_dependents_and_refresh_invalidates_the_mart(publication_db):
    conn, _ = publication_db
    publish(conn, publication_args(conn))

    query(conn, "UPDATE analytics.house_elo_game SET home_postgame_elo=1600")
    assert query(conn, "SELECT count(*) FROM meta.asset_current_generations") == [(0,)]

    publish(conn, publication_args(conn, expected=state(conn), postgame=1519.0))
    query(conn, "REFRESH MATERIALIZED VIEW marts.house_elo_game")
    assert query(
        conn, "SELECT asset_key FROM meta.asset_current_generations ORDER BY asset_key"
    ) == [("analytics.house_elo_game",)]

    publish(conn, publication_args(conn, expected=state(conn), postgame=1520.0))
    query(conn, "REFRESH MATERIALIZED VIEW CONCURRENTLY marts.house_elo_game")
    assert query(
        conn, "SELECT asset_key FROM meta.asset_current_generations ORDER BY asset_key"
    ) == [("analytics.house_elo_game",)]

    query(conn, "TRUNCATE analytics.house_elo_game")
    assert query(conn, "SELECT count(*) FROM meta.asset_current_generations") == [(0,)]


def test_ddl_invalidation_and_disabled_source_guard_fail_closed(publication_db):
    conn, _ = publication_db
    publish(conn, publication_args(conn))
    query(conn, "ALTER MATERIALIZED VIEW marts.house_elo_game RENAME TO house_elo_game_moved")
    assert query(
        conn, "SELECT asset_key FROM meta.asset_current_generations ORDER BY asset_key"
    ) == [("analytics.house_elo_game",)]
    query(conn, "ALTER MATERIALIZED VIEW marts.house_elo_game_moved RENAME TO house_elo_game")

    query(conn, "ALTER TABLE analytics.house_elo_game DISABLE TRIGGER USER")
    assert query(conn, "SELECT count(*) FROM meta.asset_current_generations") == [(0,)]
    with pytest.raises(psycopg2.Error, match="trigger|guard|invalidation"):
        publish(conn, publication_args(conn, expected=state(conn), postgame=1521.0))
    conn.rollback()
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts") == [(2,)]
    assert query(conn, "SELECT count(*) FROM meta.asset_current_generations") == [(0,)]


def test_failed_receipts_are_append_only_and_never_advance_pointers(publication_db):
    conn, _ = publication_db
    run_id = start_run(conn)
    source_generation = uuid.uuid4()
    mart_generation = uuid.uuid4()
    statement = """
        SELECT * FROM warehouse_publication.record_house_elo_failure(%s,%s,%s,%s,%s)
    """
    values = (
        str(run_id),
        str(source_generation),
        str(mart_generation),
        "failed",
        "publication_failed",
    )
    first = publisher_query(conn, statement, values)[0]
    replay = publisher_query(conn, statement, values)[0]
    assert first == (str(source_generation), str(mart_generation), False)
    assert replay == (str(source_generation), str(mart_generation), True)
    assert query(
        conn,
        "SELECT asset_key,outcome,error_summary FROM meta.asset_receipts ORDER BY asset_key",
    ) == [
        ("analytics.house_elo_game", "failed", "publication_failed"),
        ("marts.house_elo_game", "failed", "publication_failed"),
    ]
    assert query(conn, "SELECT count(*) FROM meta.asset_current_generations") == [(0,)]

    with pytest.raises(psycopg2.Error, match="eligible|successful|current"):
        query(
            conn,
            """
            INSERT INTO meta.asset_current_generations(asset_key,generation_id)
            VALUES ('analytics.house_elo_game', %s)
            """,
            (str(source_generation),),
        )
    conn.rollback()

    for command in (
        "UPDATE meta.asset_receipts SET error_summary='rewritten'",
        "DELETE FROM meta.asset_receipts",
        "TRUNCATE meta.asset_receipts CASCADE",
    ):
        with pytest.raises(psycopg2.Error, match="append-only|cannot be"):
            query(conn, command)
        conn.rollback()


@pytest.mark.parametrize(
    ("schema", "remaining"),
    [("analytics", []), ("marts", [("analytics.house_elo_game",)])],
)
def test_schema_rename_invalidates_canonical_asset_pointers(publication_db, schema, remaining):
    conn, _ = publication_db
    publish(conn, publication_args(conn))
    query(conn, f"ALTER SCHEMA {schema} RENAME TO renamed_{schema}")
    assert (
        query(conn, "SELECT asset_key FROM meta.asset_current_generations ORDER BY asset_key")
        == remaining
    )
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts") == [(2,)]


def test_refresh_queued_before_publication_commit_invalidates_the_new_mart_pointer(publication_db):
    conn, target = publication_db
    args = publication_args(conn)
    observer = psycopg2.connect(target)
    pool = ThreadPoolExecutor(max_workers=1)

    def refresh():
        other = psycopg2.connect(target, application_name="receipt-refresh-waiter")
        try:
            query(other, "SET statement_timeout='10s'")
            query(other, "REFRESH MATERIALIZED VIEW marts.house_elo_game")
        finally:
            other.close()

    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL ROLE warehouse_publisher")
            cur.execute(PUBLISH_SQL, args)
            cur.fetchone()
        future = pool.submit(refresh)
        deadline = monotonic() + 5
        while monotonic() < deadline:
            waiting = query(
                observer,
                "SELECT wait_event_type='Lock' FROM pg_stat_activity "
                "WHERE datname=current_database() AND application_name='receipt-refresh-waiter'",
            )
            if waiting == [(True,)]:
                break
            sleep(0.02)
        else:
            pytest.fail("legacy refresh did not wait on the publishing transaction")
        conn.commit()
        future.result(timeout=10)
        assert query(
            observer, "SELECT asset_key FROM meta.asset_current_generations ORDER BY asset_key"
        ) == [("analytics.house_elo_game",)]
        assert query(observer, "SELECT count(*) FROM meta.asset_receipts") == [(2,)]
    finally:
        conn.rollback()
        pool.shutdown(wait=True)
        observer.close()


def test_administrator_clears_pointers_before_disabling_event_guard(publication_db):
    conn, _ = publication_db
    publish(conn, publication_args(conn))
    query(conn, "DELETE FROM meta.asset_current_generations")
    query(conn, "ALTER EVENT TRIGGER warehouse_publication_invalidate_ddl DISABLE")
    args = publication_args(conn)
    with pytest.raises(psycopg2.Error, match="guard"):
        publish(conn, args)
    conn.rollback()
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts") == [(2,)]
    assert query(conn, "SELECT count(*) FROM meta.asset_current_generations") == [(0,)]


def add_reviewed_game(conn, game_id, *, completed=False, season=2025, home="Alpha", away="Beta"):
    query(
        conn,
        """
        INSERT INTO core.games(id,season,week,season_type,start_date,completed,
            neutral_site,home_team,away_team,home_points,away_points,
            _dlt_load_id,_dlt_id)
        VALUES (%s,%s,1,'regular','2025-08-31 17:00+00',%s,false,%s,%s,28,14,'fixture',%s)
        """,
        (game_id, season, completed, home, away, str(game_id)),
    )


@pytest.mark.parametrize(
    "defect",
    ["missing", "season", "home", "away", "original_season", "original_home", "original_away"],
)
def test_superseded_pair_defects_cannot_advance_complete_receipts(publication_db, defect):
    conn, _ = publication_db
    publish(conn, publication_args(conn))
    before = current_snapshot(conn)
    args = list(publication_args(conn, expected=state(conn)))
    original, replacement = next(iter(SUPERSEDED_GAME_REPLACEMENTS.items()))
    original_values = {}
    if defect.startswith("original_"):
        original_values[defect.removeprefix("original_")] = None
    add_reviewed_game(conn, original, **original_values)
    if defect != "missing":
        replacement_values = {}
        if defect in {"season", "home", "away"}:
            replacement_values[defect] = 2024 if defect == "season" else "Different"
        add_reviewed_game(conn, replacement, **replacement_values)
    # Supply the current digest directly to exercise locked publication validation,
    # even when the optional preparation lookup rejects the broken pair first.
    args[5] = query(
        conn,
        "SELECT md5(coalesce(string_agg(md5(to_jsonb(g)::text),'' ORDER BY id),'')) "
        "FROM core.games g",
    )[0][0]
    with pytest.raises(psycopg2.Error, match="replacement|superseded"):
        publish(conn, tuple(args))
    conn.rollback()
    assert current_snapshot(conn) == before


@pytest.mark.parametrize("original,replacement", list(SUPERSEDED_GAME_REPLACEMENTS.items()))
def test_every_reviewed_replacement_pair_can_publish(publication_db, original, replacement):
    from scripts.compute_house_elo import run_full_published

    conn, _ = publication_db
    add_reviewed_game(conn, original, completed=True)
    add_reviewed_game(conn, replacement, completed=True)
    rows = run_full_published(conn)
    assert {row["game_id"] for row in rows} == {101, replacement}
    assert query(conn, "SELECT count(*) FROM meta.asset_current_generations") == [(2,)]


def test_publisher_cannot_use_generic_operation_rpcs_even_after_migration_reapply(publication_db):
    conn, _ = publication_db
    # Simulate grants from the original PR version before reapplying the fix.
    query(
        conn,
        """
        GRANT USAGE ON SCHEMA warehouse_quota TO warehouse_publisher;
        GRANT EXECUTE ON FUNCTION
            warehouse_quota.start_operation_run(uuid,text,text,jsonb,text,text),
            warehouse_quota.finish_operation_run(uuid,text,text) TO warehouse_publisher;
        """,
    )
    query(conn, MIGRATIONS[-1].read_text())
    for statement in (
        "SELECT warehouse_quota.start_operation_run(%s,'extract','forged','{}',NULL,NULL)",
        "SELECT warehouse_quota.finish_operation_run(%s,'failed','forged')",
    ):
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            publisher_query(conn, statement, (str(uuid.uuid4()),))
        conn.rollback()
    assert query(conn, "SELECT count(*) FROM meta.operation_runs") == [(0,)]


def test_elo_wrappers_reject_other_principal_and_unrelated_operation(publication_db):
    conn, target = publication_db
    unrelated = str(uuid.uuid4())
    query(
        conn,
        "SELECT warehouse_quota.start_operation_run(%s,'extract','fixture','{}',NULL,NULL)",
        (unrelated,),
    )
    for statement in (
        "SELECT warehouse_publication.start_house_elo_run(%s)",
        "SELECT warehouse_publication.finish_house_elo_run(%s,'failed')",
    ):
        with pytest.raises(psycopg2.Error):
            publisher_query(conn, statement, (unrelated,))
        conn.rollback()
    other_role = "publication_caller_" + uuid.uuid4().hex
    query(conn, f"CREATE ROLE {other_role} NOLOGIN")
    query(conn, f"GRANT warehouse_publisher TO {other_role}")
    other = psycopg2.connect(target)
    try:
        query(other, f"SET SESSION AUTHORIZATION {other_role}")
        other_run = str(start_run(other))
        for statement in (
            "SELECT warehouse_publication.start_house_elo_run(%s)",
            "SELECT warehouse_publication.finish_house_elo_run(%s,'failed')",
        ):
            with pytest.raises(psycopg2.Error):
                publisher_query(conn, statement, (other_run,))
            conn.rollback()
        assert query(
            conn,
            "SELECT recorded_by,outcome FROM meta.operation_runs WHERE operation_run_id=%s",
            (other_run,),
        ) == [(other_role, "running")]
        assert query(
            conn, "SELECT outcome FROM meta.operation_runs WHERE operation_run_id=%s", (unrelated,)
        ) == [("running",)]
    finally:
        other.close()
        query(conn, f"DROP ROLE {other_role}")
