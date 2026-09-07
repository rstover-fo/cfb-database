"""Executed PostgreSQL regressions for the F04 prediction-provenance cutover.

These tests intentionally do not use ``db_conn``. They require an explicit
``F04_TEST_DB_URL`` pointing at an empty PostgreSQL database on loopback, then
build and remove their own minimal warehouse fixture there. This keeps schema
and role mutations away from any configured warehouse credentials.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
import pytest

from scripts import simulate_season

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS = PROJECT_ROOT / "src" / "schemas" / "migrations"
MARTS = PROJECT_ROOT / "src" / "schemas" / "marts"
API = PROJECT_ROOT / "src" / "schemas" / "api"

FIT_ID = "a" * 64
INPUT_HASH = "b" * 64
MODEL = "f04_test_v1"


@dataclass(frozen=True)
class F04Database:
    conn: psycopg2.extensions.connection
    legacy_prediction_id: int
    baseline_accuracy: tuple
    accuracy_after_history: tuple
    baseline_edges: tuple
    edges_after_history: tuple


def _execute_file(conn: psycopg2.extensions.connection, path: Path) -> None:
    with conn.cursor() as cur:
        cur.execute(path.read_text())


def _fetchone(conn: psycopg2.extensions.connection, query: str, params=None) -> tuple:
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()


def _guard_disposable_database(conn: psycopg2.extensions.connection, dsn: str) -> None:
    parsed = urlparse(dsn)
    if parsed.scheme not in {"postgres", "postgresql"}:
        pytest.fail("F04_TEST_DB_URL must be a postgresql:// URL")
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        pytest.fail("F04_TEST_DB_URL must point at a loopback PostgreSQL server")

    if _fetchone(
        conn,
        """
        SELECT COUNT(*) FROM pg_namespace
        WHERE nspname IN ('api', 'marts', 'predictions', 'metrics', 'core')
    """,
    )[0]:
        pytest.fail("F04 fixture refuses to own or remove any preexisting target schema")

    current_database, user_relation_count = _fetchone(
        conn,
        """
        SELECT
            current_database(),
            (
                SELECT count(*)
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND n.nspname !~ '^pg_toast'
                  AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
            )
        """,
    )
    requested_database = parsed.path.removeprefix("/")
    if requested_database and current_database != requested_database:
        pytest.fail(
            "F04_TEST_DB_URL database does not match the connected database "
            f"({requested_database!r} != {current_database!r})"
        )
    if user_relation_count:
        pytest.fail(
            "F04_TEST_DB_URL must target an empty disposable database; "
            f"found {user_relation_count} existing user relation(s)"
        )


def _create_roles_and_minimal_schema(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DO $roles$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
                    CREATE ROLE anon NOLOGIN;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
                    CREATE ROLE authenticated NOLOGIN;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst_ro') THEN
                    CREATE ROLE analyst_ro NOLOGIN;
                END IF;
            END
            $roles$;

            CREATE SCHEMA core;
            CREATE SCHEMA metrics;
            CREATE SCHEMA marts;
            CREATE SCHEMA api;

            CREATE TABLE core.games (
                id BIGINT PRIMARY KEY,
                season BIGINT,
                start_date TIMESTAMPTZ,
                completed BOOLEAN,
                home_points BIGINT,
                away_points BIGINT
            );
            CREATE TABLE metrics.pregame_win_probability (
                game_id BIGINT PRIMARY KEY,
                home_win_probability NUMERIC
            );

            GRANT USAGE ON SCHEMA api TO anon, authenticated, analyst_ro;
            """
        )


def _insert_prediction(
    conn: psycopg2.extensions.connection,
    *,
    game_id: int,
    mode: str,
    expected_margin: Decimal | int,
    published_at: str | None = None,
    simulated_as_of_at: str | None = None,
    experiment_label: str | None = None,
    fit_id: str | None = FIT_ID,
    input_hash: str | None = INPUT_HASH,
    input_snapshot: str | None = '{"source":"fixture"}',
    model_version: str = MODEL,
    prediction_date: str = "2026-09-01",
) -> tuple[int, object, object, bool]:
    created_at = published_at or simulated_as_of_at or "2026-09-01 12:00:00+00"
    edge = Decimal(expected_margin) - Decimal("3")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO predictions.game_predictions (
                computed_at, prediction_date, model_version, game_id, season, week,
                season_type, home_team, away_team, neutral_site,
                expected_home_margin, home_win_prob, market_provider,
                market_home_margin, market_spread, market_captured_at, edge, edge_pick,
                evaluation_mode, created_at, published_at, simulated_as_of_at,
                experiment_label, fit_id, input_hash, input_snapshot
            ) VALUES (
                '2099-01-01 00:00:00+00', %s, %s, %s, 2026, 1,
                'regular', 'Home', 'Away', false,
                %s, CASE WHEN %s = 1004 THEN 0.4 ELSE 0.7 END, 'fixture',
                3, -3, '2026-09-01 17:00:00+00', %s,
                CASE WHEN %s >= 3 THEN 'home' ELSE 'away' END,
                %s, %s, %s, %s, %s, %s, %s, %s::jsonb
            )
            RETURNING prediction_id, created_at, published_at,
                      created_at = statement_timestamp()
            """,
            (
                prediction_date,
                model_version,
                game_id,
                expected_margin,
                game_id,
                edge,
                edge,
                mode,
                created_at,
                published_at,
                simulated_as_of_at,
                experiment_label,
                fit_id,
                input_hash,
                input_snapshot,
            ),
        )
        return cur.fetchone()


def _accuracy_row(conn: psycopg2.extensions.connection) -> tuple:
    return _fetchone(
        conn,
        """
        SELECT n_games, margin_mae, brier, cfbd_brier, n_scored_win_prob
        FROM marts.prediction_accuracy
        WHERE model_version = %s AND season = 2026 AND edge_threshold = 0
        """,
        (MODEL,),
    )


def _edge_rows(conn: psycopg2.extensions.connection) -> tuple:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT game_id, expected_home_margin
            FROM marts.scored_matchup_edges
            WHERE model_version = %s
            ORDER BY game_id
            """,
            (MODEL,),
        )
        return tuple(cur.fetchall())


@pytest.fixture(scope="module")
def f04_db() -> F04Database:
    dsn = os.environ.get("F04_TEST_DB_URL")
    if not dsn:
        pytest.skip("set F04_TEST_DB_URL to an empty disposable local PostgreSQL database")

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cleanup_allowed = False
    try:
        _guard_disposable_database(conn, dsn)
        # From here onward every schema named below belongs to this fixture.
        cleanup_allowed = True
        _create_roles_and_minimal_schema(conn)
        _execute_file(conn, MIGRATIONS / "024_predictions_schema.sql")

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO core.games VALUES
                    (1001, 2026, '2026-09-01 20:00:00+00', true, 30, 20),
                    (1002, 2026, NULL, true, 24, 21),
                    (1003, 2026, '2026-09-08 20:00:00+00', false, NULL, NULL),
                    (1004, 2026, '2026-09-01 20:00:00+00', true, 17, 17);
                INSERT INTO metrics.pregame_win_probability VALUES (1001, 0.8), (1004, 0.3);

                INSERT INTO predictions.game_predictions (
                    computed_at, prediction_date, model_version, game_id, season, week,
                    season_type, home_team, away_team, neutral_site,
                    expected_home_margin, home_win_prob, market_spread, edge, edge_pick
                ) VALUES (
                    '2099-12-31 00:00:00+00', '2099-12-31', %s, 1003, 2026, 1,
                    'regular', 'Home', 'Away', false, 900, 0.99, -3, 897, 'home'
                ) RETURNING prediction_id
                """,
                (MODEL,),
            )
            legacy_prediction_id = cur.fetchone()[0]

        legacy_before = _fetchone(
            conn,
            """
            SELECT prediction_id, computed_at, prediction_date, expected_home_margin
            FROM predictions.game_predictions WHERE prediction_id = %s
            """,
            (legacy_prediction_id,),
        )

        # The actual upgrade is applied twice before any F04 fixture writes.
        _execute_file(conn, MIGRATIONS / "063_prediction_provenance.sql")
        _execute_file(conn, MIGRATIONS / "063_prediction_provenance.sql")

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO predictions.model_artifacts (fit_id, model_version, artifact)
                VALUES (%s, %s, '{"kind":"closed_form","implementation":"fixture-v1"}')
                """,
                (FIT_ID, MODEL),
            )

            # Controlled publication history is allowed only because the guard
            # above proved this is a dedicated empty loopback database.
            cur.execute(
                "ALTER TABLE predictions.game_predictions "
                "DISABLE TRIGGER stamp_prediction_provenance"
            )

        _insert_prediction(
            conn,
            game_id=1001,
            mode="published_forecast",
            expected_margin=3,
            published_at="2026-09-01 18:00:00+00",
        )
        _insert_prediction(
            conn,
            game_id=1001,
            mode="published_forecast",
            expected_margin=7,
            published_at="2026-09-01 19:00:00+00",
        )
        _insert_prediction(
            conn,
            game_id=1001,
            mode="published_forecast",
            expected_margin=80,
            published_at="2026-09-01 20:00:00+00",
        )
        _insert_prediction(
            conn,
            game_id=1001,
            mode="published_forecast",
            expected_margin=90,
            published_at="2026-09-01 21:00:00+00",
        )
        _insert_prediction(
            conn,
            game_id=1002,
            mode="published_forecast",
            expected_margin=20,
            published_at="2026-09-01 19:00:00+00",
        )
        _insert_prediction(
            conn,
            game_id=1003,
            mode="published_forecast",
            expected_margin=12,
            published_at="2026-09-01 19:00:00+00",
        )
        _insert_prediction(
            conn,
            game_id=1004,
            mode="published_forecast",
            expected_margin=5,
            published_at="2026-09-01 19:00:00+00",
        )

        with conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE predictions.game_predictions "
                "ENABLE TRIGGER stamp_prediction_provenance"
            )

        # First build creates the old dependent API views; the second build
        # exercises DROP ... CASCADE followed by explicit API restoration.
        for _ in range(2):
            _execute_file(conn, MARTS / "037_scored_matchup_edges.sql")
            _execute_file(conn, MARTS / "038_prediction_accuracy.sql")
            _execute_file(conn, API / "030_scored_matchup_edges.sql")
            _execute_file(conn, API / "031_prediction_accuracy.sql")
            _execute_file(conn, API / "032_game_predictions.sql")

        baseline_accuracy = _accuracy_row(conn)
        baseline_edges = _edge_rows(conn)

        # These rows have newer immutable IDs and intentionally extreme values.
        # Neither may alter prospective API/mart outputs.
        _insert_prediction(
            conn,
            game_id=1001,
            mode="walk_forward_reconstruction",
            expected_margin=700,
            simulated_as_of_at="2026-09-01 19:59:00+00",
        )
        _insert_prediction(
            conn,
            game_id=1001,
            mode="hindsight_experiment",
            expected_margin=800,
            simulated_as_of_at="2026-09-01 19:59:00+00",
            experiment_label="full-season hindsight fixture",
        )
        _insert_prediction(
            conn,
            game_id=1003,
            mode="walk_forward_reconstruction",
            expected_margin=700,
            simulated_as_of_at="2026-09-08 19:59:00+00",
        )
        _insert_prediction(
            conn,
            game_id=1003,
            mode="hindsight_experiment",
            expected_margin=800,
            simulated_as_of_at="2026-09-08 19:59:00+00",
            experiment_label="newer than publication",
        )

        with conn.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW marts.scored_matchup_edges")
            cur.execute("REFRESH MATERIALIZED VIEW marts.prediction_accuracy")

        legacy_after = _fetchone(
            conn,
            """
            SELECT prediction_id, computed_at, prediction_date, expected_home_margin
            FROM predictions.game_predictions WHERE prediction_id = %s
            """,
            (legacy_prediction_id,),
        )
        assert legacy_after == legacy_before

        db = F04Database(
            conn=conn,
            legacy_prediction_id=legacy_prediction_id,
            baseline_accuracy=baseline_accuracy,
            accuracy_after_history=_accuracy_row(conn),
            baseline_edges=baseline_edges,
            edges_after_history=_edge_rows(conn),
        )
        yield db
    finally:
        try:
            if cleanup_allowed:
                with conn.cursor() as cur:
                    cur.execute("RESET ROLE")
                    cur.execute(
                        "DROP SCHEMA IF EXISTS api, marts, predictions, metrics, core CASCADE"
                    )
        finally:
            conn.close()


def test_migration_is_idempotent_and_preserves_legacy_rows(f04_db: F04Database) -> None:
    conn = f04_db.conn
    row = _fetchone(
        conn,
        """
        SELECT evaluation_mode, created_at, published_at, simulated_as_of_at,
               experiment_label, fit_id, input_hash, input_snapshot
        FROM predictions.game_predictions WHERE prediction_id = %s
        """,
        (f04_db.legacy_prediction_id,),
    )
    assert row == ("legacy_unknown", None, None, None, None, None, None, None)
    assert _fetchone(
        conn,
        "SELECT to_regclass('predictions.game_predictions_daily_key')",
    ) == (None,)
    assert (
        _fetchone(
            conn,
            """
        SELECT count(*)
        FROM predictions.game_predictions
        WHERE game_id = 1001 AND model_version = %s AND prediction_date = '2026-09-01'
        """,
            (MODEL,),
        )[0]
        >= 6
    )


def test_provenance_trigger_stamps_database_write_time(f04_db: F04Database) -> None:
    conn = f04_db.conn
    before_insert = _fetchone(conn, "SELECT clock_timestamp()")[0]
    prediction_id, created_at, published_at, used_statement_timestamp = _insert_prediction(
        conn,
        game_id=2001,
        mode="published_forecast",
        expected_margin=1,
        published_at="2000-01-01 00:00:00+00",
    )
    after_insert = _fetchone(conn, "SELECT clock_timestamp()")[0]
    assert created_at == published_at
    assert used_statement_timestamp is True
    assert before_insert <= created_at <= after_insert

    before_reconstruction = _fetchone(conn, "SELECT clock_timestamp()")[0]
    (
        _,
        reconstructed_created_at,
        reconstructed_published_at,
        reconstruction_used_statement_timestamp,
    ) = _insert_prediction(
        conn,
        game_id=2002,
        mode="walk_forward_reconstruction",
        expected_margin=1,
        published_at="2000-01-01 00:00:00+00",
        simulated_as_of_at="2025-09-01 00:00:00+00",
    )
    after_reconstruction = _fetchone(conn, "SELECT clock_timestamp()")[0]
    assert reconstruction_used_statement_timestamp is True
    assert before_reconstruction <= reconstructed_created_at <= after_reconstruction
    assert reconstructed_published_at is None
    assert _fetchone(
        conn,
        """
        SELECT published_at = created_at
        FROM predictions.game_predictions
        WHERE prediction_id = %s
        """,
        (prediction_id,),
    ) == (True,)


def test_constraints_enforce_modes_hashes_and_artifact_identity(f04_db: F04Database) -> None:
    conn = f04_db.conn
    modes = _fetchone(
        conn,
        """
        SELECT array_agg(DISTINCT evaluation_mode ORDER BY evaluation_mode)
        FROM predictions.game_predictions
        """,
    )[0]
    assert modes == [
        "hindsight_experiment",
        "legacy_unknown",
        "published_forecast",
        "walk_forward_reconstruction",
    ]

    with pytest.raises(psycopg2.Error, match="explicit non-legacy provenance"):
        _insert_prediction(
            conn,
            game_id=2101,
            mode="legacy_unknown",
            expected_margin=1,
            fit_id=None,
            input_hash=None,
        )
    with pytest.raises(psycopg2.IntegrityError):
        _insert_prediction(
            conn,
            game_id=2102,
            mode="walk_forward_reconstruction",
            expected_margin=1,
            simulated_as_of_at=None,
        )
    with pytest.raises(psycopg2.IntegrityError):
        _insert_prediction(
            conn,
            game_id=2103,
            mode="hindsight_experiment",
            expected_margin=1,
            simulated_as_of_at="2025-09-01 00:00:00+00",
            experiment_label="   ",
        )
    with pytest.raises(psycopg2.IntegrityError):
        _insert_prediction(
            conn,
            game_id=2104,
            mode="published_forecast",
            expected_margin=1,
            published_at="2025-09-01 00:00:00+00",
            input_hash="not-a-sha256",
        )
    with pytest.raises(psycopg2.IntegrityError):
        _insert_prediction(
            conn,
            game_id=2105,
            mode="published_forecast",
            expected_margin=1,
            published_at="2025-09-01 00:00:00+00",
            fit_id=None,
        )
    with pytest.raises(psycopg2.IntegrityError):
        _insert_prediction(
            conn,
            game_id=2106,
            mode="published_forecast",
            expected_margin=1,
            published_at="2025-09-01 00:00:00+00",
            input_hash=None,
        )
    with pytest.raises(psycopg2.IntegrityError):
        _insert_prediction(
            conn,
            game_id=2107,
            mode="published_forecast",
            expected_margin=1,
            published_at="2025-09-01 00:00:00+00",
            input_snapshot=None,
        )
    with pytest.raises(psycopg2.IntegrityError):
        _insert_prediction(
            conn,
            game_id=2108,
            mode="published_forecast",
            expected_margin=1,
            published_at="2025-09-01 00:00:00+00",
            model_version="wrong-model",
        )
    with pytest.raises(psycopg2.IntegrityError):
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO predictions.model_artifacts (fit_id, model_version, artifact)
                VALUES ('short', 'bad-artifact', '{}')
                """
            )

    assert re.fullmatch(
        r"[0-9a-f]{64}",
        _fetchone(conn, "SELECT fit_id FROM predictions.model_artifacts")[0],
    )


def test_prediction_and_artifact_snapshots_are_immutable(f04_db: F04Database) -> None:
    conn = f04_db.conn
    with pytest.raises(psycopg2.Error, match="immutable"):
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE predictions.game_predictions SET expected_home_margin = 0 "
                "WHERE prediction_id = %s",
                (f04_db.legacy_prediction_id,),
            )
    with pytest.raises(psycopg2.Error, match="immutable"):
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE predictions.model_artifacts SET artifact = '{}' WHERE fit_id = %s",
                (FIT_ID,),
            )


def test_api_and_marts_keep_historical_modes_out_of_prospective_outputs(
    f04_db: F04Database,
) -> None:
    conn = f04_db.conn
    assert f04_db.accuracy_after_history == f04_db.baseline_accuracy
    assert f04_db.edges_after_history == f04_db.baseline_edges
    assert f04_db.accuracy_after_history == (
        2,
        Decimal("4.0000"),
        Decimal("0.050000"),
        Decimal("0.040000"),
        2,
    )
    assert f04_db.edges_after_history == ((1003, Decimal("12.00")),)

    # The general API shows the newest publication even when it was post-kickoff;
    # the accuracy mart independently selects the newest strictly pre-kickoff row.
    assert _fetchone(
        conn,
        """
        SELECT expected_home_margin, evaluation_mode
        FROM api.game_predictions WHERE game_id = 1001 AND model_version = %s
        """,
        (MODEL,),
    ) == (Decimal("90.00"), "published_forecast")
    assert _fetchone(
        conn,
        """
        SELECT count(DISTINCT evaluation_mode), count(*)
        FROM api.prediction_history
        WHERE model_version = %s AND game_id BETWEEN 1001 AND 1004
        """,
        (MODEL,),
    ) == (4, 12)
    assert _fetchone(
        conn,
        """
        SELECT to_regclass('api.scored_matchup_edges'),
               to_regclass('api.prediction_accuracy'),
               to_regclass('api.game_predictions'),
               to_regclass('api.prediction_history')
        """,
    ) == (
        "api.scored_matchup_edges",
        "api.prediction_accuracy",
        "api.game_predictions",
        "api.prediction_history",
    )


@pytest.mark.parametrize("role", ["anon", "authenticated", "analyst_ro"])
def test_api_roles_can_read_but_cannot_mutate(role: str, f04_db: F04Database) -> None:
    conn = f04_db.conn
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET ROLE {role}")
            cur.execute("SELECT count(*) FROM api.game_predictions")
            assert cur.fetchone()[0] >= 1
            cur.execute("SELECT count(*) FROM api.prediction_history")
            assert cur.fetchone()[0] >= 1
            for query in [
                "SELECT input_snapshot FROM predictions.game_predictions "
                "WHERE input_snapshot IS NOT NULL LIMIT 1",
                "SELECT artifact FROM predictions.model_artifacts LIMIT 1",
            ]:
                if role == "analyst_ro":
                    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                        cur.execute(query)
                else:
                    # The predictions read contract deliberately exposes these
                    # public-source snapshots to anon/authenticated, not analyst_ro.
                    cur.execute(query)
                    assert cur.fetchone()[0] is not None
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cur.execute(
                    "UPDATE api.prediction_history SET expected_home_margin = 0 "
                    "WHERE prediction_id = %s",
                    (f04_db.legacy_prediction_id,),
                )
    finally:
        with conn.cursor() as cur:
            cur.execute("RESET ROLE")

    if role == "analyst_ro":
        with conn.cursor() as cur:
            cur.execute("SELECT has_schema_privilege('analyst_ro', 'predictions', 'USAGE')")
            assert cur.fetchone()[0] is False


def test_residual_sigma_uses_only_strictly_prekickoff_publications(
    f04_db: F04Database,
) -> None:
    conn = f04_db.conn
    with conn.cursor() as cur:
        cur.execute(simulate_season.RESIDUAL_SIGMA_QUERY, {"model": MODEL})
        sigma, count = cur.fetchone()
    assert count == 2
    assert sigma == pytest.approx(4.0)

    with pytest.raises(RuntimeError, match=r"Only 2 completed game\(s\).+need >= 100"):
        simulate_season.fetch_sigma(conn, MODEL)


def test_python_writers_round_trip_same_day_snapshots_and_artifacts(f04_db):
    """Execute shared writer SQL, JSON adaptation and artifact deduplication."""
    from scripts import compute_predictions as writer
    from scripts.prediction_provenance import (
        attach_prediction_provenance,
        canonical_hash,
        identify_model_artifact,
    )

    conn = f04_db.conn
    model = "writer_round_trip"
    fit_id, artifact = identify_model_artifact(model, {"coefficient": 1.25})
    kickoff = _fetchone(conn, "SELECT statement_timestamp() + interval '1 hour'")[0]
    payload = dict.fromkeys(writer._ROW_COLUMNS)
    payload.update(
        model_version=model, game_id=9001, home_team="A", away_team="B", expected_home_margin=7.25
    )
    source = {"rating": 1700.125, "kickoff": kickoff}
    published = attach_prediction_provenance(
        payload,
        evaluation_mode="published_forecast",
        fit_id=fit_id,
        model_artifact=artifact,
        input_snapshot=source,
    )
    conn.autocommit = False
    try:
        transaction_start = _fetchone(conn, "SELECT transaction_timestamp(), pg_sleep(0.01)")[0]
        writer.write_upcoming(conn, [published])
        first = _fetchone(
            conn,
            """
            SELECT prediction_id, created_at, published_at, prediction_date, input_snapshot
            FROM predictions.game_predictions WHERE model_version = %s
        """,
            (model,),
        )
        assert first[1] == first[2] and first[1] > transaction_start
        assert canonical_hash(first[4]) == published["input_hash"]
        reconstructed = attach_prediction_provenance(
            {**payload, "prediction_date": first[3]},
            evaluation_mode="walk_forward_reconstruction",
            fit_id=fit_id,
            model_artifact=artifact,
            input_snapshot=source,
            simulated_as_of_at=kickoff,
        )
        writer.write_backfill_season(conn, [reconstructed])
        writer.write_upcoming(conn, [published])
        assert _fetchone(
            conn,
            """
            SELECT COUNT(*), COUNT(DISTINCT prediction_id), COUNT(DISTINCT prediction_date)
            FROM predictions.game_predictions WHERE model_version = %s
        """,
            (model,),
        ) == (3, 3, 1)
        assert _fetchone(
            conn, "SELECT COUNT(*) FROM predictions.model_artifacts WHERE fit_id=%s", (fit_id,)
        ) == (1,)
        assert _fetchone(
            conn,
            "SELECT published_at FROM predictions.game_predictions WHERE prediction_id=%s",
            (first[0],),
        ) == (first[2],)
    finally:
        conn.rollback()
        conn.autocommit = True


@pytest.mark.parametrize(
    "scenario, failing_model",
    [
        ("complete", None),
        ("sparse_elo", "elo_v1"),
        ("sparse_blend", "elo_epa_blend_v1"),
        ("sparse_fitted", "fitted_v1"),
        ("missing_elo", "elo_v1"),
        ("unknown_kickoff", "elo_epa_blend_v1"),
        ("at_kickoff", "elo_epa_blend_v1"),
        ("after_kickoff", "elo_epa_blend_v1"),
        ("missing_margin", "elo_v1"),
        ("duplicate_sparse_elo", "elo_v1"),
        ("sparse_next_season", "elo_v1"),
    ],
)
def test_rollout_coverage_gates_each_model_and_known_kickoff(f04_db, scenario, failing_model):
    """Execute the operational SQL gate against controlled temporary relations."""
    from psycopg2.extras import execute_values

    source = (PROJECT_ROOT / "docs/plans/2026-09-07-f04-post-scoring.sql").read_text()
    block = source.split("-- F04_COVERAGE_BEGIN:", 1)[1].split("-- F04_COVERAGE_END", 1)[0]
    # Drop the marker's prose line; keep the actual deployed FOR/query/assertion body.
    block = block.split("\n", 1)[1]
    block = block.replace("core.games", "pg_temp.f04_rollout_games").replace(
        "predictions.game_predictions", "pg_temp.f04_rollout_predictions"
    )
    conn = f04_db.conn
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE f04_rollout_games (
                    id bigint,season integer,start_date timestamptz,completed boolean
                );
                CREATE TEMP TABLE f04_rollout_predictions (
                    game_id bigint,model_version text,evaluation_mode text,
                    published_at timestamptz,expected_home_margin numeric
                );
            """)
            games = [
                (
                    i,
                    2026,
                    None if scenario == "unknown_kickoff" and i <= 3 else "2026-09-02 12:00:00+00",
                    False,
                )
                for i in range(1, 21)
            ]
            if scenario == "sparse_next_season":
                games += [
                    (21, 2027, "2027-09-02 12:00:00+00", False),
                    (22, 2027, "2027-09-02 12:00:00+00", False),
                ]
            execute_values(cur, "INSERT INTO f04_rollout_games VALUES %s", games)
            predictions = []
            sparse = {
                "sparse_elo": "elo_v1",
                "sparse_blend": "elo_epa_blend_v1",
                "sparse_fitted": "fitted_v1",
                "duplicate_sparse_elo": "elo_v1",
            }
            for model in ["elo_v1", "elo_epa_blend_v1", "fitted_v1"]:
                for game_id, season, _, _ in games:
                    if (
                        model == sparse.get(scenario)
                        and game_id > 1
                        or scenario == "missing_elo"
                        and model == "elo_v1"
                        or scenario == "sparse_next_season"
                        and model == "elo_v1"
                        and game_id == 22
                    ):
                        continue
                    publication = f"{season}-09-01 12:00:00+00"
                    if game_id <= 3 and scenario in {"at_kickoff", "after_kickoff"}:
                        publication = (
                            "2026-09-02 12:00:00+00"
                            if scenario == "at_kickoff"
                            else ("2026-09-02 12:00:01+00")
                        )
                    margin = (
                        None
                        if scenario == "missing_margin" and model == "elo_v1" and (game_id <= 3)
                        else 7
                    )
                    row = (game_id, model, "published_forecast", publication, margin)
                    predictions.extend(
                        [row]
                        * (100 if scenario == "duplicate_sparse_elo" and model == "elo_v1" else 1)
                    )
            execute_values(cur, "INSERT INTO f04_rollout_predictions VALUES %s", predictions)
            statement = "DO $test$ DECLARE r record; BEGIN\n" + block + "\nEND $test$;"
            if failing_model is None:
                cur.execute(statement)
            else:
                with pytest.raises(
                    psycopg2.Error, match="Insufficient eligible model coverage"
                ) as err:
                    cur.execute(statement)
                assert failing_model in str(err.value)
    finally:
        with conn.cursor() as cur:
            cur.execute(
                "DROP TABLE IF EXISTS pg_temp.f04_rollout_predictions, pg_temp.f04_rollout_games"
            )
