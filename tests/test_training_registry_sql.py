"""F05 SQL behavior on an explicitly opted-in empty loopback database only."""

import os
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
import pytest

from scripts import training_registry as registry

MIGRATION = (
    Path(__file__).resolve().parents[1] / "src/schemas/migrations/064_immutable_training_fits.sql"
)


def query(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall() if cur.description else None


@pytest.fixture(scope="module")
def registry_db():
    dsn = os.environ.get("F05_TEST_DB_URL")
    if not dsn:
        pytest.skip("set F05_TEST_DB_URL to an empty disposable loopback PostgreSQL database")
    parsed = urlparse(dsn)
    if parsed.scheme not in {"postgres", "postgresql"} or parsed.hostname not in {
        "localhost",
        "127.0.0.1",
        "::1",
    }:
        pytest.fail("F05_TEST_DB_URL must be a loopback PostgreSQL URL")
    conn = psycopg2.connect(dsn)
    try:
        database, relations, schemas = query(
            conn,
            """
            SELECT current_database(),
                (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                 WHERE n.nspname NOT IN ('pg_catalog','information_schema')
                   AND n.nspname !~ '^pg_toast' AND c.relkind IN ('r','p','v','m','S','f')),
                (SELECT count(*) FROM pg_namespace WHERE nspname='features')
        """,
        )[0]
        if relations or schemas or database != parsed.path.removeprefix("/"):
            pytest.fail(
                "F05 fixture requires an empty database with no preexisting features schema"
            )
        # All fixture DDL/data is transactional. Rollback removes only our work;
        # no DROP cleanup can run against a database that failed the guard.
        query(
            conn,
            """
            DO $$ BEGIN
                IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='anon') THEN
                    CREATE ROLE anon;
                END IF;
                IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='authenticated') THEN
                    CREATE ROLE authenticated;
                END IF;
                IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='analyst_ro') THEN
                    CREATE ROLE analyst_ro;
                END IF;
                IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='f05_writer') THEN
                    CREATE ROLE f05_writer;
                END IF;
            END $$;
            CREATE SCHEMA features;
            CREATE TABLE features.model_metadata (
                model_version text, train_through_season bigint, ridge_alpha numeric(8,3),
                winprob_ridge_alpha numeric(8,3), platt_a numeric(12,6), platt_b numeric(12,6),
                train_seasons bigint[], n_train_games bigint, feature_means jsonb,
                feature_diff_means jsonb, feature_diff_stds jsonb, fit_at timestamptz DEFAULT now()
            );
            CREATE TABLE features.model_coefficients (
                model_version text, train_through_season bigint, model_component text,
                feature_order bigint, feature_name text, coefficient numeric(12,6)
            );
        """,
        )
        query(conn, MIGRATION.read_text())
        query(conn, MIGRATION.read_text())
        query(
            conn,
            """
            GRANT USAGE ON SCHEMA features TO f05_writer;
            GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
                ON ALL TABLES IN SCHEMA features TO f05_writer;
            GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA features TO f05_writer;
        """,
        )
        yield conn
    finally:
        conn.rollback()
        conn.close()


@pytest.fixture
def db(registry_db):
    query(registry_db, "SAVEPOINT test_case")
    try:
        yield registry_db
    finally:
        query(registry_db, "ROLLBACK TO SAVEPOINT test_case")


def params(value=1.0):
    return {
        "feature_names": ["intercept"],
        "feature_means": {},
        "diff_means": {},
        "diff_stds": {},
        "beta_margin": [value],
        "beta_winprob": [0.2],
        "platt_a": 1.0,
        "platt_b": 0.0,
    }


def add_fit(db, value=1.0, season=2025, model="fitted_v1"):
    return registry.insert_fit(
        db, model, season, {"lineage": "test", "input_hash": "a" * 64}, params(value)
    )


def test_candidates_are_immutable_and_selection_is_explicit(db):
    first = add_fit(db)
    assert add_fit(db) == first
    second = add_fit(db, 2.0)
    assert second != first
    assert registry.selected_fits(db, "fitted_v1") == []
    registry.promote_fit(db, first, "initial reviewed selection")
    assert registry.load_selected_fit(db, "fitted_v1", 2025)["training_fit_id"] == first
    registry.promote_fit(db, second, "reviewed replacement")
    assert registry.load_selected_fit(db, "fitted_v1", 2025)["training_fit_id"] == second
    registry.promote_fit(db, first, "rollback")
    assert registry.load_selected_fit(db, "fitted_v1", 2025)["training_fit_id"] == first
    assert query(db, "SELECT count(*) FROM features.training_fits")[0][0] == 2
    assert query(
        db,
        "SELECT parameters->'beta_margin' FROM features.training_fits WHERE training_fit_id=%s",
        (first,),
    )[0][0] == [1.0]


@pytest.mark.parametrize(
    "operation",
    [
        "UPDATE features.training_fits SET model_version='corrupted'",
        "DELETE FROM features.training_fits",
        "TRUNCATE features.training_fits CASCADE",
    ],
)
def test_privileged_writer_cannot_mutate_fits(db, operation):
    add_fit(db)
    query(db, "SET LOCAL ROLE f05_writer")
    with pytest.raises(psycopg2.Error):
        query(db, operation)


@pytest.mark.parametrize("role", ["anon", "authenticated"])
def test_consumer_read_access(db, role):
    add_fit(db)
    query(db, f"SET LOCAL ROLE {role}")
    assert query(db, "SELECT count(*) FROM features.training_fits")[0][0] == 1
    assert query(db, "SELECT count(*) FROM features.model_deployments")[0][0] == 0


@pytest.mark.parametrize("role", ["anon", "authenticated", "analyst_ro"])
def test_consumer_write_denied(db, role):
    first = add_fit(db)
    query(db, f"SET LOCAL ROLE {role}")
    with pytest.raises(psycopg2.Error):
        registry.promote_fit(db, first, "unauthorized")


def test_analyst_has_no_raw_training_access(db):
    query(db, "SET LOCAL ROLE analyst_ro")
    with pytest.raises(psycopg2.Error):
        query(db, "SELECT * FROM features.training_fits")


def test_deployment_cannot_mismatch_fit_model_or_season(db):
    first = add_fit(db)
    registry.promote_fit(db, first, "initial")
    with pytest.raises(psycopg2.IntegrityError):
        query(db, "UPDATE features.model_deployments SET train_through_season=2024")


def test_promotion_and_fit_rollback_together(db):
    query(db, "SAVEPOINT atomic_write")
    first = add_fit(db)
    registry.promote_fit(db, first, "candidate promotion")
    query(db, "ROLLBACK TO SAVEPOINT atomic_write")
    assert query(db, "SELECT count(*) FROM features.training_fits")[0][0] == 0
    assert registry.selected_fits(db, "fitted_v1") == []


def test_promotion_history_retains_replacement_and_rollback(db):
    first, second = add_fit(db), add_fit(db, 2.0)
    registry.promote_fit(db, first, "initial")
    registry.promote_fit(db, second, "replacement")
    registry.promote_fit(db, first, "rollback")
    assert query(
        db,
        """
        SELECT previous_training_fit_id, training_fit_id, reason
        FROM features.model_deployment_history ORDER BY deployment_history_id
    """,
    ) == [(None, first, "initial"), (first, second, "replacement"), (second, first, "rollback")]


@pytest.mark.parametrize(
    "operation",
    [
        "UPDATE features.model_deployment_history SET reason='rewritten'",
        "DELETE FROM features.model_deployment_history",
        "TRUNCATE features.model_deployment_history",
    ],
)
def test_history_cannot_be_rewritten(db, operation):
    registry.promote_fit(db, add_fit(db), "initial")
    with pytest.raises(psycopg2.Error):
        query(db, operation)


def test_legacy_import_preserves_values_and_does_not_promote(db):
    query(
        db,
        """
        INSERT INTO features.model_metadata VALUES
        ('fitted_v1',2025,7,0.001,1.234567,-0.123456,ARRAY[2015,2016,2017],321,
         '{"elo_pregame":1500}', '{}','{}','2026-01-01');
        INSERT INTO features.model_coefficients VALUES
        ('fitted_v1',2025,'margin',0,'intercept',12.123456),
        ('fitted_v1',2025,'winprob',0,'intercept',0.123456);
    """,
    )
    before = query(db, "SELECT row_to_json(m) FROM features.model_metadata m")
    fit_ids = registry.import_legacy_fits(db, "fitted_v1")
    assert registry.import_legacy_fits(db, "fitted_v1") == fit_ids
    assert registry.selected_fits(db, "fitted_v1") == []
    manifest, parameters = query(db, "SELECT manifest, parameters FROM features.training_fits")[0]
    assert manifest["lineage"] == "legacy_unknown"
    assert manifest["training_data"]["digest"] is None
    assert float(parameters["platt_a"]) == 1.234567
    assert float(parameters["beta_margin"][0]) == 12.123456
    assert query(db, "SELECT row_to_json(m) FROM features.model_metadata m") == before


def test_scoring_uses_selected_immutable_values_and_records_upstream_id(db):
    from scripts import score_fitted as scoring
    from scripts.train_model import FEATURE_NAMES, TEAM_WEEK_SOURCE_COLUMNS

    parameters = {
        "feature_names": FEATURE_NAMES,
        "feature_means": dict.fromkeys(TEAM_WEEK_SOURCE_COLUMNS, 0.0),
        "diff_means": dict.fromkeys(FEATURE_NAMES[2:], 0.0),
        "diff_stds": dict.fromkeys(FEATURE_NAMES[2:], 1.0),
        "beta_margin": [1.234567] + [0.0] * (len(FEATURE_NAMES) - 1),
        "beta_winprob": [0.25] + [0.0] * (len(FEATURE_NAMES) - 1),
        "platt_a": 1.0,
        "platt_b": 0.0,
    }
    first = registry.insert_fit(db, "fitted_v1", 2025, {"lineage": "test"}, parameters)
    registry.promote_fit(db, first, "baseline")
    frozen = scoring.load_fit(db, 2025)
    artifact_id, artifact = scoring.fitted_model_artifact(frozen)
    second = registry.insert_fit(
        db,
        "fitted_v1",
        2025,
        {"lineage": "test"},
        {**parameters, "beta_margin": [9.0] + parameters["beta_margin"][1:]},
    )
    assert scoring.load_fit(db, 2025)["training_fit_id"] == first
    registry.promote_fit(db, second, "replacement")
    assert scoring.load_fit(db, 2025)["training_fit_id"] == second
    game = {
        "season": 2026,
        "neutral_site": False,
        "home_tw": parameters["feature_means"],
        "away_tw": parameters["feature_means"],
    }
    assert scoring.score_game(game, frozen)[0] == 1.234567
    assert artifact["training_fit_id"] == first
    assert scoring.fitted_model_artifact(frozen)[0] == artifact_id


def test_privileged_writer_promotion_is_journaled(db):
    first = add_fit(db)
    query(db, "SET LOCAL ROLE f05_writer")
    registry.promote_fit(db, first, "writer promotion")
    assert query(db, "SELECT reason FROM features.model_deployment_history") == [
        ("writer promotion",)
    ]


def test_pointer_deletion_is_journaled(db):
    first = add_fit(db)
    registry.promote_fit(db, first, "initial")
    query(db, "DELETE FROM features.model_deployments")
    assert query(
        db,
        """
        SELECT action, previous_training_fit_id, training_fit_id
        FROM features.model_deployment_history ORDER BY deployment_history_id
    """,
    ) == [("promote", None, first), ("delete", first, None)]


def test_pointer_truncate_cannot_bypass_history(db):
    registry.promote_fit(db, add_fit(db), "initial")
    with pytest.raises(psycopg2.Error):
        query(db, "TRUNCATE features.model_deployments")


@pytest.mark.parametrize("value", [1.2345665, -1.2345665, 0.9999995, -0.9999995, 12.1234567])
def test_parameter_quantization_matches_executed_postgres_numeric(db, value):
    from scripts.train_model import _numeric6

    stored = query(db, "SELECT %s::NUMERIC(12,6)", (value,))[0][0]
    assert _numeric6(value) == float(stored)
