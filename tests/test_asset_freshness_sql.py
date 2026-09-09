"""Executed query-time receipt freshness and public compatibility on disposable PG17."""

from __future__ import annotations

import uuid
from pathlib import Path

import psycopg2
import pytest
from psycopg2 import sql

from tests.test_asset_publication_sql import (
    publication_args,
    publish,
    publisher_query,
    start_run,
    state,
)
from tests.test_asset_publication_sql import publication_db as _publication_db  # noqa: F401
from tests.test_warehouse_migrations_sql import query
from tests.test_warehouse_migrations_sql import warehouse_db as _warehouse_db  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "src/schemas/migrations/069_receipt_backed_freshness.sql"
SOURCE = "analytics.house_elo_game"
MART = "marts.house_elo_game"


@pytest.fixture
def freshness_db(request):
    conn, target = request.getfixturevalue("_publication_db")
    query(conn, MIGRATION.read_text())
    return conn, target


def rows(conn, role="anon"):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
        cur.execute("SELECT to_jsonb(f) FROM public.get_asset_freshness() f")
        result = {row[0]["asset_key"]: row[0] for row in cur.fetchall()}
    conn.commit()
    return result


def record_failure(conn, outcome="failed"):
    publisher_query(
        conn,
        "SELECT * FROM warehouse_publication.record_house_elo_failure(%s,%s,%s,%s,%s)",
        (str(start_run(conn)), str(uuid.uuid4()), str(uuid.uuid4()), outcome, "publication_failed"),
    )


def denied(conn, role, statement, error=psycopg2.errors.InsufficientPrivilege):
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
            with pytest.raises(error):
                cur.execute(statement)
    finally:
        conn.rollback()


def test_unrecorded_is_unknown_not_empty_or_fresh(freshness_db):
    conn, _ = freshness_db
    for role in ("anon", "authenticated"):
        result = rows(conn, role)
        assert set(result) == {SOURCE, MART}
        for row in result.values():
            assert row["coverage_key"] == "source-wide"
            assert row["publication_state"] == "unrecorded"
            assert row["generation_id"] is None
            assert row["current_outcome"] is None
            assert row["published_at"] is None
            assert row["age_seconds"] is None
            assert row["is_stale"] is None
            assert row["expected_refresh_interval"] is None
            assert row["recorded_inputs_current"] is None
            assert row["input_closure_current"] is None
            assert row["unversioned_input_assets"] == ["core.games"]


def test_complete_publication_has_exact_input_evidence_without_provider_claim(freshness_db):
    conn, _ = freshness_db
    args = publication_args(conn)
    publish(conn, args)
    result = rows(conn)
    source, mart = result[SOURCE], result[MART]
    assert source["generation_id"] == args[1]
    assert mart["generation_id"] == args[2]
    for row in result.values():
        assert row["publication_state"] == "current"
        assert row["current_outcome"] == "succeeded"
        assert row["coverage"]["complete"] is True
        assert row["row_delta"]["published_rows"] == 1
        assert row["source_watermark"] == args[5]
        assert row["age_seconds"] >= 0
        assert row["is_stale"] is None
        assert row["input_closure_current"] is None
        assert "input_observations" not in row
        assert "operation_run_id" not in row
        assert "error_summary" not in row
    assert source["recorded_inputs_current"] is None
    assert source["required_input_generations"] == []
    assert mart["recorded_inputs_current"] is True
    assert mart["required_input_generations"] == [
        {
            "asset_key": SOURCE,
            "coverage_key": "source-wide",
            "generation_id": args[1],
            "current_generation_id": args[1],
            "is_current": True,
        }
    ]


def test_age_advances_inside_transaction_without_mart_refresh(freshness_db):
    conn, _ = freshness_db
    publish(conn, publication_args(conn))
    query(conn, "UPDATE meta.asset_freshness_policies SET expected_refresh_interval='0.3 seconds'")
    with conn.cursor() as cur:
        cur.execute("SET LOCAL ROLE anon")
        cur.execute(
            "SELECT generation_id,age_seconds FROM public.get_asset_freshness() ORDER BY asset_key"
        )
        before = cur.fetchall()
        cur.execute("SELECT pg_sleep(0.35)")
        cur.execute(
            "SELECT generation_id,age_seconds,is_stale "
            "FROM public.get_asset_freshness() ORDER BY asset_key"
        )
        after = cur.fetchall()
    conn.commit()
    assert [r[0] for r in before] == [r[0] for r in after]
    for old, new in zip(before, after, strict=True):
        assert new[1] - old[1] >= 0.3
        assert new[2] is True
    query(conn, "ANALYZE analytics.house_elo_game")
    assert all(r["is_stale"] is True for r in rows(conn).values())
    query(conn, "UPDATE meta.asset_freshness_policies SET expected_refresh_interval='100 years'")
    assert all(r["is_stale"] is False for r in rows(conn).values())


@pytest.mark.parametrize("outcome", ["failed", "partial", "deferred", "blocked"])
def test_failed_attempt_does_not_replace_current_publication(freshness_db, outcome):
    conn, _ = freshness_db
    args = publication_args(conn)
    publish(conn, args)
    record_failure(conn, outcome)
    for key, generation in ((SOURCE, args[1]), (MART, args[2])):
        row = rows(conn)[key]
        assert row["generation_id"] == generation
        assert row["publication_state"] == "current"
        assert row["current_outcome"] == "succeeded"
        assert row["latest_outcome"] == outcome
        assert row["last_failure_outcome"] == outcome
        assert row["last_failure_category"] == "publication_failed"
        assert row["last_failure_at"] is not None
    publish(conn, publication_args(conn, expected=state(conn)))
    assert all(row["latest_outcome"] == "succeeded" for row in rows(conn).values())
    assert all(row["last_failure_outcome"] == outcome for row in rows(conn).values())


def test_failure_without_publication_is_not_expected_no_data(freshness_db):
    conn, _ = freshness_db
    record_failure(conn)
    for row in rows(conn).values():
        assert row["publication_state"] == "unpublished"
        assert row["generation_id"] is None
        assert row["current_outcome"] is None
        assert row["latest_outcome"] == "failed"
        assert row["is_stale"] is None


def test_empty_eligible_completed_set_is_evaluated_no_data(freshness_db):
    conn, _ = freshness_db
    query(conn, "UPDATE core.games SET completed=false,home_points=NULL,away_points=NULL")
    args = list(publication_args(conn))
    args[6], args[7] = "[]", "[]"
    publish(conn, tuple(args))
    for row in rows(conn).values():
        assert row["publication_state"] == "current"
        assert row["current_outcome"] == "expected_no_data"
        assert row["row_delta"]["published_rows"] == 0
        assert row["coverage"]["complete"] is True
        assert row["input_closure_current"] is None


def test_legacy_write_invalidates_and_history_cannot_resurrect_pointer(freshness_db):
    conn, _ = freshness_db
    publish(conn, publication_args(conn))
    query(conn, "UPDATE analytics.house_elo_game SET home_postgame_elo=1519")
    for row in rows(conn).values():
        assert row["publication_state"] == "unpublished"
        assert row["latest_outcome"] == "succeeded"
        assert row["generation_id"] is None
        assert row["age_seconds"] is None
        assert row["recorded_inputs_current"] is None


@pytest.mark.parametrize("change", ["absent", "mismatch", "missing_edge"])
def test_mart_detects_missing_or_changed_input_pointer(freshness_db, change):
    conn, _ = freshness_db
    first = publication_args(conn)
    publish(conn, first)
    if change == "mismatch":
        publish(conn, publication_args(conn, expected=state(conn)))
        query(
            conn,
            "UPDATE meta.asset_current_generations SET generation_id=%s WHERE asset_key=%s",
            (first[1], SOURCE),
        )
    elif change == "missing_edge":
        # Model a defective owner-written receipt: its required edge was never recorded.
        incomplete_generation = str(uuid.uuid4())
        query(
            conn,
            """
            INSERT INTO meta.asset_receipts (
                generation_id,operation_run_id,asset_key,coverage_key,outcome,coverage,
                source_watermark,input_observations,row_delta,request_digest,published_at
            ) SELECT %s,operation_run_id,asset_key,coverage_key,outcome,coverage,
                source_watermark,input_observations,row_delta,request_digest,published_at
            FROM meta.asset_receipts WHERE generation_id=%s
            """,
            (incomplete_generation, first[2]),
        )
        query(
            conn,
            "UPDATE meta.asset_current_generations SET generation_id=%s WHERE asset_key=%s",
            (incomplete_generation, MART),
        )
    else:
        query(conn, "DELETE FROM meta.asset_current_generations WHERE asset_key=%s", (SOURCE,))
    mart = rows(conn)[MART]
    assert mart["publication_state"] == "current"
    assert mart["recorded_inputs_current"] is False
    assert mart["input_closure_current"] is False
    if change != "missing_edge":
        assert mart["required_input_generations"][0]["is_current"] is False


def test_policy_reapply_and_access_boundary(freshness_db):
    conn, _ = freshness_db
    query(conn, "UPDATE meta.asset_freshness_policies SET expected_refresh_interval='2 days'")
    query(conn, MIGRATION.read_text())
    assert all(row["expected_refresh_interval"] == "2 days" for row in rows(conn).values())
    for role in (
        "anon",
        "authenticated",
        "analyst_ro",
        "publication_bystander",
        "warehouse_publisher",
    ):
        for table in (
            "asset_receipts",
            "asset_current_generations",
            "asset_receipt_inputs",
            "asset_freshness_policies",
            "operation_runs",
        ):
            denied(conn, role, f"SELECT * FROM meta.{table}")
        denied(
            conn,
            role,
            "UPDATE meta.asset_freshness_policies SET expected_refresh_interval='1 hour'",
        )
        assert query(
            conn,
            "SELECT has_table_privilege(%s,'marts.asset_receipt_freshness',"
            "'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')",
            (role,),
        ) == [(False,)]
        denied(
            conn,
            role,
            "DELETE FROM marts.asset_receipt_freshness",
            (psycopg2.errors.InsufficientPrivilege, psycopg2.errors.ObjectNotInPrerequisiteState),
        )
    for role in ("analyst_ro", "publication_bystander", "warehouse_publisher"):
        denied(conn, role, "SELECT * FROM public.get_asset_freshness()")
    with pytest.raises(psycopg2.errors.CheckViolation):
        query(
            conn, "UPDATE meta.asset_freshness_policies SET expected_refresh_interval='0 seconds'"
        )
    conn.rollback()
    query(conn, "DELETE FROM meta.asset_freshness_policies")
    assert set(rows(conn)) == {SOURCE, MART}


def test_migration_preserves_legacy_rpc_definition_results_and_grants(request):
    conn, _ = request.getfixturevalue("_publication_db")
    query(conn, (ROOT / "src/schemas/marts/028_data_freshness.sql").read_text())
    query(conn, (ROOT / "src/schemas/public/011_data_freshness_function.sql").read_text())
    query(conn, "GRANT USAGE ON SCHEMA marts TO anon,authenticated")

    def legacy():
        with conn.cursor() as cur:
            cur.execute("SET LOCAL ROLE anon")
            cur.execute("SELECT * FROM public.get_data_freshness()")
            result = cur.fetchall()
            columns = [(col.name, col.type_code) for col in cur.description]
        conn.commit()
        definition = query(
            conn, "SELECT pg_get_functiondef('public.get_data_freshness()'::regprocedure)"
        )
        return result, columns, definition

    before = legacy()
    query(conn, MIGRATION.read_text())
    query(conn, MIGRATION.read_text())
    assert legacy() == before
    assert len(before[0]) == 24
    assert len(before[1]) == 6


def test_verifier_reads_real_rpc_and_does_not_claim_upstream_freshness(freshness_db, capsys):
    from scripts.verify_load import Report, check_receipt_freshness

    conn, _ = freshness_db
    publish(conn, publication_args(conn))
    report = Report()
    with conn.cursor() as cur:
        check_receipt_freshness(cur, in_season=True, strict=False, report=report)
    conn.rollback()
    output = capsys.readouterr().out
    assert report.failures == 0
    assert SOURCE in output and MART in output
    assert "[WARN]" in output
    assert "[PASS]" not in output
    query(conn, "DELETE FROM meta.asset_current_generations WHERE asset_key=%s", (SOURCE,))
    with conn.cursor() as cur:
        check_receipt_freshness(cur, in_season=True, strict=False, report=report)
    conn.rollback()
    assert report.failures >= 1
    assert "[FAIL]" in capsys.readouterr().out


def test_untrusted_precreated_policy_rejected_before_trigger_can_fire(request):
    conn, _ = request.getfixturevalue("_publication_db")
    # Sequence increments survive rollback, so an aborted INSERT cannot conceal
    # that the attacker-controlled trigger executed under the migration identity.
    query(conn, "CREATE SEQUENCE public.freshness_attack_marker")
    query(conn, "GRANT USAGE ON SEQUENCE public.freshness_attack_marker TO publication_bystander")
    query(conn, "GRANT CREATE ON SCHEMA meta TO publication_bystander")
    query(
        conn,
        """
        SET ROLE publication_bystander;
        CREATE TABLE meta.asset_freshness_policies (
            asset_key text PRIMARY KEY, coverage_key text DEFAULT 'source-wide',
            expected_refresh_interval interval
        );
        CREATE FUNCTION meta.freshness_attack() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
            PERFORM nextval('public.freshness_attack_marker'); RETURN NEW;
        END $$;
        CREATE TRIGGER attack BEFORE INSERT ON meta.asset_freshness_policies
            FOR EACH ROW EXECUTE FUNCTION meta.freshness_attack();
        RESET ROLE;
    """,
    )
    with pytest.raises(psycopg2.Error, match="migration-owned"):
        query(conn, MIGRATION.read_text())
    conn.rollback()
    assert query(conn, "SELECT is_called FROM public.freshness_attack_marker") == [(False,)]
    assert query(conn, "SELECT to_regprocedure('public.get_asset_freshness()')") == [(None,)]
