"""Executed receipt freshness checks for the four SDV season sources."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import psycopg2
import pytest
from psycopg2 import sql

from tests.test_asset_freshness_sql import freshness_db as _freshness_db  # noqa: F401
from tests.test_asset_publication_sql import publication_db as _publication_db  # noqa: F401
from tests.test_flat_file_publication_sql import (
    SOURCES as BATCH_SOURCES,
)
from tests.test_flat_file_publication_sql import batch_db as _batch_db  # noqa: F401
from tests.test_flat_file_publication_sql import (
    plan as batch_plan,
)
from tests.test_flat_file_publication_sql import (
    publication_args as batch_publication_args,
)
from tests.test_flat_file_publication_sql import (
    publish as publish_batch,
)
from tests.test_flat_file_publication_sql import (
    source_query as batch_query,
)
from tests.test_flat_file_publication_sql import (
    start_load as start_batch_load,
)
from tests.test_generation_refresh_sql import generation_db as _generation_db  # noqa: F401
from tests.test_sdv_ratings_publication_sql import (
    ASSET as RATINGS_ASSET,
)
from tests.test_sdv_ratings_publication_sql import (
    plan as ratings_plan,
)
from tests.test_sdv_ratings_publication_sql import (
    publication_args as ratings_publication_args,
)
from tests.test_sdv_ratings_publication_sql import (
    publish as publish_ratings,
)
from tests.test_sdv_ratings_publication_sql import (
    source_query as ratings_query,
)
from tests.test_sdv_ratings_publication_sql import (
    start_load as start_ratings_load,
)
from tests.test_warehouse_migrations_sql import query
from tests.test_warehouse_migrations_sql import warehouse_db as _warehouse_db  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "src/schemas/migrations/073_sdv_source_freshness.sql"

SOURCE_ASSETS = {
    "sdv_ratings_weekly": RATINGS_ASSET,
    **{source: config["asset"] for source, config in BATCH_SOURCES.items()},
}
SOURCE_ORDER = tuple(SOURCE_ASSETS)
EXPECTED_COLUMNS = (
    "source_name",
    "asset_key",
    "season",
    "coverage_key",
    "generation_id",
    "published_at",
    "age_seconds",
    "expected_refresh_interval",
    "is_stale",
    "publication_state",
    "current_outcome",
    "is_complete",
    "source_rows",
    "published_rows",
    "artifact_origin",
    "season_basis",
    "latest_outcome",
    "latest_recorded_at",
    "last_failure_outcome",
    "last_failure_at",
    "last_failure_category",
)
EXPECTED_TYPE_OIDS = (
    25,  # text
    25,
    20,  # bigint
    25,
    2950,  # uuid
    1184,  # timestamptz
    1700,  # numeric
    1186,  # interval
    16,  # boolean
    25,
    25,
    16,
    20,
    20,
    25,
    25,
    25,
    1184,
    25,
    1184,
    25,
)
CURRENT_EVIDENCE_COLUMNS = (
    "generation_id",
    "published_at",
    "age_seconds",
    "current_outcome",
    "is_complete",
    "source_rows",
    "published_rows",
    "artifact_origin",
    "season_basis",
)
PRIVATE_TABLES = (
    "asset_publication_locks",
    "asset_receipts",
    "asset_current_generations",
    "asset_receipt_inputs",
    "asset_freshness_policies",
    "operation_runs",
)


@pytest.fixture
def source_freshness_db(request):
    conn, target = request.getfixturevalue("_batch_db")
    query(conn, MIGRATION.read_text())
    return conn, target


def source_rows(conn, season=2025, *, role="anon"):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
        cur.execute("SELECT * FROM public.get_source_freshness(%s)", (season,))
        columns = tuple(column.name for column in cur.description)
        type_oids = tuple(column.type_code for column in cur.description)
        result = [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]
    conn.commit()
    return result, columns, type_oids


def rows_by_source(conn, season=2025, *, role="anon"):
    result, _, _ = source_rows(conn, season, role=role)
    return {row["source_name"]: row for row in result}


def denied(conn, role, statement):
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
            try:
                cur.execute(statement)
            except psycopg2.errors.InsufficientPrivilege:
                return
            pytest.fail(f"{role} unexpectedly executed: {statement}")
    finally:
        conn.rollback()


def publish_source(conn, source, *, season=2025, suffix="a", artifact_origin="registered_url"):
    if source == "sdv_ratings_weekly":
        args = ratings_publication_args(
            conn,
            season=season,
            sha=(f"{season:04x}{suffix}" * 16)[:64],
            artifact_origin=artifact_origin,
        )
        publish_ratings(conn, args)
    else:
        args = batch_publication_args(
            conn,
            source,
            season=season,
            sha=(f"{season:04x}{suffix}" * 16)[:64],
            artifact_origin=artifact_origin,
        )
        publish_batch(conn, args)
    return args


def record_failure(conn, source, *, season=2025, outcome="failed"):
    generation_id = str(uuid.uuid4())
    if source == "sdv_ratings_weekly":
        run_id = start_ratings_load(conn, ratings_plan(conn, season))
        ratings_query(
            conn,
            "SELECT warehouse_source.fail_sdv_ratings_load(%s,%s,%s)",
            (run_id, generation_id, outcome),
        )
    else:
        run_id = start_batch_load(conn, batch_plan(conn, source, season))
        batch_query(
            conn,
            "SELECT warehouse_source_batch.fail_source_load(%s,%s,%s)",
            (run_id, generation_id, outcome),
        )
    return generation_id


def test_rpc_has_exact_typed_four_row_missing_contract(source_freshness_db):
    conn, _ = source_freshness_db
    for role in ("anon", "authenticated"):
        result, columns, type_oids = source_rows(conn, role=role)
        assert columns == EXPECTED_COLUMNS
        assert type_oids == EXPECTED_TYPE_OIDS
        assert [row["source_name"] for row in result] == list(SOURCE_ORDER)
        assert [row["asset_key"] for row in result] == list(SOURCE_ASSETS.values())
        for row in result:
            assert row["season"] == 2025
            assert row["coverage_key"] == "season:2025"
            assert row["publication_state"] == "unrecorded"
            assert row["expected_refresh_interval"] is None
            assert all(
                row[column] is None
                for column in EXPECTED_COLUMNS[4:]
                if column not in {"publication_state"}
            )


@pytest.mark.parametrize("season", [1869, 2200])
def test_rpc_accepts_exact_season_boundaries(source_freshness_db, season):
    conn, _ = source_freshness_db
    result = rows_by_source(conn, season)
    assert tuple(result) == SOURCE_ORDER
    assert {(row["season"], row["coverage_key"]) for row in result.values()} == {
        (season, f"season:{season}")
    }


@pytest.mark.parametrize("season", [None, 1868, 2201])
def test_rpc_rejects_null_and_out_of_range_seasons(source_freshness_db, season):
    conn, _ = source_freshness_db
    with pytest.raises(psycopg2.errors.InvalidParameterValue):
        query(conn, "SELECT * FROM public.get_source_freshness(%s)", (season,))
    conn.rollback()


@pytest.mark.parametrize("source", SOURCE_ORDER)
def test_each_publisher_yields_allowlisted_current_evidence_only_for_its_season(
    source_freshness_db, source
):
    conn, _ = source_freshness_db
    args = publish_source(conn, source)

    current = rows_by_source(conn)
    row = current[source]
    assert str(row["generation_id"]) == args[1]
    assert row["publication_state"] == "current"
    assert row["current_outcome"] == "succeeded"
    assert row["is_complete"] is True
    assert row["source_rows"] == 2
    assert row["published_rows"] == 2
    assert row["artifact_origin"] == "registered_url"
    assert row["season_basis"] == (
        "artifact_field"
        if source in {"sdv_ratings_weekly", "sdv_fpi_weekly"}
        else "registered_artifact_name"
    )
    assert row["published_at"] is not None
    assert row["age_seconds"] >= 0
    assert row["expected_refresh_interval"] is None
    assert row["is_stale"] is None
    assert row["latest_outcome"] == "succeeded"
    assert row["latest_recorded_at"] is not None
    assert row["last_failure_outcome"] is None
    for other_source in set(SOURCE_ORDER) - {source}:
        assert current[other_source]["publication_state"] == "unrecorded"
    assert all(
        other["publication_state"] == "unrecorded" for other in rows_by_source(conn, 2024).values()
    )


def test_source_and_season_publications_remain_isolated(source_freshness_db):
    conn, _ = source_freshness_db
    first = publish_source(conn, "sdv_ratings_weekly", season=2024, suffix="b")
    second = publish_source(conn, "sdv_team_xwalk", season=2025, suffix="c")

    rows_2024 = rows_by_source(conn, 2024)
    rows_2025 = rows_by_source(conn, 2025)
    assert str(rows_2024["sdv_ratings_weekly"]["generation_id"]) == first[1]
    assert rows_2024["sdv_team_xwalk"]["publication_state"] == "unrecorded"
    assert str(rows_2025["sdv_team_xwalk"]["generation_id"]) == second[1]
    assert rows_2025["sdv_ratings_weekly"]["publication_state"] == "unrecorded"


@pytest.mark.parametrize("source", ["sdv_team_xwalk", "sdv_game_xwalk"])
def test_local_crosswalk_publication_exposes_only_caller_declared_basis(
    source_freshness_db, source
):
    conn, _ = source_freshness_db
    publish_source(conn, source, artifact_origin="local_file")
    row = rows_by_source(conn)[source]
    assert row["publication_state"] == "current"
    assert row["artifact_origin"] == "local_file"
    assert row["season_basis"] == "caller_declared"


@pytest.mark.parametrize("source", SOURCE_ORDER)
def test_latest_failure_preserves_older_current_proof(source_freshness_db, source):
    conn, _ = source_freshness_db
    published = publish_source(conn, source)
    failed_generation = record_failure(conn, source)

    row = rows_by_source(conn)[source]
    assert str(row["generation_id"]) == published[1]
    assert str(row["generation_id"]) != failed_generation
    assert row["publication_state"] == "current"
    assert row["current_outcome"] == "succeeded"
    assert row["latest_outcome"] == "failed"
    assert row["last_failure_outcome"] == "failed"
    assert row["last_failure_at"] == row["latest_recorded_at"]
    assert row["last_failure_category"] == "source_publication_failed"


def test_missing_pointer_is_unpublished_and_cannot_resurrect_history(source_freshness_db):
    conn, _ = source_freshness_db
    published = publish_source(conn, "sdv_ratings_weekly")
    query(
        conn,
        "DELETE FROM meta.asset_current_generations "
        "WHERE asset_key=%s AND coverage_key='season:2025'",
        (RATINGS_ASSET,),
    )

    row = rows_by_source(conn)["sdv_ratings_weekly"]
    assert row["publication_state"] == "unpublished"
    assert row["latest_outcome"] == "succeeded"
    assert row["latest_recorded_at"] is not None
    assert all(row[column] is None for column in CURRENT_EVIDENCE_COLUMNS)
    assert query(
        conn,
        "SELECT count(*) FROM meta.asset_receipts WHERE generation_id=%s",
        (published[1],),
    ) == [(1,)]


@pytest.mark.parametrize(
    ("broken_field", "broken_expression", "broken_value", "private_marker"),
    [
        (
            "coverage",
            "jsonb_set(coverage,'{season}',%s::jsonb)",
            json.dumps("private-season-marker"),
            "private-season-marker",
        ),
        (
            "coverage",
            "jsonb_set(coverage,'{source_rows}',%s::jsonb)",
            json.dumps("private-source-rows-marker"),
            "private-source-rows-marker",
        ),
        (
            "coverage",
            "jsonb_set(coverage,'{published_rows}',%s::jsonb)",
            "1.5",
            None,
        ),
        (
            "input_observations",
            "%s::jsonb",
            json.dumps({"artifact_origin": "/private/secret.parquet"}),
            "/private/secret.parquet",
        ),
        (
            "row_delta",
            "jsonb_set(row_delta,'{published_rows}',%s::jsonb)",
            json.dumps("private-delta-marker"),
            "private-delta-marker",
        ),
        ("published_at", "%s::timestamptz", "infinity", None),
        ("published_at", "%s::timestamptz", "2201-01-01T00:00:00Z", None),
        ("outcome", "%s::text", "expected_no_data", None),
    ],
)
def test_unusable_pointer_is_invalid_and_exposes_no_current_fields(
    source_freshness_db,
    broken_field,
    broken_expression,
    broken_value,
    private_marker,
):
    conn, _ = source_freshness_db
    published = publish_source(conn, "sdv_ratings_weekly")
    invalid_generation = str(uuid.uuid4())
    replacement = {
        "outcome": "outcome",
        "coverage": "coverage",
        "input_observations": "input_observations",
        "row_delta": "row_delta",
        "published_at": "published_at",
    }
    replacement[broken_field] = broken_expression
    query(
        conn,
        f"""
        INSERT INTO meta.asset_receipts (
            generation_id, operation_run_id, asset_key, coverage_key, outcome,
            coverage, source_watermark, input_observations, row_delta,
            request_digest, published_at
        )
        SELECT %s, operation_run_id, asset_key, coverage_key,
            {replacement["outcome"]},
            {replacement["coverage"]}, source_watermark,
            {replacement["input_observations"]}, {replacement["row_delta"]},
            request_digest, {replacement["published_at"]}
        FROM meta.asset_receipts WHERE generation_id=%s
        """,
        (invalid_generation, broken_value, published[1]),
    )
    query(
        conn,
        "ALTER TABLE meta.asset_current_generations "
        "DISABLE TRIGGER guard_asset_current_generation; "
        "UPDATE meta.asset_current_generations SET generation_id=%s "
        "WHERE asset_key=%s AND coverage_key='season:2025'; "
        "ALTER TABLE meta.asset_current_generations ENABLE TRIGGER guard_asset_current_generation",
        (invalid_generation, RATINGS_ASSET),
    )

    row = rows_by_source(conn)["sdv_ratings_weekly"]
    assert row["publication_state"] == "invalid"
    assert row["latest_outcome"] == (broken_value if broken_field == "outcome" else "succeeded")
    assert all(row[column] is None for column in CURRENT_EVIDENCE_COLUMNS)
    serialized = json.dumps(row, default=str)
    assert invalid_generation not in serialized
    if private_marker is not None:
        assert private_marker not in serialized


def test_age_advances_at_query_time_and_analyze_cannot_refresh_proof(source_freshness_db):
    conn, _ = source_freshness_db
    published = publish_source(conn, "sdv_fpi_weekly")
    asset = SOURCE_ASSETS["sdv_fpi_weekly"]
    query(
        conn,
        "INSERT INTO meta.asset_freshness_policies "
        "(asset_key,coverage_key,expected_refresh_interval) VALUES (%s,%s,'0.3 seconds')",
        (asset, "season:2025"),
    )

    with conn.cursor() as cur:
        cur.execute("SET LOCAL ROLE anon")
        cur.execute(
            "SELECT generation_id,age_seconds FROM public.get_source_freshness(2025) "
            "WHERE source_name='sdv_fpi_weekly'"
        )
        before = cur.fetchone()
        cur.execute("SELECT pg_sleep(0.35)")
        cur.execute(
            "SELECT generation_id,age_seconds,is_stale "
            "FROM public.get_source_freshness(2025) WHERE source_name='sdv_fpi_weekly'"
        )
        after = cur.fetchone()
    conn.commit()
    assert str(before[0]) == published[1] == str(after[0])
    assert after[1] - before[1] >= 0.3
    assert after[2] is True

    query(conn, "ANALYZE ratings.espn_fpi_weekly")
    analyzed = rows_by_source(conn)["sdv_fpi_weekly"]
    assert str(analyzed["generation_id"]) == published[1]
    assert analyzed["is_stale"] is True
    query(
        conn,
        "UPDATE meta.asset_freshness_policies SET expected_refresh_interval='100 years' "
        "WHERE asset_key=%s AND coverage_key='season:2025'",
        (asset,),
    )
    assert rows_by_source(conn)["sdv_fpi_weekly"]["is_stale"] is False


def test_null_cadence_remains_unknown_for_current_publication(source_freshness_db):
    conn, _ = source_freshness_db
    publish_source(conn, "sdv_game_xwalk")
    asset = SOURCE_ASSETS["sdv_game_xwalk"]
    query(
        conn,
        "INSERT INTO meta.asset_freshness_policies(asset_key,coverage_key) VALUES (%s,%s)",
        (asset, "season:2025"),
    )
    row = rows_by_source(conn)["sdv_game_xwalk"]
    assert row["publication_state"] == "current"
    assert row["expected_refresh_interval"] is None
    assert row["is_stale"] is None


@pytest.mark.parametrize(
    ("asset", "coverage", "cadence"),
    [
        ("ratings.sdv_ratings_weekly", "source-wide", "1 day"),
        ("analytics.house_elo_game", "season:2025", "1 day"),
        ("ref.team_id_xwalk", "season:1868", "1 day"),
        ("ref.game_id_xwalk", "season:2201", "1 day"),
        ("ratings.espn_fpi_weekly", "season:2025", "0 seconds"),
    ],
)
def test_policy_rejects_other_grains_bounds_and_nonpositive_cadence(
    source_freshness_db, asset, coverage, cadence
):
    conn, _ = source_freshness_db
    with pytest.raises(psycopg2.errors.CheckViolation):
        query(
            conn,
            "INSERT INTO meta.asset_freshness_policies "
            "(asset_key,coverage_key,expected_refresh_interval) VALUES (%s,%s,%s)",
            (asset, coverage, cadence),
        )
    conn.rollback()


def test_access_boundary_scrubs_hostile_defaults_and_private_evidence(
    source_freshness_db,
):
    conn, _ = source_freshness_db
    publish_source(conn, "sdv_team_xwalk")
    record_failure(conn, "sdv_team_xwalk")

    for role in ("anon", "authenticated"):
        result, columns, _ = source_rows(conn, role=role)
        assert len(result) == 4
        assert columns == EXPECTED_COLUMNS
    for role in (
        "analyst_ro",
        "publication_bystander",
        "warehouse_publisher",
        "warehouse_source_publisher",
    ):
        denied(conn, role, "SELECT * FROM public.get_source_freshness(2025)")
    for role in (
        "anon",
        "authenticated",
        "analyst_ro",
        "publication_bystander",
        "warehouse_publisher",
        "warehouse_source_publisher",
    ):
        for table in PRIVATE_TABLES:
            denied(conn, role, f"SELECT * FROM meta.{table}")
        denied(
            conn,
            role,
            "UPDATE meta.asset_freshness_policies SET expected_refresh_interval='1 day'",
        )
    assert query(
        conn,
        "SELECT has_table_privilege('anon','meta.flat_file_loads','SELECT'), "
        "has_table_privilege('authenticated','meta.flat_file_loads','SELECT')",
    ) == [(True, True)]

    document = json.dumps(rows_by_source(conn), default=str)
    for forbidden in (
        "input_observations",
        "error_summary",
        "operation_run_id",
        "source_watermark",
        "request_digest",
        "warehouse_source_stage",
        "github.com/",
        ".parquet",
    ):
        assert forbidden not in document


def test_migration_reapply_preserves_policy_old_elo_rpc_and_existing_acl(request):
    conn, _ = request.getfixturevalue("_batch_db")
    old_definition = query(
        conn,
        "SELECT pg_get_functiondef('public.get_asset_freshness()'::regprocedure)",
    )
    old_rows = query(
        conn,
        "SELECT asset_key,coverage_key,expected_refresh_interval "
        "FROM public.get_asset_freshness() ORDER BY asset_key",
    )
    old_acl = query(
        conn,
        "SELECT has_function_privilege('anon','public.get_asset_freshness()','EXECUTE'), "
        "has_function_privilege('authenticated','public.get_asset_freshness()','EXECUTE'), "
        "has_function_privilege('publication_bystander',"
        "'public.get_asset_freshness()','EXECUTE')",
    )
    flat_file_acl = query(
        conn,
        "SELECT has_table_privilege('anon','meta.flat_file_loads','SELECT'), "
        "has_table_privilege('authenticated','meta.flat_file_loads','SELECT')",
    )

    query(conn, MIGRATION.read_text())
    query(
        conn,
        "INSERT INTO meta.asset_freshness_policies "
        "(asset_key,coverage_key,expected_refresh_interval) VALUES (%s,%s,'2 days')",
        (RATINGS_ASSET, "season:2025"),
    )
    query(conn, MIGRATION.read_text())

    assert query(
        conn,
        "SELECT expected_refresh_interval::text FROM meta.asset_freshness_policies "
        "WHERE asset_key=%s AND coverage_key='season:2025'",
        (RATINGS_ASSET,),
    ) == [("2 days",)]
    assert (
        query(
            conn,
            "SELECT pg_get_functiondef('public.get_asset_freshness()'::regprocedure)",
        )
        == old_definition
    )
    assert (
        query(
            conn,
            "SELECT asset_key,coverage_key,expected_refresh_interval "
            "FROM public.get_asset_freshness() ORDER BY asset_key",
        )
        == old_rows
        == [
            ("analytics.house_elo_game", "source-wide", None),
            ("marts.house_elo_game", "source-wide", None),
        ]
    )
    assert (
        query(
            conn,
            "SELECT has_function_privilege('anon','public.get_asset_freshness()','EXECUTE'), "
            "has_function_privilege('authenticated','public.get_asset_freshness()','EXECUTE'), "
            "has_function_privilege('publication_bystander',"
            "'public.get_asset_freshness()','EXECUTE')",
        )
        == old_acl
        == [(True, True, False)]
    )
    assert (
        query(
            conn,
            "SELECT has_table_privilege('anon','meta.flat_file_loads','SELECT'), "
            "has_table_privilege('authenticated','meta.flat_file_loads','SELECT')",
        )
        == flat_file_acl
        == [(True, True)]
    )


@pytest.mark.parametrize(
    "sabotage",
    [
        "ALTER TABLE meta.asset_freshness_policies OWNER TO publication_bystander",
        "ALTER TABLE meta.asset_freshness_policies DISABLE ROW LEVEL SECURITY",
    ],
)
def test_migration_rejects_untrusted_private_policy_before_rpc_exposure(request, sabotage):
    conn, _ = request.getfixturevalue("_batch_db")
    query(conn, sabotage)
    with pytest.raises(psycopg2.Error, match="trusted migration-owned private table"):
        query(conn, MIGRATION.read_text())
    conn.rollback()
    assert query(
        conn,
        "SELECT to_regprocedure('public.get_source_freshness(bigint)')",
    ) == [(None,)]


def test_real_verifier_reports_failure_recovery_without_losing_history(source_freshness_db, capsys):
    from scripts.verify_load import Report, check_source_freshness

    conn, _ = source_freshness_db
    for suffix, source in zip("abcd", SOURCE_ORDER, strict=True):
        publish_source(conn, source, suffix=suffix)
    query(
        conn,
        "INSERT INTO meta.asset_freshness_policies "
        "(asset_key,coverage_key,expected_refresh_interval) "
        "SELECT asset_key,'season:2025','100 years'::interval "
        "FROM unnest(%s::text[]) AS asset(asset_key)",
        (list(SOURCE_ASSETS.values()),),
    )

    def verify_as_anon():
        report = Report()
        with conn.cursor() as cur:
            cur.execute("SET LOCAL ROLE anon")
            check_source_freshness(
                cur,
                season=2025,
                in_season=True,
                strict=True,
                report=report,
            )
        conn.rollback()
        return report, capsys.readouterr().out

    report, output = verify_as_anon()
    assert report.failures == 0
    assert output.count("[PASS] source_freshness:") == 4

    record_failure(conn, "sdv_ratings_weekly")
    report, output = verify_as_anon()
    assert report.failures == 0
    assert output.count("[PASS] source_freshness:") == 4
    assert "[WARN] source_receipt_attempt:" in output
    assert "latest attempt failed" in output

    recovered = publish_source(conn, "sdv_ratings_weekly", suffix="e")
    report, output = verify_as_anon()
    assert report.failures == 0
    assert output.count("[PASS] source_freshness:") == 4
    assert "[WARN] source_receipt_attempt:" not in output
    row = rows_by_source(conn)["sdv_ratings_weekly"]
    assert str(row["generation_id"]) == recovered[1]
    assert row["latest_outcome"] == "succeeded"
    assert row["last_failure_outcome"] == "failed"
    assert row["last_failure_category"] == "source_publication_failed"
