"""Executed season-scoped SDV ratings publication checks on PostgreSQL 17."""

from __future__ import annotations

import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from psycopg2 import sql
from psycopg2.extensions import parse_dsn

from tests.test_asset_freshness_sql import freshness_db as _freshness_db  # noqa: F401
from tests.test_asset_publication_sql import publication_args as elo_publication_args
from tests.test_asset_publication_sql import publication_db as _publication_db  # noqa: F401
from tests.test_asset_publication_sql import publish as publish_elo
from tests.test_generation_refresh_sql import generation_db as _generation_db  # noqa: F401
from tests.test_warehouse_migrations_sql import query
from tests.test_warehouse_migrations_sql import warehouse_db as _warehouse_db  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "src/schemas/migrations/071_sdv_ratings_publication.sql"
ASSET = "ratings.sdv_ratings_weekly"
PROTOCOL = "sdv-ratings-season-v1"
PARSER_CONTRACT = "sdv-ratings-v1"

SOURCE_OBJECTS = """
CREATE SCHEMA ratings;
CREATE TABLE ratings.sdv_ratings_weekly (
    season bigint NOT NULL,
    through_week bigint NOT NULL,
    team_id bigint NOT NULL,
    adj_off_epa double precision,
    adj_def_epa double precision,
    adj_st_epa double precision,
    adj_net double precision,
    fei_off double precision,
    fei_def double precision,
    fei_net double precision,
    games bigint,
    off_pace double precision,
    off_rank bigint,
    def_rank bigint,
    net_rank bigint,
    net_z double precision,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    _dlt_load_id varchar NOT NULL,
    _dlt_id varchar NOT NULL,
    PRIMARY KEY (season, through_week, team_id),
    UNIQUE (_dlt_id)
);
CREATE INDEX idx_sdv_ratings_weekly_team
    ON ratings.sdv_ratings_weekly(team_id, season);

CREATE TABLE meta.flat_file_loads (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source text NOT NULL,
    file_sha256 text NOT NULL,
    source_url text,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    row_count integer,
    status text NOT NULL CHECK (status IN ('loaded', 'skipped', 'failed')),
    error text
);
CREATE UNIQUE INDEX uq_flat_file_loads_loaded
    ON meta.flat_file_loads(source, file_sha256) WHERE status='loaded';
CREATE INDEX idx_flat_file_loads_source_time
    ON meta.flat_file_loads(source, loaded_at DESC);
"""

PUBLISH_SQL = """
SELECT * FROM warehouse_source.publish_sdv_ratings_load(
    %s, %s, %s, %s::jsonb, %s::jsonb
)
"""


@pytest.fixture
def sdv_db(request):
    conn, target = request.getfixturevalue("_generation_db")
    query(conn, SOURCE_OBJECTS)
    query(conn, MIGRATION.read_text())
    return conn, target


def source_query(conn, statement, values=None, *, commit=True):
    with conn.cursor() as cur:
        cur.execute("SET LOCAL ROLE warehouse_source_publisher")
        cur.execute(statement, values)
        result = cur.fetchall() if cur.description else None
    if commit:
        conn.commit()
    return result


def plan(conn, season=2025):
    return source_query(conn, "SELECT warehouse_source.get_sdv_ratings_plan(%s)", (season,))[0][0]


def start_load(conn, load_plan, run_id=None):
    run_id = str(run_id or uuid.uuid4())
    result = source_query(
        conn,
        "SELECT warehouse_source.start_sdv_ratings_load(%s,%s::jsonb)",
        (run_id, json.dumps(load_plan)),
    )[0][0]
    assert str(result) == run_id
    return run_id


def rating_row(season=2025, through_week=1, team_id=1, *, adj_net=1.25, suffix="a"):
    return {
        "season": season,
        "through_week": through_week,
        "team_id": team_id,
        "adj_off_epa": 0.25 + team_id,
        "adj_def_epa": -0.1 - team_id,
        "adj_st_epa": 0.05,
        "adj_net": adj_net,
        "fei_off": 0.4,
        "fei_def": -0.3,
        "fei_net": 0.7,
        "games": 1,
        "off_pace": 70.5,
        "off_rank": team_id,
        "def_rank": team_id + 1,
        "net_rank": team_id,
        "net_z": 0.2,
        "_dlt_load_id": f"load-{season}-{suffix}",
        "_dlt_id": f"row-{season}-{through_week}-{team_id}-{suffix}",
    }


def rows_for(season=2025, *, adj_net=1.25, suffix="a"):
    return [
        rating_row(season, 1, 1, adj_net=adj_net, suffix=suffix),
        rating_row(season, 1, 2, adj_net=adj_net + 1, suffix=suffix),
    ]


def evidence(rows, *, artifact_origin="registered_url"):
    return {
        "artifact_origin": artifact_origin,
        "stage_schema": "warehouse_source_stage",
        "parser_contract": PARSER_CONTRACT,
        "dlt_load_ids": sorted({row["_dlt_load_id"] for row in rows}),
        "source_rows": len(rows),
    }


def publication_args(
    conn,
    *,
    season=2025,
    rows=None,
    generation_id=None,
    sha=None,
    load_plan=None,
    artifact_origin="registered_url",
):
    rows = rows if rows is not None else rows_for(season)
    load_plan = load_plan or plan(conn, season)
    return (
        start_load(conn, load_plan),
        str(generation_id or uuid.uuid4()),
        sha or (f"{season:04x}" * 16)[:64],
        json.dumps(rows),
        json.dumps(evidence(rows, artifact_origin=artifact_origin)),
    )


def publish(conn, args, *, commit=True):
    return source_query(conn, PUBLISH_SQL, args, commit=commit)[0]


def current(conn, season=None):
    values = (f"season:{season}",) if season is not None else None
    where = " AND coverage_key=%s" if season is not None else ""
    return query(
        conn,
        "SELECT coverage_key,generation_id::text FROM meta.asset_current_generations "
        f"WHERE asset_key='{ASSET}'{where} ORDER BY coverage_key",
        values,
    )


def denied(conn, role, statement):
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cur.execute(statement)
    finally:
        conn.rollback()


def test_plan_is_canonical_season_scoped_and_has_no_freshness_claim(sdv_db):
    conn, _ = sdv_db
    assert plan(conn) == {
        "protocol": PROTOCOL,
        "asset_key": ASSET,
        "coverage_key": "season:2025",
        "season": 2025,
        "expected_generation_id": None,
        "parser_contract": PARSER_CONTRACT,
    }
    assert query(
        conn,
        "SELECT count(*) FROM meta.asset_freshness_policies "
        "WHERE asset_key='ratings.sdv_ratings_weekly'",
    ) == [(0,)]
    with pytest.raises(psycopg2.Error):
        plan(conn, 1868)
    conn.rollback()
    with pytest.raises(psycopg2.Error):
        plan(conn, 2201)
    conn.rollback()


def test_correction_replaces_one_season_and_commits_exact_receipt_and_ledger(sdv_db):
    conn, _ = sdv_db
    other_args = publication_args(conn, season=2024, rows=rows_for(2024), sha="4" * 64)
    publish(conn, other_args)
    first_rows = rows_for(2025)
    first_args = publication_args(conn, rows=first_rows, sha="a" * 64)
    first = publish(conn, first_args)
    corrected = [rating_row(2025, 1, 1, adj_net=9.5, suffix="b")]
    corrected_args = publication_args(conn, rows=corrected, sha="b" * 64)
    result = publish(conn, corrected_args)

    assert first == (first_args[1], False, 2)
    assert result == (corrected_args[1], False, 1)
    assert query(
        conn,
        "SELECT season,through_week,team_id,adj_net,_dlt_id "
        "FROM ratings.sdv_ratings_weekly ORDER BY season,team_id",
    ) == [
        (2024, 1, 1, 1.25, "row-2024-1-1-a"),
        (2024, 1, 2, 2.25, "row-2024-1-2-a"),
        (2025, 1, 1, 9.5, "row-2025-1-1-b"),
    ]
    assert current(conn) == [
        ("season:2024", other_args[1]),
        ("season:2025", corrected_args[1]),
    ]
    receipt = query(
        conn,
        """
        SELECT r.asset_key,r.coverage_key,r.outcome,r.source_watermark,
               r.coverage,r.input_observations,r.row_delta,
               o.operation_kind,o.initiator,o.outcome
        FROM meta.asset_receipts r
        JOIN meta.operation_runs o USING(operation_run_id)
        WHERE r.generation_id=%s
        """,
        (corrected_args[1],),
    )[0]
    assert receipt[:4] == (ASSET, "season:2025", "succeeded", "b" * 64)
    assert receipt[4] == {
        "complete": True,
        "scope": "season",
        "season": 2025,
        "mode": "full_file_for_season",
        "source_rows": 1,
        "published_rows": 1,
    }
    assert receipt[5] == {
        "publisher": "load_flat_files",
        "protocol": PROTOCOL,
        "artifact_origin": "registered_url",
        "stage_schema": "warehouse_source_stage",
        "parser_contract": PARSER_CONTRACT,
        "dlt_load_ids": ["load-2025-b"],
    }
    assert receipt[6] == {
        "previous_rows": 2,
        "published_rows": 1,
        "inserted_rows": 0,
        "deleted_rows": 1,
        "changed_rows": 1,
    }
    assert receipt[7:] == ("load", "load_flat_files", "succeeded")
    assert query(conn, "SELECT count(*) FROM meta.asset_receipt_inputs") == [(0,)]
    assert query(
        conn,
        "SELECT source,file_sha256,source_url,row_count,status,error "
        "FROM meta.flat_file_loads ORDER BY id",
    ) == [
        (
            "sdv_ratings_weekly:2024",
            "4" * 64,
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_ratings_weekly/cfb_ratings_weekly_2024.parquet",
            2,
            "loaded",
            None,
        ),
        (
            "sdv_ratings_weekly:2025",
            "a" * 64,
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_ratings_weekly/cfb_ratings_weekly_2025.parquet",
            2,
            "loaded",
            None,
        ),
        (
            "sdv_ratings_weekly:2025",
            "b" * 64,
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_ratings_weekly/cfb_ratings_weekly_2025.parquet",
            1,
            "loaded",
            None,
        ),
    ]


def test_data_receipt_pointer_and_ledger_are_rollback_atomic(sdv_db):
    conn, target = sdv_db
    original = publication_args(conn, sha="a" * 64)
    publish(conn, original)
    before = query(
        conn,
        "SELECT team_id,adj_net FROM ratings.sdv_ratings_weekly ORDER BY team_id",
    )
    changed_rows = [rating_row(2025, 1, 1, adj_net=8.0, suffix="rollback")]
    changed = publication_args(conn, rows=changed_rows, sha="c" * 64)
    observer = psycopg2.connect(target)
    try:
        result = publish(conn, changed, commit=False)
        assert result == (changed[1], False, 1)
        assert (
            query(
                observer,
                "SELECT team_id,adj_net FROM ratings.sdv_ratings_weekly ORDER BY team_id",
            )
            == before
        )
        conn.rollback()
        assert (
            query(
                observer,
                "SELECT team_id,adj_net FROM ratings.sdv_ratings_weekly ORDER BY team_id",
            )
            == before
        )
        assert current(observer, 2025) == [("season:2025", original[1])]
        assert query(
            observer,
            "SELECT count(*) FROM meta.asset_receipts WHERE generation_id=%s",
            (changed[1],),
        ) == [(0,)]
        assert query(
            observer,
            "SELECT count(*) FROM meta.flat_file_loads WHERE file_sha256=%s",
            (changed[2],),
        ) == [(0,)]
    finally:
        observer.close()


def test_exact_replay_never_restores_an_older_pointer_or_data(sdv_db):
    conn, _ = sdv_db
    first = publication_args(conn, rows=rows_for(2025, adj_net=1.0), sha="a" * 64)
    first_result = publish(conn, first)
    second = publication_args(conn, rows=rows_for(2025, adj_net=5.0, suffix="new"), sha="b" * 64)
    publish(conn, second)
    rows_before = query(
        conn, "SELECT team_id,adj_net FROM ratings.sdv_ratings_weekly ORDER BY team_id"
    )

    replay = publish(conn, first)
    assert replay == (first_result[0], True, 2)
    assert current(conn, 2025) == [("season:2025", second[1])]
    assert (
        query(conn, "SELECT team_id,adj_net FROM ratings.sdv_ratings_weekly ORDER BY team_id")
        == rows_before
    )
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts WHERE asset_key=%s", (ASSET,)) == [
        (2,)
    ]
    assert query(conn, "SELECT status FROM meta.flat_file_loads ORDER BY id") == [
        ("loaded",),
        ("loaded",),
    ]


def test_new_generation_reparses_same_hash_and_records_a_skipped_ledger_check(sdv_db):
    conn, _ = sdv_db
    rows = rows_for(2025)
    first = publication_args(conn, rows=rows, sha="a" * 64)
    publish(conn, first)
    second = publication_args(conn, rows=rows, sha="a" * 64)
    result = publish(conn, second)

    assert result == (second[1], False, 2)
    assert current(conn, 2025) == [("season:2025", second[1])]
    assert query(
        conn,
        "SELECT status,row_count FROM meta.flat_file_loads ORDER BY id",
    ) == [("loaded", 2), ("skipped", 2)]
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts WHERE asset_key=%s", (ASSET,)) == [
        (2,)
    ]


@pytest.mark.parametrize(
    "malformation",
    ["duplicate", "required_null", "wrong_season", "nonfinite", "extra", "missing"],
)
def test_malformed_payload_is_rejected_before_any_publication(malformation, sdv_db):
    conn, _ = sdv_db
    rows = rows_for(2025)
    if malformation == "duplicate":
        rows.append(dict(rows[0], _dlt_id="different-dlt-id"))
    elif malformation == "required_null":
        rows[0]["team_id"] = None
    elif malformation == "wrong_season":
        rows[0]["season"] = 2024
    elif malformation == "nonfinite":
        rows[0]["adj_net"] = "Infinity"
    elif malformation == "extra":
        rows[0]["unexpected"] = 1
    else:
        del rows[0]["adj_net"]
    args = publication_args(conn, rows=rows, sha="d" * 64)
    if malformation == "nonfinite":
        args = (*args[:3], args[3].replace('"adj_net": "Infinity"', '"adj_net": 1e10000'), args[4])

    with pytest.raises(psycopg2.Error):
        publish(conn, args)
    conn.rollback()
    assert query(conn, "SELECT count(*) FROM ratings.sdv_ratings_weekly") == [(0,)]
    assert current(conn) == []
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts WHERE asset_key=%s", (ASSET,)) == [
        (0,)
    ]
    assert query(conn, "SELECT count(*) FROM meta.flat_file_loads") == [(0,)]


def test_local_file_provenance_records_no_provider_url_and_unknown_origin_is_rejected(sdv_db):
    conn, _ = sdv_db
    local = publication_args(conn, artifact_origin="local_file", sha="a" * 64)
    publish(conn, local)
    assert query(
        conn,
        "SELECT source_url FROM meta.flat_file_loads WHERE file_sha256=%s",
        (local[2],),
    ) == [(None,)]
    assert query(
        conn,
        "SELECT input_observations->>'artifact_origin' FROM meta.asset_receipts "
        "WHERE generation_id=%s",
        (local[1],),
    ) == [("local_file",)]

    bad = list(publication_args(conn, sha="b" * 64))
    bad_evidence = json.loads(bad[4])
    bad_evidence["artifact_origin"] = "unknown"
    bad[4] = json.dumps(bad_evidence)
    with pytest.raises(psycopg2.Error, match="evidence"):
        publish(conn, tuple(bad))
    conn.rollback()
    assert current(conn, 2025) == [("season:2025", local[1])]
    assert query(conn, "SELECT count(*) FROM meta.flat_file_loads") == [(1,)]


def test_publisher_role_has_only_bounded_rpc_access(sdv_db):
    conn, _ = sdv_db
    query(conn, MIGRATION.read_text())
    assert query(
        conn,
        "SELECT rolcanlogin,rolinherit,rolsuper,rolcreatedb,rolcreaterole,"
        "rolreplication,rolbypassrls FROM pg_roles WHERE rolname='warehouse_source_publisher'",
    ) == [(False, False, False, False, False, False, False)]
    for role in ("anon", "authenticated", "analyst_ro", "publication_bystander"):
        denied(conn, role, "SELECT warehouse_source.get_sdv_ratings_plan(2025)")
        assert query(
            conn,
            "SELECT has_schema_privilege(%s,'warehouse_source_stage','USAGE,CREATE')",
            (role,),
        ) == [(False,)]
    for relation in (
        "ratings.sdv_ratings_weekly",
        "meta.flat_file_loads",
        "meta.asset_receipts",
        "meta.asset_current_generations",
        "meta.operation_runs",
    ):
        assert query(
            conn,
            "SELECT has_table_privilege('warehouse_source_publisher',%s,"
            "'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')",
            (relation,),
        ) == [(False,)]
    denied(conn, "warehouse_source_publisher", "UPDATE ratings.sdv_ratings_weekly SET games=9")
    denied(conn, "warehouse_source_publisher", "SELECT * FROM meta.asset_receipts")
    assert query(
        conn,
        "SELECT has_schema_privilege(current_user,'warehouse_source_stage','USAGE,CREATE'),"
        "has_schema_privilege('warehouse_source_publisher','warehouse_source_stage',"
        "'USAGE,CREATE'),has_function_privilege('warehouse_source_publisher',"
        "'warehouse_source.require_sdv_ratings_load(uuid)','EXECUTE')",
    ) == [(True, False, False)]
    for statement in (
        "SELECT warehouse_quota.start_operation_run("
        "gen_random_uuid(),'load','forged','{}',NULL,NULL)",
        "SELECT warehouse_quota.finish_operation_run(gen_random_uuid(),'failed','forged')",
    ):
        denied(conn, "warehouse_source_publisher", statement)


def test_different_seasons_have_independent_canonical_pointers(sdv_db):
    conn, _ = sdv_db
    one = publication_args(conn, season=2024, rows=rows_for(2024), sha="4" * 64)
    two = publication_args(conn, season=2025, rows=rows_for(2025), sha="5" * 64)
    publish(conn, one)
    publish(conn, two)
    assert current(conn) == [("season:2024", one[1]), ("season:2025", two[1])]
    assert query(
        conn,
        "SELECT count(*) FROM meta.asset_current_generations p "
        "JOIN meta.asset_receipts r USING(asset_key,coverage_key,generation_id) "
        "WHERE p.asset_key=%s AND r.outcome='succeeded' AND r.coverage->>'complete'='true'",
        (ASSET,),
    ) == [(2,)]


@pytest.mark.parametrize(
    "asset_key,coverage_key",
    [(ASSET, "source-wide"), ("analytics.house_elo_game", "season:2025")],
)
def test_cross_grain_receipt_scope_is_rejected(sdv_db, asset_key, coverage_key):
    conn, _ = sdv_db
    run_id = str(uuid.uuid4())
    query(
        conn,
        "SELECT warehouse_quota.start_operation_run(%s,'load','fixture','{}',NULL,NULL)",
        (run_id,),
    )
    with pytest.raises(psycopg2.Error):
        query(
            conn,
            """
            INSERT INTO meta.asset_receipts(
                generation_id,operation_run_id,asset_key,coverage_key,outcome,
                coverage,request_digest,error_summary
            ) VALUES (%s,%s,%s,%s,'failed','{"complete":false}',%s,'fixture')
            """,
            (str(uuid.uuid4()), run_id, asset_key, coverage_key, "0" * 32),
        )
    conn.rollback()


def test_concurrent_first_publication_compare_and_swap_has_one_winner(sdv_db):
    conn, target = sdv_db
    expected = plan(conn)
    args = [
        publication_args(
            conn,
            load_plan=expected,
            rows=rows_for(2025, adj_net=value, suffix=str(value)),
            sha=char * 64,
        )
        for value, char in ((1.0, "a"), (2.0, "b"))
    ]

    def contender(call_args):
        other = psycopg2.connect(target)
        try:
            try:
                return "ok", source_query(other, PUBLISH_SQL, call_args)[0]
            except psycopg2.Error as exc:
                other.rollback()
                return "error", str(exc)
        finally:
            other.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(contender, args))
    assert sorted(status for status, _ in results) == ["error", "ok"]
    assert "changed" in next(detail for status, detail in results if status == "error").lower()
    winner = next(detail for status, detail in results if status == "ok")
    assert current(conn, 2025) == [("season:2025", str(winner[0]))]
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts WHERE asset_key=%s", (ASSET,)) == [
        (1,)
    ]
    assert query(conn, "SELECT count(*) FROM meta.flat_file_loads") == [(1,)]


@pytest.mark.parametrize("outcome", ["failed", "deferred"])
def test_failure_finishes_run_and_never_repoints(outcome, sdv_db):
    conn, _ = sdv_db
    good = publication_args(conn, sha="a" * 64)
    publish(conn, good)
    before = current(conn, 2025)
    failed_plan = plan(conn)
    run_id = start_load(conn, failed_plan)
    generation_id = str(uuid.uuid4())
    result = source_query(
        conn,
        "SELECT warehouse_source.fail_sdv_ratings_load(%s,%s,%s)",
        (run_id, generation_id, outcome),
    )[0][0]
    assert str(result) == generation_id
    assert current(conn, 2025) == before
    assert query(
        conn,
        "SELECT r.outcome,r.published_at,r.coverage->>'complete',r.error_summary,"
        "o.outcome FROM meta.asset_receipts r JOIN meta.operation_runs o USING(operation_run_id) "
        "WHERE r.generation_id=%s",
        (generation_id,),
    ) == [
        (
            outcome,
            None,
            "false",
            "source_publication_failed",
            "blocked" if outcome == "deferred" else outcome,
        )
    ]


def test_legacy_row_writes_invalidate_only_affected_seasons_and_truncate_all(sdv_db):
    conn, _ = sdv_db
    publish(conn, publication_args(conn, season=2024, rows=rows_for(2024), sha="4" * 64))
    publish(conn, publication_args(conn, season=2025, rows=rows_for(2025), sha="5" * 64))
    query(conn, "UPDATE ratings.sdv_ratings_weekly SET adj_net=8 WHERE season=2025")
    assert [row[0] for row in current(conn)] == ["season:2024"]

    publish(conn, publication_args(conn, season=2025, rows=rows_for(2025), sha="6" * 64))
    query(conn, "DELETE FROM ratings.sdv_ratings_weekly WHERE season=2024 AND team_id=1")
    assert [row[0] for row in current(conn)] == ["season:2025"]

    query(conn, "TRUNCATE ratings.sdv_ratings_weekly")
    assert current(conn) == []


@pytest.mark.parametrize(
    "statement",
    [
        "ALTER TABLE ratings.sdv_ratings_weekly RENAME TO sdv_ratings_weekly_moved",
        "DROP TABLE ratings.sdv_ratings_weekly",
        "ALTER TABLE ratings.sdv_ratings_weekly DISABLE TRIGGER USER",
    ],
)
def test_ddl_changes_and_disabled_guard_invalidate_all_seasons(sdv_db, statement):
    conn, _ = sdv_db
    publish(conn, publication_args(conn, season=2024, rows=rows_for(2024), sha="4" * 64))
    publish(conn, publication_args(conn, season=2025, rows=rows_for(2025), sha="5" * 64))
    query(conn, statement)
    assert current(conn) == []


def test_catalog_drift_fails_closed_without_installing_a_receipt(sdv_db):
    conn, _ = sdv_db
    args = publication_args(conn)
    query(conn, "ALTER TABLE ratings.sdv_ratings_weekly ADD COLUMN drifted text")
    with pytest.raises(psycopg2.Error, match="catalog|contract"):
        publish(conn, args)
    conn.rollback()
    assert query(conn, "SELECT count(*) FROM ratings.sdv_ratings_weekly") == [(0,)]
    assert current(conn) == []
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts WHERE asset_key=%s", (ASSET,)) == [
        (0,)
    ]


def test_inherited_child_invalidates_pointer_and_blocks_unsupported_topology(sdv_db):
    conn, _ = sdv_db
    first = publication_args(conn)
    publish(conn, first)
    query(
        conn,
        "CREATE TABLE ratings.sdv_ratings_weekly_child () INHERITS (ratings.sdv_ratings_weekly)",
    )
    query(
        conn,
        """
        INSERT INTO ratings.sdv_ratings_weekly_child(
            season,through_week,team_id,loaded_at,_dlt_load_id,_dlt_id
        ) VALUES (2025,2,99,now(),'inherited-load','inherited-row')
        """,
    )
    assert current(conn) == []

    retry = publication_args(conn, rows=rows_for(2025, suffix="retry"), sha="b" * 64)
    with pytest.raises(psycopg2.Error, match="catalog|contract|inherit"):
        publish(conn, retry)
    conn.rollback()
    assert current(conn) == []
    assert query(conn, "SELECT count(*) FROM meta.asset_receipts WHERE asset_key=%s", (ASSET,)) == [
        (1,)
    ]
    assert query(conn, "SELECT count(*) FROM ONLY ratings.sdv_ratings_weekly_child") == [(1,)]


@pytest.mark.parametrize(
    "statement",
    [
        "ALTER TABLE ratings.sdv_ratings_weekly RENAME TO sdv_ratings_weekly_moved",
        "DROP TABLE ratings.sdv_ratings_weekly",
    ],
)
def test_unrelated_sdv_ddl_does_not_block_house_elo_publication(sdv_db, statement):
    conn, _ = sdv_db
    publish(conn, publication_args(conn))
    query(conn, statement)

    args = elo_publication_args(conn)
    result = publish_elo(conn, args)
    assert result[:3] == (args[1], args[2], False)
    assert query(
        conn,
        "SELECT asset_key,coverage_key FROM meta.asset_current_generations ORDER BY asset_key",
    ) == [
        ("analytics.house_elo_game", "source-wide"),
        ("marts.house_elo_game", "source-wide"),
    ]


def test_public_freshness_contract_remains_house_elo_only(sdv_db):
    conn, _ = sdv_db
    publish(conn, publication_args(conn))
    assert query(
        conn,
        "SELECT asset_key,coverage_key,expected_refresh_interval "
        "FROM public.get_asset_freshness() ORDER BY asset_key",
    ) == [
        ("analytics.house_elo_game", "source-wide", None),
        ("marts.house_elo_game", "source-wide", None),
    ]


def test_real_python_adapter_stages_local_parquet_and_publishes_typed_nulls(
    sdv_db, monkeypatch, tmp_path
):
    from src.pipelines.sources.flat_files import REGISTRY
    from src.pipelines.utils import sdv_ratings_publication as adapter

    conn, target = sdv_db
    schema = pq.read_schema(ROOT / "tests/fixtures/flatfiles/sdv_ratings_weekly_sample.parquet")
    raw_row = {name: None for name in schema.names}
    raw_row.update(season=2025, through_week=1, team_id="333")
    local_file = tmp_path / "sdv-ratings-null-optionals.parquet"
    pq.write_table(pa.Table.from_pylist([raw_row], schema=schema), local_file)
    dsn = parse_dsn(target)
    target_url = (
        f"postgresql://{dsn['user']}:{dsn['password']}@{dsn['host']}:{dsn['port']}/{dsn['dbname']}"
    )
    monkeypatch.setattr(adapter, "get_db_url", lambda: target_url)

    result = adapter.run_sdv_ratings_publication(
        REGISTRY["sdv_ratings_weekly"], file_path=str(local_file), season=2025
    )

    assert result["error"] is None
    assert result["status"] == "loaded"
    assert result["rows"] == 1
    row = query(
        conn,
        """
        SELECT season,through_week,team_id,adj_off_epa,adj_def_epa,adj_st_epa,
               adj_net,fei_off,fei_def,fei_net,games,off_pace,off_rank,def_rank,
               net_rank,net_z,_dlt_load_id,_dlt_id
        FROM ratings.sdv_ratings_weekly
        """,
    )[0]
    assert row[:3] == (2025, 1, 333)
    assert row[3:16] == (None,) * 13
    assert all(isinstance(value, str) and value for value in row[16:])
    receipt = query(
        conn,
        "SELECT generation_id::text,input_observations,source_watermark "
        "FROM meta.asset_receipts WHERE asset_key=%s",
        (ASSET,),
    )[0]
    assert receipt[0] == result["generation_id"]
    assert receipt[1]["artifact_origin"] == "local_file"
    assert receipt[1]["dlt_load_ids"] == [row[16]]
    assert receipt[2] == result["sha"]
    assert query(
        conn,
        "SELECT count(*) FROM warehouse_source_stage._dlt_loads WHERE load_id=%s",
        (row[16],),
    ) == [(1,)]
    stage_name = f"warehouse_source_stage.sdv_ratings_{uuid.UUID(result['run_id']).hex}"
    assert query(
        conn,
        "SELECT pg_catalog.to_regclass(%s),"
        "pg_catalog.to_regnamespace('warehouse_source_stage_staging')",
        (stage_name,),
    ) == [(None, None)]
    assert query(
        conn,
        "SELECT source_url,status,row_count FROM meta.flat_file_loads",
    ) == [(None, "loaded", 1)]
