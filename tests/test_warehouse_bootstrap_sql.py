"""Executed catalog and access checks for the managed F06 warehouse bootstrap."""

import json
from pathlib import Path

import psycopg2
import pytest
from psycopg2 import sql

from scripts.warehouse_migrations import apply_manifest, load_manifest
from tests.test_warehouse_migrations_sql import query, warehouse_db  # noqa: F401

ROOT = Path(__file__).parents[1]
MANIFEST_PATH = ROOT / "src" / "schemas" / "warehouse-manifest.json"
CATALOG_PATH = ROOT / "src" / "schemas" / "baseline" / "20260907_catalog.json"
ROWS_PATH = ROOT / "tests" / "fixtures" / "warehouse" / "representative_rows.sql"
BASELINE_TARGET = "warehouse.baseline.ready.20260907"
CALLER_ROLES = ("anon", "authenticated", "analyst_ro")
PUBLIC_CALLER_ROLES = ("anon", "authenticated")
REGISTRY_TABLES = (
    "features.training_fits",
    "features.model_deployments",
    "features.model_deployment_history",
)
EXPECTED_LINE_SCORES = [
    (999900000001, 2099, 7, 0, 14, 3, 8, 0, 10, 7, 7, 3),
    (999900000002, 2099, None, 0, None, None, None, None, None, None, None, None),
]


def _query_as(conn, role, statement, values=None):
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
            cur.execute(statement, values)
            return cur.fetchall() if cur.description else None
    finally:
        conn.rollback()


def _assert_denied(
    conn,
    role,
    statement,
    values=None,
    error=psycopg2.errors.InsufficientPrivilege,
):
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
            with pytest.raises(error):
                cur.execute(statement, values)
    finally:
        conn.rollback()


def _assert_relations_queryable(conn, roles, relations):
    failures = []
    for role in roles:
        for schema_name, relation_name in relations:
            statement = sql.SQL("SELECT * FROM {}.{} LIMIT 1").format(
                sql.Identifier(schema_name), sql.Identifier(relation_name)
            )
            try:
                _query_as(conn, role, statement)
            except psycopg2.Error as exc:
                failures.append(f"{role}:{schema_name}.{relation_name}: {exc.diag.message_primary}")
    assert not failures, "caller relation checks failed:\n" + "\n".join(failures)


def _insert_representative_rows(conn):
    query(conn, ROWS_PATH.read_text())
    query(
        conn,
        """
        INSERT INTO features.model_metadata (
            model_version, train_through_season, ridge_alpha, winprob_ridge_alpha,
            platt_a, platt_b, train_seasons, n_train_games, feature_means,
            feature_diff_means, feature_diff_stds
        ) VALUES (
            'fixture-legacy-v0', 2098, 1.250, 2.500, 1.100000, -0.200000,
            ARRAY[2096, 2097, 2098]::bigint[], 42,
            '{"tempo": 1.5}'::jsonb, '{"d_tempo": 0.25}'::jsonb,
            '{"d_tempo": 2.0}'::jsonb
        );
        INSERT INTO features.model_coefficients (
            model_version, train_through_season, model_component,
            feature_order, feature_name, coefficient
        ) VALUES ('fixture-legacy-v0', 2098, 'margin', 0, 'intercept', 1.250000);
        """,
    )


def _fixture_snapshot(conn):
    return {
        "games": query(
            conn,
            "SELECT to_jsonb(g) FROM core.games g "
            "WHERE id BETWEEN 999900000001 AND 999900000002 ORDER BY id",
        ),
        "home_scores": query(
            conn,
            "SELECT to_jsonb(s) FROM core.games__home_line_scores s "
            "WHERE _dlt_root_id LIKE 'fixture-game-%' ORDER BY _dlt_parent_id, _dlt_list_idx",
        ),
        "away_scores": query(
            conn,
            "SELECT to_jsonb(s) FROM core.games__away_line_scores s "
            "WHERE _dlt_root_id LIKE 'fixture-game-%' ORDER BY _dlt_parent_id, _dlt_list_idx",
        ),
        "metadata": query(
            conn,
            "SELECT to_jsonb(m) FROM features.model_metadata m "
            "WHERE model_version='fixture-legacy-v0'",
        ),
        "coefficients": query(
            conn,
            "SELECT to_jsonb(c) FROM features.model_coefficients c "
            "WHERE model_version='fixture-legacy-v0'",
        ),
    }


def _assert_catalog_shape(conn):
    catalog = json.loads(CATALOG_PATH.read_text())
    expected_counts = {tuple(row[:2]): row[2] for row in catalog["object_counts"]}
    # F14 adds three private tables and six indexes; keep baseline evidence immutable.
    expected_counts[("meta", "r")] += 3
    expected_counts[("meta", "i")] += 6
    assert query(
        conn,
        """
        SELECT n.nspname, c.relkind, count(*)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY(%s)
        GROUP BY 1, 2
        ORDER BY 1, 2
        """,
        (catalog["schemas"],),
    ) == [(*key, count) for key, count in sorted(expected_counts.items())]

    assert query(
        conn,
        "SELECT count(*), bool_and(ispopulated) FROM pg_matviews WHERE schemaname = ANY(%s)",
        (catalog["schemas"],),
    ) == [(60, True)]
    assert query(
        conn,
        "SELECT extname FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace "
        "WHERE n.nspname='public' ORDER BY extname",
    ) == [("btree_gist",), ("fuzzystrmatch",), ("pg_trgm",), ("vector",)]
    assert query(
        conn,
        """
        SELECT parent_ns.nspname, parent.relname, child_ns.nspname, child.relname,
               pg_get_expr(child.relpartbound, child.oid)
        FROM pg_inherits
        JOIN pg_class parent ON parent.oid = inhparent
        JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
        JOIN pg_class child ON child.oid = inhrelid
        JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
        WHERE parent_ns.nspname='core' AND parent.relname='plays'
          AND child_ns.nspname='core' AND child.relname='plays_y2026'
        """,
    ) == [("core", "plays", "core", "plays_y2026", "FOR VALUES IN ('2026')")]


def _assert_dlt_shape_and_static_seeds(conn):
    expected_columns = [
        ("betting", "lines", "spread", "double precision", "YES"),
        ("core", "games", "id", "bigint", "NO"),
        ("core", "games__home_line_scores", "_dlt_id", "character varying", "NO"),
        (
            "core",
            "games__home_line_scores",
            "_dlt_list_idx",
            "bigint",
            "NO",
        ),
        (
            "core",
            "games__home_line_scores",
            "_dlt_parent_id",
            "character varying",
            "NO",
        ),
        (
            "core",
            "games__home_line_scores",
            "_dlt_root_id",
            "character varying",
            "NO",
        ),
        ("core", "games__home_line_scores", "value", "bigint", "YES"),
        (
            "metrics",
            "ppa_teams",
            "defense__first_down",
            "bigint",
            "YES",
        ),
        (
            "metrics",
            "ppa_teams",
            "defense__first_down__v_double",
            "double precision",
            "YES",
        ),
        (
            "metrics",
            "pregame_win_probability",
            "spread__v_double",
            "double precision",
            "YES",
        ),
        ("recruiting", "recruits", "height", "bigint", "YES"),
        (
            "recruiting",
            "recruits",
            "height__v_double",
            "double precision",
            "YES",
        ),
    ]
    assert (
        query(
            conn,
            """
        SELECT table_schema, table_name, column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE (table_schema, table_name, column_name) IN (
            ('betting','lines','spread'),
            ('core','games','id'),
            ('core','games__home_line_scores','value'),
            ('core','games__home_line_scores','_dlt_root_id'),
            ('core','games__home_line_scores','_dlt_parent_id'),
            ('core','games__home_line_scores','_dlt_list_idx'),
            ('core','games__home_line_scores','_dlt_id'),
            ('metrics','ppa_teams','defense__first_down'),
            ('metrics','ppa_teams','defense__first_down__v_double'),
            ('metrics','pregame_win_probability','spread__v_double'),
            ('recruiting','recruits','height'),
            ('recruiting','recruits','height__v_double')
        )
        ORDER BY 1, 2, 3
        """,
        )
        == expected_columns
    )

    xwalk_seed_count = sum(
        line.startswith("INSERT INTO ref.team_name_xwalk")
        for line in (ROOT / "src/schemas/baseline/20260907_static_seeds.sql")
        .read_text()
        .splitlines()
    )
    assert query(
        conn,
        "SELECT (SELECT count(*) FROM ref.eras), "
        "(SELECT count(*) FROM pff.team_map), "
        "(SELECT count(*) FROM ref.team_name_xwalk)",
    ) == [(4, 137, xwalk_seed_count)]
    assert query(conn, "SELECT * FROM ref.get_era(2024) ORDER BY era_code") == [
        ("PLAYOFF_V2", "Playoff V2"),
        ("PORTAL_NIL", "Portal/NIL Era"),
    ]


def _assert_fixture_and_access(conn):
    assert (
        query(
            conn,
            """
        SELECT game_id, season, home_q1, home_q2, home_q3, home_q4, home_ot,
               away_q1, away_q2, away_q3, away_q4, away_ot
        FROM api.game_line_scores
        WHERE game_id BETWEEN 999900000001 AND 999900000002
        ORDER BY game_id
        """,
        )
        == EXPECTED_LINE_SCORES
    )
    assert query(
        conn,
        """
        SELECT count(*), count(DISTINCT (child_table, _dlt_parent_id, _dlt_list_idx)),
               bool_and(_dlt_root_id = _dlt_parent_id)
        FROM (
            SELECT 'home' AS child_table, _dlt_root_id, _dlt_parent_id, _dlt_list_idx
            FROM core.games__home_line_scores WHERE _dlt_root_id LIKE 'fixture-game-%'
            UNION ALL
            SELECT 'away' AS child_table, _dlt_root_id, _dlt_parent_id, _dlt_list_idx
            FROM core.games__away_line_scores WHERE _dlt_root_id LIKE 'fixture-game-%'
        ) nested
        """,
    ) == [(14, 14, True)]

    api_views = query(
        conn,
        "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n "
        "ON n.oid=c.relnamespace WHERE n.nspname='api' AND c.relkind='v' ORDER BY 2",
    )
    public_views = query(
        conn,
        "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n "
        "ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='v' "
        "AND c.reloptions @> ARRAY['security_invoker=true'] ORDER BY 2",
    )
    assert len(api_views) == 54
    assert len(public_views) == 13
    _assert_relations_queryable(conn, CALLER_ROLES, api_views)
    _assert_relations_queryable(conn, PUBLIC_CALLER_ROLES, public_views)

    for role in CALLER_ROLES:
        _assert_denied(conn, role, "SELECT * FROM warehouse_control.schema_migrations")
        _assert_denied(conn, role, "SELECT * FROM scouting.players")
    for role in (*CALLER_ROLES, "warehouse_ingest"):
        for table in ("operation_runs", "api_quota_periods", "api_request_attempts"):
            _assert_denied(conn, role, f"SELECT * FROM meta.{table}")
    for role in ("anon", "authenticated"):
        assert _query_as(conn, role, "SELECT count(*) FROM features.training_fits") == [(0,)]
        assert _query_as(conn, role, "SELECT count(*) FROM marts.team_season_trajectory") == [(0,)]
        _assert_denied(conn, role, "DELETE FROM features.training_fits")
        _assert_denied(conn, role, "TRUNCATE features.training_fits CASCADE")
        analyst_result = _query_as(
            conn,
            role,
            "SELECT public.run_analyst_query(%s)",
            ("SELECT game_id, home_q2 FROM api.game_line_scores WHERE game_id=999900000001",),
        )
        assert analyst_result == [([{"game_id": 999900000001, "home_q2": 0}],)]
        assert (
            _query_as(
                conn,
                role,
                "SELECT * FROM public.get_player_detail('fixture-missing', 2099)",
            )
            == []
        )
    _assert_denied(conn, "analyst_ro", "SELECT * FROM features.training_fits")
    _assert_denied(conn, "analyst_ro", "SELECT * FROM marts.team_season_trajectory")
    for role in CALLER_ROLES:
        _assert_denied(
            conn,
            role,
            "SELECT public.run_analyst_query('SELECT * FROM scouting.players')",
            error=psycopg2.errors.InsufficientPrivilege,
        )
        _assert_denied(
            conn,
            role,
            "SELECT public.run_analyst_query('DELETE FROM api.game_line_scores')",
            error=psycopg2.errors.RaiseException,
        )


@pytest.mark.parametrize("bootstrap_path", ["fresh", "prior-baseline-upgrade"])
def test_managed_warehouse_catalog_data_and_access(
    warehouse_db,  # noqa: F811
    bootstrap_path,
):
    conn, _ = warehouse_db
    manifest = load_manifest(MANIFEST_PATH, ROOT)
    initial_search_path = query(conn, "SHOW search_path")

    if bootstrap_path == "fresh":
        plan = apply_manifest(conn, manifest, mode="bootstrap")
        assert [step.id for step in plan.pending] == [m.id for m in manifest.migrations]
        _insert_representative_rows(conn)
    else:
        baseline = apply_manifest(conn, manifest, mode="bootstrap", target=BASELINE_TARGET)
        assert [step.id for step in baseline.pending] == [
            migration.id for migration in manifest.migrations[:4]
        ]
        assert query(
            conn,
            "SELECT to_regclass('features.training_fits'), "
            "to_regclass('features.model_deployments'), "
            "to_regclass('features.model_deployment_history')",
        ) == [(None, None, None)]
        _insert_representative_rows(conn)
        before_upgrade = _fixture_snapshot(conn)
        upgrade = apply_manifest(conn, manifest, mode="upgrade")
        assert len(upgrade.pending) == 3
        assert [step.id for step in upgrade.pending] == [
            migration.id for migration in manifest.migrations[4:]
        ]
        assert _fixture_snapshot(conn) == before_upgrade

    assert query(conn, "SHOW search_path") == initial_search_path
    assert query(conn, "SHOW row_security") == [("on",)]
    assert query(
        conn,
        "SELECT to_regclass('features.training_fits'), "
        "to_regclass('features.model_deployments'), "
        "to_regclass('features.model_deployment_history')",
    ) == [
        (
            "features.training_fits",
            "features.model_deployments",
            "features.model_deployment_history",
        )
    ]

    second = apply_manifest(conn, manifest, mode="upgrade")
    assert not second.pending
    assert [step.action for step in second.steps] == ["skip"] * len(manifest.migrations)

    _assert_catalog_shape(conn)
    _assert_dlt_shape_and_static_seeds(conn)
    _assert_fixture_and_access(conn)
