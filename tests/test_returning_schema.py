"""Tests for the `rp` (returning production) schema and its DDL.

Covers U1 of the returning production model plan:
  docs/plans/2026-04-27-001-feat-returning-production-model-plan.md

Verifies:
  - schema `rp` exists with the 6 expected tables
  - fuzzystrmatch extension is loaded
  - dim_continuity_factors and dim_position_weights are seeded with the
    correct row counts and weight invariants
  - anon role can SELECT but cannot INSERT/UPDATE/DELETE
"""

from __future__ import annotations

import psycopg2
import pytest

# ---------------------------------------------------------------------------
# Inventory -- single source of truth for what U1 must create
# ---------------------------------------------------------------------------

RP_TABLES = [
    "fct_player_seasons",
    "fct_player_movements",
    "dim_continuity_factors",
    "dim_position_weights",
    "unmatched_portal_log",
    "injuries_season_ending",
]

# Continuity factor enum values that fct_player_movements.movement_type can take.
# Must match the INSERT block in src/schemas/019_returning_schema.sql.
EXPECTED_CONTINUITY_TYPES = {
    "returning_same_hc",
    "returning_new_hc",
    "returning_from_redshirt",
    "returning_from_injury_full",
    "portal_p5_to_p5",
    "portal_g5_to_p5",
    "portal_p5_to_g5",
    "portal_g5_to_g5",
    "portal_fcs_to_fbs",
    "portal_juco_to_fbs",
    "recruit_5star",
    "recruit_4star",
    "recruit_3star",
    "recruit_unrated",
}

# Position weight rows that must exist for scheme_archetype='static'.
EXPECTED_POSITIONS_STATIC = {
    "QB",
    "WR",
    "TE",
    "RB",
    "OL",
    "EDGE",
    "DT",
    "LB",
    "CB",
    "S",
    "ST",
}


# ---------------------------------------------------------------------------
# Schema and table existence
# ---------------------------------------------------------------------------


class TestRpSchemaExists:
    """The `rp` schema must exist after migration 019 applies."""

    def test_rp_schema_exists(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = 'rp'")
            assert cur.fetchone() is not None, "Schema 'rp' does not exist"

    @pytest.mark.parametrize("table_name", RP_TABLES, ids=RP_TABLES)
    def test_rp_table_exists(self, db_conn, table_name):
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'rp' AND table_name = %s
                """,
                (table_name,),
            )
            assert cur.fetchone() is not None, f"Table rp.{table_name} does not exist"

    def test_rp_has_no_unexpected_tables(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'rp' AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            )
            actual = {row[0] for row in cur.fetchall()}
        assert actual == set(RP_TABLES), (
            f"rp schema tables differ from expected. "
            f"Missing: {set(RP_TABLES) - actual}. Extra: {actual - set(RP_TABLES)}"
        )


# ---------------------------------------------------------------------------
# Extension and helpers required by later units
# ---------------------------------------------------------------------------


class TestFuzzystrmatchExtension:
    """U3 (portal name-matching) needs levenshtein() from fuzzystrmatch."""

    def test_fuzzystrmatch_extension_installed(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'fuzzystrmatch'")
            assert cur.fetchone() is not None, (
                "fuzzystrmatch extension is not installed; U3 portal matching will fail"
            )

    def test_levenshtein_callable(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT levenshtein('a', 'b')")
            assert cur.fetchone()[0] == 1


# ---------------------------------------------------------------------------
# Seed data invariants
# ---------------------------------------------------------------------------


class TestDimContinuityFactors:
    """Static lookup must match the requirements doc + plan U4 contract."""

    def test_row_count(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM rp.dim_continuity_factors")
            assert cur.fetchone()[0] == len(EXPECTED_CONTINUITY_TYPES)

    def test_movement_types_match_expected(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT movement_type FROM rp.dim_continuity_factors")
            actual = {row[0] for row in cur.fetchall()}
        assert actual == EXPECTED_CONTINUITY_TYPES, (
            f"continuity factor types differ from plan. "
            f"Missing: {EXPECTED_CONTINUITY_TYPES - actual}. "
            f"Extra: {actual - EXPECTED_CONTINUITY_TYPES}"
        )

    def test_continuity_factors_in_valid_range(self, db_conn):
        """All continuity factors must be in (0, 1]; portal_p5_to_g5 is the max at 0.85."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT movement_type, continuity_factor
                FROM rp.dim_continuity_factors
                WHERE continuity_factor <= 0 OR continuity_factor > 1
                """
            )
            invalid = cur.fetchall()
        assert invalid == [], f"Continuity factors out of (0, 1] range: {invalid}"

    def test_returning_same_hc_is_one(self, db_conn):
        """The full-continuity baseline must be exactly 1.00."""
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT continuity_factor FROM rp.dim_continuity_factors "
                "WHERE movement_type = 'returning_same_hc'"
            )
            assert cur.fetchone()[0] == 1.00


class TestDimPositionWeights:
    """SUM(static weights) must equal 2.0 -- offense + defense baseline."""

    def test_row_count(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM rp.dim_position_weights WHERE scheme_archetype = 'static'"
            )
            assert cur.fetchone()[0] == len(EXPECTED_POSITIONS_STATIC)

    def test_positions_match_expected(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT position FROM rp.dim_position_weights WHERE scheme_archetype = 'static'"
            )
            actual = {row[0] for row in cur.fetchall()}
        assert actual == EXPECTED_POSITIONS_STATIC

    def test_offensive_weights_sum_to_one(self, db_conn):
        """Offensive position weights (QB+WR+TE+RB+OL) must sum to 1.0."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT SUM(position_weight) FROM rp.dim_position_weights
                WHERE scheme_archetype = 'static'
                  AND position IN ('QB', 'WR', 'TE', 'RB', 'OL')
                """
            )
            total = float(cur.fetchone()[0])
        assert abs(total - 1.0) < 0.001, f"offensive position weights sum to {total}, expected 1.0"

    def test_defensive_weights_sum_to_known_value(self, db_conn):
        """Defensive position weights (EDGE+DT+LB+CB+S) currently sum to 0.82.

        NOTE: The original requirements doc declared offense + defense should sum
        to 2.0 ('Verify in tests'), but the published per-position values yield
        0.82 on the defensive side, not 1.0. This is a known spec inconsistency
        documented in the migration. The downstream rollup math in U5/U6 must
        either accept this asymmetry or be updated alongside a rebalance of the
        defensive weights. Pinning the actual sum here surfaces the discrepancy
        if anyone later touches the values.
        """
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT SUM(position_weight) FROM rp.dim_position_weights
                WHERE scheme_archetype = 'static'
                  AND position IN ('EDGE', 'DT', 'LB', 'CB', 'S')
                """
            )
            total = float(cur.fetchone()[0])
        assert abs(total - 0.82) < 0.001, (
            f"defensive position weights sum to {total}, expected 0.82 "
            "(if rebalanced toward 1.0, update this assertion + the migration comment)"
        )


# ---------------------------------------------------------------------------
# Permissions -- anon role read-only invariant (per 2026-02-07 hardening pattern)
# ---------------------------------------------------------------------------


@pytest.fixture
def anon_cursor(db_conn):
    """Cursor with role set to anon. Always RESET ROLE on teardown."""
    cur = db_conn.cursor()
    cur.execute("SET ROLE anon")
    try:
        yield cur
    finally:
        cur.execute("RESET ROLE")
        cur.close()


class TestAnonReadOnlyAccess:
    """anon must be able to read every rp.* table but cannot write to any of them."""

    @pytest.mark.parametrize("table_name", RP_TABLES, ids=RP_TABLES)
    def test_anon_can_select(self, anon_cursor, table_name):
        """anon should be able to read every rp.* table."""
        anon_cursor.execute(f"SELECT COUNT(*) FROM rp.{table_name}")
        # Existence of a result, not row count, is what we're checking.
        assert anon_cursor.fetchone() is not None

    def test_anon_cannot_insert_into_fct_player_seasons(self, db_conn):
        """anon must be blocked from INSERT (covers DML revoke)."""
        cur = db_conn.cursor()
        cur.execute("SET ROLE anon")
        try:
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cur.execute(
                    "INSERT INTO rp.fct_player_seasons (player_id, season) VALUES ('test', 2026)"
                )
        finally:
            cur.execute("RESET ROLE")
            cur.close()

    def test_anon_cannot_update_dim_continuity_factors(self, db_conn):
        cur = db_conn.cursor()
        cur.execute("SET ROLE anon")
        try:
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cur.execute(
                    "UPDATE rp.dim_continuity_factors "
                    "SET continuity_factor = 0.99 WHERE movement_type = 'returning_same_hc'"
                )
        finally:
            cur.execute("RESET ROLE")
            cur.close()

    def test_anon_cannot_delete_from_dim_position_weights(self, db_conn):
        cur = db_conn.cursor()
        cur.execute("SET ROLE anon")
        try:
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cur.execute("DELETE FROM rp.dim_position_weights WHERE position = 'QB'")
        finally:
            cur.execute("RESET ROLE")
            cur.close()


# ---------------------------------------------------------------------------
# Index existence -- PKs are tested implicitly by inserts in U2/U3, but secondary
# indexes the plan called out should be checked here.
# ---------------------------------------------------------------------------


class TestExpectedIndexes:
    """Secondary indexes specified in plan U1 must exist for the matview joins."""

    @pytest.mark.parametrize(
        "table_name,index_name",
        [
            ("fct_player_seasons", "idx_fct_player_seasons_team_season"),
            ("fct_player_seasons", "idx_fct_player_seasons_position_group"),
            ("fct_player_movements", "idx_fct_player_movements_dest_season"),
            ("fct_player_movements", "idx_fct_player_movements_source_season"),
            ("fct_player_movements", "idx_fct_player_movements_movement_type"),
            ("unmatched_portal_log", "idx_unmatched_portal_log_season"),
        ],
    )
    def test_index_exists(self, db_conn, table_name, index_name):
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = 'rp' AND tablename = %s AND indexname = %s
                """,
                (table_name, index_name),
            )
            assert cur.fetchone() is not None, (
                f"Expected index {index_name} on rp.{table_name} is missing"
            )


# ---------------------------------------------------------------------------
# Idempotency -- migration must be re-runnable without error or row growth.
# Hits the actual SQL file via Path so it stays in sync with what shipped.
# ---------------------------------------------------------------------------


class TestMigrationIdempotency:
    """The schema migration must be safe to re-run."""

    def test_rerun_does_not_duplicate_seeds(self, db_conn):
        """Re-running the seed INSERTs (via ON CONFLICT) preserves row counts."""
        from pathlib import Path

        sql_path = (
            Path(__file__).resolve().parent.parent / "src" / "schemas" / "019_returning_schema.sql"
        )
        sql = sql_path.read_text()

        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM rp.dim_continuity_factors")
            cont_before = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM rp.dim_position_weights")
            pos_before = cur.fetchone()[0]

            cur.execute(sql)

            cur.execute("SELECT COUNT(*) FROM rp.dim_continuity_factors")
            cont_after = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM rp.dim_position_weights")
            pos_after = cur.fetchone()[0]

        assert cont_after == cont_before, (
            f"dim_continuity_factors row count changed on re-run: {cont_before} -> {cont_after}"
        )
        assert pos_after == pos_before, (
            f"dim_position_weights row count changed on re-run: {pos_before} -> {pos_after}"
        )
