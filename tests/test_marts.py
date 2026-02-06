"""Tests for materialized views in marts, analytics, and scouting schemas.

Verifies that all materialized views exist in the database, contain data,
and that key marts have the expected column structure.
"""

import pytest

# ---------------------------------------------------------------------------
# View inventory
# ---------------------------------------------------------------------------

MARTS_VIEWS = [
    "_game_epa_calc",
    "coach_record",
    "coaching_tenure",
    "conference_comparison",
    "conference_era_summary",
    "conference_head_to_head",
    "data_freshness",
    "defensive_havoc",
    "matchup_edges",
    "matchup_history",
    "play_epa",
    "player_comparison",
    "player_game_epa",
    "player_season_epa",
    "recruiting_class",
    "recruiting_roi",
    "scoring_opportunities",
    "situational_splits",
    "team_epa_season",
    "team_playcalling_tendencies",
    "team_season_summary",
    "team_season_trajectory",
    "team_situational_success",
    "team_style_profile",
    "team_talent_composite",
    "team_tempo_metrics",
    "transfer_portal_impact",
]

ANALYTICS_VIEWS = [
    "conference_standings",
    "game_results",
    "player_career_stats",
    "team_recruiting_trend",
    "team_season_summary",
]

SCOUTING_VIEWS = [
    "player_mart",
]

ALL_MATERIALIZED_VIEWS = (
    [("marts", v) for v in MARTS_VIEWS]
    + [("analytics", v) for v in ANALYTICS_VIEWS]
    + [("scouting", v) for v in SCOUTING_VIEWS]
)


# ---------------------------------------------------------------------------
# Existence tests
# ---------------------------------------------------------------------------


class TestMartViewsExist:
    """Every expected materialized view must be present in pg_matviews."""

    @pytest.mark.parametrize(
        "schema_name,view_name",
        ALL_MATERIALIZED_VIEWS,
        ids=[f"{s}.{v}" for s, v in ALL_MATERIALIZED_VIEWS],
    )
    def test_view_exists(self, db_conn, schema_name, view_name):
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM pg_matviews
                WHERE schemaname = %s AND matviewname = %s
                """,
                (schema_name, view_name),
            )
            result = cur.fetchone()
        assert result is not None, f"Materialized view {schema_name}.{view_name} does not exist"


# ---------------------------------------------------------------------------
# Row count tests
# ---------------------------------------------------------------------------


class TestMartViewsHaveData:
    """Every materialized view should contain at least one row."""

    @pytest.mark.parametrize(
        "schema_name,view_name",
        ALL_MATERIALIZED_VIEWS,
        ids=[f"{s}.{v}" for s, v in ALL_MATERIALIZED_VIEWS],
    )
    def test_view_has_rows(self, db_conn, schema_name, view_name):
        with db_conn.cursor() as cur:
            # Use quoted identifiers to handle leading underscores safely
            cur.execute(f'SELECT COUNT(*) FROM "{schema_name}"."{view_name}"')
            count = cur.fetchone()[0]
        assert count > 0, f"{schema_name}.{view_name} is empty (0 rows)"


# ---------------------------------------------------------------------------
# Column structure tests for key marts
# ---------------------------------------------------------------------------


class TestTeamSeasonSummaryColumns:
    """Verify marts.team_season_summary has the expected column set."""

    EXPECTED_COLUMNS = {
        "team",
        "conference",
        "season",
        "games",
        "wins",
        "losses",
        "conf_wins",
        "conf_losses",
        "ppg",
        "opp_ppg",
        "avg_margin",
        "sp_rating",
        "sp_rank",
        "sp_offense",
        "sp_defense",
        "elo",
        "fpi",
        "recruiting_rank",
        "recruiting_points",
    }

    def test_has_all_expected_columns(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname
                FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'marts'
                  AND c.relname = 'team_season_summary'
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                """,
            )
            actual_columns = {row[0] for row in cur.fetchall()}

        missing = self.EXPECTED_COLUMNS - actual_columns
        assert not missing, f"marts.team_season_summary missing columns: {missing}"

    def test_has_team_column(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT team FROM marts.team_season_summary LIMIT 1")
            row = cur.fetchone()
        assert row is not None and row[0] is not None

    def test_has_season_column(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT season FROM marts.team_season_summary LIMIT 1")
            row = cur.fetchone()
        assert row is not None and row[0] is not None


class TestTeamEpaSeasonColumns:
    """Verify marts.team_epa_season has the expected column set."""

    EXPECTED_COLUMNS = {
        "team",
        "season",
        "epa_per_play",
        "success_rate",
        "explosiveness",
        "epa_tier",
        "total_plays",
        "games_played",
    }

    def test_has_all_expected_columns(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname
                FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'marts'
                  AND c.relname = 'team_epa_season'
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                """,
            )
            actual_columns = {row[0] for row in cur.fetchall()}

        missing = self.EXPECTED_COLUMNS - actual_columns
        assert not missing, f"marts.team_epa_season missing columns: {missing}"

    def test_epa_per_play_is_numeric(self, db_conn):
        """EPA per play should be a numeric value, not null for populated rows."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT epa_per_play
                FROM marts.team_epa_season
                WHERE epa_per_play IS NOT NULL
                LIMIT 1
                """
            )
            row = cur.fetchone()
        assert row is not None, "No non-null epa_per_play values found"

    def test_epa_tier_values(self, db_conn):
        """EPA tier should be a categorized label, not empty."""
        with db_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT epa_tier FROM marts.team_epa_season")
            tiers = {row[0] for row in cur.fetchall()}
        assert len(tiers) > 1, "Expected multiple EPA tier categories"


class TestGameEpaCalcColumns:
    """Verify marts._game_epa_calc has the expected column set."""

    EXPECTED_COLUMNS = {
        "game_id",
        "team",
        "epa_per_play",
        "success_rate",
        "explosiveness",
        "plays_non_garbage",
        "plays_total",
    }

    def test_has_all_expected_columns(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname
                FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'marts'
                  AND c.relname = '_game_epa_calc'
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                """,
            )
            actual_columns = {row[0] for row in cur.fetchall()}

        missing = self.EXPECTED_COLUMNS - actual_columns
        assert not missing, f"marts._game_epa_calc missing columns: {missing}"

    def test_game_id_is_populated(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT game_id FROM marts._game_epa_calc LIMIT 1")
            row = cur.fetchone()
        assert row is not None and row[0] is not None

    def test_plays_total_positive(self, db_conn):
        """Every game-team row should have at least one play."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM marts._game_epa_calc
                WHERE plays_total <= 0 OR plays_total IS NULL
                """
            )
            bad_rows = cur.fetchone()[0]
        assert bad_rows == 0, f"Found {bad_rows} rows with non-positive plays_total"
