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
    "adjusted_epa_week",
    "coach_record",
    "coach_tenures",
    "coaching_tenure",
    "conference_comparison",
    "conference_era_summary",
    "conference_head_to_head",
    "core_ratings",
    "data_freshness",
    "defensive_havoc",
    "epa_crossvalidation",
    "matchup_edges",
    "matchup_history",
    "passing_charting_player_season",
    "passing_charting_target_season",
    "passing_charting_team_season",
    "penalty_log",
    "play_epa",
    "player_comparison",
    "player_game_epa",
    "player_season_epa",
    "recruiting_class",
    "recruiting_roi",
    "rushing_charting_direction_season",
    "rushing_charting_player_season",
    "rushing_charting_team_season",
    "scoring_opportunities",
    "situational_splits",
    "team_adjusted_epa",
    "team_epa_season",
    "team_penalty_box",
    "team_playcalling_tendencies",
    "team_season_summary",
    "team_season_trajectory",
    "team_situational_success",
    "team_style_profile",
    "team_talent_composite",
    "team_tempo_metrics",
    "team_wepa_season",
    "team_week_features",
    "player_wepa_season",
    "returning_production",
    "player_usage",
    "team_ats_records",
    "transfer_portal_impact",
    "house_elo",
    "house_elo_game",
    "scored_matchup_edges",
    "prediction_accuracy",
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

    # Legitimately empty out of season / before the schedule loads (documented
    # in the mart headers) -- existence and columns are still asserted above.
    #
    # epa_crossvalidation is empty until BOTH a house adjusted-EPA fit and at
    # least one external ratings table have rows. Two of its three external
    # anchors (ratings.sdv_ratings_weekly, ratings.espn_fpi_weekly) ship dark
    # and only fill once the sportsdataverse flat-file sources first run, so a
    # freshly provisioned database legitimately has zero rows here. The
    # per-system coverage assertions live in TestEpaCrossvalidation below,
    # which skip rather than fail on a dark system -- an untested anchor is
    # UNTESTABLE, not a negative result.
    EMPTY_OK = {
        ("marts", "scored_matchup_edges"),
        ("marts", "epa_crossvalidation"),
        # ref.coach_tenures is a per-team fan-out resource, deliberately
        # excluded from the daily/incremental path (coaches.py's
        # cfbd_coach_tenures source) -- an environment that hasn't yet run
        # the backfill (`--source coach_tenures`) legitimately has zero rows
        # here. See marts/048_coach_tenures.sql's header.
        ("marts", "coach_tenures"),
        # 2026-09-03 rushing-charting unit (U6), KTD8: these three marts are
        # created fresh in this unit and must stay green on a pre-backfill
        # database (Stage A's `rushing` source has not necessarily loaded
        # yet on every environment that applies this file). Row-count floors
        # replace this entry once the backfill is verified (U9). See
        # marts/050_rushing_charting_player_season.sql's header.
        ("marts", "rushing_charting_player_season"),
        ("marts", "rushing_charting_team_season"),
        ("marts", "rushing_charting_direction_season"),
    }

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
        if (schema_name, view_name) in self.EMPTY_OK and count == 0:
            pytest.skip(f"{schema_name}.{view_name} legitimately empty out of season")
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
        # CORE ratings (2016+; NULL = not-rated). Added 2026-08-08.
        "core_overall",
        "core_offense",
        "core_defense",
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


class TestEpaCrossvalidation:
    """Verify marts.epa_crossvalidation (the refresh-campaign plausibility harness).

    Deliberately tolerant of empty external anchors: ratings.sdv_ratings_weekly
    and ratings.espn_fpi_weekly land dark with the sportsdataverse flat-file
    sources and only fill once those first run. Every assertion below either
    holds vacuously on zero rows or skips explicitly -- a dark anchor must not
    be reported as a passing comparison OR as a failure.
    """

    EXPECTED_COLUMNS = {
        # Grain + provenance
        "season",
        "external_system",
        "team",
        "external_metric",
        "external_team_id",
        "external_through_week",
        # House series (EPA/play scale)
        "house_net_adj_epa",
        "house_off_adj_epa",
        "house_def_adj_epa",
        "house_epa_per_play",
        "house_plays",
        "house_games",
        # External series (each system's own scale)
        "external_value",
        "external_rank_source",
        # Per-team cross-scale-safe comparison
        "house_net_rank",
        "house_epa_rank",
        "external_rank",
        "house_net_z",
        "external_z",
        "rank_delta",
        # Per-(season, external_system) harness statistics
        "matched_teams",
        "corr_pairs",
        "house_teams",
        "external_teams",
        "coverage_pct",
        "spearman_net_adj_epa",
        "spearman_epa_per_play",
        "pearson_net_adj_epa",
        "mean_abs_rank_delta",
        "source_rank_agreement",
    }

    KNOWN_SYSTEMS = {"cfbd_fpi_season", "sdv_adj_net", "espn_fpi_weekly"}

    def test_has_all_expected_columns(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname
                FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'marts'
                  AND c.relname = 'epa_crossvalidation'
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                """,
            )
            actual_columns = {row[0] for row in cur.fetchall()}

        missing = self.EXPECTED_COLUMNS - actual_columns
        assert not missing, f"marts.epa_crossvalidation missing columns: {missing}"

    def test_grain_is_unique(self, db_conn):
        """(season, external_system, team) must be unique -- it is the REFRESH
        CONCURRENTLY key, and a duplicate means a team-name collision leaked
        through the per-system DISTINCT ON de-duplication."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT season, external_system, team
                    FROM marts.epa_crossvalidation
                    GROUP BY season, external_system, team
                    HAVING COUNT(*) > 1
                ) dups
                """
            )
            dup_keys = cur.fetchone()[0]
        assert dup_keys == 0, f"{dup_keys} duplicate (season, external_system, team) keys"

    def test_only_known_external_systems(self, db_conn):
        """Guards against a typo'd or silently renamed system label, which
        would make a before/after comparison join to nothing."""
        with db_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT external_system FROM marts.epa_crossvalidation")
            systems = {row[0] for row in cur.fetchall()}
        unknown = systems - self.KNOWN_SYSTEMS
        assert not unknown, f"Unexpected external_system values: {unknown}"

    def test_correlations_within_unit_interval(self, db_conn):
        """Every correlation column must lie in [-1, 1]. A value outside that
        range means the statistic is not the correlation it claims to be."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM marts.epa_crossvalidation
                WHERE spearman_net_adj_epa  NOT BETWEEN -1 AND 1
                   OR spearman_epa_per_play NOT BETWEEN -1 AND 1
                   OR pearson_net_adj_epa   NOT BETWEEN -1 AND 1
                   OR source_rank_agreement NOT BETWEEN -1 AND 1
                """
            )
            bad_rows = cur.fetchone()[0]
        assert bad_rows == 0, f"{bad_rows} rows with an out-of-range correlation"

    def test_ranks_and_coverage_are_sane(self, db_conn):
        """Ranks start at 1, and the matched set can never exceed the external
        panel it is drawn from (coverage_pct > 100 is a join fan-out)."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM marts.epa_crossvalidation
                WHERE external_rank < 1
                   OR house_net_rank < 1
                   OR house_epa_rank < 1
                   OR matched_teams > external_teams
                   OR coverage_pct > 100
                """
            )
            bad_rows = cur.fetchone()[0]
        assert bad_rows == 0, f"{bad_rows} rows with an impossible rank or coverage value"

    def test_external_sign_conventions_hold(self, db_conn):
        """Self-check on the higher-is-better assumption for each external value
        column: our derived external_rank must agree with the rank the source
        publishes itself. Near -1 means the sign assumption in the mart is
        inverted (sdv_ratings_weekly.adj_net in particular has no documented
        upstream sign convention). Systems with no rows are skipped, not
        assumed correct."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT external_system, MIN(source_rank_agreement)
                FROM marts.epa_crossvalidation
                WHERE source_rank_agreement IS NOT NULL
                GROUP BY external_system
                """
            )
            agreements = dict(cur.fetchall())

        if not agreements:
            pytest.skip(
                "marts.epa_crossvalidation has no rows with a published source rank "
                "(external ratings tables not loaded yet) -- sign check UNTESTABLE"
            )

        inverted = {s: float(v) for s, v in agreements.items() if float(v) < 0}
        assert not inverted, (
            f"Derived external_rank disagrees with the source's own published rank "
            f"for {inverted} -- the higher-is-better assumption on that system's "
            f"value column is inverted in 044_epa_crossvalidation.sql"
        )
