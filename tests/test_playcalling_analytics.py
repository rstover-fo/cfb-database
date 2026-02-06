"""Tests for play-calling tendencies analytics.

Verifies marts.team_playcalling_tendencies, marts.team_situational_success,
and api.team_playcalling_profile views exist, contain valid data, and
enforce expected constraints (score diff buckets, min play threshold, etc.).
"""


def _fetch_all(conn, query, params=None):
    """Execute a query and return (rows, column_names)."""
    with conn.cursor() as cur:
        cur.execute(query, params)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    return rows, columns


def _fetch_one(conn, query, params=None):
    """Execute a query and return the first row."""
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()


def _fetch_count(conn, query, params=None):
    """Execute a COUNT query and return the integer result."""
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# TestTeamPlaycallingTendencies
# ---------------------------------------------------------------------------


class TestTeamPlaycallingTendencies:
    """Tests for marts.team_playcalling_tendencies matview."""

    def test_exists_and_has_data(self, db_conn):
        count = _fetch_count(db_conn, "SELECT COUNT(*) FROM marts.team_playcalling_tendencies")
        assert count > 0, "tendencies matview should have rows"

    def test_score_diff_buckets_valid(self, db_conn):
        """All score_diff_bucket values in expected set."""
        rows, _ = _fetch_all(
            db_conn,
            "SELECT DISTINCT score_diff_bucket FROM marts.team_playcalling_tendencies",
        )
        actual = {r[0] for r in rows}
        expected = {"big_lead", "small_lead", "tied", "small_deficit", "big_deficit"}
        assert actual == expected

    def test_run_rate_shift(self, db_conn):
        """League-wide avg run_rate for big_lead > avg run_rate for big_deficit."""
        row = _fetch_one(
            db_conn,
            """
            SELECT
                AVG(run_rate) FILTER (WHERE score_diff_bucket = 'big_lead') AS lead_rate,
                AVG(run_rate) FILTER (WHERE score_diff_bucket = 'big_deficit') AS deficit_rate
            FROM marts.team_playcalling_tendencies
            WHERE season >= 2014
            """,
        )
        assert row[0] > row[1], f"big_lead run_rate ({row[0]}) should exceed big_deficit ({row[1]})"

    def test_no_zero_total_plays(self, db_conn):
        """No rows with total_plays < 1."""
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM marts.team_playcalling_tendencies WHERE total_plays < 1",
        )
        assert count == 0

    def test_run_pass_sum(self, db_conn):
        """rush_plays + pass_plays = total_plays for all rows."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.team_playcalling_tendencies
            WHERE rush_plays + pass_plays != total_plays
            """,
        )
        assert count == 0

    def test_all_fbs_teams_present(self, db_conn):
        """At least 100 distinct teams per recent season."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(DISTINCT team)
            FROM marts.team_playcalling_tendencies
            WHERE season = 2024
            """,
        )
        assert count >= 100, f"Expected 100+ teams in 2024, got {count}"


# ---------------------------------------------------------------------------
# TestTeamSituationalSuccess
# ---------------------------------------------------------------------------


class TestTeamSituationalSuccess:
    """Tests for marts.team_situational_success matview."""

    def test_exists_and_has_data(self, db_conn):
        count = _fetch_count(db_conn, "SELECT COUNT(*) FROM marts.team_situational_success")
        assert count > 0, "success matview should have rows"

    def test_success_rate_range(self, db_conn):
        """All non-NULL success_rate between 0 and 1."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.team_situational_success
            WHERE success_rate IS NOT NULL
              AND (success_rate < 0 OR success_rate > 1)
            """,
        )
        assert count == 0

    def test_min_play_threshold(self, db_conn):
        """Rows with total_plays < 10 have NULL success_rate."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.team_situational_success
            WHERE total_plays < 10 AND success_rate IS NOT NULL
            """,
        )
        assert count == 0

    def test_yardage_success_only_on_third_fourth(self, db_conn):
        """yardage_success_rate NULL when down NOT IN (3, 4)."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.team_situational_success
            WHERE yardage_success_rate IS NOT NULL AND down NOT IN (3, 4)
            """,
        )
        assert count == 0

    def test_epa_range(self, db_conn):
        """avg_epa between -3.0 and 3.0 for all non-NULL rows."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.team_situational_success
            WHERE avg_epa IS NOT NULL AND (avg_epa < -3.0 OR avg_epa > 3.0)
            """,
        )
        assert count == 0


# ---------------------------------------------------------------------------
# TestTeamPlaycallingProfile
# ---------------------------------------------------------------------------


PROFILE_COLUMNS = {
    "team",
    "season",
    "conference",
    "games_played",
    "overall_run_rate",
    "early_down_run_rate",
    "third_down_pass_rate",
    "red_zone_run_rate",
    "overall_success_rate",
    "overall_avg_epa",
    "third_down_success_rate",
    "red_zone_success_rate",
    "leading_run_rate",
    "trailing_run_rate",
    "run_rate_delta",
    "pace_plays_per_game",
    "overall_run_rate_pctl",
    "early_down_run_rate_pctl",
    "third_down_pass_rate_pctl",
    "overall_epa_pctl",
    "third_down_success_pctl",
    "red_zone_success_pctl",
    "run_rate_delta_pctl",
    "pace_pctl",
}


class TestTeamPlaycallingProfile:
    """Tests for api.team_playcalling_profile view."""

    def test_view_exists(self, db_conn):
        """api.team_playcalling_profile exists in pg_views."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM information_schema.views
            WHERE table_schema = 'api' AND table_name = 'team_playcalling_profile'
            """,
        )
        assert count == 1

    def test_returns_rows(self, db_conn):
        count = _fetch_count(db_conn, "SELECT COUNT(*) FROM api.team_playcalling_profile")
        assert count > 0

    def test_columns(self, db_conn):
        """Expected column set matches."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'api' AND table_name = 'team_playcalling_profile'
            """,
        )
        actual = {r[0] for r in rows}
        assert actual == PROFILE_COLUMNS

    def test_one_row_per_team_season(self, db_conn):
        """No duplicates on (team, season)."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM (
                SELECT team, season FROM api.team_playcalling_profile
                GROUP BY team, season HAVING COUNT(*) > 1
            ) x
            """,
        )
        assert count == 0, "profile should have one row per team-season"

    def test_percentiles_range(self, db_conn):
        """All non-NULL percentiles between 0 and 1."""
        pctl_cols = [
            "overall_run_rate_pctl",
            "early_down_run_rate_pctl",
            "third_down_pass_rate_pctl",
            "overall_epa_pctl",
            "third_down_success_pctl",
            "red_zone_success_pctl",
            "run_rate_delta_pctl",
            "pace_pctl",
        ]
        for col in pctl_cols:
            count = _fetch_count(
                db_conn,
                f"""
                SELECT COUNT(*) FROM api.team_playcalling_profile
                WHERE {col} IS NOT NULL AND ({col} < 0 OR {col} > 1)
                """,
            )
            assert count == 0, f"{col} has values outside [0, 1]"

    def test_run_rate_delta_calculation(self, db_conn):
        """run_rate_delta = leading_run_rate - trailing_run_rate (within rounding)."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM api.team_playcalling_profile
            WHERE leading_run_rate IS NOT NULL
              AND trailing_run_rate IS NOT NULL
              AND ABS(run_rate_delta - (leading_run_rate - trailing_run_rate)) > 0.001
            """,
        )
        assert count == 0

    def test_pace_range(self, db_conn):
        """pace_plays_per_game between 1 and 150 for all non-NULL rows."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM api.team_playcalling_profile
            WHERE pace_plays_per_game IS NOT NULL
              AND (pace_plays_per_game < 1 OR pace_plays_per_game > 150)
            """,
        )
        assert count == 0

    def test_filter_pushdown(self, db_conn):
        """WHERE team = 'Ohio State' returns exactly one row per season."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT team, season, COUNT(*) FROM api.team_playcalling_profile
            WHERE team = 'Ohio State'
            GROUP BY team, season
            HAVING COUNT(*) > 1
            """,
        )
        assert len(rows) == 0, "Ohio State should have one row per season"
