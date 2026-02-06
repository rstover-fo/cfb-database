"""Tests for coaching tenure and recruiting ROI analytics.

Verifies marts.coaching_tenure, marts.recruiting_roi, and their
corresponding API views exist, contain valid data, and enforce expected constraints.
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
# TestCoachingTenure
# ---------------------------------------------------------------------------


class TestCoachingTenure:
    """Tests for marts.coaching_tenure matview."""

    def test_exists_and_has_data(self, db_conn):
        count = _fetch_count(db_conn, "SELECT COUNT(*) FROM marts.coaching_tenure")
        assert count > 0, "coaching_tenure should have rows"

    def test_tenure_is_contiguous(self, db_conn):
        """seasons_count should equal tenure_end - tenure_start + 1 for all tenures."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.coaching_tenure
            WHERE seasons_count != (tenure_end - tenure_start + 1)
            """,
        )
        assert count == 0, "tenure seasons_count should match start-end range"

    def test_win_pct_range(self, db_conn):
        """All non-NULL win_pct between 0 and 1."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.coaching_tenure
            WHERE win_pct IS NOT NULL AND (win_pct < 0 OR win_pct > 1)
            """,
        )
        assert count == 0

    def test_no_duplicate_tenures(self, db_conn):
        """Unique on (first_name, last_name, team, tenure_start)."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM (
                SELECT first_name, last_name, team, tenure_start
                FROM marts.coaching_tenure
                GROUP BY first_name, last_name, team, tenure_start
                HAVING COUNT(*) > 1
            ) x
            """,
        )
        assert count == 0

    def test_known_coach_saban(self, db_conn):
        """Saban at Alabama should have > 150 wins."""
        row = _fetch_one(
            db_conn,
            """
            SELECT total_wins FROM marts.coaching_tenure
            WHERE last_name = 'Saban' AND team = 'Alabama'
            AND tenure_start = 2007
            """,
        )
        assert row is not None, "Saban at Alabama not found"
        assert row[0] > 150, f"Saban should have 150+ wins, got {row[0]}"

    def test_bowl_games_gte_wins(self, db_conn):
        """bowl_games >= bowl_wins for all rows."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.coaching_tenure
            WHERE bowl_games < bowl_wins
            """,
        )
        assert count == 0

    def test_active_coaches_recent(self, db_conn):
        """Active coaches should have recent tenure_end."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.coaching_tenure
            WHERE is_active = true AND tenure_end < 2023
            """,
        )
        assert count == 0, "active coaches should have tenure_end >= 2023"

    def test_talent_improvement_calculation(self, db_conn):
        """talent_improvement = inherited_talent_rank - year3_talent_rank where both exist."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.coaching_tenure
            WHERE inherited_talent_rank IS NOT NULL
              AND year3_talent_rank IS NOT NULL
              AND talent_improvement != inherited_talent_rank - year3_talent_rank
            """,
        )
        assert count == 0

    def test_conf_win_pct_range(self, db_conn):
        """All non-NULL conf_win_pct between 0 and 1."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.coaching_tenure
            WHERE conf_win_pct IS NOT NULL AND (conf_win_pct < 0 OR conf_win_pct > 1)
            """,
        )
        assert count == 0


# ---------------------------------------------------------------------------
# TestRecruitingROI
# ---------------------------------------------------------------------------


class TestRecruitingROI:
    """Tests for marts.recruiting_roi matview."""

    def test_exists_and_has_data(self, db_conn):
        count = _fetch_count(db_conn, "SELECT COUNT(*) FROM marts.recruiting_roi")
        assert count > 0, "recruiting_roi should have rows"

    def test_one_row_per_team_season(self, db_conn):
        """No duplicates on (team, season)."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM (
                SELECT team, season FROM marts.recruiting_roi
                GROUP BY team, season HAVING COUNT(*) > 1
            ) x
            """,
        )
        assert count == 0

    def test_blue_chip_ratio_range(self, db_conn):
        """blue_chip_ratio between 0 and 1 for all non-NULL rows."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.recruiting_roi
            WHERE blue_chip_ratio IS NOT NULL
              AND (blue_chip_ratio < 0 OR blue_chip_ratio > 1)
            """,
        )
        assert count == 0

    def test_known_blue_bloods_high_bcr(self, db_conn):
        """Alabama and Ohio State should have BCR > 0.4 in recent years."""
        for team in ["Alabama", "Ohio State"]:
            row = _fetch_one(
                db_conn,
                """
                SELECT AVG(blue_chip_ratio) FROM marts.recruiting_roi
                WHERE team = %s AND season >= 2018
                """,
                (team,),
            )
            assert row[0] is not None, f"{team} BCR not found"
            assert row[0] > 0.4, f"{team} BCR should be > 0.4, got {row[0]}"

    def test_wins_over_expected_distribution(self, db_conn):
        """wins_over_expected spans negative and positive range."""
        row = _fetch_one(
            db_conn,
            """
            SELECT MIN(wins_over_expected), MAX(wins_over_expected)
            FROM marts.recruiting_roi
            WHERE wins_over_expected IS NOT NULL
            """,
        )
        assert row[0] < 0, "should have negative wins_over_expected"
        assert row[1] > 0, "should have positive wins_over_expected"

    def test_efficiency_percentiles_range(self, db_conn):
        """All non-NULL percentiles between 0 and 1."""
        for col in ["win_pct_pctl", "epa_pctl", "recruiting_efficiency_pctl"]:
            count = _fetch_count(
                db_conn,
                f"""
                SELECT COUNT(*) FROM marts.recruiting_roi
                WHERE {col} IS NOT NULL AND ({col} < 0 OR {col} > 1)
                """,
            )
            assert count == 0, f"{col} has values outside [0, 1]"

    def test_draft_picks_non_negative(self, db_conn):
        """players_drafted >= 0 for all rows."""
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM marts.recruiting_roi WHERE players_drafted < 0",
        )
        assert count == 0

    def test_conference_populated(self, db_conn):
        """Conference not NULL for most rows (some teams may lack conference info)."""
        total = _fetch_count(db_conn, "SELECT COUNT(*) FROM marts.recruiting_roi")
        with_conf = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM marts.recruiting_roi WHERE conference IS NOT NULL",
        )
        assert with_conf / total > 0.8, "most rows should have conference"


# ---------------------------------------------------------------------------
# TestCoachingHistoryAPI
# ---------------------------------------------------------------------------


class TestCoachingHistoryAPI:
    """Tests for api.coaching_history view."""

    def test_view_exists(self, db_conn):
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM information_schema.views
            WHERE table_schema = 'api' AND table_name = 'coaching_history'
            """,
        )
        assert count == 1

    def test_returns_rows(self, db_conn):
        count = _fetch_count(db_conn, "SELECT COUNT(*) FROM api.coaching_history")
        assert count > 0

    def test_filter_by_team(self, db_conn):
        """Filtering by team returns results."""
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.coaching_history WHERE team = 'Alabama'",
        )
        assert count > 0

    def test_filter_by_coach(self, db_conn):
        """Filtering by last_name returns results."""
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.coaching_history WHERE last_name = 'Saban'",
        )
        assert count > 0

    def test_active_coaches_filter(self, db_conn):
        """is_active filter works."""
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.coaching_history WHERE is_active = true",
        )
        assert count > 0


# ---------------------------------------------------------------------------
# TestRecruitingROI_API
# ---------------------------------------------------------------------------


class TestRecruitingROIAPI:
    """Tests for api.recruiting_roi view."""

    def test_view_exists(self, db_conn):
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM information_schema.views
            WHERE table_schema = 'api' AND table_name = 'recruiting_roi'
            """,
        )
        assert count == 1

    def test_returns_rows(self, db_conn):
        count = _fetch_count(db_conn, "SELECT COUNT(*) FROM api.recruiting_roi")
        assert count > 0

    def test_filter_by_season(self, db_conn):
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.recruiting_roi WHERE season = 2023",
        )
        assert count > 0

    def test_filter_by_conference(self, db_conn):
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.recruiting_roi WHERE conference = 'SEC'",
        )
        assert count > 0

    def test_one_row_per_team_season(self, db_conn):
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM (
                SELECT team, season FROM api.recruiting_roi
                GROUP BY team, season HAVING COUNT(*) > 1
            ) x
            """,
        )
        assert count == 0
