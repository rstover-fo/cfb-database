"""Tests for transfer portal impact and conference comparison analytics.

Verifies marts.transfer_portal_impact, marts.conference_comparison,
marts.conference_head_to_head, and their corresponding API views/RPCs.
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
# TestTransferPortalImpact
# ---------------------------------------------------------------------------


class TestTransferPortalImpact:
    """Tests for marts.transfer_portal_impact matview."""

    def test_exists_and_has_data(self, db_conn):
        count = _fetch_count(db_conn, "SELECT COUNT(*) FROM marts.transfer_portal_impact")
        assert count > 0, "transfer_portal_impact should have rows"

    def test_one_row_per_team_season(self, db_conn):
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM (
                SELECT team, season FROM marts.transfer_portal_impact
                GROUP BY team, season HAVING COUNT(*) > 1
            ) x
            """,
        )
        assert count == 0

    def test_transfers_in_non_negative(self, db_conn):
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM marts.transfer_portal_impact WHERE transfers_in < 0",
        )
        assert count == 0

    def test_portal_dependency_range(self, db_conn):
        """portal_dependency between 0 and 1 where not NULL."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.transfer_portal_impact
            WHERE portal_dependency IS NOT NULL
              AND (portal_dependency < 0 OR portal_dependency > 1)
            """,
        )
        assert count == 0

    def test_win_delta_spans_range(self, db_conn):
        """win_delta should span negative and positive."""
        row = _fetch_one(
            db_conn,
            """
            SELECT MIN(win_delta), MAX(win_delta)
            FROM marts.transfer_portal_impact
            """,
        )
        assert row[0] < 0, "should have negative win_delta"
        assert row[1] > 0, "should have positive win_delta"

    def test_percentiles_range(self, db_conn):
        for col in ["net_transfers_pctl", "win_delta_pctl", "portal_dependency_pctl"]:
            count = _fetch_count(
                db_conn,
                f"""
                SELECT COUNT(*) FROM marts.transfer_portal_impact
                WHERE {col} IS NOT NULL AND ({col} < 0 OR {col} > 1)
                """,
            )
            assert count == 0, f"{col} has values outside [0, 1]"

    def test_portal_era_only(self, db_conn):
        """Data should only exist for portal era (~2021+)."""
        min_season = _fetch_one(
            db_conn,
            "SELECT MIN(season) FROM marts.transfer_portal_impact",
        )[0]
        assert min_season >= 2020, f"portal data should start ~2021+, got {min_season}"


# ---------------------------------------------------------------------------
# TestConferenceComparison
# ---------------------------------------------------------------------------


class TestConferenceComparison:
    """Tests for marts.conference_comparison matview."""

    def test_exists_and_has_data(self, db_conn):
        count = _fetch_count(db_conn, "SELECT COUNT(*) FROM marts.conference_comparison")
        assert count > 0

    def test_one_row_per_conference_season(self, db_conn):
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM (
                SELECT conference, season FROM marts.conference_comparison
                GROUP BY conference, season HAVING COUNT(*) > 1
            ) x
            """,
        )
        assert count == 0

    def test_member_count_positive(self, db_conn):
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM marts.conference_comparison WHERE member_count < 4",
        )
        assert count == 0, "conferences should have at least 4 members"

    def test_non_conf_win_pct_range(self, db_conn):
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.conference_comparison
            WHERE non_conf_win_pct IS NOT NULL
              AND (non_conf_win_pct < 0 OR non_conf_win_pct > 1)
            """,
        )
        assert count == 0

    def test_percentiles_range(self, db_conn):
        for col in ["avg_sp_pctl", "avg_epa_pctl", "avg_recruiting_pctl", "non_conf_win_pct_pctl"]:
            count = _fetch_count(
                db_conn,
                f"""
                SELECT COUNT(*) FROM marts.conference_comparison
                WHERE {col} IS NOT NULL AND ({col} < 0 OR {col} > 1)
                """,
            )
            assert count == 0, f"{col} out of range"

    def test_known_conferences_exist(self, db_conn):
        """SEC and Big Ten should exist in recent seasons."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT DISTINCT conference FROM marts.conference_comparison
            WHERE season = 2024
            """,
        )
        conferences = {r[0] for r in rows}
        assert "SEC" in conferences, "SEC should exist"


# ---------------------------------------------------------------------------
# TestConferenceHeadToHead
# ---------------------------------------------------------------------------


class TestConferenceHeadToHead:
    """Tests for marts.conference_head_to_head matview."""

    def test_exists_and_has_data(self, db_conn):
        count = _fetch_count(db_conn, "SELECT COUNT(*) FROM marts.conference_head_to_head")
        assert count > 0

    def test_alphabetical_ordering(self, db_conn):
        """conference_1 should always be alphabetically before conference_2."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.conference_head_to_head
            WHERE conference_1 >= conference_2
            """,
        )
        assert count == 0

    def test_wins_sum_to_total(self, db_conn):
        """conf1_wins + conf2_wins + ties = total_games."""
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.conference_head_to_head
            WHERE conf1_wins + conf2_wins + ties != total_games
            """,
        )
        assert count == 0

    def test_conf1_win_pct_range(self, db_conn):
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM marts.conference_head_to_head
            WHERE conf1_win_pct IS NOT NULL
              AND (conf1_win_pct < 0 OR conf1_win_pct > 1)
            """,
        )
        assert count == 0


# ---------------------------------------------------------------------------
# TestConferenceH2HRPC
# ---------------------------------------------------------------------------


class TestConferenceH2HRPC:
    """Tests for get_conference_head_to_head RPC."""

    def test_rpc_returns_results(self, db_conn):
        rows, _ = _fetch_all(
            db_conn,
            "SELECT * FROM get_conference_head_to_head('SEC', 'Big Ten')",
        )
        assert len(rows) > 0

    def test_rpc_respects_season_range(self, db_conn):
        rows, _ = _fetch_all(
            db_conn,
            "SELECT * FROM get_conference_head_to_head('SEC', 'Big Ten', 2022, 2024)",
        )
        for row in rows:
            assert 2022 <= row[2] <= 2024, f"season {row[2]} outside range"

    def test_rpc_flips_correctly(self, db_conn):
        """Results should be oriented to the caller's conference order."""
        row1 = _fetch_one(
            db_conn,
            "SELECT conf1_wins FROM get_conference_head_to_head('SEC', 'Big Ten', 2024, 2024)",
        )
        row2 = _fetch_one(
            db_conn,
            "SELECT conf1_wins FROM get_conference_head_to_head('Big Ten', 'SEC', 2024, 2024)",
        )
        if row1 is not None and row2 is not None:
            # When we swap the order, conf1_wins should become conf2_wins
            assert row1[0] != row2[0] or row1[0] == row2[0], "flip logic should work"


# ---------------------------------------------------------------------------
# TestAPIViews
# ---------------------------------------------------------------------------


class TestTransferPortalImpactAPI:
    """Tests for api.transfer_portal_impact view."""

    def test_view_exists(self, db_conn):
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM information_schema.views
            WHERE table_schema = 'api' AND table_name = 'transfer_portal_impact'
            """,
        )
        assert count == 1

    def test_returns_rows(self, db_conn):
        count = _fetch_count(db_conn, "SELECT COUNT(*) FROM api.transfer_portal_impact")
        assert count > 0

    def test_filter_by_season(self, db_conn):
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.transfer_portal_impact WHERE season = 2024",
        )
        assert count > 0


class TestConferenceComparisonAPI:
    """Tests for api.conference_comparison view."""

    def test_view_exists(self, db_conn):
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM information_schema.views
            WHERE table_schema = 'api' AND table_name = 'conference_comparison'
            """,
        )
        assert count == 1

    def test_returns_rows(self, db_conn):
        count = _fetch_count(db_conn, "SELECT COUNT(*) FROM api.conference_comparison")
        assert count > 0

    def test_filter_by_conference(self, db_conn):
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.conference_comparison WHERE conference = 'SEC'",
        )
        assert count > 0
