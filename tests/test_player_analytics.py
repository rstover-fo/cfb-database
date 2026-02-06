"""Tests for player analytics views and RPC functions.

Verifies api.player_season_leaders, api.player_detail views exist
and return expected data, and that get_player_search RPC works
with fuzzy matching via pg_trgm.
"""


def _fetch_all(conn, query, params=None):
    """Execute a query and return (rows, column_names)."""
    with conn.cursor() as cur:
        cur.execute(query, params)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    return rows, columns


def _fetch_count(conn, query, params=None):
    """Execute a COUNT query and return the integer result."""
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Expected schemas for each view
# ---------------------------------------------------------------------------

PLAYER_SEASON_LEADERS_COLUMNS = {
    "season",
    "category",
    "player_id",
    "player_name",
    "team",
    "yards",
    "touchdowns",
    "interceptions",
    "pct",
    "attempts",
    "completions",
    "carries",
    "yards_per_carry",
    "receptions",
    "yards_per_reception",
    "longest",
    "total_tackles",
    "solo_tackles",
    "sacks",
    "tackles_for_loss",
    "passes_defended",
    "yards_rank",
}

PLAYER_DETAIL_COLUMNS = {
    "player_id",
    "name",
    "team",
    "position",
    "season",
    "height",
    "weight",
    "jersey",
    "home_city",
    "home_state",
    "stars",
    "recruit_rating",
    "national_ranking",
    "recruit_class",
    "pass_att",
    "pass_cmp",
    "pass_yds",
    "pass_td",
    "pass_int",
    "pass_pct",
    "rush_car",
    "rush_yds",
    "rush_td",
    "rush_ypc",
    "rec",
    "rec_yds",
    "rec_td",
    "rec_ypr",
    "tackles",
    "sacks",
    "tfl",
    "pass_def",
    "ppa_avg",
    "ppa_total",
}

PLAYER_SEARCH_COLUMNS = {
    "player_id",
    "name",
    "team",
    "position",
    "season",
    "height",
    "weight",
    "jersey",
    "stars",
    "recruit_rating",
    "similarity_score",
}


# ---------------------------------------------------------------------------
# api.player_season_leaders
# ---------------------------------------------------------------------------


class TestPlayerSeasonLeaders:
    """Tests for api.player_season_leaders view."""

    def test_exists_and_returns_rows(self, db_conn):
        count = _fetch_count(db_conn, "SELECT COUNT(*) FROM api.player_season_leaders")
        assert count > 0, "player_season_leaders should have rows"

    def test_columns(self, db_conn):
        rows, columns = _fetch_all(db_conn, "SELECT * FROM api.player_season_leaders LIMIT 1")
        assert PLAYER_SEASON_LEADERS_COLUMNS.issubset(set(columns)), (
            f"Missing columns: {PLAYER_SEASON_LEADERS_COLUMNS - set(columns)}"
        )

    def test_has_all_categories(self, db_conn):
        rows, _ = _fetch_all(
            db_conn,
            "SELECT DISTINCT category FROM api.player_season_leaders ORDER BY category",
        )
        categories = {row[0] for row in rows}
        expected = {"passing", "rushing", "receiving", "defense"}
        assert expected.issubset(categories), f"Missing categories: {expected - categories}"

    def test_season_filter(self, db_conn):
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.player_season_leaders WHERE season = %s",
            (2024,),
        )
        assert count > 0, "Should have 2024 season data"

    def test_passing_leaders_have_yards(self, db_conn):
        rows, columns = _fetch_all(
            db_conn,
            """SELECT yards FROM api.player_season_leaders
               WHERE category = 'passing' AND season = 2024 AND yards IS NOT NULL
               ORDER BY yards DESC LIMIT 5""",
        )
        assert len(rows) >= 5, "Should have at least 5 passing leaders"
        # Top passer should have significant yardage
        assert rows[0][0] > 1000, "Top passer should have >1000 yards"

    def test_rankings_ordered(self, db_conn):
        rows, _ = _fetch_all(
            db_conn,
            """SELECT yards, yards_rank FROM api.player_season_leaders
               WHERE category = 'passing' AND season = 2024 AND yards IS NOT NULL
               ORDER BY yards_rank LIMIT 10""",
        )
        assert len(rows) >= 5
        # Rank 1 should have the most yards
        yards_values = [r[0] for r in rows if r[0] is not None]
        assert yards_values[0] >= yards_values[-1], "Rank 1 should have more yards than lower ranks"

    def test_defense_leaders_have_tackles(self, db_conn):
        rows, _ = _fetch_all(
            db_conn,
            """SELECT total_tackles FROM api.player_season_leaders
               WHERE category = 'defense' AND season = 2024 AND total_tackles IS NOT NULL
               ORDER BY total_tackles DESC LIMIT 5""",
        )
        assert len(rows) >= 5, "Should have defensive leaders"
        assert rows[0][0] > 50, "Top tackler should have >50 tackles"


# ---------------------------------------------------------------------------
# api.player_detail
# ---------------------------------------------------------------------------


class TestPlayerDetail:
    """Tests for api.player_detail view."""

    def test_exists_and_returns_rows(self, db_conn):
        count = _fetch_count(db_conn, "SELECT COUNT(*) FROM api.player_detail")
        assert count > 0, "player_detail should have rows"

    def test_columns(self, db_conn):
        rows, columns = _fetch_all(db_conn, "SELECT * FROM api.player_detail LIMIT 1")
        assert PLAYER_DETAIL_COLUMNS.issubset(set(columns)), (
            f"Missing columns: {PLAYER_DETAIL_COLUMNS - set(columns)}"
        )

    def test_team_filter(self, db_conn):
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.player_detail WHERE team = %s AND season = %s",
            ("Alabama", 2024),
        )
        assert count > 0, "Should have Alabama 2024 players"

    def test_has_passing_stats(self, db_conn):
        count = _fetch_count(
            db_conn,
            """SELECT COUNT(*) FROM api.player_detail
               WHERE pass_yds IS NOT NULL AND season = 2024""",
        )
        assert count > 0, "Some players should have passing stats"

    def test_has_rushing_stats(self, db_conn):
        count = _fetch_count(
            db_conn,
            """SELECT COUNT(*) FROM api.player_detail
               WHERE rush_yds IS NOT NULL AND season = 2024""",
        )
        assert count > 0, "Some players should have rushing stats"

    def test_has_recruiting_data(self, db_conn):
        count = _fetch_count(
            db_conn,
            """SELECT COUNT(*) FROM api.player_detail
               WHERE stars IS NOT NULL AND season = 2024""",
        )
        assert count > 0, "Some players should have recruiting data"

    def test_has_ppa_data(self, db_conn):
        count = _fetch_count(
            db_conn,
            """SELECT COUNT(*) FROM api.player_detail
               WHERE ppa_avg IS NOT NULL AND season = 2024""",
        )
        assert count > 0, "Some players should have PPA data"

    def test_multi_season(self, db_conn):
        rows, _ = _fetch_all(
            db_conn,
            """SELECT DISTINCT season FROM api.player_detail
               ORDER BY season""",
        )
        seasons = [r[0] for r in rows]
        assert len(seasons) >= 5, "Should have data across multiple seasons"

    def test_row_count_reasonable(self, db_conn):
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.player_detail WHERE season = 2024",
        )
        # Should have thousands of players per season
        assert count > 1000, f"Expected >1000 players for 2024, got {count}"


# ---------------------------------------------------------------------------
# get_player_search RPC
# ---------------------------------------------------------------------------


class TestPlayerSearch:
    """Tests for get_player_search RPC function."""

    def test_basic_search(self, db_conn):
        rows, columns = _fetch_all(
            db_conn,
            "SELECT * FROM get_player_search(%s)",
            ("Bryce Young",),
        )
        assert len(rows) > 0, "Should find Bryce Young"
        assert PLAYER_SEARCH_COLUMNS.issubset(set(columns))

    def test_fuzzy_matching(self, db_conn):
        """Slight misspelling should still match via pg_trgm."""
        rows, columns = _fetch_all(
            db_conn,
            "SELECT * FROM get_player_search(%s)",
            ("Bryce Yung",),
        )
        assert len(rows) > 0, "Fuzzy search should match 'Bryce Yung' to 'Bryce Young'"
        # Check the top result is Bryce Young
        name_idx = columns.index("name")
        names = [r[name_idx] for r in rows]
        assert any("Bryce Young" in n for n in names), (
            f"'Bryce Young' should be in results, got: {names[:5]}"
        )

    def test_position_filter(self, db_conn):
        rows, columns = _fetch_all(
            db_conn,
            "SELECT * FROM get_player_search(%s, p_position := %s)",
            ("Sanders", "QB"),
        )
        if len(rows) > 0:
            pos_idx = columns.index("position")
            positions = {r[pos_idx] for r in rows}
            assert positions == {"QB"}, f"All results should be QB, got {positions}"

    def test_team_filter(self, db_conn):
        rows, columns = _fetch_all(
            db_conn,
            "SELECT * FROM get_player_search(%s, p_team := %s)",
            ("Smith", "Alabama"),
        )
        if len(rows) > 0:
            team_idx = columns.index("team")
            teams = {r[team_idx] for r in rows}
            assert teams == {"Alabama"}, f"All results should be Alabama, got {teams}"

    def test_season_filter(self, db_conn):
        rows, columns = _fetch_all(
            db_conn,
            "SELECT * FROM get_player_search(%s, p_season := %s)",
            ("Williams", 2024),
        )
        if len(rows) > 0:
            season_idx = columns.index("season")
            seasons = {r[season_idx] for r in rows}
            assert seasons == {2024}, f"All results should be 2024, got {seasons}"

    def test_limit_respected(self, db_conn):
        rows, _ = _fetch_all(
            db_conn,
            "SELECT * FROM get_player_search(%s, p_limit := %s)",
            ("Smith", 3),
        )
        assert len(rows) <= 3, f"Should respect limit of 3, got {len(rows)}"

    def test_ordered_by_similarity(self, db_conn):
        rows, columns = _fetch_all(
            db_conn,
            "SELECT * FROM get_player_search(%s)",
            ("Caleb Williams",),
        )
        if len(rows) >= 2:
            score_idx = columns.index("similarity_score")
            scores = [r[score_idx] for r in rows]
            assert scores == sorted(scores, reverse=True), (
                "Results should be ordered by similarity_score DESC"
            )

    def test_no_results_for_gibberish(self, db_conn):
        rows, _ = _fetch_all(
            db_conn,
            "SELECT * FROM get_player_search(%s)",
            ("xyzzyplugh",),
        )
        assert len(rows) == 0, "Gibberish query should return no results"
