"""Tests for API views in the api schema.

Verifies API views exist, return expected row counts,
expose the correct columns, and respond to filtered queries.
"""

import pytest


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

TEAM_DETAIL_COLUMNS = {
    "school",
    "mascot",
    "abbreviation",
    "color",
    "alternate_color",
    "logo_url",
    "conference",
    "classification",
    "current_season",
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
    "epa_per_play",
    "epa_tier",
    "success_rate",
    "explosiveness",
    "recruiting_rank",
    "recruiting_points",
}

TEAM_HISTORY_COLUMNS = {
    "team",
    "season",
    "conference",
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
    "elo",
    "fpi",
    "epa_per_play",
    "epa_tier",
    "success_rate",
    "explosiveness",
    "total_plays",
    "recruiting_rank",
    "recruiting_points",
}

GAME_DETAIL_COLUMNS = {
    "game_id",
    "season",
    "week",
    "season_type",
    "start_date",
    "start_time_tbd",
    "completed",
    "neutral_site",
    "conference_game",
    "home_team",
    "home_conference",
    "home_points",
    "home_pregame_elo",
    "home_epa",
    "home_success_rate",
    "away_team",
    "away_conference",
    "away_points",
    "away_pregame_elo",
    "away_epa",
    "away_success_rate",
    "winner",
    "point_diff",
    "home_spread",
    "over_under",
    "line_provider",
    "spread_result",
    "ou_result",
    "pregame_home_win_prob",
    "venue",
    "venue_id",
    "attendance",
    "excitement_index",
}

MATCHUP_COLUMNS = {
    "team1",
    "team2",
    "total_games",
    "team1_wins",
    "team2_wins",
    "ties",
    "first_meeting",
    "last_meeting",
    "recent_results",
    "team1_season",
    "team1_wins_season",
    "team1_losses_season",
    "team1_sp_rank",
    "team1_epa",
    "team1_epa_tier",
    "team2_season",
    "team2_wins_season",
    "team2_losses_season",
    "team2_sp_rank",
    "team2_epa",
    "team2_epa_tier",
}

LEADERBOARD_TEAMS_COLUMNS = {
    "team",
    "conference",
    "season",
    "games",
    "wins",
    "losses",
    "win_pct",
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
    "epa_per_play",
    "epa_tier",
    "success_rate",
    "explosiveness",
    "total_plays",
    "recruiting_rank",
    "recruiting_points",
    "wins_rank",
    "ppg_rank",
    "defense_ppg_rank",
    "epa_rank",
}

ROSTER_LOOKUP_COLUMNS = {
    "id",
    "first_name",
    "last_name",
    "team",
    "position",
    "height",
    "weight",
    "year",
    "jersey",
    "home_city",
    "home_state",
    "home_country",
}

RECRUIT_LOOKUP_COLUMNS = {
    "id",
    "athlete_id",
    "recruit_type",
    "year",
    "ranking",
    "name",
    "school",
    "committed_to",
    "position",
    "height",
    "weight",
    "stars",
    "rating",
    "city",
    "state_province",
    "country",
}

GAME_DRIVES_COLUMNS = {
    "game_id",
    "season",
    "drive_number",
    "offense",
    "defense",
    "start_period",
    "start_yards_to_goal",
    "end_yards_to_goal",
    "plays",
    "yards",
    "drive_result",
    "scoring",
    "start_offense_score",
    "end_offense_score",
    "start_defense_score",
    "end_defense_score",
    "start_time_minutes",
    "start_time_seconds",
    "elapsed_minutes",
    "elapsed_seconds",
    "is_home_offense",
}

GAME_PLAYS_COLUMNS = {
    "game_id",
    "season",
    "drive_number",
    "play_number",
    "offense",
    "defense",
    "period",
    "clock_minutes",
    "clock_seconds",
    "down",
    "distance",
    "yards_to_goal",
    "yards_gained",
    "play_type",
    "play_text",
    "ppa",
    "scoring",
    "offense_score",
    "defense_score",
}

POLL_RANKINGS_COLUMNS = {
    "season",
    "season_type",
    "week",
    "poll",
    "rank",
    "school",
    "conference",
    "first_place_votes",
    "points",
}

TEAM_ELO_COLUMNS = {
    "team",
    "season",
    "season_end_elo",
    "elo_rank",
    "games_played",
    "low_confidence",
    "cfbd_elo",
}

GAME_ELO_HISTORY_COLUMNS = {
    "game_id",
    "season",
    "week",
    "season_type",
    "start_date",
    "neutral_site",
    "home_team",
    "away_team",
    "home_pregame_elo",
    "away_pregame_elo",
    "home_postgame_elo",
    "away_postgame_elo",
    "home_win_prob",
    "expected_home_margin",
    "actual_home_margin",
    "mov_multiplier",
    "cfbd_home_pregame_elo",
    "cfbd_away_pregame_elo",
    "margin_error",
    "abs_margin_error",
}

SCORED_MATCHUP_EDGES_COLUMNS = {
    "game_id",
    "season",
    "week",
    "season_type",
    "start_date",
    "home_team",
    "away_team",
    "neutral_site",
    "model_version",
    "prediction_date",
    "home_elo_pregame",
    "away_elo_pregame",
    "elo_margin",
    "epa_margin",
    "expected_home_margin",
    "home_win_prob",
    "market_provider",
    "market_spread",
    "market_home_margin",
    "market_captured_at",
    "edge",
    "edge_pick",
    "abs_edge",
}

PREDICTION_ACCURACY_COLUMNS = {
    "model_version",
    "season",
    "edge_threshold",
    "n_games",
    "n_with_market",
    "margin_mae",
    "margin_rmse",
    "ats_wins",
    "ats_losses",
    "ats_pushes",
    "ats_hit_rate",
    "brier",
    "cfbd_brier",
    "n_scored_win_prob",
}

GAME_PREDICTIONS_COLUMNS = {
    "prediction_id",
    "computed_at",
    "prediction_date",
    "model_version",
    "game_id",
    "season",
    "week",
    "season_type",
    "home_team",
    "away_team",
    "neutral_site",
    "home_elo_pregame",
    "away_elo_pregame",
    "elo_margin",
    "epa_margin",
    "expected_home_margin",
    "home_win_prob",
    "market_provider",
    "market_home_margin",
    "market_spread",
    "market_captured_at",
    "edge",
    "edge_pick",
}

# P3.2 Lane B (docs/pipeline-manifest.md row 47) -- in-game per-play win
# probability, distinct from the Tier 2 pregame house win probability above
# (GAME_ELO_HISTORY_COLUMNS / GAME_PREDICTIONS_COLUMNS). NOT added to
# TestViewsExistAndReturnRows's row-floor list, same treatment as
# SCORED_MATCHUP_EDGES_COLUMNS: row count depends on how much of the
# multi-manifest backfill (deploys/p32-backfill-manifests.md) has completed
# by the time this test runs, not a fixed floor.
GAME_WIN_PROBABILITY_COLUMNS = {
    "game_id",
    "season",
    "play_id",
    "home_team",
    "away_team",
    "home_win_probability",
    "down",
    "distance",
    "yard_line",
    "play_text",
    "period",
    "clock_minutes",
    "clock_seconds",
}


# ---------------------------------------------------------------------------
# Test: views exist and return rows
# ---------------------------------------------------------------------------


class TestViewsExistAndReturnRows:
    """Each API view must exist and contain data."""

    @pytest.mark.parametrize(
        "view_name, min_rows",
        [
            ("api.team_detail", 100),
            ("api.team_history", 3000),
            ("api.game_detail", 40000),
            ("api.matchup", 10000),
            ("api.leaderboard_teams", 3000),
            ("api.roster_lookup", 300000),
            ("api.recruit_lookup", 60000),
            # Floors below actual counts from docs/db-snapshot-current.json
            # (2026-01-28: core.drives=183,603, core.plays=3,611,707,
            # core.rankings=29,579). Not re-verified live in this environment --
            # the Supabase REST endpoint was unreachable through the outbound
            # proxy (curl exit 56 / gateway 502 on CONNECT to
            # uvzwxwfjiunyceplmiru.supabase.co). Data only grows over time via
            # the merge-disposition pipeline, so these floors should hold.
            ("api.game_drives", 150000),
            ("api.game_plays", 3000000),
            ("api.poll_rankings", 20000),
            # Sized from 2026-07-21 Tier 2 backfill: house Elo 1869+, predictions
            # retro 2015-2025 x 2 models
            ("api.team_elo", 5000),
            ("api.game_elo_history", 40000),
            ("api.prediction_accuracy", 80),
            ("api.game_predictions", 15000),
        ],
        ids=[
            "team_detail",
            "team_history",
            "game_detail",
            "matchup",
            "leaderboard_teams",
            "roster_lookup",
            "recruit_lookup",
            "game_drives",
            "game_plays",
            "poll_rankings",
            "team_elo",
            "game_elo_history",
            "prediction_accuracy",
            "game_predictions",
        ],
    )
    def test_view_returns_rows(self, db_conn, view_name, min_rows):
        """View exists and has at least the expected minimum row count."""
        count = _fetch_count(db_conn, f"SELECT COUNT(*) FROM {view_name}")
        assert count >= min_rows, f"{view_name} returned {count} rows, expected at least {min_rows}"


# ---------------------------------------------------------------------------
# Test: column schemas match expectations
# ---------------------------------------------------------------------------


class TestViewColumns:
    """Each view must expose its documented columns."""

    @pytest.mark.parametrize(
        "view_name, expected_columns",
        [
            ("api.team_detail", TEAM_DETAIL_COLUMNS),
            ("api.team_history", TEAM_HISTORY_COLUMNS),
            ("api.game_detail", GAME_DETAIL_COLUMNS),
            ("api.matchup", MATCHUP_COLUMNS),
            ("api.leaderboard_teams", LEADERBOARD_TEAMS_COLUMNS),
            ("api.roster_lookup", ROSTER_LOOKUP_COLUMNS),
            ("api.recruit_lookup", RECRUIT_LOOKUP_COLUMNS),
            ("api.game_drives", GAME_DRIVES_COLUMNS),
            ("api.game_plays", GAME_PLAYS_COLUMNS),
            ("api.poll_rankings", POLL_RANKINGS_COLUMNS),
            ("api.team_elo", TEAM_ELO_COLUMNS),
            ("api.game_elo_history", GAME_ELO_HISTORY_COLUMNS),
            ("api.scored_matchup_edges", SCORED_MATCHUP_EDGES_COLUMNS),
            ("api.prediction_accuracy", PREDICTION_ACCURACY_COLUMNS),
            ("api.game_predictions", GAME_PREDICTIONS_COLUMNS),
            ("api.game_win_probability", GAME_WIN_PROBABILITY_COLUMNS),
        ],
        ids=[
            "team_detail",
            "team_history",
            "game_detail",
            "matchup",
            "leaderboard_teams",
            "roster_lookup",
            "recruit_lookup",
            "game_drives",
            "game_plays",
            "poll_rankings",
            "team_elo",
            "game_elo_history",
            "scored_matchup_edges",
            "prediction_accuracy",
            "game_predictions",
            "game_win_probability",
        ],
    )
    def test_columns_present(self, db_conn, view_name, expected_columns):
        """All expected columns are present in the view."""
        _, columns = _fetch_all(db_conn, f"SELECT * FROM {view_name} LIMIT 1")
        actual = set(columns)
        missing = expected_columns - actual
        assert not missing, f"{view_name} missing columns: {missing}. Actual: {sorted(actual)}"


# ---------------------------------------------------------------------------
# Test: team_detail filters and data quality
# ---------------------------------------------------------------------------


class TestTeamDetail:
    """api.team_detail — FBS team dashboard view."""

    def test_only_fbs_teams(self, db_conn):
        """View should only contain FBS classification teams."""
        rows, _ = _fetch_all(
            db_conn,
            "SELECT DISTINCT classification FROM api.team_detail",
        )
        classifications = {r[0] for r in rows}
        assert classifications == {"fbs"}, f"Expected only 'fbs', got {classifications}"

    def test_filter_by_school(self, db_conn):
        """Filtering by school returns exactly one row."""
        rows, columns = _fetch_all(
            db_conn,
            "SELECT * FROM api.team_detail WHERE school = %s",
            ("Alabama",),
        )
        assert len(rows) == 1, f"Expected 1 row for Alabama, got {len(rows)}"
        row = dict(zip(columns, rows[0]))
        assert row["school"] == "Alabama"
        assert row["conference"] is not None
        assert row["mascot"] is not None

    def test_alabama_has_ratings(self, db_conn):
        """Alabama should have SP+ rating populated (perennial top-tier)."""
        rows, columns = _fetch_all(
            db_conn,
            "SELECT sp_rating, elo, wins FROM api.team_detail WHERE school = %s",
            ("Alabama",),
        )
        row = dict(zip(columns, rows[0]))
        assert row["sp_rating"] is not None, "Alabama should have an SP+ rating"
        assert row["elo"] is not None, "Alabama should have an Elo rating"
        assert row["wins"] is not None, "Alabama should have a win count"


# ---------------------------------------------------------------------------
# Test: team_history filters and ordering
# ---------------------------------------------------------------------------


class TestTeamHistory:
    """api.team_history — multi-season team trends."""

    def test_filter_by_team(self, db_conn):
        """Filtering by team returns multiple seasons."""
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.team_history WHERE team = %s",
            ("Ohio State",),
        )
        assert count >= 3, f"Ohio State should have 3+ seasons, got {count}"

    def test_seasons_are_integers(self, db_conn):
        """Season column should contain reasonable year values."""
        rows, _ = _fetch_all(
            db_conn,
            "SELECT MIN(season), MAX(season) FROM api.team_history",
        )
        min_season, max_season = rows[0]
        assert min_season >= 1869, f"Min season {min_season} is too low"
        assert max_season <= 2026, f"Max season {max_season} is too high"

    def test_team_history_has_record(self, db_conn):
        """Each row should have games, wins, losses populated."""
        count_nulls = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*)
            FROM api.team_history
            WHERE games IS NULL OR wins IS NULL OR losses IS NULL
            """,
        )
        assert count_nulls == 0, f"Found {count_nulls} rows with NULL games/wins/losses"


# ---------------------------------------------------------------------------
# Test: game_detail filters
# ---------------------------------------------------------------------------


class TestGameDetail:
    """api.game_detail — single game detail view."""

    def test_filter_by_season(self, db_conn):
        """Filtering by season returns a reasonable number of games."""
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.game_detail WHERE season = %s",
            (2024,),
        )
        # Includes FBS, FCS, and all divisions — typically 3000-5000 games/season
        assert 1000 <= count <= 5000, f"Season 2024 returned {count} games, expected 1000-5000"

    def test_completed_games_have_scores(self, db_conn):
        """Completed games should have non-null scores."""
        count_missing = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*)
            FROM api.game_detail
            WHERE completed = true
              AND season >= 2004
              AND (home_points IS NULL OR away_points IS NULL)
            """,
        )
        # Allow a small number of edge cases (cancelled/forfeited games)
        assert count_missing <= 5, (
            f"Found {count_missing} completed games with NULL scores (>5 is concerning)"
        )

    def test_winner_matches_scores(self, db_conn):
        """Winner column should match the team with more points."""
        rows, columns = _fetch_all(
            db_conn,
            """
            SELECT game_id, home_team, away_team, home_points, away_points, winner
            FROM api.game_detail
            WHERE completed = true
              AND home_points != away_points
              AND season = 2024
            LIMIT 50
            """,
        )
        for row in rows:
            r = dict(zip(columns, row))
            if r["home_points"] > r["away_points"]:
                assert r["winner"] == r["home_team"], (
                    f"Game {r['game_id']}: home won but winner is {r['winner']}"
                )
            else:
                assert r["winner"] == r["away_team"], (
                    f"Game {r['game_id']}: away won but winner is {r['winner']}"
                )

    def test_game_has_teams(self, db_conn):
        """Every game should have home_team and away_team populated."""
        count_missing = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*)
            FROM api.game_detail
            WHERE home_team IS NULL OR away_team IS NULL
            """,
        )
        assert count_missing == 0, f"Found {count_missing} games with NULL home/away team"


# ---------------------------------------------------------------------------
# Test: matchup filters
# ---------------------------------------------------------------------------


class TestMatchup:
    """api.matchup — head-to-head rivalry view."""

    def test_alabama_auburn_rivalry(self, db_conn):
        """Iron Bowl matchup should exist with substantial history."""
        rows, columns = _fetch_all(
            db_conn,
            """
            SELECT * FROM api.matchup
            WHERE team1 = %s AND team2 = %s
            """,
            ("Alabama", "Auburn"),
        )
        assert len(rows) == 1, f"Expected 1 Alabama vs Auburn matchup row, got {len(rows)}"
        row = dict(zip(columns, rows[0]))
        # Our data starts ~2000, so expect 20+ Iron Bowl games
        assert row["total_games"] >= 20, f"Iron Bowl total_games={row['total_games']}, expected 20+"
        assert row["first_meeting"] <= 2001, (
            f"Iron Bowl first meeting was {row['first_meeting']}, expected <= 2001"
        )

    def test_team_ordering_is_alphabetical(self, db_conn):
        """team1 should always be alphabetically before team2 (LEAST/GREATEST)."""
        count_misordered = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.matchup WHERE team1 > team2",
        )
        assert count_misordered == 0, f"Found {count_misordered} matchups where team1 > team2"

    def test_wins_plus_ties_equals_total(self, db_conn):
        """team1_wins + team2_wins + ties should equal total_games."""
        count_mismatched = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*)
            FROM api.matchup
            WHERE team1_wins + team2_wins + ties != total_games
            """,
        )
        assert count_mismatched == 0, (
            f"Found {count_mismatched} matchups where wins + ties != total"
        )


# ---------------------------------------------------------------------------
# Test: leaderboard_teams filters and rankings
# ---------------------------------------------------------------------------


class TestLeaderboardTeams:
    """api.leaderboard_teams — team rankings by season."""

    def test_filter_by_season(self, db_conn):
        """Filtering by season returns teams for that year."""
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.leaderboard_teams WHERE season = %s",
            (2024,),
        )
        # Should have 130+ FBS teams
        assert count >= 100, f"Season 2024 leaderboard has {count} teams, expected 100+"

    def test_win_pct_range(self, db_conn):
        """Win percentage should be between 0 and 1."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT MIN(win_pct), MAX(win_pct)
            FROM api.leaderboard_teams
            WHERE win_pct IS NOT NULL
            """,
        )
        min_pct, max_pct = rows[0]
        assert min_pct >= 0, f"Min win_pct is {min_pct}, expected >= 0"
        assert max_pct <= 1, f"Max win_pct is {max_pct}, expected <= 1"

    def test_ranks_are_positive(self, db_conn):
        """Rank columns should be positive integers where populated."""
        count_bad_ranks = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*)
            FROM api.leaderboard_teams
            WHERE season = 2024
              AND (wins_rank <= 0 OR ppg_rank <= 0 OR defense_ppg_rank <= 0)
            """,
        )
        assert count_bad_ranks == 0, f"Found {count_bad_ranks} rows with non-positive ranks"

    def test_top_epa_team_exists(self, db_conn):
        """The #1 EPA-ranked team should have epa_rank = 1."""
        rows, columns = _fetch_all(
            db_conn,
            """
            SELECT team, epa_rank, epa_per_play
            FROM api.leaderboard_teams
            WHERE season = 2024 AND epa_rank = 1
            """,
        )
        assert len(rows) >= 1, "No team with epa_rank = 1 for 2024"
        row = dict(zip(columns, rows[0]))
        assert row["epa_per_play"] is not None


# ---------------------------------------------------------------------------
# Test: roster_lookup filters
# ---------------------------------------------------------------------------


class TestRosterLookup:
    """api.roster_lookup — player roster view."""

    def test_no_null_teams(self, db_conn):
        """View filters out rows with NULL team."""
        count_nulls = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.roster_lookup WHERE team IS NULL",
        )
        assert count_nulls == 0, f"Found {count_nulls} rows with NULL team (should be filtered)"

    def test_filter_by_team(self, db_conn):
        """Filtering by team returns reasonable roster size."""
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.roster_lookup WHERE team = %s",
            ("Alabama",),
        )
        # Multi-year rosters; Alabama should have hundreds of entries
        assert count >= 50, f"Alabama roster has {count} entries, expected 50+"

    def test_has_player_names(self, db_conn):
        """Most players should have first and last names populated."""
        count_missing = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*)
            FROM api.roster_lookup
            WHERE first_name IS NULL OR last_name IS NULL
            """,
        )
        total = _fetch_count(db_conn, "SELECT COUNT(*) FROM api.roster_lookup")
        # Allow some missing names in historical data, but less than 5%
        assert count_missing < total * 0.05, (
            f"{count_missing}/{total} players missing names (>{5}%)"
        )


# ---------------------------------------------------------------------------
# Test: recruit_lookup filters
# ---------------------------------------------------------------------------


class TestRecruitLookup:
    """api.recruit_lookup — recruiting view."""

    def test_filter_by_committed_to(self, db_conn):
        """Filtering by committed_to returns recruits for that school."""
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.recruit_lookup WHERE committed_to = %s",
            ("Alabama",),
        )
        assert count >= 100, f"Alabama has {count} recruits, expected 100+"

    def test_stars_range(self, db_conn):
        """Stars should be between 1 and 5 (or NULL for unranked)."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT MIN(stars), MAX(stars)
            FROM api.recruit_lookup
            WHERE stars IS NOT NULL
            """,
        )
        min_stars, max_stars = rows[0]
        assert min_stars >= 1, f"Min stars is {min_stars}, expected >= 1"
        assert max_stars <= 5, f"Max stars is {max_stars}, expected <= 5"

    def test_has_recruit_years(self, db_conn):
        """Recruits should span multiple years."""
        rows, _ = _fetch_all(
            db_conn,
            "SELECT MIN(year), MAX(year) FROM api.recruit_lookup",
        )
        min_year, max_year = rows[0]
        assert max_year - min_year >= 10, (
            f"Recruit years span only {max_year - min_year}, expected 10+"
        )

    def test_filter_by_position(self, db_conn):
        """Filtering by position returns results."""
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.recruit_lookup WHERE position = %s",
            ("QB",),
        )
        assert count >= 100, f"QBs in recruit_lookup: {count}, expected 100+"


# ---------------------------------------------------------------------------
# Test: game_drives filters and ordering
# ---------------------------------------------------------------------------

# Known game used as an example throughout the repo (docs/pipeline-manifest.md,
# api.game_player_leaders PostgREST examples).
EXAMPLE_GAME_ID = 401628455


class TestGameDrives:
    """api.game_drives — drive-by-drive summary for a game."""

    def test_filter_by_game_id_ordered_by_drive_number(self, db_conn):
        """A known game_id returns drives ordered by drive_number."""
        rows, columns = _fetch_all(
            db_conn,
            """
            SELECT drive_number
            FROM api.game_drives
            WHERE game_id = %s
            ORDER BY drive_number
            """,
            (EXAMPLE_GAME_ID,),
        )
        assert len(rows) > 0, f"No drives found for game_id {EXAMPLE_GAME_ID}"
        drive_numbers = [r[0] for r in rows]
        assert drive_numbers == sorted(drive_numbers), "Drives are not ordered by drive_number"

    def test_offense_and_defense_populated(self, db_conn):
        """Drives for a completed game should have offense/defense teams."""
        count_missing = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*)
            FROM api.game_drives
            WHERE game_id = %s
              AND (offense IS NULL OR defense IS NULL)
            """,
            (EXAMPLE_GAME_ID,),
        )
        assert count_missing == 0, f"Found {count_missing} drives with NULL offense/defense"


# ---------------------------------------------------------------------------
# Test: game_plays filters
# ---------------------------------------------------------------------------


class TestGamePlays:
    """api.game_plays — play-by-play for a game."""

    def test_filter_by_game_id_returns_plays(self, db_conn):
        """Plays for a known game_id are non-empty."""
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.game_plays WHERE game_id = %s",
            (EXAMPLE_GAME_ID,),
        )
        assert count > 0, f"No plays found for game_id {EXAMPLE_GAME_ID}"

    def test_play_types_are_not_filtered(self, db_conn):
        """View should expose more than one distinct play_type (no filtering)."""
        rows, _ = _fetch_all(
            db_conn,
            "SELECT DISTINCT play_type FROM api.game_plays WHERE game_id = %s",
            (EXAMPLE_GAME_ID,),
        )
        distinct_types = {r[0] for r in rows}
        assert len(distinct_types) > 1, (
            f"Expected multiple play_types for game_id {EXAMPLE_GAME_ID}, "
            f"got {distinct_types} (view may be filtering play types)"
        )

    def test_plays_ordered_by_drive_and_play_number(self, db_conn):
        """Plays for a game can be ordered by drive_number, play_number."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT drive_number, play_number
            FROM api.game_plays
            WHERE game_id = %s
            ORDER BY drive_number, play_number
            """,
            (EXAMPLE_GAME_ID,),
        )
        assert len(rows) > 0
        pairs = [(r[0], r[1]) for r in rows]
        assert pairs == sorted(pairs), "Plays are not ordered by drive_number, play_number"


# ---------------------------------------------------------------------------
# Test: poll_rankings filters
# ---------------------------------------------------------------------------


class TestPollRankings:
    """api.poll_rankings — weekly poll rankings."""

    def test_ap_top_25_has_25_rows_for_a_2024_week(self, db_conn):
        """AP Top 25 for a 2024 regular-season week should have 25 rows.

        Regression: under the old [season, week, poll, rank] merge key this
        week had only 24 rows -- two teams were tied at #11 (rank 12 skipped)
        and the rank-keyed merge silently dropped one of them. The key now
        includes school, so tied teams both survive.
        """
        count = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*)
            FROM api.poll_rankings
            WHERE season = %s AND poll = %s AND week = %s AND season_type = %s
            """,
            (2024, "AP Top 25", 10, "regular"),
        )
        assert count == 25, f"AP Top 25 week 10, 2024 has {count} rows, expected 25"

    def test_tied_ranks_are_preserved(self, db_conn):
        """2024 week 10 had two teams tied at #11 (and therefore no #12).
        Both tied teams must be present -- the tie is the exact shape the old
        rank-keyed merge destroyed."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT COUNT(*) FILTER (WHERE rank = 11),
                   COUNT(*) FILTER (WHERE rank = 12)
            FROM api.poll_rankings
            WHERE season = %s AND poll = %s AND week = %s AND season_type = %s
            """,
            (2024, "AP Top 25", 10, "regular"),
        )
        at_11, at_12 = rows[0]
        assert at_11 == 2, f"Expected 2 teams tied at rank 11, got {at_11}"
        assert at_12 == 0, f"Expected rank 12 to be skipped (tie above), got {at_12} rows"

    def test_rank_range(self, db_conn):
        """Rank should be between 1 and 25 for AP Top 25."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT MIN(rank), MAX(rank)
            FROM api.poll_rankings
            WHERE poll = %s
            """,
            ("AP Top 25",),
        )
        min_rank, max_rank = rows[0]
        assert min_rank >= 1, f"Min rank is {min_rank}, expected >= 1"
        assert max_rank <= 25, f"Max rank is {max_rank}, expected <= 25"

    def test_week_1_not_clobbered_by_postseason(self, db_conn):
        """CFBD reports the final (postseason) poll as week 1. Under the old
        [season, week, poll, rank] merge key that collided with the actual
        regular-season week 1 poll and one silently overwrote the other. The
        key now includes season_type, so both coexist: this asserts the
        regular-season week 1 poll is a full 25 rows AND the postseason final
        poll exists as its own distinct rows.
        """
        regular = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*)
            FROM api.poll_rankings
            WHERE season = %s AND poll = %s AND week = %s AND season_type = %s
            """,
            (2024, "AP Top 25", 1, "regular"),
        )
        assert regular == 25, f"AP Top 25 regular week 1, 2024 has {regular} rows, expected 25"

        postseason = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*)
            FROM api.poll_rankings
            WHERE season = %s AND poll = %s AND season_type = %s
            """,
            (2024, "AP Top 25", "postseason"),
        )
        assert postseason == 25, (
            f"AP Top 25 postseason (final) poll for 2024 has {postseason} rows, expected 25"
        )
