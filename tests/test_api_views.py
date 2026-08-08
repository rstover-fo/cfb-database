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
    "classification",
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

# Phase 5 of the preseason outlook plan -- latest Monte Carlo season
# projection per (season, team, model). games_unscored is derived in the view
# (games_scheduled - games_simulated) so a consumer cannot mistake an unscored
# game for a loss.
EXPECTED_POINTS_COLUMNS = {
    "era",
    "state",
    "down",
    "distance_bucket",
    "field_zone",
    "yards_to_goal_min",
    "yards_to_goal_max",
    "n_obs",
    "ep_drive",
    "ep_net",
    "p_td",
    "p_fg",
    "p_punt",
    "p_turnover",
    "se_boot",
    "computed_at",
}

SEASON_OUTLOOK_COLUMNS = {
    "projection_id",
    "computed_at",
    "projection_date",
    "model_version",
    "season",
    "team",
    "conference",
    "games_scheduled",
    "games_simulated",
    "games_unscored",
    "games_completed",
    "actual_wins",
    "schedule_complete",
    "projected_wins",
    "projected_losses",
    "median_wins",
    "wins_p10",
    "wins_p25",
    "wins_p75",
    "wins_p90",
    "p_win_dist",
    "p_bowl_eligible",
    "p_ten_plus",
    "sos_rating",
    "sos_rank",
    "conf_title_prob",
    "playoff_prob",
    "n_sims",
    "residual_sigma",
    "strength_share",
    # Appended 2026-07-26 (cfb-app review): the view mixes FBS/FCS/DII/DIII, so
    # a division column is what makes an unfiltered leaderboard safe, and a
    # completed season had no way to say it was a record rather than a forecast.
    "classification",
    "is_projection",
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

TEAM_WEEK_FEATURES_COLUMNS = {
    "season",
    "season_type",
    "week",
    "week_index",
    "team",
    "conference",
    "game_id",
    "games_played_to_date",
    "elo_pregame",
    "adj_epa_off",
    "adj_epa_def",
    "adj_epa_net",
    "adj_epa_hfa",
    "adj_epa_source",
    "off_epa_per_play",
    "off_success_rate",
    "off_explosiveness_rate",
    "off_plays_per_game",
    "def_epa_per_play_allowed",
    "def_success_rate_allowed",
    "def_explosiveness_rate_allowed",
    "havoc_rate_defense",
    "havoc_rate_offense_allowed",
    "returning_ppa_pct",
    "returning_passing_ppa_pct",
    "returning_rushing_ppa_pct",
    "returning_usage",
    "preseason_sp_rating",
    "preseason_sp_offense",
    "preseason_sp_defense",
    "computed_at",
    "feature_build_version",
}

LIVE_SCOREBOARD_COLUMNS = {
    "game_id",
    "season",
    "week",
    "season_type",
    "status",
    "period",
    "clock",
    "seconds_remaining",
    "home_team",
    "away_team",
    "home_points",
    "away_points",
    "possession",
    "spread",
    "over_under",
    "cfbd_home_wp",
    "house_live_home_wp",
    "pregame_expected_margin",
    "captured_at",
}

ADJUSTED_EPA_WEEK_COLUMNS = {
    "team",
    "season",
    "week_index",
    "off_coef",
    "def_coef",
    "hfa_coef",
    "mu",
    "plays",
    "lambda",
    "n_teams",
}


GAME_RECAPS_COLUMNS = {
    "game_id",
    "season",
    "week",
    "headline",
    "recap",
    "wp_available",
    "model",
    "generated_at",
}

COACH_RECORDS_COLUMNS = {
    "coach_name",
    "first_name",
    "last_name",
    "team",
    "first_season",
    "last_season",
    "seasons_count",
    "games",
    "wins",
    "losses",
    "ties",
    "win_pct",
    "ats_games",
    "ats_wins",
    "ats_losses",
    "ats_pushes",
    "ats_win_pct",
    "seasons_with_ats_data",
}

PENALTY_LOG_COLUMNS = {
    "play_id",
    "game_id",
    "season",
    "week",
    "season_type",
    "period",
    "down",
    "distance",
    "offense",
    "defense",
    "play_type",
    "is_penalty_play_type",
    "penalized_team",
    "benefiting_team",
    "infraction",
    "penalty_yards",
    "declined",
    "offsetting",
    "no_play",
    "multi_penalty",
    "yards_gained",
    "ppa",
    "play_text",
    "parse_ok",
}

TEAM_PENALTIES_COLUMNS = {
    "game_id",
    "season",
    "week",
    "season_type",
    "team",
    "opponent",
    "home_away",
    "penalties",
    "penalty_yards",
    "opponent_penalties",
    "opponent_penalty_yards",
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
            # Tier 3 analytics: feature vectors and adjusted EPA coefficients
            ("api.team_week_features", 15000),
            ("api.adjusted_epa_week", 50000),
            # Coach x team career grain -- coaches x schools over the loaded
            # coaching era (ref.coaches__seasons), conservative floor.
            ("api.coach_records", 200),
            # Penalty layer: ~5-15K penalty-type plays per season 2004+, plus
            # embedded mentions; box rows = 2 per game with team stats loaded.
            ("api.penalty_log", 100000),
            ("api.team_penalties", 30000),
            # Season projections: ~350 teams x the projection seasons
            # simulate_season maintains (latest completed season plus every
            # later season with a published schedule). Conservative floor of
            # one season's worth -- the view is latest-snapshot, so the count
            # does not grow with the append-only daily history.
            ("api.season_outlook", 300),
            # Drive-chain EP: ~160 states x 3 eras, all written in one compute
            # run -- conservative floor of two eras' worth.
            ("api.expected_points", 300),
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
            "team_week_features",
            "adjusted_epa_week",
            "coach_records",
            "penalty_log",
            "team_penalties",
            "season_outlook",
            "expected_points",
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
            # game_recaps starts empty (fills nightly) -- column check only, no
            # row-count entry in TestViewsExistAndReturnRows.
            ("api.game_recaps", GAME_RECAPS_COLUMNS),
            ("api.game_win_probability", GAME_WIN_PROBABILITY_COLUMNS),
            ("api.team_week_features", TEAM_WEEK_FEATURES_COLUMNS),
            ("api.live_scoreboard", LIVE_SCOREBOARD_COLUMNS),
            ("api.adjusted_epa_week", ADJUSTED_EPA_WEEK_COLUMNS),
            ("api.coach_records", COACH_RECORDS_COLUMNS),
            ("api.penalty_log", PENALTY_LOG_COLUMNS),
            ("api.team_penalties", TEAM_PENALTIES_COLUMNS),
            ("api.season_outlook", SEASON_OUTLOOK_COLUMNS),
            ("api.expected_points", EXPECTED_POINTS_COLUMNS),
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
            "game_recaps",
            "game_win_probability",
            "team_week_features",
            "live_scoreboard",
            "adjusted_epa_week",
            "coach_records",
            "penalty_log",
            "team_penalties",
            "season_outlook",
            "expected_points",
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

    def test_epa_rank_scoped_to_fbs(self, db_conn):
        """epa_rank should be computed within classification, not across FBS+FCS+lower.

        FBS has ~136 teams, so the max epa_rank among classification='fbs' rows
        for the most recent season with data must stay well under the old
        cross-classification bug (e.g. Oklahoma showing "#176"). Regression
        test for the FBS rank-scoping fix.
        """
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT MAX(season) FROM api.leaderboard_teams
            WHERE classification = 'fbs' AND epa_rank IS NOT NULL
            """,
        )
        season = rows[0][0]
        assert season is not None, "No FBS rows with epa_rank found in api.leaderboard_teams"

        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT MAX(epa_rank)
            FROM api.leaderboard_teams
            WHERE season = %s AND classification = 'fbs'
            """,
            (season,),
        )
        max_epa_rank = rows[0][0]
        assert max_epa_rank is not None
        assert max_epa_rank <= 140, (
            f"Max epa_rank among FBS teams in season {season} is {max_epa_rank}, "
            "expected <= 140 (FBS has ~136 teams; a higher max indicates ranks "
            "are leaking in FCS/lower-classification teams)"
        )

    def test_classification_is_season_accurate(self, db_conn):
        """classification reflects the team's classification IN THAT SEASON, not current membership.

        North Dakota State moved up to FBS (Mountain West) for 2026, but its
        2025 season was played (and dominated, 12-1) at the FCS level. If
        classification were sourced from ref.teams (current CFBD /teams
        membership) instead of core.games (season-accurate), NDSU's 2025 row
        would incorrectly show 'fbs' and leak onto FBS leaderboards/rank
        partitions. Regression test for the season-classification fix.
        """
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT classification
            FROM api.leaderboard_teams
            WHERE team = 'North Dakota State' AND season = 2025
            """,
        )
        if not rows:
            pytest.skip("North Dakota State 2025 row not present in api.leaderboard_teams")
        assert rows[0][0] == "fcs", (
            f"North Dakota State 2025 classification is {rows[0][0]!r}, expected 'fcs' "
            "(season-accurate, not current 2026 FBS membership)"
        )


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


# ---------------------------------------------------------------------------
# Test: coach_records filters and data quality
# ---------------------------------------------------------------------------


class TestCoachRecords:
    """api.coach_records -- coach x team career record, straight-up and ATS."""

    def test_saban_alabama_exists(self, db_conn):
        """A well-known durable coach's career record at his school should exist."""
        rows, columns = _fetch_all(
            db_conn,
            """
            SELECT wins
            FROM api.coach_records
            WHERE coach_name ILIKE %s AND team = %s
            """,
            ("%Saban%", "Alabama"),
        )
        assert len(rows) == 1, f"Expected 1 Saban/Alabama row, got {len(rows)}"
        row = dict(zip(columns, rows[0]))
        assert row["wins"] > 100, f"Saban at Alabama should have 100+ wins, got {row['wins']}"

    def test_win_pct_range(self, db_conn):
        """Straight-up win_pct should be between 0 and 1 where populated."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT win_pct
            FROM api.coach_records
            WHERE win_pct IS NOT NULL
            LIMIT 5000
            """,
        )
        for (win_pct,) in rows:
            assert 0 <= win_pct <= 1, f"win_pct {win_pct} out of [0, 1] range"

    def test_ats_win_pct_null_or_in_range(self, db_conn):
        """ats_win_pct should be NULL (no ATS coverage) or between 0 and 1."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT ats_win_pct
            FROM api.coach_records
            LIMIT 5000
            """,
        )
        for (ats_win_pct,) in rows:
            assert ats_win_pct is None or 0 <= ats_win_pct <= 1, (
                f"ats_win_pct {ats_win_pct} is neither NULL nor in [0, 1] range"
            )


# ---------------------------------------------------------------------------
# Test: season_outlook semantics
# ---------------------------------------------------------------------------


class TestSeasonOutlook:
    """Phase 5 surface over predictions.season_projections.

    These pin the invariants a consumer will assume without checking. Several
    correspond to defects already fixed upstream (unscored games counted as
    losses, postseason games inflating the slate); asserting them here means a
    regression in simulate_season.py surfaces at the contract boundary rather
    than as a plausible-looking win total.
    """

    def test_one_row_per_team_season_model(self, db_conn):
        """The view is latest-snapshot: the append-only daily history must not
        leak through as duplicate rows."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT season, team, model_version, COUNT(*) AS n
            FROM api.season_outlook
            GROUP BY season, team, model_version
            HAVING COUNT(*) > 1
            LIMIT 10
            """,
        )
        assert not rows, f"duplicate (season, team, model_version) rows: {rows}"

    def test_unscored_games_are_not_losses(self, db_conn):
        """projected_wins + projected_losses must span games_simulated, not
        games_scheduled. Counting the gap as losses is worse than a coin flip
        and was a real defect in the first cut of simulate_season.py."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT team, season, games_simulated, projected_wins, projected_losses
            FROM api.season_outlook
            WHERE projected_wins IS NOT NULL
              AND projected_losses IS NOT NULL
              AND ABS((projected_wins + projected_losses) - games_simulated) > 0.01
            LIMIT 10
            """,
        )
        assert not rows, f"projected wins+losses != games_simulated: {rows}"

    def test_games_unscored_is_consistent_and_non_negative(self, db_conn):
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT team, season, games_scheduled, games_simulated, games_unscored
            FROM api.season_outlook
            WHERE games_unscored <> games_scheduled - games_simulated
               OR games_unscored < 0
            LIMIT 10
            """,
        )
        assert not rows, f"games_unscored inconsistent or negative: {rows}"

    def test_percentiles_are_ordered(self, db_conn):
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT team, season, wins_p10, wins_p25, median_wins, wins_p75, wins_p90
            FROM api.season_outlook
            WHERE wins_p10 IS NOT NULL
              AND NOT (wins_p10 <= wins_p25
                       AND wins_p25 <= median_wins
                       AND median_wins <= wins_p75
                       AND wins_p75 <= wins_p90)
            LIMIT 10
            """,
        )
        assert not rows, f"percentiles out of order: {rows}"

    def test_probabilities_in_unit_range(self, db_conn):
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT team, season, p_bowl_eligible, p_ten_plus, conf_title_prob
            FROM api.season_outlook
            WHERE (p_bowl_eligible IS NOT NULL AND (p_bowl_eligible < 0 OR p_bowl_eligible > 1))
               OR (p_ten_plus IS NOT NULL AND (p_ten_plus < 0 OR p_ten_plus > 1))
               OR (conf_title_prob IS NOT NULL AND (conf_title_prob < 0 OR conf_title_prob > 1))
            LIMIT 10
            """,
        )
        assert not rows, f"probability outside [0, 1]: {rows}"

    def test_win_distribution_sums_to_one(self, db_conn):
        """p_win_dist is the whole reason this table beats a scalar; a
        distribution that does not sum to 1 is silently malformed."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT team, season, s
            FROM (
                SELECT team, season,
                       (SELECT SUM(value::numeric) FROM jsonb_each_text(p_win_dist)) AS s
                FROM api.season_outlook
                WHERE p_win_dist IS NOT NULL
                LIMIT 500
            ) d
            WHERE ABS(s - 1) > 0.01
            LIMIT 10
            """,
        )
        assert not rows, f"p_win_dist does not sum to 1: {rows}"

    def test_projected_wins_within_simulated_games(self, db_conn):
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT team, season, projected_wins, games_simulated
            FROM api.season_outlook
            WHERE projected_wins IS NOT NULL
              AND (projected_wins < 0 OR projected_wins > games_simulated)
            LIMIT 10
            """,
        )
        assert not rows, f"projected_wins outside [0, games_simulated]: {rows}"

    def test_regular_season_slate_only(self, db_conn):
        """Bowls and playoff games must not inflate the slate.

        Checked against core.games rather than a magic ceiling: a fixed bound
        would be both weak (a bowl only pushes a 12-game team to 13) and
        wrong, since the Hawaii exemption allows a 13th regular-season game
        and a conference championship -- also season_type='regular' in CFBD --
        can make 14 legitimately. Comparing to the actual regular-season count
        pins the real invariant.
        """
        rows, _ = _fetch_all(
            db_conn,
            """
            -- Scoped to the seasons the view actually holds. Aggregating all
            -- of core.games (1869+) to check two seasons scans hundreds of
            -- thousands of rows for nothing; LATERAL also avoids the second
            -- pass a home/away UNION ALL would cost.
            WITH seasons AS (
                SELECT DISTINCT season FROM api.season_outlook
            ),
            actual AS (
                SELECT g.season, t.team, COUNT(*) AS n
                FROM core.games g
                JOIN seasons s ON s.season = g.season
                CROSS JOIN LATERAL (VALUES (g.home_team), (g.away_team)) AS t(team)
                WHERE g.season_type = 'regular'
                GROUP BY g.season, t.team
            )
            SELECT o.season, o.team, o.games_scheduled, a.n AS regular_season_games
            FROM api.season_outlook o
            JOIN actual a ON a.season = o.season AND a.team = o.team
            WHERE o.games_scheduled <> a.n
            LIMIT 10
            """,
        )
        assert not rows, (
            f"games_scheduled != regular-season game count (postseason leaked in?): {rows}"
        )

    def test_playoff_prob_is_null_in_v1(self, db_conn):
        """Documented as deliberately absent. If this starts failing, the
        12-team format got modeled and the column comment needs updating."""
        count = _fetch_count(
            db_conn,
            "SELECT COUNT(*) FROM api.season_outlook WHERE playoff_prob IS NOT NULL",
        )
        assert count == 0, f"{count} rows have a playoff_prob but v1 ships it NULL"

    def test_residual_sigma_is_positive_where_present(self, db_conn):
        """sigma drives every probability in the row; a zero or negative one
        makes the distribution meaningless rather than merely wrong."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT team, season, residual_sigma
            FROM api.season_outlook
            WHERE residual_sigma IS NOT NULL AND residual_sigma <= 0
            LIMIT 10
            """,
        )
        assert not rows, f"non-positive residual_sigma: {rows}"

    # -- classification (added 2026-07-26) ---------------------------------

    def test_classification_join_neither_drops_nor_multiplies_rows(self, db_conn):
        """The whole risk of adding the column. team_season_class is grouped by
        (team, season) and teams_deduped is DISTINCT ON (school), so both joins
        are 1:1 -- but an INNER join, or a dedup that stopped deduping, would
        silently change the population of a view consumers rank on."""
        view_rows = _fetch_count(db_conn, "SELECT COUNT(*) FROM api.season_outlook")
        snapshots = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM (
                SELECT DISTINCT season, team, model_version
                FROM predictions.season_projections
            ) d
            """,
        )
        assert view_rows == snapshots, (
            f"view has {view_rows} rows for {snapshots} latest snapshots -- "
            "the classification join changed the row count"
        )

    def test_classification_values_are_known_divisions(self, db_conn):
        """NULL is allowed (unplaceable team, LEFT JOIN by design); a value
        outside CFBD's four is not.

        This also pins the spelling that scripts/simulate_season.py's
        STANDARD_SLATE_GAMES keys off. Only 'fbs' and 'fcs' appear anywhere
        else in this repo -- 'ii'/'iii' are taken from CFBD's own vocabulary,
        so a failure here means that assumption is wrong and the DII/DIII slate
        lengths are falling through to the default.
        """
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT DISTINCT classification
            FROM api.season_outlook
            WHERE classification IS NOT NULL
              AND classification NOT IN ('fbs', 'fcs', 'ii', 'iii')
            """,
        )
        assert not rows, f"unexpected classification values: {rows}"

    def test_classification_is_populated_and_fbs_is_plausibly_sized(self, db_conn):
        """The column has to be usable as a filter, which means it cannot be
        mostly NULL, and 'fbs' has to actually select FBS. Asserted on the
        per-season MAX rather than every season: a future season whose schedule
        is only half published legitimately projects fewer teams, and the flag
        for that is schedule_complete, not this."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT MAX(fbs), SUM(unknown), SUM(total)
            FROM (
                SELECT COUNT(*) FILTER (WHERE classification = 'fbs') AS fbs,
                       COUNT(*) FILTER (WHERE classification IS NULL) AS unknown,
                       COUNT(*) AS total
                FROM api.season_outlook
                GROUP BY season
            ) s
            """,
        )
        max_fbs, unknown, total = rows[0]
        assert total, "no rows in api.season_outlook"
        # FBS has run 127-136 teams over the modern era; nothing may exceed it.
        assert 120 <= max_fbs <= 140, f"largest FBS season is {max_fbs} teams"
        assert unknown * 10 < total, f"{unknown} of {total} rows have no classification"

    def test_classification_is_season_accurate_not_current_membership(self, db_conn):
        """North Dakota State played 2025 in FCS and moves to FBS in 2026, so
        ref.teams (current membership) says 'fbs' for both. Mirrors
        TestLeaderboardTeams::test_classification_is_season_accurate -- the
        same defect, one view over."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT season, classification
            FROM api.season_outlook
            WHERE team = 'North Dakota State' AND season = 2025
            """,
        )
        if not rows:
            pytest.skip("North Dakota State has no 2025 projection row")
        assert all(r[1] == "fcs" for r in rows), f"2025 NDSU classified as {rows}"

    # -- is_projection (added 2026-07-26) ----------------------------------

    def test_is_projection_tracks_whether_anything_was_simulated(self, db_conn):
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT season, team, games_simulated, games_completed, is_projection
            FROM api.season_outlook
            WHERE is_projection IS DISTINCT FROM (games_simulated > games_completed)
            LIMIT 10
            """,
        )
        assert not rows, f"is_projection disagrees with the games it is derived from: {rows}"

    def test_a_settled_row_is_pure_hindsight(self, db_conn):
        """is_projection = false has to MEAN something: with no simulated game
        the win total is the record book, so projected_wins must equal
        actual_wins. This is the shape the whole 699-row 2025 season has, and
        the reason a consumer defaulting to 'current season' needed the flag."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT season, team, projected_wins, actual_wins, wins_p10, wins_p90
            FROM api.season_outlook
            WHERE NOT is_projection
              AND (projected_wins IS DISTINCT FROM actual_wins::numeric
                   OR wins_p10 IS DISTINCT FROM wins_p90)
            LIMIT 10
            """,
        )
        assert not rows, f"is_projection=false but the row is not a settled record: {rows}"

    def test_a_forward_looking_season_is_labelled_a_projection(self, db_conn):
        """The inverse guard: if nothing anywhere is flagged as a projection,
        the column has degenerated into a constant and consumers branching on
        it would show a schedule as a finished season."""
        seasons = _fetch_count(
            db_conn,
            """
            SELECT COUNT(*) FROM (
                SELECT season FROM api.season_outlook
                GROUP BY season HAVING bool_or(is_projection)
            ) s
            """,
        )
        assert seasons >= 1, "no season has a single projected row"

    # -- division-aware stored columns (2026-07-26) ------------------------
    #
    # SEQUENCING. Unlike everything above, these two assert values WRITTEN by
    # scripts/simulate_season.py, not derived by the view. Replacing the view
    # does not satisfy them; they go green when the simulator is re-run, and
    # until then a failure here means the deploy is half done.

    def test_bowl_probability_is_absent_outside_fbs(self, db_conn):
        """Bowl eligibility is an FBS rule. Yale carried p_bowl_eligible =
        0.888 -- arithmetic about a postseason its division does not have."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT season, team, classification, p_bowl_eligible
            FROM api.season_outlook
            WHERE classification IS DISTINCT FROM 'fbs'
              AND p_bowl_eligible IS NOT NULL
            LIMIT 10
            """,
        )
        assert not rows, f"p_bowl_eligible set outside FBS (re-run simulate_season.py?): {rows}"

    def test_a_full_ten_game_ivy_slate_counts_as_complete(self, db_conn):
        """The reported defect: all 8 Ivy teams at games_scheduled = 10,
        games_unscored = 0 were flagged schedule_complete = false against a
        flat 11-game FBS threshold, and cfb-app rendered that as a "these are
        floors" warning about a conference whose season was fully described."""
        rows, _ = _fetch_all(
            db_conn,
            """
            SELECT season, team, games_scheduled, games_unscored, schedule_complete
            FROM api.season_outlook
            -- ILIKE because CFBD's conference label for the league is not
            -- pinned anywhere in this repo ('Ivy' vs 'Ivy League').
            WHERE conference ILIKE 'Ivy%'
              AND games_scheduled >= 10
              AND games_unscored = 0
              AND NOT schedule_complete
            LIMIT 10
            """,
        )
        assert not rows, f"full Ivy slates flagged short (re-run simulate_season.py?): {rows}"
