-- Run ANALYZE on all tables after index creation to update query planner statistics.
-- Execute this after running 002-009 schema migration scripts.

-- =============================================================================
-- REFERENCE TABLES
-- =============================================================================
ANALYZE ref.conferences;
ANALYZE ref.teams;
ANALYZE ref.venues;
ANALYZE ref.coaches;
ANALYZE ref.play_types;

-- =============================================================================
-- CORE TABLES
-- =============================================================================
ANALYZE core.games;
ANALYZE core.drives;
ANALYZE core.plays;

-- =============================================================================
-- STATS TABLES
-- =============================================================================
ANALYZE stats.team_season_stats;
ANALYZE stats.player_season_stats;

-- =============================================================================
-- RATINGS TABLES
-- =============================================================================
ANALYZE ratings.sp_ratings;
ANALYZE ratings.elo_ratings;
ANALYZE ratings.fpi_ratings;
ANALYZE ratings.srs_ratings;

-- =============================================================================
-- RECRUITING TABLES
-- =============================================================================
ANALYZE recruiting.recruits;
ANALYZE recruiting.team_recruiting;
ANALYZE recruiting.transfer_portal;

-- =============================================================================
-- BETTING TABLES
-- =============================================================================
ANALYZE betting.lines;

-- =============================================================================
-- DRAFT TABLES
-- =============================================================================
ANALYZE draft.draft_picks;

-- =============================================================================
-- METRICS TABLES
-- =============================================================================
ANALYZE metrics.ppa_teams;
ANALYZE metrics.ppa_players_season;
ANALYZE metrics.pregame_win_probability;
