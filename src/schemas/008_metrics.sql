-- Metrics schema hardening: indexes for PPA and win probability tables
-- ALTER-based migration against dlt's existing auto-created tables
-- All statements are idempotent (IF NOT EXISTS)

-- =============================================================================
-- PPA TEAMS (792 rows)
-- =============================================================================

-- Business indexes
CREATE INDEX IF NOT EXISTS idx_ppa_teams_season ON metrics.ppa_teams(season);
CREATE INDEX IF NOT EXISTS idx_ppa_teams_team ON metrics.ppa_teams(team);
CREATE INDEX IF NOT EXISTS idx_ppa_teams_conference ON metrics.ppa_teams(conference);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_ppa_teams_season_team ON metrics.ppa_teams(season, team);

-- =============================================================================
-- PPA PLAYERS SEASON (24,475 rows)
-- =============================================================================

-- Business indexes
CREATE INDEX IF NOT EXISTS idx_ppa_players_season_season ON metrics.ppa_players_season(season);
CREATE INDEX IF NOT EXISTS idx_ppa_players_season_team ON metrics.ppa_players_season(team);
CREATE INDEX IF NOT EXISTS idx_ppa_players_season_position ON metrics.ppa_players_season(position);
CREATE INDEX IF NOT EXISTS idx_ppa_players_season_conference ON metrics.ppa_players_season(conference);
CREATE INDEX IF NOT EXISTS idx_ppa_players_season_name ON metrics.ppa_players_season(name);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_ppa_players_season_season_team ON metrics.ppa_players_season(season, team);
CREATE INDEX IF NOT EXISTS idx_ppa_players_season_season_position ON metrics.ppa_players_season(season, position);

-- =============================================================================
-- PREGAME WIN PROBABILITY (5,080 rows)
-- =============================================================================

-- Business indexes
CREATE INDEX IF NOT EXISTS idx_pregame_wp_season ON metrics.pregame_win_probability(season);
CREATE INDEX IF NOT EXISTS idx_pregame_wp_game_id ON metrics.pregame_win_probability(game_id);
CREATE INDEX IF NOT EXISTS idx_pregame_wp_week ON metrics.pregame_win_probability(week);
CREATE INDEX IF NOT EXISTS idx_pregame_wp_home_team ON metrics.pregame_win_probability(home_team);
CREATE INDEX IF NOT EXISTS idx_pregame_wp_away_team ON metrics.pregame_win_probability(away_team);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_pregame_wp_season_week ON metrics.pregame_win_probability(season, week);
CREATE INDEX IF NOT EXISTS idx_pregame_wp_season_game_id ON metrics.pregame_win_probability(season, game_id);
