-- Stats schema hardening: indexes for team and player season stats
-- ALTER-based migration against dlt's existing auto-created tables
-- All statements are idempotent (IF NOT EXISTS)

-- =============================================================================
-- TEAM SEASON STATS (49,819 rows)
-- =============================================================================

-- Business indexes
CREATE INDEX IF NOT EXISTS idx_team_season_stats_season ON stats.team_season_stats(season);
CREATE INDEX IF NOT EXISTS idx_team_season_stats_team ON stats.team_season_stats(team);
CREATE INDEX IF NOT EXISTS idx_team_season_stats_conference ON stats.team_season_stats(conference);
CREATE INDEX IF NOT EXISTS idx_team_season_stats_stat_name ON stats.team_season_stats(stat_name);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_team_season_stats_season_team ON stats.team_season_stats(season, team);
CREATE INDEX IF NOT EXISTS idx_team_season_stats_team_stat ON stats.team_season_stats(team, stat_name);

-- =============================================================================
-- PLAYER SEASON STATS (131,268 rows)
-- =============================================================================

-- Business indexes
CREATE INDEX IF NOT EXISTS idx_player_season_stats_season ON stats.player_season_stats(season);
CREATE INDEX IF NOT EXISTS idx_player_season_stats_player_id ON stats.player_season_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_player_season_stats_team ON stats.player_season_stats(team);
CREATE INDEX IF NOT EXISTS idx_player_season_stats_position ON stats.player_season_stats(position);
CREATE INDEX IF NOT EXISTS idx_player_season_stats_category ON stats.player_season_stats(category);
CREATE INDEX IF NOT EXISTS idx_player_season_stats_stat_type ON stats.player_season_stats(stat_type);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_player_season_stats_season_team ON stats.player_season_stats(season, team);
CREATE INDEX IF NOT EXISTS idx_player_season_stats_player_season ON stats.player_season_stats(player_id, season);
CREATE INDEX IF NOT EXISTS idx_player_season_stats_season_category ON stats.player_season_stats(season, category, stat_type);
CREATE INDEX IF NOT EXISTS idx_player_season_stats_team_category ON stats.player_season_stats(team, season, category);
