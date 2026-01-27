-- Betting schema hardening: indexes for betting lines
-- ALTER-based migration against dlt's existing auto-created tables
-- All statements are idempotent (IF NOT EXISTS)

-- =============================================================================
-- LINES (20,192 rows)
-- =============================================================================

-- Business indexes
CREATE INDEX IF NOT EXISTS idx_lines_game_id ON betting.lines(game_id);
CREATE INDEX IF NOT EXISTS idx_lines_season ON betting.lines(season);
CREATE INDEX IF NOT EXISTS idx_lines_week ON betting.lines(week);
CREATE INDEX IF NOT EXISTS idx_lines_home_team ON betting.lines(home_team);
CREATE INDEX IF NOT EXISTS idx_lines_away_team ON betting.lines(away_team);
CREATE INDEX IF NOT EXISTS idx_lines_provider ON betting.lines(provider);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_lines_season_week ON betting.lines(season, week);
CREATE INDEX IF NOT EXISTS idx_lines_game_id_provider ON betting.lines(game_id, provider);
