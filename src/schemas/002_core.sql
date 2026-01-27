-- Core schema hardening: indexes for games, drives, plays
-- ALTER-based migration against dlt's existing auto-created tables
-- All statements are idempotent (IF NOT EXISTS)

-- =============================================================================
-- GAMES (18,650 rows)
-- =============================================================================

-- Business indexes
CREATE INDEX IF NOT EXISTS idx_games_season ON core.games(season);
CREATE INDEX IF NOT EXISTS idx_games_week ON core.games(week);
CREATE INDEX IF NOT EXISTS idx_games_home_id ON core.games(home_id);
CREATE INDEX IF NOT EXISTS idx_games_away_id ON core.games(away_id);
CREATE INDEX IF NOT EXISTS idx_games_home_team ON core.games(home_team);
CREATE INDEX IF NOT EXISTS idx_games_away_team ON core.games(away_team);
CREATE INDEX IF NOT EXISTS idx_games_venue_id ON core.games(venue_id);
CREATE INDEX IF NOT EXISTS idx_games_start_date ON core.games(start_date);
CREATE INDEX IF NOT EXISTS idx_games_home_conference ON core.games(home_conference);
CREATE INDEX IF NOT EXISTS idx_games_away_conference ON core.games(away_conference);
CREATE INDEX IF NOT EXISTS idx_games_season_type ON core.games(season_type);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_games_season_week ON core.games(season, week);
CREATE INDEX IF NOT EXISTS idx_games_season_home_team ON core.games(season, home_team);
CREATE INDEX IF NOT EXISTS idx_games_season_away_team ON core.games(season, away_team);

-- =============================================================================
-- DRIVES (183,603 rows)
-- =============================================================================

-- Business indexes
CREATE INDEX IF NOT EXISTS idx_drives_game_id ON core.drives(game_id);
CREATE INDEX IF NOT EXISTS idx_drives_season ON core.drives(season);
CREATE INDEX IF NOT EXISTS idx_drives_offense ON core.drives(offense);
CREATE INDEX IF NOT EXISTS idx_drives_defense ON core.drives(defense);
CREATE INDEX IF NOT EXISTS idx_drives_drive_result ON core.drives(drive_result);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_drives_game_id_drive_number ON core.drives(game_id, drive_number);
CREATE INDEX IF NOT EXISTS idx_drives_season_offense ON core.drives(season, offense);
CREATE INDEX IF NOT EXISTS idx_drives_game_id_offense ON core.drives(game_id, offense);

-- =============================================================================
-- PLAYS (3,611,707 rows — largest table)
-- =============================================================================

-- Business indexes
CREATE INDEX IF NOT EXISTS idx_plays_game_id ON core.plays(game_id);
CREATE INDEX IF NOT EXISTS idx_plays_drive_id ON core.plays(drive_id);
CREATE INDEX IF NOT EXISTS idx_plays_season ON core.plays(season);
CREATE INDEX IF NOT EXISTS idx_plays_offense ON core.plays(offense);
CREATE INDEX IF NOT EXISTS idx_plays_defense ON core.plays(defense);
CREATE INDEX IF NOT EXISTS idx_plays_play_type ON core.plays(play_type);
CREATE INDEX IF NOT EXISTS idx_plays_down ON core.plays(down);
CREATE INDEX IF NOT EXISTS idx_plays_scoring ON core.plays(scoring) WHERE scoring = true;

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_plays_game_id_drive_id ON core.plays(game_id, drive_id);
CREATE INDEX IF NOT EXISTS idx_plays_season_offense ON core.plays(season, offense);
CREATE INDEX IF NOT EXISTS idx_plays_season_play_type ON core.plays(season, play_type);
CREATE INDEX IF NOT EXISTS idx_plays_game_id_play_number ON core.plays(game_id, play_number);

-- BRIN index: plays are physically ordered by season (loaded season-by-season)
-- BRIN is ~1000x smaller than B-tree for large sequential data
CREATE INDEX IF NOT EXISTS idx_plays_season_brin ON core.plays USING brin(season);

-- =============================================================================
-- LINE SCORES (child tables — dlt auto-created)
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_games_home_line_scores_parent ON core.games__home_line_scores(_dlt_parent_id);
CREATE INDEX IF NOT EXISTS idx_games_away_line_scores_parent ON core.games__away_line_scores(_dlt_parent_id);
