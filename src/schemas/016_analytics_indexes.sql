-- Analytics-driven indexes for mart queries
-- Derived from JOIN/WHERE patterns in existing 9 marts + 5 API views
--
-- NOTE: Plays table is partitioned by season; indexes created on parent
-- table will automatically propagate to partitions.

-- plays: most queried table (2.7M rows with PPA)
-- Core patterns: game lookup, team-season aggregations
CREATE INDEX IF NOT EXISTS idx_plays_game_id ON core.plays (game_id);
CREATE INDEX IF NOT EXISTS idx_plays_drive_id ON core.plays (drive_id);
CREATE INDEX IF NOT EXISTS idx_plays_game_drive ON core.plays (game_id, drive_id);
CREATE INDEX IF NOT EXISTS idx_plays_offense ON core.plays (offense);
CREATE INDEX IF NOT EXISTS idx_plays_defense ON core.plays (defense);
CREATE INDEX IF NOT EXISTS idx_plays_offense_season ON core.plays (offense, season);
CREATE INDEX IF NOT EXISTS idx_plays_defense_season ON core.plays (defense, season);
CREATE INDEX IF NOT EXISTS idx_plays_play_type ON core.plays (play_type);
CREATE INDEX IF NOT EXISTS idx_plays_ppa ON core.plays (ppa) WHERE ppa IS NOT NULL;

-- drives: game-level rollups
CREATE INDEX IF NOT EXISTS idx_drives_game_offense ON core.drives (game_id, offense);

-- games: filtering and matchup lookups
-- (home_team, away_team) for matchup joins
CREATE INDEX IF NOT EXISTS idx_games_teams ON core.games (home_team, away_team);

-- game_team_stats nested tables: box score joins
-- The stats are in core.game_team_stats__teams which links to parent via _dlt_parent_id
CREATE INDEX IF NOT EXISTS idx_game_team_stats_teams_team
    ON core.game_team_stats__teams (team);
CREATE INDEX IF NOT EXISTS idx_game_team_stats_teams_parent
    ON core.game_team_stats__teams (_dlt_parent_id);

-- player_season_stats: already has idx_player_season_stats_player_season

-- recruits: talent composite by school/year
-- Already has idx_recruits_year_committed_to (year, committed_to)
-- Add school-first version for queries filtering by school
CREATE INDEX IF NOT EXISTS idx_recruits_school_year
    ON recruiting.recruits (school, year);

-- team_recruiting: already has idx_team_recruiting_year_team

-- sp_ratings: already has idx_sp_ratings_year_team

-- Analyze tables after index creation
ANALYZE core.plays;
ANALYZE core.drives;
ANALYZE core.games;
ANALYZE core.game_team_stats__teams;
ANALYZE recruiting.recruits;
