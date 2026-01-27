-- Recruiting schema hardening: indexes for recruits, team recruiting, transfer portal
-- ALTER-based migration against dlt's existing auto-created tables
-- All statements are idempotent (IF NOT EXISTS)

-- =============================================================================
-- RECRUITS (16,086 rows)
-- =============================================================================

-- Business indexes
CREATE INDEX IF NOT EXISTS idx_recruits_year ON recruiting.recruits(year);
CREATE INDEX IF NOT EXISTS idx_recruits_school ON recruiting.recruits(school);
CREATE INDEX IF NOT EXISTS idx_recruits_committed_to ON recruiting.recruits(committed_to);
CREATE INDEX IF NOT EXISTS idx_recruits_position ON recruiting.recruits(position);
CREATE INDEX IF NOT EXISTS idx_recruits_stars ON recruiting.recruits(stars);
CREATE INDEX IF NOT EXISTS idx_recruits_state_province ON recruiting.recruits(state_province);
CREATE INDEX IF NOT EXISTS idx_recruits_athlete_id ON recruiting.recruits(athlete_id);
CREATE INDEX IF NOT EXISTS idx_recruits_ranking ON recruiting.recruits(ranking);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_recruits_year_position ON recruiting.recruits(year, position);
CREATE INDEX IF NOT EXISTS idx_recruits_year_committed_to ON recruiting.recruits(year, committed_to);
CREATE INDEX IF NOT EXISTS idx_recruits_year_stars ON recruiting.recruits(year, stars);

-- =============================================================================
-- TEAM RECRUITING (1,184 rows)
-- =============================================================================

-- Business indexes
CREATE INDEX IF NOT EXISTS idx_team_recruiting_year ON recruiting.team_recruiting(year);
CREATE INDEX IF NOT EXISTS idx_team_recruiting_team ON recruiting.team_recruiting(team);
CREATE INDEX IF NOT EXISTS idx_team_recruiting_rank ON recruiting.team_recruiting(rank);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_team_recruiting_year_team ON recruiting.team_recruiting(year, team);

-- =============================================================================
-- TRANSFER PORTAL (14,356 rows)
-- =============================================================================

-- Business indexes
CREATE INDEX IF NOT EXISTS idx_transfer_portal_season ON recruiting.transfer_portal(season);
CREATE INDEX IF NOT EXISTS idx_transfer_portal_position ON recruiting.transfer_portal(position);
CREATE INDEX IF NOT EXISTS idx_transfer_portal_origin ON recruiting.transfer_portal(origin);
CREATE INDEX IF NOT EXISTS idx_transfer_portal_destination ON recruiting.transfer_portal(destination);
CREATE INDEX IF NOT EXISTS idx_transfer_portal_stars ON recruiting.transfer_portal(stars);
CREATE INDEX IF NOT EXISTS idx_transfer_portal_transfer_date ON recruiting.transfer_portal(transfer_date);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_transfer_portal_season_position ON recruiting.transfer_portal(season, position);
CREATE INDEX IF NOT EXISTS idx_transfer_portal_season_origin ON recruiting.transfer_portal(season, origin);
CREATE INDEX IF NOT EXISTS idx_transfer_portal_season_destination ON recruiting.transfer_portal(season, destination);
