-- Ratings schema hardening: indexes for SP+, Elo, FPI, SRS ratings
-- ALTER-based migration against dlt's existing auto-created tables
-- All statements are idempotent (IF NOT EXISTS)

-- =============================================================================
-- SP RATINGS (800 rows)
-- =============================================================================

-- Business indexes
CREATE INDEX IF NOT EXISTS idx_sp_ratings_year ON ratings.sp_ratings(year);
CREATE INDEX IF NOT EXISTS idx_sp_ratings_team ON ratings.sp_ratings(team);
CREATE INDEX IF NOT EXISTS idx_sp_ratings_conference ON ratings.sp_ratings(conference);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_sp_ratings_year_team ON ratings.sp_ratings(year, team);

-- =============================================================================
-- ELO RATINGS (791 rows)
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_elo_ratings_year ON ratings.elo_ratings(year);
CREATE INDEX IF NOT EXISTS idx_elo_ratings_team ON ratings.elo_ratings(team);
CREATE INDEX IF NOT EXISTS idx_elo_ratings_conference ON ratings.elo_ratings(conference);

CREATE INDEX IF NOT EXISTS idx_elo_ratings_year_team ON ratings.elo_ratings(year, team);

-- =============================================================================
-- FPI RATINGS (791 rows)
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_fpi_ratings_year ON ratings.fpi_ratings(year);
CREATE INDEX IF NOT EXISTS idx_fpi_ratings_team ON ratings.fpi_ratings(team);
CREATE INDEX IF NOT EXISTS idx_fpi_ratings_conference ON ratings.fpi_ratings(conference);

CREATE INDEX IF NOT EXISTS idx_fpi_ratings_year_team ON ratings.fpi_ratings(year, team);

-- =============================================================================
-- SRS RATINGS (1,258 rows)
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_srs_ratings_year ON ratings.srs_ratings(year);
CREATE INDEX IF NOT EXISTS idx_srs_ratings_team ON ratings.srs_ratings(team);
CREATE INDEX IF NOT EXISTS idx_srs_ratings_conference ON ratings.srs_ratings(conference);

CREATE INDEX IF NOT EXISTS idx_srs_ratings_year_team ON ratings.srs_ratings(year, team);
