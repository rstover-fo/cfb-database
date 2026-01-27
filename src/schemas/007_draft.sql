-- Draft schema hardening: indexes for draft picks
-- ALTER-based migration against dlt's existing auto-created tables
-- All statements are idempotent (IF NOT EXISTS)

-- =============================================================================
-- DRAFT PICKS (1,549 rows)
-- =============================================================================

-- Business indexes
CREATE INDEX IF NOT EXISTS idx_draft_picks_year ON draft.draft_picks(year);
CREATE INDEX IF NOT EXISTS idx_draft_picks_round ON draft.draft_picks(round);
CREATE INDEX IF NOT EXISTS idx_draft_picks_position ON draft.draft_picks(position);
CREATE INDEX IF NOT EXISTS idx_draft_picks_college_team ON draft.draft_picks(college_team);
CREATE INDEX IF NOT EXISTS idx_draft_picks_nfl_team ON draft.draft_picks(nfl_team);
CREATE INDEX IF NOT EXISTS idx_draft_picks_college_athlete_id ON draft.draft_picks(college_athlete_id);
CREATE INDEX IF NOT EXISTS idx_draft_picks_college_conference ON draft.draft_picks(college_conference);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_draft_picks_year_round ON draft.draft_picks(year, round);
CREATE INDEX IF NOT EXISTS idx_draft_picks_year_position ON draft.draft_picks(year, position);
