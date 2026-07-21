-- Features schema: team-week modeling substrate + fitted_v1 walk-forward fits
-- =============================================================================
-- Tier 3 analytics (docs/plans/2026-07-21-tier3-analytics-plan.md), Pillars B
-- (fitted/calibrated models) and C (features.team_week), Phase 1.
--
-- The `features` schema holds the modeling substrate for `fitted_v1` (ridge
-- margin + IRLS/Platt win-prob):
--   - features.team_week -- one row per (season, season_type, week, team),
--     the as-of feature vector entering that team's game. Written by
--     scripts/build_features.py.
--   - features.model_coefficients / features.model_metadata -- frozen
--     walk-forward fits (one fit per train_through_season), written by
--     scripts/train_model.py and read by scripts/score_fitted.py.
--
-- Column contract: every column, type, source, and leak rule below is fixed
-- by docs/brainstorms/2026-07-21-team-week-feature-design.md (THE
-- authoritative spec -- section 1 for features.team_week, section 2d for
-- model_coefficients/model_metadata). Do not add, rename, or retype columns
-- here without updating that doc first.
--
-- As-of / leak-free convention (design doc section 0, shared with migration
-- 026's analytics.adjusted_epa_week_build): a row keyed to `week_index = WI`
-- may only use data with `week_index < WI` within the same season, plus
-- explicitly leak-free preseason constants and prior-season fallbacks known
-- before the season starts.
--
--   week_index = week                          when season_type = 'regular'
--   week_index = 100 + week                     when season_type = 'postseason'
--
-- These tables are created empty here so schema and grants are in place
-- before build_features.py / train_model.py / score_fitted.py land in later
-- phases.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-026. Idempotent (IF NOT EXISTS throughout).

CREATE SCHEMA IF NOT EXISTS features;

-- -----------------------------------------------------------------------------
-- features.team_week -- one row per (season, season_type, week, team): the
-- as-of feature vector entering that team's game (design doc section 1).
-- Grain: a team plays <=1 game/week, so both the home and away side of every
-- core.games row get a row here. Spine driver is core.games team-sides
-- (completed + scheduled), not ref.calendar (current-season metadata only).
-- Written by scripts/build_features.py.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS features.team_week (
    -- 1a. Identity / spine
    season BIGINT NOT NULL,
    season_type VARCHAR NOT NULL,
    week BIGINT NOT NULL,
    week_index BIGINT NOT NULL,
    team VARCHAR NOT NULL,
    conference VARCHAR,
    game_id BIGINT NOT NULL,
    games_played_to_date BIGINT NOT NULL,

    -- 1b. House Elo (pregame entering week W; walk-forward, never NULL)
    elo_pregame NUMERIC(8, 2),

    -- 1c. Adjusted EPA as-of (opponent-adjusted, entering week W; as-of week
    -- fit else prior-season (S-1) fallback else NULL -- see design doc's
    -- MIN_TEAM_PLAYS=150 predicate). adj_epa_source is the provenance flag
    -- for the leak audit: 'week' | 'prior_season' | NULL.
    adj_epa_off NUMERIC(8, 5),
    adj_epa_def NUMERIC(8, 5),
    adj_epa_net NUMERIC(8, 5),
    adj_epa_hfa NUMERIC(8, 5),
    adj_epa_source VARCHAR,

    -- 1d. Season-to-date raw production (marts.play_epa JOIN core.games,
    -- week_index < WI, garbage time excluded). NULL (not 0) when no plays
    -- exist yet -- the model layer imputes with a frozen train-window mean.
    off_epa_per_play NUMERIC(8, 5),
    off_success_rate NUMERIC(8, 5),
    off_explosiveness_rate NUMERIC(8, 5),
    off_plays_per_game NUMERIC(8, 3),
    def_epa_per_play_allowed NUMERIC(8, 5),
    def_success_rate_allowed NUMERIC(8, 5),
    def_explosiveness_rate_allowed NUMERIC(8, 5),

    -- 1e. Havoc, season-to-date (stats.game_havoc joined by game_id,
    -- week_index < WI; event-weighted rate). NULL when no games yet.
    havoc_rate_defense NUMERIC(8, 5),
    havoc_rate_offense_allowed NUMERIC(8, 5),

    -- 1f. Preseason-known constants (populated for ALL weeks, constant
    -- within season). preseason_sp_* is the prior-season (S-1) final SP+ as
    -- the leak-free preseason proxy (no true preseason SP+ snapshot loaded).
    returning_ppa_pct NUMERIC(8, 4),
    returning_passing_ppa_pct NUMERIC(8, 4),
    returning_rushing_ppa_pct NUMERIC(8, 4),
    returning_usage NUMERIC(8, 4),
    preseason_sp_rating NUMERIC(8, 3),
    preseason_sp_offense NUMERIC(8, 3),
    preseason_sp_defense NUMERIC(8, 3),

    -- 1g. Bookkeeping
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    feature_build_version VARCHAR
);

-- Grain key: one row per TEAM-GAME. (game_id, team), NOT the calendar key
-- (season, season_type, week, team): a team can play two postseason games
-- both numbered week 1 (CFP semifinal + championship), and data quirks can
-- duplicate a regular week -- see migration 030's header. The spine is
-- core.games team-sides, so game_id is never NULL.
CREATE UNIQUE INDEX IF NOT EXISTS team_week_key
    ON features.team_week (game_id, team);

-- Calendar-grain lookups (joins from rankings/predictions surfaces).
CREATE INDEX IF NOT EXISTS team_week_calendar_idx
    ON features.team_week (season, season_type, week, team);

-- As-of / ordering lookups keyed on the monotone week_index (design doc
-- section 0).
CREATE INDEX IF NOT EXISTS team_week_season_week_index_idx
    ON features.team_week (season, week_index);

-- Per-team history lookups.
CREATE INDEX IF NOT EXISTS team_week_team_season_idx
    ON features.team_week (team, season);

COMMENT ON TABLE features.team_week IS
    'As-of feature vector for fitted_v1, one row per (season, season_type, week, team) -- a team plays <=1 game/week, both sides of every core.games row get a row. week_index is the derived monotone ordering key (week_index = week for season_type=''regular'', 100 + week for ''postseason'', since CFBD restarts week numbering at 1 for bowls). As-of rule: the row for week_index WI uses only data with week_index < WI within the same season, plus explicitly leak-free preseason constants (returning_*, preseason_sp_*) and prior-season (S-1) fallbacks (adj_epa_* when adj_epa_source=''prior_season''), both known before the season starts. Column contract: docs/brainstorms/2026-07-21-team-week-feature-design.md section 1. Written by scripts/build_features.py.';

-- -----------------------------------------------------------------------------
-- features.model_coefficients -- one row per feature per component per
-- walk-forward fit (design doc section 2d). fitted_v1's design matrix is
-- fixed at 15 features + an unpenalized intercept (section 2a); feature_order
-- is that fixed column position, feature_name its label (e.g. 'd_elo').
-- Written by scripts/train_model.py.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS features.model_coefficients (
    model_version VARCHAR NOT NULL,
    train_through_season BIGINT NOT NULL,
    model_component VARCHAR NOT NULL,
    feature_order BIGINT NOT NULL,
    feature_name VARCHAR NOT NULL,
    coefficient NUMERIC(12, 6) NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS model_coefficients_key
    ON features.model_coefficients (model_version, train_through_season, model_component, feature_name);

COMMENT ON TABLE features.model_coefficients IS
    'Frozen walk-forward fitted_v1 coefficients: one row per feature per model_component (''margin'' ridge or ''winprob'' IRLS-logistic) per fit. Scoring season S uses the fit at train_through_season = S-1 (score_fitted.py selects it by that key, or MAX(train_through_season) for daily upcoming scoring); fits are never mutated in place, only inserted per train_through_season. Column contract: docs/brainstorms/2026-07-21-team-week-feature-design.md section 2d. Written by scripts/train_model.py.';

-- -----------------------------------------------------------------------------
-- features.model_metadata -- one row per walk-forward fit: hyperparameters,
-- Platt calibration, and the frozen imputation/standardization statistics
-- (train-window only, never recomputed at score time -- design doc section
-- 2b/2c). Written by scripts/train_model.py.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS features.model_metadata (
    model_version VARCHAR NOT NULL,
    train_through_season BIGINT NOT NULL,
    ridge_alpha NUMERIC(8, 3),
    winprob_ridge_alpha NUMERIC(8, 3),
    platt_a NUMERIC(12, 6),
    platt_b NUMERIC(12, 6),
    train_seasons BIGINT[],
    n_train_games BIGINT,
    feature_means JSONB,
    feature_diff_means JSONB,
    feature_diff_stds JSONB,
    fit_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS model_metadata_key
    ON features.model_metadata (model_version, train_through_season);

COMMENT ON TABLE features.model_metadata IS
    'One row per frozen walk-forward fitted_v1 fit: train_through_season = S-1 for scoring season S, hyperparameters (ridge_alpha, winprob_ridge_alpha), Platt calibration (platt_a/b), the explicit train_seasons list, and the frozen train-window-only statistics used at score time -- feature_means (imputation, {team_week_column: mean_c}), feature_diff_means/feature_diff_stds (z-score standardization, keyed by feature_name). Scoring never recomputes these; it reads the row for its train_through_season. Column contract: docs/brainstorms/2026-07-21-team-week-feature-design.md section 2d. Written by scripts/train_model.py.';

-- Grant USAGE + read-only SELECT per the repo's read-access pattern
-- (see grant_read_access_for_security_invoker.sql), matching 024/025/026/028
-- -- no write grants to anon/authenticated; writes come only from the compute
-- scripts via the direct connection owner.
GRANT USAGE ON SCHEMA features TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA features TO anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA features FROM anon, authenticated;
