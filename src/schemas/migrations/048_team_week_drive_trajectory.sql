-- features.team_week: off_ppd
-- =============================================================================
-- Starter Pack model feature candidates plan
-- (docs/plans/2026-07-28-001-feat-starter-pack-model-features-plan.md), U2.
--
-- Column contract authority: migration 028's header forbids adding columns to
-- features.team_week without first updating
-- docs/brainstorms/2026-07-21-team-week-feature-design.md. That doc has been
-- amended -- section 1d carries this row (it is a season-to-date offensive
-- production column, same family as off_epa_per_play), section 1i's NULL rule
-- covers it by the existing season-to-date rule, and the column count goes
-- 39 -> 40.
--
-- WHY. Of the six drive-efficiency/weekly-trajectory candidates the Starter
-- Pack triage surfaced (KTD4), five retired on the pre-registered in-season
-- screen (scripts/screen_week_features.py, |partial_r| >= 0.08, BH-FDR
-- q <= 0.10): def_ppd_allowed, off_field_pos, def_field_pos_allowed,
-- form_net_epa_last4, vol_net_epa. off_ppd is the sole survivor -- v1 control
-- (Elo + adj_epa_net diffs) measured +0.0700 (just under the 0.08 floor); the
-- v2 amendment (model-margin control, holding fitted_v1's frozen walk-forward
-- expected margin constant) measured +0.0901 (p=5.7e-12, BH pass, floor pass).
-- See the design doc's 2026-07-28 entries for the full run history and the
-- other five candidates' final numbers.
--
-- A screen pass ships a COLUMN, not a fitted-vector slot (KTD2). off_ppd earns
-- this column and enters the isolated walk-forward evaluation (U4) under the
-- plan's strict no-regression gate (held-out MAE improves, Brier and ATS
-- hold against the production fitted_v1 baseline); it enters DIFF_FEATURE_COLUMNS
-- only if that gate passes. Until then it is populated but NOT in the model.
--
-- WHAT IT IS. Offensive points per drive, season-to-date as-of entering week W:
-- mean(end_offense_score - start_offense_score) over the team's own offensive
-- drives with week_index < W, same season (KTD6 -- exact score-delta points
-- from core.drives, not the TD=7/FG=3 estimate marts.scoring_opportunities
-- uses). Same season-to-date family and leak rule as off_epa_per_play (design
-- doc section 1d): NULL, never 0, when no qualifying drive exists yet (week 1).
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-028 and 041-047. Idempotent.

ALTER TABLE features.team_week
    ADD COLUMN IF NOT EXISTS off_ppd NUMERIC(8, 5);

COMMENT ON COLUMN features.team_week.off_ppd IS
    'Offensive points per drive, season-to-date as-of entering week W: mean(end_offense_score - start_offense_score) over the team''s own offensive drives from core.drives with week_index < W, same season (exact score-delta points, not a TD=7/FG=3 estimate). NULL, never 0, when the team has no qualifying prior drive (week 1) -- same season-to-date NULL rule as off_epa_per_play. Screened at +0.0700 (v1, Elo/adj_epa_net control) and +0.0901 (v2, model-margin control, p=5.7e-12) against home margin -- the sole survivor of the six 2026-07-28 drive/trajectory candidates. Populated for transparency; enters the fitted_v1 vector only if the U4 isolated walk-forward gate passes (see docs/plans/2026-07-28-001-feat-starter-pack-model-features-plan.md).';
