-- features.team_week: five screened preseason columns
-- =============================================================================
-- Preseason outlook plan (docs/plans/2026-07-25-preseason-outlook-model-plan.md)
-- Phase 2, gated on the section 2.5 partial-correlation screen
-- (scripts/screen_preseason_features.py).
--
-- Column contract authority: migration 028's header forbids adding columns to
-- features.team_week without first updating
-- docs/brainstorms/2026-07-21-team-week-feature-design.md. That doc has been
-- amended -- section 1f carries all five rows, section 1i their NULL rule,
-- section 2a their position in the fitted_v1 vector, and the column count goes
-- 31 -> 36.
--
-- WHY THESE FIVE. Every candidate was screened against season-S SP+
-- controlling for BOTH prior-season SP+ and recruiting_points_3yr, over
-- 2015-2025 (n=1,439), against a pre-registered |partial_r| >= 0.08 floor with
-- Benjamini-Hochberg FDR at 0.10:
--
--     recruiting_points_3yr   +0.2642   strongest preseason signal in the set
--     hc_first_year           -0.1615   second strongest
--     prior_def_line_yards    -0.0997   yards ALLOWED, so negative confirms
--     prior_def_stuff_rate    +0.0816   clears the floor by 0.0016 -- marginal
--     blue_chip_pipeline      +0.0782   BELOW the floor; explicit override
--
-- blue_chip_pipeline is the one column here that did not clear the rule. It
-- misses by ~0.07 standard errors at this n -- a gap the data cannot resolve --
-- and its q-value is 0.0095, so the effect is real. Shipping it is a recorded
-- judgment (screen_preseason_features.SHIPPED_BY_DECISION), not a screen
-- result, and the two are kept in separate structures so the override stays
-- legible as an override.
--
-- WHAT FAILED, recorded so it is not re-proposed without new evidence: every
-- OFFENSIVE line measure (prior_line_yards +0.0223, prior_power_success
-- +0.0520, prior_stuff_rate_allowed -0.0290). The trenches thesis is supported
-- for the defensive front and unsupported for the offensive front as measured
-- by prior-season line play. The two havoc__front_seven splits are UNTESTABLE
-- rather than null -- they exist for only 17.4% of team-seasons.
--
-- NULL, NEVER 0 (design doc section 1i). These are rates and decayed sums, and
-- a team-season whose source row is absent has an UNKNOWN value, not a zero
-- one: no team posts 0.000 line yards. The teams that are missing -- new FBS
-- entrants, programs up from FCS -- are also disproportionately weak in season
-- S, so zero-filling would plant the floor exactly where the outcome is low and
-- manufacture signal. train_model.py imputes with a frozen train-window league
-- mean instead (section 2b). hc_first_year is the exception worth stating: 0.0
-- there is a real value ("established coach"), and NULL means only "no coach
-- record exists for this school-year".
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-028 and 041-044. Idempotent.

ALTER TABLE features.team_week
    ADD COLUMN IF NOT EXISTS recruiting_points_3yr NUMERIC(10, 3),
    ADD COLUMN IF NOT EXISTS blue_chip_pipeline    NUMERIC(6, 4),
    ADD COLUMN IF NOT EXISTS hc_first_year         NUMERIC(2, 1),
    ADD COLUMN IF NOT EXISTS prior_def_line_yards  NUMERIC(8, 4),
    ADD COLUMN IF NOT EXISTS prior_def_stuff_rate  NUMERIC(8, 4);

COMMENT ON COLUMN features.team_week.recruiting_points_3yr IS
    'Decayed sum of recruiting.team_recruiting.points over the four classes signed before season S (weight 0.8^(S-year-1), CLASS_WINDOW=4). Preseason-known: every class in the window signed before S began. Screened +0.2642 against season-S SP+ controlling for prior-season SP+ -- the strongest preseason signal in the candidate set, and the control every other candidate was screened against. NULL (never 0) when no recruiting rows exist for the window.';

COMMENT ON COLUMN features.team_week.blue_chip_pipeline IS
    'Share of signees rated 4-5 stars across the same four-class window (blue chips / total signees). Preseason-known. Screened +0.0782 controlling for prior SP+ AND recruiting_points_3yr -- BELOW the pre-registered 0.08 floor, shipped as an explicit recorded override because the shortfall is ~0.07 standard errors at n=1,439 and q=0.0095. A rate: NULL, never 0, when the window has no signees.';

COMMENT ON COLUMN features.team_week.hc_first_year IS
    '1.0 when the head coach''s tenure at this school begins in season S, else 0.0. Tenure computed gaps-and-islands over ref.coaches__seasons so a coach returning for a second stint does not inherit the first stint''s start year. Preseason-known: the hire precedes the season. Screened -0.1615 -- the second-strongest signal in the set, and previously invisible: it was hidden inside a zero-filled regime-scoped recruiting column until the two were separated. NULL means no coach record for this school-year; 0.0 is a real value.';

COMMENT ON COLUMN features.team_week.prior_def_line_yards IS
    'stats.advanced_team_stats.defense__line_yards for season S-1 -- line yards ALLOWED by this defense last season. Leak-free: S-1 is complete before S begins. Screened -0.0997 controlling for prior SP+ and recruiting; the sign is the trenches thesis CONFIRMED (allowing fewer line yards predicts a better season), not refuted. NULL, never 0, when the team had no prior FBS season.';

COMMENT ON COLUMN features.team_week.prior_def_stuff_rate IS
    'stats.advanced_team_stats.defense__stuff_rate for season S-1 -- rate at which this defense stuffed runs at or behind the line. Leak-free: S-1 is complete before S begins. Screened +0.0816, clearing the pre-registered 0.08 floor by 0.0016; treat as marginal. NULL, never 0, when the team had no prior FBS season.';
