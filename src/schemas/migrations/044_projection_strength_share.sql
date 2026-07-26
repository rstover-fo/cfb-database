-- Season projections: record the correlated-draw strength share
-- =============================================================================
-- Preseason outlook plan (docs/plans/2026-07-25-preseason-outlook-model-plan.md),
-- simulation v1.1.
--
-- WHY. Migration 043 stores residual_sigma per row so "a projection always
-- carries the assumption that produced it". v1.1 introduces a SECOND
-- distributional assumption -- the share of margin variance carried by a
-- per-team season-strength offset -- and the same rule applies. Without this
-- column, two rows simulated under materially different correlation
-- structures would be indistinguishable, and the append-only history could
-- not explain why an interval widened on the day the share changed.
--
-- The share does not move projected_wins: total per-game margin variance is
-- held at sigma^2 by construction (2*tau^2 + game_sd^2 == sigma^2), and the
-- 2019-2025 backtest measured win MAE of 1.784 at EVERY share swept. What it
-- moves is the spread -- p10-p90 coverage went 71.7% at share 0.00 to 79.6%
-- at 0.15 against a nominal 80%, which is how the shipped default was chosen.
--
-- NULL means "written before v1.1" -- i.e. an independent-draw row, share 0
-- semantically, but recorded as unknown rather than back-filled with a value
-- the writer never actually used.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-028 and 041-043. Idempotent.

ALTER TABLE predictions.season_projections
    ADD COLUMN IF NOT EXISTS strength_share NUMERIC(4, 3);

COMMENT ON COLUMN predictions.season_projections.strength_share IS
    'Share of per-game margin variance carried by the per-team season-strength offset (simulation v1.1). 0 = independent draws (v1); NULL = row written before v1.1 existed. Total per-game variance is held at residual_sigma^2 regardless, so this changes the WIDTH of the win distribution, not projected_wins. Calibrated against backtest p10-p90 coverage.';

-- Supersedes the v1 caveat: the tails are no longer understated by
-- construction, though offsets remain independent ACROSS teams.
COMMENT ON COLUMN predictions.season_projections.n_sims IS
    'Simulation count. From v1.1 each simulation draws one season-strength offset per team and applies it to every game that team plays (see strength_share), so win-total tails are no longer understated the way independent per-game draws made them. Remaining simplification: offsets are independent across teams, so a conference whose teams all overperform together is still underweighted.';
