-- Model backtest: append-only accuracy snapshots for season-win projections
-- =============================================================================
-- Preseason outlook plan (docs/plans/2026-07-25-preseason-outlook-model-plan.md),
-- sections 4.3/4.5 -- the gate that must run before a 2026 win total means
-- anything.
--
-- predictions.model_backtest holds one immutable snapshot row per
-- (model_version, UTC run_date, scope, season range, strength_share): what
-- scripts/backtest_preseason.py measured for that configuration on that day.
--
-- WHY THIS TABLE EXISTS. The backtest produced a printed log line and nothing
-- else:
--
--   BACKTEST_GATE model=fitted_v1 n=921 win_mae=1.743 rmse=2.168 bias=-0.126
--   coverage=0.800 baseline_prior_mae=2.128 baseline_flat_mae=2.140
--   resid_p10=-2.684 resid_p90=+3.022 verdict=above_1.5
--
-- Every downstream answer that quotes a season win total has to carry those
-- honesty numbers -- the MAE, the asymmetric 80% interval
-- [proj-2.68, proj+3.02], the baselines -- and because nothing was queryable,
-- cfb-app hardcoded them as a TypeScript constant. That works exactly until
-- the backtest is re-run, at which point the shipped numbers describe a model
-- that no longer exists and NOTHING fails. Same silent-staleness shape as the
-- fitted_v1 coverage gap and the finished-season re-ingest: a stale value that
-- still renders is worse than an error, because nobody looks.
--
-- Append-only across days, exactly like predictions.game_predictions
-- (migration 024) and predictions.season_projections (043): the same-day
-- ON CONFLICT DO UPDATE only lets a re-run *converge* today's snapshot, never
-- overwrite a prior day's. The history is the point -- it is the only way to
-- see accuracy drift across refits, e.g. whether the annual train_model.py
-- refit made the projections better or worse.
--
-- GRAIN -- WHY THE KEY IS SIX COLUMNS AND NOT TWO.
-- (model_version, run_date) alone is too narrow. `--all-divisions` measures a
-- materially different population (CFBD files the entire FCS/D2 playoff
-- bracket as season_type='regular', so those slates run long), a different
-- --start/--end measures different seasons, and --strength-share changes the
-- width of every simulated distribution and therefore `coverage` -- the
-- 2026-07-26 sweep moved it 71.7% at 0.00 to 79.6% at 0.15 with win MAE
-- unchanged at 1.784. Keying on the model and the day alone would let the
-- second run of the day silently replace the first with a number that answers
-- a different question, which is the failure this table exists to end.
--
-- n_sims and seed are deliberately NOT in the key. They are Monte Carlo
-- nuisance parameters: they move the measurement by simulation noise, they do
-- not change WHAT is being measured. A same-day re-run at a different --sims
-- converges onto today's row and records the sims it actually used, so the
-- row still explains itself.
--
-- Every key column is NOT NULL because a NULL in a unique index does not
-- conflict with another NULL: a nullable key column would let two runs of the
-- same configuration on the same day insert two rows instead of converging,
-- which is precisely the duplicate this design is built to prevent.
--
-- Writer: scripts/backtest_preseason.py (single-share reporting path only --
-- --sweep-strength-share is a calibration exercise over candidate
-- configurations, not a measurement of the shipped model, and persisting a
-- sweep would fill the table with rows nobody should quote).
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-028 and 041-044. Idempotent (IF NOT EXISTS throughout).

CREATE SCHEMA IF NOT EXISTS predictions;

CREATE TABLE IF NOT EXISTS predictions.model_backtest (
    backtest_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_date DATE NOT NULL DEFAULT ((now() AT TIME ZONE 'utc')::date),

    -- What was measured. model_version is the fit being scored (fitted_v1);
    -- feature_build_version is the features.team_week substrate the week-1
    -- vectors were read from. Both are needed: the same coefficients over a
    -- rebuilt feature substrate is a different model in every way that
    -- matters, and a number with no idea which model produced it is the thing
    -- this table exists to stop.
    model_version TEXT NOT NULL,
    feature_build_version TEXT,

    -- 'fbs' (default reporting scope) or 'all_divisions'. Part of the key
    -- because the two measure different populations -- see the header.
    scope TEXT NOT NULL,

    -- The REQUESTED reporting range (--start/--end). seasons_covered below is
    -- what actually contributed; the two differ whenever a season had no
    -- frozen S-1 fit or too few prior residuals to estimate sigma.
    season_start BIGINT NOT NULL,
    season_end BIGINT NOT NULL,

    -- The seasons that actually produced team-season rows, in order. This is
    -- the honest denominator: a reader must never assume the requested range
    -- was fully measured. Seasons BEFORE the first entry may still have been
    -- scored to seed the residual sigma -- they contribute no metrics and are
    -- deliberately not listed.
    seasons_covered BIGINT[] NOT NULL,

    -- Span of frozen fits exercised. Season S is always scored with the
    -- train_through = S-1 fit (walk-forward: S is never in its own fit's train
    -- window), so this is min/max of seasons_covered minus one -- stored
    -- explicitly rather than left to be re-derived by a reader who would have
    -- to know that rule.
    train_through_min BIGINT NOT NULL,
    train_through_max BIGINT NOT NULL,

    -- Simulation provenance. strength_share is in the key (it sets the width
    -- of every distribution, hence `coverage`); n_sims and seed are recorded
    -- but not keyed -- see the header.
    n_sims BIGINT NOT NULL,
    seed BIGINT NOT NULL,
    strength_share NUMERIC(4, 3) NOT NULL,

    -- Leak evidence, carried with the numbers rather than left in a log line.
    -- Every "week-1" vector should have games_played_to_date = 0; anything
    -- above 0 means a row already had games of its own season baked in and
    -- every error metric below is understated. Non-zero does not invalidate
    -- the row -- it qualifies it, and a consumer quoting these numbers is
    -- entitled to know.
    max_games_played_to_date BIGINT NOT NULL,
    -- Games removed by the chronological 12-game slate cap: conference
    -- championships and FCS/D2 playoff rounds, whose PARTICIPANTS were decided
    -- by that season's results. Measured at 0.3-1.2% of team-games per season
    -- (2019-2025).
    games_dropped_outcome_dependent BIGINT NOT NULL,

    -- ==== The measurements ====================================================

    -- Team-seasons compared. Named `n` to match the BACKTEST_GATE log line
    -- exactly, so a row and the log it came from can be diffed by eye.
    n BIGINT NOT NULL,

    -- Mean absolute error in WINS, the headline honesty number. NUMERIC(6,3)
    -- holds +/-999.999 at the 3 decimals the gate line prints, so a stored
    -- value and a printed value are the same characters.
    win_mae NUMERIC(6, 3),
    rmse NUMERIC(6, 3),
    -- Signed mean(projected - actual): a model can be accurate on average and
    -- still systematically optimistic, and only the signed term shows it.
    bias NUMERIC(6, 3),
    -- Share of teams whose actual wins fell inside the simulated [p10, p90].
    -- Nominal 0.80. Stored at 4 decimals -- one more than the gate prints --
    -- so nothing is lost on the way in.
    coverage NUMERIC(5, 4),

    -- An MAE means nothing alone. Both baselines share the model's denominator
    -- (the simulated slate, not the full schedule): prior-season win RATE
    -- scaled to that slate, and a flat .500. A preseason model that cannot
    -- beat "last year's record" has earned nothing.
    baseline_prior_mae NUMERIC(6, 3),
    baseline_flat_mae NUMERIC(6, 3),

    -- Empirical quantiles of (actual - projected) wins. THESE are the interval,
    -- not +/- win_mae: MAE is an average loss, and for a roughly normal error
    -- a +/-MAE band spans only about +/-0.8 SD (~58% coverage), not the ~80%
    -- a reader assumes from a stated range. p10/p90 give the 80% band and it
    -- is ASYMMETRIC -- 2026-07-26 measured [-2.684, +3.022], i.e. the upper
    -- tail of the error is the wider one, so mirroring either end would
    -- misstate the interval. Read both.
    resid_p05 NUMERIC(6, 3),
    resid_p10 NUMERIC(6, 3),
    resid_p25 NUMERIC(6, 3),
    resid_p50 NUMERIC(6, 3),
    resid_p75 NUMERIC(6, 3),
    resid_p90 NUMERIC(6, 3),
    resid_p95 NUMERIC(6, 3),

    -- Brier scores for the two threshold probabilities the outlook surface
    -- publishes (api.season_outlook.p_bowl_eligible / p_ten_plus). First-class
    -- columns rather than JSON keys because "how good is that 62%" is the
    -- common question and should not need a JSON traversal.
    bowl_brier NUMERIC(6, 5),
    ten_plus_brier NUMERIC(6, 5),

    -- Reliability buckets for the same two probabilities:
    -- {"p_bowl_eligible": [{bucket, n, mean_predicted, observed}, ...], ...}.
    --
    -- WHY JSONB AND NOT SEPARATE COLUMNS -- and why it is in scope at all.
    -- It is variable-length (empty buckets are omitted), so it cannot be a
    -- fixed column set without inventing rows. It is in scope because
    -- api.season_outlook publishes p_bowl_eligible and p_ten_plus as
    -- probability CLAIMS, and the Brier scalars above cannot say the thing a
    -- consumer needs -- that the 0.8-0.9 bucket came in at 0.72. Dropping it
    -- would leave that measurement living only in the printed log this table
    -- exists to replace. Ten-ish objects per probability; the size is
    -- irrelevant next to the honesty.
    --
    -- Deliberately NOT stored: the per-season breakdown. Those numbers are a
    -- model-development diagnostic (read the printed table), and no downstream
    -- surface exposes per-season projection accuracy. seasons_covered records
    -- which seasons went in, which is what a consumer of the aggregate needs.
    calibration JSONB,

    -- The advisory bar the run was evaluated against (plan 4.5: "~1.5 wins MAE
    -- is respectable"). Stored as the PARAMETER it is, so the printed
    -- verdict=above_1.5 can be reconstructed from the row.
    --
    -- The VERDICT itself is deliberately not stored. A stored judgement is
    -- worse than a stored measurement: it freezes one team's threshold into
    -- the contract, and it goes stale the moment anyone revises what
    -- "respectable" means -- while win_mae stays true forever. A consumer that
    -- wants a verdict computes `win_mae <= respectable_win_mae`, or applies
    -- its own bar, which is the point.
    respectable_win_mae NUMERIC(4, 2)
);

-- One snapshot per configuration per UTC day (mirrors
-- season_projections_daily_key in migration 043 and
-- game_predictions_daily_key in 024). All six columns are NOT NULL above, so
-- a same-day re-run of the same configuration always CONFLICTS and converges
-- rather than inserting a second row.
CREATE UNIQUE INDEX IF NOT EXISTS model_backtest_daily_key
    ON predictions.model_backtest (
        model_version, run_date, scope, season_start, season_end, strength_share
    );

-- Supports api.model_backtest's DISTINCT ON (...) ORDER BY ... run_date DESC.
-- The daily key cannot serve it: run_date sits second there, so the ordering
-- the view needs is not a prefix of it.
CREATE INDEX IF NOT EXISTS model_backtest_latest_idx
    ON predictions.model_backtest (
        model_version, scope, season_start, season_end, strength_share, run_date DESC
    );

CREATE INDEX IF NOT EXISTS model_backtest_computed_at_idx
    ON predictions.model_backtest (computed_at);

COMMENT ON TABLE predictions.model_backtest IS
    'Append-only walk-forward preseason backtest snapshots: one immutable row per (model_version, UTC run_date, scope, season_start, season_end, strength_share), same-day ON CONFLICT DO UPDATE for intra-day convergence only. Measures season-win projection accuracy by re-scoring every game of season S from each team''s week-1 features.team_week vector using the frozen S-1 fit. Written by scripts/backtest_preseason.py; read downstream through api.model_backtest.';

COMMENT ON COLUMN predictions.model_backtest.n IS
    'Team-seasons compared. Named to match the BACKTEST_GATE log line so a row and its log can be diffed by eye.';

COMMENT ON COLUMN predictions.model_backtest.seasons_covered IS
    'Seasons that actually contributed team-season rows, ascending. May be shorter than [season_start, season_end] when a season had no frozen S-1 fit or too few prior residuals for a sigma estimate. Seasons scored only to seed sigma are not listed -- they contribute no metrics.';

COMMENT ON COLUMN predictions.model_backtest.coverage IS
    'Share of teams whose actual wins landed inside the simulated [p10, p90]; nominal 0.80. Materially below means the projection claims more confidence than it has; materially above means it is too wide to be useful.';

COMMENT ON COLUMN predictions.model_backtest.resid_p10 IS
    'p10 of (actual - projected) wins. With resid_p90 this is the empirical 80% interval around a projection: [projected + resid_p10, projected + resid_p90]. It is ASYMMETRIC and it is NOT +/- win_mae -- a +/-MAE band spans only ~58% for a normal error.';

COMMENT ON COLUMN predictions.model_backtest.max_games_played_to_date IS
    'Largest games_played_to_date on any joined week-1 vector. 0 is the honest expectation; above 0 means some "week-1" row already contained that season''s own games and every error metric on this row is understated.';

COMMENT ON COLUMN predictions.model_backtest.calibration IS
    'Reliability buckets per published probability: {"p_bowl_eligible": [{bucket, n, mean_predicted, observed}, ...], "p_ten_plus": [...]}. Empty buckets are omitted rather than reported as 0/0.';

COMMENT ON COLUMN predictions.model_backtest.respectable_win_mae IS
    'The advisory bar in force for this run (plan 4.5). The verdict is NOT stored: a stored judgement goes stale when the bar is revised, while the measurement does not. Compute win_mae <= respectable_win_mae, or apply your own bar.';

-- Grant USAGE + read-only SELECT per the repo's read-access pattern
-- (see grant_read_access_for_security_invoker.sql), matching 024/028/043 --
-- no write grants to anon/authenticated; writes come only from the compute
-- scripts via the direct connection owner.
GRANT USAGE ON SCHEMA predictions TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA predictions TO anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA predictions FROM anon, authenticated;
