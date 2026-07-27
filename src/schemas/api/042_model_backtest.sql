-- api.model_backtest
-- Latest walk-forward preseason backtest per (model, scope, season range,
-- strength share): how wrong the season-win projections actually were.
-- Thin latest-snapshot view over predictions.model_backtest (migration 045).
--
-- WHY THIS VIEW EXISTS. Every downstream answer that quotes a season win total
-- has to carry the honesty numbers beside it -- the MAE, the empirical 80%
-- interval, the baselines the model is supposed to beat. Those numbers existed
-- only as a printed BACKTEST_GATE log line, so cfb-app hardcoded them as a
-- TypeScript constant (SEASON_OUTLOOK_ACCURACY). That is correct until the
-- backtest is re-run, at which point the shipped numbers describe a model that
-- no longer exists and nothing anywhere fails. This view is the fix: the
-- consumer reads the measurement live, and `run_date` makes staleness
-- assertable instead of invisible.
--
-- The api schema is the ONLY surface downstream consumers can reach.
-- public.run_analyst_query drops to the analyst_ro NOLOGIN role, which holds
-- USAGE+SELECT on `api` and deliberately nothing else -- see
-- public/validation_run_analyst_query.sql, which FAILS if analyst_ro can reach
-- core or marts. So neither predictions.model_backtest nor the features.*
-- substrate it measures is reachable from cfb-app or cfb-scout; this view is.
--
-- api views are owner-rights (NOT security_invoker -- see
-- public/012_run_analyst_query.sql), so reading through this view does not
-- require the caller to hold grants on the predictions schema. Same mechanism
-- api.season_outlook and api.game_predictions already rely on.
--
-- LATEST PER CONFIGURATION, NOT PER MODEL. predictions.model_backtest is
-- append-only across days, and DISTINCT ON picks the most recent run_date per
-- (model_version, scope, season_start, season_end, strength_share) -- the same
-- idiom as api.season_outlook.
--
-- The key is the configuration and not just the model on purpose. An
-- --all-divisions run measures a different population (CFBD files the whole
-- FCS/D2 playoff bracket as season_type='regular', so those slates run long);
-- a different season range measures different seasons; a different
-- --strength-share changes the width of every simulated distribution and
-- therefore `coverage`. Collapsing those onto one row per model would hand a
-- consumer whichever ran last. Ask for the canonical configuration instead:
--   ?model_version=eq.fitted_v1&scope=eq.fbs
-- and if that returns more than one row, the season range or the strength
-- share moved -- which is information, not noise.
--
-- HISTORY LIVES IN THE BASE TABLE. Accuracy drift across annual refits is
-- real and worth watching, but a consumer attaching an interval to one answer
-- needs exactly one row, and making them order-and-limit their way to it is
-- how the wrong row gets quoted. Day-by-day history stays in
-- predictions.model_backtest for anyone with direct access; if drift ever
-- needs to be downstream-visible it should be a SEPARATE
-- api.model_backtest_history view, not a shape change here.
--
-- NO ROW MEANS NEVER MEASURED. A model version with no backtest gets no row,
-- deliberately, rather than a row of NULLs. This does not contradict the
-- house rule that NULL means unknown and never zero -- that rule governs a
-- COLUMN inside a measured row (see season_projections.playoff_prob). A
-- synthesized all-NULL row would assert that a run happened and produced
-- nothing, and the view would have to know which model versions ought to
-- exist, which nothing in this database does. Consumers must render the empty
-- case as "accuracy not measured for this model" and must not present that
-- model's projections with an implied interval.
--
-- PostgREST usage:
--   GET /api/model_backtest?model_version=eq.fitted_v1&scope=eq.fbs
--   GET /api/model_backtest?select=win_mae,resid_p10,resid_p90,run_date

CREATE OR REPLACE VIEW api.model_backtest AS
SELECT DISTINCT ON (model_version, scope, season_start, season_end, strength_share)
    backtest_id,
    computed_at,
    -- Read this. It is what makes staleness detectable: a consumer that
    -- caches these numbers can compare run_date against its own copy and fail
    -- loudly, which is the whole reason the table exists.
    run_date,

    -- What was measured against what. The coefficients (model_version) and
    -- the feature substrate they were read through (feature_build_version)
    -- are both required to identify a model; the same coefficients over a
    -- rebuilt substrate is a different model in every way that matters.
    model_version,
    feature_build_version,
    scope,

    -- Requested range vs what actually contributed. They differ when a season
    -- had no frozen S-1 fit or too few prior residuals for a sigma estimate,
    -- so never assume the requested range was fully measured.
    season_start,
    season_end,
    seasons_covered,
    train_through_min,
    train_through_max,

    -- Simulation provenance travelling with the numbers, for the same reason
    -- season_projections carries residual_sigma: a measurement always carries
    -- the assumptions that produced it.
    n_sims,
    seed,
    strength_share,

    -- Leak evidence. 0 is the honest expectation; above 0 means some "week-1"
    -- vector already contained that season's own games and every error metric
    -- below is understated. Surfaced rather than buried so a consumer quoting
    -- these numbers can see the qualification.
    max_games_played_to_date,
    games_dropped_outcome_dependent,

    -- The measurements
    n,
    win_mae,
    rmse,
    bias,
    coverage,
    baseline_prior_mae,
    baseline_flat_mae,

    -- Derived in the view, never stored: a comparison between two measured
    -- columns cannot go stale relative to them, whereas a stored copy could.
    -- A preseason model that cannot beat "last year's record scaled to this
    -- slate" has earned nothing, and that is the question this answers.
    win_mae < baseline_prior_mae AS beats_prior_baseline,
    win_mae < baseline_flat_mae AS beats_flat_baseline,

    -- The interval. Use resid_p10/resid_p90 as an ASYMMETRIC 80% band around
    -- a projection: [projected_wins + resid_p10, projected_wins + resid_p90].
    -- Do NOT use +/- win_mae -- MAE is an average loss, and for a roughly
    -- normal error that band spans only about +/-0.8 SD, near 58% coverage
    -- rather than the ~80% a reader assumes from a stated range.
    resid_p05,
    resid_p10,
    resid_p25,
    resid_p50,
    resid_p75,
    resid_p90,
    resid_p95,

    -- Honesty numbers for the probability claims api.season_outlook publishes
    -- (p_bowl_eligible, p_ten_plus). Brier as a scalar for the common
    -- question; `calibration` holds the reliability buckets behind it.
    bowl_brier,
    ten_plus_brier,
    calibration,

    -- The advisory bar the run was evaluated against (plan 4.5). The verdict
    -- itself is deliberately NOT stored or exposed -- a stored judgement goes
    -- stale when the bar is revised while the measurement does not. Apply
    -- your own bar, or compute win_mae <= respectable_win_mae.
    respectable_win_mae
FROM predictions.model_backtest
ORDER BY model_version, scope, season_start, season_end, strength_share, run_date DESC;

-- Grants are part of the definition: an apply that DROPs/recreates the view
-- would otherwise leave the PostgREST roles without read access (no ALTER
-- DEFAULT PRIVILEGES for them in this database).
GRANT SELECT ON api.model_backtest TO anon, authenticated;

-- analyst_ro is covered by ALTER DEFAULT PRIVILEGES IN SCHEMA api
-- (public/012_run_analyst_query.sql), but only when the view is created by the
-- role that set those defaults. Granting explicitly removes that dependency --
-- and since unblocking analyst_ro is half the point of this view, inheriting
-- the grant silently is not good enough. Guarded so a database that has not
-- applied 012 yet still applies this file.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst_ro') THEN
        -- EXECUTE rather than a bare GRANT: the unambiguously portable way to
        -- run conditional DDL from PL/pgSQL.
        EXECUTE 'GRANT SELECT ON api.model_backtest TO analyst_ro';
    ELSE
        RAISE NOTICE 'analyst_ro not present; skipping grant (apply public/012_run_analyst_query.sql first)';
    END IF;
END
$$;

COMMENT ON VIEW api.model_backtest IS 'Latest walk-forward preseason backtest per (model_version, scope, season_start, season_end, strength_share), from the append-only predictions.model_backtest log. Columns: backtest_id, computed_at, run_date, model_version, feature_build_version, scope, season_start, season_end, seasons_covered, train_through_min, train_through_max, n_sims, seed, strength_share, max_games_played_to_date, games_dropped_outcome_dependent, n, win_mae, rmse, bias, coverage, baseline_prior_mae, baseline_flat_mae, beats_prior_baseline, beats_flat_baseline, resid_p05/p10/p25/p50/p75/p90/p95, bowl_brier, ten_plus_brier, calibration, respectable_win_mae. Measures season-win projection accuracy by re-scoring every game of season S from each team''s week-1 feature vector with the frozen S-1 fit, then simulating; n counts TEAM-SEASONS, not games. Use resid_p10/resid_p90 as an ASYMMETRIC 80% interval around projected_wins -- NOT +/- win_mae, which spans only ~58% for a normal error. scope=''fbs'' is the default reporting population; ''all_divisions'' includes FCS/D2, whose slates run long because CFBD labels their playoff bracket season_type=''regular''. seasons_covered may be shorter than [season_start, season_end] when a season had no frozen S-1 fit. max_games_played_to_date above 0 means some week-1 vector was not truly preseason and the errors are understated. NO ROW for a model version means it has never been backtested -- render that as unmeasured, never as an unqualified projection. Query predictions.model_backtest directly for run-by-run history.';
