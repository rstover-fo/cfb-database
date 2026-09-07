-- Approved F04 production cutover, submitted as ONE file/transaction.
-- Generated from the merged manifest; constituent source hashes are recorded below.
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '300s';
LOCK TABLE predictions.game_predictions IN ACCESS EXCLUSIVE MODE;
LOCK TABLE predictions.season_projections IN SHARE MODE;
DO $guard$
DECLARE r record;
BEGIN
    IF current_user <> 'postgres' THEN RAISE EXCEPTION 'Unexpected rollout owner'; END IF;
    FOR r IN SELECT c.oid,c.oid::regclass AS relation,c.relowner,c.relacl FROM pg_class c
        WHERE c.oid IN ('marts.scored_matchup_edges'::regclass,'marts.prediction_accuracy'::regclass)
    LOOP
        IF pg_get_userbyid(r.relowner) <> 'postgres' OR r.relacl IS NOT NULL THEN
            RAISE EXCEPTION 'Mart owner/grants changed since preflight: %',r.relation;
        END IF;
    END LOOP;
    FOR r IN
        WITH RECURSIVE closure(oid) AS (
            SELECT oid FROM pg_class WHERE oid IN
                ('marts.scored_matchup_edges'::regclass,'marts.prediction_accuracy'::regclass)
            UNION
            SELECT rw.ev_class FROM closure c
            JOIN pg_depend d ON d.refclassid='pg_class'::regclass AND d.refobjid=c.oid
            JOIN pg_rewrite rw ON d.classid='pg_rewrite'::regclass AND rw.oid=d.objid
            WHERE rw.ev_class<>c.oid
        ) SELECT oid,oid::regclass AS relation FROM closure
    LOOP
        IF r.relation::text NOT IN ('marts.scored_matchup_edges','marts.prediction_accuracy',
                                  'api.scored_matchup_edges','api.prediction_accuracy') THEN
            RAISE EXCEPTION 'Unexpected cascade dependency: %',r.relation;
        END IF;
        IF EXISTS (
            SELECT 1 FROM pg_depend d
            WHERE d.classid='pg_proc'::regclass
              AND ((d.refclassid='pg_class'::regclass AND d.refobjid=r.oid)
                OR (d.refclassid='pg_type'::regclass AND d.refobjid=
                    (SELECT reltype FROM pg_class WHERE oid=r.oid)))
        ) THEN RAISE EXCEPTION 'Dependent routine needs preservation: %',r.relation; END IF;
    END LOOP;
END
$guard$;
CREATE TEMP TABLE f04_baseline ON COMMIT DROP AS
SELECT 'ledger'::text AS object_name,count(*) AS n,
       md5(COALESCE(string_agg(md5((to_jsonb(p)-ARRAY['evaluation_mode','created_at',
          'published_at','simulated_as_of_at','experiment_label','fit_id','input_hash',
          'input_snapshot'])::text),'' ORDER BY prediction_id),'')) AS fingerprint
FROM predictions.game_predictions p
UNION ALL
SELECT 'outlook',count(*),md5(COALESCE(string_agg(md5(to_jsonb(p)::text),''
                             ORDER BY to_jsonb(p)::text),''))
FROM predictions.season_projections p;

-- SOURCE src/schemas/migrations/063_prediction_provenance.sql SHA256 126242f8f5eefed7b0b1c37b4708acda4bf41e83cd7a3f808938a49e9b9f1fb0
-- F04 prediction ledger. Explicit-file migration (not MIGRATION_ORDER).
-- Pause prediction writers for the coordinated schema/code cutover. Then apply
-- marts/037, marts/038, api/030, api/031 and api/032 in that order.
-- Existing computed_at/prediction_date values are NOT publication evidence.

CREATE TABLE IF NOT EXISTS predictions.model_artifacts (
    fit_id TEXT PRIMARY KEY CHECK (fit_id ~ '^[0-9a-f]{64}$'),
    model_version TEXT NOT NULL,
    artifact JSONB NOT NULL CHECK (jsonb_typeof(artifact) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    UNIQUE (fit_id, model_version)
);

ALTER TABLE predictions.game_predictions
    ADD COLUMN IF NOT EXISTS evaluation_mode TEXT NOT NULL DEFAULT 'legacy_unknown',
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS simulated_as_of_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS experiment_label TEXT,
    ADD COLUMN IF NOT EXISTS fit_id TEXT,
    ADD COLUMN IF NOT EXISTS input_hash TEXT,
    ADD COLUMN IF NOT EXISTS input_snapshot JSONB;

-- No default mode for new writes: each producer must state its intent.
ALTER TABLE predictions.game_predictions ALTER COLUMN evaluation_mode DROP DEFAULT;
DROP INDEX IF EXISTS predictions.game_predictions_daily_key;

DO $migration$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'predictions.game_predictions'::regclass
          AND conname = 'game_predictions_provenance_check'
    ) THEN
        ALTER TABLE predictions.game_predictions ADD CONSTRAINT game_predictions_provenance_check
        CHECK (
            (evaluation_mode = 'legacy_unknown'
                AND created_at IS NULL AND published_at IS NULL
                AND simulated_as_of_at IS NULL AND experiment_label IS NULL
                AND fit_id IS NULL AND input_hash IS NULL AND input_snapshot IS NULL)
            OR
            (evaluation_mode IN ('published_forecast', 'walk_forward_reconstruction', 'hindsight_experiment')
                AND created_at IS NOT NULL
                AND fit_id IS NOT NULL AND fit_id ~ '^[0-9a-f]{64}$'
                AND input_hash IS NOT NULL AND input_hash ~ '^[0-9a-f]{64}$'
                AND input_snapshot IS NOT NULL AND jsonb_typeof(input_snapshot) = 'object'
                AND (
                    (evaluation_mode = 'published_forecast'
                        AND published_at IS NOT NULL AND published_at = created_at
                        AND simulated_as_of_at IS NULL AND experiment_label IS NULL)
                    OR
                    (evaluation_mode = 'walk_forward_reconstruction'
                        AND published_at IS NULL AND simulated_as_of_at IS NOT NULL
                        AND experiment_label IS NULL)
                    OR
                    (evaluation_mode = 'hindsight_experiment'
                        AND published_at IS NULL AND simulated_as_of_at IS NOT NULL
                        AND experiment_label IS NOT NULL AND length(btrim(experiment_label)) > 0)
                ))
        );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'predictions.game_predictions'::regclass
          AND conname = 'game_predictions_artifact_fk'
    ) THEN
        ALTER TABLE predictions.game_predictions ADD CONSTRAINT game_predictions_artifact_fk
            FOREIGN KEY (fit_id, model_version)
            REFERENCES predictions.model_artifacts (fit_id, model_version);
    END IF;
END
$migration$;

CREATE OR REPLACE FUNCTION predictions.stamp_prediction_provenance()
RETURNS trigger LANGUAGE plpgsql SET search_path = '' AS $function$
BEGIN
    IF NEW.evaluation_mode IS NULL OR NEW.evaluation_mode = 'legacy_unknown' THEN
        RAISE EXCEPTION 'New predictions require explicit non-legacy provenance';
    END IF;
    -- statement_timestamp, unlike now(), is not the start of a transaction
    -- which may have spent minutes loading/scoring. Never trust a supplied time.
    NEW.created_at := statement_timestamp();
    NEW.published_at := CASE WHEN NEW.evaluation_mode = 'published_forecast'
        THEN NEW.created_at ELSE NULL END;
    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION predictions.reject_snapshot_update()
RETURNS trigger LANGUAGE plpgsql SET search_path = '' AS $function$
BEGIN
    RAISE EXCEPTION 'Prediction and model artifact snapshots are immutable; append a new snapshot';
END
$function$;

DROP TRIGGER IF EXISTS stamp_prediction_provenance ON predictions.game_predictions;
CREATE TRIGGER stamp_prediction_provenance BEFORE INSERT ON predictions.game_predictions
    FOR EACH ROW EXECUTE FUNCTION predictions.stamp_prediction_provenance();
DROP TRIGGER IF EXISTS reject_prediction_update ON predictions.game_predictions;
CREATE TRIGGER reject_prediction_update BEFORE UPDATE ON predictions.game_predictions
    FOR EACH ROW EXECUTE FUNCTION predictions.reject_snapshot_update();
DROP TRIGGER IF EXISTS reject_artifact_update ON predictions.model_artifacts;
CREATE TRIGGER reject_artifact_update BEFORE UPDATE ON predictions.model_artifacts
    FOR EACH ROW EXECUTE FUNCTION predictions.reject_snapshot_update();

CREATE INDEX IF NOT EXISTS game_predictions_published_idx
    ON predictions.game_predictions (game_id, model_version, published_at DESC, prediction_id DESC)
    WHERE evaluation_mode = 'published_forecast';
CREATE INDEX IF NOT EXISTS game_predictions_fit_idx ON predictions.game_predictions (fit_id);

COMMENT ON TABLE predictions.game_predictions IS
    'Immutable prediction ledger keyed by prediction_id. New rows explicitly distinguish published_forecast, walk_forward_reconstruction and hindsight_experiment. Legacy rows retain unknown provenance; prediction_date and computed_at are not publication evidence.';
COMMENT ON COLUMN predictions.game_predictions.created_at IS
    'Actual database INSERT statement time for new ledger entries; NULL for unverifiable legacy rows.';
COMMENT ON COLUMN predictions.game_predictions.published_at IS
    'Database write-time publication, set only for published_forecast. Prospective scoring requires known kickoff and published_at strictly before kickoff.';
COMMENT ON COLUMN predictions.game_predictions.simulated_as_of_at IS
    'Nominal reconstructed kickoff, not evidence that corrected inputs or closing market lines were available then.';
COMMENT ON TABLE predictions.model_artifacts IS
    'Content-addressed immutable snapshot of the artifact actually consumed for scoring, including implementation fingerprint. Does not reconstruct upstream training history.';

GRANT USAGE ON SCHEMA predictions TO anon, authenticated;
GRANT SELECT ON predictions.model_artifacts, predictions.game_predictions TO anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON predictions.model_artifacts, predictions.game_predictions
    FROM anon, authenticated;
REVOKE ALL ON FUNCTION predictions.stamp_prediction_provenance() FROM PUBLIC;
REVOKE ALL ON FUNCTION predictions.reject_snapshot_update() FROM PUBLIC;

-- SOURCE src/schemas/marts/037_scored_matchup_edges.sql SHA256 d29f718e370c74b40284b557052c6c980b967347f576184866db2dcf849052ac
-- marts.scored_matchup_edges
-- =============================================================================
-- Tier 2 analytics (docs/plans/2026-07-21-tier2-analytics-plan.md), Phase 4.
--
-- The FORWARD-LOOKING surface: house expected margin vs the market line for
-- UPCOMING (not-yet-completed) games only. One row per (game_id, model_version):
-- the LATEST prediction snapshot for that game+model, chosen by
-- DISTINCT ON (game_id, model_version) ordered by published_at and prediction_id. As new
-- daily snapshots land in predictions.game_predictions the mart re-materializes
-- to the freshest read on each game.
--
-- Grain: (game_id, model_version). scripts/compute_predictions.py writes TWO
-- model_versions for every game -- 'elo_v1' (Elo-only expected margin) and
-- 'elo_epa_blend_v1' (0.6*Elo + 0.4*ridge-EPA blend) -- so each upcoming game
-- appears twice, once per model. home_win_prob is Elo-only in BOTH rows
-- (the blend only changes the expected margin, not the win probability).
--
-- EDGE CONVENTION (matches api/003_game_detail.sql cover logic and migration
-- 024's column semantics):
--   market_home_margin = -market_spread   (negative spread => home favored)
--   edge = expected_home_margin + market_spread
--   edge > 0  => model's expected home margin beats the market => home is
--               undervalued => edge_pick = 'home'; edge <= 0 => edge_pick = 'away'.
--   abs_edge = ABS(edge) is the conviction magnitude consumers rank by.
--
-- NULL-MARKET ROWS: a game with no line yet (market_spread IS NULL) has a NULL
-- edge (and NULL abs_edge) written upstream -- it is still LISTED here (the
-- house expected margin is meaningful on its own) but is UNSCOREABLE against a
-- market until a line posts. Consumers should sort by abs_edge DESC and treat
-- NULLs as "no ranked edge" -- the abs_edge index is DESC NULLS LAST for exactly
-- this ordering.
--
-- NO EMPTY-GUARD (by design): this mart is legitimately EMPTY out of season and
-- until prospective in-season scoring runs populate
-- predictions.game_predictions. An empty result is a valid state, not a failure,
-- so it must not RAISE at deploy time (unlike the Tier 1 marts).
--
-- Source: predictions.game_predictions p JOIN core.games g ON g.id = p.game_id,
-- filtered to NOT COALESCE(g.completed, false).

DROP MATERIALIZED VIEW IF EXISTS marts.scored_matchup_edges CASCADE;

CREATE MATERIALIZED VIEW marts.scored_matchup_edges AS
SELECT DISTINCT ON (p.game_id, p.model_version)
    p.game_id,
    p.season,
    p.week,
    p.season_type,
    g.start_date,
    p.home_team,
    p.away_team,
    p.neutral_site,

    p.model_version,
    p.prediction_date,

    -- House ratings / expected margins
    p.home_elo_pregame,
    p.away_elo_pregame,
    p.elo_margin,
    p.epa_margin,
    p.expected_home_margin,
    p.home_win_prob,

    -- Market line (as captured in the snapshot)
    p.market_provider,
    p.market_spread,
    p.market_home_margin,
    p.market_captured_at,

    -- Edge = expected_home_margin + market_spread (>0 => home undervalued).
    -- NULL when no market line has posted yet: listed but unscoreable.
    p.edge,
    p.edge_pick,
    ABS(p.edge) AS abs_edge

FROM predictions.game_predictions p
JOIN core.games g ON g.id = p.game_id
WHERE NOT COALESCE(g.completed, false)
  AND p.evaluation_mode = 'published_forecast'
ORDER BY p.game_id, p.model_version, p.published_at DESC, p.prediction_id DESC;

-- Required for REFRESH CONCURRENTLY; also the natural grain key. DISTINCT ON
-- (game_id, model_version) guarantees one row per pair, so this is unique.
CREATE UNIQUE INDEX ON marts.scored_matchup_edges (game_id, model_version);

-- Query indexes
CREATE INDEX ON marts.scored_matchup_edges (season, week);

-- Consumers rank the slate by conviction; NULLs (no line yet) sort last.
CREATE INDEX ON marts.scored_matchup_edges (abs_edge DESC NULLS LAST);

-- SOURCE src/schemas/marts/038_prediction_accuracy.sql SHA256 3f69ee08795500549703f2889507dd3f39180f950adc331382877eea16e25d2f
-- marts.prediction_accuracy
-- =============================================================================
-- Tier 2 analytics (docs/plans/2026-07-21-tier2-analytics-plan.md), Phase 4/5.
--
-- THE PUBLISHED-FORECAST AUDIT SURFACE. This file IS the prediction-scoring methodology
-- for the house model -- every rule below is authoritative and intentionally
-- documented in full so the numbers are reproducible from the SQL alone.
--
-- Grain: (model_version, season, edge_threshold). scripts/compute_predictions.py
-- writes TWO model_versions per historical game -- 'elo_v1' (Elo-only expected
-- margin) and 'elo_epa_blend_v1' (0.6*Elo + 0.4*ridge-EPA blend); home_win_prob
-- is Elo-only in BOTH. Each (model_version, season) is scored at FOUR edge
-- thresholds via CROSS JOIN (VALUES (0),(3),(6),(10)) t(edge_threshold), so a
-- consumer can read "how did this model do when it only bet games it liked by
-- >= t points."
--
-- ---------------------------------------------------------------------------
-- SCORING BASE
-- ---------------------------------------------------------------------------
-- One row per (game_id, model_version): the LATEST prediction snapshot,
-- DISTINCT ON (game_id, model_version) ordered by published_at and prediction_id, joined to
-- core.games and restricted to COMPLETED games that actually have both scores
-- (home_points/away_points NOT NULL). Derived per game:
--   actual_home_margin  = home_points - away_points
--   actual_home_result  = 1.0 if home won, 0.5 if tied, 0.0 if home lost
--                         (ties are rare/effectively nonexistent in the modern
--                          era; scored as 0.5 for Brier, and treated as PUSHES
--                          -- i.e. excluded -- in ATS below).
-- CFBD's own pregame number is pulled via a 1:1 LEFT JOIN to
-- metrics.pregame_win_probability (home_win_probability) AFTER the DISTINCT ON,
-- so the win-prob join can never fan out the "latest snapshot" selection.
--
-- ---------------------------------------------------------------------------
-- THRESHOLD SEMANTICS (each row is self-consistent -- a deliberate choice)
-- ---------------------------------------------------------------------------
-- A game "qualifies" for a threshold row when:
--   edge_threshold = 0  -> ALL scored games qualify, INCLUDING NULL-market ones
--                          (edge may be NULL). The t=0 row is the unconditional
--                          "every game the model predicted" baseline.
--   edge_threshold > 0  -> edge IS NOT NULL AND ABS(edge) >= edge_threshold.
--                          (edge is NULL exactly when no market line existed, so
--                           t>0 rows are automatically market-only.)
-- Every metric in a row is computed over THAT row's qualifying population, so a
-- row is internally consistent: margin error, ATS record, and Brier all describe
-- the same slice of games the model flagged at that threshold. The t=0 row is
-- therefore the only one where margin_mae/margin_rmse are the whole-season "all
-- games" numbers; higher-threshold rows recompute the same formulas over the
-- |edge| >= t subset rather than reusing the t=0 value.
--
-- ---------------------------------------------------------------------------
-- METRIC DEFINITIONS
-- ---------------------------------------------------------------------------
-- n_games      : count of qualifying games (t=0 includes NULL-market games).
-- n_with_market: qualifying games that carry a market_spread (for t>0 this
--                equals n_games; it only differs at t=0).
-- margin_mae   : AVG(ABS(expected_home_margin - actual_home_margin)) over the
--                qualifying games.
-- margin_rmse  : SQRT(AVG(POWER(expected_home_margin - actual_home_margin, 2)))
--                over the qualifying games.
--
-- ATS (against-the-spread) -- ONLY games with market_spread NOT NULL AND
-- |edge| >= t (so ties/pushes and NULL-market games never inflate the record):
--   cover math uses the same convention as api/003_game_detail.sql and the edge
--   sign: home covers when (actual_home_margin + market_spread) > 0.
--   The model's pick WINS  when:
--       (edge_pick = 'home' AND actual_home_margin + market_spread > 0) OR
--       (edge_pick = 'away' AND actual_home_margin + market_spread < 0)
--   PUSH when actual_home_margin + market_spread = 0 (excluded from hit rate).
--   LOSS otherwise.
--   ats_wins / ats_losses / ats_pushes are counts; ats_hit_rate =
--   ats_wins::numeric / NULLIF(ats_wins + ats_losses, 0) -- pushes excluded from
--   the denominator, NULL when there are no decided ATS games.
--
-- BRIER (probability calibration) -- computed over the SAME subset for the house
-- model and CFBD so the comparison is meaningful: games in the qualifying
-- population where BOTH home_win_prob AND CFBD's home_win_probability are present
-- (inner-present intersection). n_scored_win_prob is exactly that subset's size.
--   brier      = AVG(POWER(home_win_prob      - actual_home_result, 2))
--   cfbd_brier = AVG(POWER(cfbd_home_win_prob - actual_home_result, 2))
--   Both over the identical n_scored_win_prob games -- a same-subset comparison,
--   otherwise the numbers are not comparable. Lower is better.
--
-- ---------------------------------------------------------------------------
-- CAVEATS (read before trusting a row)
-- ---------------------------------------------------------------------------
-- Only observed publications strictly before known kickoff are scored. Historical
-- reconstructions, hindsight experiments, and legacy_unknown rows are excluded.
-- As-of reconstruction cannot prove inputs/closing lines were available then.
-- Empty accuracy after migration is valid until published games complete; backfill
-- never populates this prospective cohort.

DROP MATERIALIZED VIEW IF EXISTS marts.prediction_accuracy CASCADE;

CREATE MATERIALIZED VIEW marts.prediction_accuracy AS
WITH latest_pred AS (
    -- Latest snapshot per (game_id, model_version) for completed, scored games.
    SELECT DISTINCT ON (p.game_id, p.model_version)
        p.game_id,
        p.model_version,
        p.season,
        p.expected_home_margin,
        p.home_win_prob,
        p.market_spread,
        p.edge,
        p.edge_pick,
        g.home_points,
        g.away_points
    FROM predictions.game_predictions p
    JOIN core.games g ON g.id = p.game_id
    WHERE g.completed
      AND g.home_points IS NOT NULL
      AND g.away_points IS NOT NULL
      AND p.evaluation_mode = 'published_forecast'
      AND g.start_date IS NOT NULL
      AND p.published_at < g.start_date
    ORDER BY p.game_id, p.model_version, p.published_at DESC, p.prediction_id DESC
),
scored AS (
    -- Attach actuals + CFBD's pregame win prob (1:1 join, post-DISTINCT ON).
    SELECT
        lp.model_version,
        lp.season,
        lp.game_id,
        lp.expected_home_margin,
        lp.home_win_prob,
        lp.market_spread,
        lp.edge,
        lp.edge_pick,
        (lp.home_points - lp.away_points)::numeric AS actual_home_margin,
        (CASE
            WHEN lp.home_points > lp.away_points THEN 1.0
            WHEN lp.home_points = lp.away_points THEN 0.5
            ELSE 0.0
        END)::numeric AS actual_home_result,
        wp.home_win_probability AS cfbd_home_win_prob
    FROM latest_pred lp
    LEFT JOIN metrics.pregame_win_probability wp ON wp.game_id = lp.game_id
),
expanded AS (
    -- Fan each scored game out across the four edge thresholds and mark
    -- whether it qualifies for that threshold's population.
    SELECT
        s.*,
        th.edge_threshold,
        (
            th.edge_threshold = 0
            OR (s.edge IS NOT NULL AND ABS(s.edge) >= th.edge_threshold)
        ) AS qualifies
    FROM scored s
    CROSS JOIN (VALUES (0), (3), (6), (10)) AS th(edge_threshold)
),
agg AS (
    SELECT
        model_version,
        season,
        edge_threshold,

        COUNT(*) FILTER (WHERE qualifies) AS n_games,
        COUNT(*) FILTER (WHERE qualifies AND market_spread IS NOT NULL) AS n_with_market,

        -- Margin error over the qualifying population (self-consistent per row).
        AVG(ABS(expected_home_margin - actual_home_margin))
            FILTER (WHERE qualifies) AS margin_mae,
        SQRT(
            AVG(POWER(expected_home_margin - actual_home_margin, 2))
                FILTER (WHERE qualifies)
        ) AS margin_rmse,

        -- ATS record: market required and |edge| >= t; pushes (margin+spread=0)
        -- excluded from wins/losses.
        COUNT(*) FILTER (
            WHERE qualifies
              AND market_spread IS NOT NULL
              AND (actual_home_margin + market_spread) <> 0
              AND (
                  (edge_pick = 'home' AND actual_home_margin + market_spread > 0)
               OR (edge_pick = 'away' AND actual_home_margin + market_spread < 0)
              )
        ) AS ats_wins,
        COUNT(*) FILTER (
            WHERE qualifies
              AND market_spread IS NOT NULL
              AND (actual_home_margin + market_spread) <> 0
              AND NOT (
                  (edge_pick = 'home' AND actual_home_margin + market_spread > 0)
               OR (edge_pick = 'away' AND actual_home_margin + market_spread < 0)
              )
        ) AS ats_losses,
        COUNT(*) FILTER (
            WHERE qualifies
              AND market_spread IS NOT NULL
              AND (actual_home_margin + market_spread) = 0
        ) AS ats_pushes,

        -- Brier over the same-subset intersection (both win probs present).
        AVG(POWER(home_win_prob - actual_home_result, 2)) FILTER (
            WHERE qualifies
              AND home_win_prob IS NOT NULL
              AND cfbd_home_win_prob IS NOT NULL
        ) AS brier,
        AVG(POWER(cfbd_home_win_prob - actual_home_result, 2)) FILTER (
            WHERE qualifies
              AND home_win_prob IS NOT NULL
              AND cfbd_home_win_prob IS NOT NULL
        ) AS cfbd_brier,
        COUNT(*) FILTER (
            WHERE qualifies
              AND home_win_prob IS NOT NULL
              AND cfbd_home_win_prob IS NOT NULL
        ) AS n_scored_win_prob

    FROM expanded
    GROUP BY model_version, season, edge_threshold
)
SELECT
    model_version,
    season,
    edge_threshold,
    n_games,
    n_with_market,
    ROUND(margin_mae::numeric, 4) AS margin_mae,
    ROUND(margin_rmse::numeric, 4) AS margin_rmse,
    ats_wins,
    ats_losses,
    ats_pushes,
    ROUND(ats_wins::numeric / NULLIF(ats_wins + ats_losses, 0), 4) AS ats_hit_rate,
    ROUND(brier::numeric, 6) AS brier,
    ROUND(cfbd_brier::numeric, 6) AS cfbd_brier,
    n_scored_win_prob
FROM agg;

-- Required for REFRESH CONCURRENTLY; also the natural grain key.
CREATE UNIQUE INDEX ON marts.prediction_accuracy (model_version, season, edge_threshold);

-- Query index: pull one model's threshold curve across seasons.
CREATE INDEX ON marts.prediction_accuracy (model_version, edge_threshold);

-- SOURCE src/schemas/api/030_scored_matchup_edges.sql SHA256 5e7fbbfd68c42d56c53ccea91341dae1e054f1f16a2607d0f5bfe1218544eeeb
-- api.scored_matchup_edges
-- House model vs market: expected margin/win-prob compared against the
-- market line for upcoming games, with the resulting edge.
-- Thin passthrough of marts.scored_matchup_edges (Tier 2 analytics,
-- docs/plans/2026-07-21-tier2-analytics-plan.md).
--
-- NOTE: marts.scored_matchup_edges surfaces UPCOMING games vs the market
-- line and is expected to be empty out of season -- that is normal, not a
-- data-quality failure.
--
-- PostgREST usage:
--   GET /api/scored_matchup_edges?season=eq.2026&order=abs_edge.desc
--   GET /api/scored_matchup_edges?week=eq.5&edge_pick=eq.home

CREATE OR REPLACE VIEW api.scored_matchup_edges AS
SELECT
    game_id,
    season,
    week,
    season_type,
    start_date,
    home_team,
    away_team,
    neutral_site,
    model_version,
    prediction_date,
    home_elo_pregame,
    away_elo_pregame,
    elo_margin,
    epa_margin,
    expected_home_margin,
    home_win_prob,
    market_provider,
    market_spread,
    market_home_margin,
    market_captured_at,
    edge,
    edge_pick,
    abs_edge
FROM marts.scored_matchup_edges;

GRANT SELECT ON api.scored_matchup_edges TO anon, authenticated;

COMMENT ON VIEW api.scored_matchup_edges IS 'House model expected margin/win-prob vs the market line for upcoming games. Columns: game_id, season, week, season_type, start_date, home_team, away_team, neutral_site, model_version, prediction_date, home_elo_pregame, away_elo_pregame, elo_margin, epa_margin, expected_home_margin, home_win_prob, market_provider, market_spread, market_home_margin, market_captured_at, edge, edge_pick, abs_edge. edge = expected_home_margin + spread (positive = home undervalued by the market). Backed by marts.scored_matchup_edges; normally empty out of season.';

-- Preserve the API-only analyst role even when deployed by a different owner.
DO $grants$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst_ro') THEN
        GRANT SELECT ON api.scored_matchup_edges TO analyst_ro;
    END IF;
END
$grants$;

-- SOURCE src/schemas/api/031_prediction_accuracy.sql SHA256 c75dd902a6556371dfdcf5ba6a658d39e8aa314ab1fd048b1824b33eb5bcc904
-- api.prediction_accuracy
-- Retroactive scoring of house predictions by season/model/edge-threshold:
-- margin MAE/RMSE, ATS record, Brier score (house and CFBD), vs the market
-- and CFBD's own pregame win probability.
-- Thin passthrough of marts.prediction_accuracy (Tier 2 analytics,
-- docs/plans/2026-07-21-tier2-analytics-plan.md).
--
-- PostgREST usage:
--   GET /api/prediction_accuracy?season=eq.2024&model_version=eq.house_v1
--   GET /api/prediction_accuracy?edge_threshold=eq.6&order=ats_hit_rate.desc

CREATE OR REPLACE VIEW api.prediction_accuracy AS
SELECT
    model_version,
    season,
    edge_threshold,
    n_games,
    n_with_market,
    margin_mae,
    margin_rmse,
    ats_wins,
    ats_losses,
    ats_pushes,
    ats_hit_rate,
    brier,
    cfbd_brier,
    n_scored_win_prob
FROM marts.prediction_accuracy;

GRANT SELECT ON api.prediction_accuracy TO anon, authenticated;

COMMENT ON VIEW api.prediction_accuracy IS 'Retroactive scoring of house predictions by season/model/edge-threshold. Columns: model_version, season, edge_threshold, n_games, n_with_market, margin_mae, margin_rmse, ats_wins, ats_losses, ats_pushes, ats_hit_rate, brier, cfbd_brier, n_scored_win_prob. brier/cfbd_brier let the house win-prob model be benchmarked directly against CFBD''s pregame win probability. Backed by marts.prediction_accuracy.';

-- Preserve the API-only analyst role even when deployed by a different owner.
DO $grants$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst_ro') THEN
        GRANT SELECT ON api.prediction_accuracy TO analyst_ro;
    END IF;
END
$grants$;

-- SOURCE src/schemas/api/032_game_predictions.sql SHA256 a003b156bf6c5229e689531cb3e08b0084f0ca8efb07a0f25a2d3e43a62f4799
-- Latest published forecast per game/model. Historical modes remain discoverable
-- through prediction_history. Neither prediction_date nor computed_at selects a row.
CREATE OR REPLACE VIEW api.game_predictions AS
SELECT DISTINCT ON (game_id, model_version)
    prediction_id,
    computed_at,
    prediction_date,
    model_version,
    game_id,
    season,
    week,
    season_type,
    home_team,
    away_team,
    neutral_site,
    home_elo_pregame,
    away_elo_pregame,
    elo_margin,
    epa_margin,
    expected_home_margin,
    home_win_prob,
    market_provider,
    market_home_margin,
    market_spread,
    market_captured_at,
    edge,
    edge_pick,
    evaluation_mode,
    created_at,
    published_at,
    simulated_as_of_at,
    experiment_label,
    fit_id,
    input_hash
FROM predictions.game_predictions
WHERE evaluation_mode = 'published_forecast'
ORDER BY game_id, model_version, published_at DESC, prediction_id DESC;

GRANT SELECT ON api.game_predictions TO anon, authenticated;

COMMENT ON VIEW api.game_predictions IS 'Latest published forecast by publication timestamp and immutable prediction_id. Published does not alone imply pre-kickoff; accuracy applies its strict kickoff cutoff.';

CREATE OR REPLACE VIEW api.prediction_history AS
SELECT
    prediction_id,
    computed_at,
    prediction_date,
    model_version,
    game_id,
    season,
    week,
    season_type,
    home_team,
    away_team,
    neutral_site,
    home_elo_pregame,
    away_elo_pregame,
    elo_margin,
    epa_margin,
    expected_home_margin,
    home_win_prob,
    market_provider,
    market_home_margin,
    market_spread,
    market_captured_at,
    edge,
    edge_pick,
    evaluation_mode,
    created_at,
    published_at,
    simulated_as_of_at,
    experiment_label,
    fit_id,
    input_hash
FROM predictions.game_predictions;

GRANT SELECT ON api.prediction_history TO anon, authenticated;
COMMENT ON VIEW api.prediction_history IS 'All prediction modes and immutable IDs, including legacy_unknown. Simulated as-of timestamps are reconstruction intent, not original publication evidence.';

-- Preserve the API-only analyst role even when deployed by a different owner.
DO $grants$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst_ro') THEN
        GRANT SELECT ON api.game_predictions, api.prediction_history TO analyst_ro;
    END IF;
END
$grants$;

-- Abort/roll back the entire cutover if any preservation or caller check fails.
DO $verify$
DECLARE n bigint; digest text; b record; r text; v text; observed bigint;
BEGIN
    SELECT count(*),md5(COALESCE(string_agg(md5((to_jsonb(p)-ARRAY['evaluation_mode',
      'created_at','published_at','simulated_as_of_at','experiment_label','fit_id',
      'input_hash','input_snapshot'])::text),'' ORDER BY prediction_id),''))
    INTO n,digest FROM predictions.game_predictions p;
    SELECT * INTO b FROM f04_baseline WHERE object_name='ledger';
    IF n<>b.n OR digest<>b.fingerprint THEN RAISE EXCEPTION 'Legacy ledger changed'; END IF;
    IF EXISTS (SELECT 1 FROM predictions.game_predictions WHERE evaluation_mode<>'legacy_unknown'
      OR created_at IS NOT NULL OR published_at IS NOT NULL OR simulated_as_of_at IS NOT NULL
      OR experiment_label IS NOT NULL OR fit_id IS NOT NULL OR input_hash IS NOT NULL
      OR input_snapshot IS NOT NULL) THEN RAISE EXCEPTION 'Unexpected legacy provenance'; END IF;
    RAISE NOTICE 'F04 verified legacy rows %, full payload/ID hash %',n,digest;
    SELECT count(*),md5(COALESCE(string_agg(md5(to_jsonb(p)::text),''
                       ORDER BY to_jsonb(p)::text),''))
    INTO n,digest FROM predictions.season_projections p;
    SELECT * INTO b FROM f04_baseline WHERE object_name='outlook';
    IF n<>b.n OR digest<>b.fingerprint THEN RAISE EXCEPTION 'Stored outlook changed'; END IF;
    RAISE NOTICE 'F04 verified unchanged outlook rows %, hash %',n,digest;
    IF to_regclass('predictions.game_predictions_daily_key') IS NOT NULL THEN
        RAISE EXCEPTION 'Obsolete daily key still present'; END IF;
    IF (SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgenabled='O'
        AND ((tgrelid='predictions.game_predictions'::regclass AND tgname IN
             ('stamp_prediction_provenance','reject_prediction_update'))
          OR (tgrelid='predictions.model_artifacts'::regclass AND tgname='reject_artifact_update')))
        <>3 THEN RAISE EXCEPTION 'Missing provenance/immutability triggers'; END IF;
    FOREACH v IN ARRAY ARRAY['game_predictions','prediction_history','scored_matchup_edges',
                              'prediction_accuracy'] LOOP
        IF EXISTS (SELECT 1 FROM pg_class WHERE oid=format('api.%I',v)::regclass
            AND 'security_invoker=true'=ANY(reloptions)) THEN
            RAISE EXCEPTION 'Unexpected invoker rights on %',v; END IF;
    END LOOP;
    FOREACH r IN ARRAY ARRAY['anon','authenticated','analyst_ro'] LOOP
        EXECUTE format('SET LOCAL ROLE %I',r);
        FOREACH v IN ARRAY ARRAY['game_predictions','prediction_history','scored_matchup_edges',
                                  'prediction_accuracy'] LOOP
            EXECUTE format('SELECT count(*) FROM api.%I',v) INTO observed;
            RAISE NOTICE 'F04 caller % view % rows %',r,v,observed;
        END LOOP;
        RESET ROLE;
    END LOOP;
    FOREACH r IN ARRAY ARRAY['anon','authenticated'] LOOP
        FOREACH v IN ARRAY ARRAY['game_predictions','model_artifacts'] LOOP
            IF has_table_privilege(r,format('predictions.%I',v),'INSERT,UPDATE,DELETE,TRUNCATE')
                THEN RAISE EXCEPTION 'Consumer DML allowed: % %',r,v; END IF;
        END LOOP;
    END LOOP;
    RAISE NOTICE 'F04 atomic schema cutover verified';
END
$verify$;
