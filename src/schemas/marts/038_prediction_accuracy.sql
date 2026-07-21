-- marts.prediction_accuracy
-- =============================================================================
-- Tier 2 analytics (docs/plans/2026-07-21-tier2-analytics-plan.md), Phase 4/5.
--
-- THE BACKTEST / AUDIT SURFACE. This file IS the prediction-scoring methodology
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
-- DISTINCT ON (game_id, model_version) ORDER BY prediction_date DESC, joined to
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
-- * LEAKAGE -- FIXED (2026-07-21, Tier 3): 'elo_epa_blend_v1' used to fold in
--   ridge-adjusted EPA fit on the FULL season, so retro rows for early-season
--   games were MILDLY LEAKY (the fit "saw" games that hadn't happened yet at
--   kickoff). This is now closed: scripts/compute_predictions.py --as-of-week
--   backfills each game using analytics.adjusted_epa_week_build coefficients
--   as of THAT game's week (prior-season fallback when the current season has
--   no fit yet at that point, Elo-only when neither is available), so both
--   model_versions are walk-forward honest. The previously documented 56.1%
--   ATS>=6 hit rate was that leakage, not real edge -- the honest numbers are
--   elo_epa_blend_v1 ATS>=6 = 50.1% (n=3,462) vs elo_v1 = 50.3%; nobody beats
--   the closing line. The blend still edges elo_v1 on margin MAE (16.31 vs
--   16.46).
-- * CLOSING-LINE PROXY: past-game market_spread comes from betting.lines, which
--   is approximately the CLOSING line. True line-movement history only begins
--   accruing 2026-07-21, so pre-2026 ATS/edge figures are scored against
--   closing, not against the number the model would have seen earlier in the week.
--
-- NO EMPTY-GUARD (by design): legitimately EMPTY until the Phase 5 predictions
-- backfill populates predictions.game_predictions with completed-game snapshots.

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
    ORDER BY p.game_id, p.model_version, p.prediction_date DESC
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
