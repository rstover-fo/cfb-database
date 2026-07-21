-- marts.scored_matchup_edges
-- =============================================================================
-- Tier 2 analytics (docs/plans/2026-07-21-tier2-analytics-plan.md), Phase 4.
--
-- The FORWARD-LOOKING surface: house expected margin vs the market line for
-- UPCOMING (not-yet-completed) games only. One row per (game_id, model_version):
-- the LATEST prediction snapshot for that game+model, chosen by
-- DISTINCT ON (game_id, model_version) ORDER BY prediction_date DESC. As new
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
-- until the Phase 5 predictions backfill / in-season daily runs populate
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
ORDER BY p.game_id, p.model_version, p.prediction_date DESC;

-- Required for REFRESH CONCURRENTLY; also the natural grain key. DISTINCT ON
-- (game_id, model_version) guarantees one row per pair, so this is unique.
CREATE UNIQUE INDEX ON marts.scored_matchup_edges (game_id, model_version);

-- Query indexes
CREATE INDEX ON marts.scored_matchup_edges (season, week);

-- Consumers rank the slate by conviction; NULLs (no line yet) sort last.
CREATE INDEX ON marts.scored_matchup_edges (abs_edge DESC NULLS LAST);
