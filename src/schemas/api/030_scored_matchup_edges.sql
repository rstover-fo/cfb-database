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
