-- api.line_movement
-- Thin view over betting.line_snapshots for line-movement history.
-- One row per (game, provider, captured_at) snapshot; pending games only.
--
-- PostgREST usage:
--   GET /api/line_movement?game_id=eq.401628455&order=provider,captured_at

CREATE OR REPLACE VIEW api.line_movement AS
SELECT
    captured_at,
    game_id,
    season,
    week,
    home_team,
    away_team,
    provider,
    spread,
    formatted_spread,
    over_under,
    home_moneyline,
    away_moneyline,
    line_hash
FROM betting.line_snapshots
ORDER BY game_id, provider, captured_at;

COMMENT ON VIEW api.line_movement IS
'Betting line movement history from append-only daily snapshots of pending games. '
'line_hash lets consumers detect no-movement streaks between captures.';

-- Grants are part of the definition: an apply that DROPs/recreates the
-- view would otherwise leave the PostgREST roles without read access
-- (no ALTER DEFAULT PRIVILEGES for them in this database).
GRANT SELECT ON api.line_movement TO anon, authenticated;
