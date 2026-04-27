-- api.team_returning_production
-- Team-grain returning production rollup for cfb-app.
-- Filter by: team, season, season + min total
-- Example: /api/team_returning_production?season=eq.2026&order=returning_production_total.desc

DROP VIEW IF EXISTS api.team_returning_production;

CREATE VIEW api.team_returning_production
WITH (security_invoker = true) AS
SELECT
    team,
    season,
    returning_production_total,
    returning_production_offense,
    returning_production_defense,
    rp_qb,
    rp_rb,
    rp_wr_te,
    rp_ol,
    rp_dl,
    rp_lb,
    rp_db,
    rp_st,
    n_returning_starters,
    n_portal_in,
    n_portal_out,
    n_recruits_contributing,
    n_unknown_position,
    cfbd_returning_production_pct,
    our_pct_normalized,
    delta_vs_cfbd,
    generated_at
FROM marts.team_returning_production;

GRANT SELECT ON api.team_returning_production TO anon, authenticated;

COMMENT ON VIEW api.team_returning_production IS
    'Team-grain returning production rollup. One row per (team, season), '
    'covering 2021-2026. Decomposition: offense/defense partition + per-position-group '
    '(rp_qb..rp_st). Counts: n_returning_starters, n_portal_in, n_portal_out, '
    'n_recruits_contributing. Calibration: cfbd_returning_production_pct (raw '
    'CFBD percent_ppa, NULL for 2026 until CFBD publishes), our_pct_normalized '
    '(total / season_max in [0,1]), delta_vs_cfbd. SECURITY INVOKER + '
    'read-only for anon.';
