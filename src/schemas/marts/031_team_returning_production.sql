-- marts.team_returning_production -- U6 of the returning production plan.
-- Team-grain rollup over marts.player_returning_value.
-- Grain: one row per (target_team, target_season).
--
-- Output composition:
--   * Totals: returning_production_total / _offense / _defense
--   * Per-position-group: rp_qb, rp_rb, rp_wr_te, rp_ol, rp_dl, rp_lb, rp_db, rp_st
--   * Counts: n_returning_starters, n_portal_in, n_portal_out, n_recruits_contributing
--   * Calibration:
--       cfbd_returning_production_pct  -- raw stats.player_returning.percent_ppa
--       our_pct_normalized             -- total / season_max_total, [0,1]
--       delta_vs_cfbd                  -- our_pct_normalized - cfbd_returning_production_pct
--
-- KNOWN LIMITATIONS:
--   * stats.player_returning only goes back to 2014 and doesn't yet include 2026
--     (CFBD typically publishes after spring -- see returning_production_model
--     memory). 2026 rows therefore carry NULL cfbd_returning_production_pct and
--     NULL delta_vs_cfbd. They are NOT row-dropped (per plan: "a team for which
--     CFBD did not publish ... has NULL cfbd_returning_production_pct ... not a
--     row drop").
--   * delta_vs_cfbd uses per-season MAX-normalization on our side. CFBD's
--     percent_ppa is "% of own team's prior-year PPA returning" -- a different
--     definition than ours. The delta is therefore a sanity-check, not a strict
--     match, and is weakened to a logged warning per the plan's Test scenarios.
--   * rp_st is always 0 because all ST position_weight = 0 in
--     rp.dim_position_weights. Kept as an explicit column for downstream
--     consumer stability.
--   * Players with NULL position_group (unmatched portal) carry
--     returning_value = 0 so they do not pollute the totals. They are counted
--     in n_unknown_position for monitoring.

DROP MATERIALIZED VIEW IF EXISTS marts.team_returning_production CASCADE;

CREATE MATERIALIZED VIEW marts.team_returning_production AS
WITH
-- Player-grain rollup over the matview built in U5. We pull games_started from
-- fct_player_seasons here (not exposed in player_returning_value) to compute
-- n_returning_starters at the gold layer. LEFT JOIN because recruits and
-- unmatched portal entries have no source-season fct row.
prv_with_starts AS (
  SELECT
    prv.target_team,
    prv.target_season,
    prv.position_group,
    prv.returning_value,
    prv.is_returning,
    prv.is_portal_in,
    prv.is_recruit,
    fps.games_started
  FROM marts.player_returning_value prv
  LEFT JOIN rp.fct_player_seasons fps
    ON fps.player_id = prv.player_id
   AND fps.season   = prv.source_season
),
team_player_rollup AS (
  SELECT
    target_team   AS team,
    target_season AS season,

    -- Total + offense/defense partition
    SUM(returning_value)::numeric(8,3)                                   AS returning_production_total,
    SUM(returning_value) FILTER (
      WHERE position_group IN ('QB','RB','WR_TE','OL')
    )::numeric(8,3)                                                      AS returning_production_offense,
    SUM(returning_value) FILTER (
      WHERE position_group IN ('DL','LB','DB')
    )::numeric(8,3)                                                      AS returning_production_defense,

    -- Per-position-group breakdowns
    SUM(returning_value) FILTER (WHERE position_group = 'QB')::numeric(7,3)    AS rp_qb,
    SUM(returning_value) FILTER (WHERE position_group = 'RB')::numeric(7,3)    AS rp_rb,
    SUM(returning_value) FILTER (WHERE position_group = 'WR_TE')::numeric(7,3) AS rp_wr_te,
    SUM(returning_value) FILTER (WHERE position_group = 'OL')::numeric(7,3)    AS rp_ol,
    SUM(returning_value) FILTER (WHERE position_group = 'DL')::numeric(7,3)    AS rp_dl,
    SUM(returning_value) FILTER (WHERE position_group = 'LB')::numeric(7,3)    AS rp_lb,
    SUM(returning_value) FILTER (WHERE position_group = 'DB')::numeric(7,3)    AS rp_db,
    SUM(returning_value) FILTER (WHERE position_group = 'ST')::numeric(7,3)    AS rp_st,

    -- Counts
    COUNT(*) FILTER (
      WHERE is_returning = true AND games_started >= 8
    )::int                                                               AS n_returning_starters,
    COUNT(*) FILTER (WHERE is_portal_in = true)::int                     AS n_portal_in,
    COUNT(*) FILTER (
      WHERE is_recruit = true AND returning_value > 0.10
    )::int                                                               AS n_recruits_contributing,
    COUNT(*) FILTER (WHERE position_group IS NULL)::int                  AS n_unknown_position
  FROM prv_with_starts
  GROUP BY target_team, target_season
),
-- Portal-out is sourced directly from fct_player_movements: players who left
-- a team (source_team) for somewhere else (or unmatched destination) via the
-- portal in a given transition_season. roster_continuity rows are excluded
-- because their source_team = destination_team.
portal_out AS (
  SELECT
    source_team       AS team,
    transition_season AS season,
    COUNT(*)::int     AS n_portal_out
  FROM rp.fct_player_movements
  WHERE source_team IS NOT NULL
    AND match_method LIKE 'portal_%'
    AND (destination_team IS NULL OR destination_team <> source_team)
  GROUP BY source_team, transition_season
),
-- CFBD calibration target. stats.player_returning is already (season, team)
-- grain so no aggregation is required.
cfbd_calibration AS (
  SELECT
    team,
    season::int  AS season,
    percent_ppa  AS cfbd_returning_production_pct
  FROM stats.player_returning
),
-- Per-season MAX of our total, used to scale our_pct_normalized into [0,1].
season_max AS (
  SELECT
    season,
    MAX(returning_production_total) AS season_max_total
  FROM team_player_rollup
  GROUP BY season
)
SELECT
  tpr.team,
  tpr.season,

  -- Totals
  tpr.returning_production_total,
  COALESCE(tpr.returning_production_offense, 0)::numeric(8,3) AS returning_production_offense,
  COALESCE(tpr.returning_production_defense, 0)::numeric(8,3) AS returning_production_defense,

  -- Per-position
  COALESCE(tpr.rp_qb,    0)::numeric(7,3) AS rp_qb,
  COALESCE(tpr.rp_rb,    0)::numeric(7,3) AS rp_rb,
  COALESCE(tpr.rp_wr_te, 0)::numeric(7,3) AS rp_wr_te,
  COALESCE(tpr.rp_ol,    0)::numeric(7,3) AS rp_ol,
  COALESCE(tpr.rp_dl,    0)::numeric(7,3) AS rp_dl,
  COALESCE(tpr.rp_lb,    0)::numeric(7,3) AS rp_lb,
  COALESCE(tpr.rp_db,    0)::numeric(7,3) AS rp_db,
  COALESCE(tpr.rp_st,    0)::numeric(7,3) AS rp_st,

  -- Counts (COALESCE so teams with no portal-out / no recruits don't NULL)
  COALESCE(tpr.n_returning_starters,    0) AS n_returning_starters,
  COALESCE(tpr.n_portal_in,             0) AS n_portal_in,
  COALESCE(po.n_portal_out,             0) AS n_portal_out,
  COALESCE(tpr.n_recruits_contributing, 0) AS n_recruits_contributing,
  COALESCE(tpr.n_unknown_position,      0) AS n_unknown_position,

  -- Calibration
  cal.cfbd_returning_production_pct,
  CASE
    WHEN sm.season_max_total IS NULL OR sm.season_max_total = 0 THEN NULL
    ELSE (tpr.returning_production_total / sm.season_max_total)::numeric(5,4)
  END AS our_pct_normalized,
  CASE
    WHEN cal.cfbd_returning_production_pct IS NULL
      OR sm.season_max_total IS NULL
      OR sm.season_max_total = 0
    THEN NULL
    ELSE (
      (tpr.returning_production_total / sm.season_max_total)
      - cal.cfbd_returning_production_pct
    )::numeric(5,4)
  END AS delta_vs_cfbd,

  NOW() AS generated_at
FROM team_player_rollup tpr
LEFT JOIN portal_out       po  ON po.team   = tpr.team   AND po.season  = tpr.season
LEFT JOIN cfbd_calibration cal ON cal.team  = tpr.team   AND cal.season = tpr.season
LEFT JOIN season_max       sm  ON sm.season = tpr.season;

-- UNIQUE INDEX is required for REFRESH MATERIALIZED VIEW CONCURRENTLY.
CREATE UNIQUE INDEX idx_team_returning_production_pk
  ON marts.team_returning_production (team, season);

-- Access patterns: cfb-app filters by season, ranks by total returning value.
CREATE INDEX idx_team_returning_production_season_total
  ON marts.team_returning_production (season, returning_production_total DESC);
CREATE INDEX idx_team_returning_production_season
  ON marts.team_returning_production (season);

-- Grants: SECURITY INVOKER + read-only-database invariant per 2026-02-07 hardening.
GRANT SELECT ON marts.team_returning_production TO anon, authenticated;

COMMENT ON MATERIALIZED VIEW marts.team_returning_production IS
  'Team-grain returning production rollup, one row per (team, season). '
  'SUM over marts.player_returning_value with offense/defense partition, '
  'per-position-group breakdown (rp_qb..rp_st), and portal/recruit counts. '
  'Includes calibration columns (cfbd_returning_production_pct from '
  'stats.player_returning, our_pct_normalized = total / season_max, '
  'delta_vs_cfbd). 2026 has NULL CFBD calibration until CFBD publishes. '
  'Refresh via REFRESH MATERIALIZED VIEW CONCURRENTLY; depends on '
  'marts.player_returning_value being current.';
