-- marts.team_returning_production -- U6 of the returning production plan.
-- Team-grain rollup over marts.player_returning_value.
-- Grain: one row per (target_team, target_season).
--
-- Output composition:
--   * Totals: returning_production_total / _offense / _defense
--   * Per-position-group: rp_qb, rp_rb, rp_wr_te, rp_ol, rp_dl, rp_lb, rp_db, rp_st
--   * Counts: n_returning_starters, n_portal_in, n_portal_out, n_recruits_contributing
--   * Portal trench exchange (added post-U6 from real-world validation):
--       portal_trench_in_value     -- SUM(returning_value) for incoming portal OL+DL
--       portal_trench_out_value    -- SUM(returning_value) for OL+DL who left this team
--                                     via the portal, computed at the destination team's
--                                     row in marts.player_returning_value (so the
--                                     in/out scales are directly comparable)
--       net_portal_trench_value    -- in_value - out_value. Positive = team won the
--                                     portal trench exchange this cycle. Surfaces the
--                                     "Tennessee -1.6 / Texas A&M +1.1" pattern that
--                                     gross-only investment hides.
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
-- Player-grain projection from the U5 matview. Originally this CTE LEFT JOINed
-- rp.fct_player_seasons to pull games_started for an n_returning_starters
-- (>= 8 starts) count, but CFBD's /roster does not return games_started --
-- the column is always NULL and the FILTER silently produced 0 for every team.
-- We now expose n_returners (count of returners regardless of starter status);
-- a true starter gate lands with U10's quality formulas (see refresh_all_marts.sql
-- Layer 6 notes).
prv_proj AS (
  SELECT
    target_team,
    target_season,
    position_group,
    returning_value,
    is_returning,
    is_portal_in,
    is_recruit
  FROM marts.player_returning_value
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

    -- Counts. n_returners is "count of returning players" not "count of returning
    -- starters" because CFBD's /roster endpoint does not return games_started --
    -- so we cannot distinguish starters from rotation players in v1. The original
    -- spec asked for `n_returning_starters AND games_started >= 8` but games_started
    -- is always NULL in rp.fct_player_seasons, which silently produced 0 for every
    -- team. U10's quality formulas may add a starter-equivalent gate via PFF data
    -- or per-game appearances joined from core.game_player_stats.
    COUNT(*) FILTER (WHERE is_returning = true)::int                     AS n_returners,
    COUNT(*) FILTER (WHERE is_portal_in = true)::int                     AS n_portal_in,
    COUNT(*) FILTER (
      WHERE is_recruit = true AND returning_value > 0.10
    )::int                                                               AS n_recruits_contributing,
    COUNT(*) FILTER (WHERE position_group IS NULL)::int                  AS n_unknown_position
  FROM prv_proj
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
-- Portal trench EXCHANGE: separate from gross n_portal_in/out because we want
-- value-weighted, position-filtered (OL+DL only) flows that are scale-comparable
-- between in and out.
-- Outgoing trench value: a player who left team X via portal lands at team Y;
-- their marts.player_returning_value row at Y has the canonical returning_value.
-- We attribute that same number as "X's lost trench value" because Y captured it
-- and X gave it up. This keeps the in/out scales consistent.
portal_trench_in AS (
  SELECT
    target_team   AS team,
    target_season AS season,
    SUM(returning_value)::numeric(7,3) AS portal_trench_in_value
  FROM marts.player_returning_value
  WHERE position_group IN ('OL','DL')
    AND is_portal_in
  GROUP BY target_team, target_season
),
portal_trench_out AS (
  SELECT
    fpm.source_team   AS team,
    prv.target_season AS season,
    SUM(prv.returning_value)::numeric(7,3) AS portal_trench_out_value
  FROM marts.player_returning_value prv
  JOIN rp.fct_player_movements fpm
    ON fpm.player_id = prv.player_id
   AND fpm.transition_season = prv.target_season
  WHERE prv.position_group IN ('OL','DL')
    AND fpm.match_method LIKE 'portal_%'
    AND fpm.source_team IS NOT NULL
    AND fpm.source_team <> prv.target_team
  GROUP BY fpm.source_team, prv.target_season
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
  COALESCE(tpr.n_returners,             0) AS n_returners,
  COALESCE(tpr.n_portal_in,             0) AS n_portal_in,
  COALESCE(po.n_portal_out,             0) AS n_portal_out,
  COALESCE(tpr.n_recruits_contributing, 0) AS n_recruits_contributing,
  COALESCE(tpr.n_unknown_position,      0) AS n_unknown_position,

  -- Portal trench exchange (value-weighted, OL+DL only)
  COALESCE(pti.portal_trench_in_value,  0)::numeric(7,3) AS portal_trench_in_value,
  COALESCE(pto.portal_trench_out_value, 0)::numeric(7,3) AS portal_trench_out_value,
  (COALESCE(pti.portal_trench_in_value,  0)
   - COALESCE(pto.portal_trench_out_value, 0))::numeric(7,3) AS net_portal_trench_value,

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
LEFT JOIN portal_out        po  ON po.team   = tpr.team   AND po.season  = tpr.season
LEFT JOIN portal_trench_in  pti ON pti.team  = tpr.team   AND pti.season = tpr.season
LEFT JOIN portal_trench_out pto ON pto.team  = tpr.team   AND pto.season = tpr.season
LEFT JOIN cfbd_calibration  cal ON cal.team  = tpr.team   AND cal.season = tpr.season
LEFT JOIN season_max        sm  ON sm.season = tpr.season;

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
