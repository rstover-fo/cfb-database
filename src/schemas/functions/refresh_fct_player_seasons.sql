-- rp.refresh_fct_player_seasons() -- U2 of the returning production plan.
-- Populates rp.fct_player_seasons from core.roster + stats.player_season_stats
-- + recruiting.recruits, with canonical position taxonomy.
--
-- Idempotent: TRUNCATE then INSERT. Re-running yields identical state.
-- SECURITY DEFINER + SET search_path='' per 2026-02-07 hardening pattern.

CREATE OR REPLACE FUNCTION rp.refresh_fct_player_seasons()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
BEGIN
  TRUNCATE rp.fct_player_seasons;

  INSERT INTO rp.fct_player_seasons (
    player_id, season, team, conference,
    position_detail, position, position_group,
    class, height_in, weight_lb,
    games_played, games_started, snaps_estimated,
    stat_pass_attempts, stat_pass_yards, stat_pass_tds, stat_pass_ints,
    stat_rush_attempts, stat_rush_yards, stat_rush_tds,
    stat_rec_targets, stat_rec_catches, stat_rec_yards, stat_rec_tds,
    stat_tackles_solo, stat_tackles_ast,
    stat_tfl, stat_sacks, stat_int, stat_pbu, stat_ff, stat_fr,
    recruiting_composite, recruiting_stars
  )
  WITH
  -- Pivot stats from long format (category, stat_type, stat) into wide-per-season.
  -- SUM aggregates across teams when a player switched mid-season (RP-002 -- stats follow
  -- the player; team attribution is handled separately via roster_dedup below).
  -- The `stat` column is VARCHAR; regex-guard the cast to handle any future non-numeric values.
  pivoted_stats AS (
    SELECT
      ps.player_id,
      ps.season::int                                                   AS season,
      MAX(ps.conference)                                               AS conference,
      SUM(CASE WHEN ps.category = 'passing'   AND ps.stat_type = 'ATT'         AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::int AS stat_pass_attempts,
      SUM(CASE WHEN ps.category = 'passing'   AND ps.stat_type = 'YDS'         AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::int AS stat_pass_yards,
      SUM(CASE WHEN ps.category = 'passing'   AND ps.stat_type = 'TD'          AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::int AS stat_pass_tds,
      SUM(CASE WHEN ps.category = 'passing'   AND ps.stat_type = 'INT'         AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::int AS stat_pass_ints,
      SUM(CASE WHEN ps.category = 'rushing'   AND ps.stat_type = 'CAR'         AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::int AS stat_rush_attempts,
      SUM(CASE WHEN ps.category = 'rushing'   AND ps.stat_type = 'YDS'         AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::int AS stat_rush_yards,
      SUM(CASE WHEN ps.category = 'rushing'   AND ps.stat_type = 'TD'          AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::int AS stat_rush_tds,
      SUM(CASE WHEN ps.category = 'receiving' AND ps.stat_type = 'REC'         AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::int AS stat_rec_catches,
      SUM(CASE WHEN ps.category = 'receiving' AND ps.stat_type = 'YDS'         AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::int AS stat_rec_yards,
      SUM(CASE WHEN ps.category = 'receiving' AND ps.stat_type = 'TD'          AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::int AS stat_rec_tds,
      SUM(CASE WHEN ps.category = 'defensive' AND ps.stat_type = 'SOLO'        AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::int AS stat_tackles_solo,
      -- AST = TOT - SOLO (CFBD reports total tackles, solo tackles; assists is the difference)
      SUM(
        CASE WHEN ps.category = 'defensive' AND ps.stat_type = 'TOT'  AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END
        - CASE WHEN ps.category = 'defensive' AND ps.stat_type = 'SOLO' AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END
      )::int                                                                                                                                              AS stat_tackles_ast,
      SUM(CASE WHEN ps.category = 'defensive'     AND ps.stat_type = 'TFL'   AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::numeric(5,1) AS stat_tfl,
      SUM(CASE WHEN ps.category = 'defensive'     AND ps.stat_type = 'SACKS' AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::numeric(5,1) AS stat_sacks,
      SUM(CASE WHEN ps.category = 'interceptions' AND ps.stat_type = 'INT'   AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::int          AS stat_int,
      SUM(CASE WHEN ps.category = 'defensive'     AND ps.stat_type = 'PD'    AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::int          AS stat_pbu,
      SUM(CASE WHEN ps.category = 'fumbles'       AND ps.stat_type = 'REC'   AND ps.stat ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ps.stat::numeric ELSE 0 END)::int          AS stat_fr
    FROM stats.player_season_stats ps
    WHERE ps.season BETWEEN 2020 AND 2026
    GROUP BY ps.player_id, ps.season
  ),
  -- Dedupe roster to one row per (player_id, season). For mid-season transfers, pick the
  -- alphabetically-last team -- deterministic and matches RP-002's "attribute to last team"
  -- in absence of within-season ordering. Stats are aggregated across teams above.
  roster_dedup AS (
    SELECT DISTINCT ON (r.id, r.year)
      r.id, r.year, r.team, r.position, r.height, r.weight,
      r.first_name, r.last_name
    FROM core.roster r
    WHERE r.id IS NOT NULL
      AND r.year BETWEEN 2020 AND 2026
    ORDER BY r.id, r.year, r.team DESC
  ),
  -- Recruits can have multiple rows per athlete_id (reclassifiers per memory 2026-02-05).
  -- Keep the latest record by year so we have one composite/stars value per player.
  recruits_dedup AS (
    SELECT DISTINCT ON (rec.athlete_id)
      rec.athlete_id, rec.stars, rec.rating
    FROM recruiting.recruits rec
    WHERE rec.athlete_id IS NOT NULL
    ORDER BY rec.athlete_id, rec.year DESC
  )
  SELECT
    r.id::text                                               AS player_id,
    r.year::int                                              AS season,
    r.team,
    ps.conference,
    r.position                                               AS position_detail,
    -- 11-canonical position (per requirements doc data contract)
    CASE
      WHEN r.position = 'QB'                              THEN 'QB'
      WHEN r.position IN ('RB', 'FB')                     THEN 'RB'
      WHEN r.position = 'WR'                              THEN 'WR'
      WHEN r.position = 'TE'                              THEN 'TE'
      WHEN r.position IN ('OL', 'OT', 'G', 'OG', 'C')     THEN 'OL'
      WHEN r.position IN ('DE', 'EDGE')                   THEN 'EDGE'
      WHEN r.position IN ('DT', 'NT', 'DL')               THEN 'DT'
      WHEN r.position IN ('LB', 'OLB', 'ILB')             THEN 'LB'
      WHEN r.position = 'CB'                              THEN 'CB'
      WHEN r.position IN ('S', 'FS', 'SS', 'DB')          THEN 'S'
      WHEN r.position IN ('PK', 'K', 'P', 'LS', 'KR', 'PR') THEN 'ST'
      ELSE 'ST'  -- ATH, '?', NULL, anything else -- bucketed to ST (weight 0 in dim_position_weights)
    END                                                      AS position,
    -- 8-group rollup (per plan U2 -- drives base_production formulas in U10)
    CASE
      WHEN r.position = 'QB'                                                THEN 'QB'
      WHEN r.position IN ('RB', 'FB')                                       THEN 'RB'
      WHEN r.position IN ('WR', 'TE')                                       THEN 'WR_TE'
      WHEN r.position IN ('OL', 'OT', 'G', 'OG', 'C')                       THEN 'OL'
      WHEN r.position IN ('DE', 'EDGE', 'DT', 'NT', 'DL')                   THEN 'DL'
      WHEN r.position IN ('LB', 'OLB', 'ILB')                               THEN 'LB'
      WHEN r.position IN ('CB', 'S', 'FS', 'SS', 'DB')                      THEN 'DB'
      ELSE 'ST'  -- PK, K, P, LS, KR, PR, ATH, '?', NULL
    END                                                      AS position_group,
    NULL::varchar                                            AS class,           -- not in CFBD roster; deferred
    r.height::int                                            AS height_in,
    r.weight::int                                            AS weight_lb,
    NULL::int                                                AS games_played,    -- not in CFBD roster; U10 may derive
    NULL::int                                                AS games_started,   -- not in CFBD roster; U10 may derive
    NULL::int                                                AS snaps_estimated, -- CFBD has no reliable snap data
    ps.stat_pass_attempts,
    ps.stat_pass_yards,
    ps.stat_pass_tds,
    ps.stat_pass_ints,
    ps.stat_rush_attempts,
    ps.stat_rush_yards,
    ps.stat_rush_tds,
    NULL::int                                                AS stat_rec_targets, -- CFBD does not return targets; deferred
    ps.stat_rec_catches,
    ps.stat_rec_yards,
    ps.stat_rec_tds,
    ps.stat_tackles_solo,
    ps.stat_tackles_ast,
    ps.stat_tfl,
    ps.stat_sacks,
    ps.stat_int,
    ps.stat_pbu,
    NULL::int                                                AS stat_ff,        -- not in CFBD player_season_stats fumbles category
    ps.stat_fr,
    rd.rating::numeric(5,4)                                  AS recruiting_composite,
    rd.stars::int                                            AS recruiting_stars
  FROM roster_dedup r
  LEFT JOIN pivoted_stats ps
    ON  ps.player_id = r.id::text
    AND ps.season    = r.year::int
  LEFT JOIN recruits_dedup rd
    ON rd.athlete_id = r.id::text
  WHERE r.year BETWEEN 2020 AND 2026;

  -- Refresh planner stats for downstream U3/U5 joins
  ANALYZE rp.fct_player_seasons;
END;
$function$;

COMMENT ON FUNCTION rp.refresh_fct_player_seasons() IS
  'U2 loader. Populates rp.fct_player_seasons for seasons 2020-2026 from '
  'core.roster + stats.player_season_stats + recruiting.recruits. Idempotent '
  '(TRUNCATE + INSERT). Multi-team-season players: stats SUM-aggregated across '
  'teams; team attribution is alphabetically-last per RP-002.';

-- SECURITY DEFINER + REVOKE EXECUTE FROM PUBLIC: the function performs TRUNCATE
-- on rp.fct_player_seasons. Without revoking PUBLIC execute, anon could call it
-- via PostgREST and trigger a full-table reload, defeating the read-only-database
-- invariant established by 019_returning_schema.sql's REVOKE on rp tables.
REVOKE EXECUTE ON FUNCTION rp.refresh_fct_player_seasons() FROM PUBLIC;
