-- marts.player_returning_value -- U5 of the returning production plan.
-- Player-grain returning value: one row per (player_id, target_team, target_season).
-- The product of five factors yields the canonical returning_value:
--
--   returning_value = base_production
--                   * position_weight
--                   * continuity_factor
--                   * competition_factor
--                   * health_factor
--
-- Each factor is stored as a separate column so decomposition is queryable
-- without recomputation.
--
-- KNOWN LIMITATIONS:
-- * base_production is a v1 placeholder = 1.0 for every row. The plan's
--   "snap-fraction (games_played / 13)" formulation assumed games_played
--   was available; CFBD /roster doesn't return it. We considered branching
--   (1.0 for prior-roster, 0.0 for recruits) but that zeroed out recruits
--   entirely -- the continuity_factor (e.g., recruit_4star = 0.15) is the
--   spec's intended "year-1 contribution cap" channel, and it only works
--   when base != 0. Setting base = 1.0 preserves the spec's semantics:
--   recruits contribute via continuity, returners via full position weight.
--   U10 replaces base_production with z-score quality formulas per
--   position; this matview's structure won't change.
-- * Movements with destination_team = NULL are filtered out. These are
--   players who entered the portal but never committed to a destination
--   (~5% residue per settled season; ~23% during an active spring window).
--   They're real movement events but don't fit a (player, target_team,
--   target_season) grain because there's no target_team. The silver layer
--   (rp.fct_player_movements) keeps them as audit trail; the gold layer
--   excludes them so cfb-app rollups are clean.
-- * health_factor sources rp.injuries_season_ending which is empty in v1
--   (U8 will populate it). All rows currently get health_factor = 1.0.
-- * competition_factor uses ratings.sp_ratings (final SP+ ranking, not
--   pre-season). FCS opponents have no ranking and are excluded from the
--   average. Players with no schedule data (recruits) get the 1.0 default.
-- * Conference-driven movement_type (portal_p5_to_p5 etc.) was assigned
--   in U3 using current ref.teams snapshot. Pre-realignment seasons
--   (esp. Pac-12 dissolution) reflect current conferences, not historical.

DROP MATERIALIZED VIEW IF EXISTS marts.player_returning_value CASCADE;

CREATE MATERIALIZED VIEW marts.player_returning_value AS
WITH
-- Base layer: every movement event from rp.fct_player_movements. Two LEFT JOINs
-- to fct_player_seasons:
--   - fps_source (transition_season - 1): the player's prior season; drives
--     position+stats for returners/portal and the schedule for competition_factor.
--   - fps_target (transition_season): the player's target-season roster row;
--     used as fallback for position when fps_source is missing (recruits).
-- Returners/portal: typically have both rows. Recruits: only fps_target. Unmatched
-- portal (synthetic id): neither -- position is NULL, position_weight defaults to 0.
base_data AS (
  SELECT
    fpm.player_id,
    fpm.transition_season                       AS target_season,
    fpm.transition_season - 1                   AS source_season,
    fpm.destination_team                        AS target_team,
    fpm.movement_type,
    fpm.match_confidence,
    fpm.match_method,
    -- Position: prefer source-season row (where they actually played); fall back
    -- to target-season row for recruits. NULL only for unmatched-synthetic rows.
    COALESCE(fps_source.position, fps_target.position)             AS position,
    COALESCE(fps_source.position_group, fps_target.position_group) AS position_group,
    fps_source.team                             AS source_team_for_schedule,
    -- v1 base_production = 1.0 universally (see KNOWN LIMITATIONS at top).
    1.00                                        AS base_production,
    fpm.match_method = 'roster_continuity'      AS is_returning,
    (fpm.match_method LIKE 'portal_%'
     OR fpm.match_method = 'unmatched')         AS is_portal_in,
    fpm.match_method = 'recruit'                AS is_recruit
  FROM rp.fct_player_movements fpm
  LEFT JOIN rp.fct_player_seasons fps_source
    ON fps_source.player_id = fpm.player_id
    AND fps_source.season   = fpm.transition_season - 1
  LEFT JOIN rp.fct_player_seasons fps_target
    ON fps_target.player_id = fpm.player_id
    AND fps_target.season   = fpm.transition_season
  -- Filter movements with no destination -- they fail the (player, target_team,
  -- target_season) grain. Keeps the matview semantically clean for downstream
  -- consumers; full audit remains in rp.fct_player_movements.
  WHERE fpm.destination_team IS NOT NULL
),
-- Schedule + opponent SP+ rank. Build a per-team schedule via UNION
-- (each game contributes to both home and away team's schedule), then
-- join ratings.sp_ratings on opponent + season for the avg opp rank.
schedule AS (
  SELECT season, home_team AS team, away_team AS opponent
  FROM core.games
  WHERE completed = true
  UNION ALL
  SELECT season, away_team AS team, home_team AS opponent
  FROM core.games
  WHERE completed = true
),
team_competition AS (
  SELECT
    s.team,
    s.season,
    AVG(sp.ranking)::numeric AS avg_opp_rank
  FROM schedule s
  -- Opponents without an SP+ rating (FCS opponents) are excluded by INNER JOIN
  JOIN ratings.sp_ratings sp
    ON sp.team = s.opponent
   AND sp.year = s.season
  GROUP BY s.team, s.season
),
-- Combine everything: factor lookups, competition, health
combined AS (
  SELECT
    bd.player_id,
    bd.target_team,
    bd.target_season,
    bd.source_season,
    bd.position,
    bd.position_group,
    bd.movement_type,
    bd.match_confidence,
    bd.is_returning,
    bd.is_portal_in,
    bd.is_recruit,
    bd.base_production,
    COALESCE(dpw.position_weight, 0.000)        AS position_weight,
    dcf.continuity_factor                        AS continuity_factor,
    -- competition_factor: clamped to [0.7, 1.3].
    -- median FBS opponent rank ~67 -> factor = 1.0 (no adjustment).
    -- top schedule (avg_rank=1) -> 1.30. bottom schedule (avg_rank=134) -> 0.70.
    -- recruits / no-schedule players -> 1.00 default.
    GREATEST(0.70, LEAST(1.30,
      COALESCE(
        1.0 + (67.0 - tc.avg_opp_rank) / 67.0 * 0.3,
        1.00
      )
    ))::numeric(4,2)                             AS competition_factor,
    -- health_factor: severity 'season' -> 0.40 (returning from full ender),
    -- 'partial' -> 0.70 (mid-season ender returning), default 1.00.
    -- v1 injuries table is empty so all rows get 1.00.
    COALESCE(
      CASE inj.severity
        WHEN 'season'  THEN 0.40
        WHEN 'partial' THEN 0.70
        ELSE 1.00
      END,
      1.00
    )::numeric(4,2)                              AS health_factor
  FROM base_data bd
  LEFT JOIN rp.dim_position_weights dpw
    ON dpw.position         = bd.position
   AND dpw.scheme_archetype = 'static'
  LEFT JOIN rp.dim_continuity_factors dcf
    ON dcf.movement_type = bd.movement_type
  LEFT JOIN team_competition tc
    ON tc.team   = bd.source_team_for_schedule
   AND tc.season = bd.source_season
  LEFT JOIN rp.injuries_season_ending inj
    ON inj.player_id     = bd.player_id
   AND inj.injury_season = bd.source_season
)
SELECT
  player_id,
  target_team,
  target_season,
  source_season,
  position,
  position_group,
  movement_type,
  base_production::numeric(6,3)                 AS base_production,
  position_weight::numeric(5,3)                 AS position_weight,
  continuity_factor::numeric(4,2)               AS continuity_factor,
  competition_factor,
  health_factor,
  (base_production
   * position_weight
   * continuity_factor
   * competition_factor
   * health_factor)::numeric(6,3)               AS returning_value,
  is_returning,
  is_portal_in,
  is_recruit,
  match_confidence,
  NOW()                                         AS generated_at
FROM combined;

-- UNIQUE INDEX is required for REFRESH MATERIALIZED VIEW CONCURRENTLY
CREATE UNIQUE INDEX idx_player_returning_value_pk
  ON marts.player_returning_value (player_id, target_team, target_season);

-- Access patterns: cfb-app filters by team-season, ranks by returning_value
CREATE INDEX idx_player_returning_value_target
  ON marts.player_returning_value (target_team, target_season);
CREATE INDEX idx_player_returning_value_top
  ON marts.player_returning_value (target_season, returning_value DESC);
CREATE INDEX idx_player_returning_value_position
  ON marts.player_returning_value (position_group, target_season);

-- Grants: SECURITY INVOKER + read-only-database invariant per 2026-02-07 hardening.
GRANT SELECT ON marts.player_returning_value TO anon, authenticated;

COMMENT ON MATERIALIZED VIEW marts.player_returning_value IS
  'Player-grain returning value, one row per (player_id, target_team, target_season). '
  'Five-factor decomposition: base * position * continuity * competition * health. '
  'v1 base_production is a placeholder (1.0 for prior-roster movements, 0.0 for '
  'recruits); U10 replaces it with z-score quality formulas. Refresh via '
  'REFRESH MATERIALIZED VIEW CONCURRENTLY; depends on rp.fct_player_seasons + '
  'rp.fct_player_movements being current.';
