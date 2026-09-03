-- marts.rushing_charting_team_season
-- =============================================================================
-- Rushing charting (spec v5.26.0, 2026-09-03 rushing-charting unit, U6).
-- Thin per-row mart over stats.rushing_team_season (PK season, team).
-- Flattens the raw dlt dunder shape (offense__*/defense__*) to
-- offense_*/defense_* -- per this repo's Contract Rule 4, raw dlt column
-- shapes must never reach the api layer. Direction splits are deliberately
-- OUT of this mart: they live in the tall
-- marts.rushing_charting_direction_season (052), same reasoning as mart 050.
--
-- Column names LIVE-VERIFIED 2026-09-03 via information_schema.columns
-- against stats.rushing_team_season (185 columns, post Stage A backfill:
-- 152 team-seasons), including a check for dlt VARIANT (__v_double) twins
-- on every metric column per side. Result for this mart's headline
-- (non-direction) columns:
--   - offense_line_yards_total, offense_second_level_yards,
--     offense_open_field_yards: bigint/mixed base columns WITH a live
--     __v_double twin -- COALESCEd below.
--   - No defense_* headline column has a live twin (the defense-side twins
--     that do exist on stats.rushing_team_season all live inside the
--     directions__* blocks, which this mart does not read).
-- Every other headline metric is cast to double precision with no twin to
-- COALESCE, applied uniformly per this unit's KTD7 rule. Count columns
-- (attempts, *_attempts, sacks, kneels, team_rushes, rushing_touchdowns,
-- *_available) stay bigint, untouched, on both sides.
--
-- CRITICAL semantic note (mirrors migration 057/mart 047's phrasing,
-- verbatim in spirit): defense_* is THIS TEAM'S RUSH DEFENSE -- what
-- opposing offenses did against them on the ground -- NOT the opponent's
-- own offensive row. NULL on a metric means the carries behind it were not
-- charted; 0 is a real observed value.
--
-- Denominators (R8): offense_rushing_yards_available /
-- defense_rushing_yards_available are the coverage denominators for each
-- side's yardage-tier metrics; offense_direction_eligible_attempts /
-- offense_direction_available_attempts (and the defense_ equivalents) are
-- the coverage denominators for direction splits at this grain;
-- offense_touchdown_status_available / defense_touchdown_status_available
-- are the coverage denominators for offense_rushing_touchdowns /
-- defense_rushing_touchdowns. These are DIFFERENT counts per side and must
-- never be merged or aliased into one column.
--
-- R10 CONTRACT (non-reconciliation, upstream-by-design): offense_attempts
-- on this team-season row will NOT equal the sum of `attempts` across every
-- player on that team in marts.rushing_charting_player_season -- see that
-- mart's header for why (team-only/multi-carrier/unattributed attempts
-- never attach to an individual player row). offense_team_rushes +
-- offense_multi_carrier_attempts + offense_unattributed_attempts
-- approximates the gap; it is not guaranteed to close it exactly (upstream
-- attribution buckets can overlap in ways CFBD does not document).
--
-- team_id: numeric CFBD/ESPN team id, derived from stats.rushing_plays'
-- (season, offense -> offense_id) mapping within CFBD's own rushing family
-- -- never a ref.teams name join (35 legitimate duplicate school names make
-- name joins a fanout trap). Published only when exactly one distinct
-- offense_id matches that (season, name), NULL on ambiguity -- the
-- never-guess aggregate-guard pattern from marts/023's coach_id, same as
-- mart 047's team_id. The team_ids CTE GROUPs BY (season, offense), so
-- (season, team) is unique there and the LEFT JOIN cannot fan out.
--
-- Data starts 2025 (RUSHING_DATA_START in src/pipelines/sources/rushing.py).
--
-- DEPLOY-ORDER CAVEAT (same shape as mart 045/047's): dlt omits a column
-- entirely from a table's schema when every value loaded for it so far was
-- NULL. This file must be applied AFTER the rushing source's first
-- successful load, not before -- the 2026-09-03 live check above confirms
-- every column this file reads already exists today.
DROP MATERIALIZED VIEW IF EXISTS marts.rushing_charting_team_season CASCADE;

CREATE MATERIALIZED VIEW marts.rushing_charting_team_season AS
WITH team_ids AS (
    SELECT
        rp.season,
        rp.offense AS team,
        CASE
            WHEN COUNT(DISTINCT rp.offense_id) = 1 THEN MIN(rp.offense_id)
        END AS team_id
    FROM stats.rushing_plays rp
    WHERE rp.offense_id IS NOT NULL
    GROUP BY rp.season, rp.offense
)
SELECT
    rts.season,
    ti.team_id,
    rts.team,
    rts.conference,
    -- ---------------------------------------------------------------
    -- offense_* (this team's run offense)
    -- ---------------------------------------------------------------
    rts.offense__attempts AS offense_attempts,
    rts.offense__rushing_yards_available AS offense_rushing_yards_available,
    rts.offense__total_rushing_yards::double precision AS offense_total_rushing_yards,
    rts.offense__yards_per_carry::double precision AS offense_yards_per_carry,
    rts.offense__individual_attempts AS offense_individual_attempts,
    rts.offense__unattributed_attempts AS offense_unattributed_attempts,
    rts.offense__sacks AS offense_sacks,
    rts.offense__kneels AS offense_kneels,
    rts.offense__team_rushes AS offense_team_rushes,
    rts.offense__multi_carrier_attempts AS offense_multi_carrier_attempts,
    rts.offense__direction_eligible_attempts AS offense_direction_eligible_attempts,
    rts.offense__direction_available_attempts AS offense_direction_available_attempts,
    rts.offense__success_rate::double precision AS offense_success_rate,
    rts.offense__ppa::double precision AS offense_ppa,
    rts.offense__total_ppa::double precision AS offense_total_ppa,
    rts.offense__line_yards::double precision AS offense_line_yards,
    COALESCE(rts.offense__line_yards_total::double precision, rts.offense__line_yards_total__v_double)
        AS offense_line_yards_total,
    COALESCE(rts.offense__second_level_yards::double precision, rts.offense__second_level_yards__v_double)
        AS offense_second_level_yards,
    rts.offense__second_level_yards_total::double precision AS offense_second_level_yards_total,
    COALESCE(rts.offense__open_field_yards::double precision, rts.offense__open_field_yards__v_double)
        AS offense_open_field_yards,
    rts.offense__open_field_yards_total::double precision AS offense_open_field_yards_total,
    rts.offense__stuff_rate::double precision AS offense_stuff_rate,
    rts.offense__power_success::double precision AS offense_power_success,
    rts.offense__explosiveness::double precision AS offense_explosiveness,
    rts.offense__touchdown_status_available AS offense_touchdown_status_available,
    rts.offense__rushing_touchdowns AS offense_rushing_touchdowns,
    -- ---------------------------------------------------------------
    -- defense_* (this team's run DEFENSE -- what opponents did against them)
    -- ---------------------------------------------------------------
    rts.defense__attempts AS defense_attempts,
    rts.defense__rushing_yards_available AS defense_rushing_yards_available,
    rts.defense__total_rushing_yards::double precision AS defense_total_rushing_yards,
    rts.defense__yards_per_carry::double precision AS defense_yards_per_carry,
    rts.defense__individual_attempts AS defense_individual_attempts,
    rts.defense__unattributed_attempts AS defense_unattributed_attempts,
    rts.defense__sacks AS defense_sacks,
    rts.defense__kneels AS defense_kneels,
    rts.defense__team_rushes AS defense_team_rushes,
    rts.defense__multi_carrier_attempts AS defense_multi_carrier_attempts,
    rts.defense__direction_eligible_attempts AS defense_direction_eligible_attempts,
    rts.defense__direction_available_attempts AS defense_direction_available_attempts,
    rts.defense__success_rate::double precision AS defense_success_rate,
    rts.defense__ppa::double precision AS defense_ppa,
    rts.defense__total_ppa::double precision AS defense_total_ppa,
    rts.defense__line_yards::double precision AS defense_line_yards,
    rts.defense__line_yards_total::double precision AS defense_line_yards_total,
    rts.defense__second_level_yards::double precision AS defense_second_level_yards,
    rts.defense__second_level_yards_total::double precision AS defense_second_level_yards_total,
    rts.defense__open_field_yards::double precision AS defense_open_field_yards,
    rts.defense__open_field_yards_total::double precision AS defense_open_field_yards_total,
    rts.defense__stuff_rate::double precision AS defense_stuff_rate,
    rts.defense__power_success::double precision AS defense_power_success,
    rts.defense__explosiveness::double precision AS defense_explosiveness,
    rts.defense__touchdown_status_available AS defense_touchdown_status_available,
    rts.defense__rushing_touchdowns AS defense_rushing_touchdowns
FROM stats.rushing_team_season rts
LEFT JOIN team_ids ti
    ON ti.season = rts.season
   AND ti.team = rts.team;

-- Required for REFRESH CONCURRENTLY. No team_id index: 152 rows, seq scans win.
CREATE UNIQUE INDEX ON marts.rushing_charting_team_season (season, team);

-- Re-grant on every apply: DROP MATERIALIZED VIEW loses grants (no ALTER
-- DEFAULT PRIVILEGES for the PostgREST roles in marts). cfb-app reads this
-- mart only via api.rushing_charting_team_season (owner-rights view), but
-- grant SELECT directly here too for symmetry with marts 050/052, which
-- public.get_player_detail() (no SECURITY DEFINER, runs as the caller)
-- reads directly.
GRANT SELECT ON marts.rushing_charting_team_season TO anon, authenticated;
