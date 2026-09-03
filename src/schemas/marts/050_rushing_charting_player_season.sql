-- marts.rushing_charting_player_season
-- =============================================================================
-- Rushing charting (spec v5.26.0, 2026-09-03 rushing-charting unit, U6).
-- Thin per-row mart over stats.rushing_player_season (PK season, player_id,
-- team) -- charting is player-season-grain already; nothing to aggregate.
-- Direction splits are deliberately OUT of this mart: they live in the tall
-- marts.rushing_charting_direction_season (052) per the product decision
-- "direction splits as tall rows in one view, not wide columns on the
-- headline views" -- wide columns here would push this mart past 60 columns
-- and duplicate the per-direction detail the tall mart already carries.
--
-- Column names LIVE-VERIFIED 2026-09-03 via information_schema.columns
-- against stats.rushing_player_season (108 columns, post Stage A backfill:
-- 1,698 player-seasons), including a check for dlt VARIANT (__v_double)
-- twins on every metric column -- same pattern as
-- src/schemas/009_variant_columns.sql / marts/045's average_yards_after_catch
-- precedent. Result for this mart's headline (non-direction) columns: only
-- two of them have a live twin --
--   - open_field_yards: bigint base column WITH a __v_double twin.
--   - power_success: bigint base column WITH a __v_double twin.
-- Both are COALESCEd below. Every other headline metric is cast to
-- double precision with no twin to COALESCE (either it is natively
-- double precision already, or its bigint values were never variant-typed
-- by dlt) -- the cast is applied uniformly per this unit's KTD7 rule so a
-- consumer never has to guess which columns are bigint vs double.
-- direction_eligible_attempts, direction_available_attempts, and every
-- other *_attempts/attempts/sacks/kneels/team_rushes count column stays
-- bigint (untouched) -- they are exact counts, never variant-typed.
--
-- NULL/denominator semantics (mirrors migration 057/mart 045's phrasing):
-- NULL on a rate or yardage-tier metric (line_yards, second_level_yards,
-- open_field_yards, stuff_rate, power_success, explosiveness, success_rate,
-- ppa, total_ppa, yards_per_carry, and their *_total companions) means the
-- carries behind that value were not charted; 0 is a real observed value.
-- rushing_yards_available is the coverage denominator for the yardage-tier
-- metrics; direction_eligible_attempts and direction_available_attempts are
-- the coverage denominators for direction splits (used together with
-- marts.rushing_charting_direction_season's per-direction rows) -- these
-- three denominators are DIFFERENT counts and must never be merged or
-- aliased into one "attempts_available" column, same discipline as the
-- passing charting marts' two-denominator rule.
--
-- R10 CONTRACT (non-reconciliation, upstream-by-design): this player-season
-- row's `attempts` will NOT sum, across all players on a team-season, to
-- that team-season's offense_attempts in marts.rushing_charting_team_season.
-- CFBD's attribution splits some carries to team-only (unresolved rusher) or
-- multi-carrier (lateral/fumble-recovery) buckets that never attach to any
-- individual player row -- see stats.rushing_team_season's
-- offense_team_rushes/offense_multi_carrier_attempts/
-- offense_unattributed_attempts. Do not build a "team share" leaderboard by
-- dividing this mart's attempts by the team mart's offense_attempts without
-- accounting for that gap.
--
-- `position` is joined from core.roster on (id, team, year) -- ALL THREE of
-- core.roster's own primary-key columns (src/pipelines/sources/rosters.py),
-- matched against rushing_player_season's player_id/team/season -- so this
-- is a join to roster's exact PK, not a name-string join, and cannot fan
-- out: at most one roster row can match. `conference` needs no join at all
-- -- it is already a column on stats.rushing_player_season itself.
--
-- Data starts 2025 (RUSHING_DATA_START in src/pipelines/sources/rushing.py).
--
-- DEPLOY-ORDER CAVEAT (same shape as mart 045's): dlt omits a column
-- entirely from a table's schema when every value loaded for it so far was
-- NULL. This file must be applied AFTER the rushing source's first
-- successful load, not before -- the 2026-09-03 live check above confirms
-- every column this file reads already exists today.
DROP MATERIALIZED VIEW IF EXISTS marts.rushing_charting_player_season CASCADE;

CREATE MATERIALIZED VIEW marts.rushing_charting_player_season AS
SELECT
    rps.season,
    rps.player_id,
    rps.player,
    rps.team,
    rps.conference,
    r.position,
    -- box-score / attribution counts -- exact counts, never variant-typed
    rps.attempts,
    rps.rushing_yards_available,
    rps.individual_attempts,
    rps.unattributed_attempts,
    rps.sacks,
    rps.kneels,
    rps.team_rushes,
    rps.multi_carrier_attempts,
    rps.direction_eligible_attempts,
    rps.direction_available_attempts,
    -- charting metrics -- cast to double precision uniformly (KTD7);
    -- open_field_yards and power_success carry a live __v_double twin
    rps.total_rushing_yards::double precision AS total_rushing_yards,
    rps.yards_per_carry::double precision AS yards_per_carry,
    rps.success_rate::double precision AS success_rate,
    rps.ppa::double precision AS ppa,
    rps.total_ppa::double precision AS total_ppa,
    rps.line_yards::double precision AS line_yards,
    rps.line_yards_total::double precision AS line_yards_total,
    rps.second_level_yards::double precision AS second_level_yards,
    rps.second_level_yards_total::double precision AS second_level_yards_total,
    COALESCE(rps.open_field_yards::double precision, rps.open_field_yards__v_double)
        AS open_field_yards,
    rps.open_field_yards_total::double precision AS open_field_yards_total,
    rps.stuff_rate::double precision AS stuff_rate,
    COALESCE(rps.power_success::double precision, rps.power_success__v_double)
        AS power_success,
    rps.explosiveness::double precision AS explosiveness
FROM stats.rushing_player_season rps
LEFT JOIN core.roster r
    ON r.id::text = rps.player_id::text
    AND r.team = rps.team
    AND r.year = rps.season;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.rushing_charting_player_season (season, player_id, team);

-- Query indexes
CREATE INDEX ON marts.rushing_charting_player_season (season, team);
CREATE INDEX ON marts.rushing_charting_player_season (player_id);

-- Re-grant on every apply: DROP MATERIALIZED VIEW loses grants (no ALTER
-- DEFAULT PRIVILEGES for the PostgREST roles in marts). public.get_player_detail()
-- is LANGUAGE sql with no SECURITY DEFINER (runs as the caller) and LEFT
-- JOINs this mart directly, so anon/authenticated need direct SELECT here
-- for the RPC to work.
GRANT SELECT ON marts.rushing_charting_player_season TO anon, authenticated;
