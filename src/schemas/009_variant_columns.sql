-- Variant column consolidation: merge __v_double columns into correct columns
-- Sprint 0 identified 3 variant columns in user tables:
--   1. recruiting.recruits.height__v_double -> height (bigint)
--   2. metrics.ppa_teams.defense__first_down__v_double -> defense__first_down (bigint)
--   3. metrics.pregame_win_probability.spread__v_double -> spread (bigint)
--
-- dlt creates __v_double columns when API returns mixed types (int vs float).
-- These migrations coalesce the data and drop the variant columns.
-- All statements are idempotent.

-- =============================================================================
-- 1. recruiting.recruits.height__v_double -> height
-- Some recruits have height as double (e.g., 73.0) instead of integer (73)
-- =============================================================================
UPDATE recruiting.recruits
SET height = height__v_double::bigint
WHERE height IS NULL
  AND height__v_double IS NOT NULL;

ALTER TABLE recruiting.recruits
DROP COLUMN IF EXISTS height__v_double;

-- =============================================================================
-- 2. metrics.ppa_teams.defense__first_down__v_double -> defense__first_down
-- defense__first_down is bigint but some values came as float
-- =============================================================================
UPDATE metrics.ppa_teams
SET defense__first_down = defense__first_down__v_double::bigint
WHERE defense__first_down IS NULL
  AND defense__first_down__v_double IS NOT NULL;

ALTER TABLE metrics.ppa_teams
DROP COLUMN IF EXISTS defense__first_down__v_double;

-- =============================================================================
-- 3. metrics.pregame_win_probability.spread__v_double -> spread
-- spread is bigint but some values came as float (e.g., -3.5)
-- NOTE: Spread values like -3.5 will be truncated to -3 when cast to bigint.
-- Consider changing the spread column to double precision if fractional spreads
-- matter for analysis.
-- =============================================================================

-- First, alter spread column to double precision to preserve fractional values
ALTER TABLE metrics.pregame_win_probability
ALTER COLUMN spread TYPE double precision USING spread::double precision;

-- Now merge the variant column
UPDATE metrics.pregame_win_probability
SET spread = spread__v_double
WHERE spread IS NULL
  AND spread__v_double IS NOT NULL;

ALTER TABLE metrics.pregame_win_probability
DROP COLUMN IF EXISTS spread__v_double;

-- =============================================================================
-- Verify: should return 0 rows if all variants are consolidated
-- =============================================================================
-- SELECT column_name, table_schema, table_name
-- FROM information_schema.columns
-- WHERE column_name LIKE '%__v_%'
--   AND table_schema NOT IN ('_dlt', 'information_schema', 'pg_catalog');
