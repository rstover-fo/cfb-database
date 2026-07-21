-- Migration: 040_team_talent_team_column
-- Corrects: 039_team_talent_staging_shell.sql's "team deliberately absent"
--
-- Validation run 29843551259 / job 88678224085 (recruiting step, 8.7s):
--   dlt.destinations.exceptions.DatabaseTerminalException:
--   column "team" of relation "team_talent" does not exist
--   LINE 1: ...rt into "recruiting_staging"."team_talent"("year","team","ta...
--
-- 039 left `team` out of the shells expecting dlt's next schema sync to
-- generate the ADD COLUMN itself (stored schema predating the v2 rename).
-- The run disproved that: dlt's stored schema for the recruiting dataset
-- ALREADY contains team_talent.team -- a schema sync in one of the earlier
-- failed runs advanced recruiting._dlt_version past the rename even though
-- the physical ALTER never landed (033 had dropped the table). With the
-- stored schema showing no diff, dlt goes straight to the staging INSERT,
-- which names the column that only exists in its bookkeeping.
--
-- Fix: add the column physically to both (still empty) shells, matching
-- what the stored schema believes is already there. Nullable is fine --
-- dlt never re-asserts nullability on existing columns, and every v2 row
-- carries a team value.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-039. Idempotent (IF NOT EXISTS).
--
--   python scripts/run_migrations.py --file src/schemas/migrations/040_team_talent_team_column.sql

ALTER TABLE recruiting.team_talent ADD COLUMN IF NOT EXISTS team varchar;
ALTER TABLE recruiting_staging.team_talent ADD COLUMN IF NOT EXISTS team varchar;
