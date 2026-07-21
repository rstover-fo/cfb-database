-- Migration: 039_team_talent_staging_shell
-- Corrects: 036_recruiting_staging_repair.sql's deliberate team_talent skip
--
-- Validation run 29840221192 / job 88666912063 (recruiting step, 5.6s):
--   dlt.destinations.exceptions.DatabaseUndefinedRelation:
--   relation "recruiting_staging.team_talent" does not exist
--
-- 033_team_talent_reset.sql dropped recruiting.team_talent outright for the
-- CFBD v2 merge-key change (school -> team), and 036 deliberately did not
-- recreate a staging shell for it, reasoning that dlt would see the table as
-- brand-new and CREATE both the destination and staging tables itself. That
-- assumption is now disproven: dlt's stored destination schema (in
-- recruiting._dlt_version) still lists team_talent, so dlt skips table
-- creation entirely and goes straight to the merge path, which fails on the
-- missing staging relation (and would next fail on the missing destination
-- table).
--
-- Fix: recreate BOTH tables as empty shells laid out exactly like the
-- stored schema dlt still remembers -- columns year/school/talent plus the
-- dlt bookkeeping pair -- with two deliberate deviations:
--
--   * school is NULLABLE here (it was NOT NULL as part of the old merge
--     key). CFBD v2 responses carry `team` instead of `school`, so every
--     row loaded from now on has school NULL; a NOT NULL school would fail
--     the first insert. dlt never re-asserts nullability on existing
--     columns, so the relaxation sticks.
--   * `team` is deliberately ABSENT. dlt's stored schema predates the
--     rename, so its next schema sync generates
--       ALTER TABLE ... ADD COLUMN team varchar NOT NULL
--     which succeeds because both shells are empty -- and would fail with
--     "column already exists" if we pre-created it (same reasoning as 038's
--     rejected hand-backfill).
--
-- The table is empty after this migration; the next recruiting load (which
-- yields the full /talent snapshot per year) repopulates the loaded years,
-- and the historical backfill re-covers the rest. That data loss already
-- happened at 033 (the drop); this migration adds none.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-038. Idempotent (IF NOT EXISTS throughout).
--
--   python scripts/run_migrations.py --file src/schemas/migrations/039_team_talent_staging_shell.sql

CREATE SCHEMA IF NOT EXISTS recruiting_staging;

CREATE TABLE IF NOT EXISTS recruiting.team_talent (
    year bigint,
    school varchar,
    talent double precision,
    _dlt_load_id varchar NOT NULL,
    _dlt_id varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS recruiting_staging.team_talent
  (LIKE recruiting.team_talent INCLUDING ALL);
