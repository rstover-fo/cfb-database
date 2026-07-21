-- Migration: 036_recruiting_staging_repair
--
-- Observed failure (run 29836367032 / job 88653633766, recruiting step, 4.0s):
--   recruiting failed after 4.0s: Pipeline execution failed at `step=load`
--   when processing package with `load_id=1784641943.0012212` with exception:
--   relation "recruiting_staging.team_recruiting" does not exist
--
-- dlt staging mechanism: for a "merge" write-disposition resource, dlt's
-- postgres destination doesn't write the child rows straight into the
-- destination table. It first loads the load package's rows into a mirror
-- table of the same name in a separate "<dataset_name>_staging" schema
-- (here recruiting_staging, mirroring the recruiting destination schema --
-- see dataset_name="recruiting" in src/pipelines/run.py), then runs the
-- merge (delete-matching-keys + insert) from that staging table into
-- recruiting.team_recruiting within the same transaction, then truncates
-- the staging table for reuse on the next load. dlt does not recreate the
-- staging schema/table on every run -- it assumes both persist between
-- runs once created. When recruiting_staging.team_recruiting is missing
-- (cause unknown here -- possibly a manual cleanup or DROP SCHEMA run by
-- another session against this same Supabase instance; not something this
-- pipeline's code does), the load step has nowhere to stage rows and fails
-- immediately, before touching the real destination table at all --
-- recruiting.team_recruiting itself is untouched by this failure.
--
-- Fix: recreate the schema and the missing staging table(s) so the next
-- daily run's merge step has somewhere to land. `LIKE recruiting.team_recruiting
-- INCLUDING ALL` mirrors columns, defaults, NOT NULL, CHECK constraints, and
-- indexes from the real destination table -- dlt's own staging tables carry
-- the same column set for its DELETE...USING / INSERT...SELECT merge SQL to
-- work, so this is a superset of what dlt requires, not an approximation of
-- it. INCLUDING ALL does *not* copy foreign keys or the primary key's
-- underlying role as PK/unique (LIKE never copies FK constraints, full stop,
-- regardless of INCLUDING ALL) -- so this can't accidentally pull in the
-- fk_*_game constraints from 035 or create a dependency between the two
-- migrations. The extra indexes it does copy (idx_team_recruiting_year,
-- etc. from 005_recruiting.sql) add a small amount of insert overhead on
-- each load but are otherwise harmless -- dlt truncates and reuses this
-- table every run, so index bloat isn't a concern either.
--
-- Same guard added for recruiting.recruits, the other long-lived
-- merge-disposition recruiting resource (src/pipelines/sources/recruiting.py
-- recruits_resource, primary_key="id") -- its staging table wasn't reported
-- missing in the failing run, but it's the same failure mode waiting to
-- happen from the same unknown root cause, and this guard is a harmless
-- no-op (IF NOT EXISTS) if the table is already present.
--
-- recruiting.team_talent is deliberately NOT guarded here: 033_team_talent_reset.sql
-- dropped it outright for a merge-key change (school -> team), so dlt sees
-- it as a brand-new table on the next load and creates both the destination
-- table and its staging table itself -- there's nothing to repair. Adding a
-- pre-created empty staging table ahead of that fresh-table load path would
-- risk mismatching whatever column set dlt infers from the current /talent
-- response shape.
--
-- transfer_portal and recruiting_groups are also merge-disposition
-- resources in the same source, but neither appeared in the failing run's
-- error and they weren't reported missing -- left alone; add the same
-- pattern for either if/when they show the same "relation ... does not
-- exist" error.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-035. Idempotent (IF NOT EXISTS throughout).
--
--   python scripts/run_migrations.py --file src/schemas/migrations/036_recruiting_staging_repair.sql

CREATE SCHEMA IF NOT EXISTS recruiting_staging;

CREATE TABLE IF NOT EXISTS recruiting_staging.team_recruiting
  (LIKE recruiting.team_recruiting INCLUDING ALL);

CREATE TABLE IF NOT EXISTS recruiting_staging.recruits
  (LIKE recruiting.recruits INCLUDING ALL);
