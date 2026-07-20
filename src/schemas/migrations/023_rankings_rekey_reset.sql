-- One-off reset BEFORE reloading rankings under the widened merge key
-- =============================================================================
-- src/pipelines/sources/rankings.py now merges on
-- [season, season_type, week, poll, school] and stamps season_type on every
-- row (fixes the confirmed rank-tie collapse and postseason/week-1 collision;
-- see src/schemas/api/027_poll_rankings.sql header).
--
-- The reload cannot run against the old table: rows written under the old
-- rank-based key predate the season_type column, and dlt adds new merge-key
-- columns as NOT NULL -- the first reload attempt failed with
--   column "season_type" of relation "rankings" contains null values
-- (deploy run 29765084967). Dropping the table (rather than deleting rows)
-- also sheds any index built for the old key, which would reject the tied
-- ranks the new key exists to preserve; dlt recreates the table with the
-- new schema on the next load.
--
-- Safe because core.rankings is fully reproducible from the CFBD API and
-- the 2000-2025 reload (the table's entire loaded range) is the immediately
-- following deploy step. api.poll_rankings is dropped here (it depends on
-- the table) and recreated by applying 027 after the reload.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-022. Idempotent (IF EXISTS).

DROP VIEW IF EXISTS api.poll_rankings;
DROP TABLE IF EXISTS core.rankings;
