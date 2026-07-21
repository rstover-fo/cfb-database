-- Migration: 038_ref_root_key_reset
--
-- Validation run 29840221192 / job 88666912063 (reference step, 7.2s):
--   dlt.destinations.exceptions.DatabaseTerminalException:
--   column "_dlt_root_id" of relation "coaches__seasons" contains null values
-- preceded by dlt's warning:
--   You are adding a root_key in column _dlt_root_id to an already existing
--   table coaches__seasons ... Column(s) ['"_dlt_root_id"'] with NOT NULL
--   are being added to existing table coaches__seasons.
--
-- Root cause: dlt now propagates a root-key column (_dlt_root_id, NOT NULL)
-- into nested child tables of merge-disposition resources. ref.coaches__seasons
-- predates that requirement, so dlt issues
--   ALTER TABLE coaches__seasons ADD COLUMN _dlt_root_id varchar NOT NULL
-- against a populated table, which Postgres rejects ("contains null values").
--
-- Fix: TRUNCATE the affected CHILD tables (only), so dlt's ADD COLUMN ...
-- NOT NULL succeeds on an empty table and its stored schema advances
-- cleanly. Parents keep their rows -- root tables never carry _dlt_root_id,
-- so they need no ALTER -- and the reference source re-yields the full
-- /coaches snapshot on every run (merge disposition, no year filter), so the
-- child rows are fully rebuilt by the next reference load. Backfilling
-- _dlt_root_id values by hand was rejected: dlt's stored destination schema
-- would still lack the column, so dlt would attempt the same ADD COLUMN on
-- the next run and fail with "column already exists" instead.
--
-- The predicate is generic (any ref/ref_staging table with _dlt_parent_id
-- but no _dlt_root_id) rather than naming coaches__seasons, because every
-- pre-root-key child table in the ref dataset -- e.g. the teams/teams_fbs
-- logo child tables, if they were created without it -- hits the identical
-- ALTER on its next schema sync, and dlt only reports the first failure per
-- run. Scoped to the ref dataset only: it is small, fully re-yielded every
-- load, and the only dataset that has shown this failure (all other sources
-- cleared their schema sync in runs 29827205023/29840221192).
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-037. Idempotent: once dlt adds _dlt_root_id, the
-- predicate no longer matches and this is a no-op.
--
--   python scripts/run_migrations.py --file src/schemas/migrations/038_ref_root_key_reset.sql

DO $$
DECLARE
    child record;
    n integer := 0;
BEGIN
    FOR child IN
        SELECT c.table_schema, c.table_name
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON t.table_schema = c.table_schema
         AND t.table_name = c.table_name
         AND t.table_type = 'BASE TABLE'
        WHERE c.table_schema IN ('ref', 'ref_staging')
          AND c.column_name = '_dlt_parent_id'
          AND NOT EXISTS (
              SELECT 1
              FROM information_schema.columns r
              WHERE r.table_schema = c.table_schema
                AND r.table_name = c.table_name
                AND r.column_name = '_dlt_root_id'
          )
        ORDER BY c.table_schema, c.table_name
    LOOP
        EXECUTE format('TRUNCATE TABLE %I.%I', child.table_schema, child.table_name);
        RAISE NOTICE 'truncated %.% (child table missing _dlt_root_id; next reference load rebuilds it)',
            child.table_schema, child.table_name;
        n := n + 1;
    END LOOP;
    RAISE NOTICE '038_ref_root_key_reset: % child table(s) truncated', n;
END $$;
