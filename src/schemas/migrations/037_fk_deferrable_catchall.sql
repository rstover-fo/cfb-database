-- Migration: 037_fk_deferrable_catchall
-- Extends: 035_fk_deferrable.sql (applied to prod 2026-07-21 ~14:34Z)
--
-- 035 converted the five known core.games child FKs to DEFERRABLE INITIALLY
-- DEFERRED so dlt's merge delete+reinsert (one transaction per load package)
-- passes FK checks at COMMIT instead of mid-transaction. The validation run
-- after 035 (run 29840221192 / job 88666912063, games step) surfaced a SIXTH
-- constraint with the identical failure:
--   psycopg2.errors.ForeignKeyViolation: update or delete on table "games"
--   violates foreign key constraint "fk_advanced_game_stats_game" on table
--   "advanced_game_stats"
--   DETAIL:  Key (id)=(401752939) is still referenced from table
--   "advanced_game_stats".
-- Like fk_play_stats_game before it, fk_advanced_game_stats_game exists only
-- in prod -- it is not defined in any tracked migration -- so enumerating
-- constraints by name in the repo keeps missing one per validation round.
--
-- This migration ends the sequential discovery: it walks pg_constraint and
-- makes EVERY not-yet-deferrable FK whose referenced table lives in a
-- dlt-loaded dataset schema DEFERRABLE INITIALLY DEFERRED. Any table in
-- those schemas is subject to merge delete+reinsert within a load
-- transaction, so every FK pointing at one is exposed to the same
-- mid-transaction false positive. Deferring the check to COMMIT preserves
-- the exact same integrity guarantee (a genuinely orphaned child row still
-- fails the load) while tolerating the transient delete+reinsert window.
--
-- ALTER CONSTRAINT (not drop/re-add) keeps each constraint's existing
-- definition -- columns, ON DELETE/ON UPDATE actions, validation state --
-- which matters precisely because the untracked ones have no repo source to
-- recreate them from. conparentid = 0 skips partition-inherited copies
-- (ALTER CONSTRAINT on those is rejected; the parent constraint covers
-- them; no such FK exists today -- core.plays has none -- but the guard
-- keeps this re-runnable if that changes).
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-036. Idempotent (already-deferrable FKs are filtered
-- out, so a re-run is a no-op).
--
--   python scripts/run_migrations.py --file src/schemas/migrations/037_fk_deferrable_catchall.sql

DO $$
DECLARE
    fk record;
    n integer := 0;
BEGIN
    FOR fk IN
        SELECT con.conname,
               con.conrelid::regclass AS child_table,
               con.confrelid::regclass AS parent_table
        FROM pg_constraint con
        JOIN pg_class parent ON parent.oid = con.confrelid
        JOIN pg_namespace pn ON pn.oid = parent.relnamespace
        WHERE con.contype = 'f'
          AND NOT con.condeferrable
          AND con.conparentid = 0
          AND pn.nspname IN ('ref', 'core', 'stats', 'ratings', 'recruiting',
                             'betting', 'draft', 'metrics')
        ORDER BY con.conname
    LOOP
        EXECUTE format(
            'ALTER TABLE %s ALTER CONSTRAINT %I DEFERRABLE INITIALLY DEFERRED',
            fk.child_table, fk.conname
        );
        RAISE NOTICE 'made deferrable: % on % (references %)',
            fk.conname, fk.child_table, fk.parent_table;
        n := n + 1;
    END LOOP;
    RAISE NOTICE '037_fk_deferrable_catchall: % constraint(s) converted', n;
END $$;
