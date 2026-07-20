-- Prep for (re)building marts.player_game_epa
-- 1. Terminate orphaned backends from killed CI deploy runs. A workflow-level
--    cancel kills the psycopg2 client, but a long CREATE MATERIALIZED VIEW
--    only notices client death when it writes to the socket -- so the backend
--    keeps running for hours holding the ACCESS EXCLUSIVE lock from the DROP,
--    and every subsequent rebuild attempt blocks behind it.
-- 2. ANALYZE the two large join inputs so the planner has real statistics
--    (stats.play_stats was bulk-loaded and may never have been analyzed).

DO $$
DECLARE
    stale RECORD;
BEGIN
    FOR stale IN
        SELECT pid
        FROM pg_stat_activity
        WHERE pid <> pg_backend_pid()
          AND state IS NOT NULL
          AND query ILIKE '%marts.player_game_epa%'
    LOOP
        PERFORM pg_terminate_backend(stale.pid);
        RAISE NOTICE 'terminated stale backend %', stale.pid;
    END LOOP;
END $$;

ANALYZE stats.play_stats;
ANALYZE marts.play_epa;
