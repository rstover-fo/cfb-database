-- 058: Pre-add the id column dlt now expects on metrics.wepa_players_*.
--
-- CFBD renamed the player-id field on the /wepa/players/* endpoints before
-- these tables were ever first loaded, so they were created carrying
-- athlete_id (text, zero NULLs -- verified live 2026-08-30) and never had
-- the id column that wepa.py declares as primary key ["id", "year"].
-- _stamp_player_id (src/pipelines/sources/wepa.py) now stamps
-- id = str(<athlete id>) on every record, which made dlt try to
-- ADD COLUMN id NOT NULL onto the populated tables and fail with
-- SchemaUpdateTerminalError ("column id ... contains null values";
-- backfill runs 33337866929 / 33337870220).
--
-- Pre-adding the column and copying athlete_id keeps merge-key
-- continuity: re-ingested rows carry the same stamped ids, so the
-- corrected-data re-runs replace these rows season by season instead of
-- duplicating them.
--
-- Applied live 2026-08-30 (mcp migration wepa_player_id_column_backfill).
-- Idempotent: re-running is a no-op. Guarded per table (PR #81 review:
-- this file rides deploys/expansion_views-manifest.json so the unit is
-- self-contained): on a fresh database the wepa player tables do not
-- exist yet -- the first load creates them WITH id, so a missing table
-- is skipped with a NOTICE rather than failing the manifest apply.
-- A SET NOT NULL failure (athlete_id NULLs in some other environment)
-- stays loud by design.

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'wepa_players_passing',
        'wepa_players_rushing',
        'wepa_players_kicking'
    ] LOOP
        IF to_regclass(format('metrics.%I', t)) IS NULL THEN
            RAISE NOTICE '058: metrics.% does not exist -- skipping (a fresh load creates it with id already present)', t;
            CONTINUE;
        END IF;
        EXECUTE format('ALTER TABLE metrics.%I ADD COLUMN IF NOT EXISTS id text', t);
        EXECUTE format('UPDATE metrics.%I SET id = athlete_id WHERE id IS NULL', t);
        EXECUTE format('ALTER TABLE metrics.%I ALTER COLUMN id SET NOT NULL', t);
    END LOOP;
END $$;
