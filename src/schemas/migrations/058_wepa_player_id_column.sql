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
-- Idempotent: re-running is a no-op.

alter table metrics.wepa_players_passing add column if not exists id text;
update metrics.wepa_players_passing set id = athlete_id where id is null;
alter table metrics.wepa_players_passing alter column id set not null;

alter table metrics.wepa_players_rushing add column if not exists id text;
update metrics.wepa_players_rushing set id = athlete_id where id is null;
alter table metrics.wepa_players_rushing alter column id set not null;

alter table metrics.wepa_players_kicking add column if not exists id text;
update metrics.wepa_players_kicking set id = athlete_id where id is null;
alter table metrics.wepa_players_kicking alter column id set not null;
