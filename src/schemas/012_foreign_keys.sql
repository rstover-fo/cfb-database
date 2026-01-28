-- Sprint 2B.2: Foreign key constraints
--
-- IMPORTANT: FKs are only added within non-reference schemas.
-- Reference tables (ref.*) use dlt `replace` disposition which drops and
-- recreates the table on each pipeline run — FKs referencing ref tables
-- would block those refreshes.
--
-- Strategy:
--   1. Add UNIQUE constraints on business keys for FK target tables
--   2. Add FK constraints from child tables to parent tables
--   All statements are idempotent.

-- =============================================================================
-- Step 1: Add unique constraints on business keys (FK targets)
-- dlt only creates unique indexes on _dlt_id, not on business keys
-- =============================================================================

-- core.games needs unique on id (bigint) for drives/plays/stats to reference
-- games.id is already NOT NULL per dlt schema
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'games_id_unique' AND conrelid = 'core.games'::regclass
    ) THEN
        ALTER TABLE core.games ADD CONSTRAINT games_id_unique UNIQUE (id);
    END IF;
END $$;

-- =============================================================================
-- Step 2: Foreign key constraints (core schema — internal relationships)
-- =============================================================================

-- core.drives.game_id -> core.games.id
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_drives_game_id' AND conrelid = 'core.drives'::regclass
    ) THEN
        ALTER TABLE core.drives
        ADD CONSTRAINT fk_drives_game_id
        FOREIGN KEY (game_id) REFERENCES core.games(id);
    END IF;
END $$;

-- SKIPPED: core.plays.game_id -> core.games.id
-- Plays table covers far more games than core.games (2.3M of 3.6M plays reference
-- game_ids not in core.games). The plays pipeline loaded 2004-2025 play-by-play
-- while games only covers a subset of seasons. FK would fail on 63% of rows.
-- Can be added after backfilling core.games with all historical seasons.

-- =============================================================================
-- Step 3: Foreign keys from stats/betting/metrics -> core.games
-- =============================================================================

-- betting.lines.game_id -> core.games.id
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_lines_game_id' AND conrelid = 'betting.lines'::regclass
    ) THEN
        ALTER TABLE betting.lines
        ADD CONSTRAINT fk_lines_game_id
        FOREIGN KEY (game_id) REFERENCES core.games(id);
    END IF;
END $$;

-- metrics.pregame_win_probability.game_id -> core.games.id
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_pregame_wp_game_id'
        AND conrelid = 'metrics.pregame_win_probability'::regclass
    ) THEN
        ALTER TABLE metrics.pregame_win_probability
        ADD CONSTRAINT fk_pregame_wp_game_id
        FOREIGN KEY (game_id) REFERENCES core.games(id);
    END IF;
END $$;

-- =============================================================================
-- Notes on skipped FKs:
--
-- SKIPPED: stats.* -> ref.teams (ref uses replace disposition)
-- SKIPPED: ratings.* -> ref.teams (ref uses replace disposition)
-- SKIPPED: recruiting.* -> ref.teams (ref uses replace disposition)
-- SKIPPED: core.games -> ref.venues (ref uses replace disposition)
-- SKIPPED: core.games -> ref.teams (ref uses replace disposition)
-- SKIPPED: core.plays -> core.games (plays covers more games than games table;
--          2.3M orphan rows — backfill games first)
-- SKIPPED: core.drives -> core.plays (plays has no unique on id yet for
--          partitioned table — would need per-partition unique constraint)
--
-- These can be added if/when ref tables are converted from `replace` to `merge`
-- and/or historical games are backfilled.
-- =============================================================================
