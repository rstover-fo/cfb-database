-- =============================================================================
-- AUDIT TIMESTAMPS FOR TRANSACTIONAL TABLES
-- =============================================================================
-- Adds created_at/updated_at columns and auto-update triggers to core tables.
-- Reference tables already have this from 001_reference.sql (uses ref schema).
-- This migration targets core schema transactional tables.
--
-- NOTE: Does NOT include core.plays (partitioned table - triggers on partitioned
-- tables require triggers on each partition, which adds complexity for minimal
-- benefit on append-mostly data).
-- =============================================================================

-- =============================================================================
-- TRIGGER FUNCTION (core schema)
-- =============================================================================
-- Separate function in core schema to keep schemas self-contained.
-- The ref schema has its own version at ref.update_updated_at_column().

CREATE OR REPLACE FUNCTION core.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION core.update_updated_at_column() IS
    'Trigger function to auto-update updated_at timestamp on row modification';

-- =============================================================================
-- GAMES TABLE
-- =============================================================================
-- Primary transactional table for game records.
-- Games may be updated when scores are finalized or corrections are made.

ALTER TABLE core.games
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();

ALTER TABLE core.games
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

-- Drop existing trigger if present (for idempotency)
DROP TRIGGER IF EXISTS update_games_updated_at ON core.games;

CREATE TRIGGER update_games_updated_at
    BEFORE UPDATE ON core.games
    FOR EACH ROW
    EXECUTE FUNCTION core.update_updated_at_column();

COMMENT ON COLUMN core.games.created_at IS 'Timestamp when record was first inserted';
COMMENT ON COLUMN core.games.updated_at IS 'Timestamp when record was last modified (auto-updated by trigger)';

-- =============================================================================
-- DRIVES TABLE
-- =============================================================================
-- Drive-level data linked to games.
-- Drives may be updated if play-by-play corrections affect drive summaries.

ALTER TABLE core.drives
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();

ALTER TABLE core.drives
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

-- Drop existing trigger if present (for idempotency)
DROP TRIGGER IF EXISTS update_drives_updated_at ON core.drives;

CREATE TRIGGER update_drives_updated_at
    BEFORE UPDATE ON core.drives
    FOR EACH ROW
    EXECUTE FUNCTION core.update_updated_at_column();

COMMENT ON COLUMN core.drives.created_at IS 'Timestamp when record was first inserted';
COMMENT ON COLUMN core.drives.updated_at IS 'Timestamp when record was last modified (auto-updated by trigger)';
