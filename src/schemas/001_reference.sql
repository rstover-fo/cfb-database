-- Reference/dimension tables for college football data
-- Run this script first to create the ref schema and base tables

-- Create schema
CREATE SCHEMA IF NOT EXISTS ref;

-- =============================================================================
-- CONFERENCES
-- =============================================================================
CREATE TABLE IF NOT EXISTS ref.conferences (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cfbd_id         INTEGER UNIQUE,
    name            TEXT NOT NULL,
    short_name      TEXT,
    abbreviation    TEXT,
    classification  TEXT,  -- 'fbs', 'fcs', etc.
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_conferences_classification ON ref.conferences(classification);

COMMENT ON TABLE ref.conferences IS 'NCAA football conferences (FBS, FCS, etc.)';

-- =============================================================================
-- TEAMS
-- =============================================================================
CREATE TABLE IF NOT EXISTS ref.teams (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cfbd_id         INTEGER UNIQUE,
    school          TEXT NOT NULL,
    mascot          TEXT,
    abbreviation    TEXT,
    alt_name_1      TEXT,
    alt_name_2      TEXT,
    alt_name_3      TEXT,
    conference      TEXT,  -- Conference name (denormalized for convenience)
    classification  TEXT,  -- 'fbs', 'fcs', etc.
    color           TEXT,  -- Primary color (hex)
    alt_color       TEXT,  -- Secondary color (hex)
    -- Location
    city            TEXT,
    state           TEXT,
    -- Logos stored as array in JSONB
    logos           JSONB,
    -- Twitter handle
    twitter         TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_teams_school ON ref.teams(school);
CREATE INDEX IF NOT EXISTS idx_teams_abbreviation ON ref.teams(abbreviation);
CREATE INDEX IF NOT EXISTS idx_teams_conference ON ref.teams(conference);
CREATE INDEX IF NOT EXISTS idx_teams_classification ON ref.teams(classification);

COMMENT ON TABLE ref.teams IS 'NCAA football teams with location and branding info';

-- =============================================================================
-- VENUES
-- =============================================================================
CREATE TABLE IF NOT EXISTS ref.venues (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cfbd_id         INTEGER UNIQUE,
    name            TEXT NOT NULL,
    city            TEXT,
    state           TEXT,
    zip             TEXT,
    country_code    TEXT,
    timezone        TEXT,
    latitude        NUMERIC(10, 7),
    longitude       NUMERIC(10, 7),
    elevation       NUMERIC(10, 2),
    capacity        INTEGER,
    year_constructed INTEGER,
    grass           BOOLEAN,
    dome            BOOLEAN,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_venues_name ON ref.venues(name);
CREATE INDEX IF NOT EXISTS idx_venues_state ON ref.venues(state);
CREATE INDEX IF NOT EXISTS idx_venues_capacity ON ref.venues(capacity);

COMMENT ON TABLE ref.venues IS 'Football stadiums and venues';

-- =============================================================================
-- COACHES
-- =============================================================================
CREATE TABLE IF NOT EXISTS ref.coaches (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    hire_date       TEXT,  -- CFBD returns as string
    -- Seasons is a JSONB array of season records
    seasons         JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(first_name, last_name)
);

CREATE INDEX IF NOT EXISTS idx_coaches_last_name ON ref.coaches(last_name);
CREATE INDEX IF NOT EXISTS idx_coaches_seasons ON ref.coaches USING GIN (seasons);

COMMENT ON TABLE ref.coaches IS 'Head coaches with their season-by-season records';

-- =============================================================================
-- PLAY TYPES
-- =============================================================================
CREATE TABLE IF NOT EXISTS ref.play_types (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cfbd_id         INTEGER UNIQUE,
    text            TEXT NOT NULL,
    abbreviation    TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_play_types_text ON ref.play_types(text);

COMMENT ON TABLE ref.play_types IS 'Types of plays (rush, pass, penalty, etc.)';

-- =============================================================================
-- STAT TYPES (for reference)
-- =============================================================================
CREATE TABLE IF NOT EXISTS ref.stat_types (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cfbd_id         INTEGER UNIQUE,
    name            TEXT NOT NULL UNIQUE,
    category        TEXT,  -- 'passing', 'rushing', 'receiving', 'defense', etc.
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stat_types_category ON ref.stat_types(category);

COMMENT ON TABLE ref.stat_types IS 'Statistical categories for player/team stats';

-- =============================================================================
-- DIVISIONS (FBS/FCS subdivisions)
-- =============================================================================
CREATE TABLE IF NOT EXISTS ref.divisions (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    classification  TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE ref.divisions IS 'NCAA divisions (FBS, FCS)';

-- =============================================================================
-- UPDATED_AT TRIGGER FUNCTION
-- =============================================================================
CREATE OR REPLACE FUNCTION ref.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply triggers to all tables
DO $$
DECLARE
    t TEXT;
BEGIN
    FOR t IN SELECT table_name FROM information_schema.tables
             WHERE table_schema = 'ref' AND table_type = 'BASE TABLE'
    LOOP
        EXECUTE format('
            DROP TRIGGER IF EXISTS update_%I_updated_at ON ref.%I;
            CREATE TRIGGER update_%I_updated_at
            BEFORE UPDATE ON ref.%I
            FOR EACH ROW EXECUTE FUNCTION ref.update_updated_at_column();
        ', t, t, t, t);
    END LOOP;
END $$;
