-- Migration: 041_flat_files
--
-- Flat-file ingestion subsystem (docs/brainstorms/2026-07-23-warehouse-extension-data-sources.md):
-- load ledger + team-name crosswalk framework tables, plus target tables for the
-- first four sources: Massey composite ratings (weekly CSV snapshots), nflverse
-- combine/draft picks (annual parquet), SBR historical odds (one-time Excel
-- backfill), and raw conference availability-report PDFs (archive-only; parsing
-- deferred).
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy manifest),
-- like 019-040. Idempotent (IF NOT EXISTS / ON CONFLICT DO NOTHING throughout).
--
-- Note on dlt coexistence: massey/nflverse/sbr tables are merge targets for dlt
-- pipelines; dlt adds its _dlt_id/_dlt_load_id bookkeeping columns on first load
-- and may evolve extra columns (e.g. long-tail nflverse stat fields). Types below
-- match dlt's inference for the parsed values (bigint/double precision/text/date).

CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS raw;

-- ---------------------------------------------------------------------------
-- Framework tables
-- ---------------------------------------------------------------------------

-- Append-only record of every flat-file load attempt. The unique partial index
-- backs the hash-skip: a (source, sha256) pair with status='loaded' means the
-- exact file bytes were already ingested, so daily cron re-runs are no-ops.
CREATE TABLE IF NOT EXISTS meta.flat_file_loads (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source text NOT NULL,
    file_sha256 text NOT NULL,
    source_url text,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    row_count integer,
    status text NOT NULL CHECK (status IN ('loaded', 'skipped', 'failed')),
    error text
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_flat_file_loads_loaded
    ON meta.flat_file_loads (source, file_sha256)
    WHERE status = 'loaded';

CREATE INDEX IF NOT EXISTS idx_flat_file_loads_source_time
    ON meta.flat_file_loads (source, loaded_at DESC);

-- External-name -> CFBD full-name crosswalk. cfbd_name must match the exact
-- team strings used across core.games / ref.teams ("Ohio State"). Seeded by
-- scripts/seed_team_xwalk.py output after human review.
CREATE TABLE IF NOT EXISTS ref.team_name_xwalk (
    source text NOT NULL,
    source_name text NOT NULL,
    cfbd_name text NOT NULL,
    PRIMARY KEY (source, source_name)
);

-- ---------------------------------------------------------------------------
-- Massey composite (weekly snapshots; no retroactive archive exists upstream,
-- so history accrues from the first snapshot we take)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ratings.massey_composite (
    season bigint NOT NULL,
    snapshot_date date NOT NULL,
    team text NOT NULL,
    composite_rank bigint,
    rating_mean double precision,
    rating_median double precision,
    rating_stdev double precision,
    n_systems bigint,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, snapshot_date, team)
);

CREATE TABLE IF NOT EXISTS ratings.massey_system_ratings (
    season bigint NOT NULL,
    snapshot_date date NOT NULL,
    team text NOT NULL,
    system_code text NOT NULL,
    system_rank bigint,
    PRIMARY KEY (season, snapshot_date, team, system_code)
);

CREATE INDEX IF NOT EXISTS idx_massey_composite_team
    ON ratings.massey_composite (team, season);

-- ---------------------------------------------------------------------------
-- nflverse combine + draft picks (annual; new tables, CFBD's draft.* untouched)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS draft.combine (
    season bigint NOT NULL,
    player_name text NOT NULL,
    pos text NOT NULL,
    school text,
    pfr_id text,
    cfb_id text,
    draft_year bigint,
    draft_round bigint,
    draft_ovr bigint,
    draft_team text,
    ht double precision,
    wt double precision,
    forty double precision,
    bench double precision,
    vertical double precision,
    broad_jump double precision,
    cone double precision,
    shuttle double precision,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, player_name, pos)
);

CREATE TABLE IF NOT EXISTS draft.nflverse_draft_picks (
    season bigint NOT NULL,
    round bigint NOT NULL,
    pick bigint NOT NULL,
    team text,
    pfr_player_id text,
    gsis_id text,
    cfb_player_id text,
    pfr_player_name text,
    position text,
    category text,
    side text,
    college text,
    age double precision,
    hof boolean,
    to_year bigint,
    allpro bigint,
    probowls bigint,
    seasons_started bigint,
    w_av bigint,
    car_av bigint,
    dr_av bigint,
    games bigint,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, round, pick)
);

CREATE INDEX IF NOT EXISTS idx_nflverse_draft_picks_college
    ON draft.nflverse_draft_picks (college, season);

-- ---------------------------------------------------------------------------
-- SBR historical odds (one-time backfill, ~2007+; team names xwalk-resolved
-- with original spellings preserved)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS betting.sbr_historical (
    season bigint NOT NULL,
    game_date date NOT NULL,
    home_team text NOT NULL,
    away_team text NOT NULL,
    home_team_source text,
    away_team_source text,
    home_rot bigint,
    away_rot bigint,
    home_final bigint,
    away_final bigint,
    spread_open double precision,
    spread_close double precision,
    total_open double precision,
    total_close double precision,
    home_ml bigint,
    away_ml bigint,
    spread_2h double precision,
    total_2h double precision,
    neutral_site boolean,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, game_date, home_team, away_team)
);

CREATE INDEX IF NOT EXISTS idx_sbr_historical_teams
    ON betting.sbr_historical (home_team, season);

-- ---------------------------------------------------------------------------
-- Availability-report PDF archive (raw bytes; structured parsing is a
-- follow-up -- the archive itself is the time-sensitive asset)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.availability_reports (
    sha256 text PRIMARY KEY,
    conference text NOT NULL,
    source_url text NOT NULL,
    report_hint text,
    report_date date,
    pdf bytea NOT NULL,
    fetched_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_availability_reports_conf_date
    ON raw.availability_reports (conference, report_date);

-- ---------------------------------------------------------------------------
-- Grants (mirror grant_read_access_for_security_invoker.sql: read-only exposure
-- of data schemas; meta is operational bookkeeping but harmless to read.
-- raw.availability_reports contains PDF blobs -- exposed read-only like the rest)
-- ---------------------------------------------------------------------------

GRANT USAGE ON SCHEMA meta TO anon, authenticated;
GRANT USAGE ON SCHEMA raw TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA meta TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA meta FROM anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA raw FROM anon, authenticated;
GRANT SELECT ON ratings.massey_composite, ratings.massey_system_ratings TO anon, authenticated;
GRANT SELECT ON draft.combine, draft.nflverse_draft_picks TO anon, authenticated;
GRANT SELECT ON betting.sbr_historical TO anon, authenticated;
