-- scouting schema: tables (codified from the live database, 2026-08-08)
-- =============================================================================
-- Adopted from the cub-scout repo merge (docs/plans/2026-08-08-cfb-scout-merge-plan.md).
-- These files CODIFY objects that already exist in production -- the live database
-- is the source of truth (cub-scout's src/storage/schema.sql was incomplete).
-- Applying this file against production must be a no-op.
--
-- Applied via: python scripts/run_migrations.py --file src/schemas/scouting/001_tables.sql
-- (not part of MIGRATION_ORDER).
--
-- Grants: the scouting schema intentionally has NO USAGE grant for anon/authenticated
-- (not PostgREST-reachable, not visible to analyst_ro). Do not add grants here; any
-- consumer surface goes through a new api.* view instead.
--
-- ID columns use serial (integer + sequence), matching the live objects created by
-- cub-scout -- not this repo's usual identity-column convention. Kept as-is so the
-- codification matches production exactly.

CREATE SCHEMA IF NOT EXISTS scouting;

-- pgvector lives in the public schema on this project (installed 2026-02 by cub-scout).
CREATE EXTENSION IF NOT EXISTS vector;

-- Scouting player profiles. roster_player_id links to core.roster.id (bigint),
-- recruit_id to recruiting.recruits.id -- soft links, no FK across schemas.
CREATE TABLE IF NOT EXISTS scouting.players (
    id serial PRIMARY KEY,
    roster_player_id bigint,
    recruit_id bigint,
    name text NOT NULL,
    position text,
    team text,
    class_year integer,
    current_status text CHECK (current_status = ANY (ARRAY['recruit', 'active', 'transfer', 'draft_eligible', 'drafted'])),
    composite_grade integer CHECK (composite_grade >= 0 AND composite_grade <= 100),
    traits jsonb DEFAULT '{}'::jsonb,
    draft_projection text,
    comps text[] DEFAULT '{}'::text[],
    last_updated timestamptz NOT NULL DEFAULT now(),
    UNIQUE (name, team, class_year)
);

CREATE INDEX IF NOT EXISTS idx_players_grade ON scouting.players (composite_grade DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_players_status ON scouting.players (current_status);
CREATE INDEX IF NOT EXISTS idx_players_team ON scouting.players (team);

-- Crawled scouting content (articles etc.), pre- and post-LLM processing.
CREATE TABLE IF NOT EXISTS scouting.reports (
    id serial PRIMARY KEY,
    source_url text NOT NULL UNIQUE,
    source_name text NOT NULL,
    published_at timestamptz,
    crawled_at timestamptz NOT NULL DEFAULT now(),
    content_type text NOT NULL CHECK (content_type = ANY (ARRAY['article', 'social', 'forum'])),
    player_ids bigint[] DEFAULT '{}'::bigint[],
    team_ids text[] DEFAULT '{}'::text[],
    raw_text text NOT NULL,
    summary text,
    sentiment_score numeric(3,2) CHECK (sentiment_score >= -1 AND sentiment_score <= 1),
    processed_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_reports_crawled ON scouting.reports (crawled_at DESC);
CREATE INDEX IF NOT EXISTS idx_reports_players ON scouting.reports USING gin (player_ids);
CREATE INDEX IF NOT EXISTS idx_reports_source ON scouting.reports (source_name);
CREATE INDEX IF NOT EXISTS idx_reports_teams ON scouting.reports USING gin (team_ids);
CREATE INDEX IF NOT EXISTS idx_reports_unprocessed ON scouting.reports (id) WHERE processed_at IS NULL;

-- OpenAI text-embedding-3-small identity embeddings keyed to core.roster.id
-- (stored as text; core.roster.id is integer -- casts live in calling code).
CREATE TABLE IF NOT EXISTS scouting.player_embeddings (
    id serial PRIMARY KEY,
    roster_id text NOT NULL,
    identity_text text NOT NULL,
    embedding vector(1536),
    created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_player_embeddings_hnsw ON scouting.player_embeddings USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_player_embeddings_roster ON scouting.player_embeddings (roster_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_player_embeddings_unique_roster ON scouting.player_embeddings (roster_id);

-- Low-confidence entity matches queued for human review.
CREATE TABLE IF NOT EXISTS scouting.pending_links (
    id serial PRIMARY KEY,
    source_name text NOT NULL,
    source_team text,
    source_context jsonb DEFAULT '{}'::jsonb,
    candidate_roster_id text,
    match_score double precision,
    match_method text CHECK (match_method = ANY (ARRAY['vector', 'fuzzy', 'deterministic'])),
    status text DEFAULT 'pending' CHECK (status = ANY (ARRAY['pending', 'approved', 'rejected'])),
    reviewed_at timestamptz,
    created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pending_links_created ON scouting.pending_links (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pending_links_status ON scouting.pending_links (status) WHERE status = 'pending';

-- Point-in-time player snapshots (sentiment/grade/traits history).
CREATE TABLE IF NOT EXISTS scouting.player_timeline (
    id serial PRIMARY KEY,
    player_id integer NOT NULL REFERENCES scouting.players (id) ON DELETE CASCADE,
    snapshot_date date NOT NULL,
    status text,
    sentiment_score numeric(3,2),
    grade_at_time integer,
    traits_at_time jsonb,
    key_narratives text[] DEFAULT '{}'::text[],
    sources_count integer DEFAULT 0,
    UNIQUE (player_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_timeline_date ON scouting.player_timeline (snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_timeline_player ON scouting.player_timeline (player_id);

-- User-defined alert rules.
CREATE TABLE IF NOT EXISTS scouting.alerts (
    id serial PRIMARY KEY,
    user_id text NOT NULL,
    name text NOT NULL,
    alert_type text NOT NULL CHECK (alert_type = ANY (ARRAY['grade_change', 'new_report', 'status_change', 'trend_change', 'portal_entry'])),
    player_id integer REFERENCES scouting.players (id) ON DELETE CASCADE,
    team text,
    threshold jsonb DEFAULT '{}'::jsonb,
    is_active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_checked_at timestamptz,
    UNIQUE (user_id, name)
);

CREATE INDEX IF NOT EXISTS idx_alerts_active ON scouting.alerts (is_active) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_alerts_player ON scouting.alerts (player_id);
CREATE INDEX IF NOT EXISTS idx_alerts_user ON scouting.alerts (user_id);

-- Fired-alert log.
CREATE TABLE IF NOT EXISTS scouting.alert_history (
    id serial PRIMARY KEY,
    alert_id integer NOT NULL REFERENCES scouting.alerts (id) ON DELETE CASCADE,
    fired_at timestamptz NOT NULL DEFAULT now(),
    trigger_data jsonb NOT NULL,
    message text NOT NULL,
    is_read boolean NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_alert_history_alert ON scouting.alert_history (alert_id);
CREATE INDEX IF NOT EXISTS idx_alert_history_unread ON scouting.alert_history (is_read) WHERE is_read = false;
-- idx_alert_history_alert_event (unique, per transfer event) is defined in
-- 004_portal_surveillance.sql next to the function whose idempotency it backs.

-- Named player watch lists.
CREATE TABLE IF NOT EXISTS scouting.watch_lists (
    id serial PRIMARY KEY,
    name text NOT NULL,
    user_id text NOT NULL,
    description text,
    player_ids integer[] DEFAULT '{}'::integer[],
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, name)
);

CREATE INDEX IF NOT EXISTS idx_watch_lists_user ON scouting.watch_lists (user_id);

-- PFF player grades by season/week.
CREATE TABLE IF NOT EXISTS scouting.pff_grades (
    id serial PRIMARY KEY,
    player_id integer REFERENCES scouting.players (id) ON DELETE CASCADE,
    pff_player_id text NOT NULL,
    season integer NOT NULL,
    week integer,
    overall_grade numeric(4,1) NOT NULL,
    position_grades jsonb DEFAULT '{}'::jsonb,
    snaps integer DEFAULT 0,
    fetched_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (player_id, season, week)
);

CREATE INDEX IF NOT EXISTS idx_pff_grades_player ON scouting.pff_grades (player_id);
CREATE INDEX IF NOT EXISTS idx_pff_grades_season ON scouting.pff_grades (season, week);

-- Transfer portal events per player.
CREATE TABLE IF NOT EXISTS scouting.transfer_events (
    id serial PRIMARY KEY,
    player_id integer REFERENCES scouting.players (id) ON DELETE CASCADE,
    event_type text NOT NULL CHECK (event_type = ANY (ARRAY['entered', 'committed', 'withdrawn'])),
    from_team text,
    to_team text,
    event_date date NOT NULL,
    source_url text,
    notes text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (player_id, event_type, event_date)
);

CREATE INDEX IF NOT EXISTS idx_transfer_events_date ON scouting.transfer_events (event_date DESC);
CREATE INDEX IF NOT EXISTS idx_transfer_events_player ON scouting.transfer_events (player_id);
CREATE INDEX IF NOT EXISTS idx_transfer_events_to_team ON scouting.transfer_events (to_team) WHERE to_team IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_transfer_events_type ON scouting.transfer_events (event_type);

-- Daily portal-wide aggregates.
CREATE TABLE IF NOT EXISTS scouting.portal_snapshots (
    id serial PRIMARY KEY,
    snapshot_date date NOT NULL UNIQUE,
    total_in_portal integer NOT NULL DEFAULT 0,
    by_position jsonb DEFAULT '{}'::jsonb,
    by_conference jsonb DEFAULT '{}'::jsonb,
    notable_entries text[] DEFAULT '{}'::text[],
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_portal_snapshots_date ON scouting.portal_snapshots (snapshot_date DESC);

-- Reserved by cub-scout, never used in practice (0 rows live; its own gap
-- analysis calls them dead). Kept because they exist in production; drop
-- candidates if the scout service is ever revived with a cleaner design.
CREATE TABLE IF NOT EXISTS scouting.team_rosters (
    id serial PRIMARY KEY,
    team text NOT NULL,
    season integer NOT NULL,
    position_groups jsonb DEFAULT '{}'::jsonb,
    overall_sentiment numeric(3,2),
    trajectory text CHECK (trajectory = ANY (ARRAY['improving', 'stable', 'declining'])),
    key_storylines text[] DEFAULT '{}'::text[],
    last_updated timestamptz NOT NULL DEFAULT now(),
    UNIQUE (team, season)
);

CREATE INDEX IF NOT EXISTS idx_team_rosters_season ON scouting.team_rosters (season DESC);
CREATE INDEX IF NOT EXISTS idx_team_rosters_team ON scouting.team_rosters (team);

CREATE TABLE IF NOT EXISTS scouting.crawl_jobs (
    id serial PRIMARY KEY,
    source_name text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    status text NOT NULL DEFAULT 'running' CHECK (status = ANY (ARRAY['running', 'completed', 'failed'])),
    records_crawled integer DEFAULT 0,
    records_new integer DEFAULT 0,
    error_message text
);

CREATE INDEX IF NOT EXISTS idx_crawl_jobs_source ON scouting.crawl_jobs (source_name, started_at DESC);
