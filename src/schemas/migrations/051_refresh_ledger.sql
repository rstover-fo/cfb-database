-- Migration: 051_refresh_ledger
--
-- Historical-refresh campaign ledger (unit A3, 2026-08-29). CFBD corrected
-- historical data upstream (15k+ garbage-time reclassifications and other
-- cleanups). The per-game endpoints behind stats.py's play_stats_resource
-- (/plays/stats) and advanced_game_stats_resource (/game/box/advanced) must
-- be re-fetched for ~2014-2025 completed games: ~1,600 games/season x 12
-- seasons x up to 2 tasks is up to ~38k calls, which cannot be spent in one
-- run against the 125k/month budget the daily load also consumes. This
-- ledger lets scripts/backfill_refresh.py spread that fan-out across many
-- budget-capped runs and resume exactly where an interrupted run left off --
-- (campaign, task, game_id) is the unit of "already re-fetched", so a
-- restarted run never re-spends a call on a game it already refreshed.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-050. Idempotent (IF NOT EXISTS throughout; GRANT/
-- REVOKE are naturally re-appliable).
--
-- Apply via:
--   python scripts/run_migrations.py --file src/schemas/migrations/051_refresh_ledger.sql

CREATE SCHEMA IF NOT EXISTS meta;

-- One row per refresh campaign (e.g. "2026-08-upstream-corrections"). seasons
-- and tasks are the campaign's declared scope, set once at --create time and
-- read back by every later run so an operator never has to repeat them.
-- completed_at is set (once, guarded by IS NULL in the UPDATE) the first time
-- every named task's backlog drains to empty across all declared seasons.
CREATE TABLE IF NOT EXISTS meta.refresh_campaigns (
    campaign text PRIMARY KEY,
    description text,
    seasons int[] NOT NULL,
    tasks text[] NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);

-- One row per (campaign, task, game_id) already re-fetched. This is the
-- resumability primitive: backlog = completed games in the campaign's
-- seasons MINUS the game_ids already recorded here for that (campaign,
-- task) -- the same set-difference shape as
-- src/pipelines/run.py's run_metrics_wp_pipeline uses against
-- metrics.win_probability, generalized to more than one task and scoped to
-- a named campaign so two campaigns over the same seasons don't collide.
CREATE TABLE IF NOT EXISTS meta.refresh_progress (
    campaign text NOT NULL REFERENCES meta.refresh_campaigns (campaign),
    task text NOT NULL,
    game_id bigint NOT NULL,
    calls int NOT NULL DEFAULT 1,
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (campaign, task, game_id)
);

-- Backs "backlog remaining for campaign X" lookups (scripts/backfill_refresh.py
-- --status and the per-run backlog query).
CREATE INDEX IF NOT EXISTS idx_refresh_progress_campaign_time
    ON meta.refresh_progress (campaign, refreshed_at);

-- Backs the month guard: SELECT SUM(calls) FROM meta.refresh_progress WHERE
-- refreshed_at >= date_trunc('month', now()) scans across ALL campaigns, not
-- just one, so it needs its own index rather than relying on the composite
-- above.
CREATE INDEX IF NOT EXISTS idx_refresh_progress_refreshed_at
    ON meta.refresh_progress (refreshed_at);

-- ---------------------------------------------------------------------------
-- Grants -- mirror 041_flat_files.sql's meta.flat_file_loads posture exactly:
-- read-only exposure for anon/authenticated. The pipeline/script itself
-- writes via the direct (service-role) connection, never through PostgREST,
-- so DML is revoked from both roles. 041's `GRANT SELECT ON ALL TABLES IN
-- SCHEMA meta` only covered tables that existed at the time it ran --
-- Postgres does not retroactively grant on tables created afterward without
-- ALTER DEFAULT PRIVILEGES -- so these two tables need their own explicit
-- grants (same reasoning as 050_expansion_grants_indexes.sql's per-table
-- grants for its new tables).
-- ---------------------------------------------------------------------------

GRANT USAGE ON SCHEMA meta TO anon, authenticated;
GRANT SELECT ON meta.refresh_campaigns, meta.refresh_progress TO anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON meta.refresh_campaigns, meta.refresh_progress
    FROM anon, authenticated;
