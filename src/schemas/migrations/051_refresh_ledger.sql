-- Migration: 051_refresh_ledger
--
-- Historical-refresh campaign ledger (unit A3, 2026-08-29; amended unit R1,
-- 2026-08-30 for PR #75 findings F6/F7). CFBD corrected historical data
-- upstream (15k+ garbage-time reclassifications and other cleanups). The
-- per-game endpoints behind stats.py's play_stats_resource (/plays/stats)
-- and advanced_game_stats_resource (/game/box/advanced) must be re-fetched
-- for ~2014-2025 completed games: ~1,600 games/season x 12 seasons x up to
-- 2 tasks is up to ~38k calls, which cannot be spent in one run against the
-- 125k/month budget the daily load also consumes. This ledger lets
-- scripts/backfill_refresh.py spread that fan-out across many budget-capped
-- runs and resume exactly where an interrupted run left off -- (campaign,
-- task, game_id) is the unit of "already re-fetched", so a restarted run
-- never re-spends a call on a game it already refreshed.
--
-- R1 additions:
--   meta.refresh_campaigns.last_finalized_at -- watermark = MAX(refresh_
--   progress.refreshed_at) covered by the last successful finalize (the
--   adjusted-EPA refit + mart refresh that runs once a campaign's per-game
--   backlog drains). NULL = never finalized. Read-compare against
--   refresh_progress.refreshed_at so a finalize is re-run only over rows
--   that landed since the last one succeeded (F7: a finalize failure must
--   not be silently forgotten, and must not be re-run over the same rows
--   twice).
--
--   meta.refresh_progress.status -- 'refreshed' (a call returned data) vs
--   'no_data' (the call was spent -- and is charged against the month guard
--   like any other call -- but the endpoint had nothing for that game, e.g.
--   a suppressed 400 or a legitimate empty 200). Both statuses are excluded
--   from a task's backlog by the (campaign, task, game_id) primary key
--   alone; 'no_data' exists so F6's failure mode (a suppressed per-game 400
--   silently marked "refreshed" forever, permanently hiding a real miss from
--   the backlog) is instead recorded honestly and can be requeued via
--   `--requeue-no-data`.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-050. Idempotent (IF NOT EXISTS throughout; GRANT/
-- REVOKE are naturally re-appliable). The ALTER TABLE ... ADD COLUMN IF NOT
-- EXISTS statements near the bottom are belt-and-braces for the case where
-- 051 was already applied (unit A3, pre-R1) before this amendment landed.
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
    completed_at timestamptz,
    -- Watermark = MAX(refresh_progress.refreshed_at) covered by the last
    -- successful finalize (adjusted-EPA refit + mart refresh). NULL = never
    -- finalized. Set to the watermark value itself, not now(), so a
    -- finalize is race-free against progress rows written after the
    -- finalize query ran but before its UPDATE committed.
    last_finalized_at timestamptz
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
    -- 'refreshed': the call returned data. 'no_data': the call was spent
    -- (still charged -- a 400 or an empty 200 costs quota like any other
    -- call) but the endpoint had nothing for this game. Both statuses are
    -- excluded from the backlog by the primary key alone; the distinction
    -- exists so a suppressed-400 miss (F6) is recorded honestly instead of
    -- silently counted as a genuine refresh, and can be requeued via
    -- `backfill_refresh.py --requeue-no-data`.
    status text NOT NULL DEFAULT 'refreshed' CHECK (status IN ('refreshed', 'no_data')),
    PRIMARY KEY (campaign, task, game_id)
);

-- Belt-and-braces: if 051 already applied (unit A3, pre-R1) before this
-- amendment landed, CREATE TABLE IF NOT EXISTS above is a no-op and these
-- columns would otherwise never appear. Also a no-op on a fresh apply,
-- where the CREATE TABLE statements above already declare both columns.
ALTER TABLE meta.refresh_campaigns
    ADD COLUMN IF NOT EXISTS last_finalized_at timestamptz;

ALTER TABLE meta.refresh_progress
    ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'refreshed'
        CHECK (status IN ('refreshed', 'no_data'));

-- No (campaign, refreshed_at) secondary index: every campaign-scoped lookup
-- the script issues (_DONE_IDS_QUERY, --status) filters on campaign+task, a
-- leftmost prefix of the PRIMARY KEY above, so a second composite index
-- would only tax every ledger insert (schema review, 2026-08-29).

-- Backs the month guard: SELECT SUM(calls) FROM meta.refresh_progress WHERE
-- refreshed_at >= date_trunc('month', now()) scans across ALL campaigns, not
-- just one, so it needs its own index rather than relying on the PK.
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
