-- Migration: 026_win_probability_indexes
--
-- Indexes for metrics.win_probability (in-game, per-play win probability;
-- see src/pipelines/sources/metrics.py::win_probability_resource,
-- docs/pipeline-manifest.md row 47).
--
-- Apply AFTER the first win-probability load has run -- dlt creates the
-- table on first write, so this migration will fail against a database
-- where metrics.win_probability doesn't exist yet (same precondition as
-- 020_line_snapshot_indexes.sql for betting.line_snapshots). The three
-- deploy-manifest backfill runs documented in deploys/p32-backfill-manifests.md
-- create the table; run this migration after the first of those completes.
--
-- Ordering column: CFBD's /metrics/wp response is per-play and includes
-- playId, but this repo has no confirmed ordering field (no "play_number"
-- equivalent in the endpoint per the 2026-01-29 investigation note) -- the
-- scripts/probe_metrics_wp.py probe (P3.2 W1) exists specifically to check
-- for one. Until that's confirmed, play_id itself is the best available
-- per-game ordering key: CFBD's play-by-play endpoints are consistently
-- chronological-by-id elsewhere in this warehouse (core.plays), so play_id
-- ascending within a game is the working assumption for
-- api.game_win_probability's ORDER BY. If the probe finds a better ordering
-- field (e.g. a distinct playNumber), add it here and to 033's view.
--
-- Apply via:
--   python scripts/run_migrations.py --file src/schemas/migrations/026_win_probability_indexes.sql

-- Doubles as the per-game ordering index for api.game_win_probability's
-- ORDER BY game_id, play_id (see note above) -- a separate plain index on
-- the same two columns would be redundant.
CREATE UNIQUE INDEX IF NOT EXISTS ux_win_probability_game_play
  ON metrics.win_probability (game_id, play_id);

CREATE INDEX IF NOT EXISTS ix_win_probability_season
  ON metrics.win_probability (season);
