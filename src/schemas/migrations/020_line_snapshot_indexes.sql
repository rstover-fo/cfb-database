-- Migration: 020_line_snapshot_indexes
--
-- Indexes for betting.line_snapshots (append-only line-movement history,
-- see src/pipelines/sources/betting.py::line_snapshots_resource).
--
-- Apply AFTER the first snapshot load has run -- dlt creates the table on
-- first write, so this migration will fail against a fresh database with
-- no prior run. Apply via:
--   python scripts/run_migrations.py --file src/schemas/migrations/020_line_snapshot_indexes.sql

CREATE INDEX IF NOT EXISTS ix_line_snapshots_game_provider_captured
  ON betting.line_snapshots (game_id, provider, captured_at);

CREATE INDEX IF NOT EXISTS ix_line_snapshots_season_week
  ON betting.line_snapshots (season, week);

CREATE INDEX IF NOT EXISTS ix_line_snapshots_captured_at
  ON betting.line_snapshots (captured_at);
