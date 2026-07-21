-- Migration: 029_ingame_wp_indexes
--
-- Indexes for metrics.win_probability (in-game win probability by play),
-- loaded via src/pipelines/sources/metrics.py::win_probability_by_game_resource
-- and scripts/backfill_ingame_wp.py -- see docs/plans/2026-07-21-tier3-analytics-plan.md,
-- Pillar D. The old year-param loader (win_probability_resource) never
-- wrote a row (the endpoint is gameId-scoped, so every call 400'd), so this
-- table is dlt-created from scratch by the new resource: columns come
-- straight from the CFBD /metrics/wp response, snake_cased by dlt --
-- notably game_id, play_id (per-play string id, the merge/primary key),
-- home_win_probability, and play_number (the response's own per-game play
-- ordinal, distinct from core.plays.play_number).
--
-- Like 020_line_snapshot_indexes.sql, this migration must be applied AFTER
-- the first backfill chunk has run -- a fresh/empty database has no
-- metrics.win_probability table yet and this migration will fail against
-- it. Apply via:
--   python scripts/run_migrations.py --file src/schemas/migrations/029_ingame_wp_indexes.sql
--
-- Index rationale:
--   ix_win_probability_game_id: every downstream consumer (scripts/calibrate_live_wp.py's
--     sigma fit, ad hoc per-game timelines, the live-WP calibration report)
--     filters this per-play table down to a single game_id first -- that is
--     the primary access pattern for an otherwise game_id-scoped table.
--   ix_win_probability_game_play_number: a composite (game_id, play_number)
--     index supports "this game's plays in order" range scans (win-probability
--     chart reconstruction, calibration replay) without a sort step. Not a
--     BRIN index -- unlike betting.line_snapshots or live.scoreboard_snapshots,
--     this table is merge-loaded per game out of chronological/insertion
--     order (backfill chunks iterate games oldest-season-first, not by
--     capture time), so a BRIN index would not track physical row order.

CREATE INDEX IF NOT EXISTS ix_win_probability_game_id
  ON metrics.win_probability (game_id);

CREATE INDEX IF NOT EXISTS ix_win_probability_game_play_number
  ON metrics.win_probability (game_id, play_number);
