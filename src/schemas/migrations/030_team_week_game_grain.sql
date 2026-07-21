-- Re-key features.team_week to (game_id, team) -- one row per TEAM-GAME
-- =============================================================================
-- Tier 3 Pillar C fix (docs/plans/2026-07-21-tier3-analytics-plan.md).
--
-- Migration 028 keyed features.team_week UNIQUE (season, season_type, week,
-- team) on the design assumption that a team plays at most one game per
-- week. The first live build (deploy run 29798893973) falsified that in two
-- ways:
--
--   * CFP semifinal AND championship are BOTH season_type='postseason',
--     week=1 in CFBD -- e.g. (2015, postseason, 1, Alabama) occurs twice.
--   * Data quirks duplicate a regular week -- (2019, regular, 12, Campbell).
--
-- The natural grain is (game_id, team): the spine IS core.games team-sides,
-- so game_id is never NULL and uniquely identifies the row. The as-of
-- semantics are unchanged -- both rows of a CFP final week carry the same
-- week_index and therefore identical as-of features; they differ only in
-- game identity. Model joins (train_model.py / score_fitted.py) key on
-- (game_id, team) accordingly.
--
-- 028's CREATE TABLE/INDEX are IF NOT EXISTS, so on the already-provisioned
-- prod DB this migration performs the live re-key; 028 itself was also
-- updated for the fresh-DB path. Table is empty at this point (every
-- first-build season failed atomically on the old key), so the ALTER is
-- instant.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-029. Idempotent.

ALTER TABLE features.team_week ALTER COLUMN game_id SET NOT NULL;

DROP INDEX IF EXISTS features.team_week_key;

CREATE UNIQUE INDEX IF NOT EXISTS team_week_key
    ON features.team_week (game_id, team);

-- The old key's columns remain useful for lookups; keep a plain index on
-- the calendar grain in its place.
CREATE INDEX IF NOT EXISTS team_week_calendar_idx
    ON features.team_week (season, season_type, week, team);
