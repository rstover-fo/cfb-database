-- Migration: 032_fk_drives_cascade
--
-- Fixes a reproducible daily-load failure (run 29827205023):
--   psycopg2.errors.ForeignKeyViolation: update or delete on table "games"
--   violates foreign key constraint "fk_drives_game" on table "drives"
--
-- Root cause: dlt's "merge" write disposition is delete+reinsert -- for each
-- resource, dlt deletes destination rows whose primary key appears in the
-- current load package, then inserts the fresh rows. core.games and
-- core.drives are sibling resources of the same cfbd_games dlt source
-- (src/pipelines/sources/games.py) loaded in a single pipeline run, and dlt
-- gives no ordering guarantee between resources within a run. When a game's
-- score/status is updated (any in-progress or recently completed game),
-- games' merge issues a DELETE on core.games for that id while core.drives
-- still holds (or has not yet had reinserted) rows referencing that game_id
-- -- the non-cascading FK added in add_fk_constraints.sql blocks the delete
-- outright and fails the whole load.
--
-- The same hazard applies to every other child table with a non-cascading
-- FK to core.games(id) that is itself loaded by a dlt merge resource in the
-- same daily run (scripts/load_season.py's SOURCE_ORDER runs games, stats,
-- betting, and metrics back to back for the same season every day). Grepping
-- add_fk_constraints.sql and 012_foreign_keys.sql for `REFERENCES core.games`
-- turns up four such FKs total:
--   - core.drives.game_id               (cfbd_games source    -- games.py)
--   - betting.lines.game_id             (cfbd_betting source  -- betting.py)
--   - metrics.pregame_win_probability.game_id (cfbd_metrics source -- metrics.py)
--   - stats.game_havoc.game_id          (cfbd_stats source    -- stats.py)
-- core.plays has no FK to core.games (partitioned tables can't carry one --
-- see 012's "SKIPPED" note) so it isn't affected here. game_team_stats and
-- game_player_stats (game_stats.py / cfbd_game_stats source) were checked
-- too -- no FK to core.games exists for either, so they aren't in scope.
--
-- Fix: recreate all four as ON DELETE CASCADE. This doesn't weaken
-- integrity in practice: every one of these child resources is re-merged
-- in the same daily run as games, so a cascade-deleted row is reinstated by
-- that resource's own insert step moments later -- the cascade only ever
-- removes a row for the instant between games' delete and the child
-- resource's own reinsert, instead of aborting the whole load.
--
-- 012_foreign_keys.sql originally added these same three (drives/lines/
-- pregame_win_probability) under `_id`-suffixed names
-- (fk_drives_game_id, etc.); add_fk_constraints.sql later re-added them
-- (plus game_havoc) under the shorter names that are live today (confirmed
-- by the error text above). Both DROP forms are included so this is
-- idempotent regardless of which names exist in a given environment.
--
-- Apply via:
--   python scripts/run_migrations.py --file src/schemas/migrations/032_fk_drives_cascade.sql

-- core.drives.game_id -> core.games.id
ALTER TABLE core.drives DROP CONSTRAINT IF EXISTS fk_drives_game;
ALTER TABLE core.drives DROP CONSTRAINT IF EXISTS fk_drives_game_id;
ALTER TABLE core.drives
  ADD CONSTRAINT fk_drives_game
  FOREIGN KEY (game_id) REFERENCES core.games(id) ON DELETE CASCADE NOT VALID;
ALTER TABLE core.drives VALIDATE CONSTRAINT fk_drives_game;

-- betting.lines.game_id -> core.games.id
ALTER TABLE betting.lines DROP CONSTRAINT IF EXISTS fk_lines_game;
ALTER TABLE betting.lines DROP CONSTRAINT IF EXISTS fk_lines_game_id;
ALTER TABLE betting.lines
  ADD CONSTRAINT fk_lines_game
  FOREIGN KEY (game_id) REFERENCES core.games(id) ON DELETE CASCADE NOT VALID;
ALTER TABLE betting.lines VALIDATE CONSTRAINT fk_lines_game;

-- metrics.pregame_win_probability.game_id -> core.games.id
ALTER TABLE metrics.pregame_win_probability DROP CONSTRAINT IF EXISTS fk_pregame_wp_game;
ALTER TABLE metrics.pregame_win_probability DROP CONSTRAINT IF EXISTS fk_pregame_wp_game_id;
ALTER TABLE metrics.pregame_win_probability
  ADD CONSTRAINT fk_pregame_wp_game
  FOREIGN KEY (game_id) REFERENCES core.games(id) ON DELETE CASCADE NOT VALID;
ALTER TABLE metrics.pregame_win_probability VALIDATE CONSTRAINT fk_pregame_wp_game;

-- stats.game_havoc.game_id -> core.games.id
ALTER TABLE stats.game_havoc DROP CONSTRAINT IF EXISTS fk_game_havoc_game;
ALTER TABLE stats.game_havoc
  ADD CONSTRAINT fk_game_havoc_game
  FOREIGN KEY (game_id) REFERENCES core.games(id) ON DELETE CASCADE NOT VALID;
ALTER TABLE stats.game_havoc VALIDATE CONSTRAINT fk_game_havoc_game;
