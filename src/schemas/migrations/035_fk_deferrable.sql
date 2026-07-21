-- Migration: 035_fk_deferrable
-- Supersedes: 032_fk_drives_cascade.sql (already applied to prod)
--
-- 032 recreated four core.games child FKs as ON DELETE CASCADE to fix a
-- daily-load failure where games' dlt merge DELETE was blocked by a
-- non-cascading FK from a sibling child table loaded in the same run. A
-- validation pass after 032 landed found a FIFTH constraint 032 missed:
--   psycopg2.errors.ForeignKeyViolation: update or delete on table "games"
--   violates foreign key constraint "fk_play_stats_game" on table "play_stats"
--   DETAIL:  Key (id)=(401752939) is still referenced from table "play_stats".
-- (run 29836367032 / job 88653633766, games step, 32.9s). fk_play_stats_game
-- is not defined in any tracked migration -- it was added as a Tier 1
-- one-off directly against prod -- but it exists live and blocks the same
-- class of update/delete as the other four.
--
-- CASCADE (032) is also the wrong design, independent of the missed fifth
-- constraint. It was justified on the assumption that every child table is
-- always reloaded in the same run as games (scripts/load_season.py's
-- SOURCE_ORDER). That assumption doesn't hold for partial loads: running
-- `python -m src.pipelines.run --source games --year 2025` alone, or any
-- --sources subset that includes games but omits stats/betting/metrics,
-- lets games' merge DELETE fire the cascade against drives/lines/
-- pregame_win_probability/game_havoc/play_stats with no reinsert to follow.
-- The cascade would then silently delete real child rows with no error and
-- no log line calling it out -- exactly the kind of data loss an FK is
-- supposed to prevent, not cause.
--
-- Root fix: DEFERRABLE INITIALLY DEFERRED, no CASCADE. dlt's "merge" write
-- disposition issues its DELETE (existing rows whose PK is in the load
-- package) and its INSERT (the fresh rows, same PK) as jobs within a single
-- destination transaction per load package -- see dlt's postgres job client,
-- which runs all jobs for a package under one connection/transaction and
-- COMMITs only at the end. A DEFERRABLE INITIALLY DEFERRED FK is not
-- rechecked after each statement; it's rechecked once at COMMIT. So within
-- that one transaction:
--   1. games DELETE removes id=401752939              (FK check deferred)
--   2. games INSERT reinserts id=401752939             (row exists again)
--   3. COMMIT -> deferred FK check runs, parent row is present -> passes
-- This holds regardless of which child tables are in the same run: a child
-- row referencing a game that legitimately still exists at COMMIT never
-- trips the check, and nothing ever gets cascade-deleted. If a games row is
-- genuinely removed for good (no reinsert before COMMIT), the deferred
-- check still fires at COMMIT and still blocks the delete -- integrity is
-- preserved, just checked at the right time instead of mid-transaction.
--
-- Net effect vs. 032: same "load doesn't spuriously fail" outcome for the
-- co-loaded case 032 was fixing, plus correctness for partial loads that
-- 032 silently broke. 032's four ON DELETE CASCADE constraints are live in
-- prod today; this migration drops and replaces all four (plus the fifth,
-- fk_play_stats_game, that 032 missed) with the DEFERRABLE form. No FK here
-- carries ON DELETE CASCADE.
--
-- Every REFERENCES core.games FK in the repo (grepped src/, plus
-- add_fk_constraints.sql, 012_foreign_keys.sql, add_unique_constraints.sql,
-- and 032 itself) plus the untracked prod-only fk_play_stats_game:
--   - core.drives.game_id                       fk_drives_game
--   - betting.lines.game_id                      fk_lines_game
--   - metrics.pregame_win_probability.game_id     fk_pregame_wp_game
--   - stats.game_havoc.game_id                    fk_game_havoc_game
--   - stats.play_stats.game_id                    fk_play_stats_game  (untracked; prod-only)
-- core.plays has no FK to core.games (partitioned tables can't carry one --
-- see 012's SKIPPED note); game_team_stats/game_player_stats have none
-- either (confirmed in 032's investigation) -- neither is in scope here.
--
-- Each DROP CONSTRAINT IF EXISTS lists every name variant seen across the
-- migration history so this is idempotent regardless of which one is live
-- in a given environment:
--   - <short name>        -- 032's CASCADE version / add_fk_constraints.sql
--   - <short name>_id      -- 012_foreign_keys.sql's original naming (drives/
--                              lines/pregame_wp only; game_havoc and
--                              play_stats were never named this way, but the
--                              drop is harmless if it never existed)
--
-- stats.play_stats is ~2.5M rows -- by far the largest of the five.
-- VALIDATE CONSTRAINT scans the child table checking each game_id against
-- core.games' PK index; it takes a SHARE UPDATE EXCLUSIVE lock (blocks other
-- DDL/VACUUM FULL on the table, does NOT block normal SELECT/INSERT/UPDATE/
-- DELETE) and does no table rewrite. Expect on the order of low single-digit
-- minutes on Supabase's small compute tiers for 2.5M rows; safe to run
-- during normal load activity. If orphans exist, VALIDATE fails outright
-- (it does not partially apply) -- run the commented-out check below first
-- if there's any doubt about play_stats.game_id integrity; check_presence.py
-- and prior ad hoc queries have not surfaced orphans, so none are expected
-- and no cleanup DELETE is included by default.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-034. Idempotent (IF EXISTS drops, NOT VALID + VALIDATE
-- add pattern).
--
--   python scripts/run_migrations.py --file src/schemas/migrations/035_fk_deferrable.sql

-- -----------------------------------------------------------------------------
-- Orphan check for stats.play_stats.game_id -- uncomment and run manually
-- first if there's any doubt about data integrity. VALIDATE CONSTRAINT below
-- will fail with a "is still referenced" / FK violation error if this
-- returns > 0; the fix would be a targeted cleanup DELETE (or a games
-- backfill) before re-running this migration, not a code change here.
-- -----------------------------------------------------------------------------
-- SELECT COUNT(*) AS orphan_play_stats_rows
-- FROM stats.play_stats ps
-- WHERE NOT EXISTS (
--     SELECT 1 FROM core.games g WHERE g.id = ps.game_id
-- );

-- core.drives.game_id -> core.games.id
ALTER TABLE core.drives DROP CONSTRAINT IF EXISTS fk_drives_game;
ALTER TABLE core.drives DROP CONSTRAINT IF EXISTS fk_drives_game_id;
ALTER TABLE core.drives
  ADD CONSTRAINT fk_drives_game
  FOREIGN KEY (game_id) REFERENCES core.games(id)
  DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE core.drives VALIDATE CONSTRAINT fk_drives_game;

-- betting.lines.game_id -> core.games.id
ALTER TABLE betting.lines DROP CONSTRAINT IF EXISTS fk_lines_game;
ALTER TABLE betting.lines DROP CONSTRAINT IF EXISTS fk_lines_game_id;
ALTER TABLE betting.lines
  ADD CONSTRAINT fk_lines_game
  FOREIGN KEY (game_id) REFERENCES core.games(id)
  DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE betting.lines VALIDATE CONSTRAINT fk_lines_game;

-- metrics.pregame_win_probability.game_id -> core.games.id
ALTER TABLE metrics.pregame_win_probability DROP CONSTRAINT IF EXISTS fk_pregame_wp_game;
ALTER TABLE metrics.pregame_win_probability DROP CONSTRAINT IF EXISTS fk_pregame_wp_game_id;
ALTER TABLE metrics.pregame_win_probability
  ADD CONSTRAINT fk_pregame_wp_game
  FOREIGN KEY (game_id) REFERENCES core.games(id)
  DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE metrics.pregame_win_probability VALIDATE CONSTRAINT fk_pregame_wp_game;

-- stats.game_havoc.game_id -> core.games.id
ALTER TABLE stats.game_havoc DROP CONSTRAINT IF EXISTS fk_game_havoc_game;
ALTER TABLE stats.game_havoc DROP CONSTRAINT IF EXISTS fk_game_havoc_game_id;
ALTER TABLE stats.game_havoc
  ADD CONSTRAINT fk_game_havoc_game
  FOREIGN KEY (game_id) REFERENCES core.games(id)
  DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE stats.game_havoc VALIDATE CONSTRAINT fk_game_havoc_game;

-- stats.play_stats.game_id -> core.games.id
-- (fk_play_stats_game is the untracked Tier 1 one-off live in prod today;
-- fk_play_stats_game_id is a defensive drop in case any environment named it
-- the 012-style way -- it was never added under that name in tracked SQL.)
ALTER TABLE stats.play_stats DROP CONSTRAINT IF EXISTS fk_play_stats_game;
ALTER TABLE stats.play_stats DROP CONSTRAINT IF EXISTS fk_play_stats_game_id;
ALTER TABLE stats.play_stats
  ADD CONSTRAINT fk_play_stats_game
  FOREIGN KEY (game_id) REFERENCES core.games(id)
  DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE stats.play_stats VALIDATE CONSTRAINT fk_play_stats_game;
