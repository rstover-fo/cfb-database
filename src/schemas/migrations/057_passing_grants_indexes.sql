-- Migration: 057_passing_grants_indexes
--
-- Grants + indexes for the /passing charting unit (spec v5.25.0,
-- 2026-08-30): passing.py's five resources -- passing_plays_resource,
-- passing_player_games_resource, passing_team_games_resource,
-- passing_player_season_resource, passing_team_season_resource -- writing
-- stats.passing_plays, stats.passing_player_games, stats.passing_team_games,
-- stats.passing_player_season, stats.passing_team_season.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-056. Idempotent (GRANT is naturally re-appliable;
-- indexes use IF NOT EXISTS).
--
-- APPLY AFTER THE FIRST SUCCESSFUL LOAD of the passing source (same
-- precondition as 026_win_probability_indexes.sql, 050_expansion_grants_indexes.sql,
-- and 054_fanout_grants_indexes.sql) -- dlt creates each table on first
-- write, so the plain GRANT/CREATE INDEX statements below will fail with
-- "relation does not exist" against a database where these tables haven't
-- been created yet. `passing` is wired into scripts/load_season.py's
-- SOURCE_ORDER (run.py::run_passing_pipeline), so the next daily/backfill
-- run against a season >= PASSING_DATA_START (2025) creates them.
--
-- `stats` already has schema USAGE granted to anon/authenticated
-- (grant_read_access_for_security_invoker.sql), so this migration only
-- needs table-level GRANTs, same as 054.
--
-- Column names below are this unit's best expectation from the CFBD
-- OpenAPI spec (spec v5.25.0) and the 2026-08-30 probe fixtures
-- (tests/fixtures/cfbd_2026/passing_*.json), NOT a live-database read --
-- dlt snake_cases camelCase fields and flattens nested dicts with "__"
-- (e.g. `offense.totalAirYards` -> `offense__total_air_yards`). Verify
-- actual column names via pg_attribute before applying, and adjust below
-- if the live schema differs from what's assumed here.
--
-- The 36 charting-aggregate COMMENT ON COLUMN statements (four metrics plus
-- two coverage denominators, per grain-side) run through a guarded DO block
-- rather than as plain statements (Codex review, PR #79): dlt omits a
-- column from a table's schema entirely when every value loaded for it so
-- far was NULL, so a fresh database whose first passing load happens to
-- land on an all-uncharted week could genuinely be missing one of these
-- columns on create. run_migrations.py --file executes a whole file as one
-- implicit transaction (single cursor.execute + commit/rollback), so an
-- unguarded COMMENT naming a column that doesn't exist would error the
-- whole file and roll back the grants/indexes above too -- the opposite of
-- "deliberately re-appliable". Did NOT fire live (deploy run 188 applied
-- green post-backfill; every column existed), but the guard removes the
-- dependency on having gotten lucky. See that block below. The remaining
-- COMMENT ON COLUMN statements (join-spine columns, parse_status,
-- offense_id/defense_id) stay unguarded, plain statements: they always
-- carry a value on every row CFBD returns, so they always materialize on a
-- table's first load -- for those, this header's pg_attribute check is
-- still the only defense against a wrong name guess.
--
-- Apply via:
--   python scripts/run_migrations.py --file src/schemas/migrations/057_passing_grants_indexes.sql

-- ---------------------------------------------------------------------------
-- Grants -- root (always-created) tables
-- ---------------------------------------------------------------------------

GRANT SELECT ON
    stats.passing_plays,
    stats.passing_player_games,
    stats.passing_team_games,
    stats.passing_player_season,
    stats.passing_team_season
    TO anon, authenticated;

REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON
    stats.passing_plays,
    stats.passing_player_games,
    stats.passing_team_games,
    stats.passing_player_season,
    stats.passing_team_season
    FROM anon, authenticated;

-- ---------------------------------------------------------------------------
-- Grants -- dlt child tables (nested arrays only; nested dicts flatten into
-- top-level "__" columns on the parent and produce no child table).
--
-- None of the five probe fixtures show an array field: passing_plays'
-- only nested structure is `clock` (a flat dict: minutes, seconds);
-- passing_team_games/passing_team_season's `offense`/`defense` are flat
-- 13-key metric dicts. No child table is expected for any of the five, but
-- this is included defensively (mirroring 050/054's inclusion of
-- never-fully-verified parents) in case a field absent from the 5-record
-- probe samples turns out to be an array on a wider pull -- a wrong guess
-- here fails safe (zero grants) rather than erroring the whole migration on
-- a table name that doesn't exist.
-- ---------------------------------------------------------------------------

DO $$
DECLARE
    parents text[] := ARRAY[
        'stats.passing_plays',
        'stats.passing_player_games',
        'stats.passing_team_games',
        'stats.passing_player_season',
        'stats.passing_team_season'
    ];
    p text;
    p_schema text;
    p_table text;
    child record;
    n integer := 0;
BEGIN
    FOREACH p IN ARRAY parents LOOP
        p_schema := split_part(p, '.', 1);
        p_table := split_part(p, '.', 2);

        FOR child IN
            SELECT DISTINCT t.table_schema, t.table_name
            FROM information_schema.tables t
            WHERE t.table_schema = p_schema
              AND t.table_type = 'BASE TABLE'
              AND starts_with(t.table_name, p_table || '__')
        LOOP
            EXECUTE format(
                'GRANT SELECT ON %I.%I TO anon, authenticated',
                child.table_schema, child.table_name
            );
            EXECUTE format(
                'REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON %I.%I FROM anon, authenticated',
                child.table_schema, child.table_name
            );
            n := n + 1;
        END LOOP;
    END LOOP;
    RAISE NOTICE '057_passing_grants_indexes: % dlt child table(s) granted SELECT', n;
END $$;

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

-- passing_plays' PK is (game_id, play_id) -- game_id is already the leading
-- PK column, but an explicit index documents the lookup path the same way
-- 050's idx_game_advanced_team_stats_game_id does for a PK-leading column.
CREATE INDEX IF NOT EXISTS idx_passing_plays_game_id
    ON stats.passing_plays (game_id);

-- The novel join surface this unit adds: passer/target (receiver) grain,
-- neither of which is the PK's leading column.
CREATE INDEX IF NOT EXISTS idx_passing_plays_passer_id
    ON stats.passing_plays (passer_id);

CREATE INDEX IF NOT EXISTS idx_passing_plays_target_id
    ON stats.passing_plays (target_id);

CREATE INDEX IF NOT EXISTS idx_passing_player_games_player_id
    ON stats.passing_player_games (player_id);

CREATE INDEX IF NOT EXISTS idx_passing_player_season_player_id_season
    ON stats.passing_player_season (player_id, season);

CREATE INDEX IF NOT EXISTS idx_passing_team_games_game_id
    ON stats.passing_team_games (game_id);

-- No index on `team` on either team-grain table, unlike player_id above on
-- the player-grain tables: `team` is ~136-value low-cardinality (roughly
-- FBS+), so a btree on it does little for selectivity and isn't worth the
-- write overhead -- deliberate, not an oversight.

-- ---------------------------------------------------------------------------
-- Column comments -- player-grain join spine, parse_status, and preferred
-- join columns. Left as plain, unguarded statements (unlike the
-- charting-aggregate family below): these columns always carry a value on
-- every row CFBD returns, so they always materialize on a table's first
-- load regardless of what has or hasn't been charted yet.
-- ---------------------------------------------------------------------------

COMMENT ON COLUMN stats.passing_player_games.player_id IS
    'CFBD athlete id (string) -- verified property of this warehouse: CFBD athlete ids ARE ESPN ids. Player-grain join spine alongside season/team.';
COMMENT ON COLUMN stats.passing_player_games.season IS
    'Season year. Player-grain join spine alongside player_id/team.';
COMMENT ON COLUMN stats.passing_player_games.team IS
    'Team full name for this game. Player-grain join spine alongside player_id/season.';

COMMENT ON COLUMN stats.passing_player_season.player_id IS
    'CFBD athlete id (string) -- verified property of this warehouse: CFBD athlete ids ARE ESPN ids. Player-grain join spine alongside season/team.';
COMMENT ON COLUMN stats.passing_player_season.season IS
    'Season year. Player-grain join spine alongside player_id/team.';
COMMENT ON COLUMN stats.passing_player_season.team IS
    'Team full name at time of record. Player-grain join spine alongside player_id/season -- included in the PK because a transferred player can appear under more than one team in a season (same reasoning as stats.player_success_season).';

COMMENT ON COLUMN stats.passing_plays.passer_id IS
    'CFBD athlete id (string) -- verified property of this warehouse: CFBD athlete ids ARE ESPN ids. The passing side of the play-grain join spine alongside target_id.';
COMMENT ON COLUMN stats.passing_plays.target_id IS
    'CFBD athlete id (string) -- verified property of this warehouse: CFBD athlete ids ARE ESPN ids. The receiver (target) side of the play-grain join spine alongside passer_id -- the novel join surface this unit adds.';

-- parseStatus (dlt: parse_status) -- CFBD's charting-completeness marker.
-- Only "partial" was observed in probing; other values are unconfirmed but
-- read as more/fully charted.
COMMENT ON COLUMN stats.passing_plays.parse_status IS
    'CFBD charting-completeness marker for this play. "partial" (the only value observed in probing) means this play''s air yards/pass depth/direction/location/YAC charting is incomplete, and those nullable columns may read NULL as a result.';

-- Preferred join columns to ref.teams(id): numeric CFBD/ESPN team ids,
-- unlike offense/defense (name strings). ref.teams has 35 legitimate
-- duplicate school names, so joining on the name string requires
-- DISTINCT ON (school) or accepting fanout -- offense_id/defense_id avoid
-- that entirely. No new index: ref.teams(id) is already the primary key.
COMMENT ON COLUMN stats.passing_plays.offense_id IS
    'CFBD/ESPN numeric team id for the offense on this play. Prefer this over the `offense` name string when joining to ref.teams(id) -- ref.teams has 35 legitimate duplicate school names, so a name-string join needs DISTINCT ON (school) or accepts fanout; offense_id avoids that.';
COMMENT ON COLUMN stats.passing_plays.defense_id IS
    'CFBD/ESPN numeric team id for the defense on this play. Prefer this over the `defense` name string when joining to ref.teams(id) -- ref.teams has 35 legitimate duplicate school names, so a name-string join needs DISTINCT ON (school) or accepts fanout; defense_id avoids that.';

-- ---------------------------------------------------------------------------
-- Column comments -- charting-aggregate family (GUARDED). 36 statements:
-- four metrics (total_air_yards, average_depth_of_target,
-- total_yards_after_catch, average_yards_after_catch) plus their two
-- coverage denominators (air_yards_attempts_available,
-- yards_after_catch_attempts_available), per grain-side -- 6 on
-- passing_player_games + 6 on passing_player_season + 12 on
-- passing_team_games (offense__/defense__ x 6) + 12 on passing_team_season
-- (offense__/defense__ x 6).
--
-- Why guarded (see header note): dlt omits a column from a table's schema
-- entirely when every value loaded for it so far was NULL. The two
-- denominator columns are plain integers that are 0 (not NULL) when
-- nothing is charted yet, so they always materialize -- only the four
-- metric columns are actually at risk on a fresh database's first,
-- all-uncharted load. They are guarded together anyway: a guarded comment
-- on a column that already exists behaves identically to the unguarded
-- form, so treating all 36 uniformly is simpler than special-casing which
-- six of the thirty-six are merely defensive.
--
-- Convention: NULL means the play(s) behind this value were not charted;
-- 0 is a real observed value; the *_attempts_available column is the
-- charting-coverage denominator (mirrors migrations 046/047's NULL-vs-0
-- phrasing). `defense__*` is this team's passing DEFENSE (what the
-- opponent's passing offense did against them), not the opponent's own
-- offensive row.
--
-- Comment text below is byte-identical to what was live in the database
-- before this guard was added (Codex review, PR #79) -- only the
-- structure moved, not the wording.
-- ---------------------------------------------------------------------------

DO $comment$
DECLARE
    rec record;
    n_commented integer := 0;
    n_skipped integer := 0;
BEGIN
    FOR rec IN
        SELECT * FROM (VALUES
            ('stats', 'passing_player_games', 'total_air_yards',
             'Sum of charted air yards across this player''s pass attempts in this game. NULL means none of the attempts have been charted for air yards yet; 0 is a real observed value. air_yards_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_player_games', 'average_depth_of_target',
             'Average charted air yards per attempt (aDOT) for this player in this game. NULL means none of the attempts have been charted for air yards yet. air_yards_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_player_games', 'air_yards_attempts_available',
             'Count of this player''s attempts in this game that have been charted for air yards -- the charting-coverage denominator for total_air_yards/average_depth_of_target. Never NULL; 0 means none of this game''s attempts are charted yet, in which case both aggregates read NULL.'),
            ('stats', 'passing_player_games', 'total_yards_after_catch',
             'Sum of charted yards after the catch across this player''s completions in this game. NULL means none of the attempts have been charted for YAC yet; 0 is a real observed value. yards_after_catch_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_player_games', 'average_yards_after_catch',
             'Average charted yards after the catch per attempt for this player in this game. NULL means none of the attempts have been charted for YAC yet. yards_after_catch_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_player_games', 'yards_after_catch_attempts_available',
             'Count of this player''s attempts in this game that have been charted for yards after catch -- the charting-coverage denominator for total_yards_after_catch/average_yards_after_catch. Never NULL; 0 means none of this game''s attempts are charted yet, in which case both aggregates read NULL.'),

            ('stats', 'passing_player_season', 'total_air_yards',
             'Sum of charted air yards across this player''s pass attempts this season. NULL means none of the attempts have been charted for air yards yet; 0 is a real observed value. air_yards_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_player_season', 'average_depth_of_target',
             'Average charted air yards per attempt (aDOT) for this player this season. NULL means none of the attempts have been charted for air yards yet. air_yards_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_player_season', 'air_yards_attempts_available',
             'Count of this player''s attempts this season that have been charted for air yards -- the charting-coverage denominator for total_air_yards/average_depth_of_target. Never NULL; 0 means none of this season''s attempts are charted yet, in which case both aggregates read NULL.'),
            ('stats', 'passing_player_season', 'total_yards_after_catch',
             'Sum of charted yards after the catch across this player''s completions this season. NULL means none of the attempts have been charted for YAC yet; 0 is a real observed value. yards_after_catch_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_player_season', 'average_yards_after_catch',
             'Average charted yards after the catch per attempt for this player this season. NULL means none of the attempts have been charted for YAC yet. yards_after_catch_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_player_season', 'yards_after_catch_attempts_available',
             'Count of this player''s attempts this season that have been charted for yards after catch -- the charting-coverage denominator for total_yards_after_catch/average_yards_after_catch. Never NULL; 0 means none of this season''s attempts are charted yet, in which case both aggregates read NULL.'),

            ('stats', 'passing_team_games', 'offense__total_air_yards',
             'Sum of charted air yards across this team''s passing offense attempts in this game. NULL means none of the attempts have been charted for air yards yet; 0 is a real observed value. offense__air_yards_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_games', 'offense__average_depth_of_target',
             'Average charted air yards per attempt (aDOT) for this team''s passing offense in this game. NULL means none of the attempts have been charted for air yards yet. offense__air_yards_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_games', 'offense__air_yards_attempts_available',
             'Count of this team''s offensive attempts in this game that have been charted for air yards -- the charting-coverage denominator for offense__total_air_yards/offense__average_depth_of_target. Never NULL; 0 means none of this game''s offensive attempts are charted yet, in which case both aggregates read NULL.'),
            ('stats', 'passing_team_games', 'offense__total_yards_after_catch',
             'Sum of charted yards after the catch across this team''s passing offense completions in this game. NULL means none of the attempts have been charted for YAC yet; 0 is a real observed value. offense__yards_after_catch_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_games', 'offense__average_yards_after_catch',
             'Average charted yards after the catch per attempt for this team''s passing offense in this game. NULL means none of the attempts have been charted for YAC yet. offense__yards_after_catch_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_games', 'offense__yards_after_catch_attempts_available',
             'Count of this team''s offensive attempts in this game that have been charted for yards after catch -- the charting-coverage denominator for offense__total_yards_after_catch/offense__average_yards_after_catch. Never NULL; 0 means none of this game''s offensive attempts are charted yet, in which case both aggregates read NULL.'),

            ('stats', 'passing_team_games', 'defense__total_air_yards',
             'Sum of charted air yards allowed by this team''s passing defense in this game (i.e. the opponent offense''s charted air yards against them). NULL means none of the attempts have been charted for air yards yet; 0 is a real observed value. defense__air_yards_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_games', 'defense__average_depth_of_target',
             'Average charted air yards per attempt (aDOT) allowed by this team''s passing defense in this game. NULL means none of the attempts have been charted for air yards yet. defense__air_yards_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_games', 'defense__air_yards_attempts_available',
             'Count of attempts against this team''s defense in this game that have been charted for air yards -- the charting-coverage denominator for defense__total_air_yards/defense__average_depth_of_target. Never NULL; 0 means none of this game''s attempts against this defense are charted yet, in which case both aggregates read NULL.'),
            ('stats', 'passing_team_games', 'defense__total_yards_after_catch',
             'Sum of charted yards after the catch allowed by this team''s passing defense in this game. NULL means none of the attempts have been charted for YAC yet; 0 is a real observed value. defense__yards_after_catch_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_games', 'defense__average_yards_after_catch',
             'Average charted yards after the catch per attempt allowed by this team''s passing defense in this game. NULL means none of the attempts have been charted for YAC yet. defense__yards_after_catch_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_games', 'defense__yards_after_catch_attempts_available',
             'Count of attempts against this team''s defense in this game that have been charted for yards after catch -- the charting-coverage denominator for defense__total_yards_after_catch/defense__average_yards_after_catch. Never NULL; 0 means none of this game''s attempts against this defense are charted yet, in which case both aggregates read NULL.'),

            ('stats', 'passing_team_season', 'offense__total_air_yards',
             'Sum of charted air yards across this team''s passing offense attempts this season. NULL means none of the attempts have been charted for air yards yet; 0 is a real observed value. offense__air_yards_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_season', 'offense__average_depth_of_target',
             'Average charted air yards per attempt (aDOT) for this team''s passing offense this season. NULL means none of the attempts have been charted for air yards yet. offense__air_yards_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_season', 'offense__air_yards_attempts_available',
             'Count of this team''s offensive attempts this season that have been charted for air yards -- the charting-coverage denominator for offense__total_air_yards/offense__average_depth_of_target. Never NULL; 0 means none of this season''s offensive attempts are charted yet, in which case both aggregates read NULL.'),
            ('stats', 'passing_team_season', 'offense__total_yards_after_catch',
             'Sum of charted yards after the catch across this team''s passing offense completions this season. NULL means none of the attempts have been charted for YAC yet; 0 is a real observed value. offense__yards_after_catch_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_season', 'offense__average_yards_after_catch',
             'Average charted yards after the catch per attempt for this team''s passing offense this season. NULL means none of the attempts have been charted for YAC yet. offense__yards_after_catch_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_season', 'offense__yards_after_catch_attempts_available',
             'Count of this team''s offensive attempts this season that have been charted for yards after catch -- the charting-coverage denominator for offense__total_yards_after_catch/offense__average_yards_after_catch. Never NULL; 0 means none of this season''s offensive attempts are charted yet, in which case both aggregates read NULL.'),

            ('stats', 'passing_team_season', 'defense__total_air_yards',
             'Sum of charted air yards allowed by this team''s passing defense this season. NULL means none of the attempts have been charted for air yards yet; 0 is a real observed value. defense__air_yards_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_season', 'defense__average_depth_of_target',
             'Average charted air yards per attempt (aDOT) allowed by this team''s passing defense this season. NULL means none of the attempts have been charted for air yards yet. defense__air_yards_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_season', 'defense__air_yards_attempts_available',
             'Count of attempts against this team''s defense this season that have been charted for air yards -- the charting-coverage denominator for defense__total_air_yards/defense__average_depth_of_target. Never NULL; 0 means none of this season''s attempts against this defense are charted yet, in which case both aggregates read NULL.'),
            ('stats', 'passing_team_season', 'defense__total_yards_after_catch',
             'Sum of charted yards after the catch allowed by this team''s passing defense this season. NULL means none of the attempts have been charted for YAC yet; 0 is a real observed value. defense__yards_after_catch_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_season', 'defense__average_yards_after_catch',
             'Average charted yards after the catch per attempt allowed by this team''s passing defense this season. NULL means none of the attempts have been charted for YAC yet. defense__yards_after_catch_attempts_available is the charting-coverage denominator.'),
            ('stats', 'passing_team_season', 'defense__yards_after_catch_attempts_available',
             'Count of attempts against this team''s defense this season that have been charted for yards after catch -- the charting-coverage denominator for defense__total_yards_after_catch/defense__average_yards_after_catch. Never NULL; 0 means none of this season''s attempts against this defense are charted yet, in which case both aggregates read NULL.')
        ) AS t(tbl_schema, tbl_name, col_name, comment_text)
    LOOP
        IF EXISTS (
            SELECT 1 FROM information_schema.columns c
            WHERE c.table_schema = rec.tbl_schema
              AND c.table_name = rec.tbl_name
              AND c.column_name = rec.col_name
        ) THEN
            EXECUTE format(
                'COMMENT ON COLUMN %I.%I.%I IS %L',
                rec.tbl_schema, rec.tbl_name, rec.col_name, rec.comment_text
            );
            n_commented := n_commented + 1;
        ELSE
            n_skipped := n_skipped + 1;
        END IF;
    END LOOP;
    RAISE NOTICE '057_passing_grants_indexes: % charting-aggregate column comment(s) applied, % skipped (column not present on this load)',
        n_commented, n_skipped;
END $comment$;
