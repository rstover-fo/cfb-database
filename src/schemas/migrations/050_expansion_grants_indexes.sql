-- Migration: 050_expansion_grants_indexes
--
-- Grants + indexes for the A2 endpoint-expansion unit (2026-08-29):
-- playoffs.py (core.cfp_bracket/cfp_games/cfp_participants), coaches.py
-- (ref.coach_seasons/coach_tenures), conferences.py
-- (ref.conference_affiliations/conference_changes), ratings.py's
-- srs_expanded_ratings_resource (ratings.srs_expanded), and stats.py's
-- player_success_season / player_success_game / game_advanced
-- (stats.player_success_season, stats.player_success_game,
-- stats.game_advanced_team_stats), plus metrics.py's reworked
-- ppa_predicted_resource (metrics.ppa_predicted).
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-049. Idempotent (GRANT is naturally re-appliable;
-- indexes use IF NOT EXISTS).
--
-- APPLY AFTER THE FIRST SUCCESSFUL LOAD of every source above (same
-- precondition as 026_win_probability_indexes.sql) -- dlt creates each
-- table on first write, so the plain GRANT/CREATE INDEX statements below
-- will fail with "relation does not exist" against a database where one of
-- these tables hasn't been created yet. That includes a coach_tenures
-- backfill (--source coach_tenures) and a metrics_ppa_predicted backfill
-- (--source metrics_ppa_predicted), both of which are excluded from the
-- daily path and must be run at least once by hand before this applies.
--
-- Column names below are this unit's best expectation from the CFBD
-- OpenAPI spec and the 2026-08-29 probe fixtures (tests/fixtures/cfbd_2026/),
-- NOT a live-database read -- dlt snake_cases camelCase fields and flattens
-- nested objects with "__" (e.g. `coach.id` -> `coach__id`), and coach_seasons
-- in particular was never observed live (every probe call 400'd; see
-- coaches.py). Verify actual column names via pg_attribute before applying,
-- and adjust the coach_seasons index/comment below if the live schema
-- differs from what's assumed here.
--
-- Apply via:
--   python scripts/run_migrations.py --file src/schemas/migrations/050_expansion_grants_indexes.sql

-- ---------------------------------------------------------------------------
-- Schema USAGE: `metrics` was never granted USAGE in
-- grant_read_access_for_security_invoker.sql (that migration's schema list
-- predates this repo's metrics.* tables having any anon-facing consumer).
-- Idempotent -- GRANT is a no-op if already held.
-- ---------------------------------------------------------------------------

GRANT USAGE ON SCHEMA metrics TO anon, authenticated;

-- ---------------------------------------------------------------------------
-- Grants -- root (always-created) tables
-- ---------------------------------------------------------------------------

GRANT SELECT ON core.cfp_bracket, core.cfp_games, core.cfp_participants
    TO anon, authenticated;

GRANT SELECT ON ref.coach_seasons, ref.coach_tenures TO anon, authenticated;

GRANT SELECT ON ref.conference_affiliations, ref.conference_changes
    TO anon, authenticated;

GRANT SELECT ON ratings.srs_expanded TO anon, authenticated;

GRANT SELECT ON
    stats.player_success_season,
    stats.player_success_game,
    stats.game_advanced_team_stats
    TO anon, authenticated;

GRANT SELECT ON metrics.ppa_predicted TO anon, authenticated;

REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON
    core.cfp_bracket, core.cfp_games, core.cfp_participants,
    ref.coach_seasons, ref.coach_tenures,
    ref.conference_affiliations, ref.conference_changes,
    ratings.srs_expanded,
    stats.player_success_season, stats.player_success_game, stats.game_advanced_team_stats,
    metrics.ppa_predicted
    FROM anon, authenticated;

-- ---------------------------------------------------------------------------
-- Grants -- dlt child tables (nested arrays only; nested dicts flatten into
-- top-level "__" columns on the parent and produce no child table). Only
-- cfp_bracket (participants[], rounds[], rounds[].matchups[],
-- matchups[].slots[]) and cfp_games (slots[]) are KNOWN to have array
-- fields from the inspected probe fixtures; coach_seasons carries several
-- richly-nested sub-objects (teamMetrics, recruiting, pollResume,
-- recordSplits, scoring, cfp, draftFollowingSeason) that were never
-- observed live (every probe call 400'd -- see coaches.py), so it is
-- included defensively in case any of those turn out to contain arrays.
-- Every other new table is flat/dict-nested only and is not expected to
-- produce children; this guarded, dynamic form means a wrong guess here
-- fails safe (zero grants) rather than erroring the whole migration on a
-- table name that doesn't exist.
-- ---------------------------------------------------------------------------

DO $$
DECLARE
    parents text[] := ARRAY[
        'core.cfp_bracket',
        'core.cfp_games',
        'ref.coach_seasons'
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
    RAISE NOTICE '050_expansion_grants_indexes: % dlt child table(s) granted SELECT', n;
END $$;

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_player_success_season_id_season
    ON stats.player_success_season (id, season);

CREATE INDEX IF NOT EXISTS idx_player_success_game_id
    ON stats.player_success_game (id);

CREATE INDEX IF NOT EXISTS idx_player_success_game_game_id
    ON stats.player_success_game (game_id);

-- coach__id: dlt's flattened name for coach_seasons' nested `coach.id`
-- (DetailedCoachSeason.coach.id per the CFBD OpenAPI spec) -- verify via
-- pg_attribute before/at apply time (see header note; this endpoint was
-- never observed live during development).
CREATE INDEX IF NOT EXISTS idx_coach_seasons_coach_id
    ON ref.coach_seasons (coach__id);

CREATE INDEX IF NOT EXISTS idx_conference_affiliations_team_id
    ON ref.conference_affiliations (team_id);

CREATE INDEX IF NOT EXISTS idx_game_advanced_team_stats_game_id
    ON stats.game_advanced_team_stats (game_id);

-- ---------------------------------------------------------------------------
-- Column comments -- player-grain join spine (id/season/team/position),
-- mirroring how migrations 042/046/047 carry provenance/NULL-semantics
-- notes on the column itself rather than only in docs.
-- ---------------------------------------------------------------------------

COMMENT ON COLUMN stats.player_success_season.id IS
    'CFBD athlete id (string). Player-grain join spine alongside season/team/position.';
COMMENT ON COLUMN stats.player_success_season.season IS
    'Season year. Player-grain join spine alongside id/team/position.';
COMMENT ON COLUMN stats.player_success_season.team IS
    'Team full name at time of record. Player-grain join spine alongside id/season/position -- included in the PK because a transferred player can appear under more than one team in a season.';
COMMENT ON COLUMN stats.player_success_season.position IS
    'Position abbreviation as reported by CFBD for this season. Part of the player-grain join spine (id/season/team/position), not itself a PK column.';

COMMENT ON COLUMN stats.player_success_game.id IS
    'CFBD athlete id (string). Player-grain join spine alongside season/team/position.';
COMMENT ON COLUMN stats.player_success_game.season IS
    'Season year. Player-grain join spine alongside id/team/position.';
COMMENT ON COLUMN stats.player_success_game.team IS
    'Team full name for this game. Player-grain join spine alongside id/season/position.';
COMMENT ON COLUMN stats.player_success_game.position IS
    'Position abbreviation as reported by CFBD for this game. Part of the player-grain join spine (id/season/team/position), not itself a PK column.';
