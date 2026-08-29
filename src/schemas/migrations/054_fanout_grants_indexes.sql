-- Migration: 054_fanout_grants_indexes
--
-- Grants + indexes for the A4 unit (2026-08-29): the two per-entity fan-out
-- drainers, coaches.py's coach_profiles_resource (ref.coach_profiles) and
-- the new player_overview.py module's player_season_overview_resource
-- (stats.player_season_overview).
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-050 and 052. Idempotent (GRANT is naturally
-- re-appliable; indexes use IF NOT EXISTS).
--
-- APPLY AFTER THE FIRST SUCCESSFUL LOAD of both sources (same precondition
-- as 026_win_probability_indexes.sql and 050_expansion_grants_indexes.sql)
-- -- dlt creates each table on first write, so the plain GRANT/CREATE INDEX
-- statements below will fail with "relation does not exist" against a
-- database where coach_profiles/player_season_overview hasn't loaded a row
-- yet. Both are drainers wired into scripts/load_season.py's SOURCE_ORDER
-- (run.py::run_coach_profiles_pipeline, run.py::run_player_overview_pipeline),
-- so the next daily/backfill run creates them; there is no separate
-- backfill-only invocation the way coach_tenures/metrics_ppa_predicted
-- needed in 050.
--
-- `ref` and `stats` already have schema USAGE granted to anon/authenticated
-- (grant_read_access_for_security_invoker.sql), so this migration only
-- needs table-level GRANTs, unlike 050's `metrics` USAGE grant.
--
-- Column names below are this unit's best expectation from the CFBD
-- OpenAPI spec (CoachProfile, PlayerSeasonOverview schemas) and the
-- 2026-08-29 probe fixture (tests/fixtures/cfbd_2026/player_season_overview.json)
-- for player_season_overview -- NOT a live-database read. `/coaches/profile`
-- was never observed live (every probe call needs a real coachId, and
-- ref.coach_seasons -- the source of those ids -- has not been backfilled
-- against the live database as of this writing; see docs/pipeline-manifest.md
-- rows 63/69). Verify actual column names via pg_attribute before applying,
-- and adjust below if the live schema differs from what's assumed here.
--
-- Apply via:
--   python scripts/run_migrations.py --file src/schemas/migrations/054_fanout_grants_indexes.sql

-- ---------------------------------------------------------------------------
-- Grants -- root (always-created) tables
-- ---------------------------------------------------------------------------

GRANT SELECT ON ref.coach_profiles TO anon, authenticated;
GRANT SELECT ON stats.player_season_overview TO anon, authenticated;

REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON
    ref.coach_profiles,
    stats.player_season_overview
    FROM anon, authenticated;

-- ---------------------------------------------------------------------------
-- Grants -- dlt child tables (nested arrays only; nested dicts flatten into
-- top-level "__" columns on the parent and produce no child table).
--
-- player_season_overview.boxScoreStats.categories is a KNOWN array (per the
-- probe fixture: {name, stats: [{name, value}]}), so it -- and its own
-- nested `stats` array -- WILL child-table
-- (player_season_overview__box_score_stats__categories and
-- ...__box_score_stats__categories__stats). usage and ppa are nested dicts
-- only (no arrays) and are expected to flatten into usage__*, ppa__average__*,
-- ppa__total__* columns on the root table with no children.
--
-- coach_profiles is included defensively, mirroring 050's inclusion of
-- ref.coach_seasons: CoachProfile's nested currentTeam/career/almaMater
-- objects are all flat per the OpenAPI spec (no arrays), so no child table
-- is expected, but this endpoint was never observed live -- a wrong guess
-- here fails safe (zero grants) rather than erroring the whole migration on
-- a table name that doesn't exist.
-- ---------------------------------------------------------------------------

DO $$
DECLARE
    parents text[] := ARRAY[
        'ref.coach_profiles',
        'stats.player_season_overview'
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
    RAISE NOTICE '054_fanout_grants_indexes: % dlt child table(s) granted SELECT', n;
END $$;

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

-- Player-grain join spine: run_player_overview_pipeline's candidate query
-- looks up by (season, id) but downstream consumers commonly start from a
-- player id across seasons, hence (id, season) rather than (season, id) --
-- the PK's own implicit ordering already covers the (season, id) direction.
CREATE INDEX IF NOT EXISTS idx_player_season_overview_id_season
    ON stats.player_season_overview (id, season);

CREATE INDEX IF NOT EXISTS idx_coach_profiles_id
    ON ref.coach_profiles (id);

-- ---------------------------------------------------------------------------
-- Column comments -- player-grain join spine, mirroring how migrations
-- 042/046/047/050 carry provenance/NULL-semantics notes on the column
-- itself rather than only in docs.
-- ---------------------------------------------------------------------------

COMMENT ON COLUMN stats.player_season_overview.id IS
    'CFBD athlete id (string). Player-grain join spine alongside season/team/position.';
COMMENT ON COLUMN stats.player_season_overview.season IS
    'Season year. Player-grain join spine alongside id/team/position.';
COMMENT ON COLUMN stats.player_season_overview.team IS
    'Team full name for this season. Player-grain join spine alongside id/season/position.';
COMMENT ON COLUMN stats.player_season_overview.position IS
    'Position abbreviation as reported by CFBD for this season. Part of the player-grain join spine (id/season/team/position), not itself a PK column.';
