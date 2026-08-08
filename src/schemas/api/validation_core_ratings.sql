-- Behavioral validation for the CORE ratings surface -- applied as its OWN
-- file/transaction (stage 1: after marts 043 + api 044 + marts 028 + public
-- 011; stage 2: after the team_season_summary rebuild) so a failed assertion
-- reports without rolling back the DDL. Read-only; safe to re-run any time
-- as a health check.
--
-- Assertion groups:
--   (a) freshness surface: marts.data_freshness carries 24 tracked tables
--       including a core_ratings row with real pg_stat activity (a typo'd
--       schema/table name in 028's VALUES list degrades silently into a
--       permanently-stale row -- this is the guard).
--   (b) mart shape: api.core_ratings row floor and internal consistency
--       (overall = offense - defense; defense_rank = 1 is the season's
--       LOWEST defense value).
--   (c) grant tripwires, checked with has_table_privilege rather than SET
--       ROLE (runner may not hold the roles): anon/authenticated kept
--       SELECT on api.core_ratings AND on marts.data_freshness (the plain
--       SQL RPC public.get_data_freshness executes as the caller).
--   (d) stage-2 embed: team_season_summary carries core_* columns and the
--       CASCADE-dropped api views were all re-created with grants intact.
--       Guarded to no-op before stage 2 so this file validates stage 1 alone.

DO $$
DECLARE
    tracked INT;
    fresh RECORD;
    total BIGINT;
    bad_identity BIGINT;
    best_defense_rank_value NUMERIC;
    min_defense_value NUMERIC;
    role_name TEXT;
    view_name TEXT;
    stage2 BOOLEAN;
BEGIN
    -- (a) freshness tracking
    SELECT COUNT(*) INTO tracked FROM marts.data_freshness;
    IF tracked <> 24 THEN
        RAISE EXCEPTION 'data_freshness: expected 24 tracked tables, found %', tracked;
    END IF;

    SELECT * INTO fresh
    FROM marts.data_freshness
    WHERE schema_name = 'ratings' AND table_name = 'core_ratings';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'data_freshness: no ratings.core_ratings row';
    END IF;
    IF fresh.row_count = 0 OR fresh.days_since_activity IS NULL THEN
        RAISE EXCEPTION 'data_freshness: core_ratings row looks like a name typo (row_count=%, days_since_activity=%)',
            fresh.row_count, fresh.days_since_activity;
    END IF;
    RAISE NOTICE 'data_freshness core_ratings: % rows, days_since_activity=%, stale=%',
        fresh.row_count, fresh.days_since_activity, fresh.is_stale;

    -- (b) mart shape and internal consistency
    SELECT COUNT(*) INTO total FROM api.core_ratings;
    RAISE NOTICE 'api.core_ratings rows: %', total;
    IF total < 1000 THEN
        RAISE EXCEPTION 'api.core_ratings: implausibly few rows (%) -- 2016-2025 backfill should give 1300+', total;
    END IF;

    SELECT COUNT(*) INTO bad_identity
    FROM api.core_ratings
    WHERE ABS(overall - (offense - defense)) > 0.05;
    IF bad_identity > 0 THEN
        RAISE EXCEPTION 'api.core_ratings: % rows violate overall = offense - defense', bad_identity;
    END IF;

    SELECT defense INTO best_defense_rank_value
    FROM api.core_ratings WHERE season = 2024 AND defense_rank = 1 LIMIT 1;
    SELECT MIN(defense) INTO min_defense_value
    FROM api.core_ratings WHERE season = 2024;
    IF best_defense_rank_value IS DISTINCT FROM min_defense_value THEN
        RAISE EXCEPTION 'api.core_ratings: defense_rank=1 (%) is not the lowest 2024 defense (%) -- rank direction is wrong',
            best_defense_rank_value, min_defense_value;
    END IF;

    -- (c) grant tripwires
    FOREACH role_name IN ARRAY ARRAY['anon', 'authenticated'] LOOP
        IF NOT has_table_privilege(role_name, 'api.core_ratings', 'SELECT') THEN
            RAISE EXCEPTION 'api.core_ratings: role % lost SELECT after apply', role_name;
        END IF;
        IF NOT has_table_privilege(role_name, 'marts.data_freshness', 'SELECT') THEN
            RAISE EXCEPTION 'marts.data_freshness: role % lost SELECT after 028 re-apply -- public.get_data_freshness() RPC is broken for PostgREST callers', role_name;
        END IF;
    END LOOP;

    -- (d) stage-2 embed (no-op until team_season_summary carries core_overall)
    SELECT EXISTS (
        SELECT 1 FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = 'marts' AND c.relname = 'team_season_summary'
          AND a.attname = 'core_overall' AND NOT a.attisdropped
    ) INTO stage2;

    IF stage2 THEN
        SELECT COUNT(*) INTO total
        FROM marts.team_season_summary
        WHERE season = 2024 AND core_overall IS NOT NULL;
        IF total < 100 THEN
            RAISE EXCEPTION 'team_season_summary: only % rows have core_overall for 2024 -- embed join is broken', total;
        END IF;

        -- Every matview the team_season_summary CASCADE drops must be rebuilt.
        FOREACH view_name IN ARRAY ARRAY[
            'marts.team_season_trajectory', 'marts.coaching_tenure',
            'marts.recruiting_roi', 'marts.transfer_portal_impact',
            'marts.conference_comparison'
        ] LOOP
            IF to_regclass(view_name) IS NULL THEN
                RAISE EXCEPTION '% was CASCADE-dropped by the team_season_summary rebuild and not re-created', view_name;
            END IF;
        END LOOP;

        -- Every view the CASCADE drops must be back, readable by PostgREST roles.
        FOREACH view_name IN ARRAY ARRAY[
            'api.team_detail', 'api.team_history', 'api.matchup',
            'api.leaderboard_teams', 'api.coaching_history', 'api.recruiting_roi',
            'api.transfer_portal_impact', 'api.conference_comparison',
            'public.team_season_trajectory'
        ] LOOP
            IF to_regclass(view_name) IS NULL THEN
                RAISE EXCEPTION '% was CASCADE-dropped by the team_season_summary rebuild and not re-created', view_name;
            END IF;
            FOREACH role_name IN ARRAY ARRAY['anon', 'authenticated'] LOOP
                IF NOT has_table_privilege(role_name, view_name, 'SELECT') THEN
                    RAISE EXCEPTION '%: role % lost SELECT after the stage-2 rebuild', view_name, role_name;
                END IF;
            END LOOP;
        END LOOP;
        -- Trajectory chain: the 013 rebuild must carry the rank/alias columns
        -- public/002 and get_trajectory_averages() select (the 2026-08-08
        -- drift), and they must be populated, not just present.
        SELECT COUNT(*) INTO total
        FROM public.team_season_trajectory
        WHERE season = 2024 AND off_epa_rank IS NOT NULL AND def_epa_rank IS NOT NULL;
        IF total < 100 THEN
            RAISE EXCEPTION 'public.team_season_trajectory: only % 2024 rows with EPA ranks -- trajectory rebuild is broken', total;
        END IF;

        RAISE NOTICE 'stage-2 embed validation passed';
    ELSE
        RAISE NOTICE 'stage-2 embed not applied yet -- skipped (d)';
    END IF;

    RAISE NOTICE 'core ratings validation passed';
END $$;
