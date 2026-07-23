-- Behavioral validation for api.team_history -- applied as its OWN
-- file/transaction (after 002_team_history.sql) so a failed assertion
-- reports without rolling back the view DDL. Read-only; safe to re-run any
-- time as a health check.
--
-- Guards the 2026-07-23 change: the view now exposes the historical SP+
-- offense/defense split (sp_offense/sp_defense from
-- marts.team_season_summary), and the definition file re-grants the
-- PostgREST roles after its DROP/CREATE (no default privileges for them in
-- this database -- a bare re-apply used to strip read access).

DO $$
DECLARE
    missing BIGINT;
    role_name TEXT;
BEGIN
    -- The exact question that exposed the gap: Venables-era (2022-2024)
    -- defensive SP+ for Oklahoma, juxtaposed with Clemson. Every one of
    -- those six team-seasons must now carry the split.
    SELECT COUNT(*) INTO missing
    FROM (VALUES ('Oklahoma'), ('Clemson')) teams(team)
    CROSS JOIN generate_series(2022, 2024) AS s(season)
    LEFT JOIN api.team_history th
      ON th.team = teams.team AND th.season = s.season
     AND th.sp_defense IS NOT NULL AND th.sp_offense IS NOT NULL
    WHERE th.team IS NULL;

    IF missing > 0 THEN
        RAISE EXCEPTION 'team_history: % OU/Clemson 2022-2024 rows lack the SP+ split', missing;
    END IF;

    -- Grant tripwire: the DROP/CREATE in 002 discards grants; the file must
    -- have restored read access for every consumer role.
    FOREACH role_name IN ARRAY ARRAY['anon', 'authenticated', 'analyst_ro'] LOOP
        IF NOT has_table_privilege(role_name, 'api.team_history', 'SELECT') THEN
            RAISE EXCEPTION 'team_history: role % lost SELECT after re-apply', role_name;
        END IF;
    END LOOP;

    RAISE NOTICE 'team_history validation passed';
END $$;
