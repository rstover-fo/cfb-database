-- Read-only verification after migration 065. Logs assertions, never table rows.
SET TRANSACTION READ ONLY;
SET LOCAL statement_timeout = '120s';
DO $verify$
DECLARE caller text; api_view record; checked integer;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relname='team_season_trajectory'
          AND c.relkind='v' AND c.reloptions @> ARRAY['security_invoker=true']
    ) THEN
        RAISE EXCEPTION 'Expected invoker-rights trajectory wrapper';
    END IF;
    FOREACH caller IN ARRAY ARRAY['anon','authenticated','analyst_ro'] LOOP
        EXECUTE format('SET LOCAL ROLE %I', caller);
        checked := 0;
        FOR api_view IN
            SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='api' AND c.relkind='v' ORDER BY c.relname
        LOOP
            EXECUTE format('SELECT * FROM api.%I LIMIT 1', api_view.relname);
            checked := checked + 1;
        END LOOP;
        IF checked < 54 THEN
            RAISE EXCEPTION 'Expected at least 54 API views, found %', checked;
        END IF;
        IF caller IN ('anon','authenticated') THEN
            EXECUTE 'SELECT * FROM public.team_season_trajectory LIMIT 1';
            EXECUTE 'SELECT * FROM marts.team_season_trajectory LIMIT 1';
            IF has_table_privilege(current_user, 'marts.team_season_trajectory',
                                   'INSERT,UPDATE,DELETE,TRUNCATE') THEN
                RAISE EXCEPTION 'Unexpected trajectory write privilege for %', caller;
            END IF;
        ELSE
            BEGIN
                EXECUTE 'SELECT * FROM marts.team_season_trajectory LIMIT 1';
                RAISE EXCEPTION 'Unexpected direct trajectory access for analyst_ro';
            EXCEPTION WHEN insufficient_privilege THEN NULL;
            END;
        END IF;
        BEGIN
            EXECUTE 'SELECT * FROM scouting.players LIMIT 1';
            RAISE EXCEPTION 'Unexpected scouting access for %', caller;
        EXCEPTION WHEN insufficient_privilege THEN NULL;
        END;
        EXECUTE 'RESET ROLE';
        RAISE NOTICE 'F06 access passed: role=%, API views=%, private boundary retained',
            caller, checked;
    END LOOP;
END
$verify$;
