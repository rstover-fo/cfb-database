-- Behavioral test matrix for public.run_analyst_query -- applied as its OWN
-- file/transaction (after 012_run_analyst_query.sql) so a failed assertion
-- reports without rolling back the function's DDL. Read-only side effects
-- only; safe to re-run any time as a health check.
--
-- Findings encoded here, all proven on prod during the 012 deploy rounds:
--   * the migration role reaches analyst_ro transitively (postgres is a
--     member of the API roles), so the happy path is testable here;
--   * SET LOCAL ROLE + read-only persist to end-of-transaction after the
--     first call (harmless: later calls run as analyst_ro, which holds
--     EXECUTE for exactly this reason);
--   * a write CTE against an api view dies as "cannot delete from view"
--     (55000 object_not_in_prerequisite_state -- api views wrap matviews
--     and are not auto-updatable), which blocks writes even before the
--     role/read-only layers get their turn; against a real table it dies
--     at the permission layer. All three error classes are accepted.

DO $$
DECLARE
    r jsonb;
BEGIN
    -- Textual rejections (fire before the role drop).
    BEGIN
        PERFORM public.run_analyst_query('DELETE FROM api.team_elo');
        RAISE EXCEPTION 'DELETE was not rejected';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%only SELECT/WITH%' THEN RAISE; END IF;
    END;

    BEGIN
        PERFORM public.run_analyst_query('SELECT 1; SELECT 2');
        RAISE EXCEPTION 'multi-statement was not rejected';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%multiple statements%' THEN RAISE; END IF;
    END;

    -- The drop happens: inside the query, current_user must be analyst_ro.
    r := public.run_analyst_query('SELECT current_user AS u');
    IF r <> '[{"u": "analyst_ro"}]'::jsonb THEN
        RAISE EXCEPTION 'query did not run as analyst_ro: %', r;
    END IF;

    -- api-view read works under the role; row cap and inner LIMIT parse.
    r := public.run_analyst_query('SELECT team FROM api.team_elo;');
    IF jsonb_array_length(r) < 1 OR jsonb_array_length(r) > 200 THEN
        RAISE EXCEPTION 'api.team_elo read returned % rows', jsonb_array_length(r);
    END IF;
    r := public.run_analyst_query('SELECT team FROM api.team_elo LIMIT 3');
    IF jsonb_array_length(r) <> 3 THEN
        RAISE EXCEPTION 'inner LIMIT handling returned % rows', jsonb_array_length(r);
    END IF;

    -- Outside the api schema: blocked by the role.
    BEGIN
        PERFORM public.run_analyst_query('SELECT id FROM core.games LIMIT 1');
        RAISE EXCEPTION 'core.games read was not blocked';
    EXCEPTION WHEN insufficient_privilege THEN
        NULL;
    END;

    -- Write CTEs: blocked by non-updatable views (55000) against api,
    -- and by permissions/read-only against anything else.
    BEGIN
        PERFORM public.run_analyst_query(
            'WITH w AS (DELETE FROM api.team_elo RETURNING *) SELECT * FROM w');
        RAISE EXCEPTION 'write CTE (api view) was not blocked';
    EXCEPTION WHEN insufficient_privilege OR read_only_sql_transaction
              OR object_not_in_prerequisite_state THEN
        NULL;
    END;
    BEGIN
        PERFORM public.run_analyst_query(
            'WITH w AS (DELETE FROM core.games RETURNING id) SELECT * FROM w');
        RAISE EXCEPTION 'write CTE (core table) was not blocked';
    EXCEPTION WHEN insufficient_privilege OR read_only_sql_transaction THEN
        NULL;
    END;

    -- Grants, catalog-side.
    IF NOT pg_has_role('anon', 'analyst_ro', 'MEMBER')
       OR NOT pg_has_role('authenticated', 'analyst_ro', 'MEMBER') THEN
        RAISE EXCEPTION 'PostgREST roles are not analyst_ro members';
    END IF;
    IF NOT has_schema_privilege('analyst_ro', 'api', 'USAGE')
       OR NOT has_table_privilege('analyst_ro', 'api.team_elo', 'SELECT') THEN
        RAISE EXCEPTION 'analyst_ro is missing api read grants';
    END IF;
    IF has_table_privilege('analyst_ro', 'core.games', 'SELECT')
       OR has_schema_privilege('analyst_ro', 'marts', 'USAGE') THEN
        RAISE EXCEPTION 'analyst_ro can reach beyond the api schema';
    END IF;

    RAISE NOTICE 'run_analyst_query validation matrix passed';
END $$;
