-- run_analyst_query: guarded free-form read-only SQL for the cfb-app MCP
-- `run_sql` tool (cfb-app src/lib/mcp/tools.ts §20 calls this RPC via
-- PostgREST; until this file is applied the tool degrades gracefully).
--
-- Security model (v3 -- reshaped twice by prod findings, see below):
--   * the boundary is the ROLE, not the textual checks: a dedicated
--     analyst_ro NOLOGIN role holds USAGE+SELECT on the `api` schema ONLY.
--     api views are owner-rights (NOT security_invoker -- confirmed against
--     src/schemas/api/*), so analyst_ro needs no grants on marts/core for
--     them to work, and direct reads of any other schema fail with
--     insufficient_privilege. No DML grants anywhere.
--   * the function is SECURITY INVOKER; anon/authenticated (the PostgREST
--     roles) are granted MEMBERSHIP in analyst_ro, so the function's
--     SET LOCAL ROLE analyst_ro is legal for them and the caller's SQL
--     executes with analyst_ro's privileges. Membership is a pure
--     privilege REDUCTION path: analyst_ro can do strictly less than anon
--     already can. Non-member callers (e.g. postgres) fail at SET ROLE --
--     the drop is mandatory, never best-effort.
--   * SET LOCAL transaction_read_only = on closes the "write via a
--     SECURITY DEFINER helper reachable from SELECT" path (e.g.
--     public.refresh_all_marts). The SET clause below gives the call a GUC
--     nest level, so role + read-only revert at function exit and never
--     leak into the caller's transaction.
--   * textual checks (SELECT/WITH prefix, single statement) exist only to
--     fail fast with clear messages. A data-modifying CTE slips past the
--     prefix check by design and dies at the permission layer instead.
--
-- Why not the handoff's SECURITY DEFINER + SET LOCAL ROLE: Postgres forbids
-- SET ROLE inside SECURITY DEFINER functions outright ("cannot set
-- parameter \"role\" within security-definer function" -- caught by this
-- file's self-validation on first deploy). Why not DEFINER via function
-- ownership: ALTER FUNCTION ... OWNER TO analyst_ro requires the migration
-- role to be able to SET ROLE analyst_ro, and Supabase TERMINATES the
-- connection on GRANT analyst_ro TO postgres (its managed-role protection
-- kills the session rather than erroring). Granting membership to
-- anon/authenticated is permitted -- probed live 2026-07-23.
--
-- Review deviations from the handoff draft (still apply in v3):
--   * LIMIT placement: the draft's `FROM (%s LIMIT 200) q` breaks any query
--     that already ends in LIMIT/OFFSET; the cap lives on an outer wrapper
--     subquery instead.
--   * statement_timeout: an in-function SET LOCAL cannot bound the
--     in-flight statement (the timer arms at top-level statement start), so
--     none is set. The effective bound is the Supabase role-level timeout
--     on the calling role plus the app's 55s MCP client timeout.
--
-- Caveats (documented, accepted):
--   * a literal ';' inside a string constant is rejected by the
--     single-statement check (false positive; rewrite the query).
--   * duplicate output column names collapse in jsonb (last one wins) --
--     alias columns when joining.
--   * migration-time validation cannot exercise the happy path (postgres
--     is deliberately not an analyst_ro member), so it asserts the grants
--     catalog-side and proves the mandatory drop; the end-to-end positive
--     test is any authenticated PostgREST call (the cfb-app tool).
--
-- Apply via: python scripts/run_migrations.py --file src/schemas/public/012_run_analyst_query.sql
-- (deploy manifest "apply"). Idempotent.

-- 1. Restricted role + memberships -------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst_ro') THEN
        CREATE ROLE analyst_ro NOLOGIN;
    END IF;
END $$;

GRANT USAGE ON SCHEMA api TO analyst_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA api TO analyst_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA api GRANT SELECT ON TABLES TO analyst_ro;

-- PostgREST roles become members so the function's SET LOCAL ROLE is legal
-- for them. Do NOT grant analyst_ro to postgres: Supabase kills the
-- connection on membership changes to its managed postgres role, and the
-- migration role staying a non-member is what makes the drop provably
-- mandatory (see self-validation).
GRANT analyst_ro TO anon, authenticated, service_role;

-- 2. Guarded executor (SECURITY INVOKER) -------------------------------------
CREATE OR REPLACE FUNCTION public.run_analyst_query(query_sql text)
RETURNS jsonb
LANGUAGE plpgsql
SET search_path = api, public
AS $$
DECLARE
    cleaned text := btrim(query_sql);
    result jsonb;
BEGIN
    -- Fail-fast checks; the analyst_ro grants are the real boundary.
    IF cleaned !~* '^(select|with)\M' THEN
        RAISE EXCEPTION 'only SELECT/WITH statements are allowed';
    END IF;
    cleaned := regexp_replace(cleaned, ';\s*$', '');
    IF position(';' IN cleaned) > 0 THEN
        RAISE EXCEPTION 'multiple statements are not allowed';
    END IF;

    SET LOCAL ROLE analyst_ro;
    SET LOCAL transaction_read_only = on;

    -- Hard row cap on an outer wrapper, so an inner LIMIT/OFFSET still
    -- parses; the smaller of the two limits wins.
    EXECUTE format(
        'SELECT COALESCE(jsonb_agg(q), ''[]''::jsonb) '
        'FROM (SELECT * FROM (%s) analyst_q LIMIT 200) q',
        cleaned
    ) INTO result;

    RETURN result;
END;
$$;

REVOKE ALL ON FUNCTION public.run_analyst_query(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.run_analyst_query(text) TO anon, authenticated, service_role;

-- 3. Self-validation ---------------------------------------------------------
DO $$
DECLARE
    r jsonb;
BEGIN
    -- Textual rejections fire before the role drop, so they are testable
    -- here regardless of membership.
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

    -- The privilege drop is mandatory: a valid query from a NON-member
    -- (this migration role) must die at SET ROLE, not run unscoped.
    BEGIN
        r := public.run_analyst_query('SELECT 1 AS x');
        RAISE EXCEPTION 'non-member call ran unscoped (returned %)', r;
    EXCEPTION WHEN insufficient_privilege THEN
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

    RAISE NOTICE 'run_analyst_query self-validation passed';
END $$;

NOTIFY pgrst, 'reload schema';
