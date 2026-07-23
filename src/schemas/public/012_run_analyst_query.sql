-- run_analyst_query: guarded free-form read-only SQL for the cfb-app MCP
-- `run_sql` tool (cfb-app src/lib/mcp/tools.ts §20 calls this RPC via
-- PostgREST; until this file is applied the tool degrades gracefully).
--
-- Security model (per the cfb-app handoff, adjusted in review -- see notes):
--   * the boundary is the ROLE, not the textual checks: a dedicated
--     analyst_ro NOLOGIN role holds USAGE+SELECT on the `api` schema ONLY.
--     api views are owner-rights (NOT security_invoker -- confirmed against
--     src/schemas/api/*), so analyst_ro needs no grants on marts/core for
--     them to work, and direct reads of any other schema fail with
--     insufficient_privilege. No DML grants anywhere.
--   * the function is SECURITY DEFINER and OWNED BY analyst_ro, so the
--     caller's SQL executes with analyst_ro's privileges directly. (The
--     handoff draft used SET LOCAL ROLE inside the function instead;
--     Postgres forbids that -- "cannot set parameter \"role\" within
--     security-definer function", caught by this file's self-validation
--     on first deploy. Owner-as-boundary needs no role switch at all.)
--     The transaction is marked read-only so even SECURITY DEFINER
--     functions reachable via SELECT (e.g. public.refresh_all_marts)
--     cannot write through it; the GUC is rolled back at function exit
--     (SET clause + SECURITY DEFINER establish a GUC nest level) and
--     never leaks into the caller's transaction.
--   * textual checks (SELECT/WITH prefix, single statement) exist only to
--     fail fast with clear messages. A data-modifying CTE slips past the
--     prefix check by design and dies at the permission layer instead.
--
-- Review deviations from the handoff draft:
--   * LIMIT placement: the draft's `FROM (%s LIMIT 200) q` breaks any query
--     that already ends in LIMIT/OFFSET (`... LIMIT 10 LIMIT 200` is a
--     syntax error). The cap lives on an outer wrapper subquery instead.
--   * statement_timeout: the draft SET LOCAL a timeout inside the function,
--     but statement_timeout is armed when the top-level statement STARTS --
--     changing it mid-statement does not bound the in-flight RPC call, so
--     the line was theater and is omitted. The effective bound is the
--     Supabase role-level timeout on the calling role (anon/authenticated)
--     plus the app's 55s MCP client timeout. The handoff's pg_sleep test is
--     dropped for the same reason.
--   * role creation is guarded for idempotent re-application, and EXECUTE
--     is revoked from PUBLIC (functions default to PUBLIC-executable).
--
-- Caveats (documented, accepted):
--   * a literal ';' inside a string constant is rejected by the
--     single-statement check (false positive; rewrite the query).
--   * duplicate output column names collapse in jsonb (last one wins) --
--     alias columns when joining.
--
-- Apply via: python scripts/run_migrations.py --file src/schemas/public/012_run_analyst_query.sql
-- (deploy manifest "apply"). Idempotent. Self-validating: the DO block at
-- the end exercises the RPC and raises (failing the migration) on any
-- behavioral regression.

-- 1. Restricted role ---------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst_ro') THEN
        CREATE ROLE analyst_ro NOLOGIN;
    END IF;
END $$;

GRANT USAGE ON SCHEMA api TO analyst_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA api TO analyst_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA api GRANT SELECT ON TABLES TO analyst_ro;

-- 2. Guarded executor --------------------------------------------------------
CREATE OR REPLACE FUNCTION public.run_analyst_query(query_sql text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = api, public
AS $$
DECLARE
    cleaned text := btrim(query_sql);
    result jsonb;
BEGIN
    -- Fail-fast checks; the analyst_ro grants below are the real boundary.
    IF cleaned !~* '^(select|with)\M' THEN
        RAISE EXCEPTION 'only SELECT/WITH statements are allowed';
    END IF;
    cleaned := regexp_replace(cleaned, ';\s*$', '');
    IF position(';' IN cleaned) > 0 THEN
        RAISE EXCEPTION 'multiple statements are not allowed';
    END IF;

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

-- 3. Ownership (the security boundary) + PostgREST exposure ------------------
-- SECURITY DEFINER runs with the OWNER's privileges: analyst_ro. Supabase's
-- postgres role is not a true superuser, so assigning ownership requires
-- membership in the target role and the target role needs CREATE on the
-- schema for the duration of the ALTER (revoked immediately after --
-- ownership survives the revoke).
GRANT analyst_ro TO CURRENT_USER;
GRANT CREATE ON SCHEMA public TO analyst_ro;
ALTER FUNCTION public.run_analyst_query(text) OWNER TO analyst_ro;
REVOKE CREATE ON SCHEMA public FROM analyst_ro;

REVOKE ALL ON FUNCTION public.run_analyst_query(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.run_analyst_query(text) TO anon, authenticated, service_role;

-- 4. Self-validation ---------------------------------------------------------
DO $$
DECLARE
    r jsonb;
BEGIN
    -- Plain scalar round-trip.
    r := public.run_analyst_query('SELECT 1 AS x');
    IF r <> '[{"x": 1}]'::jsonb THEN
        RAISE EXCEPTION 'scalar round-trip returned %', r;
    END IF;

    -- Trailing semicolon tolerated; api-view read works under analyst_ro;
    -- row cap applies.
    r := public.run_analyst_query('SELECT team FROM api.team_elo;');
    IF jsonb_array_length(r) < 1 OR jsonb_array_length(r) > 200 THEN
        RAISE EXCEPTION 'api.team_elo read returned % rows', jsonb_array_length(r);
    END IF;

    -- Inner LIMIT survives the wrapper (the draft's construction broke here).
    r := public.run_analyst_query('SELECT team FROM api.team_elo LIMIT 3');
    IF jsonb_array_length(r) <> 3 THEN
        RAISE EXCEPTION 'inner LIMIT handling returned % rows', jsonb_array_length(r);
    END IF;

    -- Non-SELECT rejected by the prefix check.
    BEGIN
        PERFORM public.run_analyst_query('DELETE FROM api.team_elo');
        RAISE EXCEPTION 'DELETE was not rejected';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%only SELECT/WITH%' THEN RAISE; END IF;
    END;

    -- Multiple statements rejected.
    BEGIN
        PERFORM public.run_analyst_query('SELECT 1; SELECT 2');
        RAISE EXCEPTION 'multi-statement was not rejected';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%multiple statements%' THEN RAISE; END IF;
    END;

    -- Outside the api schema: blocked by the role, not by text checks.
    BEGIN
        PERFORM public.run_analyst_query('SELECT id FROM core.games LIMIT 1');
        RAISE EXCEPTION 'core.games read was not blocked';
    EXCEPTION WHEN insufficient_privilege THEN
        NULL;
    END;

    -- Data-modifying CTE: passes the prefix check, dies at the
    -- permission/read-only layer.
    BEGIN
        PERFORM public.run_analyst_query(
            'WITH w AS (DELETE FROM api.team_elo RETURNING *) SELECT * FROM w');
        RAISE EXCEPTION 'write CTE was not blocked';
    EXCEPTION WHEN insufficient_privilege OR read_only_sql_transaction THEN
        NULL;
    END;

    RAISE NOTICE 'run_analyst_query self-validation passed';
END $$;

NOTIFY pgrst, 'reload schema';
