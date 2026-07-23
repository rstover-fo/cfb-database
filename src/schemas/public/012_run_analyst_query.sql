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
--     already can. Supabase's postgres role is itself a member of the API
--     roles, so it reaches analyst_ro transitively and the drop applies to
--     it too (proven below by asserting current_user inside the query).
--   * SET LOCAL transaction_read_only = on closes the "write via a
--     SECURITY DEFINER helper reachable from SELECT" path (e.g.
--     public.refresh_all_marts). NOTE (proven live): SET LOCAL role and
--     read-only do NOT revert at function exit -- they persist to the END
--     of the calling transaction. Under PostgREST every request is its own
--     transaction, so callers never observe it; a client batching several
--     RPC calls in one transaction simply STAYS dropped to analyst_ro
--     (conservative direction), which is why analyst_ro itself needs
--     EXECUTE on this function -- see the grant below.
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
--
-- Apply via: python scripts/run_migrations.py --file src/schemas/public/012_run_analyst_query.sql
-- (deploy manifest "apply"). Idempotent. DDL-ONLY by design: the behavioral
-- test matrix lives in validation_run_analyst_query.sql, applied as a
-- SEPARATE file (own transaction) so a validation surprise reports instead
-- of rolling back the function.

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
-- for them. Never GRANT analyst_ro TO postgres directly -- Supabase kills
-- the connection on membership changes to its managed postgres role. It is
-- also unnecessary: postgres is a member of the API roles and reaches
-- analyst_ro through them.
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
-- analyst_ro needs EXECUTE for re-entry: after one call, the transaction's
-- role IS analyst_ro (see stickiness note above), and the next call in the
-- same transaction would otherwise be denied.
GRANT EXECUTE ON FUNCTION public.run_analyst_query(text)
    TO anon, authenticated, service_role, analyst_ro;

-- PostgREST schema reload.
NOTIFY pgrst, 'reload schema';
