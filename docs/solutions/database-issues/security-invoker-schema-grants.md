---
title: "SECURITY INVOKER Requires Schema Grants for Cross-Schema Views"
category: database-issues
tags: [supabase, security, rls, security-invoker, permissions, postrgrest]
module: public views
symptom: "Views return 'permission denied for table X' after switching from SECURITY DEFINER to SECURITY INVOKER"
root_cause: "SECURITY INVOKER views execute as the calling role (anon), which lacks USAGE/SELECT on underlying schemas"
---

# SECURITY INVOKER Requires Schema Grants for Cross-Schema Views

## Problem

After converting all 13 public schema views from `SECURITY DEFINER` to `SECURITY INVOKER` (to satisfy Supabase security linter), cfb-app broke with permission denied errors on most views.

## Root Cause

- `SECURITY DEFINER` views execute as the **view owner** (postgres superuser) — they can access any schema
- `SECURITY INVOKER` views execute as the **calling role** (anon via PostgREST) — they can only access schemas the caller has USAGE + SELECT on
- The `anon` role only had USAGE on `public`, `core`, and `marts` schemas
- Views like `public.teams` read from `ref.teams` — anon had no access to `ref` schema
- Views like `public.team_special_teams_sos` read from `ratings.*` — anon had no access to `ratings` schema

## Fix

Grant USAGE + SELECT on all data schemas to anon/authenticated, and REVOKE DML to maintain read-only:

```sql
GRANT USAGE ON SCHEMA ref TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA ref TO anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA ref FROM anon, authenticated;
-- Repeat for: ratings, recruiting, stats, marts, core, api, analytics, betting, draft
```

## Testing Gotcha

**Always test as the anon role, not as postgres superuser.** The Supabase MCP connection uses the postgres superuser role, which has access to everything. This masked the permission issue during initial testing.

```sql
-- WRONG: Tests as superuser (always works)
SELECT * FROM public.teams LIMIT 1;

-- RIGHT: Tests as anon (catches permission issues)
SET ROLE anon;
SELECT * FROM public.teams LIMIT 1;
RESET ROLE;
```

## Prevention

When switching any view to SECURITY INVOKER:
1. List all schemas referenced by the view's query
2. Verify the calling role (anon/authenticated) has USAGE on each schema
3. Verify the calling role has SELECT on each referenced table
4. Test with `SET ROLE anon` before deploying
