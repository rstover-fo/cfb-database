---
title: "Materialized view vs regular view confusion"
category: database-issues
tags: [materialized-views, views, pg_matviews, refresh, supabase]
module: src/schemas/functions
symptoms:
  - "REFRESH MATERIALIZED VIEW fails on a regular view"
  - "pg_matviews returns empty for expected view"
  - "relation does not exist when refreshing"
severity: medium
date: 2026-02-06
---

# Materialized view vs regular view confusion

## Problem

A view was included in a materialized view refresh function and test inventory,
but it was actually a regular view in a different schema. This would cause
runtime errors when the refresh function tries to `REFRESH MATERIALIZED VIEW`
on a non-materialized view.

## Symptoms

```sql
-- Returns empty (not a matview):
SELECT * FROM pg_matviews WHERE matviewname = 'data_quality_dashboard';

-- The view exists, but as a regular view:
SELECT * FROM pg_views WHERE viewname = 'data_quality_dashboard';
-- Returns: analytics.data_quality_dashboard
```

## Investigation

1. `data_quality_dashboard` was listed in SCHEMA_CONTRACT.md under marts
2. Added to `refresh_all_marts.sql` layer 1 and `test_marts.py` MARTS_VIEWS
3. Tests failed: existence check against `pg_matviews` returned NULL
4. Cascading failures: the SQL error poisoned the shared connection (see
   related doc on psycopg2 transaction failures)
5. Queried `pg_views` — found it in `analytics` schema as a regular view

## Root Cause

`analytics.data_quality_dashboard` is a regular `CREATE VIEW`, not a
`CREATE MATERIALIZED VIEW`. It was incorrectly assumed to be a mart matview
because:
- It appeared in documentation alongside materialized views
- The naming followed the same pattern as mart views
- No one verified the view type before adding it to the refresh function

## Solution

1. Removed from `MARTS_VIEWS` list in `test_marts.py`
2. Removed from layer 1 of `refresh_all_marts.sql`
3. Removed from marts section of `SCHEMA_CONTRACT.md`
4. Redeployed the corrected `marts.refresh_all()` function

## Prevention

**Always verify view type before adding to refresh functions or test inventories:**

```sql
-- Check if it's a materialized view
SELECT schemaname, matviewname
FROM pg_matviews
WHERE matviewname = 'your_view_name';

-- Check if it's a regular view
SELECT schemaname, viewname
FROM pg_views
WHERE viewname = 'your_view_name';

-- Or check both at once
SELECT
    n.nspname AS schema,
    c.relname AS name,
    CASE c.relkind
        WHEN 'm' THEN 'materialized view'
        WHEN 'v' THEN 'regular view'
    END AS type
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE c.relname = 'your_view_name'
  AND c.relkind IN ('m', 'v');
```

## Related

- `marts.refresh_all()` — the function that was corrected
- `analytics.data_quality_dashboard` — the view in question
- `test_marts.py` — test inventory that needed correction
