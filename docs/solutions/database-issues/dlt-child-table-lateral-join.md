---
title: "dlt child table LATERAL JOIN pattern"
category: database-issues
tags: [dlt, lateral-join, child-tables, jsonb, supabase]
module: src/schemas/api
symptoms:
  - "column t.logos does not exist"
  - "column does not exist for JSONB array field"
  - "dlt flattened column missing from parent table"
severity: high
date: 2026-02-06
---

# dlt child table LATERAL JOIN pattern

## Problem

When creating SQL views that reference dlt-managed tables, JSONB array columns
from the API response don't exist as columns on the parent table. Attempting to
access them (e.g., `t.logos->0`) fails with "column does not exist."

## Symptoms

```
ERROR: 42703: column t.logos does not exist
```

## Investigation

1. Checked `ref.teams` columns via `pg_attribute` — no `logos` column exists
2. Discovered dlt flattens JSONB arrays into **child tables** following the
   naming pattern `{parent}__{field_name}`
3. Child tables use `_dlt_parent_id` to link back to the parent's `_dlt_id`
4. Array ordering is preserved via `_dlt_list_idx`

## Root Cause

dlt (dlthub) automatically normalizes nested JSON structures. When the CFBD API
returns a team with `"logos": ["url1", "url2"]`, dlt creates:

- **Parent table** `ref.teams` — no `logos` column
- **Child table** `ref.teams__logos` with columns:
  - `value` (the actual logo URL)
  - `_dlt_parent_id` (references `ref.teams._dlt_id`)
  - `_dlt_list_idx` (array position: 0, 1, ...)
  - `_dlt_id` (unique row identifier)

## Solution

Use a `LEFT JOIN LATERAL` to fetch the first element from the child table:

```sql
SELECT
    t.school,
    logo.value AS logo_url
FROM ref.teams t
LEFT JOIN LATERAL (
    SELECT value FROM ref.teams__logos
    WHERE _dlt_parent_id = t._dlt_id AND _dlt_list_idx = 0
    LIMIT 1
) logo ON true
```

Key points:
- `LEFT JOIN` preserves parent rows even when no child exists (logo_url = NULL)
- `_dlt_list_idx = 0` gets the first array element
- `LIMIT 1` is defensive but technically redundant with the idx filter
- `ON true` is required syntax for LATERAL joins

## Prevention

1. **Never assume JSONB column structure matches the API response.** Always
   check actual columns with `\d table_name` or `pg_attribute` queries.
2. **Look for `__` child tables** when a field is missing from a dlt parent table.
   Pattern: `{schema}.{parent}__{field_name}`
3. **Common dlt child table columns:** `value`, `_dlt_parent_id`, `_dlt_list_idx`, `_dlt_id`

## Related

- dlt normalization docs: https://dlthub.com/docs/general-usage/schema#data-normalization
- `ref.teams__logos`, `ref.teams__alternate_names` — both follow this pattern
- `core.game_player_stats__teams__categories__types__athletes` — deeply nested example
