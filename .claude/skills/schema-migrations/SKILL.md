---
name: schema-migrations
description: Schema-change workflow for this repo — migrations, grants/RLS, matview refresh chains, column contracts, and the downstream-consumer gate. Use before writing or applying any migration, altering any schema, adding a matview or API view, or changing anything cfb-app/cfb-scout read.
---

# Schema Migrations (cfb-database)

## Before touching anything

1. **Downstream gate.** Changes to `core`, `recruiting`, `api`, or `public`
   can break cfb-app and cfb-scout. Check `docs/SCHEMA_CONTRACT.md` first;
   if the surface is listed, the change needs a contract update and a
   heads-up in the PR body.
2. **Column contracts.** `features.team_week` is governed by
   `docs/brainstorms/2026-07-21-team-week-feature-design.md`: no column is
   added without first amending sections 1f (row), 1i (NULL rule), 2a
   (vector position) and the stated column count. Candidate features also
   need a recorded screen verdict before a migration ships (see the
   pre-registration entries in that doc's "Decisions made").
3. **Verify current state from the catalog**, not docs: column names via
   `pg_attribute`, view type (matview vs view) before adding to test
   inventories. dlt renames columns on load.
4. **Variant-twin allow-lists (KTD7).** When a mart COALESCEs a new dlt
   `<col>__v_double` twin, extend `EXPECTED_VARIANT_TWINS` in
   `src/pipelines/utils/variant_twins.py` AND the matching allow-list array
   in `src/schemas/api/validation_rushing_views.sql` (group (e)) — the two
   must stay equal (`tests/test_variant_twins.py` guards it). Miss either
   one and either `verify_load.py` fails every daily run on a twin it
   doesn't recognize, or the deploy-time SQL validation does.

## Writing the migration

- Numbering: next integer after the highest in `src/schemas/migrations/`,
  zero-padded 3 digits. Never renumber.
- Two application paths — pick the one the file header declares:
  - `MIGRATION_ORDER` in `scripts/run_migrations.py` (the ordered chain).
  - Deploy-manifest style, applied via `run_migrations.py --file <path>`
    (the 019-028 / 041+ pattern). State this in the header comment.
- `src/schemas/scouting/` (own numbering, 001+) is DDL codified FROM the
  live database during the 2026-08 cub-scout merge — applied via `--file`,
  never in `MIGRATION_ORDER`. Applying any of it against prod must be a
  no-op; the scouting schema deliberately has no anon/authenticated grants.
- Idempotent by construction: `ADD COLUMN IF NOT EXISTS`,
  `CREATE OR REPLACE`, guarded indexes. The verification bar is "applies
  twice without error".
- `COMMENT ON COLUMN` carries provenance: screened partial-r values, NULL
  semantics, and source notes live on the column, not only in docs
  (mirror migrations 042/046/047).
- NULL-never-0 for rates and coefficients; a fabricated zero is an extreme
  value. Counts may use guarded zeros only with an EXISTS check that the
  source data was actually loaded (the 047 draft-columns exception).

## Grants, RLS, and view security (hard-won)

- All API/public views are **owner-rights by design** (the Postgres
  default — `security_invoker` is NOT set): they execute with the view
  owner's privileges, so `anon`/`authenticated` need `USAGE` on the view's
  schema and `SELECT` on the view itself, not on every underlying table.
  The trap is on the write side: a recreate (`DROP`+`CREATE`) loses the
  PostgREST roles' grants, and there is no `ALTER DEFAULT PRIVILEGES`
  covering them here — every migration that recreates a view must
  re-GRANT, or cfb-app breaks silently.
- **Always test as the caller**: `SET ROLE anon;` then query. The MCP/
  superuser connection masks permission failures.
- RLS on any user-facing table; `SET search_path = ''` plus fully-qualified
  names on functions; revoke DML from `anon` on newly exposed schemas.

## Matviews and refresh chains

- New matviews must join the refresh chain (`marts.refresh_all()` layers /
  `scripts/refresh_marts.py`) in dependency order — an orphaned matview
  serves stale data forever (the `player_comparison` lesson).
- Window functions (PERCENT_RANK) belong in a matview with a thin view on
  top, not a plain view (~2s vs <5ms).
- `ref.teams` has 35 duplicate school names: `DISTINCT ON (school)` before
  any school-name join.

## After applying

- Update `tests/` inventories (view lists, column counts, contract tests).
- Full backfill/rebuild runs that need warehouse credentials can run via
  the `model-experiments` workflow when local credentials are unavailable.
