---
name: schema-migrations
description: Use when changing warehouse DDL, migrations, grants, or consumer-facing SQL contracts.
---

# Schema migrations

Preserve warehouse grain, keys, null meaning, security boundaries, refresh
dependencies, and consumer contracts. Inspect the current migration and caller
before choosing an implementation.

## Load only the relevant contracts

- For a public or consumer-used object, read `docs/SCHEMA_CONTRACT.md` and update
  it when the exposed contract changes. Breaking removals or renames require a
  compatible rollout; regenerate consumer types when the exposed shape changes.
- For a feature or model input, read
  `docs/brainstorms/2026-07-21-team-week-feature-design.md` and
  `docs/modeling-contract.md`. Amend the design contract before implementing a
  governed change.
- When SQL consumes dlt nested tables or variant twins, inspect the actual
  loaded shape and the relevant variant allow-lists. Preserve
  `_dlt_parent_id` relationships and keep `EXPECTED_VARIANT_TWINS` in
  `src/pipelines/utils/variant_twins.py` consistent with the validation allow-list
  in `src/schemas/api/validation_rushing_views.sql`.
- For `scouting.*`, its DDL in `src/schemas/scouting/` has independent numbering
  and applies through `--file`, outside the main ordered chain. Preserve its private boundary. Do not grant `anon` or
  `authenticated` access unless the requested contract explicitly exposes a
  reviewed API surface.

## Migration design

- Use the next migration number without renumbering history. Follow the
  repository's existing ordered-chain or explicit-file application path and
  state the intended path in the migration.
- Make repeat application safe: use guarded additions/indexes and replaceable
  definitions where appropriate. Preserve explicit row grain, primary/unique
  keys, foreign keys, and source provenance.
- Rates and coefficients remain null when unavailable; zero is valid only when
  it has a defined observed meaning. Counts may default to zero only when the
  source's presence makes that distinction sound.
- Prefer stable provider identifiers. A name-only join requires a reviewed
  crosswalk or documented deterministic resolution with verified cardinality.
  Do not use unordered `DISTINCT ON (school)` to choose an arbitrary
  `ref.teams` row.
- Record non-obvious source, null, grain, and measurement semantics in comments
  near the schema they govern.

## Views, privileges, and dependencies

- API/public views use the existing owner-rights design. Do not set
  `security_invoker` unless the requested security model changes. Recreating a
  view drops its grants, so restore schema usage and view/RPC privileges for
  each intended caller.
- Preserve RLS and DML restrictions on user-facing tables. Functions should use
  an empty search path and fully qualified objects where the existing security
  design requires it.
- Inspect the full dependency closure before replacing a view or materialized
  view. A materialized view must join the refresh chain in dependency order.
- Choose materialization from measured latency, freshness, storage, and refresh
  costs. Window functions alone do not require a materialized view; use one
  when representative measurement justifies it and keep a thin API view when
  that preserves the consumer contract.

## Verification

Run focused SQL or tests that exercise grain, idempotence, null behavior,
dependencies, and affected contracts. With a suitable database, apply
representative SQL twice and query under each caller role, including `anon` or
`authenticated` where applicable. A superuser query does not validate grants.

Database credentials are not required to draft or fix a migration. When they
are unavailable, use current DDL, migrations, and tests, and report that catalog
shape, caller-role behavior, executed idempotence, and query performance remain
unverified rather than claiming those checks passed.
