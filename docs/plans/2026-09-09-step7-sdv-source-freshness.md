# Step 7: SDV season publication freshness

Status: implemented and independently reviewed after the four source publishers
merged in PRs #143 and #144. Production migration, policy configuration and workflow
activation remain separate rollout steps.

## Additive API

Migration 073 adds `public.get_source_freshness(p_season bigint)`. An explicit
season from 1869 through 2200 returns exactly four rows, ordered as SDV ratings,
weekly FPI, team crosswalk and game crosswalk. Missing receipts do not remove
rows. NULL and out-of-range seasons are errors.

The existing `get_asset_freshness()` two-row house Elo contract and the legacy
six-column `get_data_freshness()` contract remain unchanged. No current app or
MCP consumer is redirected. Consumers can adopt the new RPC separately after
deployment; generated client types live in the downstream repositories.

The new RPC uses owner rights with an empty search path and grants only its
bounded read result to `anon` and `authenticated`. Private receipts, operation
records, policy tables and staging data remain private. The response contains
typed scalar columns; raw JSON, file paths, URLs, operation scopes and arbitrary
error strings are excluded.

| Fields | Meaning |
|---|---|
| `source_name`, `asset_key`, `season`, `coverage_key` | Fixed source identity and exact `season:YYYY` scope. |
| `generation_id`, `published_at`, `current_outcome`, `is_complete` | Exact valid current successful season receipt, or NULL when no valid current publication exists. |
| `source_rows`, `published_rows` | Equal nonempty artifact and published counts from the current receipt. |
| `age_seconds`, `expected_refresh_interval`, `is_stale` | Publication age computed at statement time, explicit per-season policy and nullable stale result. |
| `publication_state` | `unrecorded`, `unpublished`, `invalid` or `current`. |
| `artifact_origin`, `season_basis` | Allowlisted provenance: registered URL or local file, with in-file, registered-name or caller-declared season evidence. |
| `latest_outcome`, `latest_recorded_at` | Latest receipt diagnostic, independent of the currently readable generation. |
| `last_failure_outcome`, `last_failure_at`, `last_failure_category` | Latest failed/partial/deferred/blocked receipt, retained after recovery; only an allowlisted category is exposed. |

## Evidence and policy

`unrecorded` means there is no pointer or receipt history. `unpublished` means
history exists but the current pointer is absent, including invalidation after
a legacy write. `invalid` means a pointer exists but its receipt cannot prove
the required successful, complete source/season publication. No historical
success is substituted for a missing or invalid pointer.

Current evidence must match the selected source, parser protocol, season and
complete-file scope, carry valid provenance, and have equal nonempty artifact
and publication counts. Publication timestamps must be finite and no later
than the current statement. Migration 071 ratings receipts predate `season_basis`;
the API derives their `artifact_field` basis from that reviewed protocol.
Migration 072 receipts carry their basis explicitly. Neither complete-file
coverage nor publication age proves provider-wide coverage, finished seasons
or availability before a prediction.

The existing private `meta.asset_freshness_policies` table gains exact SDV
season scopes while preserving both Elo `source-wide` scopes and their policy
values. No SDV policies or thresholds are seeded. Missing or NULL intervals
produce NULL `is_stale`; fetch cadence does not silently become an SLA.
Publication age advances between SQL statements without a materialized-view
refresh, and `ANALYZE` cannot renew it.

A later unsuccessful attempt does not hide an older valid current publication.
The response reports both independently. A subsequent successful publication
becomes the latest outcome while preserving the historical failure diagnostic.
Latest-outcome diagnostics accept the full receipt outcome enum, including
`expected_no_data`; current SDV evidence requires `succeeded`. Failure categories
are limited to `source_publication_failed` or NULL.
This RPC reads recorded publication evidence; it does not reread source files,
verify every target row or enforce downstream generation dependencies. The
trusted-administrator guard-bypass limits of the publication protocol still
apply.

## Verifier behavior

`verify_load.py` checks the new RPC for the requested season after the existing
Elo receipt check. Missing migration or unrecorded optional publishers warn,
including in strict mode. Receipt history with no current publication warns
normally and fails in strict mode. Invalid identity, grain or current evidence
always fails. Query, permission and transport errors propagate.

A declared stale publication fails in season or strict mode, otherwise warns.
Unknown policy always warns. A valid current publication inside its declared
interval can pass this publication check. A latest unsuccessful attempt emits
a separate warning; an older historical failure does not keep a recovered
publication in warning status forever.

## Verification

- 125 verifier and manifest unit tests passed, including optional missing
  evidence, strict grading, malformed contracts and separate failure diagnostics.
- 38 new SQL tests passed on disposable PostgreSQL 17, including exact types
  and grain, actual public caller roles, private-data exclusions, malformed and
  future-dated evidence, policy isolation, aging, compatibility and migration
  reapplication. The real verifier passed across publication, failure and
  recovery using database-serialized responses.
- 154 existing bootstrap, upgrade, freshness, generation and SDV publication
  SQL regressions passed on a separate disposable PostgreSQL 17 fixture.
- 3 legacy MCP freshness tests and affected Python lint/format checks passed.
- Independent SQL and Python review found no remaining material issues.

No production migration, policy activation or provider request was performed.
Production query latency and costs at representative receipt-history volume
remain unmeasured.
