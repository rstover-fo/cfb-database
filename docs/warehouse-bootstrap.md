# Managed warehouse bootstrap

The default manifest reconstructs the reviewed warehouse catalog captured on
2026-09-07. It includes 31 application schemas, normalized dlt/staging tables,
partitions, indexes, grants, 60 materialized views, and API/public functions and
views. It loads static repository seeds, not production rows or dlt load state.

## Local database

Install development dependencies with `bash scripts/setup_dev.sh`, then start
an isolated PostgreSQL 17 with pgvector. Use a dedicated container and loopback
port; do not reuse a production tunnel or another project's database.

```bash
docker run --name cfb-fixture-postgres \
  -e POSTGRES_PASSWORD=local-password -p 127.0.0.1:55436:5432 -d \
  pgvector/pgvector:pg17@sha256:cf134a767f474095eeba57e0117be8e568e011a63f33fbf252f14c9b760f8e6f
export WAREHOUSE_DB_URL='postgresql://postgres:local-password@127.0.0.1:55436/postgres'
```

The CLI requires an explicit action and reads only `WAREHOUSE_DB_URL`:

```bash
.venv/bin/python scripts/bootstrap_warehouse.py bootstrap
.venv/bin/python scripts/bootstrap_warehouse.py plan
.venv/bin/python scripts/bootstrap_warehouse.py upgrade
.venv/bin/python scripts/bootstrap_warehouse.py status
```

Use a complete `postgresql://user:password@host:port/database` URL with one TCP
host. Percent-encode reserved characters in credentials. Target and credential
overrides in the query string, service files, socket paths, and host lists are
rejected. Unset ambient `PGDATABASE`, `PGHOST`, `PGHOSTADDR`, `PGPASSFILE`,
`PGPASSWORD`, `PGPORT`, `PGSERVICE`, `PGSERVICEFILE`, and `PGUSER` before running
the CLI; it rejects these variables instead of inheriting a different target
or password. Connection options such as `sslmode=require` remain supported.

`--manifest` selects a reviewed version-1 JSON manifest. `--target` stops at an
existing migration ID; it cannot roll back applied history. `--dry-run`, `plan`
and `status` use read-only transactions and never install the ledger.

## Release semantics

Manifest entries have unique `id`, repository-relative SQL `path`, and `kind`
(`immutable` or `repeatable`). Immutable entries retain exact bytes and order
forever once applied. Forward prerequisites run before changed repeatables;
repeatables then run in manifest order. Add prerequisite objects as immutable
forward migrations when a changed view/function needs them.

The private `warehouse_control` ledger stores identities, paths, exact SHA256
checksums and timestamps. Repeatable execution history retains each checksum.
A transaction-scoped advisory lock covers state validation, all migration SQL,
and ledger changes. Any SQL failure rolls back the entire batch. Migration SQL
cannot contain top-level transaction controls. Unchanged invocations are no-ops;
changed applied immutable files, deleted/reordered identities, and unmanaged
upgrade targets fail clearly. No command silently adopts production history.

The old `run_migrations.py --file` interface deliberately continues to execute
explicit diagnostics/repairs each time. It is not ledger-managed. Historical
001–018 chain execution now needs `--legacy-history`; inspecting it with
`--dry-run` remains available without that opt-in.

## Executed integration

The ledger tests create uniquely named databases in an explicitly selected
loopback cluster and drop only those databases afterward:

```bash
F06_TEST_DB_URL="$WAREHOUSE_DB_URL" F06_REQUIRE_DB=1 \
  .venv/bin/python -m pytest tests/test_warehouse_migrations_sql.py -q
```

The full catalog integration suite is `tests/test_warehouse_bootstrap_sql.py`.
Run both files to exercise fresh installation, prior-version upgrade, repeated
no-op, catalog counts, nested dlt rows, and actual consumer-role access. CI uses
the same pinned disposable image with mandatory connection checks. Ordinary
dependency installation does not provision data.

## Prior-baseline upgrade exercise

On a second empty disposable database, stop at the structural pre-F05 baseline,
then upgrade through the original migration 064 and access correction 065:

```bash
.venv/bin/python scripts/bootstrap_warehouse.py bootstrap \
  --target warehouse.baseline.ready.20260907
.venv/bin/python scripts/bootstrap_warehouse.py upgrade
.venv/bin/python scripts/bootstrap_warehouse.py upgrade
```

The second upgrade applies nothing. This fixture was derived from the current
catalog by removing the three F05 registry tables and three trigger functions.
It is a structural prior version, not a claim to recover an exact historical
production snapshot; comments on legacy model tables still describe F05.

## Capture and platform boundaries

[Approved schema-only capture](https://github.com/rstover-fo/cfb-database/actions/runs/34157877609)
and its SHA256/object inventory are recorded in
`src/schemas/baseline/20260907_catalog.json`. The generated baseline is immutable
once applied. The first capture exposed an untracked dependency on `rp` tables
used by live returning-production marts; a second capture included that schema.
That capture preceded production migration 065 and explicit ledger adoption.

The restore requires the `postgres` owner. Platform roles are NOLOGIN stand-ins
and include the captured analyst membership grants. Public extensions are
pg_trgm, fuzzystrmatch, and pgvector. Supabase authentication, storage, cron,
vault, GraphQL and platform-managed extensions are outside this fixture. Other
application/incident schemas (`app`, `bot`, `tracking`, `recovery`) are excluded;
no captured warehouse definition depends on them. This baseline does not
codify the user's uncommitted film/tracking work.

All 54 API views retain their captured owner-rights behavior. The 13 public
wrapper views retain their existing invoker-rights settings. Scouting stays
private. Role stand-ins do not emulate Supabase JWT/authentication or service
administrator privileges.

The static seeds are current repository-owned era definitions, PFF team mappings,
and the reviewed Massey crosswalk. The obsolete root `ref.positions` seed is
excluded because that relation is absent from the captured warehouse. Provider
reference rows, training fits, prediction history, scouting records, and `rp`
weight/configuration rows are not fabricated. Populate those through their
separately authorized ingestion/configuration workflows before relying on data
coverage or model outputs. Empty-view refresh proves executable definitions,
not representative production refresh cost or populated warehouse completeness.

Schema-only capture retains normalized dlt columns and relationships, but does
not copy `_dlt_version`/`_dlt_pipeline_state` row contents. An unledgered existing
warehouse is deliberately rejected by managed upgrade. Adoption requires a
separately reviewed catalog comparison and provenance plan; never mark historical
transformations as applied solely from matching names. This production warehouse
now has a separately verified catalog-adoption root, described below.

## Explicit production adoption

`scripts/adopt_warehouse_catalog.py` prepares a schema-only capture and an
immutable adoption receipt. It uses the approved Deploy Schema environment's
`SUPABASE_DB_URL`, with the same complete-URL validation as the bootstrap CLI.
It does not fall back to dlt configuration. Preparation writes schema definitions
and ownership/access metadata, never table rows; review the artifact destination
as part of the rollout scope.

```bash
python scripts/adopt_warehouse_catalog.py prepare \
  --output /tmp/warehouse-adoption \
  --source-revision <full-reviewed-git-sha> \
  --capture-provenance <capture-run-reference> \
  --root-path src/schemas/adoptions/20260907_production_catalog_receipt.sql
```

Review the generated catalog against the prior approved capture and explain
every difference. Check in the receipt JSON, its exact generated root SQL, and
the separate production manifest at its declared paths. The root binds the
receipt bytes, capture provenance, and schema/metadata fingerprints. It attests
to catalog equivalence observed at adoption; it does not claim historical
baseline, seed, or migration execution. It also refuses ordinary bootstrap.

During a window without other schema writers, run `status`, then `adopt`, then
`status` again with explicit `--manifest` and `--receipt` paths. Adoption compares
the reviewed fingerprint before creating the private ledger and records one
root entry transactionally. A mismatch must be investigated and reviewed,
never bypassed by substituting the currently observed hash. Preserve and restore
the prior writer-workflow states around the maintenance window. The migration
advisory lock coordinates managed tools; other administrators must avoid DDL.

Future production changes append new immutable migrations to
`src/schemas/production-manifest.json` and use
`bootstrap_warehouse.py upgrade --manifest src/schemas/production-manifest.json`
with an explicit `WAREHOUSE_DB_URL`. Do not use the disposable bootstrap manifest
on production. The original receipt remains immutable after later upgrades;
its fingerprint describes the adoption catalog, not those future schema changes.
Once later managed migrations are recorded, adoption `status` and repeat `adopt`
validate the complete migration history and explicitly report
`catalog_verification: not_checked_after_managed_upgrades`, with null current
fingerprint/match fields. They do not mistake expected schema evolution for
adoption-time drift or claim to verify the evolved live catalog. A root-only
ledger still requires the exact adoption fingerprint, and incomplete or tampered
history fails validation.

The approved production receipt is
`src/schemas/adoptions/20260907_production_catalog_receipt.json`. It records only
the observed catalog-adoption event; migration 065's actual executions are
recorded in the rollout evidence, not fabricated as managed migration history.

See [production rollout evidence](plans/2026-09-07-f06-production-rollout.md).

## Managed production workflow

The Deploy Schema workflow has three explicit `workflow_dispatch` actions:

| Action | Behavior |
| --- | --- |
| `managed_plan` | Read-only comparison of the production manifest and ledger. |
| `managed_upgrade` | Apply pending production entries in one managed transaction, then observe status in a separate workflow step. |
| `managed_status` | Read-only inspection of the production ledger and pending entries. |

Select a reviewed commit/ref and one action, leaving the legacy mart, file,
refresh, backfill and compute inputs empty. Managed actions cannot be mixed
with those options or dispatched through a pushed `deploy-manifest.json`.
They always select `src/schemas/production-manifest.json`; there is no manifest
override or bootstrap action in this workflow path. The workflow maps its
existing `SUPABASE_DB_URL` secret to `WAREHOUSE_DB_URL` for the managed step.
The bootstrap CLI's complete-URL and ambient-libpq validation still applies.

On an authorized secure runner with `WAREHOUSE_DB_URL` configured, the same
deployment driver commands are:

```bash
python scripts/deploy_schema.py --action managed_plan
python scripts/deploy_schema.py --action managed_upgrade
python scripts/deploy_schema.py --action managed_status
```

These actions invoke `bootstrap_warehouse.py` rather than the per-file SQL
runner. They do not run ingestion, refresh marts, seed freshness policies or
grant runtime memberships. Preparing or merging this path does not dispatch it.

Before an upgrade, review the plan/checksums and quiesce target DML and
administrator DDL. The managed advisory lock coordinates managed runners, not
all existing warehouse jobs. The current runner sets neither `lock_timeout`
nor `statement_timeout`, and DDL locks last until the transaction ends. Monitor
the backend during the window; do not treat workflow cancellation as a normal
timeout policy.

The workflow runs an observational managed status step after an attempted
upgrade, including an unsuccessful attempt. Status takes the same advisory lock
before reading, so it cannot overtake a still-running managed transaction. It
does not retry the upgrade or turn a failed upgrade step green. If a runner is
killed or commit acknowledgement is lost, a failed job alone does not prove
rollback. Wait for the backend/locks to disappear and run `managed_status`
before deciding whether another upgrade is needed. A completed upgrade should
have the reviewed checksums recorded and no pending entries. Verify actual
consumer roles, private ACLs and enabled warehouse event triggers separately.

## PostgreSQL 17 role creation and activation

On PostgreSQL 17, a non-superuser with `CREATEROLE` receives an automatic
membership in a new role with administration enabled, inheritance disabled
and role switching disabled. The prepared 070/071 role guards and 072 dependency
guard accept that narrow migration-owner administration edge, as well as the
membership-free state produced by the superuser fixtures. They reject outbound
memberships, unrelated members and activated `SET`/`INHERIT` edges. They do not
grant runtime access as part of schema deployment.

The correction changes previously prepared migration files because the
September 10 read-only production ledger check still found only the adopted
root, with no 066–073 executions. Once these exact bytes are applied, they are
immutable. Recreate disposable fixtures with older applied bytes; never rewrite
their ledger checksums to hide drift.

After the complete migration chain commits, resolve each actual connection's
`session_user` before configuring its bounded role:

| Bounded role | Runtime connection |
| --- | --- |
| `warehouse_ingest` | Dedicated `CFBD_QUOTA_DB_URL` connection |
| `warehouse_publisher` | House Elo compute connection |
| `warehouse_refresher` | Generation-enforced mart refresh connection |
| `warehouse_source_publisher` | Receipt-enabled flat-file publication connection |

A distinct runtime login needs `ADMIN=false`, `INHERIT=false`, `SET=true` on
the granted membership. If the migration-owner login is reused, grant role
switching with explicit `INHERIT FALSE, SET TRUE` while preserving its automatic
administration edge. The stock PostgreSQL 17 test produces two membership rows:
the original bootstrap-superuser grant with `ADMIN=true, INHERIT=false, SET=false`,
and a self-granted runtime edge with `ADMIN=false, INHERIT=false, SET=true`.
Inspect all grantor rows on the actual target; do not assume one row per role
and member. Explicitly disable membership inheritance even when the login has
`INHERIT` enabled. Do not blindly revoke the creator edge: that may
remove the non-superuser's ability to administer the bounded role later. A
reused `postgres` credential still has its underlying broad `BYPASSRLS` rights.
Exercise `SET LOCAL ROLE` and the allowlisted RPC permissions on each actual
runtime connection before enabling its job.

After activation, direct reapplication of the guarded SQL intentionally fails
on those runtime memberships. Normal managed upgrades skip already applied
immutable entries and do not re-run the guards. Schema deployment, each runtime
activation, positive freshness intervals and quota capacity remain separate
rollout decisions; see the
[production preflight](plans/2026-09-09-production-receipts-preflight.md).

Focused stock PostgreSQL 17 tests execute the non-superuser role creation,
revalidation/dependency guards and runtime grant/role-switching paths. The full
chain still runs in the superuser fixture. Stock PostgreSQL does not allow the
non-superuser to create event triggers; exact full-chain non-superuser execution
requires Supautils-equivalent privileges and remains part of the authorized
Supabase deployment verification.

## Forward correction discovered by executed role checks

The captured `public.team_season_trajectory` wrapper grants consumer access but
uses invoker rights without SELECT on its underlying mart. All other public
wrappers and API views passed. Migration 065 grants only SELECT on that
public-source mart to anon/authenticated; analyst_ro retains its API-only
boundary, and no consumer write grant is added. The source mart definition
retains this grant on a later reviewed recreation. The generated captured
baseline is unchanged; the managed manifest applies the fix as a forward step.

Migration 065 was approved and applied twice in production. Actual-role checks
passed for the trajectory wrapper, all 54 API views, and private/write boundaries;
see the production rollout evidence above.
