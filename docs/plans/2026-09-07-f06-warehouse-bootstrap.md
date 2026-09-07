# F06 — warehouse bootstrap and migration history

Status: implemented; initial PR CI passed, automated review fixes in validation. Production
ledger adoption and migration 065 application are separate rollout decisions.

## Deliverable and boundaries

Replace the documented replay of historical 001–018 SQL with an explicit,
reviewed catalog baseline. Build an empty disposable warehouse without CFBD
requests or production data. Include normalized dlt tables and nested variants,
roles, extensions, keys, partitions, static seeds, private scouting definitions,
and consumer functions/views. Schema construction and fixture/data population
are separate operations; an empty warehouse is not an ingested warehouse.

A manifest names ordered immutable migrations by unique identity and exact-byte
SHA256. A private ledger and transaction-held advisory lock serialize CLI runs.
Applied immutable drift, removals/reordering, incomplete histories and unmanaged
upgrade targets fail before executing a migration. Unchanged repeatable units
skip; changed repeatable definitions execute and retain their previous checksum
in history. A failing batch rolls back both DDL and ledger writes.

The existing explicit `run_migrations.py --file` route remains available for
reviewed diagnostics, reconciliation and one-off operations. It does not become
an apply-once API. Historical default-chain execution requires an explicit
legacy opt-in; old SQL remains unchanged as history. F07's generic mart CASCADE
and deployment dependency redesign remains separate.

## Work allocation

- Lead: deployed catalog review, portable baseline/manifest, executed PostgreSQL
  integration, fixture coverage, CI and operational documentation.
- Explorer workers: complete schema dependencies and migration caller map.
- Implementer: isolated ledger engine and unit regressions.
- Implementer: explicit CLI and legacy-path safeguards.
- Independent reviewer: catalog capture, baseline and substantive final diff.

## Verification gates

1. One documented command builds an empty disposable PostgreSQL database.
2. A baseline target plus upgrade reaches the same current catalog; subsequent
   invocation applies nothing. Legacy data survives the upgrade.
3. Changed applied bytes, missing/reordered identities and mismatched ledgers
   fail clearly. Concurrent migration processes cannot race ledger state.
4. Failed migration batch leaves neither partial DDL nor ledger entries.
5. Executed SQL checks actual API roles, private scouting denial, nested dlt
   shapes, variant fields and representative consumer results on synthetic rows.
6. Required CI integration uses a disposable service and fails if unavailable;
   it never substitutes a production connection for a fixture database.

## Production boundary

F06 development does not authorize applying a new ledger/baseline to production.
A schema-only catalog export requires explicit approval for its Actions artifact
because it contains production-derived definitions and role metadata. The export
has no table data or credentials. Current production has no migration ledger;
upgrade must refuse implicit adoption. A separately reviewed reconciliation and
adoption process is required before production uses the managed release path.

F26's broader test-fixture conversion and F27's environment locking are related
follow-ups; only integration needed to prove F06 belongs to this change.

## Initial development checks

The ledger/CLI implementation is complete pending full-baseline integration.
Focused tests: 103 passed. Executed disposable PostgreSQL 17 tests: 7 passed,
including upgrade row preservation, unchanged no-op, exact-byte drift rejection,
atomic batch failure, repeatable audit history, lock contention, column-before-
view ordering, and autocommit rejection. Independent review identified the
repeatable ordering and string-scanner issues; both now have regressions.

The catalog artifact approval is still pending. No production export or schema
change has run for F06. Full warehouse reconstruction is not yet verified.

Full credential-free suite: 2,483 passed, 507 skipped (live integrations not
opted in). Ruff, formatting, whitespace and agent setup checks passed. Final
independent engine/CLI review is clean; the full-baseline review remains pending.

## Follow-up: private ledger under host defaults

Executed role checks exposed a real gap: revoking PUBLIC alone did not remove
named-role privileges inherited from broad host defaults. Ledger creation now
removes non-owner ACL entries from only its private schema, two tables and
identity sequence. It does not modify host default privileges or other schemas.
Actual anon/authenticated/analyst_ro reads and writes are denied; direct
schema/table/sequence privilege checks are false, and the same roles retain
default access to ordinary newly created objects. Owners/administrators remain
trusted; this does not revoke their role memberships.

Eight disposable PostgreSQL migration tests and 56 engine/CLI unit tests pass.
Independent review cleared the ACL change. Catalog export approval remains
pending; no production export was attempted after the approval rejection.

## Approved catalog and baseline construction

The user approved the schema/role metadata export to Actions. Run 34157756739
succeeded; dependency review discovered live marts reference the `rp` schema,
so approved-scope follow-up run 34157877609 captured that closure. No rows or
credentials were exported, and no production schema changes were executed.

A fresh PostgreSQL 17 restore succeeded after installing required public
extensions and role stand-ins. All relation owners are postgres. The managed
initial five-entry manifest contains platform prerequisites, generated structural
pre-F05 catalog, current static seeds, dependency-ordered initialization of 60
materialized views, and the original migration 064. Full bootstrap matched the
captured per-schema/per-kind object counts exactly. The nested line-score
fixture returned expected zero/NULL/overtime values.

The first managed attempt correctly rolled back when the historical positions
seed referenced a table absent from production. That obsolete seed was removed;
current era/PFF/Massey seeds remain. Catalog-derived definitions, including rp
objects absent from tracked source DDL, are captured rather than guessed.
See `docs/warehouse-bootstrap.md` for included schemas and platform/data limits.

## Executed consumer-access finding

Fresh and prior-version integration both reached actual-role checks and exposed
one captured production ACL defect: anon/authenticated could not read
`public.team_season_trajectory` because its invoker-rights dependency lacked
SELECT. All 54 API views and the other 12 public wrappers passed. A reviewed
sixth manifest entry (065) restores only SELECT on the public-source trajectory
mart for these two roles; source013 retains that grant on recreation. It does
not grant analyst_ro direct access or change any private-schema policy.

The captured baseline remains unchanged. This is an explicit forward correction,
executed in disposable databases only; production migration065 is not approved
by the schema-export authorization and has not been run.

## Final local verification

- Full credential-free suite: **2,486 passed, 510 skipped**. The skipped live
  integrations are not production evidence.
- Mandatory disposable PostgreSQL 17 suite: **10 passed** (eight migration-engine
  cases and both full warehouse paths). Both paths verify exact captured object
  counts across all 31 schemas, all 60 materialized views initialized, static
  seeds, extensions, partition and representative nested/variant dlt contracts.
- Upgrade from the structural pre-F05 baseline preserves full synthetic game,
  nested line-score and legacy-model row payloads through 064/065. Subsequent
  upgrade is a no-op. The actual CLI was also executed against a fresh database
  and an already managed database; 065 applied once and then skipped.
- Actual roles read all 54 API views; anon/authenticated read all 13 public
  wrappers after 065. Ledger/scouting and analyst raw-fit boundaries are enforced
  with specific permission errors. Representative RPCs execute, and analyst SQL
  rejects private reads and writes. Empty RPC paths are not populated-data tests.
- A pre-existing text guard incorrectly rejected preserved raw `start_yardline`
  column declarations. Its narrow declaration exception retains the analytical
  ban, verified by view and CTAS counterexamples; six split-RPC tests passed.
- Independent reviews are clean for the engine, baseline/manifest, access fix,
  integration scope and adjusted guard. Ruff, formatting, whitespace and shared
  agent-setup checks passed. No production DDL or data writes occurred.

## PR 129 review follow-up

CI at `639e603` passed all jobs, including 10 warehouse bootstrap SQL tests,
2,935 main tests (61 skipped), and 59 MCP tests. Greptile identified the new
artifact-upload action's mutable tag; it is now pinned to the verified v4.6.2
commit. Codex identified libpq fallback from incomplete connection URLs. The
CLI now requires a complete URL and rejects ambient target/credential variables
and query overrides before connecting. Focused regressions and independent
review cover these corrections; production rollout remains separate.
