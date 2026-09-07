# F06 — warehouse bootstrap and migration history

Status: development in progress after PR #128 merged (`b6c8e98`).

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
