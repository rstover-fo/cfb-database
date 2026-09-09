# F11: generation-enforced house Elo refresh

Status: prepared implementation; production migration, runtime membership and
cadence declaration require a separate rollout. This is delivery step 6 after
the receipt-backed freshness compatibility layer.

## Bounded planner contract

The existing registry still determines selection and dependency order. New
`refresh_marts.py --views marts.house_elo_game --require-receipts` opts into a
controlled adapter for the single declared input `analytics.house_elo_game` at
`source-wide` grain. The adapter rejects unsupported or mixed selections before
opening a database connection or executing any refresh. In particular,
`--changed analytics.house_elo_game` also selects the unreceipted house Elo
summary, so it cannot silently refresh only a supported subset in strict mode.
The adapter checks that the registry still declares exactly its supported edge.
Legacy refreshes and workflows retain their existing behavior.

Dry runs are offline: they describe the bounded RPC sequence and explicitly do
not claim to have validated live generations or cadence. Actual receipt-enforced
refreshes use ordinary `REFRESH`, holding the mart read-blocking lock until
commit, even when the legacy CLI defaults to concurrent refreshes.

The database plan pins the source generation, expected current mart generation,
source publication interval and fixed input boundary. It requires a current
complete successful or evaluated `expected_no_data` source receipt, a declared
positive source cadence, and an age inside that interval. A later failed,
partial, deferred or blocked source attempt blocks this strict plan even if an
older valid publication is still readable. The diagnostic freshness API can
continue showing that older publication; publication admission is stricter.

Only the house Elo source boundary is proven. `core.games` remains unversioned;
this mode neither bypasses a required registered edge nor claims full provider
freshness. An explicit refresh can create a new mart generation against the same
source generation, but it retains that exact edge and cannot renew the source's
publication age. Unknown source cadence blocks execution instead of inventing
an SLA.

## Atomic protocol and concurrency

1. Read the eligible plan through the bounded read RPC.
2. Start an operation with the exact plan and commit its parent record.
3. In a fresh READ COMMITTED transaction, invoke the publication RPC. It checks
   session ownership and scope, pins both source tables and registered assets,
   locks the declared policy and current source pointer, and prevents concurrent
   source-failure receipt insertion during admission/publication.
4. Revalidate exact generations, policy, source completion, latest outcome,
   source row coverage and installed invalidation guards. Refresh the mart,
   recheck its registered OID and row coverage, and check that the source has not
   aged past its interval while waiting or refreshing.
5. Commit the refreshed mart, successful receipt, exact input edge, new mart
   pointer and successful operation outcome together. The source generation and
   timestamp do not advance.

The lock order is operation -> both source tables -> ordered asset rows ->
source policy -> receipt table -> source pointer -> ordinary mart refresh.
Source and snapshot DML serialize with this path; a legacy write after commit
invalidates the receipts normally. The mart pointer is not locked before
REFRESH, avoiding a lock cycle with a legacy refresh's event-trigger deletion.
The receipt-table SHARE lock is intentionally conservative because the earlier
failure writer does not lock asset rows. This may delay other receipt writers;
production contention and lock duration are unmeasured.

Stale plans or invalid inputs cannot advance data or success pointers. A known
failure rolls back the refresh, then records a failed or blocked mart receipt
and terminal operation outcome in a separate transaction; it never records a
source failure on behalf of the source producer. Exact publication replay is an
immutable lookup, not a pointer update. Terminal runs cannot publish a second
generation. An uncertain start/publication commit logs both IDs and stops;
there is no automatic retry or contradictory failure receipt.

## Access and rollout

Migration 070 adds the owner-controlled `warehouse_refresh` routines namespace
and a bounded `warehouse_refresher` NOLOGIN role. Only its four public protocol
RPCs are executable by that role. The internal run validator, generic operation
helpers, direct target/ledger mutation and consumer access remain excluded.
There are no new consumer RPCs or changes to the F19 public response.

Append-only receipts and operation semantics from 066–069 are reused without
rewriting those migrations. The new migration follows 069 in the managed
bootstrap and production manifests. Runtime login membership and an appropriate
source cadence must be separately configured before activation. No scheduled
workflow is switched to the new mode by this change.

Migration application rejects preexisting memberships in either direction for
`warehouse_refresher`, including runtime logins already granted the role. A direct
reapplication after activation therefore requires membership to be removed first;
the managed runner normally skips an already recorded migration.

The existing trusted-administrator boundary still applies: administrators must
clear current pointers before disabling publication event guards. This protocol
checks guard presence and refuses new publication when it cannot trust them;
it cannot defend against an administrator rewriting system catalogs.

## Verification

The final combined local regression suite passed **157 tests** in 20.29s:
27 executed generation-enforcement SQL cases, 33 existing publication cases,
17 freshness cases, two fresh/upgrade bootstrap cases, and 78 planner/adapter/
migration unit checks. All four affected Python files passed Ruff lint and
format checks; `git diff --check` was clean.

The SQL cases ran on explicitly selected disposable PostgreSQL 17 databases.
They exercised the real Python adapter, exact plan/edge evidence, atomic
rollback, no-data coverage, all unsuccessful source outcomes, stale and changed
policies, concurrent compare-and-swap, replay, source/snapshot writes and source
failure receipts queued during publication, expiry while blocked on a mart,
invalid guards/OIDs, actual caller roles, reapplication, and session ownership.

Independent subagent reviews of code not authored by each reviewer found no
remaining material issues after fixing receipt-writer serialization, direct
read/mutation and unrelated RPC grants, and unexpected overload admission.
The final role check also rejects PostgreSQL 17's direct `MAINTAIN` privilege,
which can authorize [materialized-view refresh](https://www.postgresql.org/docs/17/sql-refreshmaterializedview.html)
without ownership; a separate executed regression covers it.

No production SQL, CFBD request, runtime membership, cadence declaration or
workflow activation was performed. Production lock duration, contention and
managed-platform privilege parity remain unmeasured.
