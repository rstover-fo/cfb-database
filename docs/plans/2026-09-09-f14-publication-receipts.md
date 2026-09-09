# F14/F19: controlled house Elo publication

Status: prepared implementation; production migration and workflow activation require a
separate rollout. This is the first publication slice after the operational quota ledger.

## Scope and meaning

`compute_house_elo.py --full --publish-receipts` publishes the complete
`analytics.house_elo_game` output and refreshes its sole-input mart,
`marts.house_elo_game`. It also replaces `analytics.house_elo_current` in that
transaction to preserve the full-run snapshot side effect. The current snapshot
has no separate receipt in this slice. Elo parameters, game selection, ordering,
season carryover and schedule-based pooling remain unchanged.

A generation identifies one publication, not a mutable table version. The private
`meta.asset_receipts` ledger is append-only. `meta.asset_current_generations`
selects at most one successful, complete generation for each asset and
`source-wide` coverage. `meta.asset_receipt_inputs` records the mart's exact source
generation. Stable asset keys match the F28 refresh registry.

The source receipt records an observation of all `core.games` rows, including
unfinished games that affect pooling, and the reviewed exclusions. This input
has no generation receipt yet: the digest is unversioned provenance, not proof
that the upstream warehouse is fresh or that the provider's history is complete.
Coverage means the complete eligible input currently present in this warehouse.
An empty eligible completed-game set can publish `expected_no_data`; a transport
failure or partially computed result cannot stand in for an empty result.
Unlike the legacy writer's silent filtering, this mode rejects eligible games
marked completed without final scores or team names: those rows require input
repair before complete publication can be claimed.
If a superseded original is present, its reviewed replacement must also be
present with matching, non-NULL season and team identities. The publisher
validates these pairs under the input lock before excluding originals and
claiming complete coverage.

`published_at` is a clock observation inside the publishing transaction, visible
only when that transaction commits. It is not the exact commit time.
`committed_at` remains NULL. Data and current-generation evidence must be read in
one statement or a shared transaction snapshot.

## Transaction protocol

1. Start and commit a `compute` operation run; log the run and both generation IDs.
2. Read the max season, current generations, input digest, completed games and
   full schedule counts in one repeatable-read snapshot. End that transaction.
3. Compute the existing Elo engine output in memory.
4. In a fresh READ COMMITTED transaction, call the bounded publication RPC. It
   locks and revalidates inputs and expected current generations, checks full
   coverage and payload grain, replaces the game and current tables, refreshes
   the mart, and inserts receipts, dependency evidence and current pointers.
   Finish the operation successfully in this same transaction, then commit.
5. A known failure rolls back publication before attempting to record failure
   evidence and finish the operation as failed. Failure, deferred, partial and
   blocked receipts never become current. Failure evidence is best effort when
   the database itself is unavailable.

A competing publisher or a source correction during preparation causes validation
to fail; it cannot publish against the stale prepared observation. Matching RPC
replay returns the original immutable receipt without making it current again.
A conflicting replay is rejected. The CLI does not retry automatically after an
uncertain commit: inspect the logged receipt IDs and operation before retrying.
No contradictory failure receipt is written after a lost commit acknowledgement.

For commit recovery, use an authorized owner connection to look up the three
IDs from the CLI log. A receipt's presence proves the publishing transaction
committed; an old receipt need not still be current. An absent current pointer
means no current evidence, not an empty asset. Check receipt absence only after
the original database session has ended, so an in-flight commit cannot be
mistaken for a rollback.

```sql
SELECT generation_id, operation_run_id, asset_key, outcome, published_at
FROM meta.asset_receipts
WHERE generation_id IN ('<source-generation-uuid>', '<mart-generation-uuid>');

SELECT operation_run_id, outcome, finished_at
FROM meta.operation_runs
WHERE operation_run_id = '<run-uuid>';
```

Existing full/incremental calls and direct source writes remain possible. Game
or current-snapshot DML invalidates both pointers; relevant DDL also invalidates
evidence, and later mart refreshes invalidate the mart pointer. The publisher
verifies its guards before publishing. Thus a legacy mutation cannot preserve
an old pointer as evidence for new contents. Historical receipts and their
dependency edges remain available after invalidation.

The guards protect ordinary SQL operations while installed and enabled. A
trusted administrator can bypass the protocol: PostgreSQL does not fire event
triggers for changes to event triggers themselves (see the
[event-trigger semantics](https://www.postgresql.org/docs/17/event-trigger-definition.html)).
Before disabling or dropping
these guards, an administrator must explicitly clear the current pointers;
restore and verify the guards before publishing again. The publisher refuses
to create a new generation when required guards are absent or disabled. This
does not make historical pointers trustworthy after an administrative bypass.

## Access and rollout

Migration `068_asset_publication_receipts.sql` follows 066/067 in the warehouse
and production manifests. It adds private ledger tables and the owner-controlled
`warehouse_publication` namespace. The bounded `warehouse_publisher` role has RPC
execution and no direct ledger/target DML privileges. The migration does not
grant login membership or change any public API, freshness response, RLS policy,
mart definition or workflow activation.

Operation lifecycle access goes through `start_house_elo_run(uuid)` and
`finish_house_elo_run(uuid, outcome)` in `warehouse_publication`. The start
wrapper fixes the compute kind, initiator and full-history asset scope; both
wrappers enforce the calling session's ownership of the run. The runtime role
cannot call the generic quota operation helpers or finish unrelated runs.

Before a production rollout, verify event-trigger privileges on the actual
platform, review the migration and role ownership, then separately authorize
application and runtime membership. The compute connection still requires read
access to `core.games`, plus permission to assume `warehouse_publisher`. Use a
controlled full run to establish the first receipts. Full-history publication
holds locks and refreshes the mart in one transaction; production lock duration,
memory use and latency have not been benchmarked.

Supabase documents event-trigger creation by its `postgres` user through
Supautils; this implementation has been designed around that capability but
still requires a target-specific rollout check. See the
[Supabase event-trigger guide](https://supabase.com/docs/guides/database/postgres/event-triggers).

After publication, inspect both receipts, their pointer IDs and the mart input
edge in a shared snapshot, plus operation outcome and representative data. Check
legacy invalidation and failure behavior under actual caller roles before any
workflow activation. Existing daily incremental computation and general mart
refreshes will invalidate receipt pointers, so scheduling this mode requires a
separate operational decision.

## Verification

`tests/test_house_elo_publication.py` covers engine/snapshot parity, transaction
ordering, empty input, partial-mode rejection and commit uncertainty.
`tests/test_asset_publication_sql.py` executes the migration and publication
protocol against explicitly selected disposable loopback PostgreSQL. The normal
bootstrap suite checks both fresh and upgrade manifest paths. These tests do not
constitute a production deployment or a provider-coverage audit.
