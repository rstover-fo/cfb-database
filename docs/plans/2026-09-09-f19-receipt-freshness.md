# F19: receipt-backed freshness compatibility

Status: prepared implementation, not a production rollout. This is delivery
step 5 after the controlled house Elo publication receipts in migration 068.

## Scope and compatibility

Migration `069_receipt_backed_freshness.sql` adds a query-time receipt surface
for exactly `analytics.house_elo_game` and `marts.house_elo_game`, each at
`source-wide` grain. `public.get_asset_freshness()` exposes a safe projection
through the ordinary owner-rights `marts.asset_receipt_freshness` view.

The existing `public.get_data_freshness()` six-column RPC and its underlying
`marts.data_freshness` materialized view remain compatible. The app's dashboard,
sidebar and MCP mappings, and this repository's MCP tool, consume that older
shape. Its vacuum/analyze statistics are maintenance heuristics; neither the
old RPC nor its 24 tracked tables acquires receipt evidence through this change.
Consumers can adopt the new RPC separately without a breaking cutover.

## Evidence semantics

Only an exact current pointer joined to a complete successful or evaluated
`expected_no_data` receipt establishes a current publication. Historical
successes are not substituted when a pointer is absent. A later failed,
partial, blocked or deferred attempt remains separate diagnostic evidence and
does not replace the current generation. No receipt is distinct from no data.

Publication age uses the start of the current SQL statement, so it advances
between queries, including within a long-running transaction, without refreshing
any materialized view. Receipt timestamps describe an observation inside the
publishing transaction, not its exact commit time. Transaction isolation still
determines which committed generation the statement can observe.

No publication cadence has been declared for this opt-in producer. Initial
expected intervals and age-based stale results are therefore NULL. An owner can
set a positive interval in the private policy table during a reviewed rollout;
reapplying the migration preserves that policy. A missing pointer has no age and
no age-based stale result. Publication state and dependency status convey that
absence independently.

The mart records the exact house Elo source generation it consumed. Its
recorded-input comparison can establish whether that generation is still
current, or identify a missing/mismatched dependency. The source has no
versioned input edge. Both assets ultimately depend on unversioned `core.games`,
so full upstream closure remains unknown even when the mart's recorded edge
matches. The `source_watermark` field in this slice is an opaque input digest,
not a provider timestamp or a CFBD generation. Coverage describes the observed
warehouse input; it does not prove provider completeness or a requested season's
expected game set.

## Verification and access

`verify_load.py` reads the additive receipt surface after its existing legacy
freshness check. Missing migration or as-yet-unrecorded optional publications
warn without activating a new production publishing requirement. Known broken
recorded dependencies fail; an explicitly declared stale interval follows the
existing in-season/strict severity rule. Unknown cadence and unversioned input
closure remain visible rather than becoming a freshness PASS. Recent failed
attempts remain diagnostic even while an older current publication is readable.

Only the safe public projection is granted to `anon` and `authenticated`.
Private receipt, operation and policy tables remain unavailable to consumers;
raw operation scopes, input observations and arbitrary error text are excluded.
The migration follows 068 in the managed bootstrap and production manifests.
No runtime memberships, publication workflows or production policy values are
activated by this development task.

## Executed validation

The final local warehouse regression run passed 164 tests: 68 verifier tests,
17 executed receipt-freshness cases, 33 controlled-publication cases, 40 managed
migration unit cases, two fresh/upgrade bootstrap cases, and four verification
transaction cases. The three legacy freshness MCP tests passed in their separate
test invocation. Ruff lint and format checks passed for all four affected Python
files; `git diff --check` was clean.

The SQL cases used explicitly selected disposable PostgreSQL 17 databases,
including actual anon/authenticated queries, private-table and DML denials,
reapplication, age advancing within a transaction, empty coverage, failure
recovery, missing/mismatched input evidence, and hostile precreated policy-table
rejection before any attacker trigger executes. The old RPC retained its exact
six-column result, definition and callable access. The real verifier also read
the new RPC and failed a deliberately broken recorded dependency.

Independent subagent review found and resolved two issues: ownership must be
validated before inserting into a possibly precreated policy table, and the
verifier must independently honor unversioned inputs even if a closure flag is
inconsistent. Both have regression coverage.

No production migration, caller-role rollout, CFBD request or cadence activation
was performed. Query performance and receipt-volume costs on representative
production history are unmeasured. Fresh/upgrade catalog tests prove the local
PostgreSQL contract, not managed-platform deployment parity.
