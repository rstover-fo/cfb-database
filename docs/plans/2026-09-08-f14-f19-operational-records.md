# F14/F19 operational records and durable quota design

## Status and scope

**Proposal only.** This document defines a staged design for F14 and F19. It
does not add a migration, change a workflow, write warehouse records, or make
a production claim. Each implementation step needs its own review and rollout
decision.

The design makes two operational claims verifiable:

1. CFBD call admission accounts for actual transport attempts across all jobs
   using the same provider account and billing period.
2. Freshness describes a successfully committed source/load/compute result and
   its coverage, rather than PostgreSQL maintenance activity or a cached age.

Quota attempt accounting and publication receipts must be separate. The first
answers how much provider capacity was consumed. The second answers which
warehouse asset generation is currently valid. Their grains, failure semantics,
and retention needs differ.

This proposal depends on the F10/F11/F28 foundation but does not expand its
present scope. The foundation currently being implemented is limited to a
static registry for 55 SQL materialized views and 48 external relation inputs,
plus deterministic `--changed` descendant selection and failure gating. It
does not define source/job metadata, work-unit coverage, durable generations,
or operational receipts. This design uses its stable asset keys and dependency
edges when available; it does not require the foundation to solve the broader
ingestion registry first.

## Accepted direction — 2026-09-08

The user accepted these rollout choices:

- Production CFBD admission fails closed when the shared quota database is
  unavailable. A future local-development bypass must be explicit; there is
  no silent fallback to the JSON counter.
- Preserve the existing freshness consumer interface while receipt-backed
  freshness is introduced alongside it and verified before consumer cutover.
- Deliver quota accounting first, then prove publication receipts for one
  controlled load and one derived mart before expanding.

PRs #135 and #136 are merged. The SQL registry foundation is available; durable
quota enforcement and publication receipts are still implementation work.
Production migration application is a separate rollout decision.

## Current evidence and boundaries

### Existing records are useful but not interchangeable

`meta.refresh_progress` is a historical-correction campaign ledger. Its grain
is `(campaign, task, game_id)` and it records a completed logical game task as
`refreshed` or `no_data`. The `calls` field and month-sum guard are specific
to that campaign's bounded, one-call-per-game model. Retries, separate jobs,
generic assets, and downstream publication generations cannot be represented
without breaking its resumability semantics.

`meta.flat_file_loads` is a content-hash ledger at
`(source, file_sha256)` grain. Its `loaded`, `skipped`, and `failed`
statuses correctly support hash-skip loading. A hash skip with no error means
current bytes were checked; a stale-snapshot skip does not. That distinction
must remain local to the flat-file contract.

`RequestOutcomeTracker` captures useful typed logical outcomes for one
resource invocation, but it is in memory and intentionally counts fetched
logical requests rather than individual retry attempts.

Prediction and model tables already hold immutable domain provenance,
including prediction fit/input evidence and immutable training fits.
Operational receipts must not replace or weaken those contracts. A later,
explicit compatibility decision may link a prediction publication to an asset
generation.

### Provider contract evidence

The official CFBD repository at revision
[`0d5559e`](https://github.com/CFBD/cfb-api-v2/tree/0d5559e337966cba08005feceac4f483209329f9)
provides these implementation contracts:

- [`/info` service](https://github.com/CFBD/cfb-api-v2/blob/0d5559e337966cba08005feceac4f483209329f9/src/app/info/service.ts):
  `resetAt` is the first of the next month at 00:00 UTC. `monthlyLimit`,
  `remainingCalls`, `usedCalls`, `sharedPool`, and `products` describe capacity.
  Admin counters may be null. The account's live values were not queried.
- [`quota middleware`](https://github.com/CFBD/cfb-api-v2/blob/0d5559e337966cba08005feceac4f483209329f9/src/config/middleware/quotas.ts):
  non-2xx responses are refunded, and `/info`, `/info/usage`, `/scoreboard`,
  and `/auth/graphql` are excluded from metering. A local reserved attempt is
  therefore a conservative admission unit, not a claimed provider charge.
- [`usage controller/service`](https://github.com/CFBD/cfb-api-v2/blob/0d5559e337966cba08005feceac4f483209329f9/src/app/info/controller.ts):
  `/info/usage` reports trailing request metrics (default 7 days, 1–31 days;
  row limit 1–50), not an authoritative monthly billing total. Use `/info`
  for capacity reconciliation and retain request metrics separately.

The first schema slice uses explicitly configured period bounds and local
capacity, with no automatic month reset. Before runtime adoption, verify the
live account's remaining capacity, reserve headroom for other callers, and
configure only the remaining allowance. Starting midmonth with a full monthly
cap would be unsafe. Strict shared enforcement requires every relevant caller
to participate; external callers can still consume the same provider pool.

### Current gaps

`make_request` checks a process-local JSON rate limiter, runs the whole
retrying HTTP call, then records one call only when the request returns. A
failed 404, exhausted 429 retries, or exhausted transient retries leaves
transport attempts without durable accounting. These attempts are not
necessarily provider-billed calls: the current official middleware refunds
non-2xx responses. Our local admission count deliberately remains conservative. The limiter state is not shared
by a new CI runner or another job.

The current `marts.data_freshness` materialized view uses PostgreSQL
vacuum/analyze activity as a loading proxy. Maintenance can happen without new
source data and a committed load does not require immediate maintenance.
Because age and stale status are materialized, they stop aging if refreshes
stop. The verifier mixes table-specific checks, flat-file ledger checks, and
this maintenance proxy; it has no common current-generation closure.

## Proposed records

### Operation runs

Add a narrow `meta.operation_runs` audit parent. One row represents an
invocation such as a CFBD extraction, flat-file import, compute stage, mart
refresh, or verification command.

| Field | Meaning |
|---|---|
| `operation_run_id` | UUID created before the invocation; primary key. |
| `operation_kind` | Controlled value such as `extract`, `load`, `compute`, `refresh`, or `verify`. |
| `initiator` | Scheduler/CLI identity, never a credential. |
| `requested_scope` | Selected sources/assets/years. |
| `plan_digest` | Digest of the deterministic selected-work plan where available. |
| `code_revision` | Optional repository revision for diagnosis, not freshness by itself. |
| `started_at`, `finished_at` | UTC lifecycle timestamps. |
| `outcome` | `running`, `succeeded`, `failed`, `partial`, `blocked`, or `cancelled`. |
| `error_summary` | Sanitized terminal error context, never secrets. |

An operation run is not a successful publication claim. It can have failures,
partial results, or no affected assets.

### Asset publication receipts

Add append-only `meta.asset_receipts`, with a generated immutable
`generation_id` per recorded asset result. A receipt is linked to an operation
run and to a stable F28 asset key.

| Field | Meaning |
|---|---|
| `generation_id` | Immutable identifier for this asset result. |
| `operation_run_id`, `asset_key` | Causal run and registry asset identity. |
| `coverage_key` | Canonical work-unit scope: season, week/season type, game id, or source-wide. |
| `outcome` | `succeeded`, `expected_no_data`, `failed`, `deferred`, `partial`, or `blocked`. |
| `coverage` | Planned, completed, deferred, and failed units with explicit completeness. |
| `source_watermark` | Provider/version/source watermark when one exists; opaque rather than invented. |
| `row_delta` | Insert/update/delete/count measurements when known. |
| `input_generations` | Required input asset generations used for a compute or refresh. |
| `committed_at` | Verified database commit time when available; otherwise null, with separately named observation time. |
| `published_at` | Set only when the result becomes the current valid generation. |
| `error_summary` | Sanitized error classification/details for non-success outcomes. |

The current-generation query must choose only a fully committed and complete
`succeeded` or complete `expected_no_data` receipt. A partial, failed,
deferred, or blocked result leaves the previous valid generation current and
is visible as a problem state. A correction creates a new receipt/generation
for the same asset and coverage; it never overwrites earlier proof.

An empty successful response is not automatically fresh. It is
`expected_no_data` only when registry-defined expected coverage has been
evaluated and completed. A zero-row expected-no-data receipt must remain
distinct from a provider failure and from an unattempted unit.

Successful receipts and current-generation pointers must become visible
atomically with the corresponding target data. For a directly controlled
transaction, insert the success receipt and update the pointer in the same
transaction as the target writes; none is visible until commit. Alternatively,
write staged/versioned outputs and atomically publish their pointer and receipt.
A separate post-commit receipt is insufficient: a crash in between would leave
new data visible while the old receipt still claimed to describe it. A dlt load
that commits independently therefore needs an adapter or staged publication
boundary; a callback after `pipeline.run` cannot establish that guarantee.
Failed-attempt receipts may be written separately after rollback, but cannot
advance the publication pointer. Exact database commit timestamps may be
unavailable; timestamp fields must document their observed semantics rather
than claim a pre-commit clock value is the exact commit time.

### Durable quota attempt accounting

Add separate quota tables, for example `meta.api_quota_periods` and
`meta.api_request_attempts`.

`api_quota_periods` is keyed by opaque provider-account identifier and
billing-period start. It records the configured local limit,
provider-observed usage when reconciled, and reconciliation metadata. It must
never expose a token, authorization header, or reversible token fingerprint.

`api_request_attempts` has one row per admitted HTTP transport attempt,
linked to an operation run and, when known, asset/work-unit identity. It
records reservation, dispatch, and result timestamps; endpoint classification;
retry ordinal; status/error category; and HTTP status where present.

Admission is a database function such as `reserve_cfbd_attempt(...)`:

1. Resolve the provider account and current provider billing period.
2. Atomically lock or update that account-period budget.
3. Refuse a reservation that would exceed the configured limit.
4. Insert and commit one attempt reservation before HTTP bytes are sent.
5. Return the attempt identifier to the HTTP client.

The callback belongs at the actual `_client.get` transport boundary. Each 429
or transient retry obtains a new reservation. A circuit-breaker refusal before
transport creates no attempt. A quota refusal creates no HTTP request. The old
local `RateLimiter` may remain temporarily as a display cache, but cannot
remain the authoritative admission decision.

There is no transaction spanning Postgres and CFBD. If a process crashes after
reservation commit, or a timeout leaves it unclear whether CFBD received the
request, the reservation remains charged for the period and is marked
`unknown` or `transport_error` when possible. It must not be automatically
released. This conservative rule prevents account-wide overspend and makes
uncertainty inspectable. Provider reconciliation records variance against the
local attempt count; it does not silently rewrite immutable attempt history.

## Freshness surface

Replace the claim made by the data-freshness materialized view with a
query-time receipt-backed view/RPC. Its age uses current query time and the
current complete receipt's `published_at` or source watermark, so it ages
while jobs are stopped. It should expose:

- asset identity and declared expected cadence/coverage;
- current generation and publication time;
- age and stale state calculated at query time;
- completion/partial/blocked/error state and last failed operation;
- required input generations and whether their closure is current;
- source watermark and row-delta evidence where applicable.

Keep `pg_stat` vacuum/analyze information as separate maintenance telemetry.
It may help diagnosis but cannot prove source freshness. Keep the existing
`get_data_freshness()` contract temporarily through an additive compatibility
layer until consumers are reviewed; a versioned RPC may be safer if generation
and coverage fields change consumer expectations.

`verify_load` should migrate incrementally to registry-declared coverage and
receipt closure. Prediction verification must prove the expected game set and
input-generation closure, not merely that historic prediction rows exist.

## Relationship to F10/F11/F28

The current F28 registry foundation supplies only materialized-view and
external-relation asset identities/dependencies. That is enough for an initial
compute/mart receipt slice and deterministic descendant gating. It is not yet
enough to declare extraction cadence, source work-unit grain, expected coverage,
cost, or watermarks for pipeline sources.

F11 should consume receipts to prevent publication of a descendant when a
required upstream receipt is failed, incomplete, or stale for the selected
plan. A downstream receipt records the exact input generation closure it used.
Existing F10 workflow dependencies remain useful sequencing safeguards, but
workflow start/order is not generation proof.

Expand the registry only as a follow-up: source asset metadata must name its
coverage grain, correction policy, expected-no-data policy, watermark rule,
and CFBD cost/admission account. Do not delay the current SQL-only registry
foundation to solve this broader catalog.

## Proposed delivery sequence

1. **Registry foundation (in progress):** land the bounded F28
   materialized-view/external-relation registry, deterministic `--changed`
   planning, and failure gating. No receipts, source metadata, or durable
   generations are claimed.
2. **Quota schema and admission primitive:** add the shared operation-run
   parent, quota period/attempt tables, atomic reservation function, grants,
   and database-fixture tests. No callers
   change yet.
3. **CFBD transport vertical slice:** create operation runs and route one
   daily, one historical-refresh, and one live caller through per-attempt
   reservations. Retire JSON admission control for those paths only.
4. **Receipt schema plus controlled publication slice:** add asset
   receipts referencing the existing operation-run parent, and record one
   directly controlled source/load plus one derived
   F28 materialized-view refresh. Prove atomic visibility of target data, publication pointer, and success receipt.
5. **Receipt-backed freshness compatibility layer:** add query-time freshness,
   migrate selected verifier checks, and preserve/review the public contract.
6. **Dependency-generation enforcement:** make the F11 planner require
   complete current inputs and record descendant input-generation closure.
7. **Incremental migration:** register source coverage/correction metadata and
   adapt remaining dlt, flat-file, compute, and prediction-verification paths.

Steps 2 and 4 share database primitives but should remain separate PRs so
quota-safety review is not coupled to a broad freshness API change. Step 3 can
start after step 2; step 4 requires registry asset keys. Steps 5 and 6 are
stacked on receipts and should merge in dependency order.

## Acceptance criteria

### Quota accounting

- A terminal 404 records one durable attempt; exhausted 429 and transient
  sequences record every attempted request.
- Concurrent workers sharing one account-period cannot reserve above the cap;
  a denied reservation performs no HTTP call.
- A fresh runner reads prior usage, and a long-lived process crossing the
  provider billing boundary reserves against the new period.
- A crash after reservation and before an observable response remains visibly
  charged/unknown. Reconciliation records provider/local variance separately.
- Attempt records distinguish logical expected-no-data from transport outcome;
  retry count is not inferred from one source-level success record.

### Receipts and freshness

- A complete zero-row expected-no-data result, error, deferred unit, and
  incomplete coverage produce distinct receipts and freshness states.
- A failed or partial run does not replace the previous current generation and
  blocks required descendants.
- A corrected unit creates a later generation and selects declared descendants.
- With a fake advancing clock and no receipt changes, a healthy asset becomes
  stale without refreshing a materialized freshness view.
- Running `ANALYZE` alone cannot make old source data fresh.
- Compute/prediction verification asserts expected game coverage and exact
  required input generations.
- Inject crashes around target commit and receipt publication. A controlled
  transaction exposes either old data with its old receipt or new data with
  its new receipt, never new data labeled with an old generation. Staged output
  stays unpublished until its pointer and receipt commit together.
- A dlt adapter must prove the same visibility invariant; independent target
  commits followed by a receipt callback remain explicitly unsupported.

## Open decisions and rollout gates

1. Confirm live account capacity before runtime adoption. The pinned official
   source above resolves UTC reset reporting and the distinction between
   `/info` capacity and `/info/usage` request metrics; live deployment parity
   and current account values remain unverified.
2. Implement the accepted fail-closed production policy. Any local development
   bypass must be explicit and never silently use JSON admission.
3. Define canonical coverage encodings per source: source-wide, season,
   `(season, season_type, week)`, game, or another provider-grain key.
4. Decide the dlt publication boundary before asserting exact load receipts; a
   post-run callback alone cannot close a crash gap after an independent commit.
5. Implement the accepted compatibility-first freshness rollout. Review
   consumers before cutover, preserve owner rights, and restore any grants
   after recreation.
6. Measure attempt/receipt volume before finalizing retention and indexes.

## Verification limits

This proposal is based on repository inspection only. No live CFBD usage
endpoint was called, no warehouse migration was applied, and no production
provider/database behavior was measured. Database fixture and caller-role
tests are required before any SQL rollout claim.
