# F14 durable quota foundation

Status: implemented and verified on disposable PostgreSQL 17; not deployed. This is the first schema
slice of the [accepted operational-records design](https://github.com/rstover-fo/cfb-database/pull/137).
It does not enable admission enforcement in existing HTTP callers.

## Accepted direction

Production callers will fail closed if the shared admission database is
unavailable. A local development bypass, if added later, must be explicit.
Freshness consumers retain their existing interface while publication receipts
are developed separately. Quota records land first; a controlled load and a
derived mart will prove atomic receipt publication in a subsequent PR.

## Scope

Migration `066_operational_quota_ledger.sql` adds a private operation-run parent,
configured quota periods, per-attempt reservations, and restricted SQL entry
points. An account key names an opaque shared CFBD/CBBD pool, never an API key.
The database serializes reservations against a configured period and refuses
requests when capacity is exhausted or the period is unavailable. Every
committed reservation remains charged locally, including ambiguous transport
outcomes and a process crash. Recording a failed result never refunds it.

The reservation UUID identifies one admission attempt. Repeating the database
reservation with the same UUID and exact context is safe after an uncertain
SQL response; it is not permission to issue another HTTP request. Each actual
HTTP retry must use a new attempt UUID. A dispatch transition will prevent
reusing a completed reservation for a new transport attempt. Callers must
commit admission/dispatch transactions before sending HTTP bytes; this schema
alone cannot prove client ordering or exactly-once external effects.

No period, cap, role membership, credential, or runtime invocation is seeded.
An administrator explicitly configures the period bounds and local allowance.
The restricted caller cannot create its own budget or update counters directly.
There is no automatic calendar rollover and no fallback to JSON admission.
Existing JSON admission behavior remains in place until the transport PR.

## Database interface

The dedicated `warehouse_ingest` role is NOLOGIN and receives schema usage
plus execution on five private functions. An operator later chooses the login
membership; no existing application role is automatically enrolled.

| Function | Contract |
|---|---|
| `meta.start_operation_run` | Creates an invocation UUID, or returns it for an exact-context replay. |
| `meta.finish_operation_run` | Closes a run; a successful finish cannot leave pending attempts. |
| `meta.reserve_cfbd_attempt` | Locks the explicit account period, reserves one local unit, and rejects UUID/context conflicts. |
| `meta.mark_cfbd_attempt_dispatched` | Allows only one reserved-to-dispatched transition, while the period and run remain active. |
| `meta.record_cfbd_attempt_result` | Records a terminal transport result; an exact replay is harmless and a conflicting result is rejected. |

`btree_gist` enforces non-overlapping `[start,end)` periods for each shared
account. The migration discovers the extension's installed schema for its
operator class; pre-existing installations are retained. New installation uses
an explicit schema so both an empty and normal session search path work.

Successful and expected-empty transport results require 2xx status. A 404 is
recorded as an HTTP error at this layer, even if a future source adapter treats
it as expected missing data. That source outcome and publication coverage are
separate from transport status.

The migration removes non-owner ACLs on only the new tables/functions, including
implicit PUBLIC function execution and arbitrary host-default grantees, then
grants the five entry points to the runtime role. Schema usage for existing
ledgers and named writers' schema privileges are preserved. PUBLIC/runtime
schema creation is denied. No existing public data surface is recreated.

## Provider evidence and initial allowance

The official CFBD source at revision
[`0d5559e337966cba08005feceac4f483209329f9`](https://github.com/CFBD/cfb-api-v2/tree/0d5559e337966cba08005feceac4f483209329f9)
reports the next reset as the first of the next month at 00:00 UTC.
[`/info`](https://github.com/CFBD/cfb-api-v2/blob/0d5559e337966cba08005feceac4f483209329f9/src/app/info/service.ts)
returns remaining capacity and the shared CFB/CBB-pool flag.
[`/info/usage`](https://github.com/CFBD/cfb-api-v2/blob/0d5559e337966cba08005feceac4f483209329f9/src/app/info/controller.ts)
is a trailing request-metrics window, not the billing counter.

The [quota middleware](https://github.com/CFBD/cfb-api-v2/blob/0d5559e337966cba08005feceac4f483209329f9/src/config/middleware/quotas.ts)
refunds non-2xx responses and exempts some endpoints. Our reserved-attempt count
is intentionally conservative; it must never be labeled provider-billed usage.
Before adoption, verify live account capacity and configure at most its
remaining allowance minus headroom for other callers. Starting midmonth with
a full monthly cap would permit overspending. All callers sharing the pool
must participate for strict local coordination. External callers can consume
provider capacity independently, so reconciliation remains necessary.

No authenticated provider request was made for this work. The pinned source
is evidence of implementation intent, not measured deployment parity or the
current account's remaining quota.

## Rollout boundary

The migration is appended to both the disposable bootstrap manifest and the
separately adopted production manifest. Historical entries and baseline bytes
are preserved. Intended application is the managed upgrade path; merging this
branch is not production application. Review the pending migration plan before
an explicitly authorized rollout. Existing public freshness RPCs, API views,
raw data, prediction provenance, and private scouting contracts are unchanged.

After schema rollout, a separate transport integration PR must create runs,
commit a reservation for each attempted send, persist dispatch state, and
record terminal outcomes. It must test database-unavailable denial, fresh
processes sharing capacity, HTTP retries, and crashes at each boundary before
retiring JSON admission for a caller. No account-wide enforcement claim is made
by the schema-only PR.

## Executed verification

- 11 quota SQL cases passed on an isolated local PostgreSQL 17 cluster. Tests
  use actual `warehouse_ingest` and public/bystander roles, ordinary and broad
  default grants, exact/conflicting concurrent UUID reuse, separate operation
  runs competing for one cap, committed-crash versus rollback accounting,
  expiry after an observed lock wait, result validation, and reapplication
  with populated records. Eight independent runs competing for a cap of three
  produced exactly three reservations and five quota refusals.
- Both full managed bootstrap and prior-baseline upgrade passed, including
  catalog shape, unchanged representative data, public caller access, private
  quota access denial, restored session settings, and an idempotent second run.
  The combined executed SQL suite passed all 13 cases.
- 77 migration-runner/CLI unit tests passed. Ruff lint/format, actionlint for
  the changed CI workflow, and diff checks passed.
- Independent review found and resolved implicit PUBLIC function execution,
  arbitrary host-default grants, and NULL/non-2xx success classification. The
  full bootstrap test also caught and resolved extension creation under an
  empty search path. Final review reported no material findings.

No production SQL, runtime provider admission, authenticated CFBD request, or
performance benchmark was run. These tests do not prove deployed Supabase
permissions or HTTP commit/send ordering; those remain rollout and transport
integration checks.
