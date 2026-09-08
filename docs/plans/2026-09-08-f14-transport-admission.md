# F14 CFBD transport admission

## Prepared implementation and rollout boundary

This implements the next slice of the accepted
[operational-records design](2026-09-08-f14-f19-operational-records.md).
Migration 067 follows immutable migration 066 in both managed manifests.
Neither merging this change nor running tests applies production SQL, creates
budgets or role memberships, or activates workflow admission.

The enrolled command entrypoints are `scripts/load_season.py` (daily),
`scripts/backfill_refresh.py` (historical), and `scripts/poll_scoreboard.py`
(live). They create one operation spanning their work when
`CFBD_QUOTA_MODE=durable`. The deployment default remains `legacy` until the
separate rollout is authorized. Legacy preserves current behavior; it is not
an automatic fallback from durable mode. Invalid modes fail explicitly.
Unenrolled CFBD callers in a process configured for durable mode refuse
transport instead of silently bypassing admission.

## Configuration and activation

After an authorized managed upgrade through 067, configure:

- `CFBD_QUOTA_MODE=durable` for each enrolled command's environment.
- `CFBD_QUOTA_DB_URL`, a dedicated database connection whose login can assume
  `warehouse_ingest`. The adapter explicitly sets this bounded role for every
  transaction; it does not reuse the dlt load transaction or auto-grant access.
- `CFBD_QUOTA_ACCOUNT`, the reviewed opaque identifier for the shared provider
  pool. It is never a token, token hash, or credential fingerprint.

An administrator must configure finite, non-overlapping windows per account
and budget class in `meta.api_quota_periods`. `extraction` remains the default
for existing rows and preserves their history. `reconciliation` has independent
windows and caps and is reserved for exact `/info` and `/info/usage` paths.
Its cap and window require an explicit small operational allowance; this
implementation seeds neither. `/scoreboard` uses extraction admission even
though provider metering may exempt it. Missing/expired/exhausted allowances
refuse transport. The old explicit-period reservation RPC is extraction-only.

Verify current provider capacity and select an extraction allowance with
headroom before activation. Provider observations and local reservations are
separate quantities: this change does not refund attempts, automatically raise
caps, poll provider information, or persist reconciled provider observations.
An enrolled administrative caller can use the control endpoints through the
same client. Automated reconciliation and any cap change remain separate work.
Configure all intended production callers before claiming account-wide
coverage; clients outside the enrolled rollout can still consume the shared
provider pool independently.

## Transaction and failure behavior

The operation parent is committed before clients can see the active operation.
Each actual HTTP attempt, including each retry, then follows:

1. Resolve the canonical endpoint's budget class and active configured window
   in PostgreSQL; reserve and commit a new attempt UUID.
2. Recheck the run/window and commit the single dispatch transition.
3. Send HTTP once and commit its transport result before returning or retrying.

A failed or uncertain database transaction stops further HTTP in that
invocation. There is no automatic replay of an uncertain reservation/dispatch
commit and no refund. A response whose result cannot be committed leaves a
pending, conservatively charged attempt; a crash can also leave a running
parent. Recovery must inspect these records under the accepted design before
closing them; this slice does not implement an automatic recovery worker.

Expected reservation denial marks the run unsuccessful but does not disable
another budget class. Thus `/info` can still use available control capacity
after extraction exhaustion. Control requests also use a separate 429 circuit
breaker so extraction refusal cannot prevent reconciliation. Database/accounting
failure blocks both classes. Existing bounded HTTP retry/backoff rules remain.

Success records a 2xx transport outcome; logical expected-no-data classification
remains the source adapter's responsibility. HTTP failures preserve their status;
network failures have NULL HTTP status. Arbitrary request values, headers,
credentials, and exception text are not written into the quota ledger. Stored
request context contains only supported numeric/boolean work-unit identifiers.

A process supports one enrolled invocation at a time. The active operation is
shared with extraction threads, and transport admission through result recording
is serialized within that process. Different processes still compete through
PostgreSQL locks. This prevents another thread from sending after an accounting
failure; no throughput improvement or benchmark is claimed.

The daily command records returned source/mart errors as run failure. Exhausted
transient request failures mark the operation partial even if a source records
a miss and continues; authentication/rate exhaustion marks it failed. Historical
SQL campaign pacing remains in place and unfinished work records a partial run;
its JSON admission guard is bypassed when enrolled. The daily drainer JSON gates
and shared source JSON gate are also bypassed when enrolled. Planning-only daily
and historical dry runs do not write operation records. Live `--dry-run` still
sends HTTP, so durable accounting applies even though target snapshots are not
written. A successful operation is not a source publication/freshness receipt.

## Verification

Executed tests use isolated PostgreSQL 17 with pgvector, actual caller roles,
and mocked HTTP; no authenticated CFBD or production database is contacted.
They cover upgrade with existing attempts, rerun safety, separate caps, exact
endpoint classification, concurrent reservations, expiry, replay, ACLs, real
adapter commit ordering visible from another connection, reconciliation after
extraction denial, and capacity shared across fresh Python processes.

Transport tests cover mixed 429/5xx/network retries, terminal 401/403/404,
reservation/dispatch/result failures, uncertain commit, sanitized errors,
independent control circuits, concurrent accounting failure, missing enrollment,
JSON gate bypass, and daily/live lifecycle wiring. Full managed bootstrap and
prior-baseline upgrade are executed separately from mocked transport checks.

Receipt publication, source freshness compatibility, automatic crash recovery,
automated provider reconciliation, production caller permissions, and actual
HTTP commit/send ordering on the deployed host remain future implementation or
rollout verification. No production enforcement is claimed by the prepared PR.
