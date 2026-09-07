# F06 production rollout — 2026-09-07

The user approved migration 065 and production ledger adoption after PR #129
merged as `e83f53f`. Rollout work uses `codex/f06-production-rollout`.

## Executed grant correction

| Operation | Actions run | Result |
|---|---|---|
| Migration 065 | [34161337223](https://github.com/rstover-fo/cfb-database/actions/runs/34161337223) | Applied successfully |
| Repeat 065 and actual-role assertions | [34161464981](https://github.com/rstover-fo/cfb-database/actions/runs/34161464981) | Repeat application and all assertions passed |

All 54 API views were queried under each of `anon`, `authenticated`, and
`analyst_ro`. The public trajectory wrapper and its underlying mart are readable
by the app roles after 065. Consumer write privileges remain absent; private
scouting reads and direct analyst mart access raise `insufficient_privilege`.
The read-only verification emits role/count notices, never table row payloads.
No view rebuild, ingestion, training, prediction write, or data backfill ran.

## Adoption boundary

Adoption is prepared separately from the disposable bootstrap manifest. Its
production manifest starts with a catalog-adoption receipt attesting to the
observed post-065 schema. It does not insert fictitious baseline, 064, or 065
execution records. The workflow runs above are the provenance for the actual
065 execution. Future managed production migrations append to the production
manifest; the disposable manifest cannot be substituted for it.

The final catalog comparison and adoption use a maintenance window for
warehouse schema writers and the existing migration advisory lock. That lock
coordinates managed migration tools; it does not universally prevent unrelated
administrators from issuing DDL. Schema-only fingerprints and owner/ACL metadata
attest catalog state, not historical data transformations or data completeness.

## Tooling verification and pending execution

The explicit adoption tool and workflow integration passed independent review.
Twelve focused adoption unit tests passed; the reviewer independently ran 96
adoption/CLI/deploy checks. The complete mandatory disposable PostgreSQL 17 suite
passed **11 tests**. Its adoption case executes real pg_dump capture, catalog-drift
rejection before ledger creation, single-receipt adoption, validated repeat no-op,
status, private ledger ACLs, and a synchronized advisory-lock regression proving
the catalog snapshot follows the lock wait. All affected Ruff checks passed.

The user explicitly approved the schema definitions, ownership, grants, and role
membership metadata export to GitHub Actions. [Capture run
34163351331](https://github.com/rstover-fo/cfb-database/actions/runs/34163351331)
succeeded on source revision `6316d1e81c2a84809324c3226c17ba027a6c29fb`.
Independent comparison with the approved F06 capture found exactly the two 065
SELECT grants; every pre-existing metadata field was unchanged. The supplemental
metadata includes schema owners and transitive consumer/owner role attributes
and membership options.
Canonical ownership/access metadata is retained beside the receipt as
`20260907_production_catalog_metadata.json`; its bytes match the receipt's
metadata SHA256 after the short-lived Actions artifact expires.

The full catalog fingerprint is
`288b4eaf817bc00f1bba71d4949c72a45d58a77ca0db6907c68a5ae0b337ab94`.
The checked-in receipt replaces the preparation-time PR provenance link with the
actual immutable Actions run URL and regenerates its SQL binding; captured data
and fingerprints are unchanged. `src/schemas/production-manifest.json` contains
only `warehouse.production.catalog-adopted.20260907.288b4eaf817b`.

## Production adoption evidence

All operations below ran from reviewed source `9fed350` using the explicit
production manifest and receipt.

| Operation | Actions run | Result |
|---|---|---|
| Read-only preflight | [34163613346](https://github.com/rstover-fo/cfb-database/actions/runs/34163613346) | Exact catalog match; no existing ledger; adoption ready |
| Explicit adoption | [34163735093](https://github.com/rstover-fo/cfb-database/actions/runs/34163735093) | One adoption receipt committed with the reviewed fingerprint |
| Repeat adoption | [34163835274](https://github.com/rstover-fo/cfb-database/actions/runs/34163835274) | Validated no-op; exactly one ledger entry retained |
| Ledger privacy and consumer access | [34163850713](https://github.com/rstover-fo/cfb-database/actions/runs/34163850713) | Exact receipt/checksum, empty repeatable history, private ledger denials, and all 54 API views passed under actual roles |

The adopted root checksum is
`d621e7026d135f6e16aede646628624357cbc51c734dc3b381b4d441d8ab7dcb`.
Only the private control schema, its two ledger tables and identity sequence,
and the one adoption row were created. The schema-only fingerprint and repeat
check establish that the captured warehouse definitions were unchanged.
Data preservation here follows the executed adoption statements having no
warehouse DML; no full production row-payload hash audit was performed.

Backfill Sources, Daily Season Load, Flat File Load, Historical Refresh, and
Model Feature Experiments were active before the maintenance window and paused
for adoption. No conflicting warehouse writer was active or queued. CI finished
before adoption; Live Scoreboard and Probe 2026 Endpoints remained active.
[Final status run 34163889045](https://github.com/rstover-fo/cfb-database/actions/runs/34163889045)
passed with the same exact fingerprint, a valid installed ledger, and one entry.
All five paused workflows were restored and the complete before/after state map
matched exactly: all nine repository workflows are active. No rollback or repair
was required. No scheduled work remains intentionally paused for this rollout.

## PR review follow-up

Greptile identified two operational improvements after the completed rollout.
Adoption status and repeat checks now distinguish a root-only ledger, which
still requires the exact catalog fingerprint, from later valid managed history.
After later managed migrations they validate the complete ledger and explicitly
report that the evolved catalog was not checked against the original receipt.
An executed forward-ALTER regression verifies status/no-op behavior and rejection
of tampered later migration checksums.

Both verification SQL files now include explicit BEGIN/ROLLBACK boundaries for
standalone runners. The original production runs already had transactions from
`run_migrations.py`, so their read-only and timeout safeguards were active.
The complete scripts passed both transactional psycopg2 and standalone psql on a
disposable full warehouse. Four additional PostgreSQL cases verify actual write
rejection and active timeout settings in both runner modes. The expanded mandatory
warehouse suite passed **15 tests**; the reviewer independently passed 98
adoption/CLI/deploy unit checks. No additional production migration was required.
