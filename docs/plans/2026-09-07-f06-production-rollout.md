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

Production adoption and final verification evidence are pending execution.
