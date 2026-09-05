---
name: dlt-pipelines
description: Use when changing ingestion resources, orchestration, merge behavior, or request budgeting.
---

# dlt pipelines

Preserve source grain, primary keys, nested-parent relationships, correction
behavior, and explicit failure outcomes. Merge is the default ingestion
disposition; use another disposition only when the established source contract
requires it.

## Load only relevant context

- Consult `docs/pipeline-manifest.md` when endpoint wiring, table status, grain,
  or backfill coverage is part of the change.
- For CFBD request parameters, endpoint semantics, or rate limits, use the
  `cfbd-api` skill. Generic dlt mechanics live in `docs/dlt-reference.md`.
- For flat files, inspect the `FlatFileSpec` registry, parser, crosswalk, and
  `meta.flat_file_loads` ledger paths relevant to that source. CFBD HTTP rules
  do not automatically apply to flat-file or other-provider ingestion.

## Resource and request behavior

- Define an explicit resource grain and primary key. Keep year selection,
  execution mode, and write disposition as separate decisions.
- Ingestion year windows come from `src/pipelines/config/years.py`. The calendar
  rule `get_current_season()` is for ingest windows; compute chains use scheduled
  and completed games through `get_projection_seasons()`.
- CFBD HTTP requests must use the shared `make_request` path so rate limiting,
  retry classification, and call accounting remain centralized. Flat-file
  sources use their fetch/archive/parser workflow; other providers use their
  established client rather than being forced through `make_request`.
- Register new scheduled CFBD work in the loader's source ordering and call
  estimate, and decide explicitly whether a finalized season can still receive
  corrections. Bound per-entity or per-game fan-out and report deferred work.
- Do not add source-local retries over the shared CFBD client. Preserve distinct
  outcomes for expected no-data, validation/auth failures, rate exhaustion,
  transport failures, and deferred work. Never convert a failed request into an
  empty successful resource.

## Skips and corrections

Merge prevents duplicate destination rows; it does not save API calls or prove
that previously loaded values are current. A DB set-difference is appropriate
for filling truly missing immutable entities. For mutable or correction-prone
data, define a refresh window, explicit re-pull, freshness rule, or campaign
ledger so corrected rows remain reachable.

Add a source to `IMMUTABLE_ONCE_FINAL` only when its data and candidate set are
stable after the repository's final-season definition. Do not use a blanket
finished-season skip for a backlog whose missing rows still need to drain or
for charting that upstream can revise. An explicit user-selected backfill
should not silently inherit an unattended-run optimization that defeats the
request.

## Loaded shape and verification

- dlt snake-cases API fields. Nested arrays become child tables connected by
  `_dlt_parent_id` and ordered by `_dlt_list_idx`; preserve that relationship
  rather than joining children by coincidental values.
- Treat variant columns as part of the observed loaded schema. Update matching
  allow-lists when a reviewed SQL consumer adds a supported variant twin.
- Preserve null as unavailable or uncomputed where the domain contract says so.

Use fixtures and side-effect fakes to verify request composition, resource
grain, primary keys, skips, correction reachability, and failure propagation.
Flat-file tests should use small synthetic inputs and verify ledger semantics.
Live CFBD or database access is not required to draft or fix a pipeline; when
unavailable, state which loaded-shape, integration, or performance checks were
not run.
