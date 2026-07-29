---
name: dlt-pipelines
description: This repo's dlt pipeline conventions — source modules, write dispositions, API budget discipline, skip logic, and test patterns. Use when adding or modifying anything under src/pipelines/, scripts/load_season.py, or a new data source.
---

# dlt Pipelines (cfb-database)

Generic dlt REST API reference: `docs/dlt-reference.md`. Endpoint-to-pipeline
status: `docs/pipeline-manifest.md`. CFBD API conventions and traps: the
`cfbd-api` skill. This file carries what is repo-specific.

## Source module shape

- One module per domain under `src/pipelines/sources/`; resources declared
  with `@dlt.resource(write_disposition="merge", primary_key=...)` and an
  `EndpointConfig` entry in `src/pipelines/config/endpoints.py`.
- **Merge is the default disposition everywhere.** Replace/append is an
  explicit opt-in used only by the game_stats batch path.
- Year windows come from `YEAR_RANGES` in `src/pipelines/config/years.py`;
  `mode` (`incremental` vs `backfill`) selects the year list, never the
  disposition. `get_current_season()` returns `year - 1` until August — it
  is an ingest-window rule only; compute chains use
  `get_projection_seasons()` instead.
- No dlt incremental cursors for API sources. For per-game fan-outs, skip
  already-loaded work with a **DB set-difference** before fetching (the
  `run_metrics_wp_pipeline` pattern in `src/pipelines/run.py`) — merge
  dedupes rows but does not refund API calls.

## API budget discipline (audit-backed)

- Every request goes through `make_request` -> the rate limiter; never a
  raw HTTP call. The limiter's JSON state does not survive CI runners —
  do not treat it as month-to-date truth there.
- New sources register an entry in `ESTIMATED_CALLS`
  (`scripts/load_season.py`) and a deliberate decision on
  `IMMUTABLE_ONCE_FINAL` membership (can this source's data change after a
  season completes?). The pre-run estimate comparison warns; per-call
  enforcement is the hard stop.
- Fan-out sources (`/plays/stats` is one call per game) must bound their
  per-run cost: caps (`MAX_WP_GAMES_PER_RUN` pattern) or already-present
  skips. Full context: the 2026-07-28 audit at
  `docs/solutions/best-practices/2026-07-28-cfbd-api-usage-audit.md`.
- 429 handling lives in `src/pipelines/utils/api_client.py` (Retry-After,
  circuit breaker, `RateLimitExhausted`) — do not add per-source retry
  loops on top of it, and never return `[]` on failure.

## dlt data-shape gotchas

- Nested arrays become child tables `{parent}__{field}` with
  `_dlt_parent_id`/`_dlt_list_idx`; access via
  `LEFT JOIN LATERAL ... WHERE _dlt_parent_id = parent._dlt_id`
  (worked example: `docs/solutions/database-issues/`).
- dlt snake_cases camelCase API fields — verify loaded names via
  `pg_attribute` before writing SQL against a new table.

## Test patterns

- Source tests mock `make_request` with side-effect fakes
  (`tests/test_sources/` pattern) and assert resource composition; no test
  touches the live API.
- `conftest.py` provides `db_conn` (autocommit) reading `SUPABASE_DB_URL`
  or `.dlt/secrets.toml`; integration tests skip cleanly when neither is
  available — never make a unit test require the DB.
- Flat-file sources: `FlatFileSpec` registry + pure parsers
  (`parse(raw, ctx) -> Iterator[dict]`), hash-skip ledger in
  `meta.flat_file_loads`, synthetic fixtures under
  `tests/fixtures/flatfiles/`.
