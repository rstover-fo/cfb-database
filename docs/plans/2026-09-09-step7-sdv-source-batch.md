# Step 7: SDV source publication batch

Status: implemented and independently reviewed. This follows the first source
slice merged in PR #143. Production migration, runtime membership and scheduled
activation remain separate rollout decisions.

## Enrolled sources

| Source | Target | Preserved primary key | Season evidence |
|---|---|---|---|
| `sdv_fpi_weekly` | `ratings.espn_fpi_weekly` | `season, season_type, week, team_id` | In-file season on every row |
| `sdv_team_xwalk` | `ref.team_id_xwalk` | `season, xwalk_key` | Registered artifact name or explicit caller declaration |
| `sdv_game_xwalk` | `ref.game_id_xwalk` | `season, matchup_key, yahoo_date` | Registered artifact name or explicit caller declaration |

The existing `sdv_ratings_weekly` source remains enrolled with its original
receipt protocol. Shared Python publication code handles staging and operation
outcomes, while each source retains its explicit column, type and key contract.
The legacy flat-file merge path retains its existing behavior.

The crosswalk files have no season column. Their receipts distinguish
`registered_artifact_name` from `caller_declared` season assignment; they do not
claim that the file independently proved its season. Local artifacts record no
filesystem path or provider URL in the receipt or load ledger. The operator
must supply the complete artifact for the declared source and season.

Nullable ESPN IDs remain nullable, and team rows with all provider IDs absent
retain the parser's canonical `norm_key#None` key when it is unique. Repeated
normalized team names with distinct source IDs remain distinct. Game rematches
retain their date discriminator, including postseason dates in the next
calendar year. FPI preserves regular/postseason keys, week zero, nullable
metrics, typed booleans and timezone-aware source timestamps.

## Batch execution and publication

```bash
python scripts/load_flat_files.py --require-receipts --season 2025 \
  --source sdv_fpi_weekly --source sdv_team_xwalk --source sdv_game_xwalk \
  --dry-run
```

Each selected source and season gets a separate operation, staging table and
publication transaction. A failed source reports its error and run/generation
IDs; the remaining selected sources still run, and the command exits nonzero
if any source did not publish. Interruptions propagate. Duplicate source
selections, unenrolled sources and `--due` are rejected. `--file` remains
restricted to one explicitly selected source and season. Dry runs remain
offline and show the season evidence and declared SQL descendants, where known.

Migration 072 adds the private `warehouse_source_batch` namespace for the three
new sources. The existing publisher role receives only its bounded lifecycle
RPCs. Source selection resolves against a fixed allowlist; callers cannot
choose arbitrary target relations. Migration 071 and its ratings RPCs retain
their original protocol. Consumer grants remain unchanged.

The publisher replaces only the selected season, including removal of keys
absent from a correction, and commits target rows, the success receipt, current
generation, legacy hash-ledger observation and terminal operation outcome
together. Raw and staged validation reject dropped rows, lossy conversions,
duplicate keys, schema drift and invalid evidence. Genuine dlt metadata remains
in private staging. An unchanged hash remains eligible for parser corrections.

Known failures preserve the previous current generation. Uncertain commits
retain evidence and are not retried or relabeled as definite failures. Legacy
row writes invalidate affected season pointers; truncation and relevant DDL
invalidate that source's pointers. Missing or altered guards block publication.

PostgreSQL does not fire DDL event triggers for changes to event triggers
themselves ([PostgreSQL 17 behavior](https://www.postgresql.org/docs/17/event-trigger-definition.html)).
Disabling a shared event trigger therefore does not immediately clear existing
pointers, but the publisher checks both shared guards and refuses new
publication while either is unavailable. Privileged maintenance that bypasses
guards must invalidate affected pointers before restoring the guards.

Complete coverage means the validated contents of the selected artifact. It
does not establish provider-wide team/game coverage, completed seasons or
prospective model eligibility. This batch does not refresh descendants or
activate public source freshness; crossvalidation still needs complete coverage
of all its other inputs before generation enforcement can cover that mart.

## Verification

- 214 focused Python tests passed across the batch adapter, existing ratings
  adapter, CLI, source plans, legacy multi-file loading and SDV parsers.
- 76 bootstrap, migration, bootstrap CLI and existing ratings SQL tests passed
  against disposable PostgreSQL 17, including real dlt staging.
- 67 new source SQL tests passed on disposable PostgreSQL 17, including all
  three real local Parquet/dlt adapters, source and season isolation, correction
  removals, rollback, replay, concurrent CAS, actual caller roles, conditional
  trigger and rewrite-rule rejection, and compatibility with ratings and Elo.
- Independent SQL and Python/CLI reviews found no remaining material issues.
- Ruff and formatting checks passed for the affected Python implementation,
  CLI and unit tests. The three-source CLI dry run also passed offline.

Fixtures use local files and disposable PostgreSQL; no production publication
or provider request was made. Production lock duration, memory at the maximum
row cap and managed-platform privilege parity remain unmeasured.
