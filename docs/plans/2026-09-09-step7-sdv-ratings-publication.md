# Step 7: controlled publication of SDV weekly ratings

Status: implemented and locally verified. Production migration, runtime permissions,
and workflow activation are separate rollout decisions.

## Bounded source contract

The first source adapter enrolls `sdv_ratings_weekly`, which loads
`ratings.sdv_ratings_weekly` from a season-specific sportsdataverse parquet file.
Its primary key remains `(season, through_week, team_id)`. The source already
feeds `marts.epa_crossvalidation` in the SQL refresh graph.

`source_publication_assets.py` records the source's season coverage grain,
replacement correction policy, nonempty-file requirement, SHA-256 watermark,
parser contract, weekly fetching cadence, and non-CFBD admission policy. It is
separate from the SQL refresh registry: source work units and whole-relation
materialized views have different coverage boundaries.

The opt-in command requires exactly one source and an explicit season:

```bash
python scripts/load_flat_files.py --source sdv_ratings_weekly \
  --season 2025 --require-receipts --dry-run
```

Remove `--dry-run` only for a separately configured target. `--file` can supply
the complete season artifact locally. The new mode rejects broad, mixed, and
`--due` selection and never falls back to an older season. A missing file is
deferred; an empty, malformed, mixed-season, duplicate-key, or partially parsed
file cannot publish complete coverage. The legacy command keeps its existing
merge and hash-skip behavior.

Coverage means every validated row of the selected artifact for that season.
It does not assert that every expected team/week exists upstream, that the
season is finished, or that observations were available before a prediction.
This mode deliberately reparses and stages unchanged bytes, so a parser fix or
an invalidated current pointer remains reachable despite an old hash-ledger
entry. Avoiding that repeated work is a later optimization.

## Publication boundary

Migration 071 extends the private receipt ledger with exactly this asset and
canonical `season:<YYYY>` coverage. Existing house Elo assets remain restricted
to `source-wide`; overlapping source-wide and season pointers for SDV ratings
are forbidden. Each valid correction replaces only the selected season,
including removal of keys absent from the corrected artifact.

The adapter starts and commits an operation run before fetching. It validates
the artifact, uses dlt to normalize and load a unique table in private
`warehouse_source_stage`, and reads that staged output with its genuine dlt
identifiers. The staging commit does not publish the target. Original dlt load
metadata remains in the private staging namespace; receipts identify that
namespace and the actual load IDs. Target column types and the sixteen source
fields are checked rather than silently discarding new fields or variants.

The bounded `warehouse_source` RPC locks the target and asset, compares the
planned current generation, and commits the season replacement, receipt,
current pointer, legacy flat-file ledger observation, and terminal operation
outcome together. Successful replay is a lookup; it cannot restore an older
generation after a later correction. `published_at` is a clock observation in
the transaction, and `committed_at` remains unknown.

Failed or deferred work keeps the previous current generation. A known failure
after rollback receives separate failure evidence. Unknown operation-start or
publication commit outcomes are reported with run/generation identifiers and
are not converted into a false failure or automatically retried. Interruptions
follow the same cleanup rules before propagating. An unresolved publication
retains its staging table for investigation.

Legacy row writes invalidate the affected season's pointer; moving a row
between seasons invalidates both. Truncation or relevant DDL invalidates all
SDV season pointers. Publication refuses missing or altered guards. Consumer
reads of data and receipt need one shared database snapshot.

## Access and limits

The publisher role is a bounded NOLOGIN role with only this source's lifecycle
and publication RPCs. It receives no generic operation or direct target/ledger
write access. Migration grants no runtime membership. The staging namespace is
private; the existing warehouse connection must separately have the staging
capabilities needed by dlt. This is not a general-purpose dlt destination
adapter or a claim that its broader connection is least-privileged.

The public freshness RPC remains the existing two-asset house Elo projection.
No SDV freshness SLA is activated and no consumer grant changes. Declared
descendants are visible in the dry-run plan but are not refreshed or given
generation receipts here. Generation enforcement for the whole crossvalidation
mart must account for its other inputs and all required season scopes before
that path can claim complete dependency coverage.

## Verification

Verification used local fixtures and a disposable PostgreSQL 17 database:

- 124 adapter, source-plan, CLI and legacy flat-file tests passed.
- 29 executed source-publication SQL cases passed, including real dlt staging
  from local parquet, typed all-NULL optional fields, genuine load identifiers,
  cleanup, actual caller roles, rollback, concurrent compare-and-swap, replay,
  season corrections and mutation/DDL invalidation.
- 123 existing bootstrap, operational-ledger, house Elo publication, freshness
  and generation-refresh SQL checks passed. The two bootstrap checks passed
  again after the final inheritance-guard correction.
- Affected Python files passed Ruff lint and formatting checks.

Independent review covered SQL and adapter correctness and access boundaries.
No production publication or provider request was performed. Production lock
duration, staging retention volume, and managed-platform privilege parity
remain unmeasured.
