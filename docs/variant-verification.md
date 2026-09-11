# Reviewing dlt variant columns

The daily verifier checks numeric `__v_double` columns on source tables with
maintained consumers. A new variant requires review: a consumer that reads
only its base column may miss values stored in the variant.

`src/pipelines/utils/variant_twins.py` holds the recognized variant inventory.
Its separate reviewed raw-only registry records exact passing fields that
maintained marts, API views, and RPCs do not read. These entries preserve raw
data; they do not canonicalize or expose the metrics. Direct raw-table callers
must still handle dlt variants themselves.

## Classify a new variant

1. Inspect the base and variant types, value distribution, and source grain.
   Trace both maintained SQL and runtime query consumers, including the actual
   deployed definitions when a suitable database is available.
2. If a consumer reads the metric, handle both storage columns while preserving
   NULL and zero semantics. The established numeric pattern is
   `COALESCE(base::double precision, base__v_double)`. Register the supported
   variant after verifying every affected consumer.
3. If no maintained consumer reads the metric, add its exact name to the
   reviewed raw-only registry with the review rationale. Do not use wildcard
   prefixes or automatically accept the live catalog.
4. Keep newly discovered names failing until reviewed. Missing supported
   variants retain their warning behavior; reviewed raw-only fields do not
   become required source columns.

Tests protect the passing consumer inventory and reject raw-only fields that
later appear in maintained consumers, including source wildcard and whole-row
reads. Their discovery covers literal references within declared maintained
roots. Fully dynamic SQL and external clients remain a manual review boundary.
Consumer mappings and SQL fallback expressions must describe executable reads;
a name appearing in a comment is insufficient.

The deploy-time variant arrays in `validation_rushing_views.sql` cover the two
rushing season tables. Update a SQL counterpart only when that table actually
has one. A reviewed raw-only passing field requires no mart rebuild or new API
column. If a consumer definition does need changing, use the
[dependency-aware mart release process](mart-releases.md).

## Verify without reloading

Run the ordinary verifier against the intended warehouse:

```bash
.venv/bin/python scripts/verify_load.py --season 2026
```

Its database connection is read-only. The full check also compares the season
game count with CFBD, normally one `/games` request through the shared client.
It reports the entire verifier outcome, so an unrelated failure is not hidden
by a passing variant check.

When credentials are available only in GitHub Actions, use the existing
`Deploy Schema` workflow with `action=compute`, `compute_script=verify_load`,
and `compute_args=--season,2026`. Keep `refresh=false`; the runner rejects any
refresh request for this diagnostic. Select the reviewed ref explicitly.
This dispatch shares the daily ingestion concurrency group and waits for its
writers to finish before checking the warehouse. Manifest-based verification is
rejected because a push manifest cannot select that group through workflow inputs.

A passing diagnostic establishes the verifier result at that time. Confirm
the next automatic Daily Season Load separately before claiming the full
scheduled pipeline has passed. Replaying ingestion is unnecessary solely to
validate this check.
