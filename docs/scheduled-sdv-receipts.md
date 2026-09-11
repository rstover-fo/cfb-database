# Scheduled SDV ratings and crosswalk receipts

`SDV Source Receipts` publishes the explicitly activated SDV ratings and
crosswalk scopes every Monday at 10:29 UTC. Each source has its own operation,
receipt, current generation, and freshness policy. The separate
[FPI publisher](scheduled-fpi-receipts.md) retains its daily schedule and
36-hour policy.

## Selected coverage and activation

| Repository variable | Accepted active value | Published asset / coverage | Required policy |
| --- | --- | --- | --- |
| `SDV_RATINGS_RECEIPT_SEASON` | `2026` | `ratings.sdv_ratings_weekly` / `season:2026` | 8 days |
| `SDV_TEAM_XWALK_RECEIPT_SEASON` | `2025` | `ref.team_id_xwalk` / `season:2025` | 8 days |
| `SDV_GAME_XWALK_RECEIPT_SEASON` | `2025` | `ref.game_id_xwalk` / `season:2025` | 8 days |

Unset variables leave their sources inactive. Unsupported nonempty values
fail configuration validation. The driver distinguishes these publication
seasons from the 2026 operating season and stops before database or provider
access when that operating season ends, on August 1, 2027. Review the next
scope or retire the variables and policies before then. A newly available
crosswalk file for another season requires an explicit canary and coverage
review; the scheduler never changes seasons or falls back automatically.

An eight-day policy measures time since a successful complete publication.
Weekly publication leaves one day for scheduling or warehouse delays before
evidence becomes stale. It does not measure how recently the provider changed
the file. Identical valid files are deliberately republished so both fetch
evidence and upstream corrections remain reachable. The normal run fetches
one artifact per active source, with the existing bounded fetch retries;
redirects can add HTTP requests. These files do not consume CFBD API quota.

Ratings use their in-file season. Registered crosswalk files have no season
column, so scheduled receipts use the reviewed registered filename as their
season basis. Publishing complete 2025 crosswalks does not satisfy freshness
for 2026: a 2026 query continues reporting that coverage as unrecorded until
genuine 2026 files are independently enrolled. Preserve unmatched provider IDs,
NULL values, repeated team names, and repeated matchup keys distinguished by
date. A complete receipt covers the validated file, not every possible team,
game, or provider mapping.

## Controlled activation

1. Review and deploy the implementation with the new activation variables
   unset. Verify the deployed revision and exact production migration manifest.
2. Record and pause competing Daily, flat-file, and receipt workflows, then
   drain their queued and active writers. The reusable canary workflow holds
   only `flat-file-load`; it needs this temporary pause of outer-lock writers.
3. Validate the registered artifact, review its business delta, and pin its SHA.
   Run the canary's `preflight` through the actual publishing connection for
   exactly one source and season. It verifies project routing, migration
   checksums, actual runtime identity, bounded role access, and the source plan.
4. Publish each selected canary separately. Reconcile a failure before starting
   another canary. Check operation and generation IDs, complete coverage, SHA,
   target keys and dlt lineage, unchanged unrelated rows, and actual `anon` and
   `authenticated` freshness responses. Ratings also refresh
   `marts.epa_crossvalidation` after a publication attempt, including failures
   that may occur after commit. Preserve the original failure status.
5. Configure only the selected policies through a guarded owner transaction,
   requiring a recent complete current receipt and rejecting any conflicting
   interval. Verify repeated application, public caller behavior, and denied
   direct access to private policy data. A scheduler cannot create policies.
6. Set each variable only after its corresponding canary and policy pass.
   Verify legacy exclusions while writers remain paused, restore the recorded
   workflow states, and dispatch `SDV Source Receipts`. Check all selected
   outcomes and the ratings refresh. Observe a later automatic run separately
   before claiming that cron execution is verified.

The manual canary command is deliberately limited to one selection:

```bash
python scripts/run_source_receipt_canary.py preflight \
  --source sdv_team_xwalk --season 2025 \
  --expected-sha256 "$REVIEWED_SHA" \
  --expected-project-ref "$EXPECTED_PROJECT_REF"
```

Use `publish` only after reviewing that same artifact and target. The canary
fetches and validates the selected bytes, then publishes a pinned temporary
copy. Its crosswalk receipt therefore honestly says `local_file` and
`caller_declared`; keep the registered URL, filename season, and checksum in
the associated operational evidence. The scheduled direct-URL path says
`registered_url` and `registered_artifact_name`. Ratings retain `artifact_field`
in both paths. A changed checksum requires a fresh artifact and delta review.

## Scheduling, verification, and legacy ownership

The new workflow takes `daily-season-load` first and `flat-file-load` second,
using queued, non-cancelling concurrency in the same order as Daily and FPI.
Inactive runs have a separate outer group. Database credentials are scoped to
publication and refresh steps. Active sources run serially; a source failure
does not prevent the other selected sources from being attempted, but any
failure makes the whole run fail.

Each source first checks the inspected connection, bounded publisher role,
source plan, and exact policy. Publication uses that same connection for both
the batch and dedicated ratings adapters. Verification matches the returned
operation, new receipt, current generation, SHA, complete row counts, and
source-specific provenance. Both public caller roles must return the new
generation and a current, non-stale eight-day policy. An older current receipt
cannot prove that this run succeeded.

The legacy `Flat File Load` workflow excludes the following planned attempts
when the corresponding variable is active:

| Source | Excluded legacy source/season pairs |
| --- | --- |
| Ratings | `sdv_ratings_weekly:2026` |
| Team crosswalk | `sdv_team_xwalk:2025`, `sdv_team_xwalk:2026` |
| Game crosswalk | `sdv_game_xwalk:2025`, `sdv_game_xwalk:2026` |

Both crosswalk pairs are necessary: legacy implicit-season fetching can try
2026 and fall back to 2025 after planning. Excluding only the 2025 attempt would
leave a path that overwrites receipted rows. These exclusions apply to implicit
`--due` calls and explicit-season `--due` backfills. FPI retains its independent
`sdv_fpi_weekly:2026` exclusion. Other source/seasons remain eligible.

An explicit legacy `--source` invocation remains an intentional operator
bypass. A local legacy loader invocation also has no repository-variable
context unless exclusions are supplied. Such writes may invalidate a current
receipt. Use the receipt publisher for maintained active scopes.

## Failure recovery

Retain each operation ID, generation ID, publication status, and failure phase.
A failure in verification may follow a committed publication. Reconcile the
exact operation, receipt, current pointer, and target rows before retrying;
never automatically replay an uncertain commit. Source-specific transaction
failures preserve the previous target/current generation and do not erase
receipt history.

Ratings consumer refresh runs after an attempted publication even when
publication verification fails or another selected source fails. Crosswalk-only
runs do not refresh that mart. A refresh failure makes the workflow fail without
undoing the source publication. The mart remains an internal season-level
comparison, excluded from as-of model inputs and shipping gates.

To suspend scheduled retries during investigation, disable the dedicated
workflow and retain activation variables so legacy exclusions still protect
the receipted partitions. Evidence can become stale while publication is
suspended. If a return to legacy ownership is deliberate, reconcile pending
operations and retire its policy and activation variable together. Subsequent
legacy writes may invalidate the old receipt; do not conceal that state by
deleting history or changing a historical season label.
