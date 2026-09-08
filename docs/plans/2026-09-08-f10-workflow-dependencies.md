# F10 immediate workflow dependency corrections

## Scope

This change addresses the three concrete refresh gaps in F10. It does not claim
completion of the broader source-to-API dependency planner shared with F28, or
F11's generation tracking and descendant failure propagation inside the Python
refresher. No SQL definitions or grants change.

## Dependency paths

| Successful producer | Required consumer refresh |
|---|---|
| Daily adjusted-EPA recompute | `marts.team_adjusted_epa`, then `marts.epa_crossvalidation` |
| `sdv_fpi_weekly` / `ratings.espn_fpi_weekly` | `marts.epa_crossvalidation` |
| `sdv_ratings_weekly` / `ratings.sdv_ratings_weekly` | `marts.epa_crossvalidation` |
| `coach_tenures` / `ref.coach_tenures` | `marts.coach_tenures` |

The crossvalidation definition also reads `marts.team_epa_season`, refreshed by
the daily full refresh before the compute chain. Massey ratings currently have
no SQL mart consumer; their freshness is checked by `verify_load.py`.
`metrics_ppa_predicted` has no materialized-view consumer and retains its
explicit pipeline-only dispatch path.

## Workflow behavior

Daily automation runs load/compute and its final selected refresh, then calls
the reusable flat-file workflow, then verifies the combined result. Recaps wait
for verification. GitHub job dependencies replace the independent 11:00 UTC
flat-file schedule; delayed or failed upstream work cannot be bypassed by the
verification job. The existing 10:00 UTC daily trigger remains.

The flat-file workflow retains manual `source` and `seasons` inputs and its
separate `flat-file-load` concurrency group. Reusing the caller's
`daily-season-load` group would deadlock the nested call. After a successful
import step it refreshes crossvalidation, including when the loader reports an
empty due plan, hash-skips, or expected no-data. This is deliberate: imported
rows may already be committed when a prior refresh fails. Retrying must refresh
consumers even if the next import has no new bytes. An import or refresh failure
fails the reusable job. The daily job retains its own concurrency group for the
entire chain; this is not a warehouse-wide lock across arbitrary CLI callers.

Coach-tenure backfills refresh their mart only after the pipeline command
succeeds. The normal season-loop path still refreshes all marts after all
selected seasons succeed. Neither provider selection nor source merge behavior
changes.

## Verification and limits

Workflow shell tests use the actual YAML commands with a stubbed Python command
to exercise source/season arguments, refresh ordering, and failure exit codes.
They do not invoke providers, database refreshes, or the GitHub scheduler.
Reusable-workflow wiring is checked against GitHub's
[documented workflow-call contract](https://docs.github.com/en/actions/how-tos/reuse-automations/reuse-workflows).

Local validation: **162 tests passed** across the two F10 workflow suites and
the existing season-loader and flat-file suites. Ruff lint/format and
`git diff --check` passed. Actionlint 1.7.12 validated all three changed workflows.
Independent review found no actionable issues.

This PR does not dispatch warehouse workflows or apply production changes.
Successful production refresh and scheduler execution remain post-merge
verification. The full mart list and refresh engine are unchanged; existing
within-job partial refresh/generation risks remain F11 work.
