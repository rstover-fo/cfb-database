# F03: season closure and fit eligibility

## Objective

Replace the three 99%-complete predicates that can close a live season and
trigger same-season training. Keep schedule corrections reachable and enforce
strict seasonal fit eligibility independently at prediction time.

## Implementation boundaries

- Shared lifecycle classification for unattended loader skips, player overview
  eligibility, and annual refit selection. No percentage tolerance. Require the
  conservative calendar floor, schedule coverage, resolved identities, complete
  scored contests, and no future or unresolved games. Automatic refits require
  all years in the expanding training window to be present and closed.
- Unattended finalized-season ingestion still refreshes `/games`; it does not
  repeatedly fetch drives or ancillary game resources. Manual loads retain their
  requested scope. A reopened schedule is visible to subsequent eligibility
  decisions; existing fit metadata never overrides the per-game scoring guard.
- Upcoming scoring resolves the latest strictly earlier fit for every pending
  season. Backfill remains exact S-1. Validate the selected vintage per game and
  reject missing eligibility before writing the batch.
- Centralize the reviewed Campbell–Western Carolina event replacement. Raw
  records remain intact. Exclude the superseded original from modeled inputs;
  full schedule/closure decisions require the matching replacement.

## Acceptance evidence to collect

1. A 1,600-game slate with ten unfinished games remains open.
2. Playoff tails, future/postponed games, missing dates/scores, a truncated
   regular-only schedule, and old unresolved records cannot imply closure.
3. An explicitly reviewed cancellation can be terminal; unreviewed missing rows
   cannot. A resolved, fully completed historical season can close.
4. Schedule-only reconciliation remains invoked on the unattended final path;
   explicit loads preserve full resources and reopening remains possible.
5. Mixed prediction seasons use independently eligible vintages; same/future
   fits and a batch with no eligible fit cannot write predictions.
6. Executed fixture queries exclude the false completed original while keeping
   the replacement and unrelated repeated matchups.
7. Independent review, affected lint/format checks, root tests, and PR CI pass.

## Rollout and limits

This change does not run ingestion, train models, deploy schema, repair stored
fits, or rebuild historical predictions/features. Existing invalid snapshots
need a separately reviewed rebuild; filtering query inputs does not erase them.
After merge, inspect the next daily lifecycle/coverage gates. An unresolved
historical season requires investigation and a reviewed correction or explicit
cancellation resolution, not loosening the threshold.

A local warehouse snapshot cannot prove the provider published every future
contest. Calendar and coverage checks detect common incomplete schedules; the
continuing reconciliation path and independent scoring guard contain that risk.
The special 2020 closure floor accommodates the spring-season disruption:
the [NCAA 2020–21 FCS bracket](https://www.ncaa.com/brackets/print/football/fcs/2020)
places the championship on May 16, 2021. July 1 is our conservative policy
buffer, not an NCAA definition of season completion.

## Implemented and verified locally

The shared classifier, schedule-only reconciliation, per-season scoring guard,
contiguous automatic-refit frontier, and shared event exclusions are implemented.
Independent cross-review found and resolved a staleness defect: an existing
premature/future fit could suppress creation of the correct eligible vintage.
Such vintages now do not count as current relative to the safe frontier.

Full offline suite: 2,327 passed, 449 skipped. MCP suite: 59 passed. Executed
SQLite fixtures cover canonical result/target queries; mocked orchestration
executes real fit selection/vectorization and confirms no writes for invalid
eligibility. Independent reviews found no remaining actionable correctness
issues. The full Postgres feature query and production rebuild were not executed
locally. No performance improvement or production repair is claimed.
