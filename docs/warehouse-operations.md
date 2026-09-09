# Warehouse operations and historical context

Use this document for operations and incident investigation. `AGENTS.md` is the
working agreement; this file is not a checklist to read before every edit.

## Architecture and sources of truth

The warehouse ingests CFBD API and flat-file data through `src/pipelines/` into
Supabase Postgres. `src/schemas/` contains domain tables, the private scouting
schema, analytics/marts, and the API/public consumer surfaces. dlt nested tables
use `_dlt_id`, `_dlt_parent_id`, and `_dlt_list_idx`; preserve their actual grains.

- Consumer behavior and ownership: `docs/SCHEMA_CONTRACT.md`.
- Endpoint-to-source mappings: `docs/pipeline-manifest.md`.
- Provider parameters and availability: `docs/cfbd-api-endpoints.md`.
- Pipeline patterns: `docs/dlt-reference.md` and the shared dlt/CFBD skills.
- As-of feature/model methodology: `docs/modeling-contract.md` and its design doc.
- Current schedules and command arguments: `.github/workflows/` and each script's
  CLI. Verify changing facts there rather than carrying counts in agent prompts.

## Operational entry points

These commands operate on an explicitly selected, authorized warehouse; use
`scripts/setup_dev.sh` for dependency installation without database work.

| Operation | Entry point |
|---|---|
| Inspect a season load plan and estimate | `scripts/load_season.py --dry-run` |
| Season ingestion | `scripts/load_season.py --weekly` (explicit `--season` for backfills) |
| Post-load verification | `scripts/verify_load.py` |
| Inspect / maintain plays partitions | `scripts/maintain_play_partitions.py` (read-only); `--apply` creates missing partitions |
| Inspect / repair plays index ownership | `scripts/maintain_play_indexes.py` (read-only); explicit `--apply --index <name>` for a maintenance window |
| Refresh existing marts | `scripts/refresh_marts.py` |
| Apply definitions / migrations | [Atomic mart releases](mart-releases.md); `scripts/run_migrations.py` for explicit non-mart migrations |
| One-off SQL application | `scripts/run_migrations.py --file <path>`; follow that file's application contract |
| Flat-file ingestion | `scripts/load_flat_files.py --due` |
| Check upstream preseason availability | `scripts/probe_offseason_availability.py` |

Read relevant workflow/CLI options before execution. Direct application is a
separate action from preparing SQL. The session pooler is used by warehouse jobs;
transaction pooling does not accept all startup/session options these jobs use.
Secrets belong in ignored config or the host's credential mechanism, never docs.
The request budget is configured in `.dlt/config.toml`; historical estimates do
not establish the cost of today's source/resource selection.

## Workflow refresh dependencies (F10)

The daily workflow runs load/compute, calls the reusable flat-file workflow,
then verifies the combined result. Recaps wait for verification. The flat-file
workflow remains manually dispatchable, but its former independent 11:00 UTC
schedule is removed. A completed import step refreshes `marts.epa_crossvalidation`
even after partial failure, while preserving the failed job status; cancellation
and setup failures skip the refresh. Hash-skips and empty due plans also trigger
refresh, allowing a failed refresh to be
retried without reloading unchanged files.

The final daily refresh includes crossvalidation after `marts.team_adjusted_epa`.
The `pipeline_run` coach-tenure backfill refreshes `marts.coach_tenures` after
loading; `metrics_ppa_predicted` still has no mart refresh. Direct script calls
do not acquire these workflow dependencies automatically. See the
[F10 plan](plans/2026-09-08-f10-workflow-dependencies.md) for scope and verification
limits; historical schedule descriptions later in this document predate F10.

## Plays partition rollover (F08)

`run_plays_pipeline()` performs catalog preflight before constructing the source
or fetching plays. It validates `core.plays` as `LIST (season)`, each child's
attachment and single-season bound, and conflicting partition names. On an
authorized ingestion run it creates missing partitions for the requested years
and the current calendar year plus one following year, in a separate atomic
transaction before dlt starts loading. Maintenance uses bounded lock waits;
catalog inconsistencies fail the load rather than being silently repaired.

Preview a target's catalog with
`.venv/bin/python scripts/maintain_play_partitions.py`; use `--apply` only for an
authorized target. An explicit historical `--years 2004` includes that partition
in the required set. Existing historical partitions remain in place, so late
records continue to route by their requested season; this does not change
provider correction or re-fetch policy. Years before 2004 or beyond the next
calendar year are rejected. No default partition is added, so NULL or unsupported
season values cannot silently accumulate in catch-all storage. The parent must
already be partitioned; this command does not replay the old table-swap migration.

Historical source ranges keep fixed coverage starts and resolve their upper end
to the calendar year on each lookup. This includes preseason data before August;
incremental selection retains the existing August season boundary. Post-load
verification checks catalog attachment/bounds and required horizon coverage.

Read-only production inspection on 2026-09-08 found `core.plays` partitioned by
`LIST (season)` with exactly 23 correctly attached, single-season partitions,
`core.plays_y2004` through `core.plays_y2026`, and no detached names matching that
pattern. The 2027 partition is absent. F08 development did not apply production
DDL; the first authorized maintenance or ingestion run must provision it.

Validation on disposable PostgreSQL 17 passed rollover and historical-row
routing, inherited partition-index creation, idempotence, rejection of catalog
drift, and rollback after the first of two partition creations succeeds. The
partition tests run in the CI PostgreSQL job. This verifies executed SQL and
loader preflight ordering with fakes; it does not represent a live CFBD/dlt
end-to-end load or a production deployment.

## Plays index ownership (F09)

The historical plays table swap could leave expected index names on `plays_old`.
The current production catalog is already correct: read-only inspection on
2026-09-08 found no old table and nine valid index families covering all 23
partitions. See [the F09 evidence and scope](plans/2026-09-08-f09-plays-index-ownership.md)
for definitions, executed query plans, and measurement limits.

The plays runner now validates the `plays_dlt_id_unique` family after partition
maintenance and before fetching data, on the same destination as dlt. Missing,
misowned, structurally different, or incompletely attached identity indexes fail
the load. This read-only check does not rebuild indexes during ingestion.

To inspect all nine reviewed index families on an explicitly selected target:

```bash
WAREHOUSE_DB_URL='<authorized PostgreSQL URL>' \
  .venv/bin/python scripts/maintain_play_indexes.py
```

For an affected warehouse, select each justified index explicitly:

```bash
WAREHOUSE_DB_URL='<authorized PostgreSQL URL>' \
  .venv/bin/python scripts/maintain_play_indexes.py \
  --apply --index plays_dlt_id_unique --index idx_plays_game_id
```

The command reads `WAREHOUSE_DB_URL` (or an explicit `--db-url`), not implicit dlt
credentials. Inspection is read-only and returns a nonzero exit status for
invalid selected state. Repair requires an idle dedicated connection and runs
in one transaction with a 10-second lock timeout and 30-minute statement timeout.
**Schedule repair for an authorized maintenance window:** ordinary parent index
builds block writes while they run; this command does not claim concurrent online
creation. It validates every selected repair first, renames recognized old-heap
indexes to `plays_old_<index_name>`, creates missing selected parent indexes and
their child indexes, and verifies the result before committing. Repeating a
completed repair is a no-op. Any failure rolls back the selected batch.

Unexpected targets or definitions, existing rename targets, equivalent differently
named parent indexes, and invalid or incomplete index trees require separate investigation.
The command does not drop old storage, alter consumer grants, recreate obsolete
historical indexes, or choose which optional indexes are worth their write cost.

## Box-score and roster request failures

The `/games/teams`, `/games/players`, and `/roster` loaders stop their resource
when a request fails after the shared client's retries. A successful empty list
is `expected_no_data`; HTTP errors (including 400/404), invalid JSON, invalid
record lists, missing provider IDs, and local budget exhaustion are failures.
These resources do not add a second retry loop.

Each request logs a receipt with its endpoint, season/week or team/season,
outcome, and fetched row count when available. A failure reports `succeeded`,
`expected_no_data`, `failed`, and `deferred` request counts. `deferred` means
requests left unattempted when this resource invocation stops. These are fetch
outcomes for one invocation, not counts of committed rows or the entire job;
weekly/year-batch loads may already have completed earlier invocations.

`load_season()` retains this context in the failed source's `request_failure`
summary field even when dlt wraps the original error. The summary's `http_status`
is the response code for `HTTPStatusError`, or `null` for other error types.
Rate-limit exhaustion and circuit-open failures retain their distinct `error_type`.
Both season and pipeline CLIs exit nonzero on source failure. An all-source run
can continue independent sources, but its final status remains failed. Existing
mart-refresh policy is
unchanged; a failed load does not establish downstream freshness.

Inspect the failed request and earlier load results before selecting a retry
scope. Existing rows and an older successful run do not prove completeness.
Historical gap detection and correction-aware backfills require separate work.

## Weekly EPA for scheduled games

If all earlier qualifying plays have the same home-offense indicator (including
neutral-site-only openings), the weekly builder logs and omits those fits:
HFA is unidentifiable. Consumers keep their prior-season/NULL fallback until
both indicator values occur in strictly earlier plays. Rebuilds remove stale
snapshots when corrected inputs lose that variation.

`compute_adjusted_epa_week.py` takes its target weeks from `core.games`, including
unplayed games. Each entering-week W snapshot uses available qualifying plays
strictly before W, with postseason encoded as week + 100. A missing play in W
no longer prevents W's snapshot. Sparse schedule gaps do not create artificial
weeks; targets with the same input state reuse the ridge solution.

The daily workflow runs this builder before `build_features.py` and
`score_fitted.py`. Keep that ordering for an authorized manual rebuild. Rebuilding
a season replaces its weekly snapshots, so corrections to earlier plays reach
later boundaries. A future snapshot can include only part of an earlier week
during a midweek run; its existence does not certify ingestion completeness.
Features still require 150 offensive plays before selecting weekly EPA, then
fall back to the labeled prior-season fit or NULL. Teams absent from the
season's play data retain that fallback behavior.

After rollout, verify representative staging and feature rows at an upcoming
week, compare their play counts and coefficients with strictly earlier plays,
and check `adj_epa_source` before interpreting prediction changes. The printed
full-season correlation is informational and cannot establish temporal safety
or coverage. Historical repairs and prediction regeneration need separately
scoped runs; a code merge alone does not refresh stored outputs. The live blend
path using full-season EPA remains separate from the weekly feature and
as-of-backfill consumers covered here.

## Season closure and fit eligibility

`src/pipelines/season_lifecycle.py` owns finished-season decisions for loader
skips, player overviews, and automatic refits. Closure requires a sufficiently
populated regular/postseason schedule, the calendar floor, complete scored
results with known dates, and no future or unresolved contests. The former 99%
tolerance is removed. Unknown or old unresolved records stay open until
reconciled; only reviewed cancellation IDs are terminal without results.

The unattended finalized-season path still fetches `/games` with a schedule-only
load. Explicit `--season`, `--sources`, and `--no-skip-final` requests retain full
loading behavior. A newly discovered unfinished game reopens the next lifecycle
assessment; expensive sources skipped earlier in that run resume on the next
unattended run, or immediately through an explicit source load.

Upcoming fitted predictions select a fit strictly earlier than each pending
season and no later than the shared contiguous closed training frontier.
Missing eligible fits fail before writes. Every pending season must separately
meet the 90% feature/scoring coverage threshold before any upcoming write.
The post-load verifier applies the same canonical pending population and
per-season threshold.
Backfills still require exactly the previous-season vintage. The reviewed event
crosswalk in `src/pipelines/game_identity.py` excludes known superseded originals
from modeled inputs while preserving raw data. The projection schedule also
requires a matching replacement. Do not infer cancellations from age, 0–0
scores, missing provider rows, or matching team names.

See [F03 plan](plans/2026-09-06-f03-season-lifecycle.md) for acceptance and rollout
limits. No existing materialization or stored prediction is repaired merely by
merging these query changes.

## Season outlook schedule drift

`api.season_outlook.games_scheduled` belongs to the saved projection snapshot;
it need not equal today's mutable `core.games` count. CI tests the producer's
regular-season filter using fixed PostgreSQL CTE fixtures and separately warns
on current-schedule drift. A drift warning needs an operational investigation:
inspect the affected teams' schedule corrections, projection `computed_at`,
and the last daily simulation step. Rebuild projections only for an authorized
target after confirming the inputs. Passing the producer test does not establish
that stored projections are fresh.

## Incident notes preserved from CLAUDE.md on 2026-09-04

The following is a dated account of previous fixes and the behavior believed to
exist when it was written. Dates, counts, costs, timings, and completion claims
are historical evidence, not current guarantees. Verify the relevant execution
path and current audit findings before relying on a statement below.

### Daily Automation

`.github/workflows/daily-load.yml` runs daily at 10:00 UTC from `main`: loads the
current season (`scripts/load_season.py --weekly`, mart refresh included), refits house
Elo/adjusted EPA (including the as-of weekly EPA build), refits fitted_v1 when it is stale
(`train_model.py --refit-if-stale`, a no-op on all but one day a year) and writes the
model's upcoming scores, then runs post-load checks (`scripts/verify_load.py`). Failures
open/update a rolling GitHub issue.

- `verify_load.py` also runs the KTD7 variant-twin tripwire (`check_variant_twins`,
  backed by `src/pipelines/utils/variant_twins.py`): dlt sometimes splits a
  charting metric into a bigint base column plus a `<col>__v_double` twin, and
  every mart reading `stats.rushing_*`/`stats.passing_player_season` COALESCEs
  only the twins that existed when it was authored. A daily load that creates a
  NEW twin now FAILs the run naming the column instead of silently going NULL
  in the mart/api view/RPC until someone notices.

**Finished-season skip:** on the unattended path (no `--season`, no `--sources`)
`load_season.py` skips sources whose data cannot change once a season is complete
(`IMMUTABLE_ONCE_FINAL` -- plays, game_stats, ratings, recruiting, draft, ...).
Before this, every off-season run re-ingested the entire finished previous season
-- `get_current_season()` returns `year - 1` until August -- roughly 2,000 calls a
day against the then-75,000/month budget for immutable data. That is what exhausted
the quota behind the 2026-07-25 three-hour rate-limited run. `reference` and
`metrics_wp` are never skipped (cheap / already self-limiting), and the
upcoming-schedule refresh is unaffected. An explicit `--season` or `--sources`
disables the skip entirely, so a backfill is never silently turned into a no-op;
`--no-skip-final` forces it off on the daily path.

**Where the per-game fan-out actually lives:** `stats`, not `plays`. `plays` is
year+week (16 calls/season). The `stats` source's `play_stats` resource issues one
`/plays/stats` call **per game** (~1,640/season) and `rosters` one per team, which
is the cost that exhausted the quota. `play_stats` now requests **completed games
only** -- an unplayed game has no play stats, and from 2026-08-01 (when
`get_current_season()` rolled to 2026, a season that is not final, so nothing was
skipped) the daily load walked all 1,638 *scheduled* 2026 games every day, was
429'd partway through, and failed the whole `stats` extract package -- discarding
`player_returning`'s already-fetched payload and burst-blocking `ratings` and
`game_stats` behind it. Note that failure mode: a resource that dies inside a
source takes every sibling resource's data with it. Because the source is not uniformly priced --
its other seven resources are one call per year -- anything running daily must name
resources rather than take the whole source: `--sources stats:player_returning`
(one call), and `PRESEASON_STATS_RESOURCES` in `load_season.py` for the automated
path.

**Upcoming-season preseason inputs:** off-season the daily run targets `year - 1`,
so the finished-season skip drops every immutable source for it -- correct, but it
left the *upcoming* season with no ingest beyond the games/betting schedule refresh.
Returning production, preseason SP+, talent and team recruiting are published
progressively through spring/summer and were never requested at all (2026 had a
schedule loaded since spring and zero rows in all four on 2026-07-28). The
upcoming-season block now also refreshes `PRESEASON_INPUT_SOURCES` (~11 calls/day);
an unpublished endpoint returns empty and merges nothing, so it self-heals as each
lands. `rosters` stays out (one call per team, and it firms up in August).
`scripts/probe_offseason_availability.py` distinguishes "never asked" from "CFBD
has not published it yet".

**Season targeting:** the compute chain's `--incremental` resolves target seasons from
`core.games` via `get_projection_seasons()` -- the most recent season with completed games
plus every later season with a published schedule -- **not** from `get_current_season()`,
which is a calendar rule returning `year - 1` until August and is correct only for ingest
year windows. `verify_load.py` asserts fitted_v1 covers >=90% of pending games so a missing
feature substrate cannot fail silently. Requires repo secrets `CFBD_API_KEY` and `SUPABASE_DB_URL` (session
pooler). `.github/workflows/flat-files.yml` runs daily at 11:00 UTC to load flat-file
sources (massey ratings, nflverse draft/combine, SBR lines, availability reports) using a
hash-skip ledger in `meta.flat_file_loads` to avoid re-processing unchanged files. Requires
repo secret `SUPABASE_DB_URL` only. `.github/workflows/live-scoreboard.yml` separately polls
CFBD's `/scoreboard` every 5 minutes on Saturdays (games-today guard) to feed
`live.scoreboard_snapshots` and the house live win-probability model.

### SQL refresh dependency planning (2026-09-08)

The Python refresher now uses the checked SQL registry described in the
[dependency foundation](plans/2026-09-08-refresh-dependency-foundation.md).
Use `python scripts/refresh_marts.py --changed <schema.relation> --dry-run` to
inspect materialized-view descendants of inputs whose writes have committed.
Remove `--dry-run` only when refresh execution is intended. This command does not
run source ingestion or computation jobs. Existing `--views` calls remain exact
selections, now validated and dependency-ordered. Failed selected refreshes block
their selected descendants while independent views continue; the command fails
if any view failed or was blocked. Durable cross-run generation checks and the
SQL `refresh_all_marts()` RPC are outside this implementation.

## Prepared durable CFBD admission

The daily, historical, and live entrypoints have an opt-in durable quota path.
See the [F14 transport rollout guide](plans/2026-09-08-f14-transport-admission.md)
for migration 067, dedicated role/connection configuration, independent control
allowances, commit-failure behavior, and activation checks. Production defaults
remain unchanged until that separate rollout is authorized.

## Prepared house Elo publication receipts

The opt-in `compute_house_elo.py --full --publish-receipts` path publishes full
game output, the current team snapshot, the game mart and private receipts in
one transaction after validating its prepared input snapshot. Migration 068 and
runtime role membership must be deployed separately before using it. Existing
incremental computation and later mart refreshes invalidate current evidence.
See the [publication rollout guide](plans/2026-09-09-f14-publication-receipts.md)
for receipt semantics, lost-commit recovery, locking and activation boundaries.
