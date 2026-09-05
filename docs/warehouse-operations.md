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
| Refresh existing marts | `scripts/refresh_marts.py` |
| Apply definitions / migrations | `scripts/run_marts.py`; `scripts/run_migrations.py` |
| One-off SQL application | `scripts/run_migrations.py --file <path>`; follow that file's application contract |
| Flat-file ingestion | `scripts/load_flat_files.py --due` |
| Check upstream preseason availability | `scripts/probe_offseason_availability.py` |

Read relevant workflow/CLI options before execution. Direct application is a
separate action from preparing SQL. The session pooler is used by warehouse jobs;
transaction pooling does not accept all startup/session options these jobs use.
Secrets belong in ignored config or the host's credential mechanism, never docs.
The request budget is configured in `.dlt/config.toml`; historical estimates do
not establish the cost of today's source/resource selection.

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
summary field even when dlt wraps the original error. Both season and pipeline
CLIs exit nonzero on source failure. An all-source run can continue independent
sources, but its final status remains failed. Existing mart-refresh policy is
unchanged; a failed load does not establish downstream freshness.

Inspect the failed request and earlier load results before selecting a retry
scope. Existing rows and an older successful run do not prove completeness.
Historical gap detection and correction-aware backfills require separate work.

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
