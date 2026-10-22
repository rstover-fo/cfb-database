# Plan: one slow CFBD season-aggregate request must not fail the Daily Season Load

Date: 2026-10-19. Status: frozen spec. Revision 2 (owner-approved tolerance
scope, same date) supersedes the "exactly one failure" rule below -- see the
"Revision 2" section at the end of this file. Revision 3 (owner-approved
source split, same date) supersedes Revision 2's "stats stays critical so a
Daily-#98-shaped success timeout still fails" conclusion by moving the flaky
resource OUT of the stats source -- see the "Revision 3" section.

## Incident

Daily Season Load run 39253582408 (2026-10-19, manual dispatch on 64dca35)
loaded 20 of 21 sources. The `passing` source's one `/passing/players/season`
request for 2026 read-timed-out on all 7 attempts (~30s each, backoff
2/4/8/16/30/60s, ~351s total), dlt raised ResourceExtractionError in pipe
`passing_player_season`, `scripts/load_season.py` counted the source failure
and exited 1, and the workflow skipped every later step: Elo, adjusted EPA,
predictions, feature substrate, projections, Tier 2/3 marts, flat files,
verify, recaps. The matching `/rushing/players/season` request answered in
~4s, so this was a slow upstream endpoint, not a client bug.

## Goal

1. Season-aggregate CFBD endpoints get a longer **read** timeout (120s)
   through a per-endpoint override in `CFBDClient`. The global 30s default
   does not change.
2. When a **non-critical** source still fails on a timeout after retries, the
   run records it as a tolerated partial success: loud log, run-summary
   warning (GitHub job summary when available), exit 0 so downstream steps
   run. At most ONE such downgrade per run; two or more source failures of
   any kind, or any critical-source failure, still fail the run (systemic
   CFBD outage must stay red -- mirrors the #152 outage-breaker idea).
3. Existing failure-issue behavior is untouched: any real failure still
   exits 1 and the workflow still opens/updates the issue.

## Critical vs non-critical classification (derived from consumers)

Non-critical: `passing`, `rushing`. Their tables (`stats.passing_*`,
`stats.rushing_*`) feed only the charting marts
(`marts.passing_charting_*`, `marts.rushing_charting_*`) and the cfb-app API
views. No daily downstream step (`compute_house_elo.py`,
`compute_adjusted_epa*.py`, `compute_predictions.py`, `build_features.py`,
`train_model.py`, `score_fitted.py`, `simulate_season.py`,
`backtest_preseason.py`, flat files, `verify_load.py`) reads them, and both
sources merge, so a missed day self-heals on the next successful run.

Critical (everything else in SOURCE_ORDER): `games` (Elo, predictions),
`plays` (adjusted EPA), `stats` (play_stats feeds player-EPA marts;
player_returning feeds features), `metrics`/`metrics_wp` (PPA features),
`betting` (scored edges), `ratings`, `rankings`, `recruiting`, `draft`,
`reference`, `conferences`, `coaches`, `coach_profiles`, `playoffs`,
`game_stats`, `wepa`, `player_overview`, `rosters`. Anything not proven
non-critical stays critical.

## Changes

### `src/pipelines/utils/api_client.py`

- Class constants on `CFBDClient`: `SEASON_AGGREGATE_READ_TIMEOUT = 120.0`
  and `SEASON_AGGREGATE_ENDPOINTS`, a frozenset of the bare-year
  season-total endpoints the warehouse requests (CFBD computes these across
  a whole season's rows at request time):
  `/passing/players/season`, `/passing/teams/season`,
  `/rushing/players/season`, `/rushing/teams/season`, `/stats/season`,
  `/stats/player/season`, `/stats/season/advanced`, `/stats/player/success`,
  `/player/usage`, `/player/returning`, `/ppa/teams`, `/ppa/players/season`,
  `/wepa/team/season`, `/wepa/players/passing`, `/wepa/players/rushing`,
  `/wepa/players/kicking`.
- Helper `_request_timeout(endpoint)` returning
  `httpx.Timeout(DEFAULT_TIMEOUT, read=SEASON_AGGREGATE_READ_TIMEOUT)` for
  members, else `None`.
- `_send_attempt` routes both of its `self._client.get(...)` calls through
  the override. When the override is None the call stays byte-identical to
  today (`self._client.get(endpoint, params=params)`) -- an existing test
  asserts that exact signature. Only member endpoints add `timeout=`.
- No change to `DEFAULT_TIMEOUT`, retry budgets, backoff, or the 429 breaker.

### `scripts/load_season.py`

- `NON_CRITICAL_SOURCES = frozenset({"passing", "rushing"})` with the
  classification rationale above as a comment.
- Timeout detection: walk the exception `__cause__`/`__context__` chain
  (same visited-set pattern as `request_failure_summary`) for
  `httpx.TimeoutException`. Record the verdict at failure time in the
  result dict (e.g. `"tolerable_timeout": True`) inside the source loop's
  except handler, so the downgrade decision is a pure function of results.
- Pure function `downgrade_tolerated_timeout(results) -> str | None`:
  among source-grain results, if there is EXACTLY ONE failure overall, its
  source is in `NON_CRITICAL_SOURCES`, and it was a timeout, set its status
  to `"tolerated"` and return the name; otherwise change nothing. Called
  after the source loop and the upcoming-schedule block, before mart
  refresh. On downgrade: `logger.warning` naming the source, and append a
  markdown warning to the file named by `$GITHUB_STEP_SUMMARY` when that
  env var is set.
- `_count_failures` ignores `"tolerated"` (and still counts `"partial"` --
  existing mart-refresh semantics unchanged). Summary print shows `WARN`
  for tolerated rows and a tolerated count in the totals line. Summary dict
  gains `"tolerated": [names]`. Exit-code logic in `main()` unchanged.

### Tests (no live DB, no live CFBD)

`tests/test_api_client.py`: aggregate endpoint passes `timeout=` with
read 120.0 / connect 30.0; non-aggregate endpoint keeps today's exact call
signature; membership spot checks (season endpoints in, week-grain
endpoints out); `DEFAULT_TIMEOUT` still 30.0.

`tests/test_load_season.py`: (a) passing timeout (exception chained from
`httpx.ReadTimeout`) + healthy games -> status `tolerated`, `errors == 0`
(downstream allowed); (b) critical-source timeout -> stays `error`,
`errors == 1`; (c) passing AND rushing both time out -> no downgrade,
`errors == 2`; (d) non-timeout passing failure -> stays `error`;
(e) `$GITHUB_STEP_SUMMARY` file receives the warning; (f) `_count_failures`
ignores tolerated, still counts partial.

## Non-goals

No change to the global timeout, retry budgets, the 429 circuit breaker,
workflow YAML, dlt sources, `verify_load.py`, Supabase schema/migrations.
No new dependencies. No secrets in code, commits, or PR text.

## Verification

- `.venv/bin/ruff check` and `.venv/bin/ruff format --check` on changed files
- `PATH="$PWD/.venv/bin:$PATH" .venv/bin/python -m pytest tests/test_api_client.py tests/test_load_season.py -q --tb=short`
- Full suite: `PATH="$PWD/.venv/bin:$PATH" .venv/bin/python -m pytest -q --tb=short`

## Revision 2: owner-approved tolerance scope (2026-10-19)

Review findings on PR #155 (Greptile P1, Codex P2 at load_season.py:719)
showed the single-failure downgrade also applied to backfills and manual
runs. The owner's decision widens the rule for the scheduled daily load and
makes it strictly opt-in everywhere else.

### Rule

- **Opt-in only.** A new `--tolerate-aggregate-timeouts` flag on
  `scripts/load_season.py`, off by default. Without it there is NO downgrade
  of any kind -- backfills, `historical-refresh`, `backfill-sources`, and
  manual CLI calls stay strict. Only the daily-load.yml load step passes the
  flag, and only on its unattended branch (no `season` input); dispatching
  daily-load.yml with an explicit season behaves like a backfill and stays
  strict.
- **Any number of tolerable timeouts.** With the flag, every failed source
  may be downgraded to `tolerated` when ALL failures in the run are
  (non-critical source) AND (an `httpx.TimeoutException` in the exception
  chain). If even one failure is critical or non-timeout, nothing is
  downgraded and the run fails. A `partial` mart refresh still fails the run
  regardless of downgrades, so "green" requires every critical source loaded
  AND the mart refresh succeeded.
- **Repeat-miss guard.** The run still fails when the same source has been
  tolerated on consecutive daily runs reaching a limit
  (`--tolerated-streak-limit`, default 3: two consecutive tolerated days
  pass, the third fails). State is a small JSON file
  (`{"version": 1, "streaks": {source: {"count": N, "last_date": "YYYY-MM-DD"}}}`)
  carried between runs by the GitHub Actions cache
  (`actions/cache/restore` with a `restore-keys` prefix before the load,
  `actions/cache/save` with `if: always()` after it), path handed to the
  script via the `TOLERATED_STREAK_FILE` env var. No database table, no
  migration, no live write -- deliberate prototype per the owner decision.
  "Consecutive" is literal (adversarial-review finding): a streak continues
  only when the previous toleration was yesterday (UTC); a same-date rerun
  counts once, and any older `last_date` -- a gap day, skipped runs, or an
  entry preserved across unrelated failures -- restarts the count at 1.
  Entries older than yesterday are pruned on update. Known limits, accepted:
  cache eviction (~7 days unused) or manual cache deletion resets a streak.
  Corrupt or unreadable state degrades to an empty state with a logged
  warning, a malformed nested entry is dropped with a warning
  (adversarial-review finding), and an unwritable state file never crashes
  the run (same best-effort hygiene as the step-summary write).
- Streak counts increment for every tolerable timeout (even on a run that
  ultimately fails), reset for every source that loads OK, and persist
  best-effort whenever the flag is on and `TOLERATED_STREAK_FILE` is set.

### Evidence-based classification of season-total endpoint owners

A dlt source failure discards every sibling resource's extract (see
passing.py's module docstring), so tolerance is decided at source grain: a
source is non-critical only when NONE of its tables has a downstream reader
outside its own charting marts/views. Consumer sweep (grep over
src/schemas/marts, src/schemas/api, src/schemas/public, src/schemas/scouting,
mcp/, scripts/, src/pipelines/config) on 2026-10-19:

| Source | Season-total tables | Readers found | Verdict |
|---|---|---|---|
| passing | stats.passing_player_season, stats.passing_team_season | marts.passing_charting_* and api passing views only | NON-CRITICAL |
| rushing | stats.rushing_player_season, stats.rushing_team_season | marts.rushing_charting_* and api rushing views only | NON-CRITICAL |
| stats | stats.player_success_season | none (grants/catalog only) | source stays CRITICAL: siblings below share its extract |
| stats | stats.advanced_team_stats, stats.player_returning | scripts/build_features.py, scripts/screen_preseason_features.py, marts.returning_production | CRITICAL |
| stats | stats.player_season_stats, stats.team_season_stats, stats.player_usage | api.player_season_leaders, api.player_detail, api.player_usage_leaders, public.player_stats_function, scouting player mart, marts.player_comparison, marts.defensive_havoc, marts.team_penalty_box, marts.player_usage | CRITICAL |
| metrics | metrics.ppa_players_season, metrics.ppa_teams | api.player_detail, marts.player_comparison, scouting player mart | CRITICAL |
| wepa | metrics.wepa_team_season, metrics.wepa_players_* | marts.team_wepa_season -> marts.team_adjusted_epa (daily Tier 2/3 refresh), marts.player_wepa_season | CRITICAL |

`stats.player_success_season` itself has no reader, but a timeout on
`/stats/player/success` fails the whole `stats` source and discards
play_stats, player_returning, advanced_team_stats, and the rest of that
run's stats extract -- all consumed downstream -- so `stats` remains
critical and a Daily-#98-shaped stats timeout still fails the run.
NON_CRITICAL_SOURCES therefore stays `{"passing", "rushing"}`.

### Tests (no live DB, no live CFBD)

- passing + rushing both time out WITH the flag -> both `tolerated`,
  errors == 0 (run green).
- The same two failures WITHOUT the flag -> both `error`, errors == 2.
- Critical timeouts WITH the flag (games; stats) -> `error`, run fails.
- Mixed tolerable + critical WITH the flag -> nothing downgraded.
- Non-timeout passing failure WITH the flag -> `error`.
- Streak guard: primed state at count 2 with limit 3 -> third day fails and
  the state file records count 3; count 1 -> tolerated and state shows 2;
  a source that loads OK has its streak cleared; same-UTC-date rerun does
  not double-increment; corrupt state degrades to empty with a warning;
  unwritable state file does not crash the run.
- Workflow YAML: daily-load.yml parses; the flag appears only on the
  unattended (no season input) branch; cache restore/save steps exist.

### Verification

Same ruff/pytest commands as revision 1, plus `actionlint` on
`.github/workflows/daily-load.yml`.

## Revision 3: split player_success out of the stats source (2026-10-19)

Incident: Daily Season Load #98 (2026-10-19, scheduled). The one
`/stats/player/success` request for 2026 read-timed-out after all retries.
Because `player_success_season_resource` shared the `stats` dlt source, the
whole stats extract was discarded -- advanced_team_stats, player_returning,
player_season_stats, player_usage, play_stats and the rest -- and the run
went red with every downstream step skipped. Revision 2 had anticipated
exactly this shape and accepted it ("stats remains critical"); the owner
approved removing the shared-extract coupling instead, so the flaky endpoint
stops reddening the daily load without weakening the critical data.

### Rule

- `player_success_season_resource` moves into its own dlt source,
  `player_success_source` (still in `src/pipelines/sources/stats.py`), with
  its own runner `run.py::run_player_success_pipeline` and load unit
  `player_success` in load_season.py's SOURCE_ORDER, directly after
  `stats`. The resource function is shared, so the table name
  (stats.player_success_season), columns, merge disposition, and primary
  key (season, id, team) are byte-identical: no migration, no DB change,
  existing rows untouched.
- The split source and runner DELIBERATELY keep the `cfbd_stats` dlt
  identity -- `@dlt.source(name="cfbd_stats")` and
  `pipeline_name="cfbd_stats"` / `dataset_name="stats"` (Codex review on
  PR #157, P1; first drafted as `cfbd_player_success`). The table's
  destination layout -- bigint success-rate base columns plus
  `passing__success_rate__v_double` / `rushing__success_rate__v_double`
  variant twins (src/schemas/baseline/20260907_pre_f05.sql) -- is a product
  of the stored `cfbd_stats` schema's history. A new schema/pipeline name
  starts a fresh dlt schema that infers those fields as double and
  conflicts with the existing bigint destination columns on its first load:
  a non-timeout failure (or worse, a silently coerced value) on an
  otherwise healthy request. Reusing the identity makes dlt restore the
  stored schema from the destination (keyed by pipeline_name, schemas by
  name from `_dlt_version`) and normalize byte-identically to the pre-split
  load. Failure isolation is unaffected: it comes from the separate
  pipeline.run() call, not the schema name. Accepted coupling: a pending
  load package left by a stats run that failed at the LOAD stage is retried
  by whichever `cfbd_stats` run comes next, possibly the player_success
  unit -- in that scenario the run is already red from the stats failure,
  so the retry can only recover data, never hide a failure.
- `player_success` joins NON_CRITICAL_SOURCES alongside passing/rushing,
  under the identical Revision 2 machinery: httpx-timeout-only, downgrade
  strictly opt-in via `--tolerate-aggregate-timeouts` (daily-load.yml's
  unattended branch only -- no workflow change needed), and the same
  consecutive-day streak guard (`--tolerated-streak-limit`, default 3),
  keyed by the new source name in the existing cache-carried state file.
- `stats` stays CRITICAL, and now a timeout anywhere in its remaining
  resources fails the run while costing nothing it didn't cost before.
- `player_success` joins IMMUTABLE_ONCE_FINAL (same policy as stats) and
  ESTIMATED_CALLS at 1 call/year; the stats estimate drops 1,675 -> 1,674.
- Wiring parity: SOURCE_ORDER, ESTIMATED_CALLS, IMMUTABLE_ONCE_FINAL,
  load_season runners, `run.py --source player_success`, active-by-default.
  `--sources stats:player_success_season` now fails validation naming the
  valid stats resources (the resource is no longer in that source); the
  replacement is `--sources player_success`.

### Reader sweep for stats.player_success_season (re-run 2026-10-19)

Grep for `player_success_season` / `player_success` over src/schemas/marts,
src/schemas/api, src/schemas/public, src/schemas/scouting, mcp/, scripts/,
src/pipelines/config, flat files, and the recaps path. Hits, exhaustively:
the pipeline itself (stats.py, endpoints.py, load_season.py estimates),
tests, docs (pipeline-manifest, handoff, plans), migration 050 and the
baseline dump (grants, index, column comments), the production catalog
metadata JSON, and comment cross-references in passing/rushing/overview
modules citing its PK reasoning. No mart, API view, RPC, scouting relation,
MCP tool, compute script, flat-file loader, or recap generator reads the
table. Verdict unchanged from Revision 2: no reader, so the table's own
load unit is safely non-critical.

### Tests (no live DB, no live CFBD)

- (a) /stats/player/success timeout WITH the flag -> stats source still runs
  and reports ok, player_success `tolerated`, errors == 0.
- (b) the same timeout WITHOUT the flag -> `error`, errors == 1.
- (c) a timeout on a remaining stats endpoint WITH the flag -> stats
  `error`, run fails (player_success itself ok).
- (d) Daily #98 shape (success + passing timeouts) WITH the flag -> both
  `tolerated`, errors == 0.
- (e) streak guard: primed count 2 with limit 3 -> third day fails and the
  state records count 3; primed count 1 -> tolerated and state shows 2.
- (f) split preserves the contract: player_success_source contains exactly
  player_success_season; stats_source no longer carries it; the union across
  both sources equals the pre-split resource set; table name, merge
  disposition, and primary key unchanged; the dlt schema name
  (`cfbd_stats`) and the runner's pipeline_name/dataset_name identity are
  pinned so the stored variant-column layout keeps being reused.
- Wiring: SOURCE_ORDER position, ESTIMATED_CALLS, IMMUTABLE_ONCE_FINAL,
  NON_CRITICAL_SOURCES == {passing, rushing, player_success}, dry-run
  pricing.

### Verification

Same ruff/pytest commands as revision 1, plus `actionlint` on
`.github/workflows/daily-load.yml` (unchanged by this revision, re-linted).
