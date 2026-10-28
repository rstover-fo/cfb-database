# Plan: one slow CFBD season-aggregate request must not fail the Daily Season Load

Date: 2026-10-19. Status: frozen spec. Revision 2 (owner-approved tolerance
scope, same date) supersedes the "exactly one failure" rule below -- see the
"Revision 2" section at the end of this file.

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

## Revision 4: tolerate 504s; split passing season aggregates (2026-10-22)

Numbering note: Revision 3 (the player_success source split) was written in
PR #157, which was merged into PR #155's head branch 35 seconds AFTER #155
itself was squash-merged -- so Revision 3's code and doc text never reached
main. Its pattern is reused here (and its own re-land is a separate
decision); this revision keeps the number 4 to match PR #157's history.

### Incident

Daily Season Load #99-#101 (runs 39253582408 .. 39796553707, 2026-10-20..22,
scheduled): `/passing/players/season` for 2026 timed out after all retries,
three days straight, failing the whole job (20 of 21 units OK, exit 1) and
skipping every downstream daily step. Two failure shapes:

- #99-#100 predate PR #155's merge (2026-10-21 19:12 UTC; the cron fires
  09:23 UTC), so they ran without the tolerance flag or the raised read
  budget and failed on the plain 30s read timeout.
- #101 ran WITH `--tolerate-aggregate-timeouts` and the 120s season-aggregate
  read budget -- and still failed. The raised budget now outlasts CFBD's own
  gateway, so the same slow endpoint answered **HTTP 504 Gateway Timeout** on
  every attempt (seven, ~1,018s total). A 504 raises httpx.HTTPStatusError,
  not httpx.TimeoutException, so `_exception_has_timeout` never marked the
  failure tolerable and the run stayed strict.

A second cost, same shape as Daily #98's stats incident: the season resource
shared the `cfbd_passing` source with the three week-grain resources, so each
timeout also discarded the ~60 week-grain calls already fetched that run.

### Rules

- An exhausted **504** anywhere in a failure's exception chain now counts as
  a timeout for the tolerance predicate (`_is_timeout_exception`). Only 504:
  a 500/502/503 is an outage, not a slow season-total computation, and stays
  strict. The NON_CRITICAL_SOURCES gate, the opt-in flag, and the streak
  guard all apply unchanged.
- `passing_player_season_resource` and `passing_team_season_resource` move
  into their own dlt source, `passing_season_source` (still in passing.py),
  with runner `run.py::run_passing_season_pipeline` and load unit
  `passing_season` in SOURCE_ORDER, directly after `passing`. Both sources
  and the runner DELIBERATELY keep the `cfbd_passing` schema/pipeline
  identity and `dataset_name="stats"` (Revision 3's P1 finding): a new
  identity would start a fresh dlt schema and re-infer column types against
  the live destination tables on first load instead of adopting them. Tables,
  merge dispositions, and primary keys are byte-identical; no migration.
- `passing_season` joins NON_CRITICAL_SOURCES (set becomes
  `{passing, passing_season, rushing}` -- same reader sweep as the Revision 2
  table's passing row), IMMUTABLE_ONCE_FINAL, and ESTIMATED_CALLS at 2;
  `passing` drops 62 -> 60. A full passing re-pull becomes
  `--sources passing,passing_season`.
- Every failed unit's exception type and message print in the end-of-run
  summary block (bounded to one line each, root cause included) and in a
  "Failed load unit(s)" section of GITHUB_STEP_SUMMARY -- run #101's
  traceback did not survive the web UI's log truncation and the unit was
  misdiagnosed from an older failure's shape.

### Tests (no live DB, no live CFBD)

- 504 on passing_season WITH the flag -> tolerated, week-grain passing OK,
  errors == 0; the read-timeout shape likewise.
- 504 WITHOUT the flag -> error (backfills stay strict).
- 502 on passing_season WITH the flag -> error (only 504 downgrades).
- 504 on a critical source (games) WITH the flag -> error.
- Streak guard applies to the new unit (primed count 2, limit 3 -> third
  day fails).
- Split preserves the contract: source resource sets, schemas, PKs, and the
  cfbd_passing schema/pipeline identity (source-text pin on the runner).
- Summary detail: failed units print type/message/root-cause; tolerated
  units do not; long messages collapse and truncate; GITHUB_STEP_SUMMARY
  gets the failure section; an unwritable summary file cannot mask the
  failure.

### Verification

Same ruff/pytest commands as revision 1.
