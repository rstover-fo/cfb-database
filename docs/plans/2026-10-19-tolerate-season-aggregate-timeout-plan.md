# Plan: one slow CFBD season-aggregate request must not fail the Daily Season Load

Date: 2026-10-19. Status: frozen spec.

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
