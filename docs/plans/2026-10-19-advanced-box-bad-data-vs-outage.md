# Advanced-box drain: distinguish bad-data game clusters from CFBD outages

Status: frozen spec (implementation in the same PR as this file).

## Goal

The Historical Refresh workflow (`.github/workflows/historical-refresh.yml`,
`scripts/backfill_refresh.py`) must stop stalling permanently on a small
cluster of games that `/game/box/advanced` cannot serve, while a real CFBD
outage still fails the run (exit 1) so the workflow's failure-issue step
fires.

## Incident (run 39253607809, 2026-10-19)

PR #152's `AdvancedBoxScoreCircuitOpen` in `src/pipelines/sources/stats.py`
trips after 3 consecutive games return 5xx (post-retries) and raises from
inside the dlt resource generator. In that run the first batch of 50 made 11
calls: 500, 500, then five 200s (401836219, 401833987, 401833986, 401833985,
401762521 — the 200s reset the streak), then 500 x3 tripped the breaker. All
six 500 games fall in ids 401837096-401838337. CFBD was up; those specific
games are bad upstream data. Two bugs:

- (a) The raise aborts `pipeline.run()` mid-extract, so NOTHING from the
  batch lands: not the five successes' rows, not the ledger rows
  (`insert_ledger_rows` is never reached). The backlog stays at 28,041 and
  the 11 calls are wasted.
- (b) The backlog is drained newest-first, so every subsequent scheduled run
  replays the same batch head and trips at the same spot, forever.

## Design

### 1. Windowed trip rule replaces "3 in a row"

New module-level health tracker in `src/pipelines/sources/stats.py`
(replacing `_ADVANCED_BOX_5XX_BREAKER`, `CONSECUTIVE_5XX_CIRCUIT_THRESHOLD`,
`reset_advanced_box_5xx_circuit`, and the `AdvancedBoxScoreCircuitOpen`
exception — the raise-from-the-generator design is what loses the batch):

- `AdvancedBoxHealth`: lock-guarded sliding window (`collections.deque`,
  `maxlen=ADVANCED_BOX_HEALTH_WINDOW = 10`) of per-game outcomes for
  `/game/box/advanced`, shared across batches in the process (same rationale
  as the old breaker: `scripts/backfill_refresh.py` builds a fresh resource
  per batch). One entry per distinct game attempted (the backlog
  set-difference guarantees a game is attempted at most once per run).
  - `record_answered(game_id, *, has_data: bool)`: any server answer — a
    200 with data, an empty 200, or a 400. When `has_data`, also remember
    `last_success_game_id` (the probe candidate).
  - `record_server_error(game_id)`: a 5xx after the client's transient
    retries were exhausted.
  - `should_probe()`: True when the window holds `>= ADVANCED_BOX_HEALTH_5XX_TRIP
    = 5` server-error games AND zero answered games.
  - `clear_window()` (used after a successful probe) and `reset()` (full
    reset, exposed as module function `reset_advanced_box_health()` for
    tests and in-process callers).
  - Module instance `_ADVANCED_BOX_HEALTH`; keep it private, tests use
    `reset_advanced_box_health()` in an autouse fixture.

Why this rule: the probe (below) is now the outage/bad-data discriminator,
so the trip only decides when to spend ONE extra confirmation call. A window
with any answered game never trips — the Oct 19 pattern (two 500s, five
200s, four 500s) drains without even probing, recording the 500 games as
`no_data`. A dense bad-data cluster with zero interleaved successes probes
at most once per 5 games (the window is cleared after a healthy probe and
must refill to the trip count). A real outage (every call 5xx) is confirmed
within at most 10 games of the last success — and each counted 5xx already
survived the client's full 7-attempt transient budget, so that is dozens of
consecutive failed HTTP exchanges, not bad luck.

### 2. Probe (re-check) on trip, inside the resource

New frozen dataclass-style mutable report passed per batch:

```python
class AdvancedBoxBatchReport:
    fallback_probe_game_id: int | None   # input, from the caller (ledger)
    attempted: list[int]                 # output: ids a call was spent on
    server_error_game_ids: list[int]     # output: 5xx-after-retries ids (subset of misses)
    probe_game_id: int | None            # output: id probed, if a probe fired
    outage: str | None                   # output: message when probe confirmed outage
```

`advanced_game_stats_resource(game_ids, *, misses=None, report=None)`:

- `misses` behavior unchanged (400, empty 200, and — with misses — 5xx
  post-retries append the id; see existing docstring).
- Health tracking, trip, probe, and outage signaling activate only when
  `report is not None` (the backfill always passes both; a plain
  `misses`-only caller gets the old pre-#152 suppress-and-continue
  behavior, documented in the docstring).
- Append to `report.attempted` on every handled outcome (success, empty
  200, 400, suppressed 5xx) — NOT on an unhandled exception, because an
  unhandled raise aborts the dlt load and the caller must not ledger games
  whose data never landed.
- On a suppressed 5xx with `report`: record the miss, append to
  `server_error_game_ids`, `record_server_error`, then if `should_probe()`:
  - Probe id precedence: `health.last_success_game_id` (most recent
    data-bearing 200 this process) -> `report.fallback_probe_game_id`
    (caller-supplied, from the ledger) -> module constant
    `ADVANCED_BOX_PROBE_FALLBACK_GAME_ID = 401836219` (a game that
    returned 200 with data in run 39253607809).
  - Probe = one `make_request(client, "/game/box/advanced", params={"id": probe_id})`
    (full normal retry budget). Its payload is discarded — never yielded,
    never ledgered (the probe game is already loaded).
  - Probe healthy (returns anything, or raises `httpx.HTTPStatusError`
    with status < 500): CFBD is up, the recent 5xx games are bad data.
    Log a warning, `clear_window()`, keep draining. The bad games are
    already in `misses` so they land as `no_data` (requeueable with
    `--requeue-no-data`).
  - Probe failed (HTTPStatusError >= 500, or ANY other exception —
    transport errors, rate-limit exhaustion): outage. Set `report.outage`
    to a message naming the probe id, the last 5xx game id, and the window
    counts; log an error; `return` from the generator — do NOT raise.
    Returning normally lets dlt load every row yielded so far, which is
    what fixes bug (a).
- In report mode, NO exception may escape the generator at all (review
  finding, 2026-10-19): a transport failure, rate-limit exhaustion, or a
  non-400 4xx on a REGULAR game call also sets `report.outage` and
  returns — a raise there would abort the dlt load and lose the batch
  exactly like bug (a), and a timeout-shaped outage would otherwise never
  reach the probe. The failing game is not appended to `attempted` (its
  outcome is unknown), so it stays in the backlog. Plain mode and
  misses-only mode keep raising.

### 3. `scripts/backfill_refresh.py`: save on every exit path, partial status

- New ledger query + helper `fetch_last_refreshed_game_id(conn, campaign,
  task) -> int | None`: `SELECT game_id FROM meta.refresh_progress WHERE
  campaign = %s AND task = %s AND status = 'refreshed' ORDER BY
  refreshed_at DESC LIMIT 1`. Computed once before the batch loop when any
  `box_advanced` batch is planned; passed into `run_batch`.
- `run_batch(pipeline, task, game_ids, *, fallback_probe_game_id=None)` now
  returns `(info, misses, report)` — `report` is an
  `AdvancedBoxBatchReport` for `box_advanced`, `None` for `plays_stats`.
- Batch loop:
  - `attempted = batch if report is None else report.attempted`;
    `insert_ledger_rows(conn, campaign, task, attempted, misses=misses)`
    and `month_spend`/`loaded_total` advance by `len(attempted)` — so a
    batch stopped mid-way by an outage still commits its successes and
    `no_data` rows, and unattempted games stay in the backlog with no
    ledger row (no call was spent on them).
  - Accumulate `report.server_error_game_ids` across batches into
    `bad_data_game_ids`.
  - On `report.outage`: print it and break out of the loop.
- After the loop, the existing finalize block runs unchanged (an outage is
  CFBD-side; finalize is DB-only and the loaded corrections should still
  propagate; a finalize failure still returns `finalize_failed`).
- Then, when an outage was confirmed, return
  `{"status": "outage", "outage": <message>, "bad_data_game_ids": [...], ...}`
  BEFORE the end-of-run completion check. Normal returns gain
  `"bad_data_game_ids"`.
- `main()`:
  - quota outcome: `status == "outage"` maps to `operation.outcome = "failed"`.
  - New helper `report_partial_bad_data(result)` (unit-testable; reads
    `GITHUB_OUTPUT`/`GITHUB_STEP_SUMMARY` paths from `os.environ`): when
    the run is NOT exiting 1 and `bad_data_game_ids` is non-empty, print a
    `::warning::` annotation line, append a markdown line to
    `GITHUB_STEP_SUMMARY` (when set), and append
    `partial_bad_data=true`, `bad_data_count=<n>`, and
    `bad_data_game_ids=<comma-separated>` to `GITHUB_OUTPUT` (when set).
  - Exit code: `sys.exit(1 if result.get("status") in {"finalize_failed",
    "outage"} else 0)`. The outage path exits 1 via a clean printed message,
    not a traceback — observable behavior (red run, failure-issue step)
    unchanged.
- Probe calls are not ledgered and so not counted by the month guard;
  because a healthy probe clears the window, the worst case is one probe
  per 5 drained games (a dense all-bad cluster with no interleaved
  successes) — note this in a comment.
- Update the module docstring's 5xx paragraph to the new design.

### 4. `.github/workflows/historical-refresh.yml`

- Give the "Drain backlog" step `id: drain`.
- New step after it, `if: steps.drain.outputs.partial_bad_data == 'true'`,
  `actions/github-script@v7`, with the ids routed through step-level `env:`
  (`BAD_DATA_GAME_IDS: ${{ steps.drain.outputs.bad_data_game_ids }}`,
  read via `process.env` — same hardening pattern as the drain step):
  comment on the open issue titled "Historical refresh failing" (create it
  when absent, mirroring the failure step) with the run URL and the
  `no_data` game ids, noting they are requeueable with
  `--requeue-no-data`.
- Keep the existing `if: failure()` step byte-identical.
- Update the header comment paragraph about the 5xx breaker.
- Do NOT touch the cron schedule, concurrency group, permissions, or
  secrets wiring.

## Non-goals

- No change to the cron schedule (separate PR), branch protection, review
  requirements, Supabase schema, or migrations.
- No change to `plays_stats` behavior, the 429 `RateLimitBreaker` path in
  `src/pipelines/utils/api_client.py`, or `make_request` retry semantics.
- No change to the ledger schema (`meta.refresh_progress` statuses stay
  `refreshed`/`no_data`).
- No secrets, project refs, connection strings, or internal hostnames
  anywhere (public repo).

## Tests (mocked HTTP client only — no live DB, no live CFBD)

In `tests/test_sources/test_stats.py` (rewrite the old breaker tests; keep
the misses/merge-disposition/one-call-per-id tests, adapted):

1. Incident replay: ids ordered 500, 500, 200 x5, 500 x4 — no probe fires
   (window still holds answered games), all 11 attempted, the six 5xx ids in
   `misses` and `server_error_game_ids`, the five success payloads yielded,
   `report.outage is None`.
2. Bad-data cluster at run start: 5xx x5 then more games — probe fires at
   the 5th, uses the fallback (no success yet this process), probe returns
   200, window cleared, draining continues through the remaining ids,
   `report.outage is None`.
3. Outage: successes then unbroken 5xx — probe fires once the window is all
   5xx, probe raises 5xx, `report.outage` set, generator stops gracefully,
   rows yielded before the stop still materialize via `list(...)`,
   `report.attempted` is exactly the prefix attempted.
4. Probe candidate precedence: in-run last data-200 beats fallback; fallback
   beats the module constant; constant used when neither exists.
5. Probe 400 counts as healthy (server answered).
6. `AdvancedBoxHealth` unit tests: trip condition, answered game blocks the
   trip until it slides out of the window, `clear_window` after probe,
   400/empty-200 count as answered.
7. Cross-batch window: two resource invocations share the module health.
8. Plain mode (`misses=None`): 5xx still re-raises immediately.

In `tests/test_backfill_refresh.py`:

1. `run_batch` returns the triple; forwards `fallback_probe_game_id` into
   the `box_advanced` resource's report; `plays_stats` report is `None`
   (adapt `TestRunBatch`).
2. Batch-loop behavior via `run_campaign` (or a factored-out drain loop)
   with a fake pipeline + monkeypatched DB helpers: (a) outage mid-batch —
   ledger rows written for exactly the attempted prefix with the right
   `no_data` subset BEFORE the function returns, status `"outage"`,
   remaining batches not run; (b) bad-data run — status stays non-failing,
   `bad_data_game_ids` populated, every batch ledgered.
3. `fetch_last_refreshed_game_id` query shape (fake cursor, like
   `TestRequeueNoData`).
4. `report_partial_bad_data`: writes the `::warning::` line, the
   `GITHUB_OUTPUT` keys, and the summary line (tmp files via monkeypatched
   env); silent when ids are empty or status exits 1.
5. Exit-code mapping for status `"outage"`.

Test command (from repo root):

```
PATH="$PWD/.venv/bin:$PATH" .venv/bin/python -m pytest \
  tests/test_sources/test_stats.py tests/test_backfill_refresh.py \
  tests/test_scheduled_fpi_receipts.py -q --tb=short
```

plus `.venv/bin/ruff check` and `.venv/bin/ruff format --check` on changed
files, and the full `PATH="$PWD/.venv/bin:$PATH" .venv/bin/python -m pytest
-q --tb=short` main suite before the PR.

## Post-review amendments (Greptile 4/5 on PR #156, 2026-10-19)

- Both issue lookups in `.github/workflows/historical-refresh.yml` use
  `github.paginate` (`per_page: 100`): `listForRepo` returns 30 issues per
  page, so a title on page 2+ would never be found and a duplicate
  "Historical refresh failing" issue would be created.
- A probe answered with 401/403 is a FAILURE (rejected credentials mean no
  call can be trusted), not "the server answered". Only 400/404 count as
  healthy request-level rejections.
- `RequestBudgetExhausted` (the local budget guard in
  `src/pipelines/sources/base.py`) gets its own `report.budget_stop` field
  and is handled as a pacing stop (exit 0, like the month guard), never as
  a CFBD outage.
- New regression test for the actual run-#51/#52 shape: five CONSECUTIVE
  500s (401838337, 401838336, 401837115, 401837097, 401837096) at the head
  of the newest-first drain, which — unlike the interleaved replay — trips
  the windowed rule; proves probe success, continued drain, partial
  success, and persisted `no_data` ledger rows.
- The outage-path `run_campaign` test now uses an attempted prefix smaller
  than the batch and asserts the ledger write happens before
  `finalize_campaign`, which runs before the outage result returns.

## Amendment: per-game timeouts (Historical Refresh #54, run 39637616538, 2026-10-21)

A request to `/game/box/advanced?id=401837096` raised `httpx.ReadTimeout`
after the client's retries and surfaced through dlt as
`ResourceExtractionError`/`PipelineStepFailed`, killing the run before the
bad-data/outage logic saw it. Fix: `httpx.ReadTimeout` after retries is
treated exactly like a persistent 5xx for that game — `no_data` miss,
server-error mark in the health window, probe-on-trip — so a lone slow game
drains past and a widespread run of read timeouts fills the window, times
out the probe too, and stops cleanly as an outage. Deliberately NOT the
broader `TimeoutException` (Codex P1 on PR #158): `ConnectTimeout` and
`PoolTimeout` are connection-level — CFBD may never have received the
request — so retiring the game as `no_data` would silently drop it from the
backlog; they remain endpoint-wide outage stops with the other transport
errors (connection refused, DNS). Covered by
`TestOct21TimeoutRegression` (run-level replay of the #54 sequence) and the
timeout tests in `tests/test_sources/test_stats.py`.

## Amendment: unusable 200 payloads (Historical Refresh #55, run 39816658236, 2026-10-22)

A 200 response carried a JSON OBJECT instead of the usual list of row
dicts. `yield from` on a dict emits its KEY STRINGS, which dlt normalized
into an all-null row (new variant column holding the string `gameInfo`)
that violated `advanced_game_stats.game_id NOT NULL` at the LOAD stage —
losing the whole 50-game batch after extraction had already succeeded. The
miss paths themselves were verified against the run logs to yield nothing;
the bad row came from the 200 path. Fix: `_usable_advanced_box_rows`
filters every 200 payload before it reaches dlt — only dict rows with a
non-null `gameId` (the source of the NOT NULL `game_id` primary-key
column) are yielded, everything else is dropped with a warning, and a
response with zero usable rows becomes a `no_data` miss for that game
(requeueable), answered as far as the health window is concerned. Covered
by the dict-shaped-200, rows-without-gameId, and mixed-batch miss-path
tests in `tests/test_sources/test_stats.py`.

## Output contract

Changed files: `src/pipelines/sources/stats.py`,
`scripts/backfill_refresh.py`, `.github/workflows/historical-refresh.yml`,
`tests/test_sources/test_stats.py`, `tests/test_backfill_refresh.py`, this
plan. No other files. No git operations by the implementer; the lead owns
commits and the PR.
