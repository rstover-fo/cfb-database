# P3.2 Lane B — win-probability deploy manifests

Reference copies of the deploy-manifest.json bodies for the win-probability
rollout (docs/pipeline-manifest.md row 47). The orchestrator pushes each of
these, in order, as `deploy-manifest.json` at the root of a `deploy/**`
branch (see `.github/workflows/deploy-schema.yml`'s header and
`docs/plans/2026-07-19-tier1-analytics-unlock-plan.md` "Deploy mechanism").
This repo's `claude/p32-winprob-db` branch never pushes a `deploy/**` branch
itself -- these are authored content only.

## Why `action: "backfill"` works unmodified

`scripts/deploy_schema.py`'s `backfill` action already does exactly what a
win-probability backfill needs: for each season in `[start, end]` it shells
out to `python scripts/load_season.py --season <season> --sources <sources>
--skip-refresh`. Since W3 wires `"metrics_wp"` into
`scripts/load_season.py`'s `SOURCE_ORDER`/`ESTIMATED_CALLS`/`runners`, passing
`"sources": "metrics_wp"` routes straight to `run_metrics_wp_pipeline(seasons=[season])`
-- no new manifest shape or deploy_schema.py changes were needed.

Each `load_season.py --sources metrics_wp` call internally re-derives the
still-missing game ids for that season (LEFT JOIN against
`metrics.win_probability`) and batches them into ≤50-game `pipeline.run()`
calls itself, so re-running any of these manifests after a partial failure
is safe and cheap -- already-loaded games are skipped, not re-fetched.

## 0. Probe (run first, before trusting anything below)

`deploys/p32-probe-manifest.json`:

```json
{
  "action": "compute",
  "compute": { "script": "probe_metrics_wp", "args": [] }
}
```

Confirms the live `/metrics/wp` field shape (see `scripts/probe_metrics_wp.py`'s
docstring) before the backfill and view creation below are trusted. Expected
log output: two `===== gameId=... =====` blocks, one per default probe game,
each printing the full field list, record count, playId sample values +
inferred type, and down/distance/clock/homeBall/spread presence flags.

**Open assumptions this must confirm before step 2 below:**
- Field names still match `playId`, `playText`, `homeWinProbability`, `down`,
  `distance`, `yardLine` (2026-01-29 investigation note) -- CFBD is reported
  to have rebuilt its WP model in 2025, which is the whole reason this probe
  exists rather than trusting the old note.
- `playId`'s type/scope (int vs string; unique per-game vs globally unique)
  -- affects nothing about correctness (the merge key is the compound
  `(game_id, play_id)`, safe either way) but is worth knowing.
- Whether `down`/`distance`/`clock`/`homeBall`/`spread` are present at all --
  `api.game_win_probability` (033) only selects `down`/`distance`, which the
  investigation note already confirmed; if the probe finds a `clock` field
  directly on the WP payload, the defensive `core.plays` join in 033 could be
  simplified, but is NOT required to be (it degrades to NULL harmlessly if
  the join key doesn't line up -- see 033's header).
- **The default 2015 probe game id (400756843) is UNVERIFIED** -- no 2015-era
  game id exists anywhere else in this repo to cross-check against, and this
  authoring environment cannot reach the CFBD API or `core.games` to confirm
  it. If it 400/404s, that's "wrong id", not "no 2015 WP data" -- rerun with
  `--game-ids <confirmed pre-rebuild id>` (e.g. via
  `python scripts/deploy_schema.py --action compute --compute-script probe_metrics_wp --compute-args <id1>,<id2>`
  or a one-off `workflow_dispatch` run).

## 1-3. Backfill 2014-2025 in three manifests

Three separate manifests (rather than one 2014-2025 run) so a `deploy/**`
push isn't a single ~180-minute (workflow `timeout-minutes`), all-or-nothing
job -- each covers 4 seasons, well inside CI's timeout, and a failure in one
doesn't lose progress from the others (idempotent re-run, see above).

`deploy-manifest.json` for `deploy/p32-backfill-2014-2017`:

```json
{
  "action": "backfill",
  "backfill": { "start": 2014, "end": 2017, "sources": "metrics_wp" }
}
```

`deploy-manifest.json` for `deploy/p32-backfill-2018-2021`:

```json
{
  "action": "backfill",
  "backfill": { "start": 2018, "end": 2021, "sources": "metrics_wp" }
}
```

`deploy-manifest.json` for `deploy/p32-backfill-2022-2025`:

```json
{
  "action": "backfill",
  "backfill": { "start": 2022, "end": 2025, "sources": "metrics_wp" }
}
```

Expected log output per season, from `run_metrics_wp_pipeline` (via
`load_season.py`'s per-source runner): a `=== Loading Win Probability Data
(seasons=[<year>]) ===` header, a candidates/already-loaded/missing count
line, `Batch i/N: <=50 games` lines, and a final `Win probability load
complete: N batches` line. `deploy_schema.py`'s `run_backfill` then runs
`refresh_marts.py` and `check_presence.py` after the season loop -- both are
harmless no-ops for this rollout (metrics.win_probability isn't in either's
tracked list yet) but shouldn't fail.

**Budget math** (Tier 3 = 75,000 calls/month, `src/pipelines/utils/rate_limiter.py`):
2014-2025 is ~12,000 completed FBS games (docs/pipeline-manifest.md's
`core.games` row count is 18,650 across all history back to 1869; the
2014-2025 subset that's actually completed is the ~12K figure this estimate
is built from) -> **~12,000 API calls total** for the full backfill, split
roughly evenly across the three manifests above (~3-4K calls each). This is
a one-time cost; it does not recur. Compare against the existing daily-load
worst case of ~22K calls/month (`.github/workflows/daily-load.yml`'s header)
-- the backfill fits comfortably in a single month's budget even run back to
back with normal daily loads, and after it completes, steady-state
incremental cost drops to the ~70 calls/week `ESTIMATED_CALLS["metrics_wp"]`
entry (`scripts/load_season.py`) covers.

## 4. Apply migration + view (run AFTER at least one backfill manifest above)

`metrics.win_probability` doesn't exist until the first successful
`pipeline.run()` of `win_probability_resource` creates it (dlt
table-on-first-write -- same precondition
`src/schemas/migrations/020_line_snapshot_indexes.sql` documents for
`betting.line_snapshots`). Both 026 (indexes) and 033 (the view, which
selects FROM `metrics.win_probability`) will fail against a database where
that table doesn't exist yet. Run this AFTER manifest 1 (2014-2017) above has
completed successfully, not before, and not necessarily after all three
backfill manifests -- indexing/exposing the view can happen as soon as any
data exists; the remaining backfill manifests keep filling the table
underneath an already-live view.

`deploy-manifest.json` for `deploy/p32-apply`:

```json
{
  "action": "apply",
  "files": [
    "src/schemas/migrations/026_win_probability_indexes.sql",
    "src/schemas/api/033_game_win_probability.sql"
  ]
}
```

Expected log output: `run_migrations --file src/schemas/migrations/026_win_probability_indexes.sql`
then `run_migrations --file src/schemas/api/033_game_win_probability.sql`,
both `exit=0`.

## After the view is live: cherry-pick the held-back test commit

The commit adding `api.game_win_probability` to `tests/test_api_views.py`
(W8) is deliberately kept out of the PR that lands the rest of this branch on
`main` -- this repo's sequencing rule (see
`docs/plans/2026-07-19-tier1-analytics-unlock-plan.md`: "never add a new mart
[view] to \[a test inventory\] ... until the object is CREATE'd + REFRESH'd
live") means adding it before step 4 above has run would turn PR CI red on a
"relation api.game_win_probability does not exist" error. Once step 4 is
confirmed (the view exists and has rows), cherry-pick that commit onto `main`
(or a follow-up PR) and let CI run for real against the populated view --
its row-floor number will need sizing from the actual backfill count at that
point, not guessed in advance.
