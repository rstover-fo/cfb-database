---
title: CFBD API usage audit vs the AI Builder Pack efficiency guide
date: 2026-07-28
module: pipelines
problem_type: best_practice
tags: [api-budget, rate-limiter, ci, backfill, audit, builder-pack]
severity: medium
---

# CFBD API Usage Audit (2026-07-28)

Audit of the ingestion pipelines against the CFBD AI Builder Pack's
`api_usage_efficiency.md` guide and `api_usage_audit.md` checklist (20
extracted criteria), triggered by the starter-pack review
(`docs/plans/2026-07-28-001-feat-starter-pack-model-features-plan.md`,
deferred workstream).

## Verdict

The pipeline is ahead of the pack's guidance on structure and discipline, and
the pack's two headline rules are wrong for this codebase. Four real findings
survive, one of them structural.

## Where the repo already exceeds the pack

- Documented per-source call estimates (`ESTIMATED_CALLS`,
  `scripts/load_season.py:42-66`) logged against remaining budget pre-run.
  Note the limit of this mechanism: the pre-run comparison only *warns*; hard
  enforcement is per-call inside `make_request`, so a load whose estimate
  exceeds the remainder can run partially before failing mid-run.
- Live calls originate only from explicit scripts; no test touches the live
  API (`tests/conftest.py:85-93`, synthetic httpx mocks throughout).
- Narrow filters per request; centralized client and rate limiter; per-game
  fan-outs are documented, capped (`MAX_WP_GAMES_PER_RUN=300`), and skipped
  for finished seasons (`IMMUTABLE_ONCE_FINAL`).

## Pack rules judged inapplicable (do not adopt)

- **"Never auto-retry 429."** CFBD's Cloudflare burst-blocks make transient
  429s routine. The client's Retry-After-honoring retry (capped 120s), the
  10-strike shared circuit breaker, and terminal `RateLimitExhausted` are the
  correct design (`src/pipelines/utils/api_client.py:243-355`). The failure
  the pack fears (2026-07-25's 3-hour retry spiral) was mitigated by exactly
  these mechanisms plus the finished-season skip.
- **Raw HTTP response caching with stable keys.** The right mechanism here is
  DB-backed skip logic, not a parallel response-cache layer: merge
  dispositions, the finished-season skip, the flat-file hash ledger, and the
  win-probability DB set-difference cover the pack's intent without
  duplicating state. One caveat the first draft of this audit got wrong: the
  in-season daily path re-fetches `/plays/stats` for every already-played
  game of the unfinished season (no skip applies until the season is final),
  so the pack's underlying concern is real there — F2's set-difference is
  the warehouse-idiomatic fix and applies to the daily path, not only to
  backfill resume.
- **"One Slice Rule"** — beginner scaffolding guidance; not applicable to a
  mature warehouse.

## Findings

- F1 (P1). **Monthly budget tracking does not persist in CI.** The rate
  limiter's month-keyed state lives in `.dlt/rate_limit_state.json`
  (`src/pipelines/utils/rate_limiter.py:29`), which resets on every ephemeral
  GitHub runner (`.github/workflows/daily-load.yml:16`). Month-to-date usage
  is therefore invisible exactly where nearly all calls happen; the 125k
  budget is enforced only against each run's own counter, and the 429
  breaker is the only real cross-run guard. Fix direction: record per-run
  call counts durably (a `meta.api_call_log` row per run, matching the
  existing `meta.flat_file_loads` pattern) and have the limiter read
  month-to-date from the DB; `actions/cache` on the JSON file is the weaker
  fallback.
- F2 (P2, wider than first assessed). **`play_stats` refetches already-loaded
  games — daily in-season, and on any backfill re-run.** The largest fan-out
  (~1,640 calls/season, one `/plays/stats` call per game,
  `src/pipelines/sources/stats.py:274-368`) has no already-present skip: the
  in-season daily load re-fetches every played game of the unfinished season
  each day (the finished-season skip does not apply until the season is
  final), and an interrupted backfill re-spends the whole season's calls.
  The DB set-difference pattern already used by `run_metrics_wp_pipeline`
  (`src/pipelines/run.py:535-539`) applies directly and would cut the
  in-season daily cost as well.
- F3 (P2). **The daily workflow never surfaces quota.** No step prints
  `--status` or compares the run's actual calls to `ESTIMATED_CALLS`
  (`verify_load.py` makes one API call and asserts nothing about budget).
  A post-load status step (and a warn annotation past a threshold) closes
  the pack's C18/C20 gap and gives F1's ledger a consumer.
- F4 (P3). **Stale budget baseline in the daily workflow comment.** The
  present-tense 75K denominator contradicted the configured 125,000
  (`.dlt/config.toml:31`), and the "~730 estimated calls, ~22K/month"
  numerator is an off-season figure: with an unfinished season the default
  sources' `ESTIMATED_CALLS` total ~2,430/day (~73K/month, ~58% of budget)
  because of the F2 fan-out. Fixed in this commit with both baselines stated;
  `scripts/load_season.py:78` and `CLAUDE.md:87` keep "then-75,000" as
  explicitly historical incident narration.

## Checklist disposition (pack criteria C1-C20)

Satisfied: C1, C2, C4, C5, C9 (skip-override flags), C11, C12, C13, C14,
C17, C19. Inapplicable by judgment: C6, C7, C8 (warehouse-is-cache), C15,
C16 (Retry-After design is correct here). Gaps: C3-partial (fan-outs exist
but governed), C10 (corrupt rate-limit state behavior unverified — minor),
C18, C20 (→ F1, F3).
