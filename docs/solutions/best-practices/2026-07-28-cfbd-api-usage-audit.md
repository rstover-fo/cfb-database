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
  `scripts/load_season.py:42-66`) compared against remaining budget pre-run —
  the pack asks for estimates; the repo also gates on them.
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
- **Raw HTTP response caching with stable keys.** The warehouse is the cache:
  merge dispositions, the finished-season skip, the flat-file hash ledger,
  and the win-probability DB set-difference cover the pack's intent. Adding a
  response-cache layer would duplicate state for no call savings on the daily
  path.
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
- F2 (P2). **`play_stats` backfill cannot resume.** The largest fan-out
  (~1,640 calls/season, one `/plays/stats` call per game,
  `src/pipelines/sources/stats.py:274-368`) refetches every game on re-run;
  an interrupted backfill re-spends the whole season's calls. The DB
  set-difference pattern already used by `run_metrics_wp_pipeline`
  (`src/pipelines/run.py:535-539`) applies directly.
- F3 (P2). **The daily workflow never surfaces quota.** No step prints
  `--status` or compares the run's actual calls to `ESTIMATED_CALLS`
  (`verify_load.py` makes one API call and asserts nothing about budget).
  A post-load status step (and a warn annotation past a threshold) closes
  the pack's C18/C20 gap and gives F1's ledger a consumer.
- F4 (P3). **Stale 75K budget references** in `daily-load.yml:17`,
  `scripts/load_season.py:78`, and `CLAUDE.md:87` contradict the configured
  125,000 (`.dlt/config.toml:31`). Fixed in this commit — historical
  incident narration now names "the then-75,000/month budget" only where it
  is explicitly historical.

## Checklist disposition (pack criteria C1-C20)

Satisfied: C1, C2, C4, C5, C9 (skip-override flags), C11, C12, C13, C14,
C17, C19. Inapplicable by judgment: C6, C7, C8 (warehouse-is-cache), C15,
C16 (Retry-After design is correct here). Gaps: C3-partial (fan-outs exist
but governed), C10 (corrupt rate-limit state behavior unverified — minor),
C18, C20 (→ F1, F3).
