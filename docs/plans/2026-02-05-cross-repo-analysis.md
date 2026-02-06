# CFB Analytics — Cross-Repo Analysis & Optimization Recommendations

**Date:** 2026-02-05
**Scope:** cfb-database, cfb-app, cfb-scout
**Method:** Parallel agent team analysis of all three repos

---

## Current Architecture

```
CFBD API → cfb-database (dlt/Python) → Supabase Postgres ← cfb-scout (Python/FastAPI)
                                              ↓
                                     PostgREST / RPCs
                                              ↓
                                    cfb-app (Next.js 16)
```

Three repos, one shared Supabase database. No shared code library. No CI/CD in any repo.

---

## Repo Summaries

### cfb-database

- **Purpose:** Data warehouse — ingests CFBD API into Supabase Postgres via dlt pipelines
- **Stack:** Python 3.11+, dlt[postgres], httpx, psycopg2
- **Size:** 59 endpoints mapped (30 working), ~4.1M rows / 1.7 GB across 6+ schemas
- **Schema layers:** ref, core, stats, ratings, recruiting, betting, draft, metrics → analytics MVs → marts MVs → api views
- **Tests:** 80 tests (unit), no integration tests against real DB
- **Docs:** Extensive (20+ plan docs, pipeline manifest, CLAUDE.md)

### cfb-app

- **Purpose:** Analytics dashboard — team pages, game browser, scatter plots, situational splits
- **Stack:** Next.js 16, React 19, TypeScript, Tailwind 4, D3 + roughjs, Supabase SSR
- **Size:** 61 source files, ~8,400 LOC TypeScript/TSX, 117 commits
- **Design:** Editorial/newspaper theme (Libre Baskerville + DM Sans, paper textures, hand-drawn charts)
- **Tests:** Zero. No test framework installed.
- **Docs:** Boilerplate README only

### cfb-scout

- **Purpose:** AI scouting agent — crawlers, entity extraction, player matching, grades, alerts
- **Stack:** Python 3.12+, FastAPI, psycopg2 (sync), anthropic, openai, pgvector
- **Size:** ~4,900 LOC across 27 Python files, 30 FastAPI endpoints, 74 commits
- **Tests:** 21 test files, integration-heavy (hits real DB), no AI service mocking
- **Docs:** Phase-based implementation docs

---

## Cross-Repo Dependencies

### Data Flow

| Source | Consumer | Coupling |
|--------|----------|----------|
| cfb-database schemas (core, recruiting, stats, etc.) | cfb-scout (`player_matching.py`, `backfill_embeddings.py`) | Direct SQL with hardcoded column names |
| cfb-database materialized views (marts.*) | cfb-app (server components) | Supabase client queries |
| cfb-database RPCs (get_drive_patterns, etc.) | cfb-app (server actions) | RPC name + parameter contracts |
| cfb-database api views | cfb-app (PostgREST) | View name + column contracts |
| cfb-database dlt schemas (core_staging.*) | cfb-app (game detail page) | `_dlt_id/_dlt_parent_id` joins |
| cfb-scout scouting schema | cfb-app (planned) | Not yet implemented |

### Shared Constants (duplicated)

| Constant | cfb-database | cfb-app | cfb-scout |
|----------|-------------|---------|-----------|
| Current season | Year range configs | `CURRENT_SEASON = 2025` (2 locations) | N/A |
| FBS conferences | Schema definitions | `FBS_CONFERENCES` (2 locations) | N/A |
| Team names/slugs | `ref.teams` table | `teamNameToSlug()` utility | Matched via DB queries |

### Type Definitions (not shared)

- **cfb-app:** Manual TypeScript interfaces in `src/lib/types/database.ts`
- **cfb-scout:** Inline SQL column lists, Pydantic models
- **cfb-database:** SQL DDL is the source of truth, but no generated types

---

## Findings

### 1. No Formal Schema Contracts

cfb-scout queries `core.roster` and `recruiting.recruits` directly with hardcoded column names. cfb-app queries `core.game_team_stats` and navigates dlt's internal `_dlt_id/_dlt_parent_id` relationships. If cfb-database changes a column name or table structure, downstream repos break silently.

The `src/schemas/api/` views in cfb-database are the right pattern but aren't used by cfb-scout, and cfb-app also bypasses them for box score / player leader data.

### 2. No CI/CD Anywhere

None of the three repos have GitHub Actions, Vercel config, or any automated pipeline. Migrations, pipeline runs, and deploys are all manual. A broken import or bad migration ships with no gate.

### 3. cfb-app Has Zero Tests

The largest consumer of the data warehouse — with complex server components, D3 visualizations, and multi-tab team detail pages — has no test framework installed and no test files.

### 4. cfb-scout Connection Management

Synchronous psycopg2 with no pooling. Every function opens/closes its own connection. Batch operations (e.g., `build_draft_board`) create N+1 connection patterns. FastAPI is async but the entire data layer blocks.

### 5. Type Drift Risk

cfb-app manually defines TypeScript interfaces. cfb-scout uses inline SQL. Neither auto-generates from the actual schema. Any schema change in cfb-database requires manual updates in 1-2 other repos.

### 6. Hardcoded Values

- `CURRENT_SEASON = 2025` appears in cfb-app (2 locations) and cfb-database year configs
- `FBS_CONFERENCES` duplicated in cfb-app (analytics page + shared.ts)
- `claude-3-haiku-20240307` hardcoded in cfb-scout (outdated model)

### 7. Missing DDL

`scouting.player_mart` materialized view is referenced in cfb-scout docs but not in `schema.sql`. Created ad-hoc via Supabase SQL Editor without version control.

### 8. Convention Inconsistencies

| Aspect | cfb-database | cfb-app | cfb-scout |
|--------|-------------|---------|-----------|
| Python version | 3.11+ | N/A | 3.12+ |
| Branch naming | `feature/*` | `feature/*` + `feat/*` | main only |
| CLAUDE.md | Detailed | Missing | Missing |
| README | Useful | Boilerplate | Useful |
| Commit style | Conventional | Conventional | Conventional |

---

## Recommendations

### Mono-Repo vs Multi-Repo

**Stay multi-repo.** The three projects have distinct tech stacks (Python pipelines, Python API, TypeScript frontend) and deployment lifecycles. A monorepo adds tooling complexity without proportional benefit for a solo developer. The fix is better contracts and automation between repos, not consolidation.

### Prioritized Action Plan

#### P0 — Critical (do first)

| Action | Effort | Impact |
|--------|--------|--------|
| Add CI/CD to all 3 repos (GitHub Actions: lint + typecheck + tests on PR) | 1-2 days | Prevents silent breakage across all repos |
| Auto-generate TypeScript types from Supabase schema (`supabase gen types typescript`) as a pre-build step in cfb-app | 2 hours | Eliminates manual type sync, catches schema drift |

#### P1 — High Priority

| Action | Effort | Impact |
|--------|--------|--------|
| Create API views for cfb-scout consumption (e.g., `api.roster_lookup`, `api.recruit_lookup`) so it stops querying raw tables | Half day | Decouples cfb-scout from cfb-database schema internals |
| Add Vitest + React Testing Library to cfb-app with smoke tests for each route | Half day | Basic safety net for the frontend |
| Fix cfb-scout connection management — add pooling (psycopg2 pool or psycopg v3) and pass connections via dependency injection | Half day | Prevents connection exhaustion under load |

#### P2 — Medium Priority

| Action | Effort | Impact |
|--------|--------|--------|
| Extract shared constants (CURRENT_SEASON, FBS_CONFERENCES) to a single source — Supabase RPC or shared config file | 2 hours | Single source of truth |
| Schema drift detection — scheduled GitHub Action that runs `supabase gen types` and opens PR if types changed | 2 hours | Catches breaking schema changes automatically |
| Update Claude model in cfb-scout from `claude-3-haiku-20240307` to Haiku 4.5 | 30 min | Better quality, same cost tier |
| Add CLAUDE.md to cfb-app and cfb-scout with project context | 1 hour | Better AI-assisted development across all repos |
| Document schema contract — which views/RPCs/tables are "public API" for downstream repos | 1 hour | Formalizes what's safe to depend on |

#### P3 — Lower Priority

| Action | Effort | Impact |
|--------|--------|--------|
| Mock AI services (Claude/OpenAI) in cfb-scout tests | Half day | Faster, cheaper, deterministic test runs |
| Migrate cfb-scout data layer to async (psycopg v3 async or supabase-py) | 2-3 days | Proper FastAPI concurrency |
| Version-control all DDL — capture missing `scouting.player_mart` and any other ad-hoc objects | 1 hour | Complete schema history |
| Standardize branch naming to `feature/*` across all repos | 30 min | Consistency |
| Replace cfb-app boilerplate README with actual project documentation | 1 hour | Onboarding and context |

---

## Schema Contract Proposal

To formalize the boundary between cfb-database and its consumers, designate these as the **stable public API**:

### For cfb-app (already mostly in place)

- `api.*` views (team_detail, team_history, game_detail, matchup, leaderboard_teams)
- `marts.*` materialized views (queried via Supabase client)
- All `get_*` RPCs (drive_patterns, down_distance_splits, etc.)
- `ref.teams` + `teams_with_logos` view

### For cfb-scout (needs creation)

- `api.roster_lookup` — filtered view of `core.roster` with stable column names
- `api.recruit_lookup` — filtered view of `recruiting.recruits` with stable column names
- `api.player_search` — RPC wrapping the multi-tier matching logic

### Contract rules

1. API views and RPCs are versioned — breaking changes require a new version
2. Raw table access (`core.*`, `recruiting.*`) is internal and may change without notice
3. `supabase gen types` output is the canonical type definition for TypeScript consumers
4. Column additions to API views are non-breaking; removals and renames are breaking

---

## Appendix: Repo Stats

| Metric | cfb-database | cfb-app | cfb-scout |
|--------|-------------|---------|-----------|
| Commits | 40+ | 117 | 74 |
| Source files | ~30 Python + 40 SQL | 61 TS/TSX | 27 Python |
| Source LOC | ~3,000 Python + ~2,000 SQL | ~8,400 | ~4,900 |
| Test files | 6 | 0 | 21 |
| Test LOC | ~400 | 0 | ~1,126 |
| Dependencies | 8 runtime | 12 runtime | 14 runtime |
| DB tables owned | ~50 tables + 24 MVs + 5 API views | 0 (consumer) | 10 tables in scouting schema |
| Branches | 1 local (main) | 10 | 1 (main) |
| CI/CD | None | None | None |
