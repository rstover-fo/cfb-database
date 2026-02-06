---
title: "Player Comparison View + cfb-app Migration"
type: feat
date: 2026-02-06
deepened: 2026-02-06
status: ready
---

## Enhancement Summary

**Deepened on:** 2026-02-06
**Research agents:** Postgres window function perf, PostgREST view patterns, Supabase docs, learnings search

### Key Architecture Change
- **Original:** `api.player_comparison` as plain view with `PERCENT_RANK()` → ~2,000ms/query
- **Revised:** `marts.player_comparison` matview + thin `api.player_comparison` view → <5ms/query
- Window functions cannot push `WHERE player_id=X` down — benchmarked at 2s for single lookup
- Matview fits existing architecture (23 matviews, `marts.refresh_all()`)

### Research Insights Applied
- Use `NULLIF(stat, '')::numeric` for safe EAV casting (stat column is varchar)
- Use `LANGUAGE sql STABLE` for any RPC fallback (enables planner inlining)
- EAV format for game_box_score (handles new stat categories without view changes)
- dlt LATERAL JOIN pattern from `docs/solutions/` applied to all child table views
- Verify view type (matview vs view) before adding to refresh — per documented learning

# Player Comparison View + cfb-app Migration

## Overview

Two features that close the remaining gaps between cfb-database and its consumers:

1. **`api.player_comparison`** — Side-by-side player stats with positional percentiles
2. **cfb-app migration views** — Replace broken/fragile raw table queries with stable API views

## Problem Statement

**Broken dependency (P0):** cfb-app queries `core_staging.game_player_stats` for game player leaders — but Sprint 6 dropped all staging schemas. Game detail pages with player leaders are broken right now.

**Fragile dependencies (P1):** cfb-app also queries raw dlt child table hierarchies (`core.game_team_stats__teams__stats`, `core.games__home_line_scores`) which couples it to dlt internals.

**Missing feature (P2):** `api.player_comparison` was deferred from Sprint 6. cfb-scout's player pages need percentile context for player evaluation.

## Current State

| What | Status |
|------|--------|
| `api.player_detail` | Deployed (340K rows) — no local SQL file |
| `api.player_season_leaders` | Deployed (152K rows) — no local SQL file |
| `get_player_search` RPC | Deployed with pg_trgm |
| `core_staging` schema | **DROPPED** (Sprint 6) |
| Child table indexes | Deployed (Sprint 6 Phase A) |
| cfb-app `api.*` usage | **Zero** — all queries go through `public.*` or raw tables |

## Proposed Solution

### Phase 1: Fix broken cfb-app dependency (P0)

**Task 1: Create `api.game_player_leaders` view**

Flattens the 5-level `core.game_player_stats` hierarchy into a queryable view. Replaces the broken `core_staging` dependency.

```
core.game_player_stats (game_id, _dlt_id)
  → __teams (school, conference, home_away, _dlt_parent_id)
    → __categories (name = passing/rushing/..., _dlt_parent_id)
      → __types (name = YDS/TD/..., _dlt_parent_id)
        → __athletes (id, name, stat, _dlt_parent_id)
```

Output shape: `game_id, season, team, conference, home_away, category, stat_type, player_id, player_name, stat`

PostgREST usage: `GET /api/game_player_leaders?game_id=eq.401628455&category=eq.passing&order=stat.desc`

**Task 2: Create `api.game_box_score` view**

Flattens `core.game_team_stats` 3-level hierarchy. EAV format (one row per stat per team per game) so new stat categories don't require view changes.

Output shape: `game_id, season, team, home_away, category, stat_value`

**Task 3: Create `api.game_line_scores` view**

Pivots `core.games__home_line_scores` / `__away_line_scores` into columns. Q1-Q4 as separate columns, all OT periods summed into a single OT column.

Output shape: `game_id, home_q1, home_q2, home_q3, home_q4, home_ot, away_q1, away_q2, away_q3, away_q4, away_ot`

### Phase 2: Track deployed views in source control

**Task 4: Extract SQL for `api.player_detail` and `api.player_season_leaders`**

These views are live in the DB but have no local SQL files. Extract via `pg_get_viewdef()` and save as `src/schemas/api/008_player_season_leaders.sql` and `009_player_detail.sql`.

### Phase 3: Player comparison matview + view (P2)

**Architecture (revised after performance research):**

Plain view with `PERCENT_RANK()` benchmarked at ~2,000ms for single-player lookup because Postgres cannot push `WHERE player_id = X` below window functions. The matview approach matches existing architecture (23 matviews) and delivers <5ms indexed lookups.

```
stats.player_season_stats (1.2M EAV rows)
  → marts.player_comparison (matview, ~127K rows, pivoted + percentiles)
    → api.player_comparison (thin view, no window functions)
```

**Task 5: Create position grouping CTE**

Map 26 raw position codes into ~8 groups for meaningful percentile cohorts:

| Group | Positions |
|-------|-----------|
| QB | QB |
| RB | RB, FB |
| WR | WR |
| TE | TE |
| OL | OL, OT, OG, C |
| DL | DL, DE, DT, NT, EDGE |
| LB | LB, OLB, ILB |
| DB | DB, CB, S, FS, SS |
| K/P | K, P |

Positions with < 5 players in a season (ATH, KR, PR, LS) get NULL percentiles.

**Task 6a: Create `marts.player_comparison` materialized view**

Pre-computes the full pivot + percentile calculation. Includes:
- EAV pivot using `MAX(CASE WHEN ... THEN NULLIF(stat, '')::numeric END)`
- Position group mapping via CTE (Task 5)
- `PERCENT_RANK() OVER (PARTITION BY season, position_group ORDER BY stat DESC NULLS LAST)` for 13 stat columns
- Only players with at least one stat row (INNER JOIN to pivoted stats)
- LEFT JOIN to `recruiting.recruits` and `metrics.ppa_players_season`

Indexes:
```sql
CREATE UNIQUE INDEX ON marts.player_comparison (player_id, season);
CREATE INDEX ON marts.player_comparison (season, position_group);
```

Add to `marts.refresh_all()` dependency chain (layer 4 — depends on base tables only).

**Task 6b: Create `api.player_comparison` thin view**

Simple `SELECT * FROM marts.player_comparison` — no window functions in the view layer. PostgREST filters push down to matview indexes.

Key columns: all of `api.player_detail` columns PLUS percentile columns:
- `pass_yds_pctl`, `pass_td_pctl`, `pass_pct_pctl`
- `rush_yds_pctl`, `rush_td_pctl`, `rush_ypc_pctl`
- `rec_yds_pctl`, `rec_td_pctl`
- `tackles_pctl`, `sacks_pctl`, `tfl_pctl`
- `ppa_avg_pctl`

**Task 7: Performance validation**

Run `EXPLAIN ANALYZE` on the thin view with:
- Single player lookup: `WHERE player_id = 'X' AND season = 2024` → target <5ms
- Two-player comparison: `WHERE player_id IN ('X', 'Y') AND season = 2024` → target <5ms
- Full position cohort: `WHERE position_group = 'QB' AND season = 2024` → target <50ms

Also validate matview refresh time (target <15s) and storage (~20-25MB).

### Phase 4: Tests and contract

**Task 8: Write tests for new views**

Add to `tests/test_player_analytics.py`:
- `TestPlayerComparison`: exists, columns, percentile range (0-1), position filter, two-player query
- `TestGamePlayerLeaders`: exists, columns, game_id filter, category filter
- `TestGameBoxScore`: exists, columns, game_id filter
- `TestGameLineScores`: exists, columns, has Q1-Q4

**Task 9: Update SCHEMA_CONTRACT.md**

Add all new views to:
- cfb-app consumer section (game_player_leaders, game_box_score, game_line_scores)
- cfb-scout consumer section (player_comparison)
- Schema dependency graph

**Task 10: Extract SQL for `get_player_search` RPC**

Track the deployed RPC as a local SQL file: `src/schemas/public/009_player_search_function.sql`

## Technical Considerations

**PostgREST predicate pushdown:** Confirmed via benchmarks — window functions block predicate pushdown. A plain view with `PERCENT_RANK()` takes ~2,000ms for single-player lookup (computes all 13.8K rows, discards 13,810). Solved by materializing the result and indexing on `(player_id, season)`.

**Stats are EAV format:** `stats.player_season_stats` stores data as `(player_id, season, category, stat_type, stat)`. Every player view must pivot with `MAX(CASE WHEN stat_type = 'X' THEN stat::numeric END)`. The `stat` column is varchar — needs `NULLIF(stat, '')::numeric` for safe casting.

**Position grouping:** Rare positions (ATH, LS, KR, PR) have < 5 players/season, making percentiles meaningless. These get NULL percentiles.

**Transfer players:** Players who transfer appear under different teams in different seasons. The join on `(player_id, season)` using roster's team column handles this correctly (same pattern as `api.player_detail`).

## Acceptance Criteria

- [ ] `api.game_player_leaders` deployed and queryable by game_id
- [ ] `api.game_box_score` deployed and queryable by game_id
- [ ] `api.game_line_scores` deployed with Q1-Q4 + OT columns
- [ ] `api.player_detail` and `api.player_season_leaders` tracked as local SQL
- [ ] `marts.player_comparison` matview deployed with percentile columns
- [ ] `api.player_comparison` thin view deployed
- [ ] Percentiles between 0.0 and 1.0, partitioned by season + position group
- [ ] Single-player comparison query < 5ms (indexed matview lookup)
- [ ] Matview added to `marts.refresh_all()` dependency chain
- [ ] All new views have tests
- [ ] SCHEMA_CONTRACT.md updated
- [ ] All existing tests still pass (461+)

## Build Order

```
Phase 1 (P0 fix) ──→ Phase 2 (source control) ──→ Phase 4 (tests/contract)
  Task 1 (game_player_leaders)  Task 4 (extract SQL)     Task 8 (tests)
  Task 2 (game_box_score)                                 Task 9 (contract)
  Task 3 (game_line_scores)                               Task 10 (extract RPC)

Phase 3 (player comparison) ──→ Phase 4
  Task 5 (position groups)
  Task 6 (comparison view)
  Task 7 (perf validation)
```

Phase 1 and Phase 3 can run in parallel. Phase 2 is independent. Phase 4 depends on 1+3.

## References

- Existing pattern: `src/schemas/api/004_matchup.sql` (side-by-side comparison)
- Existing pattern: `src/schemas/api/005_leaderboard_teams.sql` (RANK window functions)
- Existing pattern: `src/schemas/public/007_player_stats_function.sql` (stat pivot)
- cfb-app broken query: `cfb-app/src/lib/queries/games.ts:280` (core_staging dependency)
- cfb-app box score query: `cfb-app/src/lib/queries/games.ts:190` (game_team_stats chain)
- cfb-app line scores: `cfb-app/src/lib/queries/games.ts:424` (games child tables)
- Sprint 6 plan Task 6: `docs/plans/2026-02-06-sprint-6-performance-and-player-analytics.md`
