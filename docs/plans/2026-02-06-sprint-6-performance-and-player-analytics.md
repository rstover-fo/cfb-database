---
title: "Sprint 6: Performance Optimization & Player Analytics Layer"
type: feature
date: 2026-02-06
status: planning
---

# Sprint 6: Performance Optimization & Player Analytics Layer

## Overview

**Goal:** Optimize query performance for large tables, build player-focused API views and RPCs, add missing indexes, and clean up staging tables — making the database fast and feature-complete for both cfb-app and cfb-scout.

**Why now:** Sprint 5 delivered the API layer and mart infrastructure. But several large tables (4.2M athlete stats rows, 2.5M play_stats, 1.2M game_team_stats child rows) have only 1 index each. The player-facing API surface is thin (roster_lookup + recruit_lookup only). And staging tables (~5M duplicate rows) waste 2+ GB of storage. This sprint closes the performance and player analytics gaps.

## Current State

| Component | Status | Gap |
|-----------|--------|-----|
| API views | 7 deployed | No player stats/leaders views |
| RPCs | 9 deployed (get_drive_patterns, etc.) | No player search/compare RPCs |
| Indexes | Heavy on marts/plays/drives | Large child tables (4.2M, 1.2M, 1.1M rows) have 1 index each |
| Staging tables | core_staging, stats_staging, metrics_staging | ~5M duplicate rows, 2+ GB wasted |
| Player mart | scouting.player_mart (2024 only) | No multi-season player view |
| play_stats | 2.5M rows (2014-2025) | Good coverage, needs indexes |
| FK constraints | 6 deployed | Missing on game_team_stats, play_stats |
| Tests | 437 passing | Need player analytics tests |

## Sprint Tasks

### Phase A: Performance — Index Large Tables (Priority 1)

**Task 1: Index dlt child tables**

The largest tables have minimal indexing. These are heavily joined via `_dlt_parent_id`:

```sql
-- game_player_stats hierarchy (4.2M + 1.1M + 225K rows)
CREATE INDEX CONCURRENTLY idx_gps_athletes_parent
  ON core.game_player_stats__teams__categories__types__athletes(_dlt_parent_id);
CREATE INDEX CONCURRENTLY idx_gps_types_parent
  ON core.game_player_stats__teams__categories__types(_dlt_parent_id);
CREATE INDEX CONCURRENTLY idx_gps_categories_parent
  ON core.game_player_stats__teams__categories(_dlt_parent_id);
CREATE INDEX CONCURRENTLY idx_gps_teams_parent
  ON core.game_player_stats__teams(_dlt_parent_id);

-- game_team_stats hierarchy (1.2M rows)
CREATE INDEX CONCURRENTLY idx_gts_stats_parent
  ON core.game_team_stats__teams__stats(_dlt_parent_id);
CREATE INDEX CONCURRENTLY idx_gts_teams_parent
  ON core.game_team_stats__teams(_dlt_parent_id);
```

**Validation:** Query plan should show Index Scan instead of Seq Scan for parent-child joins.

**Task 2: Index play_stats for analytics queries**

```sql
CREATE INDEX CONCURRENTLY idx_play_stats_game_season
  ON stats.play_stats(game_id, season);
CREATE INDEX CONCURRENTLY idx_play_stats_athlete
  ON stats.play_stats(athlete_id) WHERE athlete_id IS NOT NULL;
CREATE INDEX CONCURRENTLY idx_play_stats_stat_type
  ON stats.play_stats(stat_type_id, season);
```

**Task 3: Index roster for player lookups**

```sql
CREATE INDEX CONCURRENTLY idx_roster_team_year
  ON core.roster(team, year);
CREATE INDEX CONCURRENTLY idx_roster_player_id
  ON core.roster(id);
CREATE INDEX CONCURRENTLY idx_roster_name
  ON core.roster(last_name, first_name);
```

### Phase B: Player Analytics API Views (Priority 2)

**Task 4: Create api.player_season_leaders view**

Top performers by stat category per season. Used by cfb-app leaderboard pages.

```sql
CREATE OR REPLACE VIEW api.player_season_leaders AS
-- Passing leaders
SELECT season, 'passing' as category, player, team,
       pass_yds as stat_value, 'yards' as stat_type,
       pass_td, pass_int, pass_pct, ppa_avg
FROM (pivoted player stats query)
...
```

**Task 5: Create api.player_detail view**

Single-player profile with career stats, recruiting info, and advanced metrics. Used by cfb-app and cfb-scout player pages.

**Task 6: Create api.player_comparison view**

Side-by-side player comparison with normalized stats and percentiles.

**Task 7: Create get_player_search RPC**

Fuzzy search across player names using pg_trgm with GIN index. Supports typo tolerance (e.g., "Bryce Yung" matches "Bryce Young"). Replaces raw table queries in cfb-scout.

**Requires:** `CREATE EXTENSION IF NOT EXISTS pg_trgm;` (available on Supabase, not yet installed)

```sql
-- Enable trigram extension
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Add GIN index for fuzzy name matching
CREATE INDEX CONCURRENTLY idx_roster_name_trgm
  ON core.roster USING gin ((lower(first_name || ' ' || last_name)) gin_trgm_ops);

-- RPC with fuzzy matching
CREATE OR REPLACE FUNCTION public.get_player_search(
    p_query text,
    p_position text DEFAULT NULL,
    p_team text DEFAULT NULL,
    p_season integer DEFAULT NULL,
    p_limit integer DEFAULT 25
)
RETURNS TABLE(
    player_id bigint, name text, team text, position text,
    year integer, height integer, weight integer,
    stars integer, recruit_rating double precision,
    similarity_score real
)
```

### Phase C: Cleanup — Drop Staging Tables (Priority 3)

**Task 8: Audit and drop staging schemas**

dlt creates `{schema}_staging` tables during loads. These are copies that waste storage:

| Schema | Rows | Size |
|--------|------|------|
| stats_staging | 3.6M | 856 MB |
| core_staging | 2.1M | 410 MB |
| metrics_staging | 330K | 71 MB |
| recruiting_staging | 186K | 41 MB |
| betting_staging | 5K | 1 MB |
| ratings_staging | 4K | 1 MB |
| draft_staging | 258 | 136 KB |
| ref_staging | 27 | 48 KB |
| **Total** | **~6.2M** | **~1.4 GB** |

**Approach:**
1. Verify staging tables have no dependent views or functions
2. Verify data matches production schemas
3. Drop staging schemas

```sql
-- After verification
DROP SCHEMA IF EXISTS core_staging CASCADE;
DROP SCHEMA IF EXISTS stats_staging CASCADE;
DROP SCHEMA IF EXISTS metrics_staging CASCADE;
```

**Risk:** Must verify dlt doesn't need these for incremental loads. Check `_dlt_pipeline_state` tables.

### Phase D: Additional FK Constraints (Priority 4)

**Task 9: Add remaining FK constraints**

```sql
-- game_team_stats -> games
ALTER TABLE core.game_team_stats
  ADD CONSTRAINT fk_game_team_stats_game
  FOREIGN KEY (id) REFERENCES core.games(id) NOT VALID;

-- play_stats -> games
ALTER TABLE stats.play_stats
  ADD CONSTRAINT fk_play_stats_game
  FOREIGN KEY (game_id) REFERENCES core.games(id) NOT VALID;

-- roster -> teams (if feasible given team name variations)
-- recruits -> teams (if feasible)
```

### Phase E: Tests & Documentation (Priority 5)

**Task 10: Add player analytics tests**

Create `tests/test_player_analytics.py`:
- Verify new API views exist and return rows
- Test player search RPC with various filters
- Test player comparison with known matchups
- Validate leader categories cover all positions

**Task 11: Update SCHEMA_CONTRACT.md**

Add new API views and RPCs to the contract. Document player search interface.

**Task 12: Run Supabase advisors**

Check for security (missing RLS) and performance advisors after all changes.

## Acceptance Criteria

- [ ] All dlt child tables indexed (6+ new indexes)
- [ ] play_stats indexed for game/season/athlete queries
- [ ] roster indexed for player lookups
- [ ] api.player_season_leaders deployed and returning data
- [ ] api.player_detail deployed
- [ ] api.player_comparison deployed
- [ ] get_player_search RPC working
- [ ] Staging schemas audited and cleaned (if safe)
- [ ] Additional FK constraints added
- [ ] Player analytics tests passing
- [ ] SCHEMA_CONTRACT.md updated
- [ ] All existing tests still pass (437+)

## Build Order

```
Phase A (indexes) ────────────────────→ Phase D (FK constraints)
    Task 1 (child table indexes)           Task 9
    Task 2 (play_stats indexes)
    Task 3 (roster indexes)

Phase B (player API) ────────────────→ Phase E (tests & docs)
    Task 4 (leaders view)                  Task 10 (tests)
    Task 5 (player detail)                 Task 11 (schema contract)
    Task 6 (player comparison)             Task 12 (advisors)
    Task 7 (player search RPC)

Phase C (cleanup) — independent
    Task 8 (drop staging)
```

Phases A, B, C can run in parallel. Phase D depends on A. Phase E depends on B.

## Risk Analysis

| Risk | Impact | Mitigation |
|------|--------|------------|
| CREATE INDEX CONCURRENTLY fails | Index creation aborted | Clean up invalid index, retry |
| Staging tables needed by dlt | Pipeline breaks on next run | Check _dlt_pipeline_state first |
| Player ID joins don't match across tables | Empty results | Verify ID types (text vs bigint) match |
| RPC too slow for real-time search | Bad UX | Add GIN/trgm index for fuzzy search |

## Performance Targets

| Query | Current (est.) | Target |
|-------|---------------|--------|
| Player stats by game (child table join) | 5-10s (seq scan) | <500ms (index scan) |
| Player search by name | 2-5s (seq scan on roster) | <200ms (index scan) |
| Season leaders query | 3-8s (full scan) | <1s (indexed aggregation) |
| Staging cleanup storage savings | 0 | ~2 GB freed |

## Success Metrics

| Metric | Target |
|--------|--------|
| New indexes created | 12+ |
| New API views | 3 |
| New RPCs | 1 |
| Storage freed | ~1.4 GB |
| New tests | 15+ |
| Total tests passing | 450+ |
