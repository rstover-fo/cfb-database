---
title: "Sprint 5: API Layer Hardening & Data Completeness"
type: feature
date: 2026-02-06
status: planning
---

# Sprint 5: API Layer Hardening & Data Completeness

## Overview

**Goal:** Deploy all API views, backfill incomplete data, add FK constraints, and establish automated mart refresh — making the database fully production-ready for cfb-app and cfb-scout consumers.

**Why now:** The foundation is solid (PK fixes, indexes, 24 marts, data quality verified). But downstream consumers (cfb-app, cfb-scout) can't use the full API layer because 5 of 7 API views aren't deployed, play_stats has only 2 years of data, and there's no automated refresh pipeline. This sprint closes those gaps.

## Current State

| Component | Status | Gap |
|-----------|--------|-----|
| Data tables | 56+ endpoints loaded, ~8.2 GB | play_stats only has 2024-2025 data |
| Marts | 24 materialized views deployed | No automated refresh |
| API views | 2 of 7 deployed (roster_lookup, recruit_lookup) | 5 views in SQL files but not deployed |
| FK constraints | None | Task #15 pending since Sprint 2 |
| scouting.player_mart | Enhanced with 45 columns | Working, no issues |
| Tests | 229 passing | No mart/API view tests |

## Sprint Tasks

### Phase A: Deploy API Views (Priority 1)

**Task 1: Deploy 5 missing API views**

Deploy the SQL files that already exist but aren't live in the database:
- `api.team_detail` — Team page data (season stats + ratings + EPA)
- `api.team_history` — Multi-season team history
- `api.game_detail` — Single game detail with betting + EPA
- `api.matchup` — Head-to-head team comparison
- `api.leaderboard_teams` — Conference/national leaderboards

**Files:** `src/schemas/api/001-005_*.sql`
**Validation:** `SELECT * FROM api.team_detail WHERE school = 'Alabama' LIMIT 1;`

**Task 2: Add API view tests**

Create `tests/test_api_views.py` that:
- Verifies each view exists and returns rows
- Checks required columns are present
- Tests key filters (team name, season, game_id)

### Phase B: Data Completeness (Priority 2)

**Task 3: Backfill play_stats (2004-2023)**

The `stats.play_stats` table only has 2024-2025 data (~4K rows). Full history (2004-2023) is needed for the play-level EPA marts. Note: `/plays/stats` has a 2000-record limit per request — must iterate by gameId.

**Approach:**
1. Get game IDs from `core.games` for each year
2. Iterate play_stats requests by game_id (avoids 2000-row limit)
3. Estimate: ~120K games x 1 call each = significant API budget usage

**Decision needed:** This will consume a large portion of the 75K monthly API budget. Consider whether to do a phased backfill (recent years first) or full backfill.

**Task 4: Backfill game_havoc history verification**

Verify `stats.game_havoc` has complete data (11.5K rows loaded). Cross-check against expected game count.

### Phase C: Schema Hardening (Priority 3)

**Task 5: Add foreign key constraints**

Add FK constraints for the core relationships:
- `core.drives.game_id` -> `core.games.id`
- `core.plays_*.game_id` -> `core.games.id` (partitioned tables)
- `betting.lines.game_id` -> `core.games.id`
- `core.game_team_stats.game_id` -> `core.games.id`
- `recruiting.recruits.team` -> `ref.teams.school`

**Pattern:** Use `NOT VALID` initially to avoid full table scan, then `VALIDATE CONSTRAINT` asynchronously.

```sql
ALTER TABLE core.drives
  ADD CONSTRAINT fk_drives_game
  FOREIGN KEY (game_id) REFERENCES core.games(id) NOT VALID;

-- Then validate (can be slow, but doesn't lock)
ALTER TABLE core.drives VALIDATE CONSTRAINT fk_drives_game;
```

**Task 6: Add unique constraints matching PKs**

For tables where dlt's merge disposition needs unique constraints:
- `ref.teams (school)`
- `ref.conferences (id)`
- `ref.venues (id)`
- `core.games (id)`

### Phase D: Automated Refresh (Priority 4)

**Task 7: Create consolidated mart refresh function**

Build `marts.refresh_all()` that refreshes materialized views in dependency order:
1. Base marts first (`_game_epa_calc`)
2. Aggregation marts second (`team_epa_season`, `situational_splits`)
3. Derived marts last (`matchup_edges`, `conference_era_summary`)

```sql
CREATE OR REPLACE FUNCTION marts.refresh_all()
RETURNS TABLE(view_name text, refreshed_at timestamptz, duration_ms bigint)
LANGUAGE plpgsql AS $$
...
$$;
```

**Task 8: Create mart refresh SQL script**

`scripts/refresh_marts.sql` — callable via `psql` for manual or cron-based refresh.

### Phase E: Validation & Documentation (Priority 5)

**Task 9: Update SCHEMA_CONTRACT.md**

Add the 5 newly-deployed API views to the schema contract. Document column stability guarantees.

**Task 10: Add mart validation tests**

Create `tests/test_marts.py` that:
- Verifies each mart exists
- Checks for non-zero row counts
- Validates key columns and data types
- Checks refresh function works

## Acceptance Criteria

- [ ] All 7 API views deployed and returning data
- [ ] API view tests pass
- [ ] FK constraints added (at least core relationships)
- [ ] Unique constraints on reference tables
- [ ] `marts.refresh_all()` function works
- [ ] SCHEMA_CONTRACT.md updated
- [ ] Mart tests pass
- [ ] `pytest` passes with all new + existing tests

## Build Order

```
Task 1 (deploy API views) ──→ Task 2 (API tests)
                              ↓
Task 5 (FK constraints) ──→ Task 6 (unique constraints)
                              ↓
Task 7 (refresh function) ──→ Task 8 (refresh script)
                              ↓
Task 9 (schema contract) ──→ Task 10 (mart tests)

Task 3 (play_stats backfill) — independent, run in parallel
Task 4 (game_havoc verify) — independent, run in parallel
```

Tasks 1, 3, 4, 5 can all start in parallel. Tasks 2, 6 depend on 1, 5 respectively.

## Risk Analysis

| Risk | Impact | Mitigation |
|------|--------|------------|
| API view SQL has bugs against live data | Views fail to create | Test each view individually |
| FK constraints fail (orphaned data) | Constraint rejected | Use NOT VALID first, investigate orphans |
| play_stats backfill exceeds API budget | Budget exhausted | Phase backfill: 2020-2023 first |
| Mart refresh too slow | Function timeout | Use CONCURRENTLY where possible |

## API Budget Impact

| Action | Estimated Calls | Notes |
|--------|----------------|-------|
| play_stats backfill (2020-2023) | ~20K | 5K games/year x 4 years |
| play_stats backfill (2004-2019) | ~60K | Deferred if budget tight |
| game_havoc verification | 0 | Query existing data |
| Total (phase 1) | ~20K | Within 75K monthly limit |

## Success Metrics

| Metric | Target |
|--------|--------|
| API views deployed | 7/7 |
| New tests added | 15+ |
| FK constraints | 5+ core relationships |
| Mart refresh function | Working |
| All tests passing | 100% |
