---
title: "feat: Supabase Security & Performance Hardening"
type: feat
date: 2026-02-06
---

# Supabase Security & Performance Hardening

## Overview

Implement all CRITICAL and HIGH priority fixes from the Supabase Best Practices Audit (`docs/SUPABASE_BEST_PRACTICES_AUDIT.md`). This covers 9 action items across security hardening (RLS, permissions, search_path) and performance optimization (unused indexes, seq scans, slow RPCs, missing indexes).

## Problem Statement

The Supabase security linter reports **13 ERROR-level** and **24 WARN-level** findings. Key issues:

1. **Security D+**: Anon role has DML (INSERT/UPDATE/DELETE) on all public views; RLS disabled on all core tables exposed via PostgREST; all 13 public views use SECURITY DEFINER; all 18+ RPCs have mutable search_path
2. **Performance B-**: 411 unused indexes consuming 901 MB; core.roster has 4,494 seq scans; 5 RPCs query raw `core.plays` instead of `marts.play_epa`; `core.game_team_stats` missing index on `id`; table cache hit ratio at 86%

## Proposed Solution

Three migration files + updated SQL source files, executed via Supabase MCP `apply_migration`.

### Phase 1: Security Hardening (Migration: `security_hardening`)

**1a. Revoke DML from anon/authenticated on public schema**
```sql
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, TRIGGER, REFERENCES
ON ALL TABLES IN SCHEMA public FROM anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon, authenticated;
```

**1b. Enable RLS on core tables exposed via PostgREST + add permissive read policies**

9 tables flagged by Supabase security advisor:
- `core.games`, `core.drives`, `core.records`, `core.rankings`
- `core.games__home_line_scores`, `core.games__away_line_scores`
- `core.game_team_stats`, `core.game_team_stats__teams`, `core.game_team_stats__teams__stats`

For each:
```sql
ALTER TABLE core.games ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_read" ON core.games FOR SELECT USING (true);
```

**1c. Fix SECURITY DEFINER on 13 public views → SECURITY INVOKER**

All 13 public views need `CREATE OR REPLACE VIEW ... WITH (security_invoker = true)`. The view definitions are in:
- `src/schemas/public/001_convenience_views.sql` (teams, teams_with_logos, games, roster)
- `src/schemas/public/002_marts_views.sql` (defensive_havoc, team_epa_season, team_season_epa, team_season_trajectory, team_style_profile, team_tempo_metrics)
- `src/schemas/public/003_ratings_views.sql` (team_special_teams_sos)
- `src/schemas/public/010_roster_builder_views.sql` (transfer_portal_search, recruits_search)

**1d. Revoke direct marts access from anon**
```sql
REVOKE SELECT ON marts.player_comparison FROM anon, authenticated;
REVOKE SELECT ON marts.player_game_epa FROM anon, authenticated;
```

### Phase 2: Search Path Hardening (Migration: `fix_search_path`)

Add `SET search_path = ''` to all 21 functions flagged by security linter and use fully-qualified table names:

| Function | Schema | Source File |
|----------|--------|-------------|
| `get_available_seasons` | public | `004_season_lookup_functions.sql` |
| `get_available_weeks` | public | `004_season_lookup_functions.sql` |
| `get_home_away_splits` | public | `005_team_split_functions.sql` |
| `get_conference_splits` | public | `005_team_split_functions.sql` |
| `get_down_distance_splits` | public | `006_play_analysis_functions.sql` |
| `get_field_position_splits` | public | `006_play_analysis_functions.sql` |
| `get_red_zone_splits` | public | `006_play_analysis_functions.sql` |
| `get_player_season_stats_pivoted` | public | `007_player_stats_function.sql` |
| `get_trajectory_averages` | public | `008_trajectory_averages_function.sql` |
| `get_player_search` | public | `009_player_search_function.sql` |
| `get_conference_head_to_head` | public | `010_conference_h2h_function.sql` |
| `get_data_freshness` | public | `011_data_freshness_function.sql` |
| `get_drive_patterns` | public | `functions/get_drive_patterns.sql` |
| `is_garbage_time` | public | `functions/is_garbage_time.sql` |
| `refresh_all` | marts | `functions/refresh_all_marts.sql` |
| `get_era` | ref | (no source file — extract from DB) |
| `refresh_all_views` | analytics | (no source file — extract from DB) |
| `refresh_player_mart` | scouting | (no source file — extract from DB) |
| `get_player_detail` | public | (no source file — extract from DB) |
| `get_player_game_log` | public | (no source file — extract from DB) |
| `get_player_percentiles` | public | (no source file — extract from DB) |
| `get_player_season_leaders` | public | (no source file — extract from DB) |

For functions without source files, extract via `pg_get_functiondef()`, add `SET search_path = ''`, fully-qualify table refs, and save to `src/schemas/functions/`.

### Phase 3: Performance Optimization (Migration: `performance_optimization`)

**3a. Drop unused indexes (top 40 by size, non-unique, zero scans)**

Key targets:
- `core.drives`: `idx_drives_game_id_drive_number` (33 MB)
- `stats.player_season_stats`: `idx_player_season_stats_team` (29 MB), `idx_player_season_stats_stat_type` (28 MB)
- `marts.play_epa`: `play_epa_offense_season_idx` (19 MB), `play_epa_field_position_idx` (18 MB), `play_epa_down_name_distance_bucket_idx` (18 MB), `play_epa_is_garbage_time_idx` (17 MB)
- `stats.play_stats`: `idx_play_stats_athlete` (18 MB), `idx_play_stats_stat_type` (17 MB)
- `analytics.player_career_stats`: 3 indexes (19 MB total)
- `core.plays_y*`: 22 `ppa_idx` partitions (~80 MB total), 22 `game_id_drive_id_idx` and `drive_id_idx` partitions
- Duplicate: `core.drives` has `idx_drives_game_id_offense` (10 MB) AND `idx_drives_game_offense` (5 MB) — drop the smaller one

**3b. Add missing index on `core.game_team_stats(id)`**
```sql
CREATE INDEX idx_game_team_stats_id ON core.game_team_stats(id);
```

**3c. Add composite index on `core.roster(team, year, id)`** to address 4,494 seq scans
```sql
CREATE INDEX idx_roster_team_year_id ON core.roster(team, year, id);
```
(Existing `idx_roster_team_year` covers `(team, year)` but views joining on `id` too need the covering index.)

### Phase 4: Slow RPC Refactor

Refactor `get_down_distance_splits`, `get_field_position_splits`, and `get_red_zone_splits` to use `marts.play_epa` instead of `core.plays JOIN core.games`.

`marts.play_epa` already has: `season`, `offense`, `defense`, `down`, `distance`, `distance_bucket`, `field_position`, `epa`, `success`, `yards_gained`, `scoring`, `is_garbage_time`, `play_category`.

**get_down_distance_splits** refactor (386ms → ~20ms target):
- Replace `FROM core.plays p JOIN core.games g ON p.game_id = g.id WHERE g.season = p_season` with `FROM marts.play_epa WHERE season = p_season`
- Use existing `distance_bucket` column instead of CASE expression
- Use `epa` column (already calculated) instead of `ppa`
- Use `success` column instead of `CASE WHEN ppa > 0`
- Filter `NOT is_garbage_time`

**get_field_position_splits** refactor (52ms → ~15ms target):
- Same FROM change
- Use `field_position` column instead of CASE yardline expressions

**get_red_zone_splits** — keep as-is (uses `core.drives` which is appropriate for drive-level aggregation, and 124ms is acceptable)

## Acceptance Criteria

### Security
- [x] Supabase security linter shows 0 ERROR findings
- [x] Supabase security linter shows 0 WARN findings for search_path
- [x] `anon` role has SELECT-only on public schema
- [x] RLS enabled on all core tables exposed via PostgREST
- [x] All public views use SECURITY INVOKER (not DEFINER)
- [x] All RPCs have `SET search_path = ''`
- [x] Direct marts access revoked from anon

### Performance
- [x] 200+ MB freed from dropped unused indexes
- [x] `core.game_team_stats(id)` index exists
- [x] `core.roster(team, year, id)` covering index exists
- [x] `get_down_distance_splits` execution < 50ms
- [x] `get_field_position_splits` execution < 30ms

### Compatibility
- [x] All existing tests pass (572 tests)
- [x] cfb-app queries still work (views return same data)
- [x] `marts.refresh_all()` still works
- [x] All RPCs return same results (just faster)

## Implementation Tasks

### Phase 1: Security Hardening
- [x] Apply migration: revoke DML from anon/authenticated on public schema
- [x] Apply migration: enable RLS + permissive read policies on 9 core tables
- [x] Update 4 source files to add `WITH (security_invoker = true)` to all 13 views
- [x] Apply migration: recreate all 13 views with security_invoker
- [x] Apply migration: revoke direct marts access from anon
- [x] Verify: run `get_advisors(security)` — 0 ERROR findings for RLS/SECURITY DEFINER

### Phase 2: Search Path Hardening
- [x] Extract 6 functions without source files via `pg_get_functiondef()`
- [x] Save extracted functions to `src/schemas/functions/`
- [x] Update all 11 source files: add `SET search_path = ''`, fully-qualify table names
- [x] Apply migration: recreate all 21+ functions with search_path fix
- [x] Verify: run `get_advisors(security)` — 0 WARN findings for search_path

### Phase 3: Performance Optimization
- [x] Apply migration: drop top ~40 unused indexes (>1MB, zero scans, non-unique)
- [x] Apply migration: drop duplicate index `idx_drives_game_offense`
- [x] Apply migration: add `idx_game_team_stats_id`
- [x] Apply migration: add `idx_roster_team_year_id`
- [x] Verify: check index count decreased, total index size decreased

### Phase 4: Slow RPC Refactor
- [x] Refactor `get_down_distance_splits` to use `marts.play_epa`
- [x] Refactor `get_field_position_splits` to use `marts.play_epa`
- [x] Add `SET search_path = ''` to refactored RPCs (done as part of Phase 2)
- [x] Apply migration: deploy refactored RPCs
- [x] Verify: execution time < 50ms for both RPCs

### Phase 5: Testing & Validation
- [x] Run full test suite: `pytest -q` (574 tests)
- [x] Run Supabase security advisor: 0 ERRORs
- [x] Run Supabase performance advisor: 0 ERRORs, 1 WARN (duplicate games index — fixed)
- [x] Verify cfb-app still works (spot-check key views)
- [x] Update `docs/SUPABASE_BEST_PRACTICES_AUDIT.md` with results
- [x] Commit and push

## Dependencies & Risks

**Risks:**
- **SECURITY INVOKER change**: Views that cross schema boundaries (e.g., `public.teams` reading from `ref.teams`) need the calling role to have SELECT on the underlying tables. Since `anon` has USAGE on `core` schema, this should work. But `ref` schema may not have USAGE grants — verify before applying.
- **search_path = ''**: All table references must be fully qualified. Missing one will break the function at runtime. Test each function after migration.
- **Index drops**: Dropping indexes is irreversible (must recreate). Double-check that zero-scan indexes aren't used by matview refreshes (which reset stats).
- **RPC refactor**: `marts.play_epa` excludes garbage time plays. If the original RPC included them, results will differ. The original doesn't filter garbage time, so we should NOT filter it in the refactor either.

**Mitigations:**
- Apply migrations one phase at a time
- Run tests after each phase
- Keep `docs/SUPABASE_BEST_PRACTICES_AUDIT.md` updated with results

## References

- Audit report: `docs/SUPABASE_BEST_PRACTICES_AUDIT.md`
- Source files: `src/schemas/public/001-011_*.sql`, `src/schemas/functions/*.sql`
- Schema contract: `docs/SCHEMA_CONTRACT.md`
- Supabase security linter docs: https://supabase.com/docs/guides/database/database-linter
