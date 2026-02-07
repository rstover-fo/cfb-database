# Supabase Best Practices Audit

> Full audit of cfb-database Supabase instance covering performance, security,
> frontend integration, and data architecture. Generated 2026-02-06.

---

## Executive Summary

| Area | Grade | Top Issue |
|------|-------|-----------|
| **Performance** | B- | 411 unused indexes (901 MB waste), 86% table cache ratio, core.roster 4,494 seq scans |
| **Security** | D+ | Anon has INSERT/UPDATE/DELETE on public views, RLS disabled everywhere, SECURITY DEFINER on all public views |
| **Frontend Integration** | B | Good server component patterns; SELECT *, waterfall queries, no revalidation |
| **Data Architecture** | B+ | Clean layering; 6 orphan matviews, slow RPCs hitting raw tables, analytics not in refresh chain |

---

## CRITICAL Priority (Fix Now)

### 1. RLS Disabled on Core Tables Exposed via PostgREST

**Severity:** CRITICAL (Supabase security linter: ERROR)

The `core` schema is exposed via PostgREST, but **no tables have RLS enabled**. This means anyone with the anon key can query raw tables directly, bypassing the API view contract.

Affected tables flagged by Supabase security advisor:
- `core.games`, `core.drives`, `core.records`, `core.rankings`
- `core.games__home_line_scores`, `core.games__away_line_scores`
- `core.game_team_stats`, `core.game_team_stats__teams`, `core.game_team_stats__teams__stats`

**Why it matters:** Even though this is public sports data (not sensitive), raw table access:
- Bypasses the schema contract (downstream apps can query tables that may change)
- Allows expensive queries against large raw tables (2.7M plays, 6.4M player stats)
- Exposes dlt internal columns (`_dlt_id`, `_dlt_parent_id`)

**Fix (choose one):**

**Option A — Enable RLS with permissive read policy (recommended):**
```sql
-- For each exposed table:
ALTER TABLE core.games ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read" ON core.games FOR SELECT USING (true);
```
This enables RLS (satisfies the linter) while still allowing reads. You can later tighten policies.

**Option B — Remove raw schemas from PostgREST exposure (cleanest):**
Change `pgrst.db_schemas` to only expose `api` and `public` schemas, not `core`, `stats`, etc. This is the cleanest approach if cfb-app only uses API views and RPCs.

**Scope:** Only 3 schemas have USAGE grants for anon/authenticated: `public`, `core`, and `marts`. The other schemas (ref, stats, ratings, recruiting, betting, draft, metrics, scouting, analytics, core_staging, api) have no USAGE grants, so they're NOT directly API-accessible. Focus remediation on `public`, `core`, and `marts`.

### 2. Anon Role Has INSERT/UPDATE/DELETE on Public Views

**Severity:** CRITICAL

The `anon` role has **full DML privileges** (INSERT, UPDATE, DELETE, TRUNCATE) on all 13 public schema views. This is a read-only analytics database — write access should not exist.

**Fix (immediate):**
```sql
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, TRIGGER, REFERENCES
ON ALL TABLES IN SCHEMA public
FROM anon, authenticated;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon, authenticated;
```

### 3. SECURITY DEFINER Views in Public Schema

**Severity:** CRITICAL (Supabase security linter: ERROR)

All 13 public schema views are defined with `SECURITY DEFINER`, meaning they execute with the **view creator's permissions** rather than the querying user's. This bypasses RLS.

Affected: `teams`, `teams_with_logos`, `games`, `roster`, `team_epa_season`, `team_season_epa`, `defensive_havoc`, `team_style_profile`, `team_season_trajectory`, `team_tempo_metrics`, `team_special_teams_sos`, `transfer_portal_search`, `recruits_search`

**Fix:** Recreate views without `SECURITY DEFINER`:
```sql
-- For each view, drop and recreate without the security_definer property
CREATE OR REPLACE VIEW public.teams AS
  SELECT ... -- same query
  WITH (security_invoker = true);  -- Supabase recommended
```

### 3. Mutable Search Path on All RPCs

**Severity:** HIGH (Supabase security linter: WARN)

All 18 custom functions have mutable `search_path`, which is a potential SQL injection vector if combined with other vulnerabilities.

Affected: Every `get_*` function, `is_garbage_time`, `marts.refresh_all`, `ref.get_era`, etc.

**Fix:** Add `SET search_path = ''` to each function and use fully-qualified table names:
```sql
CREATE OR REPLACE FUNCTION public.get_player_search(...)
RETURNS TABLE(...)
LANGUAGE plpgsql
SET search_path = ''  -- ADD THIS
AS $$
BEGIN
  -- Use public.teams instead of just teams, etc.
END;
$$;
```

---

## HIGH Priority (Fix Soon)

### 5. 411 Unused Indexes Consuming 901 MB

**Severity:** HIGH — 11.5% of total database size is wasted on indexes that have never been scanned.

Top unused indexes by size:

| Table | Index | Size |
|-------|-------|------|
| `stats.player_season_stats` | `player_season_stats__dlt_id_key` | 89 MB |
| `core.drives` | `drives__dlt_id_key` | 43 MB |
| `marts.team_situational_success` | `idx_situational_success_pk` | 40 MB |
| `marts.team_playcalling_tendencies` | `idx_playcalling_tendencies_pk` | 40 MB |
| `core.drives` | `idx_drives_game_id_drive_number` | 33 MB |
| `stats.player_season_stats` | `idx_player_season_stats_team` | 29 MB |
| All `plays_y{year}__dlt_id_season_idx` | 22 partitions | ~6-11 MB each |

Additionally, 2 **duplicate index pairs**: `core.drives` has `idx_drives_game_id_offense` and `idx_drives_game_offense` (same columns), `core.games` has `games_id_unique` and `uq_games_id`.

Note: `stats.player_season_stats` has index size (371 MB) that is **2.1x its table size** (175 MB) — heavily over-indexed.

**Fix:** Drop unused indexes in batches. Start with the largest ones. This will also improve cache hit ratio by freeing buffer space.

### 6. core.roster: 4,494 Sequential Scans (771M Tuples Read)

The worst sequential scan offender. Has indexes on `player_id` but common query patterns aren't hitting them.

Other seq scan concerns:
| Table | Seq Scans | Tuples Read | Index Scans |
|-------|-----------|-------------|-------------|
| `core.roster` | 4,494 | 771M | 87K |
| `marts.team_situational_success` | 439 | 111M | 29 |
| `marts.play_epa` | 44 | 96M | 44 |
| `core.game_team_stats__teams__stats` | 79 | 70M | 277 |

**Fix:** Investigate which queries are causing roster seq scans. Likely needs a composite index for common WHERE patterns (team + season).

### 7. Slow RPCs Hitting Raw Tables (Not Matviews)

Several RPCs bypass the marts layer entirely and query raw `core.plays`/`core.drives`:

| RPC | Execution Time | Should Use |
|-----|---------------|------------|
| `get_down_distance_splits` | **386ms** | `marts.play_epa` (has EPA + indexes) |
| `get_red_zone_splits` | **124ms** | `marts.scoring_opportunities` or `core.drives` (already OK) |
| `get_field_position_splits` | 52ms | `marts.play_epa` |
| `get_home_away_splits` | 42ms | `marts.play_epa` + `core.games` |
| `get_conference_splits` | 40ms | `marts.play_epa` + `core.games` |

**Fix:** Refactor `get_down_distance_splits` (worst offender) to use `marts.play_epa` instead of `core.plays`. The matview already has EPA calculations, success/explosive flags, down/distance buckets, and field position — exactly what these RPCs need.

### 8. Missing Index on core.game_team_stats(id)

`api.game_box_score` does a seq scan on 25K rows to find a single game because `core.game_team_stats` has no index on `id` (only `_dlt_id_key`).

**Fix:**
```sql
CREATE INDEX idx_game_team_stats_id ON core.game_team_stats(id);
```

### 9. Table Cache Hit Ratio at 86% (Target: 99%+)

**Current:** Table cache hit ratio is 86.47%, index cache hit ratio is 99.03%.

The table ratio is low because the working set exceeds available shared_buffers. Top tables by size:

| Table | Total Size | Rows |
|-------|-----------|------|
| `core.game_player_stats__...__athletes` | 1,042 MB | 6.4M |
| `marts.play_epa` | 953 MB | 2.7M |
| `stats.play_stats` | 881 MB | 2.6M |
| `stats.player_season_stats` | 546 MB | 1.2M |
| `core.drives` | 494 MB | 548K |
| `scouting.player_embeddings` | 417 MB | 26K |

Total database footprint is roughly **6-7 GB**.

**Fix options:**
- **Upgrade Supabase plan** if on free tier — Pro plan gives more RAM (and thus more shared_buffers)
- **Drop unused data:** `scouting.player_embeddings` is 417 MB for only 26K rows (vector data). If cfb-scout isn't actively using it, consider truncating
- **Column-level SELECT:** Ensure queries only select needed columns (see frontend findings below)

### 5. Extensions in Public Schema

**Severity:** MEDIUM (Supabase security linter: WARN)

`vector` and `pg_trgm` extensions are installed in `public` schema. Supabase recommends moving them to the `extensions` schema.

**Fix:**
```sql
-- Move extensions (requires superuser, may need to be done via Supabase dashboard)
ALTER EXTENSION pg_trgm SET SCHEMA extensions;
ALTER EXTENSION vector SET SCHEMA extensions;
```

Note: This may break function references. Test in a branch first.

### 6. Materialized Views Directly Accessible via API

**Severity:** MEDIUM (Supabase security linter: WARN)

`marts.player_comparison` and `marts.player_game_epa` are directly queryable by anon/authenticated roles. Per the schema contract, marts should be accessed through API views, not directly.

**Fix:** Revoke direct access:
```sql
REVOKE SELECT ON marts.player_comparison FROM anon, authenticated;
REVOKE SELECT ON marts.player_game_epa FROM anon, authenticated;
```

Ensure the API views that wrap these matviews still work (they query as the view owner, not the caller).

---

## MEDIUM Priority (Improve When Convenient)

### 7. Frontend: SELECT * Patterns

Several cfb-app query files use `.select('*')` or omit `.select()` entirely, pulling all columns when only a subset is needed. This wastes bandwidth and prevents Supabase from optimizing queries.

**Examples found:**
- Dashboard queries pulling full `team_epa_season` when only team + epa_per_play needed
- Game detail pulling all columns when the component only renders 8 fields
- Rankings queries pulling full row when only rank + team + record shown

**Fix:** Use explicit column selection:
```typescript
// Instead of:
const { data } = await supabase.from('team_detail').select('*')

// Use:
const { data } = await supabase.from('team_detail').select('school, wins, losses, sp_rating, epa_per_play, logo_url')
```

### 8. Frontend: Missing Error Handling on Some Queries

Some Supabase calls don't check the `error` property from the response, which silently swallows database errors.

**Fix:** Always destructure and handle errors:
```typescript
const { data, error } = await supabase.from('team_detail').select('...')
if (error) throw new Error(`Failed to fetch team: ${error.message}`)
```

### 9. Frontend: Waterfall Queries That Could Be Parallelized

Some page components make sequential Supabase calls that have no dependency between them. These should use `Promise.all()`.

**Example pattern to fix:**
```typescript
// Instead of sequential:
const team = await getTeamDetail(slug)
const history = await getTeamHistory(slug)
const playcalling = await getPlaycallingProfile(slug)

// Use parallel:
const [team, history, playcalling] = await Promise.all([
  getTeamDetail(slug),
  getTeamHistory(slug),
  getPlaycallingProfile(slug),
])
```

### 10. Schema Redundancy: analytics vs marts

Two pairs of overlapping matviews exist:
- `analytics.team_season_summary` (9,374 rows) overlaps with `marts.team_season_summary` (9,374 rows)
- `analytics.game_results` (45,885 rows) provides similar data to `api.game_detail`

**Recommendation:**
- Drop `analytics.team_season_summary` (marts version is the canonical one)
- Keep `analytics.game_results` only if something queries it directly
- Promote `analytics.player_career_stats` (628K rows) to marts — it's the only analytics matview with unique value
- Promote `analytics.team_recruiting_trend` (4,356 rows) to marts — useful for recruiting charts

### 11. RPCs Without Source Files

4 RPCs exist in the database but have no SQL source file in `src/schemas/`:
- `get_player_detail`
- `get_player_game_log`
- `get_player_percentiles`
- `get_player_season_leaders` (RPC version)

**Risk:** These can't be reproduced from source if the database needs rebuilding.

**Fix:** Extract current definitions and save to `src/schemas/functions/`:
```sql
SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = 'get_player_detail';
```

### 12. Public Schema View Duplication

Several public views are thin wrappers over marts matviews with no transformation:
- `public.defensive_havoc` = `SELECT * FROM marts.defensive_havoc`
- `public.team_style_profile` = `SELECT * FROM marts.team_style_profile`
- `public.team_season_trajectory` = `SELECT * FROM marts.team_season_trajectory`
- `public.team_tempo_metrics` = `SELECT * FROM marts.team_tempo_metrics`

Once cfb-app migrates to `api.*` views, these can be dropped.

---

## LOW Priority (Consider Later)

### 13. Index Overhead on Matviews

The 96 indexes on marts matviews total significant storage. Some matviews have 5-6 indexes on small tables (< 5K rows). For tables under 10K rows, Postgres often does sequential scans anyway because it's faster.

**Consider removing indexes on:**
- `marts.conference_era_summary` (172 rows, 3 indexes)
- `marts.data_freshness` (23 rows, 1 index)
- `marts.conference_comparison` (826 rows, 3 indexes)
- `marts.transfer_portal_impact` (1,406 rows, 4 indexes)

### 14. play_stats Table (881 MB, Partially Loaded)

`stats.play_stats` is 881 MB with 2.6M rows but is only loaded for 2024-2025. If you load all years, this could grow to 15-20M rows. Consider whether this data is needed or if `marts.play_epa` (which already has per-play EPA) covers the use case.

### 15. player_embeddings Size vs Row Count

`scouting.player_embeddings` is 417 MB for only 26K rows (vector embeddings). The index alone is 206 MB. This is expected for vector indexes but worth checking if cfb-scout is actively using similarity search. If not, this is dead weight.

---

## Summary Action Items

| # | Priority | Issue | Effort |
|---|----------|-------|--------|
| 1 | CRITICAL | Enable RLS on core tables or remove from PostgREST | 1-2 hrs |
| 2 | CRITICAL | Revoke INSERT/UPDATE/DELETE from anon on public views | 15 min |
| 3 | CRITICAL | Fix SECURITY DEFINER on 13 public views | 1 hr |
| 4 | HIGH | Add `SET search_path = ''` to all 18 RPCs | 2-3 hrs |
| 5 | HIGH | Drop 411 unused indexes (free 901 MB) | 1-2 hrs |
| 6 | HIGH | Investigate core.roster seq scans (4,494 scans, 771M tuples) | 1 hr |
| 7 | HIGH | Refactor slow RPCs to use matviews (get_down_distance_splits 386ms) | 2-3 hrs |
| 8 | HIGH | Add index on core.game_team_stats(id) for game_box_score | 5 min |
| 9 | HIGH | Investigate table cache hit ratio (86%) / consider plan upgrade | 30 min |
| 10 | MEDIUM | Move pg_trgm + vector to extensions schema | 30 min |
| 11 | MEDIUM | Revoke direct marts access from anon role | 15 min |
| 12 | MEDIUM | Add explicit column selection in cfb-app queries | 2 hrs |
| 13 | MEDIUM | Add error handling to cfb-app Supabase calls | 1 hr |
| 14 | MEDIUM | Parallelize waterfall queries in cfb-app | 1 hr |
| 15 | MEDIUM | Consolidate analytics/marts schema overlap | 1 hr |
| 16 | MEDIUM | Backfill 4 missing RPC source files | 30 min |
| 17 | LOW | Remove duplicate public views after cfb-app migration | 30 min |
| 18 | LOW | Audit index overhead on small matviews | 30 min |
| 19 | LOW | Assess play_stats full-history loading plan | 15 min |
| 20 | LOW | Check player_embeddings usage in cfb-scout | 15 min |
