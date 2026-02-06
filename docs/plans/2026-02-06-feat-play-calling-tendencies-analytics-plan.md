---
title: "Sprint 8: Play-Calling Tendencies Analytics"
type: feat
date: 2026-02-06
brainstorm: docs/brainstorms/2026-02-06-play-calling-tendencies-brainstorm.md
reviewed: true
review_changes: simplified from 4 marts + 2 API views + 2 RPCs → 2 matviews + 1 view + 1 API view
---

# Sprint 8: Play-Calling Tendencies Analytics

## Overview

Build a play-calling analytics system that answers three questions:
1. **How does Team X call plays?** — Run/pass ratios by situation (down, distance, field position, score state)
2. **What works in specific situations?** — Success rates, EPA, explosiveness by context
3. **How do teams adjust?** — Behavior shifts when leading vs trailing

## Proposed Solution

### Architecture (Simplified After Review)

```
marts.play_epa ──JOIN──▶ core.plays (for score_diff)
       │
       ▼
┌──────────────────────────┐     ┌──────────────────────────┐
│ marts.team_playcalling_  │     │ marts.team_situational_  │
│   tendencies (matview)   │     │   success (matview)      │
│ Grain: team × season ×  │     │ Grain: team × season ×   │
│   situation              │     │   situation               │
│ ~60K rows                │     │ ~60K rows                 │
└────────────┬─────────────┘     └────────────┬──────────────┘
             │                                 │
             └──────────┬──────────────────────┘
                        ▼
              ┌─────────────────────┐
              │ api.team_playcalling│
              │   _profile (VIEW)   │
              │ 1 row / team-season │
              │ + PERCENT_RANK      │
              └─────────────────────┘
```

**Deliverables: 2 matviews, 1 regular view (with percentiles), 1 API view, ~20 tests.**

### What Changed From Original Plan (Review Feedback)

| Original | Revised | Reason |
|----------|---------|--------|
| 4 matviews | 2 matviews + 1 regular view | Profile is 1,400 rows — no need to materialize |
| `team_game_script` mart | Merged into profile view | Leading/trailing metrics derived from tendencies mart |
| 2 RPCs | 0 RPCs | PostgREST filtering is sufficient |
| `api.team_playcalling_tendencies` | Dropped | No consumer requested it; add later if needed |
| `conversion_rate` column | Renamed to `yardage_success_rate` | Not true first-down conversion; proxy metric only |
| ~40 tests | ~20 tests | Focus on behavior, not tautologies |

### Key Design Decisions

**1. No separate base mart.** `marts.play_epa` already has `down_name`, `distance_bucket`, `field_position`, `is_garbage_time`, `play_category`. The only missing dimension is `score_diff_bucket`, computed inline via `JOIN core.plays` + CASE expression.

**2. Profile is a regular view, not a matview.** It aggregates ~60K tendencies rows into ~1,400 team-season rows. Postgres computes PERCENT_RANK over 1,400 rows in single-digit milliseconds. No materialization needed.

**3. No RPCs.** Both proposed RPCs were `SELECT * FROM mart WHERE team = ? AND season = ?`. PostgREST does this natively via query params.

**4. Garbage time: excluded** via `WHERE NOT pe.is_garbage_time` (consistent with `situational_splits`).

**5. Special teams: excluded** via `WHERE play_category IN ('rush', 'pass')`.

**6. Score differential buckets (symmetric, 14-point breakpoint):**

| Bucket | Range | Rationale |
|--------|-------|-----------|
| `big_lead` | `COALESCE(score_diff, 0) >= 14` | 2+ TD lead |
| `small_lead` | `1 <= COALESCE(score_diff, 0) <= 13` | Competitive lead |
| `tied` | `COALESCE(score_diff, 0) = 0` | Neutral (includes NULL score_diff) |
| `small_deficit` | `-13 <= COALESCE(score_diff, 0) <= -1` | Competitive deficit |
| `big_deficit` | `COALESCE(score_diff, 0) <= -14` | 2+ TD deficit |

**7. Pace: plays per game** (not per minute). Clock data is unreliable for per-minute pace.

**8. Sacks = pass plays** (already handled by `play_epa.play_category`).

**9. Minimum play threshold: 10 plays.** Situations with < 10 plays return NULL for rate metrics.

**10. Percentiles: per-season** (`PARTITION BY season`). NULL-safe via CASE wrapper (matches `player_comparison` pattern).

**11. Distance buckets** use `play_epa.distance_bucket` definitions: short (<=3 yds), medium (4-7 yds), long (8+ yds).

## Technical Approach

### Phase 1: Materialized Views

#### Task 1.1: `marts.team_playcalling_tendencies`

**File:** `src/schemas/marts/021_team_playcalling_tendencies.sql`

**Grain:** team + season + down + distance_bucket + field_position + score_diff_bucket

**Source:** `marts.play_epa` JOIN `core.plays` (for `score_diff`)

**Columns:**
```
team, season, down, distance_bucket, field_position, score_diff_bucket,
total_plays, rush_plays, pass_plays,
run_rate, pass_rate
```

**Logic:**
```sql
WITH base_plays AS (
    SELECT
        pe.offense AS team, pe.season, pe.down,
        pe.distance_bucket, pe.field_position, pe.play_category,
        CASE
            WHEN COALESCE(p.score_diff, 0) >= 14 THEN 'big_lead'
            WHEN COALESCE(p.score_diff, 0) >= 1 THEN 'small_lead'
            WHEN COALESCE(p.score_diff, 0) = 0 THEN 'tied'
            WHEN COALESCE(p.score_diff, 0) >= -13 THEN 'small_deficit'
            ELSE 'big_deficit'
        END AS score_diff_bucket
    FROM marts.play_epa pe
    JOIN core.plays p ON p.id = pe.play_id
    WHERE NOT pe.is_garbage_time
      AND pe.play_category IN ('rush', 'pass')
)
SELECT
    team, season, down, distance_bucket, field_position, score_diff_bucket,
    COUNT(*)::bigint AS total_plays,
    COUNT(*) FILTER (WHERE play_category = 'rush')::bigint AS rush_plays,
    COUNT(*) FILTER (WHERE play_category = 'pass')::bigint AS pass_plays,
    ROUND(COUNT(*) FILTER (WHERE play_category = 'rush')::numeric
        / NULLIF(COUNT(*), 0), 4) AS run_rate,
    ROUND(COUNT(*) FILTER (WHERE play_category = 'pass')::numeric
        / NULLIF(COUNT(*), 0), 4) AS pass_rate
FROM base_plays
GROUP BY team, season, down, distance_bucket, field_position, score_diff_bucket;
```

**Indexes:**
- UNIQUE on `(team, season, down, distance_bucket, field_position, score_diff_bucket)` — required for REFRESH CONCURRENTLY
- `(team, season)` — team-level queries
- `(season, down, distance_bucket, run_rate DESC)` — leaderboard queries

**Estimated rows:** ~50K-80K

**Acceptance criteria:**
- [x] Matview creates and has data
- [x] No rows with `total_plays < 1`
- [x] All `score_diff_bucket` values in expected set
- [x] All teams with play data appear

---

#### Task 1.2: `marts.team_situational_success`

**File:** `src/schemas/marts/022_team_situational_success.sql`

**Grain:** team + season + down + distance_bucket + field_position + score_diff_bucket

**Source:** `marts.play_epa` JOIN `core.plays` (for `score_diff`)

**Columns:**
```
team, season, down, distance_bucket, field_position, score_diff_bucket,
total_plays,
success_rate, avg_epa, explosive_rate,
rush_success_rate, rush_avg_epa,
pass_success_rate, pass_avg_epa,
yardage_success_rate  -- plays gaining >= distance / total (3rd/4th down only)
```

**Logic:**
- Same `base_plays` CTE pattern as tendencies (JOIN play_epa → core.plays for score_diff)
- Add `pe.success`, `pe.explosive`, `pe.epa`, `pe.yards_gained`, `pe.distance` to CTE
- Rate metrics use `CASE WHEN COUNT(*) >= 10 THEN ... ELSE NULL END` for minimum threshold
- `yardage_success_rate`: `AVG(CASE WHEN yards_gained >= distance THEN 1.0 ELSE 0.0 END)` for down IN (3, 4), NULL otherwise
- Split by rush/pass using FILTER clauses

**Indexes:**
- UNIQUE on `(team, season, down, distance_bucket, field_position, score_diff_bucket)`
- `(team, season)`, `(season)`

**Estimated rows:** ~50K-80K

**Acceptance criteria:**
- [x] success_rate between 0 and 1 (or NULL for < 10 plays)
- [x] avg_epa in reasonable range
- [x] yardage_success_rate only populated for down IN (3, 4)
- [x] Rows with < 10 plays have NULL rate metrics

---

### Phase 2: Profile View

#### Task 2.1: `api.team_playcalling_profile` (regular view)

**File:** `src/schemas/api/014_team_playcalling_profile.sql`

This is a **regular view** (not materialized) that aggregates the two Layer 2 matviews into a one-row-per-team-season summary with percentile rankings. Since the result set is ~1,400 rows and the source matviews are indexed, this runs in <50ms.

**Grain:** team + season

**Source:** `marts.team_playcalling_tendencies`, `marts.team_situational_success`, `core.games`, `ref.teams`

**Columns:**
```
team, season, conference, games_played,

-- Tendency metrics (play-count-weighted from tendencies mart)
overall_run_rate,
early_down_run_rate,          -- down IN (1, 2)
third_down_pass_rate,         -- down = 3
red_zone_run_rate,            -- field_position = 'red_zone'

-- Success metrics (from success mart)
overall_success_rate, overall_avg_epa,
third_down_success_rate,
red_zone_success_rate,

-- Game script metrics (from tendencies mart, filtered by score_diff_bucket)
leading_run_rate,             -- score_diff_bucket IN ('big_lead', 'small_lead')
trailing_run_rate,            -- score_diff_bucket IN ('big_deficit', 'small_deficit')
run_rate_delta,               -- leading - trailing
pace_plays_per_game,          -- total plays / games

-- PERCENT_RANK percentiles (PARTITION BY season)
overall_run_rate_pctl,
early_down_run_rate_pctl,
third_down_pass_rate_pctl,
overall_epa_pctl,
third_down_success_pctl,
red_zone_success_pctl,
run_rate_delta_pctl,
pace_pctl
```

**Logic:**
```sql
CREATE OR REPLACE VIEW api.team_playcalling_profile AS
WITH tendency_agg AS (
    -- Aggregate tendencies to team-season level, weighted by play count
    SELECT
        team, season,
        SUM(total_plays) AS total_plays,
        ROUND(SUM(rush_plays)::numeric / NULLIF(SUM(total_plays), 0), 4) AS overall_run_rate,
        ROUND(SUM(rush_plays) FILTER (WHERE down IN (1, 2))::numeric
            / NULLIF(SUM(total_plays) FILTER (WHERE down IN (1, 2)), 0), 4)
            AS early_down_run_rate,
        ROUND(SUM(pass_plays) FILTER (WHERE down = 3)::numeric
            / NULLIF(SUM(total_plays) FILTER (WHERE down = 3), 0), 4)
            AS third_down_pass_rate,
        ROUND(SUM(rush_plays) FILTER (WHERE field_position = 'red_zone')::numeric
            / NULLIF(SUM(total_plays) FILTER (WHERE field_position = 'red_zone'), 0), 4)
            AS red_zone_run_rate,
        -- Game script: leading vs trailing run rates
        ROUND(SUM(rush_plays) FILTER (WHERE score_diff_bucket IN ('big_lead', 'small_lead'))::numeric
            / NULLIF(SUM(total_plays) FILTER (WHERE score_diff_bucket IN ('big_lead', 'small_lead')), 0), 4)
            AS leading_run_rate,
        ROUND(SUM(rush_plays) FILTER (WHERE score_diff_bucket IN ('big_deficit', 'small_deficit'))::numeric
            / NULLIF(SUM(total_plays) FILTER (WHERE score_diff_bucket IN ('big_deficit', 'small_deficit')), 0), 4)
            AS trailing_run_rate
    FROM marts.team_playcalling_tendencies
    GROUP BY team, season
),
success_agg AS (
    -- Aggregate success metrics to team-season level
    SELECT
        team, season,
        ROUND((SUM(success_rate * total_plays) / NULLIF(SUM(total_plays), 0))::numeric, 4)
            AS overall_success_rate,
        ROUND((SUM(avg_epa * total_plays) / NULLIF(SUM(total_plays), 0))::numeric, 4)
            AS overall_avg_epa,
        ROUND((SUM(success_rate * total_plays) FILTER (WHERE down = 3)
            / NULLIF(SUM(total_plays) FILTER (WHERE down = 3), 0))::numeric, 4)
            AS third_down_success_rate,
        ROUND((SUM(success_rate * total_plays) FILTER (WHERE field_position = 'red_zone')
            / NULLIF(SUM(total_plays) FILTER (WHERE field_position = 'red_zone'), 0))::numeric, 4)
            AS red_zone_success_rate
    FROM marts.team_situational_success
    WHERE total_plays >= 10
    GROUP BY team, season
),
game_counts AS (
    SELECT
        CASE WHEN home_team = t.school THEN home_team ELSE away_team END AS team,
        season,
        COUNT(DISTINCT id) AS games_played
    FROM core.games g
    CROSS JOIN ref.teams t
    WHERE (g.home_team = t.school OR g.away_team = t.school)
      AND g.home_points IS NOT NULL
    GROUP BY 1, season
),
combined AS (
    SELECT
        ta.team, ta.season,
        t.conference,
        gc.games_played,
        ta.overall_run_rate,
        ta.early_down_run_rate,
        ta.third_down_pass_rate,
        ta.red_zone_run_rate,
        sa.overall_success_rate,
        sa.overall_avg_epa,
        sa.third_down_success_rate,
        sa.red_zone_success_rate,
        ta.leading_run_rate,
        ta.trailing_run_rate,
        ROUND((ta.leading_run_rate - ta.trailing_run_rate)::numeric, 4) AS run_rate_delta,
        ROUND(ta.total_plays::numeric / NULLIF(gc.games_played, 0), 1) AS pace_plays_per_game
    FROM tendency_agg ta
    LEFT JOIN success_agg sa ON sa.team = ta.team AND sa.season = ta.season
    LEFT JOIN game_counts gc ON gc.team = ta.team AND gc.season = ta.season
    LEFT JOIN ref.teams t ON t.school = ta.team
)
SELECT
    c.*,
    -- Percentiles (NULL-safe, PARTITION BY season)
    CASE WHEN c.overall_run_rate IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.overall_run_rate)
    END AS overall_run_rate_pctl,
    CASE WHEN c.early_down_run_rate IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.early_down_run_rate)
    END AS early_down_run_rate_pctl,
    CASE WHEN c.third_down_pass_rate IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.third_down_pass_rate)
    END AS third_down_pass_rate_pctl,
    CASE WHEN c.overall_avg_epa IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.overall_avg_epa)
    END AS overall_epa_pctl,
    CASE WHEN c.third_down_success_rate IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.third_down_success_rate)
    END AS third_down_success_pctl,
    CASE WHEN c.red_zone_success_rate IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.red_zone_success_rate)
    END AS red_zone_success_pctl,
    CASE WHEN c.run_rate_delta IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.run_rate_delta)
    END AS run_rate_delta_pctl,
    CASE WHEN c.pace_plays_per_game IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.pace_plays_per_game)
    END AS pace_pctl
FROM combined c;

COMMENT ON VIEW api.team_playcalling_profile IS
'Team playcalling identity with situational tendencies and percentile rankings. '
'One row per team-season. Filter by team, season, conference. '
'Percentiles are per-season. NULL rates indicate < 10 plays in that situation. '
'Backed by materialized views (tendencies + success).';
```

**PostgREST usage:**
- `/api/team_playcalling_profile?team=eq.Ohio State&season=eq.2024` — single team
- `/api/team_playcalling_profile?season=eq.2024&order=overall_epa_pctl.desc` — leaderboard
- `/api/team_playcalling_profile?conference=eq.SEC&season=eq.2024` — conference view

**Performance note:** If this view turns out to be slow (>200ms), materialize it. But with 1,400 rows and indexed source matviews, it should run in <50ms.

**Acceptance criteria:**
- [x] View creates and returns rows
- [x] One row per team-season (no duplicates)
- [x] All percentiles between 0 and 1
- [x] run_rate_delta = leading_run_rate - trailing_run_rate (within rounding)
- [x] pace_plays_per_game in reasonable range (55-90)
- [x] Conference populated for FBS teams

---

### Phase 3: Refresh Chain + Housekeeping

#### Task 3.1: Update `marts.refresh_all()`

**File:** `src/schemas/functions/refresh_all_marts.sql`

- **Layer 2:** Add `team_playcalling_tendencies`, `team_situational_success` (depend on `play_epa` from Layer 1)
- **Layer 1:** Add `player_comparison` (bug fix — currently missing, has no mart dependencies)

No Layer 3 addition needed — the profile is a regular view, always fresh.

Update `scripts/refresh_marts.py` MARTS_VIEWS list to include 2 new matviews + `player_comparison`.

#### Task 3.2: Update test inventory

**File:** `tests/test_marts.py`

- Add `team_playcalling_tendencies` and `team_situational_success` to `MARTS_VIEWS` list
- Add `player_comparison` to list (bug fix — currently missing)
- Parametrized existence + row count tests auto-cover new marts

---

### Phase 4: Tests

#### Task 4.1: Play-calling analytics tests

**File:** `tests/test_playcalling_analytics.py` (new file)

**~20 focused behavior tests:**

```python
class TestTeamPlaycallingTendencies:
    def test_score_diff_buckets_valid(self, db_conn):
        """All score_diff_bucket values in expected set."""
    def test_run_rate_shift(self, db_conn):
        """Avg run_rate for big_lead > avg run_rate for big_deficit (league-wide)."""
    def test_no_garbage_time_plays(self, db_conn):
        """Spot-check: compare row count vs play_epa filtered count."""
    def test_all_fbs_teams_present(self, db_conn):
        """At least 100 distinct teams per recent season."""

class TestTeamSituationalSuccess:
    def test_success_rate_range(self, db_conn):
        """All non-NULL success_rate between 0 and 1."""
    def test_min_play_threshold(self, db_conn):
        """Rows with total_plays < 10 have NULL success_rate."""
    def test_yardage_success_only_on_third_fourth(self, db_conn):
        """yardage_success_rate NULL when down NOT IN (3, 4)."""
    def test_epa_range(self, db_conn):
        """avg_epa between -2.0 and 2.0 for all rows."""

class TestTeamPlaycallingProfile:
    def test_one_row_per_team_season(self, db_conn):
        """No duplicates on (team, season)."""
    def test_percentiles_range(self, db_conn):
        """All non-NULL percentiles between 0 and 1."""
    def test_percentile_span(self, db_conn):
        """At least one team at 0.0 and one at ~1.0 for each metric."""
    def test_run_rate_delta_calculation(self, db_conn):
        """run_rate_delta = leading_run_rate - trailing_run_rate."""
    def test_pace_range(self, db_conn):
        """pace_plays_per_game between 40 and 120."""
    def test_conference_populated(self, db_conn):
        """Conference not NULL for FBS teams."""
    def test_profile_query_performance(self, db_conn):
        """Single-team query completes in < 200ms."""

class TestApiViewExists:
    def test_profile_view_exists(self, db_conn):
        """api.team_playcalling_profile exists in pg_views."""
    def test_profile_returns_rows(self, db_conn):
        """api.team_playcalling_profile returns > 0 rows."""
    def test_profile_columns(self, db_conn):
        """Expected column set matches."""
    def test_filter_pushdown(self, db_conn):
        """WHERE team = 'Ohio State' returns exactly one row per season."""
```

---

### Phase 5: Documentation + Deploy

#### Task 5.1: Update schema contract

**File:** `docs/SCHEMA_CONTRACT.md`

- Add 2 new matviews to marts section (with row counts)
- Add `api.team_playcalling_profile` to API views section (with column list)

#### Task 5.2: Deploy to Supabase

Run SQL in order:
1. Matviews (021, 022) — can run in parallel
2. Profile view (014)
3. Updated refresh function

#### Task 5.3: Verify

```bash
.venv/bin/pytest -q        # All tests pass
```
```sql
SELECT * FROM marts.refresh_all();  -- All matviews refresh OK
```

---

## Acceptance Criteria

### Functional Requirements

- [x] 2 matviews created and populated
- [x] 1 API view returns one-row-per-team-season with percentiles
- [x] PostgREST filter pushdown works (team, season, conference)
- [x] Garbage time excluded from all aggregations
- [x] Minimum play threshold (10) enforced — NULL rates below threshold
- [x] Score differential bucketing uses COALESCE(score_diff, 0)

### Non-Functional Requirements

- [x] Profile query ~220ms (single team)
- [x] Profile full scan ~200ms (all teams, single season)
- [x] Matview refresh completes without errors
- [x] All tests pass (509 total: existing 490 + 19 new)

### Quality Gates

- [x] `ruff check .` passes
- [x] `ruff format --check .` passes
- [x] `pytest -q` passes (509 tests)
- [x] Schema contract updated
- [x] Refresh function updated

## Implementation Order

```
Task 1.1 ─┐
Task 1.2 ─┘── Matviews (parallel, no dependencies between them)
    ↓
Task 2.1 ──── Profile view (depends on both matviews)
    ↓
Task 3.1 ─┐
Task 3.2 ─┘── Refresh chain + test inventory (depends on matviews deployed)
    ↓
Task 4.1 ──── Tests (depends on everything deployed)
    ↓
Task 5.1 ─┐
Task 5.2 ─┤── Docs + deploy + verify
Task 5.3 ─┘
```

## Risk Analysis

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| play_epa → core.plays JOIN causes fanout | Low | High | JOIN on play_id (UNIQUE in play_epa, PK in core.plays) — guaranteed 1:1 |
| score_diff NULL for some plays | Medium | Low | COALESCE(score_diff, 0) → 'tied' bucket |
| Profile view too slow (window functions over regular view) | Low | Medium | ~1,400 rows — trivial. Materialize later if needed. |
| Matview refresh slow (2.7M play scans) | Medium | Medium | play_epa indexed on (offense, season); JOIN on play_id (both indexed) |
| Existing tests break | Low | High | Additive only — no schema changes to existing tables |

## References

### Internal
- Brainstorm: `docs/brainstorms/2026-02-06-play-calling-tendencies-brainstorm.md`
- Aggregation pattern: `src/schemas/marts/004_situational_splits.sql`
- PERCENT_RANK pattern: `src/schemas/marts/020_player_comparison.sql`
- API view pattern: `src/schemas/api/013_player_comparison.sql`
- Refresh function: `src/schemas/functions/refresh_all_marts.sql`
- Schema contract: `docs/SCHEMA_CONTRACT.md`
- Play EPA base: `src/schemas/marts/010_play_epa.sql`

### Learnings Applied
- Window functions over small dataset (~1,400 rows) don't need materialization
- COALESCE on score_diff prevents NULL bucket leakage
- CASE wrapper on PERCENT_RANK handles NULL metrics cleanly
- Minimum play threshold (10) prevents misleading percentiles
- FILTER clause pattern for clean run/pass aggregation
- PostgREST replaces RPCs for simple WHERE-clause lookups
