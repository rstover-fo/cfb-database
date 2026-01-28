# CFB Database Gap Fill Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Backfill missing pipeline data and add remaining analytics views to complete the database per the design spec.

**Architecture:** The pipelines already exist but weren't fully run. Phase 1 runs existing pipelines to backfill data. Phase 2 adds the 6 missing analytics materialized views. Phase 3 (optional) adds API/Features schemas.

**Tech Stack:** Python 3.12, dlt pipelines, PostgreSQL (Supabase), SQL materialized views

---

## Execution Progress (Updated 2026-01-28)

### Phase 1: Raw Data Backfill — ✅ COMPLETE

| Table | Row Count | Status |
|-------|-----------|--------|
| `stats.advanced_game_stats` | 25,752 | ✅ Loaded |
| `stats.player_usage` | 44,037 | ✅ Loaded |
| `stats.player_returning` | 1,555 | ✅ Loaded |
| `metrics.ppa_games` | 19,359 | ✅ Loaded |
| `metrics.ppa_players_games` | 19,782 | ✅ Loaded |
| `metrics.win_probability` | — | ⏭️ Skipped (requires game_id, not needed for analytics) |
| `recruiting.team_talent` | 2,275 | ✅ Loaded |
| `recruiting.recruiting_groups` | 55,588 | ✅ Loaded |

### Phase 2: Marts Views — ✅ COMPLETE

Views created in `marts` schema (not `analytics` as originally planned):

| View | Row Count | Build Time | Notes |
|------|-----------|------------|-------|
| `marts.team_season_summary` | 3,667 | Fast | Record, scoring, ratings, recruiting |
| `marts.matchup_history` | 7,479 | Fast | Head-to-head records |
| `marts.recruiting_class` | 4,227 | Fast | Recruiting class metrics |
| `marts.scoring_opportunities` | 1,549 | ~46s | Drive efficiency |
| `marts.coach_record` | 2,613 | ~43s | Coach tenure/performance |
| `marts._game_epa_calc` | 40,221 | 12.1 min | Helper view for EPA aggregations |
| `marts.team_epa_season` | 1,548 | 0.9 min | Season-level EPA by team |
| `marts.situational_splits` | 1,548 | 11.6 min | Down/distance, red zone, etc. |
| `marts.defensive_havoc` | 1,549 | 12.0 min | Sacks, TFLs, turnovers |

**Total: 9 of 9 views created successfully** 🎉

### Technical Notes

**EPA Views Required Direct Connection:**
The EPA-related views (`_game_epa_calc`, `team_epa_season`, `situational_splits`, `defensive_havoc`) process 2.7M plays and timeout on Supabase's connection pooler. Solution: use `options=-c statement_timeout=0` in connection string.

**Refresh Considerations:**
EPA views take ~12 minutes each to refresh. For production, consider:
1. Use `REFRESH MATERIALIZED VIEW CONCURRENTLY` (non-blocking)
2. Schedule during off-peak hours
3. Direct DB connection for refresh scripts

### Schema Fixes Applied

- Added `score_diff` generated column to `core.plays` (for garbage time filtering)
- Fixed column names in SQL files to match dlt naming convention (`clock__minutes`, `elapsed__minutes`)
- Fixed `coach_record` to use dlt's flattened `coaches__seasons` table structure
- Inlined `is_garbage_time()` function logic for performance (avoids per-row function call)

---

## Original Plan (Reference)

### Raw Data Gaps (pipelines exist, not backfilled)

| Table | Pipeline | Endpoint | Status |
|-------|----------|----------|--------|
| `stats.advanced_game_stats` | stats | `/game/box/advanced` | ✅ Done |
| `stats.player_usage` | stats | `/player/usage` | ✅ Done |
| `stats.player_returning` | stats | `/player/returning` | ✅ Done |
| `metrics.ppa_games` | metrics | `/ppa/games` | ✅ Done |
| `metrics.ppa_players_games` | metrics | `/ppa/players/games` | ✅ Done |
| `metrics.win_probability` | metrics | `/metrics/wp` | ⏭️ Skipped |
| `recruiting.team_talent` | recruiting | `/talent` | ✅ Done |
| `recruiting.recruiting_groups` | recruiting | `/recruiting/groups` | ✅ Done |

### Analytics Views Gaps

| View | Depends On | Status |
|------|-----------|--------|
| `analytics.team_epa_season` | metrics.ppa_teams, metrics.wepa_team_season | ✅ marts.team_epa_season |
| `analytics.situational_splits` | core.plays | ✅ marts.situational_splits |
| `analytics.defensive_havoc` | core.plays | ✅ marts.defensive_havoc |
| `analytics.scoring_opportunities` | core.drives | ✅ marts.scoring_opportunities |
| `analytics.matchup_history` | core.games | ✅ marts.matchup_history |
| `analytics.coach_record` | ref.coaches, analytics.team_season_summary | ✅ marts.coach_record |

---

## Phase 1: Backfill Missing Raw Data

### Task 1.1: Check API Budget

**Files:**
- None (CLI only)

**Step 1: Check current API usage**

```bash
cd /Users/robstover/Development/personal/cfb-database
.venv/bin/python -m src.pipelines.run --status
```

Expected: Shows remaining API calls (need ~5,000 for full backfill)

**Step 2: Estimate API calls needed**

| Pipeline | Endpoint | Years | Est. Calls |
|----------|----------|-------|------------|
| stats (advanced_game_stats) | /game/box/advanced | 2014-2026 | ~12 |
| stats (player_usage) | /player/usage | 2014-2026 | ~12 |
| stats (player_returning) | /player/returning | 2014-2026 | ~12 |
| metrics (ppa_games) | /ppa/games | 2014-2026 | ~12 |
| metrics (ppa_players_games) | /ppa/players/games | 2014-2026 | ~12 |
| metrics (win_probability) | /metrics/wp | 2014-2026 | ~12 |
| recruiting (team_talent) | /talent | 2000-2026 | ~26 |
| recruiting (recruiting_groups) | /recruiting/groups | 2000-2026 | ~26 |
| **Total** | | | **~124 calls** |

---

### Task 1.2: Backfill Stats Pipeline (Advanced Game Stats, Usage, Returning)

**Files:**
- Run: `src/pipelines/run.py`
- Source: `src/pipelines/sources/stats.py` (already has resources)

**Step 1: Run stats pipeline in backfill mode**

```bash
cd /Users/robstover/Development/personal/cfb-database
.venv/bin/python -m src.pipelines.run --source stats --mode backfill
```

Expected: Loads `advanced_game_stats`, `player_usage`, `player_returning` tables

**Step 2: Verify tables were created**

```bash
.venv/bin/python -c "
import psycopg2
import tomllib
from pathlib import Path

secrets = tomllib.load(open('.dlt/secrets.toml', 'rb'))
conn = psycopg2.connect(secrets['destination']['postgres']['credentials'])
cur = conn.cursor()

tables = ['stats.advanced_game_stats', 'stats.player_usage', 'stats.player_returning']
for t in tables:
    try:
        cur.execute(f'SELECT COUNT(*) FROM {t}')
        print(f'{t}: {cur.fetchone()[0]} rows')
    except Exception as e:
        print(f'{t}: ERROR - {e}')
        conn.rollback()
conn.close()
"
```

Expected: All three tables exist with row counts > 0

**Step 3: Commit checkpoint**

```bash
git add -A && git commit -m "chore: backfill stats pipeline (advanced_game_stats, player_usage, player_returning)"
```

---

### Task 1.3: Backfill Metrics Pipeline (PPA Games, Win Probability)

**Files:**
- Run: `src/pipelines/run.py`
- Source: `src/pipelines/sources/metrics.py` (already has resources)

**Step 1: Run metrics pipeline in backfill mode**

```bash
cd /Users/robstover/Development/personal/cfb-database
.venv/bin/python -m src.pipelines.run --source metrics --mode backfill
```

Expected: Loads `ppa_games`, `ppa_players_games`, `win_probability` tables

**Step 2: Verify tables were created**

```bash
.venv/bin/python -c "
import psycopg2
import tomllib

secrets = tomllib.load(open('.dlt/secrets.toml', 'rb'))
conn = psycopg2.connect(secrets['destination']['postgres']['credentials'])
cur = conn.cursor()

tables = ['metrics.ppa_games', 'metrics.ppa_players_games', 'metrics.win_probability']
for t in tables:
    try:
        cur.execute(f'SELECT COUNT(*) FROM {t}')
        print(f'{t}: {cur.fetchone()[0]} rows')
    except Exception as e:
        print(f'{t}: ERROR - {e}')
        conn.rollback()
conn.close()
"
```

Expected: All three tables exist with row counts > 0

**Step 3: Commit checkpoint**

```bash
git add -A && git commit -m "chore: backfill metrics pipeline (ppa_games, ppa_players_games, win_probability)"
```

---

### Task 1.4: Backfill Recruiting Pipeline (Team Talent, Recruiting Groups)

**Files:**
- Run: `src/pipelines/run.py`
- Source: `src/pipelines/sources/recruiting.py` (already has resources)

**Step 1: Run recruiting pipeline in backfill mode**

```bash
cd /Users/robstover/Development/personal/cfb-database
.venv/bin/python -m src.pipelines.run --source recruiting --mode backfill
```

Expected: Loads `team_talent`, `recruiting_groups` tables (recruits, team_recruiting, transfer_portal already exist)

**Step 2: Verify tables were created**

```bash
.venv/bin/python -c "
import psycopg2
import tomllib

secrets = tomllib.load(open('.dlt/secrets.toml', 'rb'))
conn = psycopg2.connect(secrets['destination']['postgres']['credentials'])
cur = conn.cursor()

tables = ['recruiting.team_talent', 'recruiting.recruiting_groups']
for t in tables:
    try:
        cur.execute(f'SELECT COUNT(*) FROM {t}')
        print(f'{t}: {cur.fetchone()[0]} rows')
    except Exception as e:
        print(f'{t}: ERROR - {e}')
        conn.rollback()
conn.close()
"
```

Expected: Both tables exist with row counts > 0

**Step 3: Commit checkpoint**

```bash
git add -A && git commit -m "chore: backfill recruiting pipeline (team_talent, recruiting_groups)"
```

---

### Task 1.5: Take Updated Database Snapshot

**Files:**
- Update: `docs/db-snapshot-current.json`

**Step 1: Run snapshot script**

```bash
cd /Users/robstover/Development/personal/cfb-database
.venv/bin/python -c "
import json
import psycopg2
import tomllib
from datetime import datetime, timezone

secrets = tomllib.load(open('.dlt/secrets.toml', 'rb'))
conn = psycopg2.connect(secrets['destination']['postgres']['credentials'])
cur = conn.cursor()

# Get table counts for key tables
tables = [
    'stats.advanced_game_stats', 'stats.player_usage', 'stats.player_returning',
    'metrics.ppa_games', 'metrics.ppa_players_games', 'metrics.win_probability',
    'recruiting.team_talent', 'recruiting.recruiting_groups'
]

print('=== Phase 1 Complete: New Tables ===')
for t in tables:
    try:
        cur.execute(f'SELECT COUNT(*) FROM {t}')
        print(f'{t}: {cur.fetchone()[0]:,} rows')
    except Exception as e:
        print(f'{t}: NOT FOUND')
        conn.rollback()

conn.close()
"
```

Expected: All 8 new tables populated

**Step 2: Commit snapshot**

```bash
git add docs/db-snapshot-current.json && git commit -m "docs: update db snapshot after Phase 1 backfill"
```

---

## Phase 2: Add Missing Analytics Views

### Task 2.1: Create team_epa_season Materialized View

**Files:**
- Modify: `src/schemas/013_analytics_views.sql`

**Step 1: Add the view definition to the SQL file**

Add after the `game_results` view (around line 366):

```sql
-- =============================================================================
-- 6. team_epa_season
--    One row per team per season — EPA metrics with benchmarking
-- =============================================================================

DROP MATERIALIZED VIEW IF EXISTS analytics.team_epa_season;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.team_epa_season AS
SELECT
    pt.season,
    pt.team,
    pt.conference,
    -- Offensive EPA
    pt.offense__overall AS offense_epa,
    pt.offense__passing AS offense_epa_passing,
    pt.offense__rushing AS offense_epa_rushing,
    -- Defensive EPA (lower is better)
    pt.defense__overall AS defense_epa,
    pt.defense__passing AS defense_epa_passing,
    pt.defense__rushing AS defense_epa_rushing,
    -- WEPA (opponent-adjusted)
    wt.wepa AS wepa_overall,
    wt.wepa_pass AS wepa_passing,
    wt.wepa_rush AS wepa_rushing,
    -- Benchmarking
    CASE
        WHEN pt.offense__overall >= 0.16 THEN 'Elite'
        WHEN pt.offense__overall >= 0.05 THEN 'Above Average'
        WHEN pt.offense__overall >= -0.05 THEN 'Average'
        WHEN pt.offense__overall >= -0.15 THEN 'Below Average'
        ELSE 'Struggling'
    END AS offense_tier,
    CASE
        WHEN pt.defense__overall <= -0.16 THEN 'Elite'
        WHEN pt.defense__overall <= -0.05 THEN 'Above Average'
        WHEN pt.defense__overall <= 0.05 THEN 'Average'
        WHEN pt.defense__overall <= 0.15 THEN 'Below Average'
        ELSE 'Struggling'
    END AS defense_tier
FROM metrics.ppa_teams pt
LEFT JOIN metrics.wepa_team_season wt
    ON wt.year = pt.season AND wt.team = pt.team;

-- Unique index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX IF NOT EXISTS ux_team_epa_season
    ON analytics.team_epa_season(season, team);

-- Query indexes
CREATE INDEX IF NOT EXISTS ix_team_epa_season_team
    ON analytics.team_epa_season(team);
CREATE INDEX IF NOT EXISTS ix_team_epa_season_offense
    ON analytics.team_epa_season(season, offense_epa DESC);
CREATE INDEX IF NOT EXISTS ix_team_epa_season_defense
    ON analytics.team_epa_season(season, defense_epa);
```

**Step 2: Apply the view to database**

```bash
cd /Users/robstover/Development/personal/cfb-database
.venv/bin/python -c "
import psycopg2
import tomllib

secrets = tomllib.load(open('.dlt/secrets.toml', 'rb'))
conn = psycopg2.connect(secrets['destination']['postgres']['credentials'])
cur = conn.cursor()

sql = '''
DROP MATERIALIZED VIEW IF EXISTS analytics.team_epa_season;

CREATE MATERIALIZED VIEW analytics.team_epa_season AS
SELECT
    pt.season,
    pt.team,
    pt.conference,
    pt.offense__overall AS offense_epa,
    pt.offense__passing AS offense_epa_passing,
    pt.offense__rushing AS offense_epa_rushing,
    pt.defense__overall AS defense_epa,
    pt.defense__passing AS defense_epa_passing,
    pt.defense__rushing AS defense_epa_rushing,
    wt.wepa AS wepa_overall,
    wt.wepa_pass AS wepa_passing,
    wt.wepa_rush AS wepa_rushing,
    CASE
        WHEN pt.offense__overall >= 0.16 THEN 'Elite'
        WHEN pt.offense__overall >= 0.05 THEN 'Above Average'
        WHEN pt.offense__overall >= -0.05 THEN 'Average'
        WHEN pt.offense__overall >= -0.15 THEN 'Below Average'
        ELSE 'Struggling'
    END AS offense_tier,
    CASE
        WHEN pt.defense__overall <= -0.16 THEN 'Elite'
        WHEN pt.defense__overall <= -0.05 THEN 'Above Average'
        WHEN pt.defense__overall <= 0.05 THEN 'Average'
        WHEN pt.defense__overall <= 0.15 THEN 'Below Average'
        ELSE 'Struggling'
    END AS defense_tier
FROM metrics.ppa_teams pt
LEFT JOIN metrics.wepa_team_season wt
    ON wt.year = pt.season AND wt.team = pt.team;

CREATE UNIQUE INDEX ux_team_epa_season ON analytics.team_epa_season(season, team);
CREATE INDEX ix_team_epa_season_team ON analytics.team_epa_season(team);
CREATE INDEX ix_team_epa_season_offense ON analytics.team_epa_season(season, offense_epa DESC);
CREATE INDEX ix_team_epa_season_defense ON analytics.team_epa_season(season, defense_epa);
'''

cur.execute(sql)
conn.commit()

cur.execute('SELECT COUNT(*) FROM analytics.team_epa_season')
print(f'analytics.team_epa_season: {cur.fetchone()[0]} rows')
conn.close()
"
```

Expected: View created with ~800 rows

**Step 3: Commit**

```bash
git add src/schemas/013_analytics_views.sql && git commit -m "feat: add team_epa_season materialized view"
```

---

### Task 2.2: Create matchup_history Materialized View

**Files:**
- Modify: `src/schemas/013_analytics_views.sql`

**Step 1: Add the view definition**

```sql
-- =============================================================================
-- 7. matchup_history
--    Head-to-head records between teams
-- =============================================================================

DROP MATERIALIZED VIEW IF EXISTS analytics.matchup_history;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.matchup_history AS
WITH games_flat AS (
    SELECT
        LEAST(home_team, away_team) AS team1,
        GREATEST(home_team, away_team) AS team2,
        season,
        start_date,
        home_team,
        away_team,
        home_points,
        away_points,
        CASE
            WHEN home_points > away_points THEN home_team
            WHEN away_points > home_points THEN away_team
            ELSE NULL
        END AS winner
    FROM core.games
    WHERE completed = true
        AND home_points IS NOT NULL
        AND away_points IS NOT NULL
)
SELECT
    team1,
    team2,
    COUNT(*) AS total_games,
    SUM(CASE WHEN winner = team1 THEN 1 ELSE 0 END) AS team1_wins,
    SUM(CASE WHEN winner = team2 THEN 1 ELSE 0 END) AS team2_wins,
    SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS ties,
    MIN(season) AS first_meeting,
    MAX(season) AS last_meeting,
    ROUND(AVG(ABS(home_points - away_points))::numeric, 1) AS avg_margin
FROM games_flat
GROUP BY team1, team2;

-- Unique index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX IF NOT EXISTS ux_matchup_history
    ON analytics.matchup_history(team1, team2);

-- Query indexes
CREATE INDEX IF NOT EXISTS ix_matchup_history_team1
    ON analytics.matchup_history(team1);
CREATE INDEX IF NOT EXISTS ix_matchup_history_team2
    ON analytics.matchup_history(team2);
CREATE INDEX IF NOT EXISTS ix_matchup_history_games
    ON analytics.matchup_history(total_games DESC);
```

**Step 2: Apply the view**

```bash
cd /Users/robstover/Development/personal/cfb-database
.venv/bin/python -c "
import psycopg2
import tomllib

secrets = tomllib.load(open('.dlt/secrets.toml', 'rb'))
conn = psycopg2.connect(secrets['destination']['postgres']['credentials'])
cur = conn.cursor()

sql = '''
DROP MATERIALIZED VIEW IF EXISTS analytics.matchup_history;

CREATE MATERIALIZED VIEW analytics.matchup_history AS
WITH games_flat AS (
    SELECT
        LEAST(home_team, away_team) AS team1,
        GREATEST(home_team, away_team) AS team2,
        season,
        start_date,
        home_team,
        away_team,
        home_points,
        away_points,
        CASE
            WHEN home_points > away_points THEN home_team
            WHEN away_points > home_points THEN away_team
            ELSE NULL
        END AS winner
    FROM core.games
    WHERE completed = true
        AND home_points IS NOT NULL
        AND away_points IS NOT NULL
)
SELECT
    team1,
    team2,
    COUNT(*) AS total_games,
    SUM(CASE WHEN winner = team1 THEN 1 ELSE 0 END) AS team1_wins,
    SUM(CASE WHEN winner = team2 THEN 1 ELSE 0 END) AS team2_wins,
    SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS ties,
    MIN(season) AS first_meeting,
    MAX(season) AS last_meeting,
    ROUND(AVG(ABS(home_points - away_points))::numeric, 1) AS avg_margin
FROM games_flat
GROUP BY team1, team2;

CREATE UNIQUE INDEX ux_matchup_history ON analytics.matchup_history(team1, team2);
CREATE INDEX ix_matchup_history_team1 ON analytics.matchup_history(team1);
CREATE INDEX ix_matchup_history_team2 ON analytics.matchup_history(team2);
CREATE INDEX ix_matchup_history_games ON analytics.matchup_history(total_games DESC);
'''

cur.execute(sql)
conn.commit()

cur.execute('SELECT COUNT(*) FROM analytics.matchup_history')
print(f'analytics.matchup_history: {cur.fetchone()[0]} rows')
conn.close()
"
```

Expected: View created with thousands of team pair records

**Step 3: Commit**

```bash
git add src/schemas/013_analytics_views.sql && git commit -m "feat: add matchup_history materialized view"
```

---

### Task 2.3: Create scoring_opportunities Materialized View

**Files:**
- Modify: `src/schemas/013_analytics_views.sql`

**Step 1: Add the view definition**

```sql
-- =============================================================================
-- 8. scoring_opportunities
--    Drive efficiency metrics per team per season
-- =============================================================================

DROP MATERIALIZED VIEW IF EXISTS analytics.scoring_opportunities;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.scoring_opportunities AS
SELECT
    d.season,
    d.offense AS team,
    d.offense_conference AS conference,
    COUNT(*) AS total_drives,
    SUM(CASE WHEN d.scoring THEN 1 ELSE 0 END) AS scoring_drives,
    ROUND(
        SUM(CASE WHEN d.scoring THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0),
        4
    ) AS scoring_rate,
    SUM(CASE WHEN d.drive_result = 'TD' THEN 1 ELSE 0 END) AS td_drives,
    ROUND(
        SUM(CASE WHEN d.drive_result = 'TD' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0),
        4
    ) AS td_rate,
    SUM(CASE WHEN d.drive_result = 'FG' THEN 1 ELSE 0 END) AS fg_drives,
    SUM(CASE WHEN d.drive_result IN ('PUNT', 'DOWNS', 'TURNOVER', 'INT', 'FUMBLE', 'INT TD', 'FUMBLE TD') THEN 1 ELSE 0 END) AS failed_drives,
    ROUND(AVG(d.plays)::numeric, 2) AS avg_plays_per_drive,
    ROUND(AVG(d.yards)::numeric, 2) AS avg_yards_per_drive,
    SUM(CASE WHEN d.drive_result IN ('INT', 'FUMBLE', 'INT TD', 'FUMBLE TD') THEN 1 ELSE 0 END) AS turnovers
FROM core.drives d
WHERE d.offense IS NOT NULL
GROUP BY d.season, d.offense, d.offense_conference;

-- Unique index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX IF NOT EXISTS ux_scoring_opportunities
    ON analytics.scoring_opportunities(season, team);

-- Query indexes
CREATE INDEX IF NOT EXISTS ix_scoring_opportunities_team
    ON analytics.scoring_opportunities(team);
CREATE INDEX IF NOT EXISTS ix_scoring_opportunities_td_rate
    ON analytics.scoring_opportunities(season, td_rate DESC);
CREATE INDEX IF NOT EXISTS ix_scoring_opportunities_scoring_rate
    ON analytics.scoring_opportunities(season, scoring_rate DESC);
```

**Step 2: Apply the view**

```bash
cd /Users/robstover/Development/personal/cfb-database
.venv/bin/python -c "
import psycopg2
import tomllib

secrets = tomllib.load(open('.dlt/secrets.toml', 'rb'))
conn = psycopg2.connect(secrets['destination']['postgres']['credentials'])
cur = conn.cursor()

sql = '''
DROP MATERIALIZED VIEW IF EXISTS analytics.scoring_opportunities;

CREATE MATERIALIZED VIEW analytics.scoring_opportunities AS
SELECT
    d.season,
    d.offense AS team,
    d.offense_conference AS conference,
    COUNT(*) AS total_drives,
    SUM(CASE WHEN d.scoring THEN 1 ELSE 0 END) AS scoring_drives,
    ROUND(SUM(CASE WHEN d.scoring THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0), 4) AS scoring_rate,
    SUM(CASE WHEN d.drive_result = 'TD' THEN 1 ELSE 0 END) AS td_drives,
    ROUND(SUM(CASE WHEN d.drive_result = 'TD' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0), 4) AS td_rate,
    SUM(CASE WHEN d.drive_result = 'FG' THEN 1 ELSE 0 END) AS fg_drives,
    SUM(CASE WHEN d.drive_result IN ('PUNT', 'DOWNS', 'TURNOVER', 'INT', 'FUMBLE', 'INT TD', 'FUMBLE TD') THEN 1 ELSE 0 END) AS failed_drives,
    ROUND(AVG(d.plays)::numeric, 2) AS avg_plays_per_drive,
    ROUND(AVG(d.yards)::numeric, 2) AS avg_yards_per_drive,
    SUM(CASE WHEN d.drive_result IN ('INT', 'FUMBLE', 'INT TD', 'FUMBLE TD') THEN 1 ELSE 0 END) AS turnovers
FROM core.drives d
WHERE d.offense IS NOT NULL
GROUP BY d.season, d.offense, d.offense_conference;

CREATE UNIQUE INDEX ux_scoring_opportunities ON analytics.scoring_opportunities(season, team);
CREATE INDEX ix_scoring_opportunities_team ON analytics.scoring_opportunities(team);
CREATE INDEX ix_scoring_opportunities_td_rate ON analytics.scoring_opportunities(season, td_rate DESC);
CREATE INDEX ix_scoring_opportunities_scoring_rate ON analytics.scoring_opportunities(season, scoring_rate DESC);
'''

cur.execute(sql)
conn.commit()

cur.execute('SELECT COUNT(*) FROM analytics.scoring_opportunities')
print(f'analytics.scoring_opportunities: {cur.fetchone()[0]} rows')
conn.close()
"
```

Expected: View created with ~3,000+ rows

**Step 3: Commit**

```bash
git add src/schemas/013_analytics_views.sql && git commit -m "feat: add scoring_opportunities materialized view"
```

---

### Task 2.4: Create coach_record Materialized View

**Files:**
- Modify: `src/schemas/013_analytics_views.sql`

**Step 1: Add the view definition**

```sql
-- =============================================================================
-- 9. coach_record
--    Coach career records with trajectory metrics
-- =============================================================================

DROP MATERIALIZED VIEW IF EXISTS analytics.coach_record;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.coach_record AS
SELECT
    c.first_name,
    c.last_name,
    cs.school AS team,
    MIN(cs.year) AS start_year,
    MAX(cs.year) AS end_year,
    COUNT(DISTINCT cs.year) AS seasons,
    SUM(cs.wins) AS total_wins,
    SUM(cs.losses) AS total_losses,
    SUM(cs.ties) AS total_ties,
    ROUND(
        SUM(cs.wins)::numeric / NULLIF(SUM(cs.wins) + SUM(cs.losses), 0),
        4
    ) AS win_pct,
    ROUND(AVG(cs.sp_overall)::numeric, 2) AS avg_sp_rating,
    MAX(cs.preseason_rank) FILTER (WHERE cs.preseason_rank <= 25) AS best_preseason_rank,
    MAX(cs.postseason_rank) FILTER (WHERE cs.postseason_rank <= 25) AS best_postseason_rank
FROM ref.coaches c
JOIN ref.coaches__seasons cs ON cs._dlt_parent_id = c._dlt_id
GROUP BY c.first_name, c.last_name, cs.school;

-- Unique index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX IF NOT EXISTS ux_coach_record
    ON analytics.coach_record(first_name, last_name, team);

-- Query indexes
CREATE INDEX IF NOT EXISTS ix_coach_record_team
    ON analytics.coach_record(team);
CREATE INDEX IF NOT EXISTS ix_coach_record_wins
    ON analytics.coach_record(total_wins DESC);
CREATE INDEX IF NOT EXISTS ix_coach_record_win_pct
    ON analytics.coach_record(win_pct DESC) WHERE seasons >= 3;
```

**Step 2: Apply the view**

```bash
cd /Users/robstover/Development/personal/cfb-database
.venv/bin/python -c "
import psycopg2
import tomllib

secrets = tomllib.load(open('.dlt/secrets.toml', 'rb'))
conn = psycopg2.connect(secrets['destination']['postgres']['credentials'])
cur = conn.cursor()

sql = '''
DROP MATERIALIZED VIEW IF EXISTS analytics.coach_record;

CREATE MATERIALIZED VIEW analytics.coach_record AS
SELECT
    c.first_name,
    c.last_name,
    cs.school AS team,
    MIN(cs.year) AS start_year,
    MAX(cs.year) AS end_year,
    COUNT(DISTINCT cs.year) AS seasons,
    SUM(cs.wins) AS total_wins,
    SUM(cs.losses) AS total_losses,
    SUM(cs.ties) AS total_ties,
    ROUND(SUM(cs.wins)::numeric / NULLIF(SUM(cs.wins) + SUM(cs.losses), 0), 4) AS win_pct,
    ROUND(AVG(cs.sp_overall)::numeric, 2) AS avg_sp_rating,
    MAX(cs.preseason_rank) FILTER (WHERE cs.preseason_rank <= 25) AS best_preseason_rank,
    MAX(cs.postseason_rank) FILTER (WHERE cs.postseason_rank <= 25) AS best_postseason_rank
FROM ref.coaches c
JOIN ref.coaches__seasons cs ON cs._dlt_parent_id = c._dlt_id
GROUP BY c.first_name, c.last_name, cs.school;

CREATE UNIQUE INDEX ux_coach_record ON analytics.coach_record(first_name, last_name, team);
CREATE INDEX ix_coach_record_team ON analytics.coach_record(team);
CREATE INDEX ix_coach_record_wins ON analytics.coach_record(total_wins DESC);
CREATE INDEX ix_coach_record_win_pct ON analytics.coach_record(win_pct DESC) WHERE seasons >= 3;
'''

cur.execute(sql)
conn.commit()

cur.execute('SELECT COUNT(*) FROM analytics.coach_record')
print(f'analytics.coach_record: {cur.fetchone()[0]} rows')
conn.close()
"
```

Expected: View created with ~2,000+ coach-team records

**Step 3: Commit**

```bash
git add src/schemas/013_analytics_views.sql && git commit -m "feat: add coach_record materialized view"
```

---

### Task 2.5: Update Refresh Function

**Files:**
- Modify: `src/schemas/013_analytics_views.sql`

**Step 1: Update the refresh function to include new views**

Update the `analytics.refresh_all_views()` function at the bottom of the file:

```sql
CREATE OR REPLACE FUNCTION analytics.refresh_all_views()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Refreshing analytics.team_season_summary...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.team_season_summary;

    RAISE NOTICE 'Refreshing analytics.player_career_stats...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.player_career_stats;

    RAISE NOTICE 'Refreshing analytics.conference_standings...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.conference_standings;

    RAISE NOTICE 'Refreshing analytics.team_recruiting_trend...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.team_recruiting_trend;

    RAISE NOTICE 'Refreshing analytics.game_results...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.game_results;

    -- New views added in gap fill
    RAISE NOTICE 'Refreshing analytics.team_epa_season...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.team_epa_season;

    RAISE NOTICE 'Refreshing analytics.matchup_history...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.matchup_history;

    RAISE NOTICE 'Refreshing analytics.scoring_opportunities...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.scoring_opportunities;

    RAISE NOTICE 'Refreshing analytics.coach_record...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.coach_record;

    RAISE NOTICE 'All analytics views refreshed.';
END;
$$;
```

**Step 2: Apply the updated function**

```bash
cd /Users/robstover/Development/personal/cfb-database
.venv/bin/python -c "
import psycopg2
import tomllib

secrets = tomllib.load(open('.dlt/secrets.toml', 'rb'))
conn = psycopg2.connect(secrets['destination']['postgres']['credentials'])
cur = conn.cursor()

sql = '''
CREATE OR REPLACE FUNCTION analytics.refresh_all_views()
RETURNS void
LANGUAGE plpgsql
AS \$\$
BEGIN
    RAISE NOTICE 'Refreshing analytics.team_season_summary...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.team_season_summary;

    RAISE NOTICE 'Refreshing analytics.player_career_stats...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.player_career_stats;

    RAISE NOTICE 'Refreshing analytics.conference_standings...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.conference_standings;

    RAISE NOTICE 'Refreshing analytics.team_recruiting_trend...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.team_recruiting_trend;

    RAISE NOTICE 'Refreshing analytics.game_results...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.game_results;

    RAISE NOTICE 'Refreshing analytics.team_epa_season...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.team_epa_season;

    RAISE NOTICE 'Refreshing analytics.matchup_history...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.matchup_history;

    RAISE NOTICE 'Refreshing analytics.scoring_opportunities...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.scoring_opportunities;

    RAISE NOTICE 'Refreshing analytics.coach_record...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.coach_record;

    RAISE NOTICE 'All analytics views refreshed.';
END;
\$\$;
'''

cur.execute(sql)
conn.commit()
print('Refresh function updated')
conn.close()
"
```

**Step 3: Commit**

```bash
git add src/schemas/013_analytics_views.sql && git commit -m "feat: update refresh function with new analytics views"
```

---

### Task 2.6: Final Verification

**Files:**
- None (verification only)

**Step 1: Verify all analytics views exist**

```bash
cd /Users/robstover/Development/personal/cfb-database
.venv/bin/python -c "
import psycopg2
import tomllib

secrets = tomllib.load(open('.dlt/secrets.toml', 'rb'))
conn = psycopg2.connect(secrets['destination']['postgres']['credentials'])
cur = conn.cursor()

views = [
    'analytics.team_season_summary',
    'analytics.player_career_stats',
    'analytics.conference_standings',
    'analytics.team_recruiting_trend',
    'analytics.game_results',
    'analytics.team_epa_season',
    'analytics.matchup_history',
    'analytics.scoring_opportunities',
    'analytics.coach_record',
]

print('=== Analytics Views Status ===')
for v in views:
    try:
        cur.execute(f'SELECT COUNT(*) FROM {v}')
        print(f'{v}: {cur.fetchone()[0]:,} rows')
    except Exception as e:
        print(f'{v}: ERROR - {e}')
        conn.rollback()

conn.close()
"
```

Expected: All 9 views exist with data

**Step 2: Test the refresh function**

```bash
.venv/bin/python -c "
import psycopg2
import tomllib

secrets = tomllib.load(open('.dlt/secrets.toml', 'rb'))
conn = psycopg2.connect(secrets['destination']['postgres']['credentials'])
cur = conn.cursor()

cur.execute('SELECT analytics.refresh_all_views()')
conn.commit()
print('Refresh completed successfully')
conn.close()
"
```

Expected: All views refresh without error

**Step 3: Final commit**

```bash
git add -A && git commit -m "chore: complete Phase 2 - all analytics views created and verified"
```

---

## Phase 3 (Future): API and Features Layers

These are deferred for future implementation:

### Deferred: API Schema Views
- `api.team_detail` - Single team page view
- `api.team_history` - Multi-season team trends
- `api.game_detail` - Single game page view
- `api.player_detail` - Single player page view
- `api.matchup` - Head-to-head comparison
- `api.leaderboard_teams` - Flexible leaderboards

### Deferred: Features Schema (ML)
- `features.team_game_features` - Pre-game feature vectors
- `features.play_features` - Play-level features

### Deferred: Predictions Schema
- `predictions.game_predictions` - Model outputs
- `predictions.model_registry` - Model metadata

---

## Summary

| Phase | Tasks | Outcome |
|-------|-------|---------|
| 1 | 1.1-1.5 | 8 new raw tables backfilled |
| 2 | 2.1-2.6 | 4 new analytics views + updated refresh function |
| 3 | Deferred | API/Features/Predictions layers |

**Total estimated time:** ~30 minutes for Phase 1 (API calls), ~20 minutes for Phase 2 (SQL)

**API calls used:** ~124 calls (well within 75K monthly budget)
