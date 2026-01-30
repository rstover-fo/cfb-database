# Data Quality & Analytics Readiness Sprint Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix critical data integrity issues, wire missing resources, and bring the database to production-ready analytics quality.

**Architecture:** Address PK mismatches first (silent data corruption), then backfill games to resolve orphan plays, convert reference sources to merge disposition, and finally wire the missing high-value resources.

**Tech Stack:** Python, dlt, Supabase Postgres, CFBD API

---

## Priority Matrix

| Issue | Severity | Impact | Sprint |
|-------|----------|--------|--------|
| 5 PK mismatches | CRITICAL | Silent data loss, merge failures | 1 |
| 2.3M orphan plays (63%) | CRITICAL | Play-level analytics broken | 1 |
| Reference `replace` fragility | HIGH | Indexes destroyed on refresh | 2 |
| Unwired: rosters | HIGH | Can't track player careers | 2 |
| Unwired: advanced_team_stats | MEDIUM | Missing box score metrics | 3 |
| Unwired: player_usage | MEDIUM | Can't weight player stats | 3 |
| Position standardization | MEDIUM | Can't analyze position groups | 3 |
| Unwired: game_media | LOW | TV data not critical | Backlog |

---

## Sprint 1: Data Integrity (Critical Fixes)

### Task 1.1: Audit Current PK Configurations

**Files:**
- Read: `src/pipelines/config/endpoints.py`
- Read: `src/pipelines/sources/*.py` (all source files)

**Step 1: Document the 5 PK mismatches**

Review `src/pipelines/config/endpoints.py` and cross-reference with actual source code.

| Table | Config PK | Actual Data PK | Fix |
|-------|-----------|----------------|-----|
| coaches | first_name, last_name, seasons | first_name, last_name | Remove `seasons` (JSONB array) |
| player_season_stats | player_id, season, stat_type | player_id, season, category | Use `category` |
| transfer_portal | player_id, season | season, first_name, last_name | Use actual fields |
| lines | id | game_id, provider | Use composite |
| draft_picks | college_athlete_id, year | year, overall | Use actual fields |

**Step 2: Verify each mismatch by inspecting source code**

Check each source file to see what fields are actually returned from the API.

---

### Task 1.2: Fix `coaches` PK (Remove JSONB from PK)

**Files:**
- Modify: `src/pipelines/config/endpoints.py`

**Step 1: Read the current config**

```python
# Current (broken):
EndpointConfig(
    name="coaches",
    path="/coaches",
    primary_key=["first_name", "last_name", "seasons"],  # seasons is JSONB!
    ...
)
```

**Step 2: Fix the primary key**

```python
# Fixed:
EndpointConfig(
    name="coaches",
    path="/coaches",
    primary_key=["first_name", "last_name"],  # Remove seasons
    ...
)
```

**Step 3: Run test to verify**

```bash
cd /Users/robstover/Development/personal/cfb-database
pytest tests/test_endpoints_config.py -v -k "coaches"
```

Expected: PASS

**Step 4: Commit**

```bash
git add src/pipelines/config/endpoints.py
git commit -m "fix(config): remove JSONB seasons from coaches PK"
```

---

### Task 1.3: Fix `player_season_stats` PK (stat_type → category)

**Files:**
- Modify: `src/pipelines/config/endpoints.py`

**Step 1: Inspect the API response**

Check what field name CFBD actually returns. Reference `docs/api-field-audit.md`.

**Step 2: Update config to match actual field name**

```python
# If API returns 'category':
EndpointConfig(
    name="player_season_stats",
    path="/stats/player/season",
    primary_key=["player_id", "season", "team", "category"],
    ...
)
```

**Step 3: Run test**

```bash
pytest tests/test_endpoints_config.py -v -k "player_season_stats"
```

**Step 4: Commit**

```bash
git commit -am "fix(config): correct player_season_stats PK field name"
```

---

### Task 1.4: Fix `transfer_portal` PK

**Files:**
- Modify: `src/pipelines/config/endpoints.py`

**Step 1: Check API response structure**

The transfer portal endpoint may not return a unique `player_id`. Check if the actual unique key is `(season, first_name, last_name)` or if there's a better identifier.

**Step 2: Update config**

```python
# If no player_id, use name-based composite:
EndpointConfig(
    name="transfer_portal",
    path="/player/portal",
    primary_key=["season", "first_name", "last_name"],
    ...
)
```

**Step 3: Run test and commit**

```bash
pytest tests/test_endpoints_config.py -v -k "transfer"
git commit -am "fix(config): correct transfer_portal PK to use available fields"
```

---

### Task 1.5: Fix `lines` PK (id → game_id, provider)

**Files:**
- Modify: `src/pipelines/config/endpoints.py`

**Step 1: Check if lines has unique `id` field**

If the API doesn't return a unique `id`, use composite:

```python
EndpointConfig(
    name="lines",
    path="/lines",
    primary_key=["game_id", "provider"],
    ...
)
```

**Step 2: Run test and commit**

```bash
pytest tests/test_endpoints_config.py -v -k "lines"
git commit -am "fix(config): use composite PK for betting lines"
```

---

### Task 1.6: Fix `draft_picks` PK

**Files:**
- Modify: `src/pipelines/config/endpoints.py`

**Step 1: Use year + overall pick as PK**

```python
EndpointConfig(
    name="draft_picks",
    path="/draft/picks",
    primary_key=["year", "overall"],
    ...
)
```

**Step 2: Run test and commit**

```bash
pytest tests/test_endpoints_config.py -v -k "draft"
git commit -am "fix(config): use year+overall as draft_picks PK"
```

---

### Task 1.7: Run Full Test Suite After PK Fixes

**Step 1: Run all config tests**

```bash
pytest tests/test_endpoints_config.py -v
```

Expected: All tests PASS

**Step 2: Run a dry-run load to verify no schema errors**

```bash
python -m src.pipelines.run --source reference --dry-run
```

Expected: No PK validation errors

---

### Task 1.8: Backfill Games 2000-2003 to Fix Orphan Plays

**Files:**
- Reference: `src/pipelines/config/years.py`
- Reference: `docs/db-snapshot-current.json`

**Step 1: Check current games coverage**

```sql
-- Run in Supabase SQL Editor
SELECT MIN(season), MAX(season), COUNT(*) FROM core.games;
```

Expected: Shows gap if games start at 2004+ but plays reference 2000-2003

**Step 2: Check orphan plays count**

```sql
SELECT
    COUNT(*) as total_plays,
    COUNT(*) FILTER (WHERE game_id IN (SELECT id FROM core.games)) as matched,
    COUNT(*) FILTER (WHERE game_id NOT IN (SELECT id FROM core.games)) as orphans
FROM core.plays;
```

**Step 3: Backfill games for missing years**

```bash
python -m src.pipelines.run --source games --mode backfill --years 2000 2001 2002 2003
```

**Step 4: Verify orphan count reduced**

Re-run the orphan query. Expected: Orphan percentage drops significantly.

**Step 5: Commit config changes if any**

```bash
git commit -am "data: backfill games 2000-2003 to resolve orphan plays"
```

---

### Task 1.9: Add Foreign Key from plays → games

**Files:**
- Create: `src/schemas/018_plays_fk.sql`

**Step 1: Write the FK migration**

```sql
-- Add FK constraint now that games are backfilled
-- Only run after Task 1.8 confirms orphans resolved

-- First, check for remaining orphans
DO $$
DECLARE
    orphan_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO orphan_count
    FROM core.plays p
    WHERE NOT EXISTS (SELECT 1 FROM core.games g WHERE g.id = p.game_id);

    IF orphan_count > 0 THEN
        RAISE EXCEPTION 'Cannot add FK: % orphan plays remain', orphan_count;
    END IF;
END $$;

-- Add the foreign key
ALTER TABLE core.plays
ADD CONSTRAINT fk_plays_game
FOREIGN KEY (game_id) REFERENCES core.games(id);

-- Index for FK performance
CREATE INDEX IF NOT EXISTS idx_plays_game_id ON core.plays(game_id);
```

**Step 2: Test in SQL Editor first**

Run the orphan check query manually before applying FK.

**Step 3: Apply migration if safe**

Run the full SQL in Supabase SQL Editor.

**Step 4: Commit**

```bash
git add src/schemas/018_plays_fk.sql
git commit -m "schema: add FK constraint plays → games"
```

---

### Task 1.10: Verify Data Integrity After Sprint 1

**Step 1: Run integrity checks**

```sql
-- Check PK uniqueness for fixed tables
SELECT 'coaches' as tbl, COUNT(*), COUNT(DISTINCT (first_name, last_name)) as unique_pks
FROM ref.coaches
UNION ALL
SELECT 'player_season_stats', COUNT(*), COUNT(DISTINCT (player_id, season, team, category))
FROM stats.player_season_stats
UNION ALL
SELECT 'transfer_portal', COUNT(*), COUNT(DISTINCT (season, first_name, last_name))
FROM recruiting.transfer_portal
UNION ALL
SELECT 'lines', COUNT(*), COUNT(DISTINCT (game_id, provider))
FROM betting.lines
UNION ALL
SELECT 'draft_picks', COUNT(*), COUNT(DISTINCT (year, overall))
FROM draft.draft_picks;
```

Expected: `count` = `unique_pks` for all tables (no duplicates)

**Step 2: Check plays → games integrity**

```sql
SELECT
    'plays_integrity' as check,
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE game_id IN (SELECT id FROM core.games)) as valid
FROM core.plays;
```

Expected: `total` ≈ `valid` (>95%)

---

## Sprint 2: Infrastructure Stability

### Task 2.1: Convert Reference Sources to Merge Disposition

**Files:**
- Modify: `src/pipelines/sources/reference.py`
- Modify: `src/pipelines/config/endpoints.py`

**Problem:** Reference sources use `replace` disposition, which drops and recreates tables on each load. This destroys indexes, FKs, and triggers.

**Step 1: Update reference endpoints to use merge**

```python
# In endpoints.py, change reference table configs:
EndpointConfig(
    name="teams",
    path="/teams",
    primary_key=["id"],
    write_disposition="merge",  # Was "replace"
    ...
)
```

**Step 2: Update all reference endpoints**

Change disposition for: conferences, teams, venues, coaches, play_types, stat_categories

**Step 3: Test incremental load**

```bash
python -m src.pipelines.run --source reference --dry-run
```

**Step 4: Commit**

```bash
git commit -am "refactor: convert reference sources to merge disposition"
```

---

### Task 2.2: Add updated_at Triggers to Transactional Tables

**Files:**
- Create: `src/schemas/019_transactional_triggers.sql`

**Step 1: Write the trigger SQL**

```sql
-- Add updated_at columns and triggers to core transactional tables
-- (Reference tables already have this from 001_reference.sql)

-- Function (reuse if exists)
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add to games table
ALTER TABLE core.games ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE core.games ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

CREATE TRIGGER update_games_updated_at
    BEFORE UPDATE ON core.games
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add to drives table
ALTER TABLE core.drives ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE core.drives ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

CREATE TRIGGER update_drives_updated_at
    BEFORE UPDATE ON core.drives
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- (Repeat for other core tables as needed)
```

**Step 2: Apply and commit**

```bash
git add src/schemas/019_transactional_triggers.sql
git commit -m "schema: add audit timestamps to transactional tables"
```

---

### Task 2.3: Wire Rosters Resource

**Files:**
- Modify: `src/pipelines/sources/players.py`
- Modify: `src/pipelines/config/endpoints.py`

**Step 1: Add rosters endpoint config**

```python
EndpointConfig(
    name="rosters",
    path="/roster",
    primary_key=["id"],  # or player_id + team + season
    write_disposition="merge",
    table_name="rosters",
    schema="core",
    params={"team": None, "year": None},
)
```

**Step 2: Add roster resource to players.py**

```python
@dlt.resource(
    name="rosters",
    write_disposition="merge",
    primary_key="id",
)
def rosters(
    api_client: CFBDClient,
    years: list[int],
) -> Iterator[dict]:
    """Load team rosters by year."""
    for year in years:
        for team in get_fbs_teams():  # Need team iteration
            data = api_client.get(f"/roster", params={"team": team, "year": year})
            yield from data
```

**Step 3: Create schema for rosters table**

```sql
-- src/schemas/020_rosters.sql
CREATE TABLE IF NOT EXISTS core.rosters (
    id BIGINT PRIMARY KEY,
    team TEXT NOT NULL,
    season INT NOT NULL,
    player_id BIGINT,
    first_name TEXT,
    last_name TEXT,
    position TEXT,
    height INT,
    weight INT,
    jersey INT,
    year_class TEXT,  -- FR, SO, JR, SR
    home_city TEXT,
    home_state TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX ON core.rosters (team, season);
CREATE INDEX ON core.rosters (player_id);
CREATE INDEX ON core.rosters (position);
```

**Step 4: Test and commit**

```bash
python -m src.pipelines.run --source players --mode backfill --years 2024 --dry-run
git commit -am "feat: wire rosters resource for player career tracking"
```

---

### Task 2.4: Create Position Standardization Reference Table

**Files:**
- Create: `src/schemas/021_position_mapping.sql`

**Step 1: Write position mapping table**

```sql
-- Standardize positions across recruiting, rosters, and stats
CREATE TABLE IF NOT EXISTS ref.position_mapping (
    raw_position TEXT PRIMARY KEY,
    standard_position TEXT NOT NULL,
    position_group TEXT NOT NULL,
    side_of_ball TEXT NOT NULL  -- offense, defense, special_teams
);

INSERT INTO ref.position_mapping (raw_position, standard_position, position_group, side_of_ball) VALUES
-- Offense
('QB', 'QB', 'Quarterback', 'offense'),
('RB', 'RB', 'Running Back', 'offense'),
('FB', 'FB', 'Running Back', 'offense'),
('WR', 'WR', 'Wide Receiver', 'offense'),
('TE', 'TE', 'Tight End', 'offense'),
('OT', 'OT', 'Offensive Line', 'offense'),
('OG', 'OG', 'Offensive Line', 'offense'),
('OL', 'OL', 'Offensive Line', 'offense'),
('C', 'C', 'Offensive Line', 'offense'),
-- Defense
('DE', 'DE', 'Defensive Line', 'defense'),
('DT', 'DT', 'Defensive Line', 'defense'),
('DL', 'DL', 'Defensive Line', 'defense'),
('NT', 'NT', 'Defensive Line', 'defense'),
('EDGE', 'EDGE', 'Edge Rusher', 'defense'),
('OLB', 'OLB', 'Linebacker', 'defense'),
('ILB', 'ILB', 'Linebacker', 'defense'),
('LB', 'LB', 'Linebacker', 'defense'),
('MLB', 'MLB', 'Linebacker', 'defense'),
('CB', 'CB', 'Defensive Back', 'defense'),
('S', 'S', 'Defensive Back', 'defense'),
('FS', 'FS', 'Defensive Back', 'defense'),
('SS', 'SS', 'Defensive Back', 'defense'),
('DB', 'DB', 'Defensive Back', 'defense'),
-- Special Teams
('K', 'K', 'Kicker', 'special_teams'),
('P', 'P', 'Punter', 'special_teams'),
('LS', 'LS', 'Long Snapper', 'special_teams'),
-- Athlete (multi-position)
('ATH', 'ATH', 'Athlete', 'offense')
ON CONFLICT (raw_position) DO NOTHING;

-- Index for joins
CREATE INDEX ON ref.position_mapping (position_group);
CREATE INDEX ON ref.position_mapping (side_of_ball);
```

**Step 2: Apply and commit**

```bash
git add src/schemas/021_position_mapping.sql
git commit -m "schema: add position standardization mapping table"
```

---

## Sprint 3: Analytics Enhancement

### Task 3.1: Wire advanced_team_stats Resource

**Files:**
- Modify: `src/pipelines/sources/stats.py`
- Modify: `src/pipelines/config/endpoints.py`

**Step 1: Add endpoint config**

```python
EndpointConfig(
    name="advanced_team_stats",
    path="/stats/game/advanced",
    primary_key=["game_id", "team"],
    write_disposition="merge",
    table_name="advanced_team_stats",
    schema="stats",
)
```

**Step 2: Add resource function in stats.py**

```python
@dlt.resource(
    name="advanced_team_stats",
    write_disposition="merge",
    primary_key=["game_id", "team"],
)
def advanced_team_stats(
    api_client: CFBDClient,
    years: list[int],
) -> Iterator[dict]:
    """Load advanced box score stats per game."""
    for year in years:
        data = api_client.get("/stats/game/advanced", params={"year": year})
        yield from data
```

**Step 3: Create schema**

```sql
-- src/schemas/022_advanced_team_stats.sql
CREATE TABLE IF NOT EXISTS stats.advanced_team_stats (
    game_id BIGINT NOT NULL,
    team TEXT NOT NULL,
    season INT,
    week INT,
    opponent TEXT,
    -- Offensive metrics
    off_plays INT,
    off_drives INT,
    off_ppa NUMERIC,
    off_total_ppa NUMERIC,
    off_success_rate NUMERIC,
    off_explosiveness NUMERIC,
    off_power_success NUMERIC,
    off_stuff_rate NUMERIC,
    off_line_yards NUMERIC,
    off_line_yards_total NUMERIC,
    off_second_level_yards NUMERIC,
    off_open_field_yards NUMERIC,
    -- Defensive metrics (same structure)
    def_plays INT,
    def_drives INT,
    def_ppa NUMERIC,
    def_total_ppa NUMERIC,
    def_success_rate NUMERIC,
    def_explosiveness NUMERIC,
    def_power_success NUMERIC,
    def_stuff_rate NUMERIC,
    def_line_yards NUMERIC,
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (game_id, team)
);

CREATE INDEX ON stats.advanced_team_stats (season, week);
CREATE INDEX ON stats.advanced_team_stats (team, season);
```

**Step 4: Test and commit**

```bash
python -m src.pipelines.run --source stats --mode backfill --years 2024 --dry-run
git commit -am "feat: wire advanced_team_stats resource"
```

---

### Task 3.2: Wire player_usage Resource

**Files:**
- Modify: `src/pipelines/sources/players.py`

**Step 1: Add resource**

```python
@dlt.resource(
    name="player_usage",
    write_disposition="merge",
    primary_key=["player_id", "season"],
)
def player_usage(
    api_client: CFBDClient,
    years: list[int],
) -> Iterator[dict]:
    """Load player usage/snap count data."""
    for year in years:
        data = api_client.get("/player/usage", params={"year": year})
        yield from data
```

**Step 2: Create schema**

```sql
-- src/schemas/023_player_usage.sql
CREATE TABLE IF NOT EXISTS stats.player_usage (
    player_id BIGINT NOT NULL,
    season INT NOT NULL,
    team TEXT,
    conference TEXT,
    position TEXT,
    name TEXT,
    -- Usage metrics
    overall_usage NUMERIC,
    pass_usage NUMERIC,
    rush_usage NUMERIC,
    first_down_usage NUMERIC,
    second_down_usage NUMERIC,
    third_down_usage NUMERIC,
    standard_downs_usage NUMERIC,
    passing_downs_usage NUMERIC,
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, season)
);

CREATE INDEX ON stats.player_usage (team, season);
CREATE INDEX ON stats.player_usage (position);
```

**Step 3: Test and commit**

```bash
git commit -am "feat: wire player_usage resource for snap count analysis"
```

---

### Task 3.3: Create Team Talent Composite View

**Files:**
- Create: `src/schemas/marts/017_team_talent_composite.sql`

**Step 1: Write the mart**

```sql
-- Combine recruiting stars + transfer portal to create roster talent score
-- Depends on: recruiting.recruits, recruiting.transfer_portal, core.rosters

DROP MATERIALIZED VIEW IF EXISTS marts.team_talent_composite CASCADE;

CREATE MATERIALIZED VIEW marts.team_talent_composite AS
WITH roster_talent AS (
    SELECT
        r.team,
        r.season,
        COUNT(*) as roster_size,
        -- Count by star rating (from original recruiting)
        COUNT(*) FILTER (WHERE rec.stars = 5) as five_stars,
        COUNT(*) FILTER (WHERE rec.stars = 4) as four_stars,
        COUNT(*) FILTER (WHERE rec.stars = 3) as three_stars,
        COUNT(*) FILTER (WHERE rec.stars <= 2) as low_stars,
        -- Average rating
        AVG(rec.rating) as avg_recruit_rating,
        -- Blue chip ratio (4* and 5*)
        ROUND(COUNT(*) FILTER (WHERE rec.stars >= 4)::numeric / NULLIF(COUNT(*), 0), 3) as blue_chip_ratio
    FROM core.rosters r
    LEFT JOIN recruiting.recruits rec ON
        r.first_name = rec.first_name
        AND r.last_name = rec.last_name
        AND rec.year BETWEEN r.season - 5 AND r.season  -- Within eligibility window
    GROUP BY r.team, r.season
),
transfer_impact AS (
    SELECT
        destination as team,
        season,
        COUNT(*) as transfers_in,
        AVG(stars) as avg_transfer_stars
    FROM recruiting.transfer_portal
    WHERE destination IS NOT NULL
    GROUP BY destination, season
)
SELECT
    rt.team,
    rt.season,
    rt.roster_size,
    rt.five_stars,
    rt.four_stars,
    rt.three_stars,
    rt.blue_chip_ratio,
    rt.avg_recruit_rating,
    COALESCE(ti.transfers_in, 0) as transfers_in,
    ti.avg_transfer_stars,
    -- Composite talent score (weighted)
    ROUND(
        (rt.five_stars * 5 + rt.four_stars * 4 + rt.three_stars * 3 + rt.low_stars * 2)::numeric
        / NULLIF(rt.roster_size, 0),
        2
    ) as talent_score
FROM roster_talent rt
LEFT JOIN transfer_impact ti ON rt.team = ti.team AND rt.season = ti.season;

CREATE UNIQUE INDEX ON marts.team_talent_composite (team, season);
CREATE INDEX ON marts.team_talent_composite (season);
CREATE INDEX ON marts.team_talent_composite (talent_score DESC);
```

**Step 2: Apply and commit**

```bash
git add src/schemas/marts/017_team_talent_composite.sql
git commit -m "feat: add team talent composite mart"
```

---

### Task 3.4: Create Analytics Data Quality Dashboard View

**Files:**
- Create: `src/schemas/marts/018_data_quality_dashboard.sql`

**Step 1: Write the monitoring view**

```sql
-- Data quality monitoring view
-- Shows coverage, freshness, and integrity metrics

CREATE OR REPLACE VIEW analytics.data_quality_dashboard AS
WITH table_stats AS (
    SELECT
        schemaname,
        relname as table_name,
        n_live_tup as row_count,
        last_autovacuum,
        last_analyze
    FROM pg_stat_user_tables
    WHERE schemaname IN ('core', 'stats', 'ratings', 'recruiting', 'betting', 'draft', 'metrics', 'ref')
),
coverage AS (
    SELECT 'games' as metric, MIN(season) as min_year, MAX(season) as max_year, COUNT(*) as count FROM core.games
    UNION ALL
    SELECT 'plays', MIN(season), MAX(season), COUNT(*) FROM core.plays
    UNION ALL
    SELECT 'drives', MIN(season), MAX(season), COUNT(*) FROM core.drives
    UNION ALL
    SELECT 'team_season_stats', MIN(season), MAX(season), COUNT(*) FROM stats.team_season_stats
    UNION ALL
    SELECT 'sp_ratings', MIN(year), MAX(year), COUNT(*) FROM ratings.sp_ratings
    UNION ALL
    SELECT 'recruits', MIN(year), MAX(year), COUNT(*) FROM recruiting.recruits
),
orphan_check AS (
    SELECT
        'plays_without_games' as metric,
        COUNT(*) FILTER (WHERE NOT EXISTS (
            SELECT 1 FROM core.games g WHERE g.id = p.game_id
        )) as orphan_count,
        COUNT(*) as total_count
    FROM core.plays p
)
SELECT
    'coverage' as check_type,
    metric,
    min_year::text as value_1,
    max_year::text as value_2,
    count::text as value_3
FROM coverage
UNION ALL
SELECT
    'orphans' as check_type,
    metric,
    orphan_count::text,
    total_count::text,
    ROUND(100.0 * orphan_count / NULLIF(total_count, 0), 1)::text || '%'
FROM orphan_check;
```

**Step 2: Commit**

```bash
git add src/schemas/marts/018_data_quality_dashboard.sql
git commit -m "feat: add data quality dashboard view"
```

---

## Sprint 4: Backlog / Future Enhancements

Items deferred for future sprints:

- [ ] Wire `game_media` resource (TV/streaming data)
- [ ] Wire `win_probability` resource (in-game win prob)
- [ ] Wire `ppa_players_games` resource (per-game player EPA)
- [ ] Backfill ratings pre-2015 (if data exists)
- [ ] Add data lineage/audit logging
- [ ] Create automated refresh scheduler for materialized views
- [ ] Add data quality tests to CI pipeline

---

## Validation Queries

Run these after each sprint to verify success:

```sql
-- Sprint 1: PK integrity
SELECT
    'coaches' as tbl,
    COUNT(*) as rows,
    COUNT(*) = COUNT(DISTINCT (first_name, last_name)) as pk_valid
FROM ref.coaches;

-- Sprint 1: Orphan plays resolved
SELECT
    ROUND(100.0 * COUNT(*) FILTER (WHERE game_id IN (SELECT id FROM core.games)) / COUNT(*), 1) as pct_matched
FROM core.plays;

-- Sprint 2: Reference tables have indexes
SELECT indexname, tablename
FROM pg_indexes
WHERE schemaname = 'ref';

-- Sprint 3: New resources loaded
SELECT COUNT(*) FROM core.rosters;
SELECT COUNT(*) FROM stats.advanced_team_stats;
SELECT COUNT(*) FROM stats.player_usage;
```

---

## Summary

| Sprint | Focus | Tasks | Outcome |
|--------|-------|-------|---------|
| **1** | Data Integrity | Fix 5 PK bugs, backfill games, add FK | No silent data loss, plays usable |
| **2** | Infrastructure | Merge disposition, rosters, positions | Stable refreshes, player tracking |
| **3** | Analytics | Advanced stats, usage, talent composite | Production analytics ready |
| **4** | Backlog | Media, win prob, pre-2015 ratings | Nice-to-have enhancements |

---

## References

- CFBD API docs: https://collegefootballdata.com/api/docs
- dlt merge disposition: https://dlthub.com/docs/general-usage/incremental-loading
- Existing pipeline manifest: `docs/pipeline-manifest.md`
- PK audit notes: `docs/api-field-audit.md`
