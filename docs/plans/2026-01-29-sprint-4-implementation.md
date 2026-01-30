# Sprint 4: Quality & Analytics Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix 5 PK bugs, add analytics-driven indexes, then build comprehensive EPA-based analytics layer with era-aware historical analysis and matchup intelligence.

**Architecture:** Two phases - 4A hardens the schema foundation (PK fixes, indexes), 4B builds the analytics layer as materialized views using existing mart patterns. All marts follow DROP CASCADE + CTE + unique index convention.

**Tech Stack:** PostgreSQL (Supabase), dlt pipelines, pytest, SQL materialized views

---

## Phase 4A: Quality & Stability

### Task 1: Verify PK Bug Test Coverage

**Files:**
- Read: `tests/test_endpoints_config.py`

**Step 1: Run existing PK tests to establish baseline**

Run: `cd /Users/robstover/Development/personal/cfb-database/.worktrees/sprint-4 && .venv/bin/pytest tests/test_endpoints_config.py -v -k "primary_key"`
Expected: PASS (tests document current state, even if PKs are wrong)

**Step 2: Commit baseline verification**

```bash
# No code changes - just documenting we verified baseline
git add -A && git commit -m "chore: verify PK test baseline before fixes

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

### Task 2: Fix coaches Primary Key

**Files:**
- Modify: `src/pipelines/config/endpoints.py` (lines 41-46)
- Test: `tests/test_endpoints_config.py`

**Step 1: Write failing test for correct coaches PK**

Add to `tests/test_endpoints_config.py`:

```python
def test_coaches_primary_key_includes_school_and_season():
    """Coaches PK must include school and season to prevent duplicates."""
    config = ENDPOINTS["coaches"]
    pk = config["primary_key"]
    assert "school" in pk, "coaches PK must include school"
    assert "season" in pk, "coaches PK must include season"
    assert pk == ["first_name", "last_name", "school", "season"]
```

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_endpoints_config.py::test_coaches_primary_key_includes_school_and_season -v`
Expected: FAIL with AssertionError

**Step 3: Fix the coaches endpoint config**

In `src/pipelines/config/endpoints.py`, update coaches config:

```python
"coaches": {
    "path": "/coaches",
    "table": "coaches",
    "primary_key": ["first_name", "last_name", "school", "season"],
    "write_disposition": "merge",
    "params": {"year": "{year}"},
},
```

**Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_endpoints_config.py::test_coaches_primary_key_includes_school_and_season -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/pipelines/config/endpoints.py tests/test_endpoints_config.py
git commit -m "$(cat <<'EOF'
fix(coaches): correct primary key to include school and season

Previous PK (first_name, last_name) caused duplicates for coaches
at multiple schools or across seasons.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Fix player_season_stats Primary Key

**Files:**
- Modify: `src/pipelines/config/endpoints.py`
- Test: `tests/test_endpoints_config.py`

**Step 1: Write failing test**

```python
def test_player_season_stats_primary_key_matches_api_fields():
    """player_season_stats PK must use actual API field names."""
    config = ENDPOINTS["player_season_stats"]
    pk = config["primary_key"]
    # API returns playerId, not player_id
    assert pk == ["playerId", "season", "team", "category", "statType"]
```

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_endpoints_config.py::test_player_season_stats_primary_key_matches_api_fields -v`
Expected: FAIL

**Step 3: Fix the config**

```python
"player_season_stats": {
    "path": "/stats/player/season",
    "table": "player_season_stats",
    "primary_key": ["playerId", "season", "team", "category", "statType"],
    "write_disposition": "merge",
    "params": {"year": "{year}"},
},
```

**Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_endpoints_config.py::test_player_season_stats_primary_key_matches_api_fields -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/pipelines/config/endpoints.py tests/test_endpoints_config.py
git commit -m "$(cat <<'EOF'
fix(player_season_stats): use actual API field names in primary key

API returns camelCase (playerId, statType), not snake_case.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Fix transfer_portal Primary Key

**Files:**
- Modify: `src/pipelines/config/endpoints.py`
- Test: `tests/test_endpoints_config.py`

**Step 1: Write failing test**

```python
def test_transfer_portal_primary_key_uses_unique_fields():
    """transfer_portal PK must uniquely identify transfers."""
    config = ENDPOINTS["transfer_portal"]
    pk = config["primary_key"]
    # API has no player_id; must use name + origin + season
    assert pk == ["firstName", "lastName", "origin", "season"]
```

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_endpoints_config.py::test_transfer_portal_primary_key_uses_unique_fields -v`
Expected: FAIL

**Step 3: Fix the config**

```python
"transfer_portal": {
    "path": "/player/portal",
    "table": "transfer_portal",
    "primary_key": ["firstName", "lastName", "origin", "season"],
    "write_disposition": "merge",
    "params": {"year": "{year}"},
},
```

**Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_endpoints_config.py::test_transfer_portal_primary_key_uses_unique_fields -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/pipelines/config/endpoints.py tests/test_endpoints_config.py
git commit -m "$(cat <<'EOF'
fix(transfer_portal): use origin school in primary key

Transfer portal API has no player_id; use name + origin + season
to uniquely identify transfer entries.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Fix lines Primary Key

**Files:**
- Modify: `src/pipelines/config/endpoints.py`
- Test: `tests/test_endpoints_config.py`

**Step 1: Write failing test**

```python
def test_lines_primary_key_is_game_and_provider():
    """lines PK should be (game_id, provider), not id."""
    config = ENDPOINTS["lines"]
    pk = config["primary_key"]
    assert pk == ["gameId", "provider"]
```

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_endpoints_config.py::test_lines_primary_key_is_game_and_provider -v`
Expected: FAIL

**Step 3: Fix the config**

```python
"lines": {
    "path": "/lines",
    "table": "lines",
    "primary_key": ["gameId", "provider"],
    "write_disposition": "merge",
    "params": {"year": "{year}"},
},
```

**Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_endpoints_config.py::test_lines_primary_key_is_game_and_provider -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/pipelines/config/endpoints.py tests/test_endpoints_config.py
git commit -m "$(cat <<'EOF'
fix(lines): use gameId and provider as primary key

Each game has multiple betting lines from different providers.
Using (gameId, provider) as PK correctly identifies unique lines.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Fix draft_picks Primary Key

**Files:**
- Modify: `src/pipelines/config/endpoints.py`
- Test: `tests/test_endpoints_config.py`

**Step 1: Write failing test**

```python
def test_draft_picks_primary_key_is_year_and_overall():
    """draft_picks PK should be (year, overall), not college_athlete_id."""
    config = ENDPOINTS["picks"]
    pk = config["primary_key"]
    assert pk == ["year", "overall"]
```

**Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_endpoints_config.py::test_draft_picks_primary_key_is_year_and_overall -v`
Expected: FAIL or PASS (may already be correct)

**Step 3: Verify/fix the config**

```python
"picks": {
    "path": "/draft/picks",
    "table": "draft_picks",
    "primary_key": ["year", "overall"],
    "write_disposition": "merge",
    "params": {"year": "{year}"},
},
```

**Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_endpoints_config.py::test_draft_picks_primary_key_is_year_and_overall -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/pipelines/config/endpoints.py tests/test_endpoints_config.py
git commit -m "$(cat <<'EOF'
fix(draft_picks): verify year and overall as primary key

Each draft pick is uniquely identified by year and overall position.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Reload Tables with Fixed PKs

**Files:**
- None (CLI operations only)

**Step 1: Truncate and reload coaches**

Run: `.venv/bin/python -m src.pipelines.run --source reference --endpoint coaches --replace --dry-run`
Expected: Shows plan to reload coaches

Run: `.venv/bin/python -m src.pipelines.run --source reference --endpoint coaches --replace`
Expected: Coaches table reloaded with correct PK

**Step 2: Verify coaches row count**

Run: `.venv/bin/python -c "
import dlt
pipeline = dlt.pipeline(pipeline_name='cfbd_reference', destination='postgres', dataset_name='ref')
with pipeline.sql_client() as client:
    result = client.execute_sql('SELECT COUNT(*) FROM ref.coaches')
    print(f'coaches: {list(result)[0][0]:,}')
"`
Expected: ~2,000 rows

**Step 3: Reload remaining 4 tables**

```bash
# player_season_stats (~131K rows, takes a few minutes)
.venv/bin/python -m src.pipelines.run --source stats --endpoint player_season_stats --replace

# transfer_portal (~14K rows)
.venv/bin/python -m src.pipelines.run --source recruiting --endpoint transfer_portal --replace

# lines (~20K rows)
.venv/bin/python -m src.pipelines.run --source betting --endpoint lines --replace

# draft_picks (~1.5K rows)
.venv/bin/python -m src.pipelines.run --source draft --endpoint picks --replace
```

**Step 4: Verify all row counts**

Run: `.venv/bin/python -c "
import dlt
tables = [
    ('cfbd_reference', 'ref', 'coaches'),
    ('cfbd_stats', 'stats', 'player_season_stats'),
    ('cfbd_recruiting', 'recruiting', 'transfer_portal'),
    ('cfbd_betting', 'betting', 'lines'),
    ('cfbd_draft', 'draft', 'draft_picks'),
]
for pipeline_name, schema, table in tables:
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination='postgres', dataset_name=schema)
    with pipeline.sql_client() as client:
        result = client.execute_sql(f'SELECT COUNT(*) FROM {schema}.{table}')
        print(f'{table}: {list(result)[0][0]:,}')
"`

**Step 5: Commit reload verification**

```bash
git add -A && git commit -m "$(cat <<'EOF'
chore: reload 5 tables with corrected primary keys

Verified row counts after reload:
- coaches: ~2K
- player_season_stats: ~131K
- transfer_portal: ~14K
- lines: ~20K
- draft_picks: ~1.5K

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Create Analytics Index Migration

**Files:**
- Create: `src/schemas/016_analytics_indexes.sql`

**Step 1: Write the index migration**

Create `src/schemas/016_analytics_indexes.sql`:

```sql
-- Analytics-driven indexes for mart queries
-- Derived from JOIN/WHERE patterns in existing 9 marts + 5 API views

-- plays: most queried table (3.6M rows)
CREATE INDEX IF NOT EXISTS idx_plays_game_drive
    ON core.plays (game_id, drive_id);
CREATE INDEX IF NOT EXISTS idx_plays_offense_season
    ON core.plays (offense, season);
CREATE INDEX IF NOT EXISTS idx_plays_defense_season
    ON core.plays (defense, season);

-- drives: game-level rollups
CREATE INDEX IF NOT EXISTS idx_drives_game
    ON core.drives (game_id);

-- games: filtering and matchup lookups
CREATE INDEX IF NOT EXISTS idx_games_season_week
    ON core.games (season, week);
CREATE INDEX IF NOT EXISTS idx_games_teams
    ON core.games (home_team, away_team);

-- game_team_stats: box score joins
CREATE INDEX IF NOT EXISTS idx_game_team_stats_game_team
    ON core.game_team_stats (game_id, team);

-- player_season_stats: player lookups
CREATE INDEX IF NOT EXISTS idx_player_season_stats_player_season
    ON stats.player_season_stats ("playerId", season);

-- recruits: talent composite
CREATE INDEX IF NOT EXISTS idx_recruits_team_year
    ON recruiting.recruits (school, year);

-- team_recruiting: recruiting rankings
CREATE INDEX IF NOT EXISTS idx_team_recruiting_team_year
    ON recruiting.team_recruiting (team, year);

-- sp_ratings: team ratings joins
CREATE INDEX IF NOT EXISTS idx_sp_ratings_team_year
    ON ratings.sp_ratings (team, year);
```

**Step 2: Run the migration**

Run: `psql $DATABASE_URL -f src/schemas/016_analytics_indexes.sql`
Expected: CREATE INDEX (multiple times)

**Step 3: Verify indexes created**

Run: `psql $DATABASE_URL -c "SELECT indexname FROM pg_indexes WHERE indexname LIKE 'idx_%' ORDER BY indexname;"`
Expected: Lists all new indexes

**Step 4: Commit**

```bash
git add src/schemas/016_analytics_indexes.sql
git commit -m "$(cat <<'EOF'
feat(schema): add analytics-driven indexes

12 indexes on most-queried columns:
- plays: game_id/drive_id, offense/season, defense/season
- drives: game_id
- games: season/week, home_team/away_team
- game_team_stats: game_id/team
- player_season_stats: playerId/season
- recruits: school/year
- team_recruiting: team/year
- sp_ratings: team/year

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Phase 4B: Analytics Expansion

### Task 9: Create Era Reference Table

**Files:**
- Create: `src/schemas/017_era_reference.sql`

**Step 1: Write the era reference table**

```sql
-- Era definitions for historical analysis
DROP TABLE IF EXISTS ref.eras CASCADE;

CREATE TABLE ref.eras (
    era_code VARCHAR(20) PRIMARY KEY,
    era_name VARCHAR(50) NOT NULL,
    start_year INT NOT NULL,
    end_year INT,  -- NULL means ongoing
    description TEXT
);

INSERT INTO ref.eras (era_code, era_name, start_year, end_year, description) VALUES
    ('BCS', 'BCS Era', 2004, 2013, 'Bowl Championship Series, pre-playoff'),
    ('PLAYOFF_V1', 'Playoff V1', 2014, 2023, '4-team playoff, conference championship emphasis'),
    ('PORTAL_NIL', 'Portal/NIL Era', 2021, NULL, 'Transfer portal explosion, NIL deals reshape rosters'),
    ('PLAYOFF_V2', 'Playoff V2', 2024, NULL, '12-team playoff, expanded access');

-- Helper function to get era for a given year
CREATE OR REPLACE FUNCTION ref.get_era(p_year INT)
RETURNS TABLE(era_code VARCHAR, era_name VARCHAR) AS $$
BEGIN
    RETURN QUERY
    SELECT e.era_code, e.era_name
    FROM ref.eras e
    WHERE p_year >= e.start_year
      AND (e.end_year IS NULL OR p_year <= e.end_year);
END;
$$ LANGUAGE plpgsql STABLE;
```

**Step 2: Run the migration**

Run: `psql $DATABASE_URL -f src/schemas/017_era_reference.sql`

**Step 3: Verify**

Run: `psql $DATABASE_URL -c "SELECT * FROM ref.eras ORDER BY start_year;"`
Expected: 4 era rows

**Step 4: Commit**

```bash
git add src/schemas/017_era_reference.sql
git commit -m "$(cat <<'EOF'
feat(schema): add era reference table for historical analysis

Defines 4 eras: BCS (2004-2013), Playoff V1 (2014-2023),
Portal/NIL (2021+), Playoff V2 (2024+).

Includes helper function get_era(year) for era lookups.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: Create play_epa Materialized View

**Files:**
- Create: `src/schemas/marts/010_play_epa.sql`

**Step 1: Write the play_epa mart**

```sql
-- Per-play EPA metrics (foundation for all EPA aggregations)
DROP MATERIALIZED VIEW IF EXISTS marts.play_epa CASCADE;

CREATE MATERIALIZED VIEW marts.play_epa AS
SELECT
    p.id AS play_id,
    p.game_id,
    p.drive_id,
    p.season,
    p.offense,
    p.defense,
    p.down,
    p.distance,
    p.yards_to_goal,
    p.yards_gained,
    p.play_type,
    p.scoring,
    p.period,
    -- EPA from CFBD API (already calculated)
    p.ppa AS epa,
    -- Success: positive EPA
    CASE WHEN p.ppa > 0 THEN 1 ELSE 0 END AS success,
    -- Explosive: EPA > 0.5 on successful plays
    CASE WHEN p.ppa > 0.5 THEN 1 ELSE 0 END AS explosive,
    -- Situational flags
    CASE
        WHEN p.down = 1 THEN 'first'
        WHEN p.down = 2 THEN 'second'
        WHEN p.down = 3 THEN 'third'
        WHEN p.down = 4 THEN 'fourth'
    END AS down_name,
    CASE
        WHEN p.distance <= 3 THEN 'short'
        WHEN p.distance <= 7 THEN 'medium'
        ELSE 'long'
    END AS distance_bucket,
    CASE
        WHEN p.yards_to_goal <= 20 THEN 'red_zone'
        WHEN p.yards_to_goal <= 40 THEN 'opponent_territory'
        WHEN p.yards_to_goal <= 60 THEN 'midfield'
        ELSE 'own_territory'
    END AS field_position
FROM core.plays p
WHERE p.ppa IS NOT NULL
  AND p.play_type NOT IN ('Timeout', 'End Period', 'End of Half', 'End of Game', 'Kickoff');

-- Indexes for aggregation queries
CREATE UNIQUE INDEX ON marts.play_epa (play_id);
CREATE INDEX ON marts.play_epa (game_id);
CREATE INDEX ON marts.play_epa (offense, season);
CREATE INDEX ON marts.play_epa (defense, season);
CREATE INDEX ON marts.play_epa (down_name, distance_bucket);
```

**Step 2: Run the migration**

Run: `psql $DATABASE_URL -f src/schemas/marts/010_play_epa.sql`
Expected: CREATE MATERIALIZED VIEW (may take 1-2 minutes for 3.6M plays)

**Step 3: Verify row count**

Run: `psql $DATABASE_URL -c "SELECT COUNT(*) FROM marts.play_epa;"`
Expected: ~2.5-3M rows (filtered plays with EPA)

**Step 4: Commit**

```bash
git add src/schemas/marts/010_play_epa.sql
git commit -m "$(cat <<'EOF'
feat(marts): add play_epa foundation view

Per-play EPA metrics with situational flags:
- down_name (first/second/third/fourth)
- distance_bucket (short/medium/long)
- field_position (red_zone/opponent/midfield/own)
- success flag (epa > 0)
- explosive flag (epa > 0.5)

Foundation for all EPA aggregation marts.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 11: Create team_game_epa Materialized View

**Files:**
- Create: `src/schemas/marts/011_team_game_epa.sql`

**Step 1: Write the team_game_epa mart**

```sql
-- Team EPA metrics per game
DROP MATERIALIZED VIEW IF EXISTS marts.team_game_epa CASCADE;

CREATE MATERIALIZED VIEW marts.team_game_epa AS
WITH offense_stats AS (
    SELECT
        game_id,
        season,
        offense AS team,
        COUNT(*) AS plays,
        SUM(epa) AS total_epa,
        AVG(epa) AS epa_per_play,
        AVG(success)::NUMERIC(5,3) AS success_rate,
        AVG(CASE WHEN success = 1 THEN epa END) AS explosiveness
    FROM marts.play_epa
    GROUP BY game_id, season, offense
),
defense_stats AS (
    SELECT
        game_id,
        season,
        defense AS team,
        COUNT(*) AS def_plays,
        SUM(epa) AS def_epa_allowed,
        AVG(epa) AS def_epa_per_play,
        AVG(success)::NUMERIC(5,3) AS def_success_rate_allowed
    FROM marts.play_epa
    GROUP BY game_id, season, defense
)
SELECT
    o.game_id,
    o.season,
    o.team,
    -- Offense
    o.plays AS off_plays,
    o.total_epa AS off_total_epa,
    o.epa_per_play AS off_epa_per_play,
    o.success_rate AS off_success_rate,
    o.explosiveness AS off_explosiveness,
    -- Defense
    d.def_plays,
    d.def_epa_allowed,
    d.def_epa_per_play,
    d.def_success_rate_allowed,
    -- Net
    o.total_epa - d.def_epa_allowed AS net_epa
FROM offense_stats o
JOIN defense_stats d ON o.game_id = d.game_id AND o.team = d.team;

CREATE UNIQUE INDEX ON marts.team_game_epa (game_id, team);
CREATE INDEX ON marts.team_game_epa (team, season);
CREATE INDEX ON marts.team_game_epa (season);
```

**Step 2: Run the migration**

Run: `psql $DATABASE_URL -f src/schemas/marts/011_team_game_epa.sql`

**Step 3: Verify**

Run: `psql $DATABASE_URL -c "SELECT team, season, off_epa_per_play, off_success_rate FROM marts.team_game_epa WHERE team = 'Alabama' AND season = 2024 LIMIT 5;"`

**Step 4: Commit**

```bash
git add src/schemas/marts/011_team_game_epa.sql
git commit -m "$(cat <<'EOF'
feat(marts): add team_game_epa aggregation

Per-game team EPA metrics:
- Offensive EPA (total, per play, success rate, explosiveness)
- Defensive EPA allowed
- Net EPA

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 12: Create team_season_epa Materialized View

**Files:**
- Create: `src/schemas/marts/012_team_season_epa.sql`

**Step 1: Write the mart**

```sql
-- Team EPA metrics per season
DROP MATERIALIZED VIEW IF EXISTS marts.team_season_epa CASCADE;

CREATE MATERIALIZED VIEW marts.team_season_epa AS
SELECT
    season,
    team,
    COUNT(DISTINCT game_id) AS games,
    SUM(off_plays) AS total_plays,
    SUM(off_total_epa) AS total_epa,
    AVG(off_epa_per_play)::NUMERIC(6,4) AS epa_per_play,
    AVG(off_success_rate)::NUMERIC(5,3) AS success_rate,
    AVG(off_explosiveness)::NUMERIC(6,4) AS explosiveness,
    -- Defense
    SUM(def_plays) AS total_def_plays,
    SUM(def_epa_allowed) AS total_def_epa_allowed,
    AVG(def_epa_per_play)::NUMERIC(6,4) AS def_epa_per_play,
    AVG(def_success_rate_allowed)::NUMERIC(5,3) AS def_success_rate_allowed,
    -- Net
    SUM(net_epa) AS total_net_epa,
    AVG(net_epa)::NUMERIC(6,2) AS avg_net_epa_per_game,
    -- Rankings (calculated after)
    RANK() OVER (PARTITION BY season ORDER BY AVG(off_epa_per_play) DESC) AS off_epa_rank,
    RANK() OVER (PARTITION BY season ORDER BY AVG(def_epa_per_play) ASC) AS def_epa_rank
FROM marts.team_game_epa
GROUP BY season, team;

CREATE UNIQUE INDEX ON marts.team_season_epa (season, team);
CREATE INDEX ON marts.team_season_epa (team);
CREATE INDEX ON marts.team_season_epa (off_epa_rank);
```

**Step 2: Run the migration**

Run: `psql $DATABASE_URL -f src/schemas/marts/012_team_season_epa.sql`

**Step 3: Verify top teams**

Run: `psql $DATABASE_URL -c "SELECT team, epa_per_play, success_rate, off_epa_rank FROM marts.team_season_epa WHERE season = 2024 ORDER BY off_epa_rank LIMIT 10;"`

**Step 4: Commit**

```bash
git add src/schemas/marts/012_team_season_epa.sql
git commit -m "$(cat <<'EOF'
feat(marts): add team_season_epa with rankings

Season-level EPA aggregations:
- Total and per-play EPA (offense and defense)
- Success rate and explosiveness
- Intra-season rankings for offense and defense

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 13: Create team_situational_epa Materialized View

**Files:**
- Create: `src/schemas/marts/013_team_situational_epa.sql`

**Step 1: Write the mart**

```sql
-- Team EPA by situation (down, distance, field position)
DROP MATERIALIZED VIEW IF EXISTS marts.team_situational_epa CASCADE;

CREATE MATERIALIZED VIEW marts.team_situational_epa AS
WITH by_down AS (
    SELECT
        season, offense AS team, 'down' AS split_type, down_name AS split_value,
        COUNT(*) AS plays, AVG(epa)::NUMERIC(6,4) AS epa_per_play,
        AVG(success)::NUMERIC(5,3) AS success_rate
    FROM marts.play_epa
    GROUP BY season, offense, down_name
),
by_distance AS (
    SELECT
        season, offense AS team, 'distance' AS split_type, distance_bucket AS split_value,
        COUNT(*) AS plays, AVG(epa)::NUMERIC(6,4) AS epa_per_play,
        AVG(success)::NUMERIC(5,3) AS success_rate
    FROM marts.play_epa
    GROUP BY season, offense, distance_bucket
),
by_field_pos AS (
    SELECT
        season, offense AS team, 'field_position' AS split_type, field_position AS split_value,
        COUNT(*) AS plays, AVG(epa)::NUMERIC(6,4) AS epa_per_play,
        AVG(success)::NUMERIC(5,3) AS success_rate
    FROM marts.play_epa
    GROUP BY season, offense, field_position
),
by_period AS (
    SELECT
        season, offense AS team, 'period' AS split_type, 'Q' || period::TEXT AS split_value,
        COUNT(*) AS plays, AVG(epa)::NUMERIC(6,4) AS epa_per_play,
        AVG(success)::NUMERIC(5,3) AS success_rate
    FROM marts.play_epa
    WHERE period <= 4
    GROUP BY season, offense, period
)
SELECT * FROM by_down
UNION ALL SELECT * FROM by_distance
UNION ALL SELECT * FROM by_field_pos
UNION ALL SELECT * FROM by_period;

CREATE UNIQUE INDEX ON marts.team_situational_epa (season, team, split_type, split_value);
CREATE INDEX ON marts.team_situational_epa (team, season);
CREATE INDEX ON marts.team_situational_epa (split_type, split_value);
```

**Step 2: Run the migration**

Run: `psql $DATABASE_URL -f src/schemas/marts/013_team_situational_epa.sql`

**Step 3: Verify situational data**

Run: `psql $DATABASE_URL -c "SELECT split_type, split_value, epa_per_play, success_rate FROM marts.team_situational_epa WHERE team = 'Georgia' AND season = 2024 ORDER BY split_type, split_value;"`

**Step 4: Commit**

```bash
git add src/schemas/marts/013_team_situational_epa.sql
git commit -m "$(cat <<'EOF'
feat(marts): add team_situational_epa splits

EPA broken down by:
- Down (1st, 2nd, 3rd, 4th)
- Distance (short, medium, long)
- Field position (own, midfield, opponent, red zone)
- Period (Q1-Q4)

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 14: Create player_game_epa Materialized View

**Files:**
- Create: `src/schemas/marts/014_player_game_epa.sql`

**Step 1: Write the mart**

Note: Play-by-play has play_text with player names but no clean player_id. We'll extract what we can from structured fields.

```sql
-- Player EPA attribution per game (from play-by-play)
-- Limited to plays where we can attribute to a player via play_text parsing
DROP MATERIALIZED VIEW IF EXISTS marts.player_game_epa CASCADE;

CREATE MATERIALIZED VIEW marts.player_game_epa AS
WITH rushing_plays AS (
    SELECT
        game_id, season, offense AS team,
        -- Extract rusher from play_text (pattern: "PlayerName rush for X yards")
        REGEXP_REPLACE(
            SPLIT_PART(play_text, ' rush ', 1),
            '^\d+-(.*?)$', '\1'
        ) AS player_name,
        'rushing' AS play_category,
        epa, success, explosive, yards_gained
    FROM marts.play_epa
    WHERE play_type IN ('Rush', 'Rushing Touchdown')
      AND play_text LIKE '% rush %'
),
passing_plays AS (
    SELECT
        game_id, season, offense AS team,
        REGEXP_REPLACE(
            SPLIT_PART(play_text, ' pass ', 1),
            '^\d+-(.*?)$', '\1'
        ) AS player_name,
        'passing' AS play_category,
        epa, success, explosive, yards_gained
    FROM marts.play_epa
    WHERE play_type IN ('Pass Reception', 'Passing Touchdown', 'Pass Incompletion', 'Pass Interception Return')
      AND play_text LIKE '% pass %'
),
all_attributed AS (
    SELECT * FROM rushing_plays
    UNION ALL
    SELECT * FROM passing_plays
)
SELECT
    game_id,
    season,
    team,
    player_name,
    play_category,
    COUNT(*) AS plays,
    SUM(epa)::NUMERIC(8,2) AS total_epa,
    AVG(epa)::NUMERIC(6,4) AS epa_per_play,
    AVG(success)::NUMERIC(5,3) AS success_rate,
    SUM(yards_gained) AS total_yards
FROM all_attributed
WHERE player_name IS NOT NULL AND player_name != ''
GROUP BY game_id, season, team, player_name, play_category
HAVING COUNT(*) >= 3;  -- Minimum 3 plays to be included

CREATE UNIQUE INDEX ON marts.player_game_epa (game_id, team, player_name, play_category);
CREATE INDEX ON marts.player_game_epa (player_name, season);
CREATE INDEX ON marts.player_game_epa (team, season);
```

**Step 2: Run the migration**

Run: `psql $DATABASE_URL -f src/schemas/marts/014_player_game_epa.sql`

**Step 3: Verify player data**

Run: `psql $DATABASE_URL -c "SELECT player_name, play_category, plays, total_epa, epa_per_play FROM marts.player_game_epa WHERE team = 'Ohio State' AND season = 2024 ORDER BY total_epa DESC LIMIT 10;"`

**Step 4: Commit**

```bash
git add src/schemas/marts/014_player_game_epa.sql
git commit -m "$(cat <<'EOF'
feat(marts): add player_game_epa attribution

Extract player EPA from play-by-play text:
- Rushing plays (rusher attribution)
- Passing plays (passer attribution)
- Minimum 3 plays per player/game/category

Workaround for deferred game_player_stats endpoint.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 15: Create player_season_epa Materialized View

**Files:**
- Create: `src/schemas/marts/015_player_season_epa.sql`

**Step 1: Write the mart**

```sql
-- Player EPA aggregated by season
DROP MATERIALIZED VIEW IF EXISTS marts.player_season_epa CASCADE;

CREATE MATERIALIZED VIEW marts.player_season_epa AS
SELECT
    season,
    team,
    player_name,
    play_category,
    COUNT(DISTINCT game_id) AS games,
    SUM(plays) AS total_plays,
    SUM(total_epa)::NUMERIC(8,2) AS total_epa,
    (SUM(total_epa) / NULLIF(SUM(plays), 0))::NUMERIC(6,4) AS epa_per_play,
    AVG(success_rate)::NUMERIC(5,3) AS success_rate,
    SUM(total_yards) AS total_yards,
    -- Usage: plays per game
    (SUM(plays)::NUMERIC / COUNT(DISTINCT game_id))::NUMERIC(5,1) AS plays_per_game,
    -- Rankings
    RANK() OVER (
        PARTITION BY season, play_category
        ORDER BY SUM(total_epa) DESC
    ) AS epa_rank
FROM marts.player_game_epa
GROUP BY season, team, player_name, play_category
HAVING SUM(plays) >= 20;  -- Minimum 20 plays on season

CREATE UNIQUE INDEX ON marts.player_season_epa (season, team, player_name, play_category);
CREATE INDEX ON marts.player_season_epa (season, play_category, epa_rank);
CREATE INDEX ON marts.player_season_epa (player_name);
```

**Step 2: Run the migration**

Run: `psql $DATABASE_URL -f src/schemas/marts/015_player_season_epa.sql`

**Step 3: Verify top players**

Run: `psql $DATABASE_URL -c "SELECT player_name, team, total_epa, epa_per_play, epa_rank FROM marts.player_season_epa WHERE season = 2024 AND play_category = 'rushing' ORDER BY epa_rank LIMIT 10;"`

**Step 4: Commit**

```bash
git add src/schemas/marts/015_player_season_epa.sql
git commit -m "$(cat <<'EOF'
feat(marts): add player_season_epa with rankings

Season-level player EPA:
- Total and per-play EPA
- Usage metrics (plays per game)
- Intra-season rankings by category
- Minimum 20 plays threshold

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 16: Create team_season_trajectory Materialized View

**Files:**
- Create: `src/schemas/marts/016_team_season_trajectory.sql`

**Step 1: Write the mart**

```sql
-- Team performance trajectory year-over-year
DROP MATERIALIZED VIEW IF EXISTS marts.team_season_trajectory CASCADE;

CREATE MATERIALIZED VIEW marts.team_season_trajectory AS
WITH team_metrics AS (
    SELECT
        t.season,
        t.team,
        t.epa_per_play,
        t.success_rate,
        t.off_epa_rank,
        t.def_epa_rank,
        -- Win percentage from games
        g.wins::NUMERIC / NULLIF(g.games, 0) AS win_pct,
        g.wins,
        g.games,
        -- Recruiting rank (if available)
        r.rank AS recruiting_rank,
        r.points AS recruiting_points
    FROM marts.team_season_epa t
    LEFT JOIN (
        SELECT
            season,
            CASE WHEN home_points > away_points THEN home_team ELSE away_team END AS team,
            COUNT(*) AS wins,
            COUNT(*) OVER (PARTITION BY season, home_team) +
            COUNT(*) OVER (PARTITION BY season, away_team) AS games
        FROM core.games
        WHERE home_points IS NOT NULL
        GROUP BY season,
            CASE WHEN home_points > away_points THEN home_team ELSE away_team END
    ) g ON t.season = g.season AND t.team = g.team
    LEFT JOIN recruiting.team_recruiting r ON t.team = r.team AND t.season = r.year
)
SELECT
    m.*,
    -- Era assignment
    e.era_code,
    e.era_name,
    -- Year-over-year deltas
    LAG(m.epa_per_play) OVER (PARTITION BY m.team ORDER BY m.season) AS prev_epa,
    m.epa_per_play - LAG(m.epa_per_play) OVER (PARTITION BY m.team ORDER BY m.season) AS epa_delta,
    LAG(m.win_pct) OVER (PARTITION BY m.team ORDER BY m.season) AS prev_win_pct,
    m.win_pct - LAG(m.win_pct) OVER (PARTITION BY m.team ORDER BY m.season) AS win_pct_delta
FROM team_metrics m
LEFT JOIN LATERAL ref.get_era(m.season) e ON TRUE;

CREATE UNIQUE INDEX ON marts.team_season_trajectory (season, team);
CREATE INDEX ON marts.team_season_trajectory (team);
CREATE INDEX ON marts.team_season_trajectory (era_code, season);
```

**Step 2: Run the migration**

Run: `psql $DATABASE_URL -f src/schemas/marts/016_team_season_trajectory.sql`

**Step 3: Verify trajectory data**

Run: `psql $DATABASE_URL -c "SELECT season, era_name, epa_per_play, epa_delta, win_pct FROM marts.team_season_trajectory WHERE team = 'Texas' ORDER BY season DESC LIMIT 10;"`

**Step 4: Commit**

```bash
git add src/schemas/marts/016_team_season_trajectory.sql
git commit -m "$(cat <<'EOF'
feat(marts): add team_season_trajectory with era awareness

Year-over-year team performance:
- EPA metrics with deltas
- Win percentage tracking
- Recruiting integration
- Era assignment (BCS, Playoff V1/V2, Portal/NIL)

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 17: Create conference_era_summary Materialized View

**Files:**
- Create: `src/schemas/marts/017_conference_era_summary.sql`

**Step 1: Write the mart**

```sql
-- Conference strength by era
DROP MATERIALIZED VIEW IF EXISTS marts.conference_era_summary CASCADE;

CREATE MATERIALIZED VIEW marts.conference_era_summary AS
WITH conference_seasons AS (
    SELECT
        e.era_code,
        e.era_name,
        t.season,
        g.home_conference AS conference,
        AVG(t.epa_per_play)::NUMERIC(6,4) AS avg_epa,
        AVG(t.success_rate)::NUMERIC(5,3) AS avg_success_rate,
        COUNT(DISTINCT t.team) AS teams
    FROM marts.team_season_epa t
    JOIN core.games g ON t.team = g.home_team AND t.season = g.season
    CROSS JOIN LATERAL ref.get_era(t.season) e
    WHERE g.home_conference IS NOT NULL
    GROUP BY e.era_code, e.era_name, t.season, g.home_conference
)
SELECT
    era_code,
    era_name,
    conference,
    COUNT(DISTINCT season) AS seasons,
    AVG(avg_epa)::NUMERIC(6,4) AS avg_epa,
    AVG(avg_success_rate)::NUMERIC(5,3) AS avg_success_rate,
    AVG(teams)::NUMERIC(4,1) AS avg_teams,
    -- Era-level ranking
    RANK() OVER (PARTITION BY era_code ORDER BY AVG(avg_epa) DESC) AS era_rank
FROM conference_seasons
GROUP BY era_code, era_name, conference;

CREATE UNIQUE INDEX ON marts.conference_era_summary (era_code, conference);
CREATE INDEX ON marts.conference_era_summary (conference);
```

**Step 2: Run the migration**

Run: `psql $DATABASE_URL -f src/schemas/marts/017_conference_era_summary.sql`

**Step 3: Verify**

Run: `psql $DATABASE_URL -c "SELECT era_name, conference, avg_epa, era_rank FROM marts.conference_era_summary WHERE era_code = 'PLAYOFF_V1' ORDER BY era_rank;"`

**Step 4: Commit**

```bash
git add src/schemas/marts/017_conference_era_summary.sql
git commit -m "$(cat <<'EOF'
feat(marts): add conference_era_summary

Conference strength by era:
- Average EPA and success rate
- Era-level rankings
- Team counts per conference

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 18: Create rivalry_history Materialized View

**Files:**
- Create: `src/schemas/marts/018_rivalry_history.sql`

**Step 1: Write the mart**

```sql
-- Head-to-head rivalry records
DROP MATERIALIZED VIEW IF EXISTS marts.rivalry_history CASCADE;

CREATE MATERIALIZED VIEW marts.rivalry_history AS
WITH matchups AS (
    SELECT
        LEAST(home_team, away_team) AS team_a,
        GREATEST(home_team, away_team) AS team_b,
        season,
        id AS game_id,
        home_team,
        away_team,
        home_points,
        away_points,
        CASE
            WHEN home_points > away_points THEN home_team
            WHEN away_points > home_points THEN away_team
            ELSE NULL
        END AS winner,
        CASE
            WHEN home_points = away_points THEN TRUE
            ELSE FALSE
        END AS tie,
        neutral_site
    FROM core.games
    WHERE home_points IS NOT NULL
)
SELECT
    team_a,
    team_b,
    COUNT(*) AS games_played,
    SUM(CASE WHEN winner = team_a THEN 1 ELSE 0 END) AS wins_a,
    SUM(CASE WHEN winner = team_b THEN 1 ELSE 0 END) AS wins_b,
    SUM(CASE WHEN tie THEN 1 ELSE 0 END) AS ties,
    MAX(season) AS last_meeting_season,
    MAX(game_id) AS last_meeting_game_id,
    MIN(season) AS first_meeting_season,
    -- Recent form (last 5)
    STRING_AGG(
        CASE WHEN winner = team_a THEN 'W' WHEN winner = team_b THEN 'L' ELSE 'T' END,
        '' ORDER BY season DESC
    ) FILTER (WHERE season >= (SELECT MAX(season) - 4 FROM core.games)) AS last_5_from_a
FROM matchups
GROUP BY team_a, team_b
HAVING COUNT(*) >= 3;  -- Minimum 3 meetings to be a "rivalry"

CREATE UNIQUE INDEX ON marts.rivalry_history (team_a, team_b);
CREATE INDEX ON marts.rivalry_history (team_a);
CREATE INDEX ON marts.rivalry_history (team_b);
```

**Step 2: Run the migration**

Run: `psql $DATABASE_URL -f src/schemas/marts/018_rivalry_history.sql`

**Step 3: Verify classic rivalries**

Run: `psql $DATABASE_URL -c "SELECT team_a, team_b, games_played, wins_a, wins_b, last_5_from_a FROM marts.rivalry_history WHERE (team_a = 'Michigan' AND team_b = 'Ohio State') OR (team_a = 'Alabama' AND team_b = 'Auburn');"`

**Step 4: Commit**

```bash
git add src/schemas/marts/018_rivalry_history.sql
git commit -m "$(cat <<'EOF'
feat(marts): add rivalry_history head-to-head records

All-time matchup records:
- Wins for each team, ties
- First and last meetings
- Last 5 results for form

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 19: Create team_style_profile Materialized View

**Files:**
- Create: `src/schemas/marts/019_team_style_profile.sql`

**Step 1: Write the mart**

```sql
-- Team offensive/defensive style profile
DROP MATERIALIZED VIEW IF EXISTS marts.team_style_profile CASCADE;

CREATE MATERIALIZED VIEW marts.team_style_profile AS
WITH play_types AS (
    SELECT
        season,
        offense AS team,
        COUNT(*) AS total_plays,
        SUM(CASE WHEN play_type IN ('Rush', 'Rushing Touchdown') THEN 1 ELSE 0 END) AS rush_plays,
        SUM(CASE WHEN play_type LIKE 'Pass%' THEN 1 ELSE 0 END) AS pass_plays,
        AVG(CASE WHEN play_type IN ('Rush', 'Rushing Touchdown') THEN epa END)::NUMERIC(6,4) AS rush_epa,
        AVG(CASE WHEN play_type LIKE 'Pass%' THEN epa END)::NUMERIC(6,4) AS pass_epa
    FROM marts.play_epa
    GROUP BY season, offense
),
tempo AS (
    SELECT
        p.season,
        p.offense AS team,
        COUNT(*)::NUMERIC / COUNT(DISTINCT p.game_id) AS plays_per_game
    FROM marts.play_epa p
    GROUP BY p.season, p.offense
),
defense AS (
    SELECT
        season,
        defense AS team,
        AVG(CASE WHEN play_type IN ('Rush', 'Rushing Touchdown') THEN epa END)::NUMERIC(6,4) AS def_rush_epa,
        AVG(CASE WHEN play_type LIKE 'Pass%' THEN epa END)::NUMERIC(6,4) AS def_pass_epa
    FROM marts.play_epa
    GROUP BY season, defense
)
SELECT
    pt.season,
    pt.team,
    -- Offensive style
    (pt.rush_plays::NUMERIC / NULLIF(pt.total_plays, 0))::NUMERIC(5,3) AS run_rate,
    (pt.pass_plays::NUMERIC / NULLIF(pt.total_plays, 0))::NUMERIC(5,3) AS pass_rate,
    pt.rush_epa AS epa_rushing,
    pt.pass_epa AS epa_passing,
    -- Tempo
    t.plays_per_game,
    CASE
        WHEN t.plays_per_game >= 75 THEN 'up_tempo'
        WHEN t.plays_per_game >= 65 THEN 'balanced'
        ELSE 'slow'
    END AS tempo_category,
    -- Defense
    d.def_rush_epa AS def_epa_vs_run,
    d.def_pass_epa AS def_epa_vs_pass,
    -- Style tags
    CASE
        WHEN pt.rush_plays::NUMERIC / NULLIF(pt.total_plays, 0) >= 0.55 THEN 'run_heavy'
        WHEN pt.pass_plays::NUMERIC / NULLIF(pt.total_plays, 0) >= 0.55 THEN 'pass_heavy'
        ELSE 'balanced'
    END AS offensive_identity
FROM play_types pt
JOIN tempo t ON pt.season = t.season AND pt.team = t.team
LEFT JOIN defense d ON pt.season = d.season AND pt.team = d.team;

CREATE UNIQUE INDEX ON marts.team_style_profile (season, team);
CREATE INDEX ON marts.team_style_profile (season, offensive_identity);
CREATE INDEX ON marts.team_style_profile (season, tempo_category);
```

**Step 2: Run the migration**

Run: `psql $DATABASE_URL -f src/schemas/marts/019_team_style_profile.sql`

**Step 3: Verify**

Run: `psql $DATABASE_URL -c "SELECT team, run_rate, pass_rate, tempo_category, offensive_identity FROM marts.team_style_profile WHERE season = 2024 ORDER BY run_rate DESC LIMIT 10;"`

**Step 4: Commit**

```bash
git add src/schemas/marts/019_team_style_profile.sql
git commit -m "$(cat <<'EOF'
feat(marts): add team_style_profile

Offensive and defensive identity:
- Run/pass rates
- EPA by play type (offense and defense)
- Tempo classification
- Style tags (run_heavy, pass_heavy, balanced)

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 20: Create matchup_edges Materialized View

**Files:**
- Create: `src/schemas/marts/020_matchup_edges.sql`

**Step 1: Write the mart**

```sql
-- Matchup edge indicators for game predictions
DROP MATERIALIZED VIEW IF EXISTS marts.matchup_edges CASCADE;

CREATE MATERIALIZED VIEW marts.matchup_edges AS
WITH team_pairs AS (
    SELECT DISTINCT
        g.season,
        g.home_team AS team_a,
        g.away_team AS team_b,
        g.id AS game_id
    FROM core.games g
    WHERE g.season >= 2020  -- Recent seasons only for predictions
)
SELECT
    tp.season,
    tp.team_a,
    tp.team_b,
    tp.game_id,
    -- Team A style
    sa.run_rate AS a_run_rate,
    sa.epa_rushing AS a_epa_rushing,
    sa.epa_passing AS a_epa_passing,
    -- Team B style
    sb.run_rate AS b_run_rate,
    sb.epa_rushing AS b_epa_rushing,
    sb.epa_passing AS b_epa_passing,
    -- Matchup edges (positive = advantage to team_a)
    (sa.epa_rushing - sb.def_epa_vs_run)::NUMERIC(6,4) AS a_rush_edge,
    (sa.epa_passing - sb.def_epa_vs_pass)::NUMERIC(6,4) AS a_pass_edge,
    (sb.epa_rushing - sa.def_epa_vs_run)::NUMERIC(6,4) AS b_rush_edge,
    (sb.epa_passing - sa.def_epa_vs_pass)::NUMERIC(6,4) AS b_pass_edge,
    -- Tempo mismatch
    ABS(sa.plays_per_game - sb.plays_per_game)::NUMERIC(5,1) AS tempo_mismatch,
    -- Overall edge estimate
    ((sa.epa_rushing - sb.def_epa_vs_run) * sa.run_rate +
     (sa.epa_passing - sb.def_epa_vs_pass) * sa.pass_rate -
     (sb.epa_rushing - sa.def_epa_vs_run) * sb.run_rate -
     (sb.epa_passing - sa.def_epa_vs_pass) * sb.pass_rate
    )::NUMERIC(6,4) AS net_edge_a
FROM team_pairs tp
JOIN marts.team_style_profile sa ON tp.season = sa.season AND tp.team_a = sa.team
JOIN marts.team_style_profile sb ON tp.season = sb.season AND tp.team_b = sb.team;

CREATE UNIQUE INDEX ON marts.matchup_edges (season, team_a, team_b);
CREATE INDEX ON marts.matchup_edges (game_id);
CREATE INDEX ON marts.matchup_edges (season);
```

**Step 2: Run the migration**

Run: `psql $DATABASE_URL -f src/schemas/marts/020_matchup_edges.sql`

**Step 3: Verify matchup edges**

Run: `psql $DATABASE_URL -c "SELECT team_a, team_b, a_rush_edge, a_pass_edge, net_edge_a FROM marts.matchup_edges WHERE season = 2024 AND (team_a = 'Georgia' OR team_b = 'Georgia') LIMIT 5;"`

**Step 4: Commit**

```bash
git add src/schemas/marts/020_matchup_edges.sql
git commit -m "$(cat <<'EOF'
feat(marts): add matchup_edges predictive indicators

Style-based matchup analysis:
- Rush/pass edge by team
- Tempo mismatch indicator
- Net edge estimate for predictions

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 21: Run All Tests and Final Verification

**Files:**
- None (verification only)

**Step 1: Run full test suite**

Run: `.venv/bin/pytest tests/ -v`
Expected: All tests pass

**Step 2: Verify all marts exist**

Run: `psql $DATABASE_URL -c "SELECT matviewname FROM pg_matviews WHERE schemaname = 'marts' ORDER BY matviewname;"`
Expected: Lists all 11+ marts

**Step 3: Run sample queries across marts**

Run: `psql $DATABASE_URL -c "
SELECT 'play_epa' AS mart, COUNT(*) FROM marts.play_epa
UNION ALL SELECT 'team_game_epa', COUNT(*) FROM marts.team_game_epa
UNION ALL SELECT 'team_season_epa', COUNT(*) FROM marts.team_season_epa
UNION ALL SELECT 'team_situational_epa', COUNT(*) FROM marts.team_situational_epa
UNION ALL SELECT 'player_game_epa', COUNT(*) FROM marts.player_game_epa
UNION ALL SELECT 'player_season_epa', COUNT(*) FROM marts.player_season_epa
UNION ALL SELECT 'team_season_trajectory', COUNT(*) FROM marts.team_season_trajectory
UNION ALL SELECT 'conference_era_summary', COUNT(*) FROM marts.conference_era_summary
UNION ALL SELECT 'rivalry_history', COUNT(*) FROM marts.rivalry_history
UNION ALL SELECT 'team_style_profile', COUNT(*) FROM marts.team_style_profile
UNION ALL SELECT 'matchup_edges', COUNT(*) FROM marts.matchup_edges;
"`

**Step 4: Final commit**

```bash
git add -A && git commit -m "$(cat <<'EOF'
chore: verify Sprint 4 complete

All marts created and verified:
- EPA foundation (play, team-game, team-season)
- Situational splits
- Player attribution
- Historical trends with era awareness
- Matchup intelligence

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Summary

**Phase 4A (Tasks 1-8):**
- 5 PK bug fixes with tests
- 12 analytics-driven indexes
- Table reloads with corrected PKs

**Phase 4B (Tasks 9-20):**
- Era reference table + helper function
- 11 new materialized views:
  - `play_epa` (foundation)
  - `team_game_epa`, `team_season_epa` (aggregations)
  - `team_situational_epa` (splits)
  - `player_game_epa`, `player_season_epa` (attribution)
  - `team_season_trajectory` (historical)
  - `conference_era_summary` (era analysis)
  - `rivalry_history` (matchup history)
  - `team_style_profile`, `matchup_edges` (predictive)

**Task 21:** Final verification

**Total commits:** ~20 atomic commits following TDD
