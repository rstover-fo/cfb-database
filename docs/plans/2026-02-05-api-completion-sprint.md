---
title: API Completion Sprint
type: feature
date: 2026-02-05
status: ready
---

# API Completion Sprint

## Overview

Add the 5 remaining CFBD API endpoints to achieve complete API coverage, and sync the endpoints config with tables that are already loaded but not tracked in config.

## Current State

**CFBD API:** 61 endpoints
**Configured:** 44 endpoints
**Actually loaded:** 51 endpoints (some loaded but not in config)
**Truly missing:** 5 endpoints

### Already Loaded (not in config)

These tables exist in the database but aren't tracked in `endpoints.py`:

| Table | Schema | Rows |
|-------|--------|------|
| wepa_team_season | metrics | 1,587 |
| wepa_players_passing | metrics | 2,313 |
| wepa_players_rushing | metrics | 4,975 |
| wepa_players_kicking | metrics | 1,732 |
| ppa_teams | metrics | 1,566 |
| pregame_win_probability | metrics | 10,073 |
| advanced_team_stats | stats | 2,887 |

### Missing Endpoints

| Endpoint | Table Name | Schema | Primary Key |
|----------|------------|--------|-------------|
| `/plays/stats` | play_stats | stats | game_id, play_id, athlete_id, stat_type |
| `/plays/stats/types` | play_stat_types | ref | id |
| `/stats/game/havoc` | game_havoc | stats | game_id, team |
| `/teams/ats` | team_ats | betting | year, team_id |
| `/metrics/fg/ep` | fg_expected_points | metrics | distance |

## Implementation Tasks

### Task 1: Add New Endpoint Configs

Add 5 new `EndpointConfig` entries to `src/pipelines/config/endpoints.py`:

```python
# In STATS_ENDPOINTS
"play_stats": EndpointConfig(
    path="/plays/stats",
    table_name="play_stats",
    primary_key=["game_id", "play_id", "athlete_id", "stat_type"],
    schema="stats",
    write_disposition="merge",
),
"game_havoc": EndpointConfig(
    path="/stats/game/havoc",
    table_name="game_havoc",
    primary_key=["game_id", "team"],
    schema="stats",
    write_disposition="merge",
),

# In REFERENCE_ENDPOINTS
"play_stat_types": EndpointConfig(
    path="/plays/stats/types",
    table_name="play_stat_types",
    primary_key=["id"],
    schema="ref",
    write_disposition="merge",
),

# In BETTING_ENDPOINTS
"team_ats": EndpointConfig(
    path="/teams/ats",
    table_name="team_ats",
    primary_key=["year", "team_id"],
    schema="betting",
    write_disposition="merge",
),

# In METRICS_ENDPOINTS
"fg_expected_points": EndpointConfig(
    path="/metrics/fg/ep",
    table_name="fg_expected_points",
    primary_key=["distance"],
    schema="metrics",
    write_disposition="merge",
),
```

### Task 2: Add Tests

Add PK validation tests to `tests/test_endpoints_config.py`:

```python
def test_play_stats_pk(self):
    """play_stats needs composite PK for player-play associations."""
    config = STATS_ENDPOINTS["play_stats"]
    assert config.primary_key == ["game_id", "play_id", "athlete_id", "stat_type"]

def test_game_havoc_pk(self):
    """game_havoc needs game_id + team composite PK."""
    config = STATS_ENDPOINTS["game_havoc"]
    assert config.primary_key == ["game_id", "team"]

def test_team_ats_pk(self):
    """team_ats needs year + team_id composite PK."""
    config = BETTING_ENDPOINTS["team_ats"]
    assert config.primary_key == ["year", "team_id"]
```

### Task 3: Sync Existing Endpoints

Add config entries for tables that are already loaded but not tracked:

```python
# In METRICS_ENDPOINTS (already have source files, just need config tracking)
"wepa_team_season": EndpointConfig(
    path="/wepa/team/season",
    table_name="wepa_team_season",
    primary_key=["year", "team"],
    schema="metrics",
    write_disposition="merge",
),
"wepa_players_passing": EndpointConfig(
    path="/wepa/players/passing",
    table_name="wepa_players_passing",
    primary_key=["id", "year"],
    schema="metrics",
    write_disposition="merge",
),
# ... etc for remaining WEPA endpoints

"ppa_teams": EndpointConfig(
    path="/ppa/teams",
    table_name="ppa_teams",
    primary_key=["season", "team"],
    schema="metrics",
    write_disposition="merge",
),
"pregame_win_probability": EndpointConfig(
    path="/metrics/wp/pregame",
    table_name="pregame_win_probability",
    primary_key=["season", "week", "team"],
    schema="metrics",
    write_disposition="merge",
),

# In STATS_ENDPOINTS
"advanced_team_stats": EndpointConfig(
    path="/stats/season/advanced",
    table_name="advanced_team_stats",
    primary_key=["season", "team"],
    schema="stats",
    write_disposition="merge",
),
```

### Task 4: Run Initial Backfill

```bash
# Load reference table first (no year param)
python -m src.pipelines.run --source reference --endpoint play_stat_types

# Load year-iterated endpoints
python -m src.pipelines.run --source stats --endpoint play_stats --mode backfill
python -m src.pipelines.run --source stats --endpoint game_havoc --mode backfill
python -m src.pipelines.run --source betting --endpoint team_ats --mode backfill
python -m src.pipelines.run --source metrics --endpoint fg_expected_points --mode backfill
```

## Acceptance Criteria

- [ ] 5 new endpoints added to `endpoints.py`
- [ ] 7 existing endpoints synced in config
- [ ] Tests pass (`pytest tests/test_endpoints_config.py`)
- [ ] All 5 new tables created in database with data
- [ ] Row counts documented

## Verification Query

```sql
SELECT 'play_stats' as table_name, COUNT(*) FROM stats.play_stats
UNION ALL SELECT 'play_stat_types', COUNT(*) FROM ref.play_stat_types
UNION ALL SELECT 'game_havoc', COUNT(*) FROM stats.game_havoc
UNION ALL SELECT 'team_ats', COUNT(*) FROM betting.team_ats
UNION ALL SELECT 'fg_expected_points', COUNT(*) FROM metrics.fg_expected_points;
```

## API Call Budget

**Estimated calls:** ~100 (20 years × 5 endpoints)
**Monthly limit:** 1,000 calls
**Status:** Well within budget

## Notes

- `play_stats` will be the largest table - contains player-level associations for every play
- `fg_expected_points` is a small reference-style table (distance → expected points)
- `play_stat_types` is a static reference table, load once
