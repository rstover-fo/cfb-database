# Sprint 4: Quality & Analytics Expansion — Design Document

**Status:** Approved
**Author:** Rob (Head of Technology)
**Date:** January 29, 2026

---

## Overview

**Goal:** Harden the schema foundation, then build a comprehensive analytics layer with advanced metrics, era-aware historical analysis, and matchup intelligence.

**Two phases:**

| Phase | Focus | Outcome |
|-------|-------|---------|
| **4A: Quality & Stability** | Fix PK bugs, add indexes, clean schema | Production-solid foundation |
| **4B: Analytics Expansion** | Advanced metrics, historical trends, matchups | Analyst-ready query layer |

**Guiding principles:**

- **Drop and reload** for PK fixes (clean slate)
- **Analytics-driven indexing** (index what the marts query, nothing speculative)
- **No FK enforcement** (warehouse pattern — integrity at mart level)
- **Era-aware design** (configurable year ranges: BCS, Playoff, Portal/NIL)
- **TDD throughout** (tests before implementation)

---

## Phase 4A: Quality & Stability

### PK Bug Fixes (Drop & Reload)

| Table | Current PK | Correct PK | Rows | API Calls |
|-------|-----------|------------|------|-----------|
| `coaches` | seasons | (first_name, last_name, school, season) | ~2K | ~22 (1/year) |
| `player_season_stats` | field mismatch | (player_id, season, team) | 131K | ~22 |
| `transfer_portal` | field mismatch | (player_id, season) | 14K | ~5 |
| `lines` | id | (game_id, provider) | 20K | ~22 |
| `draft_picks` | college_athlete_id | (year, overall) | 1.5K | ~22 |

**Process per table:**

1. Fix endpoint config (correct `primary_key` field)
2. Truncate existing table
3. Reload via CLI with `--replace` flag
4. Verify row count and PK uniqueness

### Analytics-Driven Indexes

Extract JOIN/WHERE columns from existing 9 marts + 5 API views:

| Table | Index Columns | Justification |
|-------|--------------|---------------|
| `plays` | (game_id, drive_id) | Join to drives/games |
| `plays` | (offense, season) | Team EPA aggregations |
| `drives` | (game_id) | Game-level rollups |
| `games` | (season, week) | Filtering |
| `games` | (home_team, away_team) | Matchup lookups |
| `game_team_stats` | (game_id, team) | Box score joins |
| `player_season_stats` | (player_id, season) | Player lookups |
| `recruiting` | (team, year) | Talent composite |

~10-12 indexes total, added via migration SQL.

---

## Phase 4B: Analytics Expansion

### Layer 1: Advanced Metrics (EPA Foundation)

**Core EPA calculation** from play-by-play:

- Expected Points Added per play (using down, distance, field position)
- Success rate (% of plays gaining positive EPA)
- Explosiveness (EPA on successful plays only)

**Marts to build:**

| Mart | Granularity | Key Columns |
|------|-------------|-------------|
| `play_epa` | Per play | game_id, play_id, offense, defense, epa, success, explosive |
| `team_game_epa` | Team × Game | team, game_id, total_epa, epa_per_play, success_rate, explosiveness |
| `team_season_epa` | Team × Season | team, season, total_epa, epa_per_play, success_rate, explosiveness |

### Layer 2: Situational Splits

Break down metrics by game situation:

| Split | Dimensions |
|-------|-----------|
| **Down** | 1st, 2nd, 3rd, 4th |
| **Distance** | Short (1-3), Medium (4-7), Long (8+) |
| **Field position** | Own territory, midfield, opponent territory, red zone |
| **Quarter** | Q1, Q2, Q3, Q4, OT |
| **Score differential** | Trailing, tied, leading (by margin buckets) |

**Mart:** `team_situational_epa` — Team × Season × Situation Type × Situation Value

### Layer 3: Player Attribution

Attribute EPA to individual players from play-by-play:

| Mart | Description |
|------|-------------|
| `player_game_epa` | Player EPA contribution per game (rushing, passing, receiving) |
| `player_season_epa` | Aggregated season totals with usage metrics |

---

## Historical Comparisons & Era Analysis

### Era Definitions

| Era | Years | Defining Characteristics |
|-----|-------|-------------------------|
| **BCS** | 2004-2013 | Bowl Championship Series, pre-playoff |
| **Playoff V1** | 2014-2023 | 4-team playoff, conference championship emphasis |
| **Portal/NIL** | 2021+ | Transfer portal explosion, NIL deals reshape rosters |
| **Playoff V2** | 2024+ | 12-team playoff, expanded access |

Note: Eras overlap (Portal/NIL is a subset of Playoff V1/V2) — queries can filter by multiple dimensions.

### Historical Trend Marts

| Mart | Purpose | Key Columns |
|------|---------|-------------|
| `team_season_trajectory` | Year-over-year team performance | team, season, era, epa_rank, recruiting_rank, win_pct, yoy_delta |
| `conference_era_summary` | Conference strength by era | conference, era, avg_epa, avg_recruiting, playoff_appearances |
| `coach_era_impact` | Coaching performance across eras | coach, team, era, seasons, win_pct, epa_vs_conf_avg |

### Query Pattern

All historical marts accept year range parameters:

```sql
-- Compare Texas across eras
SELECT * FROM team_season_trajectory
WHERE team = 'Texas'
  AND season BETWEEN 2004 AND 2025;

-- Conference strength in Portal era
SELECT * FROM conference_era_summary
WHERE era = 'Portal/NIL';
```

---

## Matchup Intelligence

### Historical Lookup

| Mart | Purpose | Key Columns |
|------|---------|-------------|
| `rivalry_history` | Head-to-head all-time records | team_a, team_b, games_played, wins_a, wins_b, ties, last_meeting |
| `rivalry_game_log` | Individual game results | team_a, team_b, season, score_a, score_b, venue, margin |
| `rivalry_splits` | Home/away/neutral breakdowns | team_a, team_b, venue_type, record, avg_margin |

### Predictive Features

Style matchup indicators derived from current season metrics:

| Mart | Purpose | Key Columns |
|------|---------|-------------|
| `team_style_profile` | Offensive/defensive identity | team, season, run_rate, pass_rate, tempo, epa_rushing, epa_passing, def_epa_vs_run, def_epa_vs_pass |
| `matchup_edges` | Style advantage indicators | team_a, team_b, season, rush_edge, pass_edge, tempo_mismatch, projected_margin |

### Matchup Dossier View

Combine into a single query-ready view:

```sql
-- Full dossier for Texas vs Oklahoma 2025
SELECT
  h.games_played, h.wins_a, h.wins_b,
  s.last_5_results,
  m.rush_edge, m.pass_edge, m.tempo_mismatch
FROM rivalry_history h
JOIN rivalry_splits s ON ...
JOIN matchup_edges m ON ...
WHERE team_a = 'Texas' AND team_b = 'Oklahoma';
```

---

## Build Order & Dependencies

```
4A (PK + indexes)
    ↓
EPA foundation (play_epa → team aggregations)
    ↓
Situational splits ←──┬──→ Player attribution
                      ↓
              Historical trends
                      ↓
             Matchup intelligence
```

| Phase | Deliverables | Dependencies |
|-------|--------------|--------------|
| **4A: Quality** | 5 PK fixes, ~12 indexes | None — do first |
| **4B: EPA Foundation** | play_epa, team_game_epa, team_season_epa | 4A complete, plays table indexed |
| **4B: Situational** | team_situational_epa | EPA foundation marts |
| **4B: Player Attribution** | player_game_epa, player_season_epa | EPA foundation marts |
| **4B: Historical** | team_season_trajectory, conference_era_summary, coach_era_impact, era reference | EPA + situational marts |
| **4B: Matchups** | rivalry_history, rivalry_game_log, rivalry_splits, team_style_profile, matchup_edges | All above |

---

## Summary

**Estimated marts:** 12-14 new materialized views
**Estimated indexes:** 10-12 business indexes
**Approach:** TDD, frequent commits, analytics-driven decisions
