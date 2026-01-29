# CFB Database — Pipeline Manifest

> Single source of truth for all endpoint-to-pipeline mappings.
> Sprint 0.5 — Generated from code audit + API inspection.

## Status Legend

- **WORKING**: Configured + implemented + wired in source return + CLI registered
- **CONFIG_ONLY**: Endpoint config exists, resource function may exist, but NOT returned from source
- **DEFERRED**: Investigated; requires non-standard iteration pattern (per-game or parameter combinations); low priority
- **UNMAPPED**: No config or source implementation

## Database Summary

| Schema | Table | Rows | Size |
|---|---|---|---|
| ref | conferences | 106 | 64 kB |
| ref | teams | 1,899 | 424 kB |
| ref | venues | 837 | 232 kB |
| ref | coaches | 1,790 | 272 kB |
| ref | play_types | 49 | 32 kB |
| core | games | 18,650 | 5.7 MB |
| core | drives | 183,603 | 63 MB |
| core | plays | 3,611,707 | 1.5 GB |
| stats | team_season_stats | 49,819 | 8.4 MB |
| stats | player_season_stats | 131,268 | 22 MB |
| ratings | sp_ratings | 800 | 216 kB |
| ratings | elo_ratings | 791 | 176 kB |
| ratings | fpi_ratings | 791 | 240 kB |
| ratings | srs_ratings | 1,258 | 240 kB |
| recruiting | recruits | 16,086 | 4.5 MB |
| recruiting | team_recruiting | 1,184 | 224 kB |
| recruiting | transfer_portal | 14,356 | 2.7 MB |
| betting | lines | 20,192 | 4.7 MB |
| draft | draft_picks | 1,549 | 552 kB |
| metrics | ppa_teams | 792 | 288 kB |
| metrics | ppa_players_season | 24,475 | 7.0 MB |
| metrics | pregame_win_probability | 5,080 | 992 kB |

**Total**: ~4.1M rows, ~1.7 GB

## Variant Columns (__v_double)

| Table | Column | Type | Action |
|---|---|---|---|
| recruiting.recruits | height__v_double | double precision | Merge into `height`, drop variant |
| metrics.ppa_teams | defense__first_down__v_double | double precision | Merge into correct column, drop |
| metrics.pregame_win_probability | spread__v_double | double precision | Merge into `spread`, drop variant |

Only 3 actual variant columns in user data tables. dlt internal tables also have some but those are managed by dlt.

## Existing Indexes & Constraints

**Only dlt-managed indexes exist** — every table has a single `UNIQUE INDEX` on `_dlt_id`. No business indexes, no composite indexes, no foreign keys.

---

## Full Endpoint Manifest

### Reference Data (replace disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Status |
|---|---|---|---|---|---|---|---|---|
| 1 | `/conferences` | ref.conferences | reference.py | conferences_resource | YES | replace | id | WORKING |
| 2 | `/teams` | ref.teams | reference.py | teams_resource | YES | replace | id | WORKING |
| 3 | `/venues` | ref.venues | reference.py | venues_resource | YES | replace | id | WORKING |
| 4 | `/coaches` | ref.coaches | reference.py | coaches_resource | YES | replace | first_name, last_name | WORKING (PK bug in config) |
| 5 | `/plays/types` | ref.play_types | reference.py | play_types_resource | YES | replace | id | WORKING |

### Core Game Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 6 | `/games` | core.games | games.py | games_resource | YES | merge | id | 2000-2026 | WORKING |
| 7 | `/drives` | core.drives | games.py | drives_resource | YES | merge | id | 2000-2026 | WORKING |
| 8 | `/games/media` | core.game_media | games.py | game_media_resource | **NO** | merge | id | 2000-2026 | CONFIG_ONLY |
| 9 | `/games/teams` | core.game_team_stats | — | — | NO | merge | id | 2004-2026 | CONFIG_ONLY |
| 10 | `/games/players` | core.game_player_stats | — | — | NO | merge | id | 2004-2026 | CONFIG_ONLY |
| 11 | `/games/weather` | — | — | — | — | — | — | — | UNMAPPED |
| 12 | `/game/box/advanced` | — | — | — | — | — | — | — | UNMAPPED |
| 13 | `/calendar` | — | — | — | — | — | — | — | UNMAPPED |
| 14 | `/records` | — | — | — | — | — | — | — | UNMAPPED |
| 15 | `/scoreboard` | — | — | — | — | — | — | — | UNMAPPED |

### Play-by-Play Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 16 | `/plays` | core.plays | plays.py | plays_resource | YES | merge | id | 2004-2026 | WORKING |
| 17 | `/plays/stats` | — | — | — | — | — | — | — | UNMAPPED |
| 18 | `/plays/stats/types` | — | — | — | — | — | — | — | UNMAPPED |
| 19 | `/live/plays` | — | — | — | — | — | — | — | UNMAPPED |

### Stats Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 20 | `/stats/season` | stats.team_season_stats | stats.py | team_season_stats_resource | YES | merge | season, team, stat_name | 2004-2026 | WORKING |
| 21 | `/stats/player/season` | stats.player_season_stats | stats.py | player_season_stats_resource | YES | merge | **player_id, season, category** (WRONG — needs stat_type too) | 2004-2026 | WORKING (PK bug) |
| 22 | `/stats/season/advanced` | stats.advanced_team_stats | stats.py | advanced_team_stats_resource | **NO** | merge | season, team | 2004-2026 | CONFIG_ONLY |
| 23 | `/stats/game/advanced` | — | — | — | — | — | — | — | UNMAPPED |
| 24 | `/stats/game/havoc` | — | — | — | — | — | — | — | UNMAPPED |
| 25 | `/stats/categories` | — | — | — | — | — | — | — | UNMAPPED |

### Ratings Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 26 | `/ratings/sp` | ratings.sp_ratings | ratings.py | sp_ratings_resource | YES | merge | year, team | 2015-2026 | WORKING |
| 27 | `/ratings/elo` | ratings.elo_ratings | ratings.py | elo_ratings_resource | YES | merge | year, team | 2015-2026 | WORKING |
| 28 | `/ratings/fpi` | ratings.fpi_ratings | ratings.py | fpi_ratings_resource | YES | merge | year, team | 2015-2026 | WORKING |
| 29 | `/ratings/srs` | ratings.srs_ratings | ratings.py | srs_ratings_resource | YES | merge | year, team | 2015-2026 | WORKING |
| 30 | `/ratings/sp/conferences` | — | — | — | — | — | — | — | UNMAPPED |

### Recruiting Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 31 | `/recruiting/players` | recruiting.recruits | recruiting.py | recruits_resource | YES | merge | id | 2000-2026 | WORKING |
| 32 | `/recruiting/teams` | recruiting.team_recruiting | recruiting.py | team_recruiting_resource | YES | merge | year, team | 2000-2026 | WORKING |
| 33 | `/player/portal` | recruiting.transfer_portal | recruiting.py | transfer_portal_resource | YES | merge | **season, first_name, last_name** (PK mismatch in config) | 2000-2026 | WORKING (PK bug) |
| 34 | `/recruiting/groups` | — | — | — | — | — | — | — | UNMAPPED |

### Player Data

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 35 | `/player/search` | — | — | — | — | — | — | — | UNMAPPED |
| 36 | `/player/usage` | — | — | — | — | — | — | — | UNMAPPED |
| 37 | `/player/returning` | — | — | — | — | — | — | — | UNMAPPED |

### Betting Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 38 | `/lines` | betting.lines | betting.py | lines_resource | YES | merge | **game_id, provider** (config says id — mismatch) | 2013-2026 | WORKING (PK bug) |

### Draft Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 39 | `/draft/picks` | draft.draft_picks | draft.py | draft_picks_resource | YES | merge | **year, overall** (config says college_athlete_id — mismatch) | 2000-2026 | WORKING (PK bug) |
| 40 | `/draft/positions` | — | — | — | — | — | — | — | UNMAPPED |
| 41 | `/draft/teams` | — | — | — | — | — | — | — | UNMAPPED |

### Metrics Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 42 | `/ppa/teams` | metrics.ppa_teams | metrics.py | ppa_teams_resource | YES | merge | season, team | 2014-2026 | WORKING |
| 43 | `/ppa/players/season` | metrics.ppa_players_season | metrics.py | ppa_players_season_resource | YES | merge | season, id | 2014-2026 | WORKING |
| 44 | `/metrics/wp/pregame` | metrics.pregame_win_probability | metrics.py | pregame_wp_resource | YES | merge | season, game_id | 2014-2026 | WORKING |
| 45 | `/ppa/games` | — | — | — | NO | merge | game_id, team | 2014-2026 | CONFIG_ONLY |
| 46 | `/ppa/players/games` | — | — | — | NO | merge | id | 2014-2026 | CONFIG_ONLY |
| 47 | `/metrics/wp` | — | — | — | NO | merge | play_id | 2014-2026 | DEFERRED |
| 48 | `/ppa/predicted` | — | — | — | — | — | down, distance, yard_line | — | DEFERRED |
| 49 | `/metrics/fg/ep` | — | — | — | — | — | — | — | UNMAPPED |

### Rankings

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 50 | `/rankings` | — | — | — | — | — | — | — | UNMAPPED |

### Teams Extended

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 51 | `/teams/fbs` | — | — | — | — | — | — | — | UNMAPPED |
| 52 | `/teams/matchup` | core.team_matchups | — | — | — | DEFERRED | team1, team2, season | — | Computed from games via matchup_history mart |
| 53 | `/teams/ats` | — | — | — | — | — | — | — | UNMAPPED |
| 54 | `/roster` | — | — | — | — | — | — | — | UNMAPPED |
| 55 | `/talent` | — | — | — | — | — | — | — | UNMAPPED |

### Adjusted Metrics (WEPA)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 56 | `/wepa/players/passing` | — | — | — | — | — | — | — | UNMAPPED |
| 57 | `/wepa/players/rushing` | — | — | — | — | — | — | — | UNMAPPED |
| 58 | `/wepa/team/season` | — | — | — | — | — | — | — | UNMAPPED |
| 59 | `/wepa/players/kicking` | — | — | — | — | — | — | — | UNMAPPED |

---

## Summary

| Status | Count |
|---|---|
| WORKING | 22 |
| WORKING (PK bug) | 4 |
| CONFIG_ONLY | 4 |
| DEFERRED | 3 |
| UNMAPPED | 26 |
| **Total** | **59** |

**Note**: The API reference lists ~61 endpoints but some are variants of others (e.g., `/stats/season` vs `/stats/player/season` are listed as one "stats" category). This manifest counts distinct loadable endpoints.

---

## Endpoint Investigation Notes

### `/metrics/wp` (In-Game Win Probability) — DEFERRED

**Investigation Date:** 2026-01-29

**Findings:**
- Endpoint requires `gameId` parameter — year-only queries return 400
- Returns play-by-play win probability data for a single game
- Each record includes: `playId`, `playText`, `homeWinProbability`, `down`, `distance`, `yardLine`, etc.
- Example: `GET /metrics/wp?gameId=401628455` returns ~150+ records per game

**Why Deferred:**
The current year-based iteration pattern doesn't work for this endpoint. Loading all games would require:
1. First query all game IDs from the `games` table
2. Then iterate per-game to fetch win probability
3. With ~18,000+ games in the database, this would consume significant API quota

**Recommendation:**
Use `pregame_win_probability` (already working) for pre-game predictions. In-game win probability is low priority for analytics use cases. If needed later, implement a targeted loader for specific games of interest rather than full historical backfill.

### `/ppa/predicted` (Predicted Points Lookup) — DEFERRED

**Investigation Date:** 2026-01-29

**Findings:**
- Endpoint requires `down` and `distance` parameters — no-parameter queries return 400
- Returns expected points by yard line (1-90) for a given down/distance situation
- Example: `GET /ppa/predicted?down=1&distance=10` returns 90 records (one per yard line)
- Full dataset would be: 4 downs × ~30 distances × 90 yard lines = ~10,800 records (small)

**Why Deferred:**
While the total dataset is small (~10K records), the endpoint requires iterating over all down/distance combinations. This is low priority for current analytics needs.

**Recommendation:**
If needed, implement a simple nested loop over realistic down/distance combinations (down 1-4, distance 1-30) to build the complete lookup table. This could be done in a single pipeline run with ~120 API calls.

### `/teams/matchup` (Historical Matchups) — DEFERRED

**Investigation Date:** 2026-01-29

**Findings:**
- Endpoint requires `team1` and `team2` parameters
- Returns historical head-to-head records between two specific teams
- Would require iterating over all FBS team pairs (130 × 129 / 2 = 8,385 calls)

**Why Deferred:**
The `analytics.matchup_history` materialized view already computes head-to-head records directly from the games table. Loading the API endpoint would be redundant and consume significant API quota.

**Recommendation:**
Use the existing `matchup_history` mart for rivalry/matchup analysis. No API endpoint needed.
