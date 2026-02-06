# CFB Database — Pipeline Manifest

> Single source of truth for all endpoint-to-pipeline mappings.
> Sprint 3 — Updated after endpoint implementation completion.

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
| core | game_team_stats | 21,044 | ~3 MB |
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

**Total**: ~4.1M rows, ~1.7 GB (game_team_stats: 21K rows added)

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
| 8 | `/games/media` | core.game_media | games.py | game_media_resource | YES | merge | id | 2000-2026 | WORKING |
| 9 | `/games/teams` | core.game_team_stats | game_stats.py | game_team_stats_resource | YES | merge | id | 2004-2026 | WORKING |
| 10 | `/games/players` | core.game_player_stats | game_stats.py | game_player_stats_resource | YES | merge | id | 2004-2026 | DEFERRED |
| 11 | `/games/weather` | core.game_weather | games.py | game_weather_resource | YES | merge | id | 2000-2026 | WORKING |
| 12 | `/game/box/advanced` | stats.advanced_game_stats | stats.py | advanced_game_stats_resource | YES | merge | game_id, team | 2014-2026 | WORKING |
| 13 | `/calendar` | ref.calendar | reference.py | calendar_resource | YES | replace | season, week | current | WORKING |
| 14 | `/records` | core.records | games.py | records_resource | YES | merge | year, team | 2000-2026 | WORKING |
| 15 | `/scoreboard` | — | — | — | — | — | — | — | UNMAPPED |

### Play-by-Play Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 16 | `/plays` | core.plays | plays.py | plays_resource | YES | merge | id | 2004-2026 | WORKING |
| 17 | `/plays/stats` | stats.play_stats | stats.py | play_stats_resource | YES | merge | game_id, play_id, athlete_id, stat_type | 2014-2026 | WORKING |
| 18 | `/plays/stats/types` | — | — | — | — | — | — | — | UNMAPPED |
| 19 | `/live/plays` | — | — | — | — | — | — | — | UNMAPPED |

### Stats Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 20 | `/stats/season` | stats.team_season_stats | stats.py | team_season_stats_resource | YES | merge | season, team, stat_name | 2004-2026 | WORKING |
| 21 | `/stats/player/season` | stats.player_season_stats | stats.py | player_season_stats_resource | YES | merge | **player_id, season, category** (WRONG — needs stat_type too) | 2004-2026 | WORKING (PK bug) |
| 22 | `/stats/season/advanced` | stats.advanced_team_stats | stats.py | advanced_team_stats_resource | YES | merge | season, team | 2004-2026 | WORKING |
| 23 | `/stats/game/advanced` | — | — | — | — | — | — | — | UNMAPPED |
| 24 | `/stats/game/havoc` | stats.game_havoc | stats.py | game_havoc_resource | YES | merge | game_id, team | 2014-2026 | WORKING |
| 25 | `/stats/categories` | ref.stat_categories | reference.py | stat_categories_resource | YES | replace | name | — | WORKING |

### Ratings Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 26 | `/ratings/sp` | ratings.sp_ratings | ratings.py | sp_ratings_resource | YES | merge | year, team | 2015-2026 | WORKING |
| 27 | `/ratings/elo` | ratings.elo_ratings | ratings.py | elo_ratings_resource | YES | merge | year, team | 2015-2026 | WORKING |
| 28 | `/ratings/fpi` | ratings.fpi_ratings | ratings.py | fpi_ratings_resource | YES | merge | year, team | 2015-2026 | WORKING |
| 29 | `/ratings/srs` | ratings.srs_ratings | ratings.py | srs_ratings_resource | YES | merge | year, team | 2015-2026 | WORKING |
| 30 | `/ratings/sp/conferences` | ratings.sp_conference_ratings | ratings.py | sp_conference_ratings_resource | YES | merge | year, conference | 2015-2026 | WORKING |

### Recruiting Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 31 | `/recruiting/players` | recruiting.recruits | recruiting.py | recruits_resource | YES | merge | id | 2000-2026 | WORKING |
| 32 | `/recruiting/teams` | recruiting.team_recruiting | recruiting.py | team_recruiting_resource | YES | merge | year, team | 2000-2026 | WORKING |
| 33 | `/player/portal` | recruiting.transfer_portal | recruiting.py | transfer_portal_resource | YES | merge | **season, first_name, last_name** (PK mismatch in config) | 2000-2026 | WORKING (PK bug) |
| 34 | `/recruiting/groups` | recruiting.recruiting_groups | recruiting.py | recruiting_groups_resource | YES | merge | year, team, position_group | 2000-2026 | WORKING |

### Player Data

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 35 | `/player/search` | — | — | — | — | — | — | — | REMOVED (requires searchTerm; use core.rosters instead) |
| 36 | `/player/usage` | stats.player_usage | stats.py | player_usage_resource | YES | merge | season, id | 2014-2026 | WORKING |
| 37 | `/player/returning` | stats.player_returning | stats.py | player_returning_resource | YES | merge | season, team | 2014-2026 | WORKING |

### Betting Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 38 | `/lines` | betting.lines | betting.py | lines_resource | YES | merge | **game_id, provider** (config says id — mismatch) | 2013-2026 | WORKING (PK bug) |

### Draft Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 39 | `/draft/picks` | draft.draft_picks | draft.py | draft_picks_resource | YES | merge | **year, overall** (config says college_athlete_id — mismatch) | 2000-2026 | WORKING (PK bug) |
| 40 | `/draft/positions` | ref.draft_positions | reference.py | draft_positions_resource | YES | replace | name | — | WORKING |
| 41 | `/draft/teams` | ref.draft_teams | reference.py | draft_teams_resource | YES | replace | location, nickname | — | WORKING |

### Metrics Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 42 | `/ppa/teams` | metrics.ppa_teams | metrics.py | ppa_teams_resource | YES | merge | season, team | 2014-2026 | WORKING |
| 43 | `/ppa/players/season` | metrics.ppa_players_season | metrics.py | ppa_players_season_resource | YES | merge | season, id | 2014-2026 | WORKING |
| 44 | `/metrics/wp/pregame` | metrics.pregame_win_probability | metrics.py | pregame_wp_resource | YES | merge | season, game_id | 2014-2026 | WORKING |
| 45 | `/ppa/games` | metrics.ppa_games | metrics.py | ppa_games_resource | YES | merge | game_id, team | 2014-2026 | WORKING |
| 46 | `/ppa/players/games` | metrics.ppa_players_games | metrics.py | ppa_players_games_resource | YES | merge | id | 2014-2026 | WORKING |
| 47 | `/metrics/wp` | — | — | — | NO | merge | play_id | 2014-2026 | DEFERRED |
| 48 | `/ppa/predicted` | — | — | — | — | — | down, distance, yard_line | — | DEFERRED |
| 49 | `/metrics/fg/ep` | metrics.fg_expected_points | metrics.py | fg_expected_points_resource | YES | merge | distance | — | WORKING |

### Rankings

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 50 | `/rankings` | core.rankings | rankings.py | rankings_resource | YES | merge | season, week, poll, rank | 2000-2026 | WORKING |

### Teams Extended

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 51 | `/teams/fbs` | ref.teams_fbs | reference.py | teams_fbs_resource | YES | replace | id | — | WORKING |
| 52 | `/teams/matchup` | core.team_matchups | — | — | — | DEFERRED | team1, team2, season | — | Computed from games via matchup_history mart |
| 53 | `/teams/ats` | betting.team_ats | betting.py | team_ats_resource | YES | merge | year, team_id | 2013-2026 | WORKING |
| 54 | `/roster` | core.rosters | rosters.py | rosters_resource | YES | merge | id, team, year | 2004-2026 | WORKING (requires team list) |
| 55 | `/talent` | recruiting.team_talent | recruiting.py | team_talent_resource | YES | merge | year, school | 2000-2026 | WORKING |

### Adjusted Metrics (WEPA)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 56 | `/wepa/players/passing` | metrics.wepa_players_passing | wepa.py | wepa_players_passing_resource | YES | merge | id, year | 2014-2026 | WORKING |
| 57 | `/wepa/players/rushing` | metrics.wepa_players_rushing | wepa.py | wepa_players_rushing_resource | YES | merge | id, year | 2014-2026 | WORKING |
| 58 | `/wepa/team/season` | metrics.wepa_team_season | wepa.py | wepa_team_season_resource | YES | merge | year, team | 2014-2026 | WORKING |
| 59 | `/wepa/players/kicking` | metrics.wepa_players_kicking | wepa.py | wepa_players_kicking_resource | YES | merge | id, year | 2014-2026 | WORKING |

---

## Summary

| Status | Count |
|---|---|
| WORKING | 45 |
| WORKING (PK bug) | 5 |
| WORKING (note) | 1 |
| CONFIG_ONLY | 0 |
| DEFERRED | 4 |
| UNMAPPED | 4 |
| REMOVED | 1 |
| **Total** | **60** |

**Sprint 4 Progress:** Promoted 15 endpoints from UNMAPPED/CONFIG_ONLY to WORKING: `/game/box/advanced`, `/plays/stats`, `/stats/season/advanced`, `/stats/game/havoc`, `/ratings/sp/conferences`, `/player/usage`, `/player/returning`, `/teams/ats`, `/ppa/games`, `/ppa/players/games`, `/metrics/fg/ep`, `/wepa/players/passing`, `/wepa/players/rushing`, `/wepa/team/season`, `/wepa/players/kicking`. Removed `/player/search` (requires searchTerm; use core.rosters instead). Deleted dead code: `adjusted_metrics.py` (duplicate of `wepa.py`), `players.py` (broken source).

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

### `/games/players` (Player Box Scores) — DEFERRED

**Investigation Date:** 2026-01-29

**Findings:**
- Endpoint works correctly and returns deeply nested player stats per game
- Structure: game → teams → categories → types → athletes
- Data includes passing, rushing, receiving, defense stats per player per game
- Isolated source created in `game_stats.py` with `--batch-size` and `--replace` CLI options

**Why Deferred:**
Supabase statement timeout (~120s) is too aggressive for the merge/upsert SQL generated by dlt. Even single-year loads of ~50K player-game records time out. Attempted solutions:
1. Batch by year (1 year at a time) — still timed out
2. Replace disposition instead of merge — still timed out
3. Replace + append batching — connection issues

The fundamental problem is Supabase's statement timeout limit, not data volume.

**Workaround:**
Player game stats can be derived by aggregating from `core.plays` (play-by-play data), which is fully loaded. This requires more complex SQL but achieves the same analytics.

**Recommendation:**
For Sprint 4, investigate:
1. Supabase Pro tier with adjustable statement_timeout
2. Loading to local Postgres, then syncing to Supabase
3. Using dlt's file-based staging to break up inserts
