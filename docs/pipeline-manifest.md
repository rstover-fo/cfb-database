# CFB Database — Pipeline Manifest

> Single source of truth for all endpoint-to-pipeline mappings.
> Sprint 3 — Updated after endpoint implementation completion.

## Status Legend

- **WORKING**: Configured + implemented + wired in source return + CLI registered
- **CONFIG_ONLY**: Endpoint config exists, resource function may exist, but NOT returned from source
- **DEFERRED**: Investigated; requires non-standard iteration pattern (per-game or parameter combinations); low priority
- **PENDING_DEPLOY**: Implemented, unit-tested, and CLI-registered, but not yet loaded against the live
  database (source, migration, and view are authored on a branch awaiting a deploy sequence). See
  `deploys/p32-backfill-manifests.md` for the pending sequence and `docs/pipeline-manifest.md`'s
  investigation-notes Resolution entry for the item currently in this state.
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
| 4 | `/coaches` | ref.coaches | reference.py | coaches_resource | YES | merge | first_name, last_name | WORKING |
| 5 | `/plays/types` | ref.play_types | reference.py | play_types_resource | YES | replace | id | WORKING |

### Core Game Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 6 | `/games` | core.games | games.py | games_resource | YES | merge | id | 2000-2026 | WORKING |
| 7 | `/drives` | core.drives | games.py | drives_resource | YES | merge | id | 2000-2026 | WORKING |
| 8 | `/games/media` | core.game_media | games.py | game_media_resource | YES | merge | id | 2000-2026 | WORKING |
| 9 | `/games/teams` | core.game_team_stats | game_stats.py | game_team_stats_resource | YES | merge | id | 2004-2026 | WORKING |
| 10 | `/games/players` | core.game_player_stats | game_stats.py | game_player_stats_resource | YES | merge | id | 2004-2026 | WORKING |
| 11 | `/games/weather` | core.game_weather | games.py | game_weather_resource | YES | merge | id | 2000-2026 | WORKING |
| 12 | `/game/box/advanced` | stats.advanced_game_stats | stats.py | advanced_game_stats_resource | NO (game-id backfill only) | merge | game_id, team | 2014-2026 | DEFERRED (see 2026-08-29 note -- CFBD dropped `year`; use `/stats/game/advanced`, row 23, for year-scoped needs) |
| 13 | `/calendar` | ref.calendar | reference.py | calendar_resource | YES | replace | season, week | current | WORKING |
| 14 | `/records` | core.records | games.py | records_resource | YES | merge | year, team | 2000-2026 | WORKING |
| 15 | `/scoreboard` | — | — | — | — | — | — | — | UNMAPPED |

### Play-by-Play Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 16 | `/plays` | core.plays | plays.py | plays_resource | YES | merge | id | 2004-2026 | WORKING |
| 17 | `/plays/stats` | stats.play_stats | stats.py | play_stats_resource | YES | merge | game_id, play_id, athlete_id, stat_type | 2014-2026 | WORKING |
| 18 | `/plays/stats/types` | ref.play_stat_types | reference.py | play_stat_types_resource | YES | merge | id | — | WORKING |
| 19 | `/live/plays` | — | — | — | — | — | — | — | UNMAPPED |

### Stats Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 20 | `/stats/season` | stats.team_season_stats | stats.py | team_season_stats_resource | YES | merge | season, team, stat_name | 2004-2026 | WORKING |
| 21 | `/stats/player/season` | stats.player_season_stats | stats.py | player_season_stats_resource | YES | merge | player_id, season, team, category, stat_type | 2004-2026 | WORKING |
| 22 | `/stats/season/advanced` | stats.advanced_team_stats | stats.py | advanced_team_stats_resource | YES | merge | season, team | 2004-2026 | WORKING |
| 23 | `/stats/game/advanced` | stats.game_advanced_team_stats | stats.py | game_advanced_resource | YES | merge | game_id, team | 2014-2026 | WORKING (not yet backfilled) |
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
| 30a | `/ratings/core` | ratings.core_ratings | ratings.py | core_ratings_resource | YES | merge | year, team | 2016-2026 | WORKING |
| 30b | `/ratings/srs/expanded` | ratings.srs_expanded | ratings.py | srs_expanded_ratings_resource | YES | merge | year, team | 2005-2026 | WORKING (not yet backfilled) |

### Recruiting Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 31 | `/recruiting/players` | recruiting.recruits | recruiting.py | recruits_resource | YES | merge | id | 2000-2026 | WORKING |
| 32 | `/recruiting/teams` | recruiting.team_recruiting | recruiting.py | team_recruiting_resource | YES | merge | year, team | 2000-2026 | WORKING |
| 33 | `/player/portal` | recruiting.transfer_portal | recruiting.py | transfer_portal_resource | YES | merge | first_name, last_name, origin, season | 2000-2026 | WORKING |
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
| 38 | `/lines` | betting.lines | betting.py | lines_resource | YES | merge | game_id, provider | 2013-2026 | WORKING |

### Draft Data (merge disposition)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 39 | `/draft/picks` | draft.draft_picks | draft.py | draft_picks_resource | YES | merge | year, overall | 2000-2026 | WORKING |
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
| 47 | `/metrics/wp` | metrics.win_probability | metrics.py | win_probability_resource (via metrics_wp_source) | YES | merge | game_id, play_id | 2014-2026 | WORKING (1,971,363 rows) |
| 48 | `/ppa/predicted` | metrics.ppa_predicted | metrics.py | ppa_predicted_resource (via metrics_ppa_predicted_source, opt-in -- `--source metrics_ppa_predicted`) | YES | merge | down, distance, yard_line | — | WORKING (loaded 2026-08-30 via backfill-sources.yml `runner=pipeline_run` -- 10,140 rows) |
| 49 | `/metrics/fg/ep` | metrics.fg_expected_points | metrics.py | fg_expected_points_resource | YES | merge | distance | — | WORKING |

### Rankings

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 50 | `/rankings` | core.rankings | rankings.py | rankings_resource | YES | merge | season, season_type, week, poll, school | 2000-2026 | WORKING |

### Teams Extended

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 51 | `/teams/fbs` | ref.teams_fbs | reference.py | teams_fbs_resource | YES | replace | id | — | WORKING |
| 52 | `/teams/matchup` | core.team_matchups | — | — | — | DEFERRED | team1, team2, season | — | Computed from games via matchup_history mart |
| 53 | `/teams/ats` | betting.team_ats | betting.py | team_ats_resource | YES | merge | year, team_id | 2013-2026 | WORKING |
| 54 | `/roster` | core.rosters | rosters.py | rosters_resource | YES | merge | id, team, year | 2004-2026 | WORKING (requires team list) |
| 55 | `/talent` | recruiting.team_talent | recruiting.py | team_talent_resource | YES | merge | year, team | 2000-2026 | WORKING |

### Adjusted Metrics (WEPA)

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 56 | `/wepa/players/passing` | metrics.wepa_players_passing | wepa.py | wepa_players_passing_resource | YES | merge | id, year | 2014-2026 | WORKING |
| 57 | `/wepa/players/rushing` | metrics.wepa_players_rushing | wepa.py | wepa_players_rushing_resource | YES | merge | id, year | 2014-2026 | WORKING |
| 58 | `/wepa/team/season` | metrics.wepa_team_season | wepa.py | wepa_team_season_resource | YES | merge | year, team | 2014-2026 | WORKING |
| 59 | `/wepa/players/kicking` | metrics.wepa_players_kicking | wepa.py | wepa_players_kicking_resource | YES | merge | id, year | 2014-2026 | WORKING |

### Expansion Endpoints (A2 unit, 2026-08-29)

12 endpoints new to the 74-count regeneration are now rowed here. The last two -- rows
69-70, `/coaches/profile` and `/player/season/overview` (A4 unit, 2026-08-29) -- are
per-entity fan-out drainers wired as a bounded backlog-draining slice per run (a cap, not
a one-time backfill), unlike the year-fetch-all shape of the other ten; see each row's
note and the summary note below.

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 60 | `/playoffs/cfp` | core.cfp_bracket | playoffs.py | cfp_bracket_resource | YES | merge | season | 2014-2026 | WORKING (not yet backfilled) |
| 61 | `/playoffs/cfp/games` | core.cfp_games | playoffs.py | cfp_games_resource | YES | merge | season, id | 2014-2026 | WORKING (not yet backfilled) |
| 62 | `/playoffs/cfp/participants` | core.cfp_participants | playoffs.py | cfp_participants_resource | YES | merge | season, team\_\_id | 2014-2026 | WORKING (not yet backfilled) |
| 63 | `/coaches/seasons` | ref.coach_seasons | coaches.py | coach_seasons_resource | YES | merge | coach\_\_id, year, team\_\_id | 2000-2026 | WORKING (not yet backfilled -- every probe call 400'd; field names are from the OpenAPI spec, unverified live) |
| 64 | `/coaches/tenures` | ref.coach_tenures | coaches.py | coach_tenures_resource | YES (backfill/preseason only -- `--source coach_tenures`, not in load_season.py's SOURCE_ORDER) | merge | id | current | WORKING (loaded 2026-08-30 via backfill-sources.yml `runner=pipeline_run` -- 2,738 rows) |
| 65 | `/conferences/affiliations` | ref.conference_affiliations | conferences.py | conference_affiliations_resource | YES | merge | team_id, conference_id, start_year | all (bulk) | WORKING (not yet backfilled) |
| 66 | `/conferences/changes` | ref.conference_changes | conferences.py | conference_changes_resource | YES | merge | effective_year, team_id | 2000-2026 | WORKING (not yet backfilled) |

See row 30b for `/ratings/srs/expanded` (ratings.srs_expanded) and rows 67-68 below for
the two `/stats/player/success*` endpoints -- kept with their thematic groups above rather
than renumbered into this section.

| 67 | `/stats/player/success` | stats.player_success_season | stats.py | player_success_season_resource | YES | merge | season, id, team | 2014-2026 | WORKING (not yet backfilled) |
| 68 | `/stats/player/success/game` | stats.player_success_game | stats.py | player_success_game_resource | YES | merge | game_id, id | 2014-2026 | WORKING (not yet backfilled) |

Rows 69-70 (A4 unit, 2026-08-29) are the two per-entity fan-out drainers left PENDING by
the A2 unit above -- see the summary note for why they needed a targeted loader rather
than a year-fetch-all.

| 69 | `/coaches/profile` | ref.coach_profiles | coaches.py | coach_profiles_resource | YES (drainer -- `run.py::run_coach_profiles_pipeline`, in `SOURCE_ORDER`, capped at 200 coach ids/run) | merge | id | current | WORKING (drainer live -- 200 rows after its first capped run, draining daily) |
| 70 | `/player/season/overview` | stats.player_season_overview | player_overview.py | player_season_overview_resource | YES (drainer -- `run.py::run_player_overview_pipeline`, in `SOURCE_ORDER`, capped at 250 player-seasons/run) | merge | season, id, team | finished seasons only (completed-season gate) | WORKING (drain complete 2026-09-02 -- 47,396 rows, seasons 2013-2025; 4 2014 player-seasons held in `meta.fanout_misses` as network-fault/502 skips, self-heal after 30 days) |

### Passing (spec v5.25.0, 2026-08-30)

Five endpoints new to the 79-count regeneration: air yards, average depth of target
(aDOT), pass depth/direction/location, and yards after catch (YAC), all manually
charted from play film rather than derived from play-by-play parsing. Data starts
2025 (`PASSING_DATA_START` in passing.py -- 2024/2014 both probed 200-with-zero-rows,
2025 is fully populated). The three game-grain endpoints (rows 71-73) require `week`
or `team`/`passerId` -- a bare year 400s, understating what the OpenAPI spec's
required-params list claims -- so each walks weeks like `player_success_game_resource`
(regular 1-16, postseason 1-4). The two season-grain endpoints (rows 74-75) take a
bare year. Deliberately its own module/source (`cfbd_passing`), not folded into
`stats.py` -- see passing.py's module docstring for why (stats.py's sibling-failure
blast radius must not grow by five more resources).

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 71 | `/passing/plays` | stats.passing_plays | passing.py | passing_plays_resource | YES | merge | game_id, play_id | 2025-2026 | WORKING (2025 backfilled 2026-08-30; weekly 2025 re-pull cadence per `docs/handoffs/2026-09-01-charting-convergence-watch.md`) |
| 72 | `/passing/players/games` | stats.passing_player_games | passing.py | passing_player_games_resource | YES | merge | game_id, player_id | 2025-2026 | WORKING (2025 backfilled 2026-08-30; weekly 2025 re-pull cadence per `docs/handoffs/2026-09-01-charting-convergence-watch.md`) |
| 73 | `/passing/teams/games` | stats.passing_team_games | passing.py | passing_team_games_resource | YES | merge | game_id, team | 2025-2026 | WORKING (2025 backfilled 2026-08-30; weekly 2025 re-pull cadence per `docs/handoffs/2026-09-01-charting-convergence-watch.md`) |
| 74 | `/passing/players/season` | stats.passing_player_season | passing.py | passing_player_season_resource | YES | merge | season, player_id, team | 2025-2026 | WORKING (2025 backfilled 2026-08-30; weekly 2025 re-pull cadence per `docs/handoffs/2026-09-01-charting-convergence-watch.md`) |
| 75 | `/passing/teams/season` | stats.passing_team_season | passing.py | passing_team_season_resource | YES | merge | season, team | 2025-2026 | WORKING (2025 backfilled 2026-08-30; weekly 2025 re-pull cadence per `docs/handoffs/2026-09-01-charting-convergence-watch.md`) |

### Rushing (spec v5.26.0, 2026-09-03)

Five endpoints new to the 84-count regeneration: charted rushing with rusher
attribution, rush direction (left/middle/right/unknown), PPA, success rate, line /
second-level / open-field yards, stuff rate, power success, and explosiveness, with
per-direction splits on every aggregate row. Data starts 2025 (`RUSHING_DATA_START` in
rushing.py); CFBD announced 2025 as partially charted and 2026 as mostly full. Live
probe 2026-09-03 (workflow run 33765658317): bare `year` 400s on the three game-grain
endpoints ("team or week is required"), so rows 76-78 walk weeks exactly like passing;
2025 week 5 returned 3,816 plays (all `parseStatus=partial` in the sample) and 2026
week 1 returned 529 (`complete`); the season-grain endpoints (rows 79-80) take a bare
year and returned 1,622 player-seasons and 136 team-seasons for 2025. Semantics that
differ from passing: player totals include only individually attributed rushes and
never sum to team totals (team totals add sacks, kneels, team-only, and unresolved
attempts); `parse_status='invalid'` is its own bucket, never folded into `partial`;
coverage denominators are `rushing_yards_available`, `direction_eligible_attempts`,
`direction_available_attempts`, and `touchdown_status_available`. dlt flattens the
nested `directions` object to `directions__<dir>__<metric>` (team rows:
`offense__directions__<dir>__<metric>`). Own module/source (`cfbd_rushing`) for the
same blast-radius reason as passing.

| # | API Path | Table | Source File | Resource Function | Wired? | Disposition | Primary Key | Year Range | Status |
|---|---|---|---|---|---|---|---|---|---|
| 76 | `/rushing/plays` | stats.rushing_plays | rushing.py | rushing_plays_resource | YES | merge | game_id, play_id | 2025-2026 | WORKING (backfill pending) |
| 77 | `/rushing/players/games` | stats.rushing_player_games | rushing.py | rushing_player_games_resource | YES | merge | game_id, player_id | 2025-2026 | WORKING (backfill pending) |
| 78 | `/rushing/teams/games` | stats.rushing_team_games | rushing.py | rushing_team_games_resource | YES | merge | game_id, team | 2025-2026 | WORKING (backfill pending) |
| 79 | `/rushing/players/season` | stats.rushing_player_season | rushing.py | rushing_player_season_resource | YES | merge | season, player_id, team | 2025-2026 | WORKING (backfill pending) |
| 80 | `/rushing/teams/season` | stats.rushing_team_season | rushing.py | rushing_team_season_resource | YES | merge | season, team | 2025-2026 | WORKING (backfill pending) |

---

## Summary

| Status | Count |
|---|---|
| WORKING | 51 |
| WORKING (note) | 14 |
| CONFIG_ONLY | 0 |
| PENDING_DEPLOY | 0 |
| DEFERRED | 2 |
| UNMAPPED | 2 |
| REMOVED | 1 |
| **Total** | **70** |

**2026-07-19 update:** The 5 "WORKING (PK bug)" entries (coaches, player_season_stats,
transfer_portal, lines, draft_picks) were already fixed in the source modules — statuses
and PKs above now reflect the code. `/plays/stats/types` was loaded (26 rows in
`ref.play_stat_types`) but still marked UNMAPPED; corrected. Also as of this date,
`/games/teams` and `/games/players` load **only** via `game_stats_source`
(`games.py` no longer yields them), whose week-by-week path avoids Supabase merge
timeouts — `run.py --source game_stats --weekly` or `scripts/load_season.py --weekly`.

**Sprint 4 Progress:** Promoted 15 endpoints from UNMAPPED/CONFIG_ONLY to WORKING: `/game/box/advanced`, `/plays/stats`, `/stats/season/advanced`, `/stats/game/havoc`, `/ratings/sp/conferences`, `/player/usage`, `/player/returning`, `/teams/ats`, `/ppa/games`, `/ppa/players/games`, `/metrics/fg/ep`, `/wepa/players/passing`, `/wepa/players/rushing`, `/wepa/team/season`, `/wepa/players/kicking`. Removed `/player/search` (requires searchTerm; use core.rosters instead). Deleted dead code: `adjusted_metrics.py` (duplicate of `wepa.py`), `players.py` (broken source).

**2026-08-29 update:** `/metrics/wp` (row 47) verified against the live database
(`SELECT COUNT(*) FROM metrics.win_probability` -> 1,971,363) and promoted
PENDING_DEPLOY -> WORKING; see the dated addendum under its investigation note
below. `/ppa/predicted` (row 48) is now returned from `metrics_source`'s
resource list (src/pipelines/sources/metrics.py), so it graduates out of
DEFERRED, but `pg_attribute` shows no `metrics.ppa_predicted` table exists in
the live database -- the endpoint still 400s on the parameterless call the
resource makes, so it has never loaded a row. See its investigation note's
2026-08-29 addendum before treating this row as "data is loaded."

**2026-08-29 update (A2 unit, later same day):** Two repairs plus ten new
endpoints rowed.

- R1: `/game/box/advanced` (row 12) is CONFIRMED BROKEN as year-scoped --
  CFBD dropped the `year` query parameter; the live OpenAPI spec now requires
  a single `id` (game id) and every year-only call 400s. `advanced_game_stats_resource`
  (stats.py) was reworked to take explicit `game_ids` (mirroring
  `play_stats_resource`'s explicit-ids mode) and REMOVED from `stats_source`'s
  default return list -- it no longer burns a silent 400 per requested year.
  Demoted WORKING -> DEFERRED (row 12); kept importable for a future
  historical-refresh campaign. Year-scoped advanced game-team stats are now
  served by `/stats/game/advanced` (row 23, NEW), which still accepts `year`.
- R2: `/ppa/predicted` (row 48) -- see its investigation note's 2026-08-29
  (continued) addendum below. The down x distance fan-out (~120 calls) is now
  implemented, but `ppa_predicted_resource` was moved OUT of `metrics_source`'s
  default resource list into its own `metrics_ppa_predicted_source` (opt-in
  via `--source metrics_ppa_predicted`), mirroring how `win_probability` is
  split into `metrics_wp_source` -- a 120-call static-lookup fan-out has no
  place in a year-driven daily source. Still not yet backfilled.
- 10 endpoints new to the 74-count regeneration are now rowed: CFP bracket/
  games/participants (rows 60-62, playoffs.py), coach seasons/tenures (rows
  63-64, coaches.py), conference affiliations/changes (rows 65-66,
  conferences.py), expanded SRS ratings (row 30b, ratings.py), and player
  success season/game (rows 67-68, stats.py). All are WORKING (wired, tested,
  CLI-registered) but none have run against the live database yet -- see each
  row's "(not yet backfilled)" note. `coaches/seasons` in particular was
  never observed live during development (every probe call 400'd on a
  parameterless request); its column names come from the CFBD OpenAPI spec,
  not an inspected response -- verify via `pg_attribute` after the first load.

**Note**: The API reference now lists 84 endpoints (see `docs/cfbd-api-endpoints.md`) but some are variants of others (e.g., `/stats/season` vs `/stats/player/season` are listed as one "stats" category). This manifest counts distinct loadable endpoints. All 12 endpoints new to the 74-count regeneration (2026-08-29) are now rowed: 10 in the A2 unit above, plus `/coaches/profile` and `/player/season/overview` (rows 69-70, A4 unit, 2026-08-29) -- the two that were PENDING here. Both needed a targeted loader rather than a year-fetch-all: `/coaches/profile` (requires `coachId`, one call per coach) is drained by `run.py::run_coach_profiles_pipeline` from the coach ids seen in `ref.coach_seasons`, and `/player/season/overview` (requires `year` + `playerId`, one call per player per season -- a large, unbounded fan-out) is drained by `run.py::run_player_overview_pipeline` from the player-seasons seen in `stats.player_usage`/`metrics.ppa_players_season`, gated to finished seasons only. Both are DB-set-difference drainers capped per run (200 and 250 respectively) and wired into `scripts/load_season.py`'s `SOURCE_ORDER`, not full backfills. The 5 endpoints new to the 79-count regeneration (spec v5.25.0, 2026-08-30) are the `/passing/*` charting group -- rows 71-75 above -- all year-fetch-all (three week-iterated, two bare-year), wired into `SOURCE_ORDER` as a single `passing` source. The 5 endpoints new to the 84-count regeneration (spec v5.26.0, 2026-09-03) are the `/rushing/*` charting group -- rows 76-80 above -- the same shape (three week-iterated, two bare-year), wired as a single `rushing` source.

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

**Resolution (P3.2 Lane B, pending deploy):**
Implemented the "targeted loader" this note recommended: `metrics_wp_source` /
`win_probability_resource` (src/pipelines/sources/metrics.py) now call
`/metrics/wp?gameId=<id>` once per game instead of the year-only query that
always 400'd, and the old year-driven `win_probability` resource was removed
from `metrics_source`'s return list (it was dead code -- see that module's
header comment). `src/pipelines/run.py::run_metrics_wp_pipeline` bounds the
call volume the original note worried about: it queries `core.games` for
completed games in the requested seasons still missing from
`metrics.win_probability`, so it only ever calls the API for games it
doesn't already have, not "every game in the database" on every run. Wired
into `scripts/load_season.py` (`SOURCE_ORDER`/`ESTIMATED_CALLS["metrics_wp"] = 70`)
so daily/weekly incremental loads pick up newly-completed games automatically
-- no workflow-file changes needed. Full 2014+ backfill is ~12,000 API calls
(one-time; see `deploys/p32-backfill-manifests.md`'s budget-math section),
comfortably inside the 125,000/month Tier 4 budget alongside the existing
~22K/month daily-load worst case. New table: `metrics.win_probability`
(indexed by `src/schemas/migrations/026_win_probability_indexes.sql`),
exposed as `api.game_win_probability`
(`src/schemas/api/033_game_win_probability.sql`). Status is PENDING_DEPLOY,
not WORKING, because none of this has run against the live database yet --
see `deploys/p32-backfill-manifests.md` for the exact deploy sequence
(probe -> three backfill manifests -> apply migration+view) and its "Open
assumptions" section for what the field-shape probe (`scripts/probe_metrics_wp.py`)
must still confirm (CFBD's WP model was reportedly rebuilt in 2025, so the
`playId`/`down`/`distance`/`yardLine` field names this note originally
recorded on 2026-01-29 are unverified against current live data).

**Resolution (2026-08-29):** `SELECT COUNT(*) FROM metrics.win_probability`
against the live database returns 1,971,363 -- the deploy sequence above has
run. Status promoted PENDING_DEPLOY -> WORKING (row 47).

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

**Resolution (2026-08-29):** `ppa_predicted_resource` is now returned from
`metrics_source`'s resource list and is CLI-registered via `--source
metrics`, so it no longer meets DEFERRED's "not returned from source"
bar and is promoted to WORKING (row 48) under this manifest's wiring
criterion. That is not the same as "data is loaded": the resource still
calls `/ppa/predicted` with no parameters and catches the 400 CFBD returns
for that call (same finding as 2026-01-29) -- confirmed against the live
database via `pg_attribute`, no `metrics.ppa_predicted` table exists, so the
resource has never produced a row. The down/distance fan-out this note
recommended (~120 calls) was never implemented.

**Resolution (2026-08-29, continued -- R2, same day):** The down/distance
fan-out is now implemented: `ppa_predicted_resource` walks
`PPA_PREDICTED_DOWNS` (1-4) x `PPA_PREDICTED_DISTANCES` (1-30) = 120 calls
to `/ppa/predicted?down=D&distance=X`, stamping `down`/`distance` onto every
row (CFBD's response for a given combination carries only `yardLine` and
`predictedPoints`, per the `PredictedPointsValue` OpenAPI schema -- it does
not echo the down/distance back). PK is `(down, distance, yard_line)`.

The resource is now correct but was moved OUT of `metrics_source`'s default
return list into its own `metrics_ppa_predicted_source`
(`src/pipelines/sources/metrics.py`), opt-in via `--source
metrics_ppa_predicted` (`run.py::run_metrics_ppa_predicted_pipeline`) --
mirroring exactly how `win_probability` is split into `metrics_wp_source`
above. Rationale: 120 calls building a static lookup table that doesn't
change with the season is fine once (or occasionally) but wasteful to repeat
on every daily `metrics` load, and unlike `metrics_wp` there is no
set-difference check to make repeating it cheap. Not part of
`scripts/load_season.py`'s `SOURCE_ORDER` for the same reason. Loaded
2026-08-30 via `backfill-sources.yml`'s new `runner=pipeline_run` input
(10,140 rows) -- `metrics.ppa_predicted` exists and
050_expansion_grants_indexes.sql's grants/comments apply.

### `/game/box/advanced` (Advanced Game Box Score) — DEFERRED

**Investigation Date:** 2026-08-29

**Findings:**
- `advanced_game_stats_resource` (stats.py) had iterated `?year=Y` since its
  original implementation and was marked WORKING (row 12) on that basis.
- Confirmed BROKEN as year-scoped: the live OpenAPI spec now requires a
  single `id` (game id) query parameter, not `year` -- every year-only call
  400s ("id required"). This was silent: the resource caught the 400,
  logged a warning, and continued, so a normal `stats` load appeared to
  succeed while burning one wasted call per requested year and loading zero
  rows.
- `/stats/game/advanced` (a different endpoint, row 23) still accepts a bare
  `year` and returns the same game-team grain (gameId, season, seasonType,
  week, team, opponent, offense{...}, defense{...}) -- confirmed via the
  2026-08-29 probe (3,112 rows for 2024 regular season).

**Why Deferred:**
Reworking the endpoint's contract (year -> id) rather than removing the
resource preserves it for a future one-off historical-refresh campaign, but
a per-game fan-out (one call per game, unbounded) has no place in a
year-driven default path -- the same reasoning `play_stats_resource` and
`metrics_wp_source` already document for their own per-game modes.

**Resolution (2026-08-29, same day -- R1):** `advanced_game_stats_resource`
now takes explicit `game_ids: list[int]` and calls `/game/box/advanced?id=<gameId>`
once per id, mirroring `play_stats_resource`'s explicit-ids branch. REMOVED
from `stats_source`'s default return list -- a normal stats load no longer
spends the wasted 400. Status demoted WORKING -> DEFERRED (row 12); the
function stays importable, game-id-driven, for a future historical-refresh
campaign that walks `core.games` and backfills id-by-id. Year-scoped
advanced game-team stats are now served by `game_advanced_resource`
(stats.game_advanced_team_stats, row 23, NEW this same unit).

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

**Resolution (2026-07-19):**
The table was successfully loaded via `game_stats_source`'s week-by-week loading path. The batching strategy (~35K rows per merge batch) effectively avoids Supabase statement timeouts. `core.game_player_stats` now holds ~6.4M rows across its athlete-level child tables and is actively consumed by `api.game_player_leaders` (src/schemas/api/010) and `api.game_box_score` (src/schemas/api/011). Status promoted to WORKING.

---

## Non-CFBD Sources (Flat-File)

| Source | Target Tables | Source Format | Cadence | Status |
|---|---|---|---|---|
| massey | ratings.massey_composite, ratings.massey_system_ratings | CSV (massey.org) | Weekly | Built (awaiting first load) |
| nflverse_combine | draft.combine | Parquet (nflverse) | Annual | Built (awaiting first load) |
| nflverse_draft | draft.nflverse_draft_picks | Parquet (nflverse) | Annual | Built (awaiting first load) |
| sbr | betting.sbr_historical | Excel (manual backfill) | Manual | Built (awaiting first load) |
| availability | raw.availability_reports | PDF archive (conf reports) | Weekly | Built (awaiting first load) |
| sdv_team_xwalk | ref.team_id_xwalk | Parquet (sportsdataverse-data, per-season) | Weekly | Built (awaiting first load) |
| sdv_game_xwalk | ref.game_id_xwalk | Parquet (sportsdataverse-data, per-season) | Weekly | Built (awaiting first load) |
| sdv_fpi_weekly | ratings.espn_fpi_weekly | Parquet (sportsdataverse-data, per-season) | Weekly | Built (awaiting first load) |
| sdv_ratings_weekly | ratings.sdv_ratings_weekly | Parquet (sportsdataverse-data, per-season) | Weekly | Built (awaiting first load) |
| ncaa_schedule | ncaa.schedule | Parquet (sportsdataverse-data ncaa_mfb_schedule, per-season) | Weekly | Built (awaiting first load) |
| ncaa_teams | ncaa.teams | Parquet (sportsdataverse-data ncaa_mfb_teams, per-season) | Annual | Built (awaiting first load) |
| ncaa_rosters | ncaa.rosters | Parquet (sportsdataverse-data ncaa_mfb_rosters, per-season) | Annual | Built (awaiting first load) |
| ncaa_linescores | ncaa.linescores | Parquet (sportsdataverse-data ncaa_mfb_linescore, per-season) | Weekly | Built (awaiting first load) |
| ncaa_player_stats | ncaa.player_stats | Parquet (sportsdataverse-data ncaa_mfb_player_stats, per-season) | Weekly | Built (awaiting first load) |
| ncaa_team_stats | ncaa.team_stats | Parquet (sportsdataverse-data ncaa_mfb_team_stats, per-season) | Weekly | Built (awaiting first load) |
| ncaa_pbp | ncaa.pbp | Parquet (sportsdataverse-data ncaa_mfb_pbp, per-season) | Weekly | Built (awaiting first load) |
| espn_player_passing | stats.espn_player_passing | Parquet (sportsdataverse-data espn_cfb_adv_passing, per-season) | Weekly | Built (awaiting first load) |
| espn_player_rushing | stats.espn_player_rushing | Parquet (sportsdataverse-data espn_cfb_adv_rushing, per-season) | Weekly | Built (awaiting first load) |
| espn_player_receiving | stats.espn_player_receiving | Parquet (sportsdataverse-data espn_cfb_adv_receiving, per-season) | Weekly | Built (awaiting first load) |
| espn_player_defense | stats.espn_player_defense | Parquet (sportsdataverse-data espn_cfb_adv_defensive_players, per-season) | Weekly | Built (awaiting first load) |
| espn_play_participants | stats.espn_play_participants | Parquet (sportsdataverse-data espn_cfb_play_participants, per-season) | Weekly | Built (awaiting first load) |

`ncaa.*` lives in its own, deliberately ungranted Postgres schema (migration
053): stats.ncaa.org ids (team/player/contest) are a disjoint id space from
CFBD/ESPN with no verified crosswalk, so the schema is reachable only via the
pipeline's service-role connection until a deliberate exposure decision.

`espn_*` (B6b) lives in the existing, already-granted `stats` schema (migration
055) -- ESPN's numeric ids are verified equal to CFBD's, so provenance is
carried by the `espn_` table-name prefix, not schema isolation. Notable gaps,
verified live rather than assumed: none of `espn_player_passing`/`_rushing`/
`_receiving`/`_defense` carries an athlete id at all (player identity is a
free-text name column only -- ESPN's advanced-stat text scrape, a different
upstream path than the id-carrying `espn_play_participants`); none of the five
tables carries a `position` column; `espn_play_participants` carries no team
id/name column at all. `espn_pbp_2002_2003` (a proposed pre-CFBD 2002-03
play-by-play gap-fill) was dropped from this unit -- the real
`espn_cfb_pbp` release's minimum season is 2004 (verified via a live download:
`play_by_play_2004.parquet` succeeds, `play_by_play_2002/2003.parquet` both
404), so the dataset this table would have held does not exist upstream.

First deploy (one-time, requires DB credentials):

1. `python scripts/run_migrations.py --file src/schemas/migrations/041_flat_files.sql`
   (idempotent -- safe to re-run).
2. `python scripts/seed_team_xwalk.py --source massey --from-fixture` then review the
   emitted seed SQL (`-- REVIEW` / `-- UNMATCHED` lines) and apply it with
   `run_migrations.py --file`. Repeat with `--names-file` over the full Massey CSV team
   list once in season, and for `sbr` before any backfill. Unmapped names fail the load
   loudly by design -- extend the crosswalk when that happens.
3. Trigger `.github/workflows/flat-files.yml` via workflow_dispatch (source input
   `nflverse_combine nflverse_draft`) for the first nflverse load; Massey/availability
   load automatically in season via the daily 11:00 UTC run.
4. SBR backfill per season file: `python scripts/load_flat_files.py --source sbr --file
   <downloaded .xlsx>` (sportsbookreviewsonline now serves HTML tables; export/convert
   to .xlsx with the documented column layout, or load archived copies).

**Status (2026-08-30):** `massey` is seeded, from the live `compare.csv` -- 131/131
names mapped (90 exact, 24 abbrev/fuzzy auto, 17 manual corrections); the seed is
committed at `src/schemas/migrations/seed/team_name_xwalk_seed_massey.sql`. `sbr`
remains unseeded -- it has no fetchable source (`fetch_url=None`, manual source
only) and is pending a real Excel file from the user.

Notes: Massey no-ops (`status=no_op_offseason`) until its CSV rolls to the current
season (typically preseason); SEC/Big 12/CFP availability reports are served through a
JS-only widget and are recorded as gaps -- Big Ten archives now, the rest need a
headless-browser follow-up.

---

## Historical Refresh Campaigns (unit A3)

CFBD corrected historical data upstream (15k+ garbage-time reclassifications and
other cleanups). The per-game endpoints behind `stats.py`'s `play_stats_resource`
(`/plays/stats`) and `advanced_game_stats_resource` (`/game/box/advanced`, row 12
above) need re-fetching for ~2014-2025 completed games -- up to ~1,600
games/season x 12 seasons x up to 2 tasks, i.e. up to ~38,000 calls total. That is
far more than any single run should spend against the 125,000/month budget the
daily load also consumes, so `scripts/backfill_refresh.py` and
`.github/workflows/historical-refresh.yml` spread it across many budget-capped,
resumable runs instead of one backfill.

**Mechanism:**

- `src/schemas/migrations/051_refresh_ledger.sql` adds `meta.refresh_campaigns`
  (one row per named campaign: its seasons, its tasks, when it completed) and
  `meta.refresh_progress` (one row per `(campaign, task, game_id)` already
  re-fetched -- the resumability primitive).
- A run's backlog for a task = completed games (both scores non-null) in the
  campaign's seasons, newest season then newest week first, MINUS the game_ids
  already in `meta.refresh_progress` for that `(campaign, task)` -- the same
  set-difference shape `src/pipelines/run.py`'s `run_metrics_wp_pipeline` uses
  against `metrics.win_probability`.
- Cross-task order: `plays_stats` drains completely before `box_advanced` starts
  spending any of a run's `--max-calls` budget (it feeds the EPA/adjusted-EPA
  chain that the rest of the compute pipeline depends on daily; `box_advanced`
  does not yet have a downstream consumer).
- Two independent, non-fatal budget guards run before any call: the
  ledger-backed month guard (`SUM(calls)` in `meta.refresh_progress` since the
  start of the current month vs. `--monthly-cap`, checked before every batch --
  this is the one that actually holds on ephemeral CI runners), and the repo's
  local `RateLimiter` (advisory only in CI, since its JSON state does not
  survive an ephemeral runner -- meaningful only on a persistent local
  machine). Either tripping prints a message and exits 0: a pacing stop, not an
  error.
- Every game-id-driven write uses the exact same dlt pipeline identity
  (`pipeline_name="cfbd_stats"`, `dataset_name="stats"`) as
  `run_stats_pipeline`'s normal year-driven load, so corrected rows MERGE into
  the existing `stats.play_stats` / `stats.advanced_game_stats` tables rather
  than standing up a parallel dlt schema.
- A run that loads anything refreshes materialized views afterward
  (`scripts/refresh_marts.py`) -- corrected play stats feed the EPA chain.

**Running it:**

```bash
# One-time: create the campaign (idempotent)
python scripts/backfill_refresh.py --campaign 2026-08-upstream-corrections \
    --create --seasons 2014-2025 --tasks plays_stats,box_advanced

# Subsequent runs: drain up to --max-calls games (default 1000)
python scripts/backfill_refresh.py --campaign 2026-08-upstream-corrections

# Check progress without spending any calls
python scripts/backfill_refresh.py --campaign 2026-08-upstream-corrections --status
```

`.github/workflows/historical-refresh.yml` runs this daily at 12:00 UTC (sharing
the daily load's `daily-season-load` concurrency group, so the two queue rather
than race) and is also `workflow_dispatch`-able for a one-off `--create` or a
larger `--max-calls`. Once a campaign's backlog fully drains the script becomes a
permanent no-op ("already complete", exit 0) -- at that point disable or delete
the workflow's `schedule:` trigger; a future upstream correction should get its
own deliberately named campaign, not silently reuse a finished one's cron.

**Call-count guidance:** a scheduled run at the default `--max-calls 1000` costs
at most 1,000 calls/day (further bounded by `--monthly-cap`, default 30,000/month,
tracked independently of the daily load's own budget). A full one-time backfill
across both tasks for 2014-2025 is ~38,000 calls, draining in ~38 days at the
default per-run cap (fewer with a larger `--max-calls` on a manual dispatch).
