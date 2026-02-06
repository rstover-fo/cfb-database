# Schema Contract

> Defines the public API surface of cfb-database. Downstream repos (cfb-app, cfb-scout) should
> only depend on objects listed here as **public**. Everything else is internal and may change
> without notice.

Last updated: 2026-02-06

---

## Contract Rules

1. **API views and RPCs are versioned.** Breaking changes require a migration and coordination
   with downstream consumers before deployment.
2. **Column additions are non-breaking.** Adding a column to a public view is safe.
3. **Column removals and renames are breaking.** These require a deprecation period or a new
   view version (e.g. `api.team_detail_v2`).
4. **Raw table access is internal.** Downstream repos must never query raw tables directly --
   use API views or RPCs instead.
5. **`supabase gen types` output is the canonical TypeScript definition.** Frontend repos
   regenerate types after any schema change.
6. **Materialized views in `marts.*` are public for read access** but their refresh schedule
   is internal. Do not assume real-time freshness.
7. **`analytics.*` materialized views are internal.** They may be promoted to public in the
   future but are not yet contracted.

---

## Consumer: cfb-app

cfb-app is the main analytics dashboard. It consumes the broadest surface area.

### API Views (schema: `api`)

These are the primary PostgREST-accessible views. Queries go through Supabase client libraries.

| View | Status | Rows | Description |
|------|--------|------|-------------|
| `api.team_detail` | **Deployed** | 136 | Single team page: current season stats, ratings, EPA. Columns: school, mascot, abbreviation, color, alternate_color, logo_url, conference, classification, current_season, games, wins, losses, conf_wins, conf_losses, ppg, opp_ppg, avg_margin, sp_rating, sp_rank, sp_offense, sp_defense, elo, fpi, epa_per_play, epa_tier, success_rate, explosiveness, recruiting_rank, recruiting_points |
| `api.team_history` | **Deployed** | 3,667 | Multi-season team history with records, ratings, EPA trends. Columns: team, season, conference, games, wins, losses, conf_wins, conf_losses, ppg, opp_ppg, avg_margin, sp_rating, sp_rank, elo, fpi, epa_per_play, epa_tier, success_rate, explosiveness, total_plays, recruiting_rank, recruiting_points |
| `api.game_detail` | **Deployed** | 45,897 | Single game: teams, scores, betting lines, EPA, venue. Columns: game_id, season, week, season_type, start_date, completed, home_team, away_team, home_points, away_points, winner, point_diff, home_spread, over_under, spread_result, ou_result, home_epa, away_epa, pregame_home_win_prob, venue, attendance, excitement_index |
| `api.matchup` | **Deployed** | 11,975 | Head-to-head matchup history and current season comparison. Columns: team1, team2, total_games, team1_wins, team2_wins, ties, first_meeting, last_meeting, recent_results (JSONB array), team1/team2 current season stats |
| `api.leaderboard_teams` | **Deployed** | 3,667 | Team leaderboard with rankings, ratings, EPA. Columns: team, conference, season, wins, losses, win_pct, ppg, opp_ppg, sp_rank, epa_per_play, epa_tier, wins_rank, ppg_rank, defense_ppg_rank, epa_rank |
| `api.roster_lookup` | **Deployed** | 340,855 | Stable roster view for player matching |
| `api.recruit_lookup` | **Deployed** | 67,179 | Stable recruiting view for recruit data |
| `api.player_season_leaders` | **Deployed** | 152,966 | Season stat leaders by category (passing, rushing, receiving, defensive). Columns: season, category, player_id, player_name, team, yards, touchdowns, interceptions, pct, attempts, completions, carries, yards_per_carry, receptions, yards_per_reception, longest, total_tackles, solo_tackles, sacks, tackles_for_loss, passes_defended, yards_rank |
| `api.player_detail` | **Deployed** | 340,878 | Single player page: bio, recruiting, season stats, PPA. Columns: player_id, name, team, position, season, height, weight, jersey, home_city, home_state, stars, recruit_rating, national_ranking, recruit_class, pass_att, pass_cmp, pass_yds, pass_td, pass_int, pass_pct, rush_car, rush_yds, rush_td, rush_ypc, rec, rec_yds, rec_td, rec_ypr, tackles, sacks, tfl, pass_def, ppa_avg, ppa_total |
| `api.player_comparison` | **Deployed** | 127,333 | Player stats with positional percentiles, backed by matview. Columns: all player_detail columns PLUS position_group, pass_yds_pctl, pass_td_pctl, pass_pct_pctl, rush_yds_pctl, rush_td_pctl, rush_ypc_pctl, rec_yds_pctl, rec_td_pctl, tackles_pctl, sacks_pctl, tfl_pctl, ppa_avg_pctl |
| `api.game_player_leaders` | **Deployed** | 4,194,621 | Per-game player stats flattened from dlt hierarchy. Columns: game_id, season, team, conference, home_away, category, stat_type, player_id, player_name, stat |
| `api.game_box_score` | **Deployed** | 1,178,727 | Per-game team stats in EAV format. Columns: game_id, season, team, home_away, category, stat_value |
| `api.game_line_scores` | **Deployed** | 45,897 | Game line scores pivoted into Q1-Q4 columns with OT periods summed. Columns: game_id, season, home_q1, home_q2, home_q3, home_q4, home_ot, away_q1, away_q2, away_q3, away_q4, away_ot |
| `api.team_playcalling_profile` | **Deployed** | 4,627 | Team playcalling identity with situational tendencies and percentile rankings. One row per team-season. Columns: team, season, conference, games_played, overall_run_rate, early_down_run_rate, third_down_pass_rate, red_zone_run_rate, overall_success_rate, overall_avg_epa, third_down_success_rate, red_zone_success_rate, leading_run_rate, trailing_run_rate, run_rate_delta, pace_plays_per_game, overall_run_rate_pctl, early_down_run_rate_pctl, third_down_pass_rate_pctl, overall_epa_pctl, third_down_success_pctl, red_zone_success_pctl, run_rate_delta_pctl, pace_pctl |
| `api.coaching_history` | **Deployed** | 2,752 | Coaching tenure analytics: career spans, W-L records, talent metrics. One row per coach-team-tenure. Columns: first_name, last_name, team, tenure_start, tenure_end, seasons_count, total_wins, total_losses, win_pct, conf_wins, conf_losses, conf_win_pct, bowl_games, bowl_wins, inherited_talent_rank, year3_talent_rank, talent_improvement, is_active |
| `api.recruiting_roi` | **Deployed** | 1,324 | Recruiting investment vs on-field outcomes. 4-year rolling BCR, wins over expected, draft production. One row per team-season. Columns: team, season, conference, blue_chip_ratio, avg_recruit_rating, total_wins, win_pct, epa_per_play, players_drafted, wins_over_expected, recruiting_efficiency, win_pct_pctl, epa_pctl, recruiting_efficiency_pctl |
| `api.transfer_portal_impact` | **Deployed** | 1,374 | Transfer portal activity correlated with team performance changes. Portal era (2021+). One row per team-season. Columns: team, season, conference, transfers_in, transfers_out, net_transfers, avg_transfer_stars, portal_dependency, win_delta, net_transfers_pctl, win_delta_pctl, portal_dependency_pctl |
| `api.conference_comparison` | **Deployed** | 347 | Conference-level season analytics with percentile rankings. One row per conference-season. Columns: conference, season, member_count, avg_wins, avg_sp_rating, avg_epa, avg_recruiting_rank, non_conf_win_pct, avg_sp_pctl, avg_epa_pctl, avg_recruiting_pctl, non_conf_win_pct_pctl |

### Marts (schema: `marts`) -- Materialized Views

Pre-computed analytical data. Used by both API views (as building blocks) and directly by
cfb-app for advanced features.

| Materialized View | Status | Description |
|-------------------|--------|-------------|
| `marts.team_season_summary` | Deployed | One row per team/season: wins, losses, ratings, recruiting |
| `marts.team_epa_season` | Deployed | Season-level EPA metrics per team |
| `marts._game_epa_calc` | Deployed | Per-game EPA calculations (internal building block for api.game_detail) |
| `marts.team_style_profile` | Deployed | Offensive/defensive style characterization |
| `marts.team_season_trajectory` | Deployed | Week-by-week performance trajectory |
| `marts.defensive_havoc` | Deployed | Defensive disruption metrics (TFL, sacks, INT, PD) |
| `marts.team_tempo_metrics` | Deployed | Pace of play and tempo analysis |
| `marts.team_talent_composite` | Deployed | Recruiting talent composite scores |
| `marts.scoring_opportunities` | Deployed | Red zone and scoring efficiency |
| `marts.situational_splits` | Deployed | Down, distance, field position splits |
| `marts.matchup_history` | Deployed | Historical head-to-head records |
| `marts.coach_record` | Deployed | Coach win/loss records by team and season |
| `marts.recruiting_class` | Deployed | Recruiting class summaries per team/year |
| `marts.conference_era_summary` | Deployed | Conference-level aggregates across eras |
| `marts.matchup_edges` | Deployed | Matchup advantage/disadvantage analysis |
| `marts.play_epa` | Deployed | Per-play EPA values |
| `marts.player_game_epa` | Deployed | Player EPA aggregated per game |
| `marts.player_season_epa` | Deployed | Player EPA aggregated per season |
| `marts.player_comparison` | Deployed | Player stats pivoted from EAV with positional percentiles (PERCENT_RANK). Indexes: (player_id, season) unique, (season, position_group) |
| `marts.team_playcalling_tendencies` | Deployed | Team play-calling mix (run/pass rates) by situation: down, distance, field position, score state. ~492K rows. Grain: team + season + situation. |
| `marts.team_situational_success` | Deployed | Team situational effectiveness (success rate, EPA, explosiveness) by context. ~492K rows. Min 10-play threshold for rate metrics. |
| `marts.coaching_tenure` | Deployed | Coaching tenure analytics with gap detection. One row per coach-team-tenure. Includes W-L, bowl record, inherited vs recruited talent. 2,752 rows. |
| `marts.recruiting_roi` | Deployed | 4-year rolling recruiting investment vs outcomes. Blue chip ratio, wins over expected, draft production, recruiting efficiency. 1,324 rows. |
| `marts.transfer_portal_impact` | Deployed | Portal activity correlated with team performance changes. Portal era only (2021+). 1,374 rows. |
| `marts.conference_comparison` | Deployed | Per-conference per-season aggregates with PERCENT_RANK percentiles. 347 rows. |
| `marts.conference_head_to_head` | Deployed | Conference vs conference records by season. Alphabetical ordering to avoid duplicate pairs. 4,818 rows. |
| `marts.data_freshness` | Deployed | Data freshness tracking for 23 key tables. Row counts, last activity, staleness detection. |

### Public Schema Views

Legacy views exposed in the `public` schema. These are consumed by cfb-app and will
eventually migrate to `api.*`.

| View | Status | Description |
|------|--------|-------------|
| `public.teams` | Deployed | Team reference data |
| `public.teams_with_logos` | Deployed | Teams with logo URLs |
| `public.games` | Deployed | Game schedule and results |
| `public.roster` | Deployed | Player roster data |
| `public.team_epa_season` | Deployed | Team EPA per season (duplicate of marts view) |
| `public.team_season_epa` | Deployed | Team season EPA (alternate shape) |
| `public.defensive_havoc` | Deployed | Defensive havoc metrics |
| `public.team_style_profile` | Deployed | Team style characterization |
| `public.team_season_trajectory` | Deployed | Week-by-week trajectory |
| `public.team_tempo_metrics` | Deployed | Tempo analysis |
| `public.team_special_teams_sos` | Deployed | Special teams strength of schedule |
| `public.transfer_portal_search` | Deployed | Transfer portal entries for roster builder search. Columns: season, first_name, last_name, position, origin, destination, stars, rating, transfer_date, eligibility |
| `public.recruits_search` | Deployed | Recruiting class entries for roster builder search. Columns: id, athlete_id, year, name, position, height, weight, stars, rating, ranking, committed_to, school, city, state_province, country |

### RPCs (Functions)

Server-side functions callable via `supabase.rpc()`.

| Function | Schema | Arguments | Description |
|----------|--------|-----------|-------------|
| `get_drive_patterns` | `public` | `(p_team, p_season)` | Drive start/end zones with outcome buckets |
| `get_down_distance_splits` | `public` | `(p_team, p_season)` | Success rate and EPA by down and distance |
| `get_red_zone_splits` | `public` | `(p_team, p_season)` | Red zone efficiency: TD rate, FG rate, scoring rate |
| `get_field_position_splits` | `public` | `(p_team, p_season)` | EPA and success rate by field position zone |
| `get_home_away_splits` | `public` | `(p_team, p_season)` | Home vs away performance comparison |
| `get_conference_splits` | `public` | `(p_team, p_season)` | Performance vs conference, non-conference, ranked opponents |
| `get_trajectory_averages` | `public` | `(p_conference, p_season_start?, p_season_end?)` | Conference and FBS average benchmarks |
| `get_player_season_stats_pivoted` | `public` | `(p_team, p_season)` | Pivoted player stats (pass/rush/rec/def/kick in columns) |
| `get_player_search` | `public` | `(p_query text, p_position?, p_team?, p_season?, p_limit? default 25)` | Fuzzy player name search using pg_trgm. Returns player_id, name, team, position, season, height, weight, jersey, stars, recruit_rating, similarity_score. Supports typo tolerance. |
| `get_available_seasons` | `public` | `()` | List of seasons with data |
| `get_available_weeks` | `public` | `(p_season)` | List of weeks for a given season |
| `is_garbage_time` | `public` | `(period, score_diff)` | Returns true if play is in garbage time |
| `get_conference_head_to_head` | `public` | `(p_conf1, p_conf2, p_season_start?, p_season_end?)` | Conference vs conference head-to-head records by season. Flips results to match caller's conference order. |
| `get_data_freshness` | `public` | `()` | Returns data freshness status for all tracked tables. Useful for cfb-app "data last updated" indicators. |

### Reference Tables (Direct Access Allowed)

These reference tables are stable enough for direct access.

| Table | Description |
|-------|-------------|
| `ref.teams` | Team master data (school, mascot, conference, colors, logos) |
| `ref.eras` | Conference era definitions (BCS, CFP-4, CFP-12) |

### Utility Functions

| Function | Schema | Description |
|----------|--------|-------------|
| `ref.get_era` | `ref` | Returns era code/name for a given year |
| `analytics.refresh_all_views` | `analytics` | Refreshes all analytics materialized views (admin use) |
| `marts.refresh_all` | `marts` | Refreshes all 28 mart materialized views in dependency order (5 layers). Returns (view_name, duration_ms, status). |

---

## Consumer: cfb-scout

cfb-scout is the recruiting/scouting application. It has a narrower dependency surface.

### API Views

| View | Schema | Description |
|------|--------|-------------|
| `api.roster_lookup` | `api` | Player roster data for matching scout reports to players |
| `api.recruit_lookup` | `api` | Recruiting data: stars, rating, committed_to, position |
| `api.player_detail` | `api` | Single player page: bio, recruiting, season stats, PPA |
| `api.player_season_leaders` | `api` | Season stat leaders by category |
| `api.player_comparison` | `api` | Player stats with positional percentiles for side-by-side comparison |

### RPCs

| Function | Schema | Description |
|----------|--------|-------------|
| `get_player_search` | `public` | Fuzzy player name search with typo tolerance. **Replaces raw roster table queries** for player lookup in cfb-scout. |

### Scouting Schema (Owned by cfb-scout)

The `scouting` schema is owned and managed by cfb-scout pipelines, not cfb-database.
cfb-database does not make stability guarantees for these objects.

| Object | Type | Description |
|--------|------|-------------|
| `scouting.players` | Table | Scouting player profiles |
| `scouting.reports` | Table | Scouting reports |
| `scouting.player_mart` | View | Denormalized player mart |
| `scouting.player_embeddings` | Table | Vector embeddings for player similarity |
| `scouting.alerts` | Table | Scouting alerts |
| `scouting.alert_history` | Table | Alert trigger history |
| `scouting.watch_lists` | Table | Scout watch lists |
| `scouting.pff_grades` | Table | PFF grade data |
| `scouting.player_timeline` | Table | Player event timeline |
| `scouting.portal_snapshots` | Table | Transfer portal snapshots |
| `scouting.transfer_events` | Table | Transfer event log |
| `scouting.crawl_jobs` | Table | Web crawl job tracking |
| `scouting.pending_links` | Table | Pending crawl links |
| `scouting.team_rosters` | Table | Crawled roster snapshots |
| `scouting.refresh_player_mart` | Function | Refreshes the player_mart view |

---

## Internal (May Change Without Notice)

These objects are implementation details. Do not depend on them from downstream repos.

### Raw Data Tables

| Schema | Tables |
|--------|--------|
| `core` | `games`, `drives`, `plays` (partitioned: `plays_y2004`..`plays_y2026`), `roster`, `roster__recruit_ids`, `records`, `rankings`, `game_media`, `game_weather`, `game_player_stats` (+ nested `__teams`, `__categories`, `__types`, `__athletes`), `game_team_stats` (+ nested `__teams`, `__stats`), `games__home_line_scores`, `games__away_line_scores` |
| `stats` | `team_season_stats`, `player_season_stats`, `advanced_team_stats`, `advanced_game_stats`, `game_havoc`, `play_stats`, `player_usage`, `player_returning` |
| `ratings` | `sp_ratings`, `sp_conference_ratings`, `elo_ratings`, `fpi_ratings`, `srs_ratings` |
| `recruiting` | `recruits`, `team_recruiting`, `team_talent`, `transfer_portal`, `recruiting_groups` |
| `betting` | `lines`, `team_ats` |
| `draft` | `draft_picks` |
| `metrics` | `ppa_teams`, `ppa_games`, `ppa_players_season`, `ppa_players_games`, `pregame_win_probability`, `fg_expected_points`, `wepa_team_season`, `wepa_players_passing`, `wepa_players_rushing`, `wepa_players_kicking` |
| `ref` | `conferences`, `venues`, `coaches`, `coaches__seasons`, `play_types`, `play_stat_types`, `teams__alternate_names`, `teams__logos` |

### dlt Pipeline Metadata

Every schema that uses dlt pipelines has these internal tables. Never depend on them.

- `{schema}._dlt_loads`
- `{schema}._dlt_pipeline_state`
- `{schema}._dlt_version`

Present in: `core`, `stats`, `ratings`, `recruiting`, `betting`, `draft`, `metrics`, `ref`

### core_staging Schema

The `core_staging` schema contains dlt staging tables with `_dlt_id` / `_dlt_parent_id`
columns. These are pipeline internals and must not be queried by downstream repos.

Tables: `drives`, `games`, `games__away_line_scores`, `games__home_line_scores`,
`game_media`, `game_player_stats` (+ nested), `game_team_stats` (+ nested), `game_weather`,
`plays`, `rankings`, `records`, `roster`, `roster__recruit_ids`, `_dlt_version`

### Analytics Materialized Views (Internal)

The `analytics` schema contains pre-computed views used for dashboard queries. These are
not yet contracted and may change shape.

| Materialized View | Description |
|-------------------|-------------|
| `analytics.team_season_summary` | Win/loss splits, point margins per team/season |
| `analytics.player_career_stats` | Career stat aggregates per player |
| `analytics.conference_standings` | Conference standings with ratings |
| `analytics.team_recruiting_trend` | Recruiting trends with rolling averages |
| `analytics.game_results` | Denormalized game results with betting data |

| View | Description |
|------|-------------|
| `analytics.data_quality_dashboard` | Data pipeline health monitoring |

---

## Schema Dependency Graph

```
cfb-app  -->  api.* views  -->  marts.* matviews  -->  raw tables (core, stats, ratings, ...)
              api.game_player_leaders   -->  core.game_player_stats (5-level dlt hierarchy)
              api.game_box_score        -->  core.game_team_stats (3-level dlt hierarchy)
              api.game_line_scores      -->  core.games + line_scores child tables
              api.team_playcalling_profile -->  marts.team_playcalling_tendencies
                                               marts.team_situational_success
                                               marts.team_epa_season, ref.teams
              public.* views
              public.get_* RPCs

cfb-scout -->  api.roster_lookup       -->  core.roster
               api.recruit_lookup      -->  recruiting.recruits
               api.player_detail       -->  core.roster, stats.*, recruiting.*, metrics.*
               api.player_season_leaders -->  stats.player_season_stats
               api.player_comparison   -->  marts.player_comparison (matview)
               get_player_search()     -->  api.roster_lookup
               scouting.* (owned by cfb-scout)
```

---

## How to Add a New Public View

1. Create the SQL in `src/schemas/api/` or `src/schemas/marts/`
2. Add an entry to this document under the appropriate consumer section
3. Deploy via migration
4. Run `supabase gen types` and commit the updated TypeScript types
5. Notify downstream repo maintainers
