# Schema Contract

> Defines the public API surface of cfb-database. Downstream repos (cfb-app, cfb-scout) should
> only depend on objects listed here as **public**. Everything else is internal and may change
> without notice.

Last updated: 2026-07-21

> **Note on cfb-analytics:** the retired OU-only app (rstover-fo/cfb-analytics) was never a
> warehouse consumer -- it ran its own DuckDB ingestion. Its unique features (rivals page,
> compare page, CSV export, a11y patterns, query tests) were ported into cfb-app in July 2026
> and the repo is kept for reference only. cfb-app and cfb-scout remain the two consumers.

---

## Recent Contract Changes

- **2026-07-21 — `api.game_recaps` added (Deployed).** P3.3 Lane D: nightly
  LLM-generated game recaps, one row per completed FBS game (season >= 2014), written by
  `scripts/generate_recaps.py` (model `claude-haiku-4-5`) from warehouse facts only --
  scores, top-EPA plays, win-probability swings when available, box-score leaders, and the
  betting-line result. **Content is LLM-generated from warehouse facts, not CFBD data** --
  treat as editorial/narrative content, not a structured stat, and do not assume two reads
  of the same game return byte-identical prose. A game is regenerated only when an operator
  flips `analytics.game_recaps.regenerate` true; otherwise a recap is written once and left
  as-is. A missing `game_id` means "not yet generated," not "no recap available." cfb-app
  consumer note: render `headline`/`recap` as prose, not as data to parse or chart, and treat
  `wp_available = false` as a signal the recap's momentum framing came from an EPA-only
  fallback rather than real win-probability data. Backed by `analytics.game_recaps`
  (`src/schemas/migrations/027_game_recaps.sql`); exposed via
  `src/schemas/api/034_game_recaps.sql`. **Deployed 2026-07-21** (see
  `deploys/p33-apply-manifest.json`); do not query until this entry is updated to
  **Live**/**Deployed**.

- **2026-07-21 — Tier 2 analytics: house Elo, ridge-adjusted EPA, scored edges, predictions.**
  Five new `marts.*` materialized views and five new `api.*` views add a house-generated
  opinion layer on top of the warehouse's authoritative CFBD data. All of it is
  **transparent math -- no fitted ML model**: `elo_v1` is Elo-only, `elo_epa_blend_v1` blends
  0.6*Elo + 0.4*ridge-adjusted EPA; both are closed-form and reproducible from the SQL/Python
  alone (`scripts/compute_house_elo.py`, `scripts/compute_adjusted_epa.py`,
  `scripts/compute_predictions.py`; see `docs/plans/2026-07-21-tier2-analytics-plan.md`).
  - **House Elo:** `marts.house_elo` (season-end rating per team-season, ranked, with a
    `low_confidence` flag for thin seasons) and `marts.house_elo_game` (game-grain pregame/
    postgame Elo, win probability, expected-vs-actual margin) -- exposed as `api.team_elo` and
    `api.game_elo_history`. Full history from 1869; CFBD's own Elo (coverage ~2015+) is carried
    alongside purely for side-by-side comparison, not used in the computation.
  - **Ridge-adjusted EPA:** `marts.team_adjusted_epa` -- opponent-adjusted offensive/defensive
    EPA per team-season from a ridge regression (lambda=200) over `marts.play_epa`, 2004+, with
    CFBD's WEPA (`marts.team_wepa_season`) joined in as a sanity check only. Not exposed as its
    own API view; it feeds the predictions below.
  - **Scored edges:** `marts.scored_matchup_edges` -- house expected margin/win probability vs.
    the market line for **upcoming** games only, with the resulting `edge` and `edge_pick`.
    Normally empty out of season -- that is expected behavior, not a data-quality failure.
    Exposed as `api.scored_matchup_edges`.
  - **Predictions:** new `predictions` schema (`predictions.game_predictions`) holds
    append-only daily snapshots -- one immutable row per `(game_id, model_version,
    prediction_date)`, written by `scripts/compute_predictions.py`. It is readable directly,
    but downstream consumers should prefer `api.game_predictions` (latest snapshot per
    game/model via `DISTINCT ON`) unless the full day-by-day history is needed.
  - **Backtest surface:** `marts.prediction_accuracy` -- retroactive scoring (margin MAE/RMSE,
    ATS record, Brier score vs. CFBD's own pregame win probability) by season, model, and
    edge-threshold. Exposed as `api.prediction_accuracy`.
  - **`marts.matchup_edges` (016) is now documented as style-only.** It predates house Elo/EPA
    and is an unvalidated style-matchup scorer; prediction use should read
    `marts.scored_matchup_edges` instead.
  - `marts.refresh_all()` now refreshes 37 materialized views in 6 dependency layers (was 32
    in 5 layers); the daily workflow runs the three compute scripts and an explicit
    `refresh_marts.py --views` pass over the 5 new Tier 2 marts after the season load step.
  - cfb-app should regenerate `supabase gen types` to pick up the new `predictions` schema and
    the new/changed views.

- **2026-07-20 — cfb-app fully contract-compliant.** As of cfb-app PRs #15-#17, the app has
  zero direct `core.*` access (a repo-side contract-guard test enforces this), consumes
  `api.game_drives` / `api.game_plays` / `api.poll_rankings` / `api.matchup`, and ships new
  `/rivals` and `/compare` pages on the contracted surface. Deprecated raw access from the
  2026-07-20 views entry below can now be considered fully retired.

- **2026-07-20 — Added `api.game_drives`, `api.game_plays`, `api.poll_rankings`.**
  Closes the Phase 0 Lane D gap where cfb-app queried `core.drives`, `core.plays`, and
  `core.rankings` directly, in violation of Contract Rule 4. Direct downstream access to
  those three raw `core.*` tables is now **deprecated** -- migrate to the new `api.*`
  views.

- **2026-07-20 — Rankings merge key fixed; `api.poll_rankings` gains `season_type`.**
  The suspected data-integrity risk was confirmed live and fixed the same day: the old
  merge key `[season, week, poll, rank]` dropped one team whenever a poll had a rank tie
  (55 AP weeks were short) and let the postseason final poll (reported as week 1)
  overwrite regular-season week 1. The pipeline now merges on
  `[season, season_type, week, poll, school]`, history was reloaded 2000-2025, and the
  view exposes `season_type`. **Consumers of `api.poll_rankings` should filter
  `season_type = 'regular'` for weekly polls** (the final poll is
  `season_type = 'postseason'`, week 1) and must tolerate duplicate rank values within
  a week (tied teams share a rank; the next rank is skipped).

- **2026-07-19 — `get_trajectory_averages` default season end now tracks loaded data.**
  `p_season_end` default changed from a pinned `2025` to `NULL`, which resolves to the
  latest season present in `public.team_season_trajectory`. Callers omitting the argument
  will start receiving 2026 rows as they materialize; explicit arguments behave unchanged.

- **2026-07-19 — Tier 1 analytics unlock.**
  - **Garbage-time exclusion (behavioral change, no signature change):** the five split RPCs
    (`get_home_away_splits`, `get_conference_splits`, `get_red_zone_splits`,
    `get_down_distance_splits`, `get_field_position_splits`) now filter out garbage-time
    plays via `public.is_garbage_time()`. Output values shift slightly (garbage-time plays
    previously inflated/deflated per-play splits); arguments and return shapes are unchanged.
  - **`get_player_detail` additive columns (return-type recreate):** gains
    `wepa_passing`, `wepa_rushing`, `paar` (opponent-adjusted EPA and kicker points-above-
    average-replacement from `marts.player_wepa_season`). The function was dropped and
    recreated to change its `RETURNS TABLE` signature; callers selecting columns by name are
    unaffected.
  - **Player EPA attribution rebuilt on athlete_id (additive):** `marts.player_game_epa` and
    `marts.player_season_epa` gain an `athlete_id` column and a new `receiving` play_category
    (alongside existing `passing`/`rushing`). Attribution now joins CFBD's `stats.play_stats`
    athlete-to-play link table instead of regex-parsing play text; coverage starts ~2014
    (previously 2004+) since `stats.play_stats` does not extend earlier.
  - **`marts.defensive_havoc` havoc rates re-sourced (additive):** havoc-rate columns are now
    sourced from authoritative `stats.game_havoc` instead of a play-text heuristic; gains
    `front_seven_havoc_rate` and `db_havoc_rate` (also added to `public.defensive_havoc`).
    Disruptive counts (sacks, interceptions, fumbles, TFLs, stuffs) remain plays-derived
    approximations, unchanged.
  - **Six new `api.*` views and five new `marts.*` materialized views added** — see the API
    Views and Marts tables below (`team_wepa_season`, `player_wepa_leaders`/
    `player_wepa_season`, `team_returning_production`/`returning_production`,
    `player_usage_leaders`/`player_usage`, `team_ats`/`team_ats_records`, `line_movement`).
  - cfb-app should regenerate `supabase gen types` to pick up the new/changed columns and
    views.

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
| `api.team_wepa_season` | **Deployed** | -- | Opponent-adjusted EPA (WEPA) by team-season. One row per team-season. Columns: season, team_id, team, conference, epa_total, epa_passing, epa_rushing, epa_allowed_total, epa_allowed_passing, epa_allowed_rushing, success_rate_total (+ standard/passing-downs and allowed variants), rushing line/second-level/open-field/highlight yards (+ allowed), explosiveness, explosiveness_allowed, epa_rank, defense_rank |
| `api.player_wepa_leaders` | **Deployed** | -- | Player WEPA leaders: passing/rushing WEPA and kicker PAAR, tall grain. One row per season-athlete-category. Columns: season, athlete_id, athlete_name, position, team, conference, category (passing/rushing/kicking), wepa, paar, metric, plays, season_rank |
| `api.team_returning_production` | **Deployed** | -- | Returning production by team-season: total and percent of last season's PPA returning. One row per team-season. Columns: season, team, conference, total_ppa, total_passing_ppa, total_receiving_ppa, total_rushing_ppa, returning_ppa_pct, returning_passing_ppa_pct, returning_receiving_ppa_pct, returning_rushing_ppa_pct, usage, passing_usage, receiving_usage, rushing_usage, returning_rank |
| `api.player_usage_leaders` | **Deployed** | -- | Player usage rates by season: share of team plays overall and by down/situation. One row per season-athlete. Columns: season, athlete_id, player_name, position, team, conference, usage_overall, usage_pass, usage_rush, usage_first_down, usage_second_down, usage_third_down, usage_standard_downs, usage_passing_downs |
| `api.team_ats` | **Deployed** | -- | Team against-the-spread (ATS) records by season. One row per team-season. Columns: season, team_id, team, conference, games, ats_wins, ats_losses, ats_pushes, avg_cover_margin, ats_win_pct |
| `api.line_movement` | **Deployed** | -- | Betting line movement history from append-only daily snapshots of pending games. One row per (game, provider, captured_at) snapshot. Columns: captured_at, game_id, season, week, home_team, away_team, provider, spread, formatted_spread, over_under, home_moneyline, away_moneyline, line_hash |
| `api.game_drives` | **Live** | 183,603 | Drive-by-drive summary for a game, one row per possession. Columns: game_id, season, drive_number, offense, defense, start_period, start_yards_to_goal, end_yards_to_goal, plays, yards, drive_result, scoring, start_offense_score, end_offense_score, start_defense_score, end_defense_score, start_time_minutes, start_time_seconds, elapsed_minutes, elapsed_seconds, is_home_offense |
| `api.game_plays` | **Live** | 3,611,707 | Play-by-play for a game, one row per snap, unfiltered by play type (cfb-app filters client-side). Columns: game_id, season, drive_number, play_number, offense, defense, period, clock_minutes, clock_seconds, down, distance, yards_to_goal, yards_gained, play_type, play_text, ppa, scoring, offense_score, defense_score |
| `api.poll_rankings` | **Live** | ~31,000 | Weekly poll rankings (AP Top 25, Coaches Poll, CFP, etc). Columns: season, season_type, week, poll, rank, school, conference, first_place_votes, points. Filter `season_type = 'regular'` for weekly polls; final poll is `season_type = 'postseason'` (week 1). Tied teams share a rank (next rank skipped) |
| `api.team_elo` | **Live** | ~29,000 | Season-end house Elo rating per team-season, ranked within season. Columns: team, season, season_end_elo, elo_rank, games_played, low_confidence, cfbd_elo |
| `api.game_elo_history` | **Live** | ~71,000 | Game-grain house Elo history: pregame/postgame Elo both sides, win probability, expected vs actual margin, CFBD Elo copies for validation. Columns: game_id, season, week, season_type, start_date, neutral_site, home_team, away_team, home_pregame_elo, away_pregame_elo, home_postgame_elo, away_postgame_elo, home_win_prob, expected_home_margin, actual_home_margin, mov_multiplier, cfbd_home_pregame_elo, cfbd_away_pregame_elo, margin_error, abs_margin_error |
| `api.scored_matchup_edges` | **Live** | Varies (in-season) | House model expected margin/win probability vs. the market line for upcoming games, with the resulting edge. Empty out of season by design -- not a failure. Columns: game_id, season, week, season_type, start_date, home_team, away_team, neutral_site, model_version, prediction_date, home_elo_pregame, away_elo_pregame, elo_margin, epa_margin, expected_home_margin, home_win_prob, market_provider, market_spread, market_home_margin, market_captured_at, edge, edge_pick, abs_edge |
| `api.prediction_accuracy` | **Live** | ~90 | Retroactive scoring of house predictions by season/model/edge-threshold: margin MAE/RMSE, ATS record, Brier score (house vs. CFBD). Columns: model_version, season, edge_threshold, n_games, n_with_market, margin_mae, margin_rmse, ats_wins, ats_losses, ats_pushes, ats_hit_rate, brier, cfbd_brier, n_scored_win_prob |
| `api.game_predictions` | **Live** | ~20,000+ | Latest house prediction snapshot per (game, model), from the append-only `predictions.game_predictions` log. Columns: prediction_id, computed_at, prediction_date, model_version, game_id, season, week, season_type, home_team, away_team, neutral_site, home_elo_pregame, away_elo_pregame, elo_margin, epa_margin, expected_home_margin, home_win_prob, market_provider, market_home_margin, market_spread, market_captured_at, edge, edge_pick |
| `api.game_recaps` | **Deployed** | 0 (fills nightly) | Nightly LLM-generated game recap. **Content is LLM-generated from warehouse facts, not CFBD data** -- regenerated only via the `regenerate` flag; a missing `game_id` means not yet generated. cfb-app should render `headline`/`recap` as prose, not structured stats. Columns: game_id, season, week, headline, recap, wp_available, model, generated_at |

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
| `marts.matchup_edges` | Deployed | Matchup advantage/disadvantage analysis from team style profiles. **Style-only** as of 2026-07-21 -- for prediction use read `marts.scored_matchup_edges` instead. |
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
| `marts.team_wepa_season` | Deployed | Opponent-adjusted EPA (WEPA) by team-season, passthrough of `metrics.wepa_team_season`. Grain: `(team, season)`. Unique key: `(team, season)`. |
| `marts.player_wepa_season` | Deployed | Player WEPA (passing/rushing) and kicker PAAR, tall union of `metrics.wepa_players_*`. Grain: `(season, athlete_id, category)`. Unique key: `(season, athlete_id, category)`. |
| `marts.returning_production` | Deployed | Returning production by team-season (PPA and usage returning from prior season), passthrough of `stats.player_returning`. Grain: `(season, team)`. Unique key: `(team, season)`. |
| `marts.player_usage` | Deployed | Player usage rates by season (overall/pass/rush/down-split shares), passthrough of `stats.player_usage`. Grain: `(season, athlete_id)`. Unique key: `(season, athlete_id)`. |
| `marts.team_ats_records` | Deployed | Team against-the-spread records by season, passthrough of `betting.team_ats` plus computed `ats_win_pct`. Grain: `(season, team_id)`. Unique key: `(team_id, season)`. |
| `marts.house_elo` | Deployed | Season-end house Elo rating per team-season (last game's postgame Elo), ranked within season, with CFBD Elo joined in for comparison. Grain: `(team, season)`. |
| `marts.house_elo_game` | Deployed | Game-grain house Elo history: pregame/postgame Elo both sides, win probability, expected vs actual margin, plus derived `margin_error`. Grain: `(game_id)`. |
| `marts.team_adjusted_epa` | Deployed | Ridge-regressed opponent-adjusted EPA (offense/defense/net, lambda=200) per team-season, 2004+, with CFBD's WEPA joined in as a sanity check. Grain: `(team, season)`. |
| `marts.scored_matchup_edges` | Deployed | House expected margin/win probability vs. the market line for upcoming games only; legitimately empty out of season (no empty-guard by design). Grain: `(game_id, model_version)`. |
| `marts.prediction_accuracy` | Deployed | Retroactive scoring of house predictions: margin MAE/RMSE, ATS record, Brier score (house vs. CFBD) by season/model/edge-threshold. Grain: `(model_version, season, edge_threshold)`. |

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
| `get_down_distance_splits` | `public` | `(p_team, p_season)` | Success rate and EPA by down and distance (excludes garbage time as of 2026-07-19) |
| `get_red_zone_splits` | `public` | `(p_team, p_season)` | Red zone efficiency: TD rate, FG rate, scoring rate (excludes garbage time as of 2026-07-19) |
| `get_field_position_splits` | `public` | `(p_team, p_season)` | EPA and success rate by field position zone (excludes garbage time as of 2026-07-19) |
| `get_home_away_splits` | `public` | `(p_team, p_season)` | Home vs away performance comparison (excludes garbage time as of 2026-07-19) |
| `get_conference_splits` | `public` | `(p_team, p_season)` | Performance vs conference, non-conference, ranked opponents (excludes garbage time as of 2026-07-19) |
| `get_trajectory_averages` | `public` | `(p_conference, p_season_start?, p_season_end?)` | Conference and FBS average benchmarks. Omitted `p_season_end` resolves to the latest loaded season (changed 2026-07-19; previously pinned to 2025). |
| `get_player_season_stats_pivoted` | `public` | `(p_team, p_season)` | Pivoted player stats (pass/rush/rec/def/kick in columns) |
| `get_player_detail` | `public` | `(p_player_id, p_season?)` | Single player page: bio, recruiting, season stats, PPA. Gains `wepa_passing`, `wepa_rushing`, `paar` (opponent-adjusted EPA and kicker PAAR from `marts.player_wepa_season`), added 2026-07-19. |
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
| `marts.refresh_all` | `marts` | Refreshes all 37 mart materialized views in dependency order (6 layers). Returns (view_name, duration_ms, status). |

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
| `betting` | `lines`, `team_ats`, `line_snapshots` (append-only line movement snapshots, no PK, `captured_at` stamped per run) |
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

### predictions Schema

Added 2026-07-21 (Tier 2 analytics). `predictions.game_predictions` is **readable**
(`SELECT` granted to `anon`/`authenticated`; `INSERT`/`UPDATE`/`DELETE` are revoked) but it is
pipeline output, not the contract surface -- it is an append-only log with one immutable row
per `(game_id, model_version, prediction_date)`. Downstream consumers should prefer
`api.game_predictions`, which resolves to the latest snapshot per game/model, unless the full
day-by-day prediction history is actually needed.

| Table | Description |
|-------|-------------|
| `predictions.game_predictions` | Append-only daily house prediction snapshots (house Elo + ridge-adjusted-EPA expected margin/win probability vs. the market line). Written by `scripts/compute_predictions.py`. Prefer `api.game_predictions` for the latest-snapshot contract view. |

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
