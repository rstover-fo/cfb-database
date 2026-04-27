# cfb-app Analytics Capabilities Report

> Full inventory of materialized views, API views, and RPCs organized by
> cfb-app feature area. Generated 2026-02-06.

---

## Summary

| Layer | Count | Total Rows |
|-------|-------|-----------|
| Materialized Views (marts) | 28 | ~4.1M |
| Materialized Views (analytics) | 5 | ~697K |
| API Views | 18 | ~7.5M |
| Public Views | 13 | ~533K |
| RPCs | 16 custom | — |

---

## Feature Area 1: Team Dashboard

**The richest area.** Everything needed for a deep team profile page.

### Available Data Sources

| Source | Type | Rows | What It Powers |
|--------|------|------|----------------|
| `api.team_detail` | View | 136 | **Team card**: current season stats, ratings (SP+, Elo, FPI), EPA tier, recruiting rank, colors, logo |
| `api.team_history` | View | 9,374 | **Multi-season trends**: W-L, ratings, EPA, recruiting over time. Sparkline charts, trajectory analysis |
| `api.leaderboard_teams` | View | 9,374 | **Team rankings table**: sortable by wins, PPG, defense, EPA. Per-season with ranks |
| `api.team_playcalling_profile` | View | 4,627 | **Playcalling identity**: run/pass rates, situational tendencies, EPA efficiency, percentile ranks |
| `marts.team_epa_season` | Matview | 4,627 | Season EPA, success rate, explosiveness, tier label |
| `marts.team_style_profile` | Matview | 4,627 | **Style DNA**: run/pass identity, rushing vs passing EPA, tempo category, defensive strengths |
| `marts.team_season_trajectory` | Matview | 4,627 | **Week-by-week trajectory**: EPA trend, win %, off/def rank movement, era context |
| `marts.team_tempo_metrics` | Matview | 2,072 | Pace of play: plays/game, tempo tier, correlation with EPA |
| `marts.defensive_havoc` | Matview | 4,629 | **Defensive disruption**: havoc rate, sacks, INTs, fumbles, stuff rate, TFL |
| `marts.scoring_opportunities` | Matview | 4,953 | **Drive efficiency**: scoring rate, TD rate, red zone %, points/drive, turnover rate |
| `marts.situational_splits` | Matview | 4,627 | **Situational EPA**: by down, distance, field zone, late/close, 2-minute, run vs pass |
| `marts.team_talent_composite` | Matview | 4,867 | **Roster talent**: blue chip ratio, star counts, transfer-in quality, talent score |

### Available RPCs (Team Context)

| RPC | Arguments | What It Returns |
|-----|-----------|----------------|
| `get_drive_patterns` | (team, season) | Drive start/end zones with outcomes — Sankey diagram data |
| `get_down_distance_splits` | (team, season) | Success rate + EPA by down and distance bucket |
| `get_red_zone_splits` | (team, season) | Red zone TD%, FG%, scoring%, turnovers, EPA |
| `get_field_position_splits` | (team, season) | EPA and success rate by field zone (own territory → opponent red zone) |
| `get_home_away_splits` | (team, season) | Home vs away: wins, PPG, EPA, success rate, yards/play |
| `get_conference_splits` | (team, season) | Performance vs conf, non-conf, and ranked opponents |

### Suggested cfb-app Pages

1. **Team Profile Page** — `team_detail` + `team_style_profile` + `team_talent_composite` for the hero section
2. **Team Season Deep-Dive** — `situational_splits` + `scoring_opportunities` + `defensive_havoc` for an advanced stats tab
3. **Team History/Trends** — `team_history` + `team_season_trajectory` for multi-year charts
4. **Team Playcalling Analysis** — `team_playcalling_profile` + `get_down_distance_splits()` for an X's and O's tab
5. **Team Leaderboard** — `leaderboard_teams` for a sortable/filterable rankings page

---

## Feature Area 2: Player Analytics

### Available Data Sources

| Source | Type | Rows | What It Powers |
|--------|------|------|----------------|
| `api.player_detail` | View | 340,878 | **Player card**: bio, recruiting data, season stats (pass/rush/rec/def/kick), PPA |
| `api.player_comparison` | View | 127,333 | **Side-by-side comparison**: all stats + positional percentiles (PERCENT_RANK) |
| `api.player_season_leaders` | View | 152,966 | **Stat leaders**: top passers/rushers/receivers/defenders by season, filterable by conference |
| `api.roster_lookup` | View | 340,855 | Player roster for search/matching |
| `api.recruit_lookup` | View | 67,179 | Recruiting profiles: stars, rating, committed_to |
| `api.game_player_leaders` | View | 6.4M | **Per-game player stats**: flattened from dlt hierarchy (game_id, category, stat_type, stat) |
| `marts.player_comparison` | Matview | 127,333 | Pivoted stats + percentiles backbone |
| `marts.player_game_epa` | Matview | 92,770 | **Player EPA per game**: plays, total EPA, EPA/play, success rate, explosive plays |
| `marts.player_season_epa` | Matview | 11,467 | **Player EPA per season**: aggregated with rank |
| `analytics.player_career_stats` | Matview | 628,886 | **Career aggregates**: total stats, avg per season, seasons played |

### Available RPCs (Player Context)

| RPC | Arguments | What It Returns |
|-----|-----------|----------------|
| `get_player_search` | (query, position?, team?, season?, limit?) | Fuzzy name search with pg_trgm similarity scoring |
| `get_player_detail` | (player_id, season?) | Complete player profile with pivoted stats + recruiting |
| `get_player_percentiles` | (player_id, season) | Stats + positional percentile rankings |
| `get_player_game_log` | (player_id, season) | Game-by-game EPA log with opponent, result, week |
| `get_player_season_leaders` | (season, category, conference?, limit?) | Top performers by stat category |
| `get_player_season_stats_pivoted` | (team, season) | Full team roster stats in pivoted columns |

### Suggested cfb-app Pages

1. **Player Profile Page** — `get_player_detail()` + `get_player_percentiles()` for hero + radar chart
2. **Player Comparison Tool** — `api.player_comparison` for side-by-side with percentile bars
3. **Player Game Log** — `get_player_game_log()` for EPA-based game log table
4. **Season Stat Leaders** — `get_player_season_leaders()` for leaderboard tables by category
5. **Team Roster Stats** — `get_player_season_stats_pivoted()` for full-team stat tables

---

## Feature Area 3: Game Analysis

### Available Data Sources

| Source | Type | Rows | What It Powers |
|--------|------|------|----------------|
| `api.game_detail` | View | 45,897 | **Game card**: teams, scores, betting lines, EPA, venue, win probability, excitement |
| `api.game_box_score` | View | 1.4M | **Box score**: per-team stats in EAV format (category + stat_value) |
| `api.game_player_leaders` | View | 6.4M | **Player performances**: per-game stats by category |
| `api.game_line_scores` | View | 45,897 | **Quarter scores**: Q1-Q4 + OT for both teams |
| `marts.play_epa` | Matview | 2.7M | **Every play**: EPA, success, explosive, down/distance, field position, play type, garbage time flag |
| `marts._game_epa_calc` | Matview | 40,221 | Per-game team EPA aggregates |
| `analytics.game_results` | Matview | 45,885 | Denormalized game results with betting data |

### Available RPCs (Game Context)

| RPC | Arguments | What It Returns |
|-----|-----------|----------------|
| `get_available_seasons` | () | Array of seasons with data |
| `get_available_weeks` | (season) | Array of weeks for a season |
| `is_garbage_time` | (period, score_diff) | Boolean garbage time check |

### Suggested cfb-app Pages

1. **Game Detail Page** — `game_detail` + `game_line_scores` + `game_box_score` for a comprehensive game view
2. **Game Schedule/Scores** — `public.games` for schedule grids with score cards
3. **Play-by-Play Viewer** — `marts.play_epa` for EPA-annotated play-by-play (filter by game_id)
4. **Game Player Stats** — `game_player_leaders` filtered by game_id for "who played well"

---

## Feature Area 4: Matchup & Head-to-Head

### Available Data Sources

| Source | Type | Rows | What It Powers |
|--------|------|------|----------------|
| `api.matchup` | View | 11,975 | **Rivalry page**: all-time record, first/last meeting, recent results (JSONB), current season comparison |
| `marts.matchup_history` | Matview | 11,975 | Historical H2H: total games, margins, last-10-year splits |
| `marts.matchup_edges` | Matview | 15,682 | **Matchup analytics**: style matchups (run rate, EPA by type), tempo mismatch, edge scores, actual outcomes |

### Suggested cfb-app Pages

1. **Matchup Predictor** — `api.matchup` + `matchup_edges` for pre-game analysis with style/edge overlays
2. **Rivalry Page** — `matchup_history` for all-time record, biggest wins, recent trend

---

## Feature Area 5: Coaching Analytics

### Available Data Sources

| Source | Type | Rows | What It Powers |
|--------|------|------|----------------|
| `api.coaching_history` | View | 2,752 | **Coaching timelines**: tenure spans, W-L records, conference records, bowl record, talent improvement, active flag |
| `marts.coaching_tenure` | Matview | 2,752 | Full tenure analytics with gap detection, peak ratings, inherited vs recruited talent |
| `marts.coach_record` | Matview | 2,613 | Per-team coaching records with SP+ and recruiting context |

### Suggested cfb-app Pages

1. **Coach Profile Page** — `coaching_history` for career timeline, tenure cards, W-L by stop
2. **Coaching Search/Compare** — Compare coaches by win%, talent improvement, bowl record
3. **Coach Impact Analysis** — Overlay `coaching_tenure.talent_improvement` with win trend

---

## Feature Area 6: Recruiting & Transfer Portal

### Available Data Sources

| Source | Type | Rows | What It Powers |
|--------|------|------|----------------|
| `api.recruiting_roi` | View | 3,927 | **Recruiting ROI**: 4-year rolling BCR, wins over expected, draft production, efficiency percentiles |
| `api.transfer_portal_impact` | View | 1,406 | **Portal impact**: transfers in/out, portal dependency, win delta, percentile ranks |
| `api.recruit_lookup` | View | 67,179 | Individual recruit profiles |
| `marts.recruiting_class` | Matview | 4,227 | **Class summaries**: rank, points, star breakdown, blue chip ratio, position mix |
| `marts.recruiting_roi` | Matview | 3,927 | Full recruiting ROI with draft value and over/under expected metrics |
| `marts.transfer_portal_impact` | Matview | 1,406 | Portal activity with pre/post performance comparison |
| `marts.team_talent_composite` | Matview | 4,867 | Current roster talent: star counts, transfers, talent score |
| `analytics.team_recruiting_trend` | Matview | 4,356 | Year-over-year recruiting with rolling 3-year average |

### Available RPCs

| RPC | Arguments | What It Returns |
|-----|-----------|----------------|
| `get_player_search` | (query, ...) | Fuzzy search — works for recruits too via roster |

### Suggested cfb-app Pages

1. **Recruiting Dashboard** — `recruiting_class` + `team_recruiting_trend` for class-by-class analysis with star breakdowns
2. **Recruiting ROI Analysis** — `api.recruiting_roi` for "are they getting value from their recruiting?"
3. **Transfer Portal Hub** — `api.transfer_portal_impact` + `public.transfer_portal_search` for portal activity analysis
4. **Recruit Search** — `api.recruit_lookup` + `public.recruits_search` for individual recruit lookup

---

## Feature Area 7: Conference Analytics

### Available Data Sources

| Source | Type | Rows | What It Powers |
|--------|------|------|----------------|
| `api.conference_comparison` | View | 826 | **Conference rankings**: member count, avg wins, SP+, EPA, recruiting, non-conf win%, all with percentiles |
| `marts.conference_comparison` | Matview | 826 | Full conference comparison with best/worst teams, std dev, blue chip totals |
| `marts.conference_head_to_head` | Matview | 4,818 | **Conference vs conference**: season-by-season records |
| `marts.conference_era_summary` | Matview | 172 | **Era-level**: avg EPA, success rate, team count across BCS/CFP-4/CFP-12 eras |
| `analytics.conference_standings` | Matview | 8,304 | Conference standings with ratings |

### Available RPCs

| RPC | Arguments | What It Returns |
|-----|-----------|----------------|
| `get_conference_head_to_head` | (conf1, conf2, season_start?, season_end?) | Season-by-season H2H records with win% |
| `get_trajectory_averages` | (conference, season_start?, season_end?) | Conference and FBS average benchmarks for trajectory comparison |

### Suggested cfb-app Pages

1. **Conference Dashboard** — `api.conference_comparison` for side-by-side conference rankings
2. **Conference vs Conference** — `get_conference_head_to_head()` for rivalry comparisons (e.g., SEC vs Big Ten)
3. **Conference Eras** — `conference_era_summary` for historical power shifts across realignment eras

---

## Feature Area 8: Advanced Play-Level Analytics

### Available Data Sources

| Source | Type | Rows | What It Powers |
|--------|------|------|----------------|
| `marts.play_epa` | Matview | 2,713,866 | **Every play** since ~2004: EPA, success, explosive, down/distance, field position, garbage time |
| `marts.team_playcalling_tendencies` | Matview | 491,666 | Run/pass rates by situation (down × distance × field position × score state) |
| `marts.team_situational_success` | Matview | 491,666 | Success rate + EPA by the same situation grid |

### Suggested cfb-app Pages

1. **Play-Calling Tendencies Explorer** — `team_playcalling_tendencies` for heatmaps (run rate by down × distance)
2. **Situational Success Dashboard** — `team_situational_success` for "where does this team thrive/struggle?"
3. **EPA Deep Dive** — `play_epa` aggregated ad-hoc for custom analytics (power users)

---

## Feature Area 9: Operational / Utility

| Source | Type | Rows | What It Powers |
|--------|------|------|----------------|
| `marts.data_freshness` | Matview | 23 | "Data last updated" indicators across 23 tables |
| `get_data_freshness()` | RPC | — | Returns freshness data for cfb-app status bar |
| `get_available_seasons()` | RPC | — | Season dropdown population |
| `get_available_weeks()` | RPC | — | Week dropdown population |
| `public.teams` / `teams_with_logos` | View | 1,899 | Team reference data for dropdowns, logos |

---

## Gap Analysis: What's Missing

### High-Value Gaps (Worth Building)

| Gap | What's Missing | Data Available? | Suggested Solution |
|-----|---------------|-----------------|-------------------|
| **Win Probability Chart** | No pre-computed WP by play for live-game-style charts | `metrics.pregame_win_probability` exists, `marts.play_epa` has play-level data | New matview: `play_win_probability` joining play_epa with pre-game WP |
| **Player Rankings/Awards** | No "Heisman Watch" or positional rankings across teams | `player_comparison` has percentiles | New API view: `player_rankings` with cross-team rank by position group |
| **Schedule Strength** | No future-schedule difficulty metric | `ratings.sp_ratings`, `core.games` exist | New matview: `team_schedule_strength` using opponent SP+ ratings |
| **Betting Analytics** | Lines data exists but no ATS record or cover trends | `betting.lines` (20K rows), `betting.team_ats` exist | New API view: `team_betting_trends` with ATS record, cover %, over/under % |
| **Draft History** | Draft picks exist but no team-level draft profile | `draft.draft_picks` (1.5K rows) exists | New API view: `team_draft_history` with picks by round, position |
| **Turnover Margin** | No explicit turnover margin tracking | Derivable from `defensive_havoc` + offensive turnovers | Add columns to `team_season_summary` or new matview |
| **Season Predictions** | No predictive model outputs | SP+ and Elo data exists for inputs | Future feature — would need a prediction model |

### Medium-Value Gaps

| Gap | Notes |
|-----|-------|
| **Player Transfer Tracking** | `transfer_portal_impact` uses star ratings as proxy because `recruiting.transfer_portal` has no `player_id`. Can't track individual player outcomes post-transfer. |
| **Special Teams Metrics** | `public.team_special_teams_sos` exists but no FG%, punt avg, return yards matview |
| **Injury Data** | Not available in CFBD API |
| **Live/Real-Time Scores** | Would need a separate data source (not CFBD) |

### Analytics Schema (Internal, Promotable)

These 5 matviews in `analytics.*` are internal but could be promoted to the contract:

| Matview | Rows | Potential Use |
|---------|------|---------------|
| `analytics.player_career_stats` | 628,886 | Career stat pages (multi-season player profiles) |
| `analytics.conference_standings` | 8,304 | Conference standings pages with ratings |
| `analytics.team_season_summary` | 9,374 | Largely duplicated by `marts.team_season_summary` — consider consolidating |
| `analytics.team_recruiting_trend` | 4,356 | Recruiting trend charts with 3-year rolling avg |
| `analytics.game_results` | 45,885 | Denormalized game results — could power a schedule/results grid |

**Recommendation:** Promote `player_career_stats` and `team_recruiting_trend` to the public contract. They fill real gaps. The others overlap with existing marts.

---

## Recommended cfb-app Page Structure

Based on the available analytics, here's a suggested page hierarchy:

```
cfb-app/
├── / (Home)
│   └── Season leaderboard, data freshness status
│
├── /teams
│   ├── /teams (Team Rankings/Leaderboard)         ← api.leaderboard_teams
│   └── /teams/[school]
│       ├── Overview tab                            ← api.team_detail + team_style_profile + team_talent_composite
│       ├── Season Stats tab                        ← situational_splits + defensive_havoc + scoring_opportunities
│       ├── Playcalling tab                         ← api.team_playcalling_profile + get_down_distance_splits()
│       ├── History tab                             ← api.team_history + team_season_trajectory
│       ├── Roster tab                              ← get_player_season_stats_pivoted()
│       ├── Recruiting tab                          ← api.recruiting_roi + recruiting_class
│       └── Schedule tab                            ← api.game_detail filtered by team
│
├── /players
│   ├── /players (Search + Leaders)                 ← get_player_search() + get_player_season_leaders()
│   ├── /players/[id]
│   │   ├── Profile                                 ← get_player_detail() + get_player_percentiles()
│   │   └── Game Log                                ← get_player_game_log()
│   └── /players/compare                            ← api.player_comparison
│
├── /games
│   ├── /games (Schedule + Scores)                  ← public.games
│   └── /games/[id]
│       ├── Summary                                 ← api.game_detail + game_line_scores
│       ├── Box Score                               ← api.game_box_score
│       └── Player Stats                            ← api.game_player_leaders
│
├── /matchups
│   └── /matchups/[team1]-vs-[team2]                ← api.matchup + matchup_edges
│
├── /coaches
│   └── /coaches/[name]                             ← api.coaching_history
│
├── /conferences
│   ├── /conferences (Rankings)                     ← api.conference_comparison
│   └── /conferences/[conf1]-vs-[conf2]             ← get_conference_head_to_head()
│
├── /recruiting
│   ├── /recruiting (Classes)                       ← recruiting_class
│   ├── /recruiting/roi                             ← api.recruiting_roi
│   └── /recruiting/portal                          ← api.transfer_portal_impact
│
└── /admin
    └── /admin/data-status                          ← get_data_freshness()
```

---

## Data Volume Summary

| Schema | Total Rows | Notes |
|--------|-----------|-------|
| marts | ~4.1M | 28 matviews, 2.7M from play_epa alone |
| analytics | ~697K | 5 matviews, 629K from player_career_stats |
| api | ~7.5M | 18 views, 6.4M from game_player_leaders |
| public | ~533K | 13 views, 341K from roster |
| scouting | ~30K | 1 matview (owned by cfb-scout) |

**Total analytical surface: ~12.8M rows across 64 queryable objects + 16 RPCs**
