# CFB Database Analytics Schema Design

> Design document for expanded endpoint coverage, analytics-optimized schema, and ML extensions.
> Created: 2025-01-28

## Overview

This design expands the CFB database from a data warehouse into a **comprehensive analytics platform** supporting:

1. **Dashboards & reports** — pre-built visualizations, team pages, leaderboards
2. **Interactive exploration** — filtering, comparisons, custom queries
3. **Game-day / real-time** — live scores, win probability, play-by-play
4. **Research tool** — deep dives, matchup analysis, player evaluation
5. **Machine learning** — game prediction, spread modeling, play-call analysis

## Architecture

### Layered Schema Design

| Layer | Schema | Purpose | Implementation | Refresh |
|-------|--------|---------|----------------|---------|
| **Raw** | `ref`, `core`, `stats`, `ratings`, `recruiting`, `betting`, `metrics` | Source of truth | Normalized tables | Pipeline loads |
| **Marts** | `marts` | Pre-aggregated analytics | Materialized views | After pipeline + pg_cron |
| **API** | `api` | App-friendly shapes | Regular views | Live (query-time) |
| **Features** | `features` | ML-ready feature vectors | Materialized views | After pipeline |
| **Predictions** | `predictions` | Model outputs | Tables | Model runs |
| **Live** | `live` (future) | Real-time game data | Tables + Supabase realtime | WebSocket |

### Why This Works for Supabase

- PostgREST exposes views as API endpoints automatically
- Materialized views = fast dashboard queries
- Regular views = flexible, always-current joins
- Realtime only works on tables (not mat views), so live data needs dedicated tables
- RLS can gate access if user auth is added later

---

## Entity Model

### Dimension Entities (the "nouns")

| Entity | Table | Description |
|--------|-------|-------------|
| Team | `ref.teams` | Football programs (Alabama, Ohio State, etc.) |
| Player | `core.rosters` | Individual athletes, linked to teams by season |
| Coach | `ref.coaches` | Coaching staff |
| Conference | `ref.conferences` | Organizational grouping |
| Venue | `ref.venues` | Stadiums |
| Position | `ref.positions` | Position reference with groupings |

### Fact Entities (the "events/measurements")

| Entity | Table | Grain |
|--------|-------|-------|
| Game | `core.games` | One row per game |
| Drive | `core.drives` | One row per possession |
| Play | `core.plays` | One row per snap (partitioned by season) |
| Player Game Stats | `stats.player_game_stats` | Player × Game |
| Player Season Stats | `stats.player_season_stats` | Player × Season |
| Team Season Stats | `stats.team_season_stats` | Team × Season |
| Ratings | `ratings.*` | Team × Season × Source |
| Poll Rankings | `ratings.poll_rankings` | Team × Season × Week × Poll |
| Recruiting | `recruiting.recruits` | Player × Commit |
| Betting Lines | `betting.lines` | Game × Provider |

### Key Relationships

```
Team (central hub)
├── Conference (many-to-many over time)
├── Players (via rosters, by season)
├── Coaches (by season)
├── Games (home/away)
│   ├── Drives
│   │   └── Plays
│   └── Betting Lines
├── Ratings (by season)
├── Stats (by season)
└── Recruiting (by class year)
```

---

## Schema Details

### Raw Layer Tables

#### ref.teams (enriched)
```sql
id              bigint PRIMARY KEY
school          text NOT NULL
mascot          text
abbreviation    text
conference_id   bigint REFERENCES ref.conferences
division        text                    -- "fbs", "fcs"
color           text                    -- Primary hex
alt_color       text                    -- Secondary hex
logo_url        text
```

#### ref.positions (new)
```sql
id              text PRIMARY KEY        -- "QB", "RB", "WR"
name            text                    -- "Quarterback"
side            text                    -- "offense", "defense", "special_teams"
position_group  text                    -- "passer", "rusher", "receiver", etc.
```

#### core.rosters (new — critical)
```sql
id              bigint                  -- CFBD player ID
team_id         bigint REFERENCES ref.teams
season          integer
first_name      text
last_name       text
position        text
height          integer                 -- inches
weight          integer                 -- pounds
year            integer                 -- 1=FR, 2=SO, etc.
jersey          integer
home_city       text
home_state      text

PRIMARY KEY (id, team_id, season)       -- Handles transfers
```

#### ratings.poll_rankings (new)
```sql
season              integer
week                integer
poll_type           text                -- "AP Top 25", "Coaches Poll", "CFP"
team_id             bigint REFERENCES ref.teams
rank                integer
first_place_votes   integer
points              integer

PRIMARY KEY (season, week, poll_type, team_id)
```

#### stats.advanced_team_stats (wire existing config)
```sql
team_id             bigint REFERENCES ref.teams
season              integer
offense_ppa         numeric
defense_ppa         numeric
offense_success     numeric
defense_success     numeric
offense_explosiveness numeric
defense_explosiveness numeric
offense_power       numeric
offense_stuff_rate  numeric
offense_line_yards  numeric

PRIMARY KEY (season, team_id)
```

#### metrics.wepa_team_season (new)
```sql
team_id             bigint REFERENCES ref.teams
season              integer
wepa_overall        numeric             -- Opponent-adjusted EPA
wepa_passing        numeric
wepa_rushing        numeric
wepa_defense        numeric

PRIMARY KEY (season, team_id)
```

---

### Marts Layer (Materialized Views)

#### marts.team_season_summary
- **Grain:** Team × Season
- **Metrics:** W-L, conf record, PPG, margin, SP+, Elo, FPI, recruiting rank, EPA
- **Use:** Team pages, comparisons, trends

#### marts.team_epa_season
- **Grain:** Team × Season
- **Metrics:** EPA/play, success rate, explosiveness, EPA tier, WEPA
- **Use:** Advanced analytics, benchmarking

#### marts.player_career
- **Grain:** Player (career totals)
- **Metrics:** Career stats, teams, seasons played, PPA
- **Use:** Player pages, draft analysis

#### marts.game_results
- **Grain:** Game
- **Metrics:** Score, spread result, EPA diff, win prob, ATS/OU result
- **Use:** Game logs, betting analysis

#### marts.situational_splits
- **Grain:** Team × Season
- **Metrics:**
  - Down & distance: EPA by down, conversion rates, standard/passing downs
  - Red zone: trips, TD rate, scoring rate, goal line
  - Field position: backed up, own territory, scoring position
  - Late & close: 2nd half within 16, 4th quarter one-score
  - Two-minute drill
  - Play type: rush/pass EPA, success rate, explosiveness, tendencies
  - Power & stuff: power success, stuff rate, line yards
- **Use:** Coaching analysis, tendencies, matchup prep

#### marts.defensive_havoc
- **Grain:** Team × Season
- **Metrics:** Stuffs, sacks, INTs, forced fumbles, havoc rate, opp EPA
- **Use:** Defensive analysis

#### marts.scoring_opportunities
- **Grain:** Team × Season
- **Metrics:** Scoring rate, TD rate, points/drive, turnover rate
- **Use:** Drive efficiency analysis

#### marts.matchup_history
- **Grain:** Team × Team
- **Metrics:** H2H record, recent form, avg margin, series dates
- **Use:** Matchup pages, rivalry analysis

#### marts.recruiting_class
- **Grain:** Team × Year
- **Metrics:** National rank, star breakdown, blue chip ratio, position groups
- **Use:** Recruiting analysis

#### marts.coach_record
- **Grain:** Coach × Team
- **Metrics:** Tenure, W-L, win %, SP+ trajectory, recruiting avg
- **Use:** Coaching analysis

#### marts.conference_standings
- **Grain:** Conference × Season × Team
- **Metrics:** Conf record, overall record, conf rank
- **Use:** Standings pages

---

### API Layer (Regular Views)

| View | Purpose | Key Joins |
|------|---------|-----------|
| `api.team_detail` | Single team page | Current season + ratings + recruiting + coach |
| `api.team_history` | Multi-season trends | Season summaries + EPA + recruiting |
| `api.game_detail` | Single game page | Teams + betting + advanced stats + venue |
| `api.player_detail` | Single player page | Career stats + current team + roster info |
| `api.matchup` | Head-to-head comparison | Matchup history + current season context |
| `api.leaderboard_teams` | Flexible leaderboards | Season summary + EPA + situational |

---

### ML Extensions

#### features.team_game_features
- **Grain:** Game × Team
- **Features:**
  - Target: won, margin, covered
  - Context: is_home, neutral_site
  - Rolling stats: points avg, margin avg, turnover margin (season-to-date)
  - Efficiency: EPA/play, success rate, explosiveness
  - Ratings: SP+, Elo, FPI, WEPA
  - Situational: 3rd down rate, red zone rate, havoc rate
  - Opponent mirror features
  - Derived: sp_diff, elo_diff, epa_diff
- **Critical:** All stats computed BEFORE each game (no data leakage)

#### features.play_features
- **Grain:** Play
- **Features:** down, distance, yard_line, field_zone, score_diff, game_state, time pressure, team tendencies
- **Use:** Play-call prediction

#### predictions.game_predictions
- **Storage:** Model outputs (win prob, spread prediction, picks)
- **Calibration:** Post-game actual results for model evaluation

#### predictions.model_registry
- **Metadata:** Model versions, training windows, hyperparameters, performance metrics

---

## Endpoint Coverage Plan

### Phase 1: Critical (blocks core functionality)

| Endpoint | Table | Effort |
|----------|-------|--------|
| `/roster` | `core.rosters` | Medium |
| `/rankings` | `ratings.poll_rankings` | Low |
| `/games/players` | `stats.player_game_stats` | Medium |

### Phase 2: High (enables key analytics)

| Endpoint | Table | Effort |
|----------|-------|--------|
| `/stats/season/advanced` | `stats.advanced_team_stats` | Low (wire config) |
| `/game/box/advanced` | `stats.game_advanced_stats` | Medium |
| `/talent` | `recruiting.team_talent` | Low |
| `/player/returning` | `stats.returning_production` | Low |
| `/wepa/team/season` | `metrics.wepa_team_season` | Low |

### Phase 3: Medium (enriches analysis)

| Endpoint | Table | Effort |
|----------|-------|--------|
| `/ppa/games` | `metrics.ppa_games` | Low (wire config) |
| `/ppa/players/games` | `metrics.ppa_players_games` | Low (wire config) |
| `/metrics/wp` | `metrics.win_probability` | Medium (wire config) |
| `/player/usage` | `stats.player_usage` | Low |
| `/games/weather` | `core.game_weather` | Low |
| `/games/media` | `core.game_media` | Low (wire config) |
| `/recruiting/groups` | `recruiting.position_groups` | Low |

### Phase 4: Nice-to-have

- `/wepa/players/*` (3 endpoints)
- `/records`
- `/draft/positions`, `/draft/teams`
- `/stats/categories`
- `/calendar`

### Phase 5: Real-time (future)

- `/scoreboard`
- `/live/plays`

### API Budget Estimate

| Endpoint | Call Pattern | Est. Calls |
|----------|--------------|------------|
| `/roster` | 130 teams × 22 years | ~2,860 |
| `/rankings` | 22 years × ~17 weeks | ~374 |
| `/games/players` | ~800 games/year × 22 years | ~17,600 |
| `/game/box/advanced` | ~800 games/year × 22 years | ~17,600 |
| Others | Various | ~5,000 |
| **Total** | | **~45,000 calls** |

Fits within 75k/month budget with room to spare.

---

## Utility Functions

### Garbage Time Filter
```sql
CREATE OR REPLACE FUNCTION is_garbage_time(play core.plays)
RETURNS boolean AS $$
BEGIN
    RETURN (
        (play.period = 4 AND ABS(play.score_diff) > 28) OR
        (play.period >= 3 AND ABS(play.score_diff) > 35)
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;
```

### EPA Benchmarks
- Elite: EPA/play >= 0.16
- Above avg: >= 0.05
- Average: >= -0.05
- Below avg: >= -0.15
- Struggling: < -0.15

---

## Refresh Strategy

| Layer | Trigger | Method |
|-------|---------|--------|
| Raw tables | Pipeline loads | dlt merge |
| Marts (materialized views) | After pipeline | `REFRESH MATERIALIZED VIEW CONCURRENTLY` |
| API views | N/A | Query-time (regular views) |
| Feature views | After marts refresh | `REFRESH MATERIALIZED VIEW CONCURRENTLY` |

Recommended: Use `pg_cron` (Supabase Pro) or external scheduler for mart refreshes.

---

## Frontend Stack Recommendation

- **Framework:** React + Next.js
- **Charting:** Recharts (simple) or Tremor (dashboard-focused)
- **Data fetching:** Supabase client libraries
- **Auth:** Supabase Auth (if needed)

---

## Implementation Phases

| Phase | Focus | Deliverables |
|-------|-------|--------------|
| **1** | Endpoint coverage | Wire Phase 1-2 endpoints, backfill rosters/rankings |
| **2** | Marts layer | Create all materialized views, refresh scripts |
| **3** | API layer | Create all API views, test with Supabase client |
| **4** | Features layer | ML feature views, training data exports |
| **5** | App scaffold | Next.js app with team/game/player pages |
| **6** | Advanced features | Predictions, real-time, betting analysis |

---

## Open Questions

1. **Supabase tier:** Free vs Pro? Pro needed for pg_cron, more storage
2. **Auth requirements:** Public data or user-gated?
3. **Backfill priority:** Full history or recent years first?
4. **Model deployment:** Where to run ML models (Supabase Edge Functions, external)?

---

## References

- CFBD API: https://collegefootballdata.com
- cfbfastR: https://cfbfastR.sportsdataverse.org
- Supabase: https://supabase.com/docs
- EPA/Success Rate research: various CFB analytics community sources
