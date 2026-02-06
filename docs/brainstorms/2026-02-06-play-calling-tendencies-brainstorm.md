# Brainstorm: Play-Calling Tendencies Analytics

**Date:** 2026-02-06
**Status:** Ready for planning
**Sprint:** 8

## What We're Building

A layered play-calling analytics system that answers:
- **How does Team X call plays?** Run/pass ratios, play types by situation
- **What works in specific situations?** 3rd-down conversion rates, red zone efficiency, EPA by game state
- **How do teams adjust?** Behavior shifts when leading vs trailing, tempo changes across quarters

This is the first analytics sprint focused on schematic/strategic insight rather than player-level data.

## Why This Approach

### Hybrid Architecture (Granular Base -> Aggregated Summaries -> Team Profiles)

We chose the hybrid approach over pure bottom-up or top-down because:

1. **Granular base mart** makes ad-hoc analysis possible without rebuilding
2. **Aggregated summary marts** pre-compute the most common queries (team tendencies by situation)
3. **Profile mart** gives cfb-app a fast, one-row-per-team-season view for dashboards
4. **Follows Sprint 7 pattern**: matview + thin API view delivers <5ms queries

### Data Foundation

- **2.5M+ plays** with full game state (down, distance, score, field position, quarter)
- **183K drives** with outcome tracking
- **EPA data from 2014+** — targeting 10 seasons of depth
- Existing `marts.play_epa` provides the base EPA calculations

## Key Decisions

### 1. Mart Architecture (5 marts, layered)

**Layer 1 — Base:**
- `play_situation_base` — Every play from 2014+ tagged with situation buckets:
  - Down (1-4)
  - Distance group (short 1-3, medium 4-6, long 7+)
  - Field zone (own 1-25, own 26-50, opp 49-25, red zone 24-1)
  - Score differential bucket (big lead 14+, small lead 1-13, tied, small deficit, big deficit)
  - Quarter/half phase (Q1, Q2, Q3, Q4, OT)
  - Play type (run, pass, other)
  - Builds on `marts.play_epa` for EPA values

**Layer 2 — Aggregated:**
- `team_playcalling_tendencies` — Team-level play mix by situation
  - Grain: team + season + situation bucket combination
  - Metrics: play count, run rate, pass rate, play type distribution
- `team_situational_success` — Effectiveness by situation
  - Grain: team + season + situation bucket combination
  - Metrics: success rate, avg EPA, explosiveness rate, conversion rate (3rd/4th down)
- `team_game_script` — Behavior by game phase and score state
  - Grain: team + season + score_diff_bucket + quarter_phase
  - Metrics: pace (plays/minute), run rate, avg EPA, play type mix

**Layer 3 — Profile:**
- `team_playcalling_profile` — One row per team-season identity summary
  - Overall run rate, early-down run rate, 3rd-down conversion rate
  - Red zone TD rate, pace (plays/game), early-down aggression (pass rate on 1st/2nd)
  - Leading vs trailing run rate delta, garbage time threshold behavior
  - Percentile ranks within season (like player_comparison pattern)

### 2. API Surface

- `api.team_playcalling_profile` — Thin view over profile mart (dashboard consumption)
- `api.team_playcalling_tendencies` — Thin view over tendencies mart (drill-down)

### 3. RPCs

- `get_team_situational_success(team, season)` — Parameterized drill-down for a specific team
- `get_team_game_script(team, season)` — Game script analysis for a specific team

### 4. Season Range

**2014+** — Aligns with EPA/PPA data availability. ~10 seasons gives meaningful trend analysis while keeping mart sizes manageable.

### 5. Situation Bucketing Strategy

Keep buckets coarse enough for statistical significance but fine enough for insight:

| Dimension | Buckets | Rationale |
|-----------|---------|-----------|
| Down | 1, 2, 3, 4 | Natural grouping |
| Distance | Short (1-3), Medium (4-6), Long (7+) | Standard football categories |
| Field zone | Own deep, Own mid, Opp mid, Red zone | 4 zones, ~25 yards each |
| Score diff | Big lead (14+), Small lead (1-13), Tied (0), Small deficit, Big deficit | Behavioral breakpoints |
| Game phase | Q1, Q2, Q3, Q4, OT | Quarter-level |

### 6. Refresh Strategy

- All 5 marts added to `marts.refresh_all()` dependency chain
- Base mart depends on `marts.play_epa`
- Layer 2 marts depend on base mart
- Profile mart depends on Layer 2 marts
- Estimated refresh time: TBD (depends on base mart size)

## Open Questions

1. **Base mart size**: With 2.5M plays and ~10 seasons, the base mart could be 1-2M rows. Is that fine for matview refresh time?
2. **Garbage time filter**: Should we exclude garbage time plays from tendencies? (Score diff > 28 in Q4?) Or flag them and let the consumer decide?
3. **Pace calculation**: Plays per minute requires clock data. Do we have snap time or just drive-level time? May need to approximate from drive data.
4. **Opponent adjustment**: Should situational success rates be adjusted for opponent strength? That adds complexity but makes the data more meaningful.
5. **Should we include special teams plays** (punts, FGs, kickoffs) in tendencies, or focus on offensive plays only?

## Sprint Sizing Estimate

| Component | Count | Complexity |
|-----------|-------|------------|
| Marts (matviews) | 5 | Medium-High (base mart is the heaviest) |
| API views | 2 | Low (thin views) |
| RPCs | 2 | Medium (parameterized queries) |
| SQL files | ~9 | — |
| Tests | ~30-40 | Integration tests for all marts + views + RPCs |
| Index additions | ~5-8 | On situation columns in base mart |

This is a **medium-large sprint** — comparable to Sprint 7 in scope. The base mart SQL will be the most complex piece.

## Next Steps

Run `/workflows:plan` to create the implementation plan with task breakdown.
