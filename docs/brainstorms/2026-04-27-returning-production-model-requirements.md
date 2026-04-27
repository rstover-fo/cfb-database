# Returning Production Model — Player-Grain Requirements

**Date:** 2026-04-27
**Status:** Ready for planning
**Owner:** Rob
**Original spec source:** Player-Grain Returning Production Specification (Draft v1)
**Supersedes:** the original spec's bronze/silver/gold layering and 5-phase plan

---

## TL;DR

Build a player-season-grain returning production model on top of the existing CFBD ingestion (already loaded via `dlt`). Each player on each team-season gets a `returning_value` score; team rollups become a `SUM`. The headline win is **portal balance**: a transfer's contribution to source and destination is computed once at the player level and balances automatically.

This brainstorm narrows the original spec's 5-phase plan to **Phases 1–3** (player grain + continuity + quality weighting) and defers Phases 4–5 (scheme classifier + Connelly backtest) until v1 data shows whether the additional complexity earns its keep.

---

## What changed from the original spec

The spec was written for a generic agent. This requirements doc reconciles it with the actual project:

| Spec assumption | Actual project state | Resolution |
|---|---|---|
| Engine: DuckDB/MotherDuck | Supabase Postgres (DEC-001) | Use Postgres. No DuckDB. |
| Orchestrator: dbt-core | `dlt` pipelines + raw SQL migrations (DEC-002) | Use existing `src/schemas/migrations/` + `src/schemas/marts/` + `src/schemas/api/`. No dbt. |
| Bronze/silver/gold layering | 12 schemas already exist (`ref`, `core`, `stats`, `ratings`, `recruiting`, `marts`, `api`, `public`, etc.) | Reuse existing schemas. New `returning` schema for intermediate fct/dim tables only. |
| Bronze ingestion (9 endpoints) | All 9 endpoints already loaded via `dlt`, including `stats.player_returning` (CFBD's calibration target) | Skip "Phase 1 ingestion." Verify freshness only. |
| Hand-curate OC/DC for SEC + comparison teams | `marts.coaching_tenure` tracks HC only | Collapse to HC-only continuity factors (2 tiers, not 4). |
| Coordinator seed CSV | n/a | Eliminated. No seed required. |
| `seeds/conferences.csv` for P5/G5 split | `ref.conferences` already populated | Map P5/G5 in a SQL CASE in `dim_continuity_factors`. No seed. |
| `/player/portal` provides `player_id` | **Confirmed: it does not.** Existing marts code (`marts/025_transfer_portal_impact.sql`) explicitly notes "transfer_portal has NO player_id." dlt PK is `(first_name, last_name, origin, season)`. | Fuzzy name-match portal entries to prior-season `core.roster` to recover `player_id`. Track `match_confidence`. Unmatched rows kept as synthetic players for audit. |
| 5 phases over ~19 working days | Exploratory build, not a research project | Phases 1–3 only (~7 working days). Defer scheme + backtest. |
| Beat Connelly correlation gate | Counterfactual unclear (Team 360 may not surface returning production at all) | Net-new capability framing. No calibration gate. Compare to CFBD's `/player/returning` informally as a smoke test. |

---

## Goals

- **G1.** Player-season-grain returning production scores stored in a single denormalized matview, queryable by `(team, season)` and decomposable by position group, transfer status, and continuity factor.
- **G2.** Portal balance: every player movement (NFL departure, portal in/out, recruit add) is reflected automatically in both source and destination team scores. No manual reconciliation.
- **G3.** Quality-weighted base production: a returning All-SEC player scores higher than a returning rotational player at the same position.
- **G4.** Backtestable: pipeline runs for any `(year_from → year_to)` pair, not just 2025 → 2026.
- **G5.** Surfaced through the existing `api.*` contract for cfb-app consumption.

### Deferred goals (not in this scope)

- **D1.** Scheme-conditional position weights (run-first vs air-raid). Defer until v1 data shows static weights miss systematically.
- **D2.** Backtest correlation vs Connelly's published numbers on 2023→2024 and 2024→2025. Defer until model is stable enough that backtest results are interpretable.
- **D3.** OC/DC granularity in continuity factors. Defer until errors correlate with OC turnover.

---

## Non-goals

- **N1.** Beating Connelly on out-of-sample SP+ correlation. Not v1.
- **N2.** PFF integration. CFBD-derivable proxies only.
- **N3.** Injury modeling beyond a hand-curated season-ender lookup.
- **N4.** PBP-derived OL pressure or DL hurry stats.
- **N5.** NFL Draft probability modeling. Players who declared for the draft are flagged `departed_nfl=true` and excluded from target-season rosters.
- **N6.** FCS roster computation. FCS teams used only for `portal_fcs_to_fbs` source attribution.

---

## Architecture

### Schema layout

```
EXISTING (unchanged inputs):
  core.roster                          -- 340K rows; has player_id, games_played, games_started
  recruiting.transfer_portal           -- 14K rows; NO player_id; first_name+last_name+origin+season natural key
  recruiting.recruits                  -- ~67K rows; athlete_id, ranking, composite stars
  stats.player_season_stats            -- 1.2M rows; per-player season totals
  stats.player_returning               -- CFBD's calibration target (locally available)
  marts.coaching_tenure                -- HC tenure with gap detection
  marts.play_epa                       -- per-play EPA (used for QB/RB quality weighting)
  ratings.sp                           -- SP+ team ratings (used for competition_factor)
  ref.conferences                      -- conference metadata + P5/G5 mapping

NEW (this work):
  returning.fct_player_seasons         -- silver: (player_id, season) one row per player-season
  returning.fct_player_movements       -- silver: (player_id, transition_season) movement events
  returning.dim_continuity_factors     -- HC-based continuity factor lookup
  returning.dim_position_weights       -- static Connelly-style position weights
  returning.unmatched_portal_log       -- audit table for portal entries that couldn't name-match

  marts.player_returning_value         -- gold matview, grain (player_id, target_team, target_season)
  marts.team_returning_production      -- gold matview, grain (team, season), SUM of player rows

  api.team_returning_production        -- contract surface for cfb-app
  api.player_returning_value           -- per-player drill-in
```

**Naming follows existing conventions.** No bronze/silver/gold prefixes. Intermediate tables live in a new `returning` schema; outputs land in `marts` and `api` per `docs/SCHEMA_CONTRACT.md`.

### Refresh chain

`marts.player_returning_value` and `marts.team_returning_production` get added to `scripts/refresh_marts.py` and the `refresh_all_marts()` RPC in dependency order:

```
Layer 1: marts.team_season_summary, marts.coaching_tenure, marts.play_epa  (existing)
Layer 2: returning.fct_player_seasons, returning.fct_player_movements      (new)
Layer 3: marts.player_returning_value                                      (new)
Layer 4: marts.team_returning_production                                   (new)
```

---

## The returning_value formula

```
returning_value(player, target_team, target_season) =
    base_production(player, source_season)        -- quality, position-specific
  × position_weight(position)                     -- static Connelly weights (v1)
  × continuity_factor(movement_type)              -- HC-based, 2-tier
  × competition_factor(opponents_faced)           -- SP+ schedule strength, [0.7, 1.3]
  × health_factor(injury_status)                  -- 1.0 unless seeded otherwise
```

Each factor is a separate column in `marts.player_returning_value` so decomposition is queryable. The product is also stored.

### Continuity factors (simplified — HC-only)

| `movement_type` | factor | derivation |
|---|---|---|
| `returning_same_hc` | 1.00 | `marts.coaching_tenure` shows continuous tenure |
| `returning_new_hc` | 0.80 | HC change between source and target season |
| `portal_p5_to_p5` | 0.70 | both origin + destination conferences in P5 |
| `portal_g5_to_p5` | 0.55 | G5 origin → P5 destination |
| `portal_p5_to_g5` | 0.85 | downward FBS move |
| `portal_g5_to_g5` | 0.65 | lateral G5 |
| `portal_fcs_to_fbs` | 0.45 | FCS origin → FBS destination |
| `portal_juco_to_fbs` | 0.40 | JUCO origin (origin field contains "JC" or known JUCO) |
| `recruit_5star` | 0.30 | recruits.stars = 5 |
| `recruit_4star` | 0.15 | recruits.stars = 4 |
| `recruit_3star` | 0.05 | recruits.stars = 3 |
| `recruit_unrated` | 0.02 | recruits.stars = 0 OR not in recruits table |
| `returning_from_redshirt` | 0.25 | on roster prior season but games_played = 0 |
| `returning_from_injury_full` | 0.70 | listed in `seeds/injuries_season_ending.csv` |

P5/G5 split derived from `ref.conferences.classification` (or hand-curated CASE if classification field doesn't distinguish them).

### Position weights (static Connelly-style, v1)

Stored in `returning.dim_position_weights` keyed on `(position, scheme_archetype)` with `scheme_archetype = 'static'` for v1. Phase 4 deferral means the table is keyed for future scheme-conditional rows but only has the static set populated.

### base_production by position

| Position | v1 formula |
|---|---|
| QB | `0.4 × pass_epa_per_play_z + 0.3 × rush_epa_per_play_z + 0.3 × snaps_proxy_z` |
| RB | `0.5 × rush_epa_per_play_z + 0.2 × success_rate_z + 0.3 × carries_z` |
| WR/TE | `0.4 × yards_per_target_z + 0.3 × target_share_z + 0.3 × games_played_z` |
| OL | allocation of team line-yards z-score by `games_started` weight (no per-player stats available) |
| DL | `0.4 × tfl_per_game_z + 0.4 × sack_per_game_z + 0.2 × games_played_z` |
| LB | `0.3 × tackles_per_game_z + 0.3 × tfl_per_game_z + 0.2 × pbu_per_game_z + 0.2 × games_played_z` |
| DB | `0.3 × tackles_per_game_z + 0.3 × pbu_per_game_z + 0.2 × int_per_game_z + 0.2 × games_played_z` |
| ST | flat 0.05 unless K/P with games_played ≥ 50% of team games |

**Snap-count substitution.** CFBD does not provide reliable snap counts. v1 uses `games_played` as the universal denominator. This is a known fidelity loss but is internally consistent and avoids fake-precision z-scores.

`_z` is z-score within `(position, source_season)`. Output `base_production` is centered on 1.0 (avg starter), σ ≈ 0.4. Cap at `[0, 3.0]`.

---

## Data contracts

### `returning.fct_player_seasons`

Grain: one row per `(player_id, season)`. Built from `core.roster` LEFT JOIN `stats.player_season_stats`.

```sql
player_id           VARCHAR     -- core.roster.id (NB: varchar, not bigint)
season              INTEGER
team                VARCHAR
conference          VARCHAR
position            VARCHAR     -- canonicalized via existing position map
position_group      VARCHAR     -- QB|RB|WR_TE|OL|DL|LB|DB|ST (8 groups)
class               VARCHAR
games_played        INTEGER
games_started       INTEGER
-- ... per-position stats from stats.player_season_stats ...
recruiting_composite DECIMAL    -- joined from recruiting.recruits if available
loaded_at           TIMESTAMPTZ
```

### `returning.fct_player_movements`

Grain: one row per `(player_id, transition_season)`. `transition_season` is the season the player is moving *into*.

```sql
player_id              VARCHAR     -- real if matched, 'portal:<hash>' if unmatched
transition_season      INTEGER
movement_type          VARCHAR     -- enum, see continuity factor table
source_team            VARCHAR     -- nullable for new recruits
source_conference      VARCHAR
destination_team       VARCHAR     -- nullable for departures (NFL, retirement)
destination_conference VARCHAR
match_confidence       DECIMAL     -- 1.0 exact, 0.8 fuzzy, 0.0 synthetic
match_method           VARCHAR     -- 'roster_continuity'|'portal_exact'|'portal_fuzzy'|'recruit'|'unmatched'
loaded_at              TIMESTAMPTZ
```

### `marts.player_returning_value` (matview)

Grain: one row per `(player_id, target_team, target_season)`. Canonical output.

```sql
player_id            VARCHAR
target_team          VARCHAR
target_season        INTEGER
source_season        INTEGER
position             VARCHAR
position_group       VARCHAR
movement_type        VARCHAR
base_production      DECIMAL(6,3)
position_weight      DECIMAL(5,3)
continuity_factor    DECIMAL(4,2)
competition_factor   DECIMAL(4,2)
health_factor        DECIMAL(4,2)
returning_value      DECIMAL(6,3)
is_returning         BOOLEAN
is_portal_in         BOOLEAN
is_recruit           BOOLEAN
match_confidence     DECIMAL(3,2)
generated_at         TIMESTAMPTZ
```

### `marts.team_returning_production` (matview)

Grain: one row per `(team, season)`.

```sql
team                          VARCHAR
season                        INTEGER
returning_production_total    DECIMAL(6,3)   -- SUM of returning_value
returning_production_offense  DECIMAL(6,3)
returning_production_defense  DECIMAL(6,3)
rp_qb                         DECIMAL(6,3)
rp_rb                         DECIMAL(6,3)
rp_wr_te                      DECIMAL(6,3)
rp_ol                         DECIMAL(6,3)
rp_dl                         DECIMAL(6,3)
rp_lb                         DECIMAL(6,3)
rp_db                         DECIMAL(6,3)
n_returning_starters          INTEGER
n_portal_in                   INTEGER
n_portal_out                  INTEGER
n_recruits_contributing       INTEGER  -- recruits w/ returning_value > 0.10
cfbd_returning_production_pct DECIMAL(5,3)  -- joined from stats.player_returning for sanity
delta_vs_cfbd                 DECIMAL(5,3)
generated_at                  TIMESTAMPTZ
```

### `api.team_returning_production` and `api.player_returning_value`

Thin views over the matviews. Conform to existing `SECURITY INVOKER` + schema-grant pattern (per memory 2026-02-07 lesson).

---

## Data constraints and risks

### Confirmed constraints (verified against codebase)

1. **`recruiting.transfer_portal` has NO `player_id`.** Documented in `src/schemas/marts/025_transfer_portal_impact.sql:1`. Resolved via fuzzy name-match below.
2. **`core.roster.id` is varchar, not bigint.** Memory 2026-02-05. All joins must respect varchar typing.
3. **35 duplicate `school` names in `ref.teams`.** Memory 2026-02-05. Use `DISTINCT ON (school)` when joining team metadata.
4. **`recruiting.recruits` uses `athlete_id` (not `id`) and `ranking` (not `national_ranking`).** Memory 2026-02-05.
5. **CFBD `/play/stats` has 2000-record limit.** Not blocking for this work — quality weighting uses `stats.player_season_stats` rollups, not PBP.

### Engineering risks

1. **Portal name-match coverage.** Some portal entries will not match any prior-season roster row. Causes:
   - Spelling variants ("DJ" vs "Daniel")
   - Two players with same name from same origin (rare but happens)
   - JUCO/FCS arrivals with no prior FBS roster
   - Origin field inconsistency in CFBD ("Texas" vs "Texas Longhorns")

   Mitigation: explicit `match_confidence` column. v1 acceptance: ≥85% exact-match rate on 2024→2025 portal cycle (measurable post-load). Unmatched entries logged to `returning.unmatched_portal_log` for inspection. Budget 1 day for fuzzy-match heuristic tuning.

2. **`games_started` reliability.** Required for OL allocation. Spot-check confirms it's populated for most CFBD rosters but not 100%. v1 fallback: assume 13 starts for "primary" OL identified by `position_detail` (LT/LG/C/RG/RT) when `games_started` is null/0.

3. **`stats.player_season_stats` shape varies by position.** Different categories populated for QB vs DL vs ST. The `base_production` formula must handle missing fields gracefully (treat NULL as below-average for unused fields, not as an error).

4. **`base_production` z-score stability for small position pools.** Punters/long-snappers have tiny sample sizes per season. v1 caps `base_production` to `[0, 3.0]` and treats ST positions with a flat factor.

5. **Refresh time.** Five new dependency layers. Estimate <30s incremental for the new layers given ~75K players × 134 teams. Acceptable; matches existing matview refresh pattern.

---

## Phased build (3 phases, ~7 working days)

### Phase 1 — Player-season foundation + portal name-matching (target: 2 days)

**Scope:** Build `returning.fct_player_seasons` and `returning.fct_player_movements` with confidence-tracked portal joins. Build `returning.dim_continuity_factors` and `returning.dim_position_weights`.

**Tasks:**
1. Create `returning` schema in a new migration: `src/schemas/migrations/019_returning_schema.sql`.
2. Build `returning.fct_player_seasons` from `core.roster` ⨝ `stats.player_season_stats` ⨝ `recruiting.recruits`.
3. Build portal name-match SQL: exact match + soundex/levenshtein fuzzy fallback. Output to `returning.fct_player_movements` with `match_confidence`.
4. Build `returning.dim_continuity_factors` and `returning.dim_position_weights` as static lookup tables (seeded via INSERT in the migration).
5. Add to `scripts/refresh_marts.py` Layer 2.
6. Build `returning.unmatched_portal_log` audit table.

**Acceptance criteria:**
- `returning.fct_player_seasons` populated for seasons 2020–2025; row count ≥ 250K (5 seasons × ~50K players).
- ≥85% exact-match rate on portal entries for 2024→2025 cycle; remainder either fuzzy-matched (≥70% confidence) or logged.
- All four lookup tables populated; SUM(position_weight) = 2.0 across all positions (test).
- Tennessee 2025 → Kentucky 2026 transfer (Lance Heard) appears in `fct_player_movements` with `target_team='Kentucky'` and `movement_type='portal_p5_to_p5'`.

### Phase 2 — Returning value calculation with continuity (target: 2 days)

**Scope:** Build `marts.player_returning_value` and `marts.team_returning_production` matviews. v1 uses snap-fraction (= `games_played / 13`) as `base_production`; quality weighting comes in Phase 3.

**Tasks:**
1. Create `marts.player_returning_value` matview joining `fct_player_movements` + `fct_player_seasons` + lookups.
2. Compute `competition_factor` from `ratings.sp` joined to schedule.
3. Compute `health_factor` from a new `seeds/injuries_season_ending.csv` (initial set: 5–10 known season-enders for 2025, hand-entered).
4. Build `marts.team_returning_production` rollup with position-group breakdowns.
5. Add both to `refresh_all_marts()` RPC.
6. Build thin views in `src/schemas/api/` for both contract surfaces.
7. Update `docs/SCHEMA_CONTRACT.md`.

**Acceptance criteria:**
- Tennessee 2026: Lance Heard contributes `0` to Tennessee, `0.70 × his_2025_value` to Kentucky.
- Auburn 2026 OL `rp_ol` is in the bottom 10 of FBS (matches roster reality of 0 returning starters).
- `delta_vs_cfbd` column populated on every row; |delta| ≤ 0.20 for ≥75% of FBS teams (sanity check, not a hard gate).
- New API views pass existing test pattern (`tests/test_api_views.py` style).
- Refresh chain runs end-to-end in <60s incremental.

### Phase 3 — Quality-weighted base_production (target: 3 days)

**Scope:** Replace flat snap-fraction with the position-specific quality formulas in `§ The returning_value formula`.

**Tasks:**
1. Build `returning.calc_player_base_production()` SQL function with position-conditional logic.
2. Build z-score helper: a SQL macro / function for `(value, position, season)` → z-score within group, capped to ±3.
3. Allocate team OL line-yards z-score to individual OL via `games_started` weighting.
4. For DL/LB/DB: per-game rates instead of per-snap (since snaps unreliable).
5. Refresh `marts.player_returning_value` with the new `base_production`.
6. Eyeball test against PuntAndRally's published "best returnees" list (informal cross-reference, no hard gate).

**Acceptance criteria:**
- A returning All-SEC OL has higher `base_production` than a returning rotational OL on the same team.
- A 4-year starter QB with positive EPA has higher `base_production` than a backup QB on the same team.
- Position-specific NULL handling tested (QB with no defensive stats, DL with no passing stats — neither errors).
- Base-production distribution per position has σ between 0.30 and 0.55 (sanity check that z-scores aren't degenerate).
- All existing tests pass; new tests in `tests/test_returning_production.py`.

---

## Open decisions to resolve in planning

| ID | Decision | Default | Notes |
|---|---|---|---|
| RP-001 | Fuzzy match algorithm for portal → roster | Postgres `levenshtein` from `fuzzystrmatch` extension; threshold = 2 | If extension not enabled, request via Supabase MCP migration |
| RP-002 | How to handle players who appear on multiple teams in one season | Attribute to last team (= `target_season - 1` row); document in matview comment | Carries forward existing memory pattern (join on player_id+team+season) |
| RP-003 | Strength-of-opposition: pre-season SP+ vs final SP+ | Final SP+ (look-ahead OK because we score retrospectively) | Matches spec DEC-004 |
| RP-004 | OL "primary starter" identification when `games_started` is missing | Use `core.roster.position` IN (LT, LG, C, RG, RT) AND `games_played` ≥ 8 | Captures most starters without external data |
| RP-005 | NFL departure inference | Player on prior-season roster + not on target-season roster + not in portal + age >= 21 → `departed_nfl=true` | Imperfect; will misattribute some grad-transfers as NFL departures. Acceptable for v1 since both reduce returning_value to zero. |
| RP-006 | Whether `marts.player_returning_value` becomes a public API view in v1 | Internal-only matview in v1; promote to `api.player_returning_value` after eyeball validation | Lower contract risk; team-level view ships immediately |
| RP-007 | When to backfill historical returning production (seasons before 2020) | Defer; CFBD `/player/returning` is incomplete pre-2014 anyway | Phase 1 acceptance scoped to 2020–2025 |

---

## File map

```
cfb-database/
├── docs/
│   ├── brainstorms/
│   │   └── 2026-04-27-returning-production-model-requirements.md   # this file
│   ├── plans/
│   │   └── 2026-04-XX-feat-returning-production-plan.md            # next step (ce-plan)
│   └── SCHEMA_CONTRACT.md                                           # update in Phase 2
├── src/
│   └── schemas/
│       ├── migrations/
│       │   └── 019_returning_schema.sql                             # Phase 1
│       ├── marts/
│       │   ├── 030_player_returning_value.sql                       # Phase 2
│       │   └── 031_team_returning_production.sql                    # Phase 2
│       ├── api/
│       │   ├── 020_team_returning_production.sql                    # Phase 2
│       │   └── 021_player_returning_value.sql                       # Phase 3 (gated)
│       └── functions/
│           └── calc_player_base_production.sql                      # Phase 3
├── seeds/
│   └── injuries_season_ending.csv                                   # Phase 2
├── scripts/
│   └── refresh_marts.py                                             # extend Layer 2-4
└── tests/
    └── test_returning_production.py                                 # all phases
```

---

## Glossary

- **Returning production** — estimated proportion of a team's prior-year on-field production that is available in the upcoming year.
- **Continuity factor** — multiplier applied to a player's prior-year production to discount for system change (HC change, portal move).
- **Movement type** — taxonomy of how a player ended up on the target team (returner, portal-in by tier, recruit by stars, redshirt, injury return).
- **Match confidence** — fidelity score for the player_id assigned to a portal entry. 1.0 = exact name+origin match, 0.8 = fuzzy, 0.0 = unmatched (synthetic id).
- **CFBD** — collegefootballdata.com.
- **Connelly** — Bill Connelly's ESPN returning production model. Public benchmark, not v1's competition.

---

## References

- Original spec (provided in brainstorm input)
- `docs/SCHEMA_CONTRACT.md` — public API surface rules
- `src/schemas/marts/025_transfer_portal_impact.sql` — verified portal/player_id constraint
- `docs/solutions/database-issues/dlt-child-table-lateral-join.md` — dlt traversal patterns
- Memory 2026-02-05: data quality findings, transfer_portal player_id absence
- Memory 2026-02-07: SECURITY INVOKER schema-grant pattern (apply to new api views)

---

## Phase ordering note

Each phase is independently shippable. After Phase 1 you have movement data. After Phase 2 you have a working (if crude) returning production score. After Phase 3 you have the quality-weighted version. The deferred Phases 4 (scheme classifier) and 5 (Connelly backtest) can be revisited as a follow-up sprint based on what the v1 numbers show.
