---
title: "feat: Returning Production Model — Player-Grain (Phases 1-3)"
type: feat
status: active
date: 2026-04-27
origin: docs/brainstorms/2026-04-27-returning-production-model-requirements.md
---

# feat: Returning Production Model — Player-Grain (Phases 1-3)

## Overview

Build a player-season-grain returning production model on top of existing CFBD ingestion. Output is two new matviews (`marts.player_returning_value`, `marts.team_returning_production`) and one PostgREST contract surface (`api.team_returning_production`). The headline win is portal balance: a transfer's contribution to source and destination is computed once at the player level and balances automatically when rolled up.

Scope is Phases 1–3 from the requirements doc (player grain + continuity + quality weighting). Phase 4 (scheme-conditional weights) and Phase 5 (Connelly backtest) are deferred to follow-up plans.

---

## Problem Frame

CFBD's `/player/returning` endpoint returns a single team-level percentage. That can't answer "did Lance Heard transferring from Tennessee to Kentucky add net production to Kentucky 2026?" because the math happens above the player layer. Existing public models (Connelly, PuntAndRally) have the same structural limit.

Pushing the working grain down to player-season unblocks portal balance, position-group decomposition, and quality weighting. Once each player has a `returning_value` for `(player, target_team, target_season)`, team scores are a `SUM`. cfb-app gains a decomposable returning production view through a new `api.*` surface.

(see origin: [docs/brainstorms/2026-04-27-returning-production-model-requirements.md](../brainstorms/2026-04-27-returning-production-model-requirements.md))

---

## Requirements Trace

- **R1.** Player-season-grain returning production scores in a single denormalized matview, queryable by `(team, season)`, decomposable by position group, transfer status, and continuity factor. *(origin G1)*
- **R2.** Portal balance: every player movement (NFL departure, portal in/out, recruit add) reflected automatically in both source and destination team scores; no manual reconciliation. *(origin G2)*
- **R3.** Quality-weighted base production: a returning All-SEC player scores higher than a returning rotational player at the same position. *(origin G3)*
- **R4.** Backtestable: pipeline runs for any `(year_from → year_to)` pair — not just 2025→2026. *(origin G4)*
- **R5.** Surfaced through the existing `api.*` contract for cfb-app consumption, conforming to `SECURITY INVOKER` + schema-grant conventions. *(origin G5)*
- **R6.** Sanity comparison column (`delta_vs_cfbd`) populated on every team rollup row using `stats.player_returning`; |delta| ≤ 0.20 for ≥75% of FBS teams. *(origin Phase 2 acceptance)*

---

## Scope Boundaries

- No scheme-archetype classifier or scheme-conditional position weights (deferred to Phase 4 follow-up plan).
- No Connelly correlation backtest, no validation gate (deferred to Phase 5 follow-up plan).
- No PFF, PBP-derived OL pressure, or NFL Draft probability modeling.
- No FCS roster computation. FCS rosters used only for `portal_fcs_to_fbs` source attribution.
- No OC/DC granularity in continuity factors; HC-only with 2-tier `returning_same_hc` / `returning_new_hc`.
- No injury feed beyond a hand-curated `seeds/injuries_season_ending.csv` lookup (5–10 entries for v1).
- No promotion of `marts.player_returning_value` to `api.player_returning_value` until v3 eyeball validation passes (decision RP-006).

### Deferred to Follow-Up Work

- Phase 4 plan: scheme classifier from `bronze.cfbd_team_stats_season` + scheme-conditional position weight A/B.
- Phase 5 plan: backtest 2023→2024 and 2024→2025, sourcing Connelly's published numbers from ESPN archives.
- Historical returning-production backfill before season 2020 (decision RP-007).

---

## Context & Research

### Relevant Code and Patterns

- **Closest matview analog:** [src/schemas/marts/025_transfer_portal_impact.sql](../../src/schemas/marts/025_transfer_portal_impact.sql) — same WITH-CTE pattern, same `DROP MATERIALIZED VIEW IF EXISTS … CASCADE` preamble, same indexing convention (`CREATE UNIQUE INDEX ON … (team, season)`).
- **Closest api view analog:** [src/schemas/api/017_transfer_portal_impact.sql](../../src/schemas/api/017_transfer_portal_impact.sql) — `SECURITY INVOKER` view over the matview with anon `SELECT` grant.
- **Closest function analog:** [src/schemas/functions/get_recruiting_roi.sql](../../src/schemas/functions/get_recruiting_roi.sql) and [src/schemas/functions/get_player_percentiles.sql](../../src/schemas/functions/get_player_percentiles.sql) — `SET search_path = ''` pattern with fully-qualified table names.
- **Refresh chain:** [scripts/refresh_marts.py](../../scripts/refresh_marts.py) and [src/schemas/functions/refresh_all_marts.sql](../../src/schemas/functions/refresh_all_marts.sql) — five-layer dependency refresh; new layers added in dependency order.
- **Existing inputs:** `core.roster` (340K rows, varchar `id`), `recruiting.transfer_portal` (14K rows, no `player_id`), `recruiting.recruits` (uses `athlete_id` and `ranking`), `stats.player_season_stats` (1.2M rows), `stats.player_returning` (CFBD calibration target, locally available), `marts.coaching_tenure`, `marts.play_epa`, `ratings.sp`.
- **Test inventory pattern:** [tests/test_marts.py](../../tests/test_marts.py) `MARTS_VIEWS` list and [tests/test_api_views.py](../../tests/test_api_views.py) `API_VIEWS` list — every new matview/view added here.

### Institutional Learnings

- **`recruiting.transfer_portal` has no `player_id`** — documented inline in `marts/025_transfer_portal_impact.sql:1`. Resolved here via fuzzy name-match (decision RP-001).
- **`core.roster.id` is varchar, not bigint** (memory 2026-02-05). All joins must respect varchar typing.
- **35 duplicate `school` names in `ref.teams`** (memory 2026-02-05). Use `DISTINCT ON (school)` when joining team metadata.
- **`recruiting.recruits` uses `athlete_id` (not `id`) and `ranking` (not `national_ranking`)** (memory 2026-02-05).
- **Transfer players can appear on multiple teams in a single season**; default attribution = last team (decision RP-002).
- **SECURITY INVOKER requires schema grants** (memory 2026-02-07). New `rp` schema needs `GRANT USAGE` to `anon`/`authenticated`; tables get `GRANT SELECT`.
- **Always test as `SET ROLE anon`** — MCP queries run as superuser and mask permission bugs.

### External References

- Postgres `fuzzystrmatch` extension (`levenshtein`, `soundex`) — already a Supabase-supported extension. Verified usable; confirm enabled in Phase 1.

---

## Key Technical Decisions

- **Schema layering reuses existing convention** (no bronze/silver/gold). New `rp` schema for intermediate fct/dim tables; outputs land in `marts` and `api`. *(see origin)*
- **Continuity factor is HC-only, 2-tier.** `returning_same_hc` / `returning_new_hc` collapses spec's 4-tier coordinator scheme. Driven by `marts.coaching_tenure`. No coordinator seed file. *(see origin)*
- **Portal name-matching uses Postgres `fuzzystrmatch.levenshtein` with threshold ≤ 2** against `(first_name, last_name, origin_team, season-1)` from `core.roster`. Match confidence: 1.0 exact, 0.8 fuzzy (levenshtein 1–2), 0.0 unmatched (synthetic id `portal:<md5_hash>`). *(decision RP-001)*
- **Player attribution for multi-team season players** = `last team in source_season`. Documented in matview comment. *(decision RP-002)*
- **Strength of opposition** uses **final** SP+ from `ratings.sp` for the source season (look-ahead OK because retrospective). *(decision RP-003)*
- **OL "primary starter" identification** = `position IN ('LT','LG','C','RG','RT')` AND `games_played ≥ 8` when `games_started` is null. *(decision RP-004)*
- **NFL departure inference** = on prior-season roster + not on target-season roster + not in portal + age ≥ 21 → `departed_nfl=true`. Imperfect (will misattribute some grad-transfers as NFL departures), accepted because both reduce returning_value to zero. *(decision RP-005)*
- **`marts.player_returning_value` is internal-only in v1.** Only `api.team_returning_production` is exposed. Promote to `api.player_returning_value` only after v3 eyeball validation. *(decision RP-006)*
- **Backfill scope** = seasons 2020–2025 only. Pre-2020 deferred. *(decision RP-007)*
- **Numbering convention:** new top-level migration `019_returning_schema.sql` creates the namespace + DDL for `returning.*` tables. New matviews `marts/030_player_returning_value.sql`, `marts/031_team_returning_production.sql`. New api view `api/020_team_returning_production.sql`. New functions in `src/schemas/functions/` with descriptive names.
- **All new objects use `SECURITY INVOKER`** with explicit `GRANT USAGE`/`GRANT SELECT` to `anon` and `authenticated`, conforming to the 2026-02-07 hardening pattern.

---

## Open Questions

### Resolved During Planning

- **RP-001 — fuzzy match algorithm:** Postgres `fuzzystrmatch.levenshtein` with threshold ≤ 2 on `(first_name, last_name)` plus exact `origin_team` match. If extension not enabled, request via Supabase MCP migration as the first sub-step of U1.
- **RP-002 — multi-team players in one season:** attribute to last team. Documented in matview comment.
- **RP-003 — SP+ flavor:** final season SP+ from `ratings.sp`.
- **RP-004 — OL primary starter:** position IN OL set AND games_played ≥ 8.
- **RP-005 — NFL departure heuristic:** roster diff + not in portal + age ≥ 21.
- **RP-006 — `player_returning_value` promotion:** internal-only matview in v1; promotion to `api.*` deferred.
- **RP-007 — historical backfill:** 2020–2025 only.

### Deferred to Implementation

- **Levenshtein threshold tuning.** v1 starts at 2; if exact-match rate is ≥85% on 2024→2025 cycle, no tuning needed. If lower, raise to 3 with confidence drop to 0.6.
- **Index strategy on `marts.player_returning_value`.** Plan establishes `(team, season)` and `(player_id, target_season)` as the primary access patterns; secondary indexes determined by EXPLAIN of typical cfb-app queries during Phase 2.
- **Final NULL-handling shape for position-specific stats.** Phase 3 will need to confirm whether `stats.player_season_stats` returns NULL or 0 for missing categories — verify against actual rows during U10.
- **Refresh-time impact.** Estimate <30s incremental for the new layers; measure during U7 and adjust if it exceeds 60s.
- **JUCO origin detection rule.** v1 heuristic: `origin` field contains "CC", "JC", or appears in a known JUCO list. Final list determined by inspecting actual `recruiting.transfer_portal.origin` values during U3.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

### Data flow

```
core.roster (year=N-1) ──┐
stats.player_season_stats ┤
recruiting.recruits      ─┴──>  rp.fct_player_seasons
                                       │
core.roster (year=N) ────────┐         │
recruiting.transfer_portal ──┤         │
recruiting.recruits (year=N)─┴──>  rp.fct_player_movements
                                       │
rp.dim_continuity_factors ──┐  │
rp.dim_position_weights ────┤  │
ratings.sp (competition) ──────────┤  │
seeds/injuries_season_ending.csv ──┴──┴──> marts.player_returning_value (matview)
                                                    │
                                                    └──> marts.team_returning_production (SUM rollup matview)
                                                                │
                                                                └──> api.team_returning_production (PostgREST view)
```

### Movement classification (decision tree, illustrative)

```
For each (player_id, target_season):
  if player on roster(target_season) AND on roster(target_season-1):
    if HC same as prior year (per coaching_tenure):  movement_type = returning_same_hc
    else:                                            movement_type = returning_new_hc
    if games_played(prior) = 0 and on prior roster:  movement_type = returning_from_redshirt
    if in seeds/injuries_season_ending and severity ≥ full season:
                                                     movement_type = returning_from_injury_full
  elif portal entry (matched by name+origin to prior roster):
    classify by (source_conference P5/G5, dest_conference P5/G5):
      portal_p5_to_p5, portal_g5_to_p5, portal_p5_to_g5, portal_g5_to_g5,
      portal_fcs_to_fbs, portal_juco_to_fbs
  elif in recruits(class=target_season):
    by stars: recruit_5star | recruit_4star | recruit_3star | recruit_unrated
```

### Returning value formula (per row)

```
returning_value = base_production
                × position_weight        (from dim_position_weights, scheme_archetype='static')
                × continuity_factor      (from dim_continuity_factors, by movement_type)
                × competition_factor     ([0.7, 1.3] from avg opponent SP+ rank)
                × health_factor          (1.0 default; 0.40/0.0 from injury seed)
```

Five factors stored as separate columns; product also stored as `returning_value`. This makes decomposition queryable without recomputation.

---

## Implementation Units

### Phase 1: Player-season foundation + portal name-matching

- U1. **Create `rp` schema and DDL for fct/dim tables**

**Goal:** Stand up the `rp` schema with empty tables, the audit log, grants, and indexes. No data yet — pure DDL.

**Requirements:** R1, R5

**Dependencies:** None

**Files:**
- Create: `src/schemas/019_returning_schema.sql`
- Create: `tests/test_returning_schema.py`

**Approach:**
- `CREATE SCHEMA IF NOT EXISTS returning`.
- `CREATE EXTENSION IF NOT EXISTS fuzzystrmatch` (idempotent; required for U3).
- DDL for `rp.fct_player_seasons`, `rp.fct_player_movements`, `rp.dim_continuity_factors`, `rp.dim_position_weights`, `rp.unmatched_portal_log`. Column types match the data contracts in the requirements doc verbatim.
- `GRANT USAGE ON SCHEMA returning TO anon, authenticated`.
- `GRANT SELECT ON ALL TABLES IN SCHEMA returning TO anon, authenticated`.
- `REVOKE INSERT, UPDATE, DELETE` from anon to match the 2026-02-07 hardening pattern.
- Indexes: `(player_id, season)` PK on fct_player_seasons; `(player_id, transition_season)` PK on fct_player_movements; `(team, season)` index on fct_player_seasons; `(destination_team, transition_season)` index on fct_player_movements.

**Patterns to follow:**
- `src/schemas/migrations/grant_read_access_for_security_invoker.sql` for the grant block.
- `src/schemas/002_core.sql` for schema-creation idiom.

**Test scenarios:**
- Happy path: schema `rp` exists with expected 6 tables (`fct_player_seasons`, `fct_player_movements`, `dim_continuity_factors`, `dim_position_weights`, `unmatched_portal_log`, `injuries_season_ending`).
- Happy path: `fuzzystrmatch` extension is loaded (`SELECT levenshtein('a','b')` returns 1).
- Edge case: anon role can `SELECT` but not `INSERT` on `rp.fct_player_seasons` (verify with `SET ROLE anon`).
- Edge case: re-running migration is idempotent (no-op on second apply).

**Verification:**
- All five tables exist, are empty, and pass anon read / no-write checks.
- Migration applies cleanly via `python scripts/run_migrations.py`.

---

- U2. **Populate `rp.fct_player_seasons` from roster + stats + recruits**

**Goal:** One row per `(player_id, season)` for seasons 2020–2025, joining `core.roster` ⨝ `stats.player_season_stats` ⨝ `recruiting.recruits` with canonicalized position groups.

**Requirements:** R1, R4

**Dependencies:** U1

**Files:**
- Create: `src/schemas/019_returning_schema.sql` (extend with `INSERT INTO rp.fct_player_seasons` block) — or split into a separate idempotent loader function `rp.refresh_fct_player_seasons()`. Implementer decides; prefer the loader-function variant so the DDL migration stays pure.
- Create: `src/schemas/functions/refresh_fct_player_seasons.sql`
- Test: `tests/test_returning_production.py`

**Approach:**
- LEFT JOIN `core.roster` to `stats.player_season_stats` on `(roster.id::text = player_season_stats.player_id, roster.year = player_season_stats.season, roster.team = player_season_stats.team)`. Handle the varchar/bigint cast.
- LEFT JOIN `recruiting.recruits` on `(athlete_id = roster.id::text)` to pull `composite_rating` and `stars`.
- LEFT JOIN team metadata via `DISTINCT ON (school)` over `ref.teams` to dedupe the 35-duplicate-school issue.
- Position canonicalization: map raw CFBD position strings to 8-group canonical (QB, RB, WR_TE, OL, DL, LB, DB, ST). Use a CASE expression; defer extracting to a seed CSV until Phase 3.
- Idempotent loader: `TRUNCATE rp.fct_player_seasons; INSERT INTO …`. Wrap in transaction.

**Patterns to follow:**
- `marts/025_transfer_portal_impact.sql` for the WITH-CTE join structure.
- `functions/refresh_player_mart.sql` for the loader-function shape.

**Test scenarios:**
- Happy path: row count for season=2025 ≥ 25,000 (sanity bound from existing `core.roster` size).
- Happy path: a known QB (e.g., Carson Beck for 2024 Georgia) has `position_group='QB'` and non-null `stat_pass_yards`.
- Edge case: a defensive lineman with no offensive stats has NULL/0 in offensive stat columns but non-null in defensive ones.
- Edge case: a player on roster with no `player_season_stats` row appears with NULL stats and non-null roster fields (proves the LEFT JOIN side, not INNER).
- Integration: position canonicalization covers all 26 raw position strings → exactly 8 canonical groups.
- Integration: re-running `refresh_fct_player_seasons()` produces identical row counts (idempotency).

**Verification:**
- Row count on `rp.fct_player_seasons` for seasons 2020–2025 is ≥ 250,000 (5 seasons × ≥50K players).
- No NULL `position_group` rows.
- All `player_id` values match an existing `core.roster.id`.

---

- U3. **Populate `rp.fct_player_movements` with portal name-matching**

**Goal:** Build the movement-event table. Three sources: roster continuity (returning players), portal entries (with name-matched `player_id`), recruit class. Track `match_confidence` and write unmatched portal entries to `rp.unmatched_portal_log`.

**Requirements:** R2, R4

**Dependencies:** U1, U2

**Files:**
- Create: `src/schemas/functions/refresh_fct_player_movements.sql`
- Test: `tests/test_returning_production.py` (extend)

**Approach:**
- Three CTEs unioned together:
  1. **Returning continuity** — players in both `(target_season-1)` and `target_season` rosters. Compare HC via `marts.coaching_tenure` to assign `returning_same_hc` vs `returning_new_hc`. Set `match_confidence = 1.0`, `match_method = 'roster_continuity'`.
  2. **Portal events** — for each `recruiting.transfer_portal` row in `season = target_season`:
     - Try exact match: `lower(portal.first_name) = lower(roster.first_name) AND lower(portal.last_name) = lower(roster.last_name) AND portal.origin = roster.team AND roster.year = target_season - 1`. → `match_method='portal_exact'`, `confidence=1.0`.
     - Fallback fuzzy: `levenshtein(lower(portal.first_name||last_name), lower(roster.first_name||last_name)) ≤ 2 AND portal.origin = roster.team AND roster.year = target_season - 1`. → `match_method='portal_fuzzy'`, `confidence=0.8`.
     - Unmatched: emit synthetic id `'portal:' || md5(first_name||last_name||origin||season::text)`, `confidence=0.0`, write to `rp.unmatched_portal_log`.
     - Classify P5/G5 via `ref.conferences.classification` (or hand-curated CASE if classification doesn't distinguish).
  3. **Recruit class** — `recruiting.recruits` for the target year, mapped to `recruit_5star`/`4star`/`3star`/`unrated` by `stars` column.
- NFL departure inference (informational only — no row in fct_player_movements; surfaces as `is_returning=false` in U5):
  - Logged in a separate audit query but does not need a row here.
- Idempotent loader.

**Patterns to follow:**
- `marts/025_transfer_portal_impact.sql` for the `recruiting.transfer_portal` join idiom.
- `marts/023_coaching_tenure.sql` for HC continuity detection (LAG window).

**Test scenarios:**
- Happy path: a known returner (e.g., DJ Lagway, Florida 2024 → 2025) has `movement_type='returning_same_hc'` or `'returning_new_hc'` depending on coaching change.
- Happy path: Lance Heard (Tennessee 2025 → Kentucky 2026) appears with `target_team='Kentucky'`, `source_team='Tennessee'`, `movement_type='portal_p5_to_p5'`, `match_confidence=1.0`.
- Happy path: a 4-star 2026 recruit at Alabama appears with `movement_type='recruit_4star'`, `source_team=NULL`, `match_confidence=1.0`.
- Edge case: portal entry with spelling variant (e.g., "DJ" vs "Daniel") name-matches via levenshtein with `match_method='portal_fuzzy'` and `match_confidence=0.8`.
- Edge case: portal entry with no roster match emits synthetic `player_id` starting with `portal:` and is logged in `unmatched_portal_log`.
- Edge case: portal entry where origin team has 35-duplicate-name issue — verify the join uses `DISTINCT ON (school)` so it doesn't fanout.
- Error path: re-running the loader produces identical row counts (idempotency); existing rows are TRUNCATE-and-replace, not duplicated.
- Integration: a JUCO origin (e.g., origin matches `%CC%` or known JUCO pattern) classifies as `portal_juco_to_fbs`, not `portal_g5_to_p5`.
- Integration: an FCS origin classifies as `portal_fcs_to_fbs` regardless of destination tier.

**Verification:**
- Exact-match rate on portal entries for `target_season=2025` is ≥85%.
- Synthetic-ID rate (unmatched) is ≤15%, all logged.
- A spot-check of 5 hand-picked portal moves resolves to the correct destination team.

---

- U4. **Seed `dim_continuity_factors` and `dim_position_weights`**

**Goal:** Populate the two static lookup tables that drive `returning_value`. Both are seeded via INSERT statements in the migration.

**Requirements:** R1

**Dependencies:** U1

**Files:**
- Modify: `src/schemas/019_returning_schema.sql` (add INSERT block at end of migration, after DDL)
- Test: `tests/test_returning_production.py` (extend)

**Approach:**
- `dim_continuity_factors` rows: 14 entries from the requirements doc table (`returning_same_hc` 1.00, `returning_new_hc` 0.80, six portal tiers, four recruit tiers, redshirt, injury_full).
- `dim_position_weights` rows: 11 entries keyed on `(position, scheme_archetype='static')`. Values from origin §6 (QB 0.223, WR 0.175, TE 0.175, RB 0.031, OL 0.396 [splittable later for T/G/C], EDGE 0.149, DT 0.149, LB 0.192, CB 0.165, S 0.165, ST 0.000).
- Use `INSERT … ON CONFLICT DO UPDATE` for idempotency.

**Test scenarios:**
- Happy path: `dim_continuity_factors` has 14 rows after migration applies.
- Happy path: `dim_position_weights` SUM(weight) WHERE scheme_archetype='static' equals 2.0 ± 0.001.
- Edge case: re-applying migration does not duplicate rows or change values.
- Integration: every `movement_type` enum value used in U3's classification logic exists in `dim_continuity_factors` (no orphan refs).

**Verification:**
- Sum-to-2.0 invariant test passes.
- All `movement_type` values that U3 emits are joinable to a continuity factor row.

---

### Phase 2: Returning value calculation with continuity

- U5. **Build `marts.player_returning_value` matview (snap-fraction base)**

**Goal:** First end-to-end returning value computation, one row per `(player_id, target_team, target_season)`. v1 uses snap-fraction (`games_played / 13`) as `base_production`; quality weighting comes in U10.

**Requirements:** R1, R2, R5

**Dependencies:** U2, U3, U4

**Files:**
- Create: `src/schemas/marts/030_player_returning_value.sql`
- Test: `tests/test_returning_production.py` (extend); `tests/test_marts.py` (add to `MARTS_VIEWS`)

**Approach:**
- WITH-CTEs:
  1. `base` — JOIN `fct_player_movements` with `fct_player_seasons` (matched on `(player_id, source_season)` where source_season = target_season - 1). For recruits, source_season is NULL and `base_production = 0` (the recruit_Nstar continuity factor is the only signal).
  2. `competition` — for each player-source-season, AVG opponent SP+ rank from `ratings.sp` joined to schedule. Map rank to factor: `1.0 + (67 - avg_rank) / 67 * 0.3`, clamped `[0.7, 1.3]`. Default 1.0 when no schedule data.
  3. `health` — LEFT JOIN to a (yet-to-be-created) `seeds/injuries_season_ending` table (loaded as `rp.injuries_season_ending` in U8). Default 1.0.
  4. `factors` — JOIN `dim_continuity_factors` on `movement_type` and `dim_position_weights` on `(position, 'static')`.
  5. Final SELECT computes `returning_value = base × pos_weight × continuity × competition × health`; emits all five factor columns.
- `CREATE UNIQUE INDEX` on `(player_id, target_team, target_season)`; secondary indexes on `(target_team, target_season)` and `(target_season, returning_value DESC)`.
- Match the 2026-02-07 grant pattern: `GRANT SELECT TO anon, authenticated`; no DML grants.

**Patterns to follow:**
- `src/schemas/marts/025_transfer_portal_impact.sql` for the WITH-CTE-combined structure and grants.
- `src/schemas/marts/020_player_comparison.sql` for the matview + `(player_id, season)` PK pattern.

**Test scenarios:**
- Happy path — Lance Heard scenario: row exists with `target_team='Kentucky'`, `target_season=2026`, `movement_type='portal_p5_to_p5'`, `continuity_factor=0.70`. No row for `target_team='Tennessee'` for him.
- Happy path: all five factor columns are non-null on every row.
- Happy path: `returning_value` equals product of the five factors within float tolerance.
- Happy path: a known returning All-SEC starter has `is_returning=true`, `is_portal_in=false`, `is_recruit=false`.
- Edge case: a recruit row has `base_production=0`, `is_recruit=true`, and `returning_value > 0` only because of the recruit continuity factor.
- Edge case: a player with no schedule data (rare) gets `competition_factor=1.0` instead of NULL or division-by-zero.
- Edge case: synthetic-id portal entries (unmatched) still produce a row with `match_confidence=0.0` carried forward.
- Integration: SUM of `returning_value` for one team-season (e.g., Kentucky 2026) is finite and within sane bounds (`[0, 200]`).

**Verification:**
- Row count for `target_season=2026` is ≥ 30,000 (≥ 134 teams × ~225 players each between roster + portal + recruits).
- Lance Heard test row passes spot check.

---

- U6. **Build `marts.team_returning_production` rollup matview**

**Goal:** SUM rollup with position-group breakdowns and `delta_vs_cfbd` calibration column.

**Requirements:** R1, R2, R5, R6

**Dependencies:** U5

**Files:**
- Create: `src/schemas/marts/031_team_returning_production.sql`
- Test: `tests/test_returning_production.py` (extend); `tests/test_marts.py` (add to `MARTS_VIEWS`)

**Approach:**
- GROUP BY `(target_team, target_season)`:
  - `returning_production_total = SUM(returning_value)`.
  - `returning_production_offense` = SUM where `position_group IN ('QB','RB','WR_TE','OL')`.
  - `returning_production_defense` = SUM where `position_group IN ('DL','LB','DB')`.
  - Per-position: `rp_qb`, `rp_rb`, `rp_wr_te`, `rp_ol`, `rp_dl`, `rp_lb`, `rp_db`.
  - `n_returning_starters` = COUNT WHERE `is_returning=true AND games_started ≥ 8`.
  - `n_portal_in` = COUNT WHERE `is_portal_in=true`.
  - `n_portal_out` = COUNT FROM `fct_player_movements` WHERE `source_team=team AND destination_team != team`.
  - `n_recruits_contributing` = COUNT WHERE `is_recruit=true AND returning_value > 0.10`.
- LEFT JOIN `stats.player_returning` aggregated to `(team, season)` to populate `cfbd_returning_production_pct` and compute `delta_vs_cfbd`.
- Unique index on `(team, season)`; secondary on `(season, returning_production_total DESC)`.

**Patterns to follow:**
- `src/schemas/marts/025_transfer_portal_impact.sql` (rollup pattern, including LEFT JOIN to perf metrics).

**Test scenarios:**
- Happy path: row count = COUNT(DISTINCT team) × 6 seasons (2020–2025) for FBS teams, ~800 rows.
- Happy path: SUM over team rollup = SUM of player rows for same team-season (round-trip identity).
- Happy path — Auburn 2026 OL `rp_ol` is in the bottom 10 FBS values for `season=2026` (matches the 0-returning-starters-OL reality).
- Happy path: `returning_production_offense + returning_production_defense + rp_st = returning_production_total ± 0.01`.
- Edge case: a team with no portal activity has `n_portal_in=0` and no NULLs (defaults via COALESCE).
- Edge case: a team for which CFBD did not publish a returning-production number (rare/historical) has NULL `cfbd_returning_production_pct` and NULL `delta_vs_cfbd`, not a row drop.
- Integration: |delta_vs_cfbd| ≤ 0.20 for ≥75% of FBS teams in `target_season=2026` (sanity check, not a hard gate; logs a warning if violated).

**Verification:**
- Row counts and identity tests pass.
- Auburn 2026 spot check passes.

---

- U7. **Wire to refresh chain + build `api.team_returning_production` view**

**Goal:** Add new matviews to `scripts/refresh_marts.py` and `refresh_all_marts()` in dependency order. Build the PostgREST contract surface.

**Requirements:** R5

**Dependencies:** U6

**Files:**
- Modify: `scripts/refresh_marts.py` (add to refresh layer ordering)
- Modify: `src/schemas/functions/refresh_all_marts.sql` (extend with new matviews in correct layer)
- Create: `src/schemas/api/020_team_returning_production.sql`
- Test: `tests/test_api_views.py` (add to `API_VIEWS` inventory; add anon read test)

**Approach:**
- Refresh ordering:
  - Layer 1 (existing): team_season_summary, coaching_tenure, play_epa, etc.
  - Layer 2 (new): `refresh_fct_player_seasons()` then `refresh_fct_player_movements()`. Both are functions, not matviews — so they're invoked, not REFRESHed.
  - Layer 3 (new): `REFRESH MATERIALIZED VIEW CONCURRENTLY marts.player_returning_value`.
  - Layer 4 (new): `REFRESH MATERIALIZED VIEW CONCURRENTLY marts.team_returning_production`.
- `api.team_returning_production` is a thin `SECURITY INVOKER` view selecting from `marts.team_returning_production`. No filtering; cfb-app applies its own filters.
- `GRANT SELECT ON api.team_returning_production TO anon, authenticated`.
- Function definitions use `SET search_path = ''` and fully-qualified names per 2026-02-07 hardening.

**Patterns to follow:**
- `src/schemas/api/017_transfer_portal_impact.sql` for the SECURITY INVOKER view shape.
- `src/schemas/functions/refresh_all_marts.sql` for the layered refresh idiom.

**Test scenarios:**
- Happy path: `refresh_all_marts()` runs end-to-end without error; new matviews populated.
- Happy path: anon role can `SELECT` from `api.team_returning_production` (verify with `SET ROLE anon`).
- Edge case: re-running `refresh_all_marts()` is idempotent (no row growth, no orphan rows).
- Edge case: a fresh run of `refresh_all_marts()` completes in <60s incremental (warn if exceeds).
- Error path: anon cannot `INSERT/UPDATE/DELETE` on `api.team_returning_production`.
- Integration: `tests/test_api_views.py::test_api_views_have_data` passes for `team_returning_production`.

**Verification:**
- New entry appears in `tests/test_marts.py::MARTS_VIEWS` and `tests/test_api_views.py::API_VIEWS`.
- Refresh chain runs in <60s incremental.
- anon read works; anon write fails with permission error.

---

- U8. **Update `SCHEMA_CONTRACT.md` and seed `injuries_season_ending`**

**Goal:** Publish the contract surface for cfb-app and seed the initial injury list. This unit is small but materially affects downstream consumers.

**Requirements:** R5

**Dependencies:** U7

**Files:**
- Modify: `docs/SCHEMA_CONTRACT.md` (add `api.team_returning_production` row to the cfb-app API Views table)
- Modify: `docs/SCHEMA_CONTRACT.md` (add `marts.team_returning_production` and `marts.player_returning_value` to the marts table; mark player-level as **Internal**)
- Create: `seeds/injuries_season_ending.csv`
- Create: `src/schemas/migrations/load_injuries_seed.sql` (loads CSV into `rp.injuries_season_ending` table; create the table here too if not in U1)

**Approach:**
- CSV columns: `player_id, player_name, team, injury_season, severity, target_season_status, source_url, source_date`.
- Initial 5–10 entries: hand-curated season-enders for 2025 (e.g., a known QB ACL tear, a returning OL MCL). Use public/official sources only for `source_url`.
- Severity enum: `season` (out for season), `partial` (returns mid-season).
- target_season_status: `out`, `limited`, `full`.
- The injuries seed is small but contractual — documented in SCHEMA_CONTRACT.md so cfb-app knows the source.

**Test scenarios:**
- Test expectation: none (seed data + docs change). Validated by U5/U6 tests that exercise the `health_factor` join.

**Verification:**
- `docs/SCHEMA_CONTRACT.md` has the new entries under cfb-app's API Views and marts sections.
- `rp.injuries_season_ending` table populated from CSV; row count = CSV row count.

---

### Phase 3: Quality-weighted base_production

- U9. **Z-score helper SQL function**

**Goal:** Reusable z-score-within-group function so per-position base_production formulas don't duplicate the math.

**Requirements:** R3

**Dependencies:** U2

**Files:**
- Create: `src/schemas/functions/z_score_within_position_season.sql`
- Test: `tests/test_returning_production.py` (extend)

**Approach:**
- Function signature: `rp.z_score_within_position_season(value DECIMAL, mean DECIMAL, stddev DECIMAL) RETURNS DECIMAL`.
- Returns `(value - mean) / NULLIF(stddev, 0)`, capped at ±3, defaulting to 0 when stddev is 0 (degenerate position-season).
- Mean and stddev computed per `(position, season)` in the calling matview using `AVG()` / `STDDEV_POP()` window functions, then passed in.
- Marked `IMMUTABLE PARALLEL SAFE` for query optimization.
- Uses `SET search_path = ''` and fully-qualified types.

**Test scenarios:**
- Happy path: `z_score(10, 5, 2)` returns 2.5.
- Edge case: `z_score(5, 5, 0)` returns 0 (no division by zero error).
- Edge case: `z_score(100, 5, 1)` returns 3.0 (capped at +3).
- Edge case: `z_score(-100, 5, 1)` returns -3.0 (capped at -3).
- Edge case: NULL value input returns NULL.

**Verification:**
- Function callable from any schema as `rp.z_score_within_position_season(...)`.
- All five test scenarios pass.

---

- U10. **Position-conditional `calc_player_base_production` function**

**Goal:** Replace the snap-fraction `base_production` placeholder with the position-specific quality-weighted formula. This is the substantive Phase 3 deliverable.

**Requirements:** R3

**Dependencies:** U2, U9

**Files:**
- Create: `src/schemas/functions/calc_player_base_production.sql`
- Test: `tests/test_returning_production.py` (extend)

**Execution note:** Test-first. The position-specific formulas are hard to verify by reading SQL; write a fixture of 8 representative players (one per position group, mix of star and rotational) before authoring the function.

**Approach:**
- Function signature: `rp.calc_player_base_production(player_id VARCHAR, source_season INTEGER) RETURNS DECIMAL`.
- Internally CASE on `position_group`:
  - **QB:** `0.4 * z(pass_epa_per_play) + 0.3 * z(rush_epa_per_play) + 0.3 * z(games_played)`. Source `pass_epa_per_play` from `marts.play_epa` aggregated to player-season.
  - **RB:** `0.5 * z(rush_epa_per_play) + 0.2 * z(success_rate) + 0.3 * z(carries)`.
  - **WR_TE:** `0.4 * z(yards_per_target) + 0.3 * z(target_share) + 0.3 * z(games_played)`. `target_share` = targets / team total targets in source_season.
  - **OL:** allocate team line-yards z-score by `games_started` weight (per RP-004 OL primary heuristic). v1 returns the team line-yards z multiplied by `games_started / 13`.
  - **DL:** `0.4 * z(tfl_per_game) + 0.4 * z(sack_per_game) + 0.2 * z(games_played)`.
  - **LB:** `0.3 * z(tackles_per_game) + 0.3 * z(tfl_per_game) + 0.2 * z(pbu_per_game) + 0.2 * z(games_played)`.
  - **DB:** `0.3 * z(tackles_per_game) + 0.3 * z(pbu_per_game) + 0.2 * z(int_per_game) + 0.2 * z(games_played)`.
  - **ST:** flat 0.05 unless K/P with `games_played ≥ 0.5 × team games`, else flat 1.0 for the qualifying K/P.
- Center output around 1.0 (avg starter), σ ≈ 0.4. Cap at `[0, 3.0]` post-formula.
- NULL handling: any per-stat NULL is treated as the position-season mean (z=0), not as an error or as 0 raw.
- `SET search_path = ''`; fully qualified.

**Patterns to follow:**
- `src/schemas/functions/get_player_percentiles.sql` for the position-conditional CASE structure.
- `marts.play_epa` for per-play EPA access (already a Layer 1 matview).

**Test scenarios:**
- Happy path — a returning All-SEC OL (e.g., Cayden Green at Missouri) has `base_production` higher than a returning rotational OL on the same team.
- Happy path — Tavion Gadson (8 sacks / 15.5 TFL) has top-quartile `base_production` among DLs.
- Happy path: a 4-year starter QB with positive EPA has higher `base_production` than a backup QB on the same team.
- Edge case: position-specific NULL handling — a QB with no defensive stats does not error; a DL with no passing stats does not error.
- Edge case: a player from a degenerate position-season pool (e.g., only 2 punters with stats) returns a finite, capped value, not NULL or +∞.
- Integration: per-position σ of `base_production` is between 0.30 and 0.55 (sanity check that z-scores aren't degenerate). Computed and asserted in the test.
- Integration: distribution mean per position is centered between 0.9 and 1.1 (avg-starter anchor).

**Verification:**
- All position groups return finite values for known-good fixture players.
- The All-SEC > rotational comparison passes for at least 5 hand-picked pairs.
- σ and mean sanity checks pass per position.

---

- U11. **Refresh `marts.player_returning_value` with quality-weighted base + extend tests**

**Goal:** Swap the snap-fraction placeholder in U5 for the new `calc_player_base_production` function. Run the full refresh chain. Add the comprehensive test set for the v1 deliverable.

**Requirements:** R3, R6

**Dependencies:** U10

**Files:**
- Modify: `src/schemas/marts/030_player_returning_value.sql` (replace base_production CTE with `rp.calc_player_base_production(player_id, source_season)`)
- Modify: `tests/test_returning_production.py` (final test pass; cover all phases)
- Modify: `tests/test_marts.py` (no change needed; matview already in inventory from U5)

**Approach:**
- Replace the snap-fraction CTE in `030_player_returning_value.sql` with a JOIN to a CTE that calls `rp.calc_player_base_production`.
- Re-run `python scripts/refresh_marts.py` to populate matviews with new values.
- Verify `delta_vs_cfbd` distribution shifts (should generally tighten vs the snap-fraction baseline) and log results.
- Final pytest sweep across all new tests.

**Test scenarios:**
- Happy path: matview row count unchanged after base_production swap (proves we're updating values, not the grain).
- Happy path: distribution of `base_production` shifts as expected (mean ~1.0, σ ~0.4 per position).
- Happy path: a 2024 returning player's quality-weighted `base_production` differs from his Phase 2 snap-fraction value by a non-trivial amount (verifies the swap actually applied).
- Edge case: player flagged as `is_recruit=true` still has `base_production=0` (recruits don't get quality-weighted base; only continuity factor matters).
- Integration: end-to-end smoke — fresh DB ingest → refresh chain → query `api.team_returning_production` for `team='Kentucky', season=2026` → returns one row with all expected columns populated.
- Integration: re-running the full chain produces identical values (bit-for-bit reproducibility within float tolerance).

**Verification:**
- Full pytest suite passes.
- `delta_vs_cfbd` distribution: ≥75% of teams have `|delta| ≤ 0.20` for `season=2026`.
- The All-SEC > rotational invariant holds when checked via the matview, not just the function output.

---

## System-Wide Impact

- **Interaction graph:** New matviews are added to `refresh_all_marts()` Layers 2–4. cfb-app gains one new PostgREST endpoint (`/api/team_returning_production`). cfb-scout is unaffected (no `core.roster`/`recruiting.recruits` schema changes).
- **Error propagation:** `calc_player_base_production` returns 0 (not NULL or error) for missing stats so downstream `returning_value` math doesn't NULL-cascade. NULL handling tested in U10.
- **State lifecycle risks:** Refresh ordering matters — `fct_player_movements` must populate before `marts.player_returning_value` refreshes. If a partial refresh runs, `delta_vs_cfbd` may transiently look wrong; mitigated by `REFRESH MATERIALIZED VIEW CONCURRENTLY` keeping prior data visible until commit.
- **API surface parity:** `api.team_returning_production` follows the same SECURITY INVOKER + grant pattern as the 17 existing `api.*` views. No deviation.
- **Integration coverage:** End-to-end smoke test in U11 verifies ingest → refresh → API query path. This covers the cross-layer scenarios that unit tests on individual matviews miss.
- **Unchanged invariants:** `core.roster`, `recruiting.recruits`, `recruiting.transfer_portal`, `stats.player_returning`, and all existing `marts.*` and `api.*` views are unchanged. The plan adds new objects only; no existing schema is mutated.

---

## Risks & Dependencies

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Portal exact-match rate falls below 85% on real 2024→2025 data | Med | Med (acceptance criterion miss) | U3 includes fuzzy match with confidence drop; raise levenshtein threshold to 3 if exact-only is too strict; `unmatched_portal_log` makes the gap inspectable |
| `fuzzystrmatch` extension not enabled in Supabase | Low | High (U3 blocked) | U1 first sub-step is `CREATE EXTENSION IF NOT EXISTS fuzzystrmatch`; if Supabase blocks it, fall back to soundex (built-in) with confidence cap of 0.6 |
| `stats.player_season_stats` data shape varies more than expected per position | Med | Med (U10 NULL-handling complexity) | U10 explicitly tests NULL handling for cross-position fields; treats missing stats as z=0 not as errors |
| Refresh chain exceeds 60s incremental | Low | Low (acceptance criterion miss, not blocking) | Measure during U7; if exceeded, switch to non-concurrent refresh on the smaller `fct_*` tables |
| `delta_vs_cfbd` shows systematic bias > 0.20 on >25% of teams | Med | Low (sanity check, not a gate) | Logs a warning; flagged for follow-up Phase 4/5 plan rather than blocking ship |
| `marts.coaching_tenure` HC continuity has gaps for 2026 (latest season) | Med | Med (continuity factor wrong) | U3 falls back to `returning_same_hc` (1.0) when HC data is missing for either side of the comparison; logged for review |
| Position canonicalization map is incomplete | Low | Med (NULL position_groups break joins) | U2 tests assert no NULL position_groups; CASE expression has explicit `ELSE 'ST'` catch-all |
| 35 duplicate `school` names in `ref.teams` cause join fanout | Low | High (silently inflates rollup) | All `ref.teams` joins explicitly use `DISTINCT ON (school)` per the documented gotcha |
| `recruiting.transfer_portal.origin` field has too many JUCO formatting variants | Med | Low (some JUCOs misclassified as G5) | U3 v1 heuristic accepts the misclassification; followup PR refines after inspecting actual values |

---

## Documentation / Operational Notes

- **`docs/SCHEMA_CONTRACT.md`** updated in U8. New `api.team_returning_production` row in cfb-app API Views table; new entries in marts table for `team_returning_production` (public) and `player_returning_value` (internal).
- **`docs/pipeline-manifest.md`** does not need updating — no new ingestion endpoints; all sources already loaded.
- **No rollout flag.** New objects only; no migration of existing data. cfb-app reads the new view at its own pace.
- **Refresh schedule.** Inherits the existing `refresh_all_marts()` cadence. cfb-app should not assume real-time freshness (per existing schema-contract rule 6).
- **Test posture.** All feature-bearing units carry an explicit test scenario set. U11 adds the end-to-end smoke test that proves the refresh chain works.
- **Pre-push hook.** `.githooks/pre-push` runs ruff + pytest, which will exercise the new tests in `tests/test_returning_production.py`. CI on push to main runs the same suite via `.github/workflows/ci.yml`.

---

## Sources & References

- **Origin document:** [docs/brainstorms/2026-04-27-returning-production-model-requirements.md](../brainstorms/2026-04-27-returning-production-model-requirements.md)
- Closest matview analog: [src/schemas/marts/025_transfer_portal_impact.sql](../../src/schemas/marts/025_transfer_portal_impact.sql)
- Closest api view analog: [src/schemas/api/017_transfer_portal_impact.sql](../../src/schemas/api/017_transfer_portal_impact.sql)
- Refresh chain owner: [src/schemas/functions/refresh_all_marts.sql](../../src/schemas/functions/refresh_all_marts.sql), [scripts/refresh_marts.py](../../scripts/refresh_marts.py)
- Schema contract: [docs/SCHEMA_CONTRACT.md](../SCHEMA_CONTRACT.md)
- Hardening pattern (SECURITY INVOKER + schema grants): memory 2026-02-07; migration `grant_read_access_for_security_invoker.sql`
- Pipeline source for portal data: [src/pipelines/sources/recruiting.py](../../src/pipelines/sources/recruiting.py) `transfer_portal_resource`
- CFBD calibration target: `stats.player_returning` (loaded via `src/pipelines/sources/stats.py:player_returning_resource`)
