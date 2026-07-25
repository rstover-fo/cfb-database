# cfb-database: Preseason Outlook Model — Unbreak, Enrich, Simulate, Surface

**Status:** proposed (awaiting review)
**Branch:** `claude/2026-outlook-prediction-model-86n2wn`
**Supersedes nothing.** Extends `docs/plans/2026-07-21-tier3-analytics-plan.md`
(Pillars B/C) and the authoritative feature spec
`docs/brainstorms/2026-07-21-team-week-feature-design.md`.

## Context

A downstream agent was asked for a 2026 Oklahoma outlook and answered, in
substance, "I can't — zero 2026 games completed means zero EPA, zero Elo
trajectory, zero predictive edges," then offered "9-3ish on vibes."

That framing is wrong in a specific and correctable way. `fitted_v1` was
designed to score a week-1 game with no games played: §1f of the feature
design doc defines a whole family of preseason-known constants, and §1i fixes
the week-1 NULL semantics so the model layer imputes cleanly. The model is
*supposed* to have an opinion in July.

It has none because the offseason pipeline has been quietly dark since
January, and because nothing in the warehouse represents a *season* as an
object. This plan fixes both, then builds the outlook product on top.

---

## Phase 0 — Diagnosis (complete; findings below are verified against prod)

### 0.1 The offseason blackout

`src/pipelines/config/years.py:50` — `get_current_season()` returns
`now.year - 1` before August. On 2026-07-25 that is **2025**. Every
`--incremental` step in `.github/workflows/daily-load.yml` has therefore spent
the entire offseason rebuilding last season:

| Step | Intended target | Actual target |
|---|---|---|
| `compute_adjusted_epa.py --season $(get_current_season())` | 2026 | 2025 |
| `compute_adjusted_epa_week.py --incremental` | 2026 | 2025 |
| `build_features.py --incremental` | 2026 | 2025 |

Verified state of `features.team_week`: 7,662 rows for 2025, **0 rows for
2026**.

Note that `compute_predictions.py` is *not* affected — its
`TARGET_GAMES_QUERY` (`scripts/compute_predictions.py:363`) selects
`season >= (SELECT MAX(season) FROM core.games WHERE completed)`, a
calendar-independent idiom. That is why `elo_v1` and `elo_epa_blend_v1` have
all 1,638 published 2026 games while `fitted_v1` has none. **The fix is to
adopt the idiom that already works.**

### 0.2 The failure is silent

`score_fitted.py` INNER JOINs `features.team_week` on both sides
(`scripts/score_fitted.py:173`). With no 2026 feature rows, zero games match,
and `run_upcoming` logs `"No pending games with team_week features; nothing to
write"` and **returns normally** (`scripts/score_fitted.py:376`). The workflow
exits 0. Six months of green checkmarks, no output.

### 0.3 The frozen fit is two seasons stale

`features.model_metadata` holds fits for `train_through_season` 2017–2024.
The 2025 season completed in January 2026; `train_model.py` was never re-run.
`select_train_through("upcoming", ...)` takes `MAX(train_through_season)`, so
2026 scoring would today use a **2024** fit — walk-forward-legal but leaving a
full season of signal on the floor.

### 0.4 The dark model is the good one

Measured on all 3,829 completed 2025 games:

| model | MAE | residual SD | straight-up acc |
|---|---|---|---|
| **`fitted_v1`** | **14.69** | **18.51** | 74.8% |
| `elo_epa_blend_v1` | 15.69 | 19.96 | 74.9% |
| `elo_v1` | 15.88 | 20.21 | 74.9% |

The two models that survived the blackout are the two with no preseason
signal at all.

### 0.5 Offseason inputs are half-loaded

2026 rows exist for some sources and not others, because loaders are split
between `YEAR_RANGES` (`end=2026`) and `get_current_season()`:

| Source | 2026 rows | Needed for |
|---|---|---|
| `recruiting.recruits` | ✅ 3,107 | talent, class quality |
| `recruiting.transfer_portal` | ✅ 4,410 | portal net rating |
| `draft.draft_picks` | ✅ 257 | production lost to NFL |
| `stats.player_returning` | ❌ | `returning_*` (§1f) |
| `ratings.sp_ratings` | ❌ | `preseason_sp_*` (§1f) |
| `recruiting.team_talent` | ❌ | talent composite |
| `recruiting.team_recruiting` | ❌ | class rank/points |
| `core.roster` | ❌ | QB continuity |
| `ref.coaches__seasons` | ❌ | coaching change |

**Consequence:** even after Phase 1 lights up the feature build, `2026`'s
`returning_ppa_pct` and `preseason_sp_rating` would be NULL for every team and
get imputed to the league mean — i.e. the preseason features would contribute
nothing. Phase 1 is not complete without these loads.

### 0.6 There is no season-level object

`predictions.game_predictions` is per-game point estimates. Nothing in the
warehouse expresses projected wins, a win distribution, schedule strength, or
title odds. Twelve `get_game_prediction` calls do not compose into an outlook,
and no API view or MCP tool attempts it. This — not the missing 2026 rows — is
why the answer degraded to "vibes."

### 0.7 Schedule completeness (constrains Phase 4)

2026 regular season, teams with a conference affiliation: 337 teams, mean 9.66
games listed, **230 with ≥12**, 39 with 8–11, **68 with <8**. Full-season win
totals are viable for the large majority, but a team with 6 listed games must
not silently receive a 6-game win projection. Projections carry
`games_scheduled` and a completeness flag (§4.3).

---

## Phase 1 — Unbreak the offseason (A)

Goal: `fitted_v1` produces 2026 numbers daily, on a fit trained through 2025,
with real preseason inputs, and fails loudly if it doesn't.

### 1.1 Calendar-independent season targeting

Add to `src/pipelines/config/years.py`:

```python
def get_projection_seasons(conn) -> list[int]:
    """Seasons the compute chain must maintain: the most recent season with
    completed games, plus every later season that has scheduled games.
    Calendar-independent -- mirrors compute_predictions.TARGET_GAMES_QUERY."""
```

Backed by `SELECT DISTINCT season FROM core.games WHERE season >= (SELECT
COALESCE(MAX(season),0) FROM core.games WHERE completed)`.

`get_current_season()` is left alone — it is correct for *ingest* year
windows and is used widely. The new helper is for the *compute* chain only.

Rewire `--incremental` in:
- `scripts/build_features.py`
- `scripts/compute_adjusted_epa_week.py`
- the `compute_adjusted_epa.py` invocation in the daily workflow

so each iterates `get_projection_seasons()`. In July 2026 that is
`[2025, 2026]`; in October 2026 it is `[2026]`. Self-healing, no calendar
branch.

### 1.2 Turn the silent zero into a gate

`score_fitted.py::run_upcoming` must distinguish two cases:

- **no pending games at all** → exit 0 (legitimate: mid-January)
- **pending games exist, but < `MIN_COVERAGE` have `team_week` rows** → log
  the count and `sys.exit(1)`

Implement by counting pending games from `core.games` directly (the
`compute_predictions` idiom) *before* the feature join, then comparing.
Proposed `MIN_COVERAGE = 0.90` of pending games in the projection window.
Emit a gate line consistent with the repo's existing style:

```
FITTED_COVERAGE_GATE pending={n} scored={m} coverage={pct} threshold=0.90
```

Add the same assertion to `scripts/verify_load.py` so the daily post-load
check fails independently.

### 1.3 Self-healing annual refit

Add a `train_model.py` invocation to the daily workflow guarded by staleness:
refit when `MAX(train_through_season) < MAX(season)` over completed seasons.
Cheap to check, runs once a year in practice, removes a manual chore that has
already been missed once. Immediately produces the `train_through=2025` fit
that 2026 scoring should be using.

### 1.4 Load the missing offseason inputs

Extend the loaders behind `stats.player_returning`, `ratings.sp_ratings`,
`recruiting.team_talent`, `recruiting.team_recruiting`, `core.roster`, and
`ref.coaches__seasons` to fetch the projection season, not
`get_current_season()`.

> **Verification required before implementation.** This environment has no
> `CFBD_API_KEY`, so CFBD's 2026 availability for each endpoint is unconfirmed.
> First implementation step is a probe script that reports, per endpoint,
> whether `year=2026` returns rows. Endpoints that are not yet published
> (preseason SP+ typically lands in spring; rosters firm up in August) must
> degrade to a logged skip, never a hard failure — the loader should re-attempt
> daily and pick them up when they appear.

### 1.5 Phase 1 gates

- `features.team_week` has ≥ 3,200 rows for 2026 (2 sides × 1,638 games)
- `predictions.game_predictions` has `fitted_v1` rows for ≥ 90% of pending 2026 games
- `features.model_metadata` contains `train_through_season = 2025`
- `returning_ppa_pct` non-NULL for ≥ 125 FBS teams in 2026 (or a logged,
  explicit "CFBD has not published this yet" skip)
- Re-run of the daily workflow is idempotent

---

## Phase 2 — Preseason feature enrichment (B)

Goal: make the week-1 feature vector actually informative.

### 2.1 The problem, stated precisely

In a week-1 row every §1d/§1e column is NULL by construction, so
`build_feature_vector` imputes all seven production features and both havoc
features to the frozen train-window league mean. The home-minus-away diff of
two identical imputed values is **exactly zero**. Nine of the thirteen diff
features are therefore structurally dead in week 1, and the prediction reduces
to:

```
d_elo + d_adj_epa_off + d_adj_epa_def + d_returning_ppa_pct + d_preseason_sp_rating
```

— where the adj-EPA pair is the prior-season fallback and `preseason_sp_rating`
is, by the design doc's own admission (§1f), *last season's final* SP+ used as
a proxy. In the portal era that is a thin basis for a season outlook.

### 2.2 New `features.team_week` columns (migration 042)

All preseason-known and constant within a season, following the §1f pattern.
Sourced from **raw tables, not marts 017/025** — `marts.transfer_portal_impact`
joins season-S wins and SP+ (`current_wins`, `current_sp_rating`,
`win_delta`), which is season-S outcome data and would leak. The portal
features must be derived directly from `recruiting.transfer_portal`.

| Column | Type | Source | Leak rule |
|---|---|---|---|
| `portal_in_count` | BIGINT | `recruiting.transfer_portal` (season=S, destination=team) | portal activity precedes S |
| `portal_out_count` | BIGINT | same, origin=team | preseason-known |
| `portal_net_stars` | NUMERIC(8,3) | Σ incoming stars − Σ outgoing stars | preseason-known |
| `portal_net_rating` | NUMERIC(8,5) | same with `rating` | preseason-known |
| `talent_composite` | NUMERIC(8,3) | `recruiting.team_talent` (year=S) | preseason-known |
| `recruiting_points_3yr` | NUMERIC(10,3) | `recruiting.team_recruiting`, decayed sum over S−2..S | preseason-known |
| `draft_departures` | BIGINT | `draft.draft_picks` (year=S, college team) | April draft precedes S |
| `draft_departure_capital` | NUMERIC(8,3) | pick-value-weighted departures | preseason-known |
| `returning_qb_usage` | NUMERIC(8,4) | share of S−1 passing usage on the S roster | preseason-known |
| `hc_first_year` | BOOLEAN | `ref.coaches__seasons` — no S−1 season at this team | preseason-known |
| `hc_tenure_years` | BIGINT | consecutive prior seasons at team | preseason-known |

`returning_qb_usage` is the one requiring real work (`core.roster` ⋈
`stats.player_usage` ⋈ position filter). It is also, on priors, the single
most valuable preseason signal here — worth the effort, but if roster data for
S is unavailable in July it must degrade to NULL rather than block the phase.

### 2.3 Resolve the preseason SP+ proxy debt

If §1.4's probe confirms CFBD exposes a genuine preseason SP+ snapshot, load it
into a preseason-specific row and repoint `preseason_sp_*` at it, retiring the
"prior-season final as proxy" decision the design doc flags as follow-up work.
If not, the proxy stands and stays documented.

### 2.4 Design-doc amendments (required — it is authoritative)

`docs/brainstorms/2026-07-21-team-week-feature-design.md` must be updated
**before** migration 042 lands, per migration 028's header instruction:

- §1f — add the eleven columns with sources and leak rules
- §1i — extend the NULL-semantics table
- §2a — the `fitted_v1` vector is a **fixed 15-feature contract**. See §3.2
  for how the new columns enter without breaking frozen fits.
- Column count (31 → 42) and the `Decisions made` list
- New decision entry: *portal/talent features come from raw tables, not marts
  017/025, because those marts mix in season-S outcomes.*

### 2.5 Phase 2 gates

- Backfill 2015–2026; per-season NULL rates logged per new column
- Leak audit: every new column is verifiably knowable before week 1 of S
- `FEATURES_GATE` line extended with the new NULL counts
- No change to any existing `fitted_v1` prediction (new columns are inert
  until Phase 3 consumes them)

---

## Phase 3 — `preseason_v1` (C)

### 3.1 Rationale

`fitted_v1` trains on all weeks pooled. Its coefficients are dominated by the
~90% of games where the season-to-date features are live, so the
preseason-known features are fit to be *marginal corrections to in-season
form* — precisely the wrong weighting when they are the only signal present.
A model trained only on games where they are the only signal will weight them
correctly.

### 3.2 Design

Needs **no DDL**: `features.model_coefficients` and `model_metadata` are
already keyed by `model_version`, and §2a's fixed-15 contract is a property of
`fitted_v1`, not of the tables. `preseason_v1` declares its own
`DIFF_FEATURE_COLUMNS` and its own `FEATURE_NAMES`; the frozen-fit loader
already reads `feature_name` by name.

- **Train set:** games where both sides have `games_played_to_date <= 1`,
  seasons 2015–2025, walk-forward (fit through S−1 scores S), same protocol as
  §3 of the design doc.
- **Features:** Elo (carryover-regressed), prior-season adj-EPA off/def,
  `preseason_sp_*`, `returning_*`, plus every Phase 2 column. No season-to-date
  or havoc columns — they are structurally NULL in scope.
- **Form:** same ridge margin + IRLS logistic + Platt as `fitted_v1`, so the
  math core is reused rather than reimplemented. Refit `RIDGE_ALPHA` via
  `tune_params.py` — the train set is ~1/10 the size, so the optimal penalty
  will differ.
- **Elo carryover is itself a hyperparameter here.** `EloEngine.CARRYOVER =
  2/3` was chosen for in-season Elo, never validated as a preseason prior.
  Include it in the sweep.

### 3.3 Model selection at score time

```
games_played_to_date < 3  ->  preseason_v1
otherwise                 ->  fitted_v1
```

with the crossover justified by backtest, not asserted. A ramped blend over
weeks 1–4 is the obvious refinement; ship the hard switch first and measure
whether the blend beats it.

### 3.4 Phase 3 gates

- Backtest on week-1/2 games 2018–2025: `preseason_v1` MAE and Brier vs
  `fitted_v1` on the identical game set
- Calibration curve for win prob in 10 buckets
- **Honest stopping rule:** if `preseason_v1` does not beat `fitted_v1` on
  week-1 MAE, ship Phase 2's columns into `fitted_v1` instead and record the
  negative result in the plan. Do not ship a second model that isn't better.

---

## Phase 4 — Season simulation (D)

The actual missing product.

### 4.1 Storage — migration 043

`predictions.season_projections`, append-only daily snapshots mirroring
`predictions.game_predictions`' conventions (immutable row per
`(season, team, model_version, projection_date)`, `DISTINCT ON` latest-snapshot
view on top):

```
season, team, conference, model_version, projection_date, computed_at,
games_scheduled, games_completed, actual_wins,
projected_wins, projected_losses, median_wins,
wins_p10, wins_p25, wins_p75, wins_p90,
p_win_dist            JSONB   -- {"0":0.001, ..., "12":0.04}
p_bowl_eligible       NUMERIC -- P(wins >= 6)
p_ten_plus            NUMERIC
sos_rank, sos_rating,
conf_title_prob, playoff_prob,
n_sims, residual_sigma, schedule_complete BOOLEAN
```

### 4.2 `scripts/simulate_season.py`

Monte Carlo, 10,000 sims:

1. Pull every game for the season with a prediction from the selected model.
2. Completed games contribute their actual result (in-season mode).
3. For each remaining game and each sim, draw
   `margin ~ Normal(expected_home_margin, sigma)`; home wins iff `margin > 0`.
   `sigma` comes from the measured backtest residual SD — **18.51** for
   `fitted_v1` on 2025, refit per model and stored in `residual_sigma` rather
   than hardcoded.
4. Accumulate wins per team per sim → full distribution, not just a mean.

**v1 draws games independently.** This is a known, documented simplification:
real season outcomes are correlated (a team that is better than its rating
beats *everyone* more often), so independent draws understate the tails —
10-win and 3-win seasons will both be underpredicted. The correlated variant
(draw a per-team season-strength offset once per sim, apply to all that team's
games) is a ~15-line change and should be the v1.1 follow-up; note the
limitation in the column comment so consumers don't overtrust the tails.

Using `home_win_prob` directly instead of the margin draw is the alternative
formulation. Prefer the margin draw: it keeps a single sigma explicit and
tunable, and win prob is Platt-calibrated per game rather than jointly.

### 4.3 Schedule completeness

Per §0.7, 68 teams have fewer than 8 games listed. Rules:

- `games_scheduled` and `schedule_complete` (`>= 11` regular-season games) on
  every row
- Projections are always over *listed* games; never extrapolate to a
  hypothetical 12-game slate
- Consumers (§5) must surface "based on N scheduled games" whenever
  `schedule_complete` is false

### 4.4 Conference title and playoff odds

Scope honestly:

- **v1 conference title:** highest conference win percentage within each
  conference per sim, ties split evenly. Ignores real tiebreakers and
  championship-game formats.
- **v1 playoff:** deferred, or a documented crude proxy. The 12-team format's
  automatic bids and seeding are a real rules-modeling project and do not
  belong in the same phase as the simulator.

Both columns exist in the schema from the start so adding them later needs no
migration, but `playoff_prob` ships NULL until modeled properly rather than
shipping a number nobody can defend.

### 4.5 Phase 4 gates

- **Backfill validation:** simulate 2018–2025 preseason, compare projected
  wins to actual. Report MAE in wins and the calibration of `p_bowl_eligible`
  and `p_ten_plus`. A preseason win-total model landing within ~1.5 wins MAE
  is respectable; report what we actually get.
- Distribution sanity: each team's `p_win_dist` sums to 1; median within
  [p10, p90]
- Determinism under a fixed seed
- Idempotent daily re-run

---

## Phase 5 — Surfacing (E)

Without this the work is invisible to the agent that prompted it.

### 5.1 Views

- `marts.season_outlook` — latest projection per (season, team) joined to
  team/conference identity, SOS, and Elo
- `api.season_outlook` — contract-surface passthrough
- `api.season_outlook_games` — a team's schedule with per-game win prob,
  opponent rating, and home/away/neutral, ordered by week. This is what turns
  a number into a narrative ("hardest game at Georgia, 31%").

### 5.2 MCP tool

`get_season_outlook(team, season)` returning projected wins with an interval,
the win distribution, SOS, and the game-by-game list.

**Cross-repo:** the `mcp__CFBD__*` tools are not defined in this repo. This
repo ships the `api` views; the tool definition lands wherever the MCP server
lives. Deliverable here is `docs/handoffs/2026-07-25-season-outlook-handoff.md`
specifying the view contract, plus a `docs/SCHEMA_CONTRACT.md` update.

### 5.3 Make the caveat quantitative

The agent's instinct to hedge was right; the hedge should carry a number.
Phase 4.5's backtest yields the historical preseason win-projection error, so
the surface can report "8.7 projected wins; preseason projections have been
off by ~1.4 wins on average since 2018" instead of "ask me again once Michigan
week kicks off."

---

## File inventory

**New**
```
src/schemas/migrations/042_preseason_features.sql
src/schemas/migrations/043_season_projections.sql
src/schemas/marts/043_season_outlook.sql
src/schemas/api/041_season_outlook.sql
src/schemas/api/042_season_outlook_games.sql
scripts/simulate_season.py
scripts/probe_offseason_availability.py
tests/test_simulate_season.py
tests/test_preseason_model.py
docs/handoffs/2026-07-25-season-outlook-handoff.md
```

**Modified**
```
src/pipelines/config/years.py            # get_projection_seasons
scripts/build_features.py                # projection-season targeting + new columns
scripts/compute_adjusted_epa_week.py     # projection-season targeting
scripts/score_fitted.py                  # coverage gate; model selection
scripts/train_model.py                   # preseason_v1 feature set
scripts/tune_params.py                   # preseason_v1 sweep + CARRYOVER
scripts/verify_load.py                   # fitted coverage + projection freshness
scripts/refresh_marts.py                 # season_outlook
.github/workflows/daily-load.yml         # staleness-guarded refit, simulate step
docs/brainstorms/2026-07-21-team-week-feature-design.md   # §1f/§1i/§2a/decisions
docs/SCHEMA_CONTRACT.md
docs/pipeline-manifest.md
CLAUDE.md                                # scripts table, schema table
```

---

## Execution order and dependencies

```
Phase 1  (independent)          -> 2026 fitted_v1 numbers exist
  |
  +-- Phase 2 (needs 1.4 loads) -> richer preseason vector
  |     |
  |     +-- Phase 3             -> preseason_v1, gated on beating fitted_v1
  |
  +-- Phase 4 (needs Phase 1 only, improves with 3) -> win totals
        |
        +-- Phase 5             -> api + MCP surface
```

Phase 4 deliberately depends only on Phase 1. If Phases 2–3 stall on CFBD data
availability, the simulator still ships on `fitted_v1` and the outlook product
still lands.

**Recommended sequencing:** Phase 1 → Phase 4 → Phase 5 → Phase 2 → Phase 3.
This puts a working 2026 outlook in front of users before the model-quality
work, and Phases 2–3 then improve a surface that already exists.

---

## Risks and open questions

| Risk | Mitigation |
|---|---|
| CFBD has not published 2026 rosters/SP+/returning production yet | §1.4 probe first; degrade to logged skip and retry daily |
| No `CFBD_API_KEY` in this environment | Probe must run where credentials exist; treat §1.4 as unverified until then |
| Independent-draw Monte Carlo understates tails | Documented in column comment; correlated variant queued as v1.1 |
| Incomplete 2026 schedules (68 teams < 8 games) | `schedule_complete` flag; never extrapolate |
| Conference realignment breaks team-name joins | `seed_team_xwalk.py` crosswalk already exists; verify against 2026 conferences |
| `preseason_v1` fails to beat `fitted_v1` | Explicit stopping rule (§3.4) — fold Phase 2 columns into `fitted_v1` and record the negative result |
| Design-doc drift | Migration 028's header forbids column changes without a doc update; §2.4 is a hard prerequisite |

**Open question for review:** should `playoff_prob` ship as NULL in v1 (my
recommendation — the 12-team format's autobids and seeding are their own
project) or is a crude top-12-by-rating proxy more useful than nothing?
