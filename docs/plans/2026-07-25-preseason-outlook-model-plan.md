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

Goal: make the week-1 feature vector actually informative, organized around the
stated thesis — **what a team did after 2025** (recruiting, portal, returning
production), **its schedule**, **roster makeup in the trenches with continuity
and development**, and **QB play**.

### 2.0 The structural finding that justifies this phase

`stats.player_returning` — the source of the *only* returning-production
feature in the model — has exactly these measures:

```
total_ppa, total_passing_ppa, total_receiving_ppa, total_rushing_ppa,
percent_ppa, percent_passing_ppa, percent_receiving_ppa, percent_rushing_ppa,
usage, passing_usage, receiving_usage, rushing_usage
```

Every one is PPA- or usage-based, and **PPA and usage accrue only to players
who touch the football.** An offensive lineman generates zero PPA and zero
usage in every season he plays. Defensive linemen likewise accrue no PPA
(it is credited to the offense).

So `returning_ppa_pct` is not merely a weak trench signal — it is
**structurally incapable of seeing the trenches at all.** A team returning all
five offensive line starters and a team returning none can have identical
`returning_ppa_pct`. The model's single returning-production input is a
skill-position measure wearing a team-level name.

This is a schema fact, not a statistical inference, and it is the strongest
argument in this plan for the phase. The thesis is pointing directly at the
hole.

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

### 2.2 What is knowable in July vs August — the binding constraint

The outlook must be answerable **now**, in July. Verified availability for
2026:

| Available today (July) | Not until ~August |
|---|---|
| `recruiting.transfer_portal` — 4,410 rows, with `position` | `core.roster` (no 2026 rows) |
| `draft.draft_picks` — 257 rows, with `position` | `stats.player_returning` |
| `recruiting.recruits` — 3,107 rows, with `position` | `recruiting.team_talent` |

This splits the feature set in two, and the split drives the design:

- **Churn features** (portal in/out, draft departures, recruiting) are
  derivable **today** from tables already loaded. They answer "what did this
  team do after 2025" directly.
- **Continuity features** (who is actually back on the roster) require the
  season-S roster and cannot be computed until CFBD publishes it.

**Design consequence:** build the July-computable churn features first and
treat roster-continuity as an August enrichment that sharpens the same
columns. A team's outlook should exist in July and improve in August, not
appear in August. Every continuity column therefore needs a defined
pre-roster fallback, and `adj_epa_source`-style provenance flags
(`trench_source = 'churn' | 'roster'`) so consumers know which they are
reading.

### 2.3 New `features.team_week` columns (migration 042)

All preseason-known and constant within a season, following the §1f pattern.
Sourced from **raw tables, not marts 017/025** — `marts.transfer_portal_impact`
joins season-S wins and SP+ (`current_wins`, `current_sp_rating`,
`win_delta`), which is season-S outcome data and would leak. The portal
features must be derived directly from `recruiting.transfer_portal`.

**Trenches** — the gap §2.0 identifies. Position-group scoped, offense and
defense separately:

| Column | Type | Source | Available |
|---|---|---|---|
| `ol_portal_in` / `ol_portal_out` | BIGINT | `recruiting.transfer_portal`, OL group | July |
| `dl_portal_in` / `dl_portal_out` | BIGINT | same, DL group | July |
| `ol_portal_net_rating` | NUMERIC(8,5) | Σ in − Σ out, `rating`-weighted | July |
| `dl_portal_net_rating` | NUMERIC(8,5) | same | July |
| `ol_draft_departures` | BIGINT | `draft.draft_picks`, OL group | July |
| `dl_draft_departures` | BIGINT | same, DL group | July |
| `ol_continuity` | NUMERIC(8,4) | share of S−1 OL roster on S roster | August |
| `dl_continuity` | NUMERIC(8,4) | same for DL | August |
| `prior_line_yards` | NUMERIC(8,4) | `stats.advanced_team_stats.offense__line_yards` (S−1) | July |
| `prior_stuff_rate_allowed` | NUMERIC(8,4) | `offense__stuff_rate` (S−1) | July |
| `prior_def_line_yards` | NUMERIC(8,4) | `defense__line_yards` (S−1) | July |
| `prior_front_seven_havoc` | NUMERIC(8,4) | `defense__havoc__front_seven` (S−1) | July |
| `trench_source` | VARCHAR | `'churn'` \| `'roster'` provenance | — |

The `prior_*` line-play columns matter independently of continuity: they carry
*how good the trenches actually were*, which the PPA-based returning
production never encodes. `stats.advanced_team_stats` already holds
`line_yards`, `power_success`, `stuff_rate`, `second_level_yards`,
`open_field_yards`, and `havoc__front_seven` on both sides of the ball, going
back to 2004. This is the single cheapest win in the phase — no new ingest, no
roster dependency, and it directly measures the thing the thesis cares about.

**QB** — the other named priority:

| Column | Type | Source | Available |
|---|---|---|---|
| `qb_portal_in_rating` | NUMERIC(8,5) | best incoming QB `rating`, portal | July |
| `qb_portal_out_rating` | NUMERIC(8,5) | best departing QB `rating` | July |
| `qb_draft_departure` | BOOLEAN | QB drafted off this team, year S | July |
| `returning_qb_usage` | NUMERIC(8,4) | share of S−1 passing usage on the S roster | August |
| `prior_passing_ppa` | NUMERIC(8,5) | `offense__passing_plays__ppa` (S−1) | July |
| `qb_source` | VARCHAR | `'returning'` \| `'portal'` \| `'unknown'` | — |

**Cohesion / development** — the harder half of the thesis, and the part most
likely to be genuinely novel:

| Column | Type | Source | Available |
|---|---|---|---|
| `roster_churn_rate` | NUMERIC(8,4) | total portal out ÷ prior roster size | July |
| `portal_dependency` | NUMERIC(8,4) | portal-in ÷ (portal-in + HS signees) | July |
| `hc_first_year` | BOOLEAN | `ref.coaches__seasons` | July |
| `hc_tenure_years` | BIGINT | consecutive prior seasons at team | July |
| `dev_index` | NUMERIC(8,5) | prior-season SP+ residual vs 3-yr recruiting talent | July |

`portal_dependency` and `dev_index` are the "assembled vs developed" contrast
the thesis is reaching for: `dev_index` asks whether a program has
historically outperformed its recruiting inputs (development) and
`portal_dependency` asks whether this year's roster was built or bought.
Neither exists anywhere in the warehouse today.

**Team-level** (from the original plan, retained):
`portal_net_rating`, `talent_composite`, `recruiting_points_3yr`,
`draft_departures`, `draft_departure_capital`.

### 2.4 Position crosswalk (prerequisite)

Position vocabularies differ across every source and must be normalized before
any of the above is computable. Verified 2026 values:

| Source | OL group | DL group |
|---|---|---|
| `core.roster` | `OL`, `OT`, `G`, `C` | `DL`, `DE`, `DT`, `NT`, `EDGE` |
| `recruiting.transfer_portal` | `IOL`, `OT` | `DL`, `EDGE` |
| `draft.draft_picks` | `Offensive Tackle`, `Offensive Guard`, `Center` | `Defensive Tackle`, `Defensive Edge` |

Ship as a small reference table (`ref.position_groups`) rather than repeated
CASE expressions, following the `seed_team_xwalk.py` precedent. Note
`core.roster` also carries ~1,189 NULL and 35 `'?'` positions per season —
the crosswalk must handle unmapped values explicitly, not silently drop them
(dropping them would inflate continuity denominators).

### 2.5 Validation gate — prove incremental value before building

**This gate exists because the naive version of the thesis already failed a
first test.** Preliminary correlations run against prod (full results in the
appendix) found roster-headcount trench continuity essentially uncorrelated
with next-season SP+ change (OL −0.015, DL +0.013) while the existing
`returning_ppa_pct` scored +0.24. Returning the primary passer scored +0.019.

Two readings, and they are not mutually exclusive:

1. **The measures were bad.** Headcount continuity counts walk-ons and fourth-
   stringers equally with starters, and CFBD gives no OL snap counts. This is a
   genuinely poor proxy for "the line is back."
2. **The framing was bad.** Correlating against *year-over-year deltas* is
   dominated by mean reversion, and the candidate features correlate with prior
   -season quality too. Delta correlation is close to the wrong instrument.

Supporting reading (1) over pure noise: against *direct trench outcomes*
rather than overall SP+, OL continuity moved consistently in the predicted
direction — line yards +0.070, power success +0.050, stuff-rate improvement
+0.083. Weak, but the mechanism appears where the theory says it should,
which is what a real-but-badly-measured effect looks like.

**The gate:** before migration 042 ships any column, run a partial-correlation
screen — regress season-S SP+ on season-S−1 SP+ and test each candidate
against the residual. That answers the only question that matters for a
prediction model: *does this add signal beyond what we already know?* A raw
correlation with a delta does not.

```
partial_r(x, sp_S | sp_S-1)  for each candidate column, 2015-2025
```

Columns that fail to clear a pre-registered threshold do not ship. Record the
failures in this plan rather than deleting them — a documented null result on
trench continuity is worth more than a quietly dropped column, and it tells us
whether the ceiling is the metric or the data.

**Pre-registered expectation:** the churn-based and `prior_*` line-play
columns are more likely to clear than the roster-continuity columns, because
they measure contributors (portal entrants and draft picks played) and actual
line performance rather than bodies on a list.

### 2.6 Resolve the preseason SP+ proxy debt

If §1.4's probe confirms CFBD exposes a genuine preseason SP+ snapshot, load it
into a preseason-specific row and repoint `preseason_sp_*` at it, retiring the
"prior-season final as proxy" decision the design doc flags as follow-up work.
If not, the proxy stands and stays documented.

### 2.7 Design-doc amendments (required — it is authoritative)

`docs/brainstorms/2026-07-21-team-week-feature-design.md` must be updated
**before** migration 042 lands, per migration 028's header instruction:

- §1f — add the surviving columns with sources and leak rules
- §1i — extend the NULL-semantics table, including the July/August split and
  the `trench_source` / `qb_source` provenance flags
- §2a — the `fitted_v1` vector is a **fixed 15-feature contract**. See §3.2
  for how the new columns enter without breaking frozen fits.
- Column count and the `Decisions made` list
- New decision entries:
  - *portal/talent features come from raw tables, not marts 017/025, because
    those marts mix in season-S outcomes*
  - *returning production is PPA-based and therefore blind to both lines;
    trench signal comes from position-scoped churn and prior line-play metrics*
  - *continuity columns carry a provenance flag and a pre-roster fallback, so a
    July outlook exists before rosters publish*

### 2.8 Phase 2 gates

- §2.5 partial-correlation screen passed, per column, with results recorded
- Backfill 2015–2026; per-season NULL rates logged per new column
- Leak audit: every new column verifiably knowable before week 1 of S — with
  particular care on `recruiting.transfer_portal.season` semantics (confirm
  season=S means "arriving for season S", not "departed after S")
- `FEATURES_GATE` line extended with the new NULL counts
- Position crosswalk covers ≥ 99% of non-NULL positions in all four sources
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

### 5.3 The opinionated layer — `api.season_outlook_drivers`

A projected-wins number is not yet a view. The thesis asks *why* a team is
where it is, in terms of what it did after 2025. Ship a per-team ranked driver
list: each Phase 2 feature expressed as a z-score against the FBS distribution,
signed by its model coefficient, so the surface can state what is actually
moving the projection.

```
season, team, driver, category, value, z_score, contribution_pts, direction
```

`category` ∈ `{trenches, qb, portal, recruiting, continuity, schedule,
coaching}` — the thesis's own vocabulary, so an answer can be assembled by
category rather than by raw feature name. `contribution_pts` is the feature's
standardized value times its fitted coefficient, i.e. its actual contribution
to the margin projection in points, which makes the ranking defensible rather
than decorative.

This is what lets the answer become "OU projects 8.7 wins; the offensive line
returns the most snap-equivalent production in the conference (+1.2 pts/game)
and the schedule ranks 7th-toughest (−0.9)" instead of "9-3ish on vibes."
The stance comes from the coefficients, not from prose.

Schedule earns its own driver via Phase 4's SOS: strength of schedule is a
first-class explanation of a win total, and is fully known in July for the 230
teams with complete slates.

### 5.4 Make the caveat quantitative

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
src/schemas/api/043_season_outlook_drivers.sql
src/schemas/migrations/044_position_groups.sql   # ref.position_groups crosswalk
scripts/simulate_season.py
scripts/probe_offseason_availability.py
scripts/screen_preseason_features.py             # §2.5 partial-correlation gate
tests/test_simulate_season.py
tests/test_preseason_model.py
tests/test_position_groups.py
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
| Trench/QB features fail the §2.5 screen | Pre-registered threshold; ship only survivors. `prior_*` line-play columns are low-risk regardless (measured performance, no roster dependency) |
| 2026 rosters unpublished until August | July outlook runs on churn features; continuity columns carry `trench_source`/`qb_source` provenance and a pre-roster fallback |
| `transfer_portal.season` semantics ambiguous | Confirm direction (arriving-for-S vs departed-after-S) before any portal feature ships — a sign error here silently inverts the entire trench signal |
| Position vocabularies differ across four sources | `ref.position_groups` crosswalk with explicit unmapped handling; NULL/`'?'` positions must not deflate continuity denominators |
| Design-doc drift | Migration 028's header forbids column changes without a doc update; §2.4 is a hard prerequisite |

**Open question for review:** should `playoff_prob` ship as NULL in v1 (my
recommendation — the 12-team format's autobids and seeding are their own
project) or is a crude top-12-by-rating proxy more useful than nothing?

---

## Appendix — preliminary empirical results

Run against prod on 2026-07-25 while scoping Phase 2. **These are screening
results, not conclusions**, and the framing of the first three is flawed in a
way §2.5 corrects. Recorded here so the Phase 2 work starts from evidence
rather than from priors — including evidence that cuts against the design.

**A1. Trench continuity vs next-season SP+ change** (1,425 team-seasons,
2015–2025, teams with ≥8 prior-season OL):

| measure | corr with SP+ delta |
|---|---|
| OL roster continuity | −0.0150 |
| DL roster continuity | +0.0134 |
| `returning_ppa_pct` (existing feature) | +0.2415 |
| OL continuity vs `returning_ppa_pct` | +0.1562 |

**A2. Trench continuity vs direct trench outcomes** (1,429 team-seasons) —
the same measures against the metrics the mechanism should move first:

| measure | corr |
|---|---|
| OL continuity → offensive line-yards change | +0.0700 |
| OL continuity → power-success change | +0.0497 |
| OL continuity → stuff-rate improvement | +0.0826 |
| DL continuity → defensive line-yards improvement | −0.0627 |

**A3. Returning starting QB** (1,421 team-seasons): 57.5% of team-seasons
return their primary passer. Correlation with offensive PPA change +0.0089,
with SP+ change +0.0187.

**Reading.** A1 and A3 look like flat nulls; A2 shows the OL effect appearing
weakly but consistently in exactly the three metrics that measure run
blocking, with the DL result contradictory. The most likely explanation is a
real effect buried under two measurement problems — headcount continuity is a
poor proxy for returning *starters*, and delta correlation is the wrong
instrument on mean-reverting data. But "the thesis is right and my test was
bad" is exactly what a wrong thesis also looks like, which is why §2.5 makes
the partial-correlation screen a hard gate rather than a formality.

**Not yet run:** the partial-correlation screen itself (`partial_r(x, sp_S |
sp_S−1)`) — the session lost direct SQL access before it could be executed. It
is the first task of Phase 2, and its results should be appended here.

**Unaffected by any of this:** §2.0's finding that `returning_ppa_pct` cannot
see the lines is structural — it follows from PPA's definition, not from these
correlations, and holds regardless of how the screen resolves. The line-play
columns (`prior_line_yards`, `prior_stuff_rate_allowed`,
`prior_def_line_yards`, `prior_front_seven_havoc`) are also low-risk
independent of the screen: they are measured performance rather than inferred
continuity, cost no new ingest, and have no roster dependency.
