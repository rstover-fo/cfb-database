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

### 2.4b Draft production — program-level, available in July

"Draft picks produced" is available today: `draft.draft_picks` carries
`college_team`, `year`, `round`, `overall`, and `position`, and the 2026 draft
(257 picks) is already loaded.

Two distinct signals live here and **must not be collapsed into one column** —
they have opposite signs:

| Column | Meaning | Expected sign |
|---|---|---|
| `draft_picks_3yr` / `draft_picks_5yr` | picks produced in S−1…S−3 (excludes S) | **+** program talent/development index |
| `draft_capital_3yr` | round-weighted equivalent | **+** |
| `draft_departures` (already planned) | picks lost in year S | **−** talent that just left |

A team that produced nine picks over three years is a program that recruits and
develops NFL players. A team that lost six picks *this* April just lost its
best players. The same underlying table, opposite implications for season S.
Conflating them would net the effects to roughly zero — which may well be part
of why raw draft counts look unpredictive in naive tests.

**The development synthesis.** This connects directly to `dev_index` (§2.3):
draft picks produced *relative to recruiting rank* is a cleaner development
measure than either alone. A program signing 30th-ranked classes and producing
top-10 draft output is developing; the reverse is squandering. Ship this as
`draft_yield` — picks produced over 3 years residualized against 3-year
recruiting points — and prefer it over the raw count if the §2.5 screen shows
the raw count is just re-expressing recruiting talent.

**Honest caveat:** raw draft production is heavily confounded with
`talent_composite` and prior SP+, and may add little beyond them. It goes
through the same §2.5 partial-correlation screen as everything else, and
`draft_yield` is the variant most likely to survive it precisely because
residualizing removes the confound.

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

## Phase 6 — Draft prospect capital on current rosters

Goal: answer "how much NFL talent is on this roster **right now**" — a
forward-looking prospectus, as distinct from §2.4b's backward-looking record of
picks already produced.

Three tiers, each strictly more informative and strictly harder/later. **Tier A
needs no roster and no subscription, so it ships in Phase 2's July window.**

### 6.0 Tier A — hybrid pipeline × conversion index (July, backtests fully)

A roster at time T is the accumulation of the last four-to-five recruiting
classes plus portal, minus what has left. So model the *stock* without
observing it:

```
talent_stock(S)   = decayed sum of recruiting class quality, S-1..S-4
                    + portal_in(S) rating-weighted
                    - draft_departures(S)
                    - portal_out(S)
conversion(S)     = draft picks produced S-1..S-3, residualized on the
                    same window's recruiting inputs        [= draft_yield]
pipeline_index(S) = talent_stock(S) × conversion(S)
```

The two terms answer different questions and neither is sufficient alone.
`talent_stock` asks *how much raw material is on hand*; `conversion` asks
*whether this staff turns raw material into NFL players* — the development
thesis expressed as a rate rather than a count. A program with top-15 classes
and bottom-half draft output is not the same team as the reverse, and
`talent_composite` alone cannot tell them apart.

Columns: `pipeline_index`, `talent_stock`, `draft_yield` (§2.4b),
`blue_chip_pipeline` (4–5★ share of the four-year window).

> **SUPERSEDED BY THE SCREEN (appendix A4, run 2026-07-26).** This section was
> written before evidence existed. `pipeline_index` and `talent_stock` both
> failed and must not ship; `conversion`/`draft_yield` failed even after
> regime-scoping. What survives is `recruiting_points_3yr`,
> `blue_chip_pipeline`, and the §6.0b variant `recruiting_points_regime`. The
> design below is retained as the record of what was tested and why.

| | Tier A hybrid | Tier B `draft_prob_v1` | Tier C external board |
|---|---|---|---|
| Available for 2026 | **July, today** | August (roster) | July, unvalidated |
| Backtests | **2015–2025 now** | 2015–2025 | not until ~2028 |
| Cost / ToS exposure | **none** | none | $120/yr + gray area |
| Grain | program | player | player |

**Prior art:** a formalization of the blue-chip-ratio idea familiar from public
CFB analysis, with a development term added. We are not inventing a new claim,
only measuring one properly.

**Honest risk:** program-level and slow-moving, so heavily collinear with
`talent_composite` and prior SP+ — it may add nothing beyond them. That is what
§2.5's screen decides. `draft_yield` and `conversion` are the terms most likely
to survive, because residualizing against recruiting inputs removes the
collinearity.

### 6.0b Regime awareness — the naive trailing window is wrong

A flat decayed sum over S-1..S-4 blends two different programs whenever a
coaching change falls inside the window, and `conversion` over S-1..S-3
attributes development to a staff that may not have done it. Riley-era and
Venables-era Oklahoma are not the same program; averaging them describes
neither.

**The key asymmetry — stock transfers, conversion does not.**

| Term | On a coaching change | Why |
|---|---|---|
| `talent_stock` | **carries over** (net of portal exodus) | The players are still on campus. Recruiting inputs are physical. |
| `conversion` | **does not carry over** | Development is staff-dependent. This is the whole claim. |

So on a regime change: keep `talent_stock`, and regress `conversion` toward the
**new head coach's career prior** — his `draft_yield` at previous stops —
rather than inheriting the predecessor's. A first-year HC with a strong
development record elsewhere and an inherited top-10 roster is a genuinely
different projection from the same roster under an unproven staff, and a flat
window cannot express that at all.

Reuse rather than rebuild: `ref.coaches__seasons`, `marts.coaching_tenure`
(023), `marts.coach_record` (009), `api.coaching_history` (015) /
`api.coach_records` (038) already carry tenure boundaries and career records.
`hc_tenure_years` / `hc_first_year` (§2.3) become **interaction** terms with the
pipeline columns rather than independent ones.

Ship two variants of each trailing term and let the screen choose: `*_raw`
(flat window) and `*_regime` (window truncated at the current HC's tenure
start, backfilled with his career prior). If `*_regime` does not beat `*_raw`,
that is itself a finding worth recording.

**The era break is the harder half.** The portal/NIL era is a structural change,
not a drift: one-time transfer plus NIL from 2021, rev-share and GM front
offices after. Roster construction gained an acquisition channel that did not
exist, so pre-2021 conversion rates may not describe post-2021 programs.

The tension is real: restricting training to 2021+ leaves ~5 seasons of a
slow-moving, highly autocorrelated program-level feature — a very thin
effective sample, and era-specific coefficients on it would likely overfit.
**Preferred approach:** keep the full window, add a `portal_era` indicator and
era × pipeline interaction terms, and let §2.5 decide whether the interaction
earns its degrees of freedom. Fall back to an era-restricted fit only if the
interaction is strong and stable. `ref.eras` already exists for the indicator.

**Front-office structure (GMs) is not automatable.** CFBD has no GM or
personnel-staff field, and no scouted source carries one. If it matters enough
to model, it would be a small hand-maintained reference table following the
`recruiting.nil_market_benchmarks` precedent in the source-scouting doc
(refreshed ~2×/yr). Out of scope for v1; noted so the omission is deliberate
rather than an oversight.

### 6.1 There is no source for this — it has to be derived

Verified: every draft-related source in or scouted for this warehouse is
**retrospective**.

- `draft.draft_picks` — players already drafted
- `draft.combine` / `draft.nflverse_draft_picks` (flat-file, already loaded) —
  post-season measurables, 1980+/2000+
- `docs/brainstorms/2026-07-23-warehouse-extension-data-sources.md` §4 scouted
  the talent pipeline in depth and surfaced **no forward-looking prospect
  board**

The one pre-draft evaluation present is `draft.draft_picks.pre_draft_ranking`,
`pre_draft_position_ranking`, and `pre_draft_grade` — but those exist only for
players who *were* drafted, only in their draft year. Using them as a roster
feature would be textbook survivorship bias: the players who busted have no row.

External consensus boards (NFL Mock Draft Database, PFF, Kiper, Brugler) are
paywalled, ToS-risky, or both — the same category the source-scouting doc
flagged as "high value, high risk" for On3. Not a dependency worth taking for a
signal we can derive internally.

### 6.2 Derive it — `predictions.draft_probability`

This is a clean supervised-learning problem with a real, already-loaded label.
`draft.draft_picks.college_athlete_id` is the outcome key, and it joins to
`core.roster.id` and `recruiting.recruits.athlete_id`.

**Label:** for each (player, season) on a roster, did that player get drafted
within the following 3 NFL drafts?

**Features** (all knowable before season S):
- recruiting: `stars`, `rating`, `ranking` from `recruiting.recruits`
- position group (`ref.position_groups`, §2.4)
- class standing / years on roster (derived from `core.roster` history)
- production: `stats.player_usage`, `marts.player_season_epa`,
  `stats.player_season_stats` through S−1
- physical: height/weight from `core.roster`
- program: `draft_picks_3yr` (§2.4b) — programs do place players

**Model:** logistic regression, reusing the IRLS + Platt core already in
`train_model.py` rather than adding a dependency. Walk-forward by season, same
protocol as §3 of the design doc. Coefficients land in
`features.model_coefficients` under `model_version = 'draft_prob_v1'` — no new
DDL for the fit itself.

**Team feature:** `Σ P(drafted)` over the season-S roster, plus a
blue-chip-weighted variant and a trenches-only variant
(`ol_draft_capital`, `dl_draft_capital`) — which is precisely the "NFL talent
in the trenches" measure the thesis wants and that nothing else in the plan
delivers.

### 6.3 The roster dependency is real

This requires the season-S roster, so like §2.2's continuity features it
**cannot run for 2026 until CFBD publishes 2026 rosters** (~August). The July
fallback is the recruiting-based approximation already in Phase 2
(`talent_composite`, `recruiting_points_3yr`, `draft_picks_3yr`), all of which
are program-level rather than roster-level.

This is why Phase 6 is last: it is the highest-effort, highest-latency, and
most speculative item in the plan. It should not block the outlook shipping.

### 6.4 Phase 6 gates

- Backtest: AUC and calibration of `draft_prob_v1` on held-out seasons,
  walk-forward. A model that cannot separate drafted from undrafted players
  better than recruiting stars alone is not worth its complexity — that is the
  stopping rule.
- Join-rate audit: what fraction of roster rows resolve to a recruit record and
  to a draft outcome. `core.roster` carries ~1,189 NULL positions per season and
  the recruit join is via `athlete_id`; both need measuring before trusting any
  team-level sum. **Unverified in this session** (SQL access was approval-gated
  before it could be checked) — this is the first task of Phase 6.
- §2.5 partial-correlation screen on the resulting team-level columns, same as
  every other candidate feature.

### 6.5 Tier C — external prospect sources (investigated July 2026)

| Source | Access | History | Verdict |
|---|---|---|---|
| **NFLMDD commercial API** — consensus of 200 big boards, 1,500 expert mocks, 850+ sources | Licensed API key, **~$5k/yr** | 8 years | **Out on cost** |
| **NFLMDD Mock+ Gold** — same consensus board | **$9.99/mo**, includes **"10 Consensus Big Board CSV downloads/mo"** | Current class only | **Recommended path** |
| **Grinding the Mocks** — Bayesian Expected Draft Position; EDP explains ~85% of draft-outcome variance | Shiny dashboard, JS-rendered; needs Playwright or an ask | Multi-year research published | Ask first; keep as upgrade |
| `array-carpenter/nfl-draft-data` | Free GitHub | 2007–2026 combine + pro day + NFL.com grades | Draft-year, not preseason |
| `JackLich10/nfl-draft-data`, `phcs971/nfl-draft-dataset`, `nflverse/nfldata` | Free GitHub | 1967+/1987+/2000+ | **Retrospective only** — good for *labels* |

**The $9.99 tier removes the scraping question.** A sanctioned CSV export, ten
per month, exceeds what a weekly snapshot needs (4–5/mo) at ~$120/yr against
the API's ~$5k. No Cloudflare fight, no watermark exposure, no ToS argument —
it is the vendor's own export button. Scraping NFLMDD is explicitly **not**
recommended: ToS prohibits redistribution, responses carry a `_meta.client_id`
watermark, and the site 403s plain HTTP.

**Fits existing infrastructure.** A periodically-dropped CSV parsed into staging
is the flat-file pattern in `src/pipelines/sources/flat_files.py` —
`FlatFileSpec` registry, `scripts/load_flat_files.py --due` cadence, and the
`meta.flat_file_loads` hash-skip ledger. Add a `prospect_board` spec plus a
parser under `src/pipelines/sources/flatfile_parsers/`, landing in
`raw.prospect_boards`. Same shape as the PFF "weekly CSV drop → staging loader"
roadmap item and the `raw.availability_reports` archiver.

**Decide knowingly.** Mock+ Gold is a consumer tier; the ~$5k API is what the
vendor sells for programmatic/commercial use. Feeding an internal warehouse from
consumer exports is a gray area — low exposure if cfb-app/cfb-scout stay
internal and nothing is redistributed, materially higher if anything becomes
public-facing. Mitigations: keep `raw.prospect_boards` internal, never expose
board rankings through `api.*` or MCP, surface only derived team aggregates.

**The blocker is the backtest, not the signal.** A July-2026 snapshot of the
**2027** board is leak-free for the 2026 season — note the class-year offset:
players on 2026 rosters are drafted in April 2027, so the already-loaded 2026
draft is the wrong class entirely. But boards are overwritten daily and nobody
archives preseason snapshots, so we can obtain a 2026 signal today and cannot
validate it historically. Therefore: **start snapshotting now** (history accrues
only forward — the Massey precedent), keep Tier A/B as primary, and treat any
board as an unvalidated overlay excluded from the fitted vector until it clears
§2.5.

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
scripts/train_draft_probability.py               # Phase 6 player-level model
src/schemas/migrations/045_draft_probability.sql # predictions.draft_probability
tests/test_draft_probability.py
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

Phase 6 (draft prospect capital) hangs off Phase 2's crosswalk and feeds team-
level columns back into it, but gates on August roster publication and is
deliberately last.

Phase 4 deliberately depends only on Phase 1. If Phases 2–3 stall on CFBD data
availability, the simulator still ships on `fitted_v1` and the outlook product
still lands.

**Recommended sequencing:** Phase 1 → Phase 4 → Phase 5 → Phase 2 → Phase 3 →
Phase 6. This puts a working 2026 outlook in front of users before the
model-quality work, and Phases 2–3 then improve a surface that already exists.

**July/August split across the whole plan.** Everything shippable before rosters
publish: Phases 1, 4, 5, and the churn/line-play/draft-production half of Phase
2. Everything gated on August rosters: Phase 2's continuity and
`returning_qb_usage` columns, and all of Phase 6. Sequencing the roster-
dependent work last is not a preference — it is the only order the calendar
allows.

---

## Delegation map

Matches the Tier 1–3 ladder, with **opus 5 as main loop** (fable usage
exhausted).

| Model | Work |
|---|---|
| **haiku** | `ref.position_groups` crosswalk seed rows, registry/inventory adds, `refresh_marts.py --views` entries, `FEATURES_GATE` line additions, mart/view count bumps in `CLAUDE.md`, `docs/pipeline-manifest.md` rows, index SQL |
| **sonnet** | All script scaffolds at the IO/CLI/idempotency layer (`simulate_season.py`, `probe_offseason_availability.py`, `screen_preseason_features.py`, `train_draft_probability.py`); migrations 042–045 DDL; thin marts + api views 041–043; `get_projection_seasons()`; `--incremental` rewiring; workflow edits; all tests; handoff doc + `SCHEMA_CONTRACT.md` |
| **opus 5** | §1.2 coverage-gate semantics; §2.5 screen methodology (residualization, threshold pre-registration, multiple comparisons — the §6.0b regime variants roughly double the candidate count); leak rules for every new column + `transfer_portal.season` direction; `dev_index`/`draft_yield`/`conversion` residualization; §6.0b regime-scoping and era-interaction design; `preseason_v1` contract + walk-forward + CARRYOVER sweep + crossover rule; Monte Carlo math (sigma, draw structure, percentiles, tiebreak, calibration); §5.3 driver contribution math; Phase 6 label + censoring; design-doc §1f/§1i/§2a amendments |
| **opus 5 (main loop)** | Orchestration, delegation, deploy branches, commits, log reading, gate go/no-go, ledger decisions, PR |

**Consequence of losing fable:** opus 5 now both orchestrates and owns
correctness design, so disciplined delegation to haiku/sonnet matters more for
cost. It also means the review gates below are **self-review by the
orchestrating model**, which is weaker than independent review. Mitigation: run
each review as a separate focused pass with fresh context against the diff —
not inline while orchestrating.

## Opus review gates

Required before landing, independent of who authored:

| Artifact | Why |
|---|---|
| Any `features.team_week` column contract change | Migration 028's header forbids it without a design-doc update; the doc is authoritative |
| Any as-of / leak predicate | A leak is invisible in output and inflates every downstream metric |
| §2.5 screen results | Decides which of ~25 candidate columns ship; the naive thesis already failed once |
| Walk-forward / frozen-fit selection | `score_fitted.py` hard-errors on a *missing* fit but silently accepts a wrong-vintage one |
| Simulation sigma + draw structure | Wrong sigma yields confident, well-formatted, wrong win totals |
| §6.0b regime + era design | Silently blending coaching regimes or eras produces a feature that describes no real program |
| Phase 6 label + join-rate audit | Censoring and join failures both produce plausible sums from broken data |
| Deploy sequencing before each PR to main | Tier 1–3 precedent |

Review is **advisory on ship/no-ship gates** — §2.5 column selection, §3.4
`preseason_v1` ship decision, §4.5 calibration acceptance. Opus produces the
analysis and recommendation; **the user rules.** Matches Tier 3's "tuning grid:
advisory table; user decides ledger changes."

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
| No forward-looking draft prospect source exists | Derive `draft_prob_v1` internally (§6.2); external boards are paywalled/ToS-risky and not worth the dependency |
| `pre_draft_grade` looks like a prospect feature but is survivorship-biased | Only drafted players have rows; explicitly excluded as a roster feature (§6.1) |
| Draft production confounded with recruiting talent | Split into produced-vs-departed with opposite signs (§2.4b); prefer `draft_yield` (residualized) over raw counts |
| Roster→recruit→draft join rates unmeasured | Hard gate before any team-level draft-capital sum is trusted (§6.4) |
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

**A4. The partial-correlation screen — RUN 2026-07-26 against prod.**
Seasons 2015–2025, n = 1,439 team-seasons. Column 2 controls for prior-season
SP+; column 3 additionally controls for `recruiting_points_3yr`, the strongest
single candidate and therefore the bar every other candidate must clear.

| candidate | vs prior SP+ | + recruiting | verdict |
|---|---|---|---|
| `recruiting_points_3yr` | **+0.2642** | *(is the control)* | **SHIP** |
| `blue_chip_pipeline` | +0.2532 | **+0.0931** | **SHIP** |
| `recruiting_points_regime` | +0.1525 | **+0.0955** | **SHIP** |
| `talent_stock` | +0.2664 | ~+0.002 | reject |
| `pipeline_index` | +0.0952 | — | reject |
| `draft_picks_3yr` | +0.0949 | **+0.0068** | reject |
| `conversion` / `draft_yield` | +0.0760 | **−0.0007** | reject |
| `conversion_regime` | +0.0876 | +0.0344 | reject |
| `draft_departures` | +0.0088 | — | reject |
| `portal_net_rating` (2021–25, n=663) | +0.0274 | +0.0731 | reject, **re-test** |
| `portal_out_n` (2021–25) | +0.0586 | −0.0139 | reject |

Portal-era recruiting partial is **+0.2840** — the signal is *stronger* after
2021, not weaker.

**What this settles.**

1. **Trailing recruiting pipeline is the strongest preseason signal found**,
   at 3× the pre-registered floor.
2. **Draft production is redundant, not absent.** +0.0949 alone, +0.0068 once
   recruiting is controlled. It identifies programs that *recruit* well, not
   ones that *develop*.
3. **Regime-scoping is real (§6.0b validated).** Restricting the recruiting
   window to classes signed under the current head coach scores lower alone
   (+0.1525 vs +0.2848) but adds **+0.0955 beyond** the flat window — the pair
   beats either alone, because a short window itself encodes "new staff,
   inherited roster."
4. **The development term survives regime-scoping but still fails.**
   `conversion` moves from −0.0007 to +0.0344 when scoped to the current staff
   — right direction, under half the floor. Draft counts are a coarse, laggy
   proxy and season-level SP+ may not isolate development; the negative result
   is on **this measurement**, not on the concept.
5. **`draft_departures` justifies the whole gate:** raw +0.3474, partial
   +0.0088.

**Both composites in §6.0 failed.** `pipeline_index` scores +0.0952 against
its own input's +0.2664 — the multiplication destroys signal. `talent_stock`
beats plain recruiting by +0.002.

**Revised Tier A — ship three columns, not five:** `recruiting_points_3yr`,
`blue_chip_pipeline`, `recruiting_points_regime`. Drop `talent_stock`,
`pipeline_index`, `conversion`/`draft_yield`, `draft_picks_3yr`,
`draft_departures`. Rejected candidates stay in
`screen_preseason_features.py`'s `CANDIDATE_COLUMNS` so the nulls remain
reproducible; `SHIPPED_COLUMNS` is what migration 042 consumes.

**Unaffected by any of this:** §2.0's finding that `returning_ppa_pct` cannot
see the lines is structural — it follows from PPA's definition, not from these
correlations, and holds regardless of how the screen resolves. The line-play
columns (`prior_line_yards`, `prior_stuff_rate_allowed`,
`prior_def_line_yards`, `prior_front_seven_havoc`) are also low-risk
independent of the screen: they are measured performance rather than inferred
continuity, cost no new ingest, and have no roster dependency.
