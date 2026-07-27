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

- **BLOCKING — adversarial pass run against the diff, findings fixed, pass re-run against the fixes, BEFORE the PR opens** (see "Opus review gates")
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

- **BLOCKING — adversarial pass run against the diff, findings fixed, pass re-run against the fixes, BEFORE the PR opens** (see "Opus review gates")
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

- **BLOCKING — adversarial pass run against the diff, findings fixed, pass re-run against the fixes, BEFORE the PR opens** (see "Opus review gates")
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

- **BLOCKING — adversarial pass run against the diff, findings fixed, pass re-run against the fixes, BEFORE the PR opens** (see "Opus review gates")
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

> **PRE-TESTED AND CLEARED (appendix A8, deploy runs 165/166, 2026-07-27).**
> Before building the estimator, the quantity it estimates was measured
> directly with hindsight. `oracle_prospects` — players on the season-S roster
> drafted in S+1..S+3 — scores a partial of **+0.3437** against season-S SP+
> controlling for prior SP+ and `recruiting_points_3yr`, versus the control's
> own +0.2642 and the pre-registered floor of 0.08. **Tier B is worth
> building.** Two caveats travel with that number and neither is optional: it
> is a *ceiling*, not a forecast, and the contamination-discounted figure to
> plan against is **+0.2589** (§A8). See appendix A8 before scoping 6.2.
>
> This also corrects a §2.4b reading. The 2026-07-26 screen rejected
> *backward*-looking draft production (`draft_picks_3yr`, +0.0834) as nearly
> redundant with recruiting; that result was taken to bear on roster draft
> potential, which it does not. The two score a factor of four apart off the
> same table.

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

- **BLOCKING — adversarial pass run against the diff, findings fixed, pass re-run against the fixes, BEFORE the PR opens** (see "Opus review gates")
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
orchestrating model**, which is measurably weaker than independent review.

**The main loop's definition of done for any phase is: pass green, adversarial
pass run, findings fixed, pass re-run against the fixes, PR opened.** Opening a
PR before that sequence completes is a process failure regardless of how the
diff looks — see the blocking gate under "Opus review gates". The pass must be
a separate invocation with fresh context against the diff, never inline while
orchestrating, because the author's context is exactly what hides the defect.

## Opus review gates

### BLOCKING: the adversarial pass runs BEFORE the PR is opened

**No PR to `main` may be opened until an adversarial review pass has run
against the full diff.** This is a hard prerequisite, not a recommendation, and
it is stated this way because the softer version already failed.

The rule, concretely:

1. Finish the work and get `ruff` + `pytest` green.
2. **Stop.** Run the adversarial pass as a *separate* invocation with fresh
   context, reading the diff as an adversary trying to find a number that is
   wrong but plausible — not as the author confirming intent.
3. Fix what it finds.
4. **Re-run the pass against the fixes**, which are themselves unreviewed code
   in correctness-critical paths.
5. Only then open the PR.

Step 4 is not padding. When this was run for the first time on PR #48 — late,
after the PR was already open — the pass against the *fix commit* found two
further defects that the fixes had introduced.

**What "adversarial" means here.** Not "does this match what I intended" but:
*where does this produce a confident, well-formatted, wrong answer?* Every
defect this project has hit shares that shape — the silent zero-row score, the
in-sample refit, sigma double-counting, unscored games as certain losses,
teams with no scorable games projected at 0.0 wins. None of them raised;
all of them looked fine.

**Evidence this gate is load-bearing** (PR #48, 2026-07-26):

| Pass | Findings |
|---|---|
| Author self-check while building | 0 |
| External review (Codex) of the original diff | **6**, all valid, 2 of them P1 |
| Adversarial pass against the fix commit | **2** more, introduced by the fixes |

Two of Codex's six were named *in advance* by the table below — "wrong sigma
yields confident, well-formatted, wrong win totals" and "a leak is invisible in
output" — and shipped anyway. Writing the gate down is not running it.

**External review does not substitute for this pass, and this pass does not
substitute for external review.** They caught disjoint sets. Where an
independent reviewer is available (Codex, a human), use both: the pre-PR pass
first, the external one on the opened PR.

**Self-review caveat, restated.** With opus 5 as both orchestrator and
correctness owner, this pass is self-review and is measurably weaker than the
external one — 6 findings vs 2 on the same body of work. Run it anyway; it is
cheap and it is not zero. But do not treat a clean self-pass as evidence the
diff is clean.

### Artifact gates

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

Two different things are called "review" here, and conflating them is what let
the pass get skipped. Keep them distinct:

- **The adversarial pass is BLOCKING.** It gates whether a PR opens at all.
  Not a judgment call, not the user's to waive by default.
- **Ship/no-ship decisions are ADVISORY** — §2.5 column selection, §3.4
  `preseason_v1` ship decision, §4.5 calibration acceptance. Opus produces the
  analysis and recommendation; **the user rules.** Matches Tier 3's "tuning
  grid: advisory table; user decides ledger changes."

The first is about whether the code is correct. The second is about whether a
correct result is worth shipping. Only the second is a matter of taste.

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

**A5. Review-gate evidence (PR #48, 2026-07-26).** The blocking pre-PR
adversarial pass exists because of this sequence, recorded so the rule is not
re-litigated as overhead:

| Pass | Findings |
|---|---|
| Author self-check while building | 0 |
| External review (Codex) of the original diff | **6** — 2×P1, 4×P2, all valid |
| Adversarial pass against the fix commit | **2** more, introduced by the fixes |

The six: refit trained on a partial season then scored it in-sample; conference
title odds computed from overall record; unscored games counted as certain
losses; postseason games inflating a regular-season outlook; residual sigma
double-counting append-only snapshots; the screen not reproducing its own
verdicts.

The two: teams with no scorable game written as 0.0-win projections; a
non-positive sigma passing the guard and making every game deterministic.

**Two of the six were named in advance** by the review-gate table — "wrong
sigma yields confident, well-formatted, wrong win totals" and "a leak is
invisible in output" — and shipped regardless. The gate was written and not
run. That is the specific failure the BLOCKING language now addresses.

**Every one of the eight shares a shape:** a confident, well-formatted, wrong
number rather than an error. That is what the adversarial pass is looking for,
and it is why "the tests pass" is not evidence of correctness here.

**Unaffected by any of this:** §2.0's finding that `returning_ppa_pct` cannot
see the lines is structural — it follows from PPA's definition, not from these
correlations, and holds regardless of how the screen resolves. The line-play
columns (`prior_line_yards`, `prior_stuff_rate_allowed`,
`prior_def_line_yards`, `prior_front_seven_havoc`) are also low-risk
independent of the screen: they are measured performance rather than inferred
continuity, cost no new ingest, and have no roster dependency.

---

### A6 — Preseason backtest results (re-run 2026-07-26 after Codex PR #51 fixes)

The section 4.5 gate. `scripts/backtest_preseason.py`, walk-forward: season S
scored from each team's **week-1** `features.team_week` vector with the frozen
S-1 fit, sigma from preseason residuals of seasons **before** S, all games
simulated as pending. FBS only, 10,000 sims, **n = 921 team-seasons**.

`maxGP = 0` every season — the selected vectors carried zero games played, so
they are genuinely as-of-season-start. 2018 is scored as a sigma seed and not
reported.

**Outcome-dependent games are excluded** (`drop_outcome_dependent`). CFBD files
conference championship games as `season_type='regular'`, and below FBS the
whole FCS/D2 playoff bracket too. Nobody could know in August that Georgia
would play Texas for the SEC, so each team's slate is capped at its first 12
chronological games; the `drop` column is what that removed.

| season | fit | teams | games | drop | sigma | mgn MAE | **win MAE** | RMSE | bias | p10-p90 | prior-yr | flat |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2019 | 2018 | 130 | 1544 | 33 | 19.69 | 15.07 | 1.76 | 2.15 | -0.24 | 76.9% | 2.03 | 2.24 |
| 2020 | 2019 | 127 | 563 | 0 | 19.37 | 15.17 | 1.43 | 1.84 | -0.06 | 74.8% | 1.72 | 1.77 |
| 2021 | 2020 | 130 | 2376 | 32 | 19.32 | 17.59 | 1.79 | 2.26 | -0.22 | 73.1% | 2.47 | 2.25 |
| 2022 | 2021 | 131 | 3614 | 43 | 20.60 | 18.22 | 1.80 | 2.24 | -0.25 | 73.3% | 2.14 | 2.03 |
| 2023 | 2022 | 133 | 3573 | 12 | 21.64 | 16.05 | 1.77 | 2.16 | -0.26 | 71.4% | 1.98 | 2.18 |
| 2024 | 2023 | 134 | 3695 | 50 | 21.32 | 15.81 | 1.95 | 2.41 | -0.27 | 68.7% | 2.27 | 2.16 |
| 2025 | 2024 | 136 | 3704 | 39 | 21.01 | 15.95 | 1.96 | 2.44 | -0.27 | 64.0% | 2.26 | 2.33 |

**Overall: win MAE 1.784, RMSE 2.225, bias -0.226, p10-p90 coverage 71.7%.**
Baselines: prior-season win rate 2.128, flat .500 2.140. **The model beats both.**

**Empirical residual quantiles of (actual - projected), n=921 — use these for an
interval, NOT +/- MAE:**

| p05 | p10 | p25 | p50 | p75 | p90 | p95 |
|---|---|---|---|---|---|---|
| -3.38 | **-2.61** | -1.21 | +0.11 | +1.81 | **+3.19** | +4.02 |

An 80% empirical interval around a projection is **[proj -2.61, proj +3.19]**.

**Findings:**

1. **The model is real but modest.** 1.784 vs 2.128 for "last year's record" is
   a 0.34-win edge (~16%). Worth having; not large. It misses the plan's ~1.5
   aspiration (`verdict=above_1.5`), recorded as measured.

2. **Removing the outcome-dependent games barely moved the headline** (MAE
   1.791 -> 1.784, coverage 72.5% -> 71.7%). The leak was real and had to go,
   but it was not what made the model look good — worth recording so the fix is
   not credited with more than it did.

3. **The distribution is too narrow**, and both calibration tables show it from
   opposite ends: `p_bowl_eligible` 0.8-0.9 predicted 0.848, observed **0.720**;
   `p_ten_plus` 0.2-0.3 predicted 0.246, observed **0.426**. Too little mass in
   *both* tails is one phenomenon — the v1 independent-draw limitation, now
   measured. **This is the case for promoting the correlated-draw variant
   (section 4.2's v1.1) to next.**

4. **Coverage decays 76.9% (2019) -> 64.0% (2025)** while sigma rose only
   19.7 -> 21.0. Real season variance is outgrowing the model's spread,
   plausibly the portal/NIL structural break section 6.0b flags. Recent
   projections are more overconfident than the pooled 71.7% suggests.

5. **A stable -0.23 win bias** in every season but 2020 — small, consistent, and
   a candidate for a calibration term rather than a modelling change.

6. **The error distribution is right-skewed** (p10 -2.61 vs p90 +3.19, median
   +0.11). Teams overperform a projection by more than they underperform it,
   which is what a win-total floor at zero and a breakout ceiling well above the
   projection would produce.

**Consequence for the 2026 numbers already published.** Oklahoma's 7.08 carries
an 80% empirical interval of **[4.5, 10.3]** — not the "5.3-8.9" an earlier
draft of this appendix derived by treating MAE as a half-width. That was wrong:
MAE is an average loss, and for a roughly normal error with MAE 1.78 the SD is
about 2.23, so +/- MAE spans only ~58%. Oklahoma's stated `p_bowl_eligible` of
84.1% sits in a bucket that historically delivered **72.0%**; its `p_ten_plus`
of 5.9% sits in one that delivered **7.3%**.

**Against a market number of 7.5:** the 0.42-win gap is a small fraction of an
80% interval spanning ~5.8 wins, so this backtest gives no basis for preferring
the model's number to the market's. That is a statement about resolution, not a
significance test — the earlier claim that the two are "statistically
indistinguishable" asserted more than was tested. Any 2026 claim needs to be
worth more than this interval before it means anything, which is also the bar
Phase 2 has to clear: new features must **reduce** this error, not merely move
the number.

---

### A7 — Correlated-draw calibration (simulation v1.1, run 2026-07-26)

Section 4.2 queued a correlated variant as v1.1 "a ~15-line change". Appendix
A6 turned that from a nicety into the obvious next fix: p10-p90 coverage 71.7%
against a nominal 80%, with `p_bowl_eligible` overconfident at the top and
`p_ten_plus` underconfident in the middle — too little mass in *both* tails,
which is one phenomenon.

**Mechanism.** `simulate_wins` now draws one season-strength offset per team
per simulation and applies it to every game that team plays. `strength_sd`
splits sigma so that

```
2*tau^2 + game_sd^2 == sigma^2
```

i.e. **total per-game margin variance is unchanged**. Single-game predictions
stay exactly as calibrated as they were; only the correlation structure across
a team's games moves. `strength_share = 0` reproduces v1 exactly.

**Sweep** (`--sweep-strength-share`, 2019-2025, n=921, scored once and
re-simulated per candidate):

| rho | coverage | \|gap to 80%\| | win MAE | RMSE | bias | bowl Brier | ten Brier |
|---|---|---|---|---|---|---|---|
| 0.00 | 71.7% | 0.083 | 1.784 | 2.225 | -0.226 | 0.1934 | 0.0965 |
| 0.05 | 75.4% | 0.046 | 1.784 | 2.225 | -0.226 | 0.1911 | 0.0953 |
| 0.10 | 78.2% | 0.018 | 1.784 | 2.225 | -0.226 | 0.1897 | 0.0944 |
| **0.15** | **79.6%** | **0.004** | **1.784** | 2.225 | -0.226 | 0.1887 | 0.0936 |
| 0.20 | 80.8% | 0.008 | 1.785 | 2.225 | -0.226 | 0.1880 | 0.0931 |
| 0.25 | 83.3% | 0.033 | 1.785 | 2.225 | -0.226 | 0.1874 | 0.0927 |
| 0.30 | 84.8% | 0.048 | 1.784 | 2.225 | -0.226 | 0.1870 | 0.0924 |
| 0.40 | 86.8% | 0.068 | 1.784 | 2.225 | -0.226 | 0.1865 | 0.0923 |

**Shipped: `DEFAULT_STRENGTH_SHARE = 0.15`.**

**Findings:**

1. **Win MAE is 1.784 at every share swept.** The variance constraint working
   as designed — the point estimate is untouched and only the spread moves.
   This is the strongest evidence the change is safe: it cannot have bought
   coverage by degrading the projection, because the projection did not move.

2. **Coverage is fixed**: 71.7% -> 79.6%, a gap to nominal of 0.004. The
   defect A6 identified is closed.

3. **A genuine tension, resolved and recorded.** Brier improves monotonically
   *past* 0.15 (bowl 0.1934 -> 0.1865 at 0.40), so a Brier-only criterion
   would pick a much larger share than a coverage criterion. Resolved in
   favour of coverage: it is the direct measure of the defect being fixed,
   0.15 already captures about two thirds of the available Brier gain, and
   beyond 0.20 the intervals become too wide to be informative (86.8% coverage
   from a nominal-80% interval is not a better interval, it is a vaguer one).
   This is a judgment call, not a derivation, and is recorded as such.

4. **Residual quantiles barely move** (p10 -2.61 -> -2.59, p90 +3.19 -> +3.21),
   which is the consistency check: those measure the error of the *point
   estimate*, and correlation is not supposed to touch it.

**Remaining simplification.** Offsets are independent ACROSS teams, so a
conference whose teams all overperform together is still underweighted. That
is a further refinement, not a defect in this one.

**Provenance.** Migration 044 stores `strength_share` per row, for the same
reason 043 stores `residual_sigma`: two rows drawn under different correlation
structures must not be indistinguishable, and the append-only history has to be
able to explain why an interval widened on the day the share changed. NULL
means "written before v1.1" rather than a back-filled value the writer never
used. The migration also supersedes the `n_sims` column comment, which still
told consumers the tails were understated.

---

### A8 — The section 6 oracle pre-test (deploy runs 164/165/166, 2026-07-27)

**The question.** §2.4b established that *backward*-looking draft production —
picks a program produced in S-1..S-3 — is nearly redundant with recruiting
(`draft_picks_3yr`, +0.0834 against a 0.08 floor). That was then read as
bearing on §6's construct, which is a different one: **how much NFL talent is
on the current roster**. "This team has 12 draftable players and that one has
2" is a claim about season S, not about what the program graduated three years
ago. Conflating them was an error and this appendix corrects it.

**The measurement.** Hindsight, deliberately:

```
oracle_prospects(S, team) = players on the season-S roster who were drafted
                            in any of the S+1 .. S+3 NFL drafts
```

Screened by `scripts/screen_preseason_features.py` under the unchanged
pre-registered rule (`MIN_PARTIAL_R = 0.08`, `FDR_ALPHA = 0.10`,
`MIN_SCREEN_N = 400`): partial correlation against season-S SP+, controlling
for prior-season SP+ **and** `recruiting_points_3yr`.

**Why it is worth a session.** `oracle_prospects` is what a `draft_prob_v1`
model would try to *estimate*. An estimator cannot beat the thing it estimates,
so this is the **ceiling** on Tier B — measurable before a line of Tier B is
written.

| candidate | n | vs prior SP+ | + recruiting | q |
|---|---|---|---|---|
| `oracle_prospects` | 1,161 | +0.4393 | **+0.3437** | <1e-5 |
| `oracle_prospects_next` (S+1 only) | 1,161 | +0.3971 | **+0.3138** | <1e-5 |
| `oracle_prospects_lagged` (S+2..S+3) | 1,161 | +0.3719 | **+0.2589** | <1e-5 |
| `oracle_prospects_weighted` | 1,161 | +0.3518 | **+0.2319** | <1e-5 |
| `recruiting_points_3yr` | 1,439 | +0.2642 | *(is the control)* | <1e-5 |
| `draft_picks_3yr` (backward-looking) | 1,439 | +0.2342 | +0.0834 | 0.003 |

**Verdict: BUILD Tier B.** The ceiling is 4.3× the effect floor and above the
strongest shipped feature in the warehouse. The NFLMDD subscription question
(§6.5) does not resolve with a cancellation and stays open on its own merits —
a purchased board is another estimator of this same quantity, so it is now
competing against a real ceiling rather than an unknown one.

**Contamination, and what survives it.** The oracle is inflated by reverse
causality: a breakout season is part of what *makes* a player a prospect. The
pre-registered failure mode was that the signal lives entirely in the S+1
draft and dies once that draft is dropped. It did not happen. `_next` is the
stronger half (+0.3138), which is the contamination appearing exactly where it
was predicted to, but `_lagged` retains **+0.2589** — three quarters of the
pooled column, 3.2× the floor, and statistically indistinguishable from the
recruiting control. Weighting by recruiting rating instead of counting heads
holds +0.2319 despite 29.9% of prospects linking to no recruiting row and so
entering at weight 0, which makes that figure a lower bound.

**Plan against +0.2589, not +0.3437.** Three deductions stack, and only the
first is quantified here: contamination (the pooled→lagged gap), residual
contamination inside `_lagged` (a breakout also lifts S+2 stock, less
directly), and estimator error — `draft_prob_v1` has to infer from a July
roster what the oracle simply looks up, which is a strictly harder problem than
anything measured here.

**Guards, and why they are in the appendix.** Four draft verdicts once stood
for a month on a column that was a fabricated zero on 54% of rows (see
`screen_preseason_features.py`, "DEFECT FOUND BY THE SPLIT"). The oracle
COALESCEs a count to zero off two sources, so it carries two guards, both
reported by `--audit-imputation` (run 164):

| counter | value | meaning |
|---|---|---|
| `oracle_draft_window_incomplete` | 269 / 1,439 | 2024–25: **all three** of S+1..S+3 must be ingested, not any |
| `oracle_roster_absent` | 11 / 1,439 | the `nationalAverages` pseudo-team in `ratings.sp_ratings` |
| `oracle_roster_thin` | 1 / 1,439 | roster under 40 players; enters the screen |
| `oracle_weighted_unrated_players` | 1,880 / 6,286 | weighted variant's attenuation |
| `draft_*_no_source_year` | 0 | the 2000–2019 backfill still holds |

Guarded rows go **NULL**, not 0, and drop from the oracle's complete cases —
so the usable window (S ∈ 2015–2023) fell out of the data rather than being
asserted. Run 165 re-screened on an explicit `--from 2015 --to 2023` and
returned all four partials identical to four decimals on the same n = 1,161,
confirming the guard rather than the CLI flag selects the seasons.

**Two measurement caveats, both attenuating rather than flattering.** The
draft→roster join is 99.6–100% for drafts 2020–2026 but 94.9–95.6% for
2016–2019 (FBS picks only; the unmatched are overwhelmingly FCS and Ivy
players `core.roster` does not carry), so 2015–2018 undercount by roughly a
twentieth — mean prospects per FBS roster 5.02 in 2015 against 5.45–5.57 from
2018 on. And the weighted variant's 29.9% unlinked share conflates "no service
rated him" with "the join failed", which is why it reads in one direction only.

**No existing verdict moved.** Run 166 reproduces every recorded 2015–2025
partial in A4 exactly. The four oracle columns are recorded in a new
`ORACLE_COLUMNS` bucket: measured with hindsight, **never shippable** at any
effect size, because a coefficient on one would be predicting the past.
`tests/test_screen_features.py::TestOraclePreTestIsGuardedAndUnshippable`
enforces that they cannot reach `SHIPPED_COLUMNS`, an owner override, or
`features.team_week`.
