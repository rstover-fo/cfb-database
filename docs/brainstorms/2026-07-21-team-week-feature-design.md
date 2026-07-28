# features.team_week + fitted_v1: Definitive Modeling-Substrate Spec

Design doc for Tier 3 Pillars B (fitted/calibrated models) and C
(`features.team_week`). This is the implementation contract for
`build_features.py`, `train_model.py`, `score_fitted.py`, and migration 028.
Every column, leak rule, imputation rule, and persistence shape below is fixed;
implementers should not re-derive them.

Upstream dependencies already fixed by the plan:
- Migration **027** lands `analytics.adjusted_epa_week_build` (as-of ridge-EPA
  coefficients *entering* week W; columns `team, season, week, off_coef,
  def_coef, hfa_coef, mu, plays, lambda, n_teams`; `plays` is the team's
  offensive play count, mirroring `analytics.adjusted_epa_build`).
- Migration **028** lands the whole `features` schema: `features.team_week`,
  `features.model_coefficients`, `features.model_metadata` (this doc §1, §2).
- `marts.play_epa` has **no** `week`/`season_type` — both come from
  `JOIN core.games g ON g.id = pe.game_id` (`g.week`, `g.season_type`).

---

## 0. Grain, week keying, and the as-of window

**Grain:** one row per **team-game**, `UNIQUE (game_id, team)`. Both the home
and away side of every game get a row. (CORRECTED post-first-build: the
original `(season, season_type, week, team)` key assumed a team plays ≤ 1
game/week, but CFP semifinal + championship are BOTH postseason week 1, and
data quirks can duplicate a regular week — see migration 030. All as-of
semantics are unchanged; same-week rows carry identical as-of features and
differ only in game identity. Model joins key on `(game_id, team)`.)

**Week keying decision (pick ONE, justified):** store the natural CFBD `week`
plus `season_type`, and **UNIQUE (season, season_type, week, team)** — *not*
`(season, week, team)` alone. Reason: CFBD restarts week numbering at 1 for
`season_type='postseason'` (bowls are week 1; see `api/027_poll_rankings.sql`),
so regular week 1 and postseason week 1 collide on `(season, week, team)`.
Adding `season_type` to the key is the minimal fix and keeps the natural join
to `core.games` / `predictions.game_predictions` (both carry
`season, week, season_type`) trivial.

For **ordering and as-of comparison** we additionally store a derived monotone
integer `week_index`:

```
week_index = week                            when season_type = 'regular'
week_index = POSTSEASON_WEEK_OFFSET + week   when season_type = 'postseason'   (OFFSET = 100)
```

`POSTSEASON_WEEK_OFFSET = 100` is safely above any regular-season week, so
postseason rows always sort after every regular-season row of the same season.
All "as-of" logic below is expressed in `week_index`, never raw `week`.

**As-of rule:** a row keyed to `week_index = WI` may only use data with
`week_index < WI` **within the same season** (data through the end of the prior
played week). No cross-season leakage except the explicitly-labeled
prior-season fallbacks (adj-EPA week-1 fallback, preseason SP proxy), which are
known before the season starts and are therefore leak-free by construction.

**Spine driver:** the authoritative spine is `core.games` (every `(season,
home_team|away_team, week, season_type)` tuple, completed or scheduled), which
already contains upcoming/unplayed rows for the current season.
`ref.calendar` only holds the *current* season (`calendar_resource` loads
`get_current_season()` only), so it cannot back a 2015+ backfill spine; it is
used only as week-metadata (season_type boundaries / dates) for the live
season. **Decision: spine = distinct team-sides of `core.games`; `ref.calendar`
is metadata, not the driver.**

**Backfill scope:** 2015+ (~21K rows). Daily cadence appends the current
season's played + upcoming weeks.

---

## 1. `features.team_week` column spec (migration 028)

Types follow the repo house style (`analytics.*` staging: `BIGINT` for
season/week counts, `NUMERIC(p,s)` for rates/coefficients, `VARCHAR`/`TEXT` for
identity, `TIMESTAMPTZ` for clocks). Null-guarded rates use `NUMERIC(8,5)` to
match `adjusted_epa_build`.

### 1a. Identity / spine

| Column | Type | Source / aggregation | Leak rule |
|---|---|---|---|
| `season` | BIGINT NOT NULL | `core.games.season` | spine |
| `season_type` | VARCHAR NOT NULL | `core.games.season_type` (`'regular'`/`'postseason'`) | spine |
| `week` | BIGINT NOT NULL | `core.games.week` (raw CFBD week) | spine |
| `week_index` | BIGINT NOT NULL | derived (see §0) | spine |
| `team` | VARCHAR NOT NULL | `home_team`/`away_team` side of `core.games` | spine |
| `conference` | VARCHAR | that side's `home_conference`/`away_conference` | known constant |
| `game_id` | BIGINT | `core.games.id` for this team-week's game | spine (identity only) |
| `games_played_to_date` | BIGINT NOT NULL | `COUNT(*)` of completed `core.games` for team with `week_index < WI`, same season | `week_index < WI`; **0, not NULL** (a genuine count) |

`UNIQUE (season, season_type, week, team)`. Indexes: unique key; `(season,
week_index)`; `(team, season)`.

### 1b. House Elo (pregame entering week W)

| Column | Type | Source / aggregation | Leak rule |
|---|---|---|---|
| `elo_pregame` | NUMERIC(8,2) | Completed game: `analytics.house_elo_game.home_pregame_elo` if team is home else `away_pregame_elo` (walk-forward by construction). Upcoming game (no `house_elo_game` row yet): `analytics.house_elo_current.rating` with `carryover_rating(rating, season − snapshot_season)` (`compute_predictions.resolve_elo`); missing team → `EloEngine.SEED` (1500). | pregame value only; never a postgame or end-of-season rating |

Week-1 carryover: `house_elo_game`'s week-1 pregame already has
`start_season()`'s carryover applied, so no special-casing in `build_features`
— just read the game's stored pregame Elo. `elo_pregame` is never NULL (SEED
fallback covers unknown teams).

### 1c. Adjusted EPA as-of (opponent-adjusted, entering week W)

Lookup order in `build_features` for `(team, season=S, week_index=WI)`:

1. **As-of week fit** — `analytics.adjusted_epa_week_build` row for
   `(team, S)` with the greatest stored entering-week `≤ WI`, **provided** that
   row's `plays ≥ MIN_TEAM_PLAYS` (see predicate below). For postseason rows
   this naturally resolves to the last regular-week fit (= full regular season).
2. **Prior-season fallback** — else `analytics.adjusted_epa_build` row for
   `(team, S−1)` (full-season fit, known before S starts → leak-free).
3. Else **NULL** (model imputes).

**Fallback predicate (exact):** use the week fit iff
`wk.plays IS NOT NULL AND wk.plays >= MIN_TEAM_PLAYS` where `MIN_TEAM_PLAYS =
150` (≈ two games of a team's offensive plays; `plays` is the team's offensive
play count). In practice this routes *entering weeks 1–2* (0 / ~70 plays) to
the prior-season fallback and *entering week 3+* to the as-of week fit.
`MIN_TEAM_PLAYS` is a documented tunable.

| Column | Type | Source | Leak rule |
|---|---|---|---|
| `adj_epa_off` | NUMERIC(8,5) | resolved `off_coef` (higher = better offense) | as-of week fit or S−1 fallback |
| `adj_epa_def` | NUMERIC(8,5) | resolved `def_coef` (LOWER/more-negative = better defense) | as-of week fit or S−1 fallback |
| `adj_epa_net` | NUMERIC(8,5) | `off_coef − def_coef` (higher = better; subtracting a more-negative def adds) | derived from the two above |
| `adj_epa_hfa` | NUMERIC(8,5) | resolved `hfa_coef` (team's fitted HFA) | as-of week fit or S−1 fallback |
| `adj_epa_source` | VARCHAR | `'week'` \| `'prior_season'` \| NULL (provenance flag for the leak audit) | — |

### 1d. Season-to-date raw production (`marts.play_epa` ⋈ `core.games`)

All aggregates over `marts.play_epa pe JOIN core.games g ON g.id = pe.game_id`,
filtered `pe.season = S AND NOT pe.is_garbage_time AND
week_index(g) < WI`. Offense family filters `pe.offense = team`; defense-allowed
family filters `pe.defense = team`. `success = (epa>0)`, `explosive =
(epa>0.5)` are precomputed 0/1 columns in `play_epa`.

| Column | Type | Aggregation | Leak rule |
|---|---|---|---|
| `off_epa_per_play` | NUMERIC(8,5) | `AVG(pe.epa)` where `offense=team` | `week_index<WI` |
| `off_success_rate` | NUMERIC(8,5) | `AVG(pe.success)` where `offense=team` | `week_index<WI` |
| `off_explosiveness_rate` | NUMERIC(8,5) | `AVG(pe.explosive)` where `offense=team` | `week_index<WI` |
| `off_plays_per_game` | NUMERIC(8,3) | `COUNT(*) where offense=team / NULLIF(games_played_to_date,0)` (tempo proxy) | `week_index<WI` |
| `def_epa_per_play_allowed` | NUMERIC(8,5) | `AVG(pe.epa)` where `defense=team` | `week_index<WI` |
| `def_success_rate_allowed` | NUMERIC(8,5) | `AVG(pe.success)` where `defense=team` | `week_index<WI` |
| `def_explosiveness_rate_allowed` | NUMERIC(8,5) | `AVG(pe.explosive)` where `defense=team` | `week_index<WI` |

### 1e. Havoc (season-to-date, sourced from `stats.game_havoc`)

**Sourcing decision:** use `stats.game_havoc` (CFBD authoritative per-game
havoc, the same source-of-truth `marts.defensive_havoc` adopted), **not**
recomputed from `play_epa`. `game_havoc` has PK `(game_id, team)` and an FK to
`core.games`, so join `gh.game_id = g.id` for the `week_index < WI` window
(finer than the mart's season-only aggregation). Rate = event-weighted
`SUM(events)/NULLIF(SUM(plays),0)`, COALESCE-ing each bigint event column with
its dlt `__v_double` VARIANT twin (mart 005 pattern).

| Column | Type | Aggregation | Leak rule |
|---|---|---|---|
| `havoc_rate_defense` | NUMERIC(8,5) | `SUM(COALESCE(defense__total_havoc_events, __v_double)) / NULLIF(SUM(defense__total_plays),0)` for team's games, `week_index<WI` | `week_index<WI` |
| `havoc_rate_offense_allowed` | NUMERIC(8,5) | same with `offense__total_havoc_events` / `offense__total_plays` (havoc committed against the team's offense) | `week_index<WI` |

> Build-time verify: `offense__*` havoc columns are present but were unused by
> mart 005; re-run the information_schema presence check for exact dlt names
> (incl. `__v_double` twins) before wiring, per mart 005's diagnosis header.

### 1f. Preseason-known constants (populated for ALL weeks, constant within season)

| Column | Type | Source | Leak rule |
|---|---|---|---|
| `returning_ppa_pct` | NUMERIC(8,4) | `marts.returning_production.returning_ppa_pct` `(season=S, team)` | preseason-known; same for all weeks of S |
| `returning_passing_ppa_pct` | NUMERIC(8,4) | `marts.returning_production.returning_passing_ppa_pct` | preseason-known |
| `returning_rushing_ppa_pct` | NUMERIC(8,4) | `marts.returning_production.returning_rushing_ppa_pct` | preseason-known |
| `returning_usage` | NUMERIC(8,4) | `marts.returning_production.usage` | preseason-known |
| `preseason_sp_rating` | NUMERIC(8,3) | **prior-season final** `ratings.sp_ratings.rating` `(year=S−1, team)` — proxy | preseason-known (see decision) |
| `preseason_sp_offense` | NUMERIC(8,3) | `ratings.sp_ratings.offense__rating` `(year=S−1, team)` | preseason-known |
| `preseason_sp_defense` | NUMERIC(8,3) | `ratings.sp_ratings.defense__rating` `(year=S−1, team)` | preseason-known |
| `recruiting_points_3yr` | NUMERIC(10,3) | decayed sum of `recruiting.team_recruiting.points` over `year ∈ [S−4, S−1]`, weight `0.8^(S−year−1)` | preseason-known; classes signed before S starts |
| `blue_chip_pipeline` | NUMERIC(6,4) | 4–5★ share of `recruiting.recruits` signees over `year ∈ [S−4, S−1]` | preseason-known |
| `hc_first_year` | NUMERIC(2,1) | 1.0 when the head coach's tenure at this school starts in S, else 0.0 (`ref.coaches__seasons`, gaps-and-islands so a second stint does not inherit the first) | preseason-known; the hire precedes the season |
| `hc_first_year_unproven` | NUMERIC(2,1) | 1.0 when S is the head coach's first at this school **and** he is unproven — no prior head-coaching season anywhere, or prior seasons averaging below 0.0 SP+. Career prior is his mean SP+ over seasons *strictly before* S at any school | preseason-known; both the hire and S−1's final SP+ precede week 1 |
| `draft_picks_3yr` | NUMERIC(6,1) | count of NFL draft picks the program produced in draft years S−3..S−1, never season S's own draft | preseason-known; all three drafts precede week 1 |
| `draft_departures` | NUMERIC(6,1) | count of picks the program LOST in the April draft of year S | preseason-known; the draft precedes week 1 |
| `prior_def_line_yards` | NUMERIC(8,4) | `stats.advanced_team_stats.defense__line_yards` `(season=S−1, team)` | **prior season only** — S−1 is complete before S starts |
| `prior_def_stuff_rate` | NUMERIC(8,4) | `stats.advanced_team_stats.defense__stuff_rate` `(season=S−1, team)` | **prior season only** |

**Migration 042 additions (2026-07-26).** The five columns above were added
after the §2.5 partial-correlation screen
(`scripts/screen_preseason_features.py`), which measured each against season-S
SP+ controlling for **both** prior-season SP+ and `recruiting_points_3yr`.
Verdicts, on 2015–2025:

| column | partial | note |
|---|---|---|
| `recruiting_points_3yr` | +0.2642 | strongest preseason signal in the set |
| `hc_first_year` | −0.1615 | second strongest; **superseded in the vector by `hc_first_year_unproven`**, migration 046 |
| `hc_first_year_unproven` | −0.1844 | migration 046 — the flat binary split by the incoming coach's career record |
| `draft_picks_3yr` | +0.0834 | migration 047 — survives the recruiting control, so NOT a recruiting proxy |
| `draft_departures` | −0.0925 | migration 047 — opposite sign; losing draftable players predicts worse |
| `prior_def_line_yards` | −0.0997 | yards **allowed**, so negative confirms the trenches thesis |
| `prior_def_stuff_rate` | +0.0816 | clears the 0.08 floor by 0.0016 — marginal |
| `blue_chip_pipeline` | +0.0782 | **below** the floor; shipped by explicit decision, see below |

Two notes that matter for anyone reading these numbers later:

- **`blue_chip_pipeline` did not clear the pre-registered rule.** It misses by
  0.07 standard errors at n=1,439, which the data cannot resolve, and its
  q-value is 0.0095. It ships as a recorded override
  (`SHIPPED_BY_DECISION`), not as a screen result.
- **Every offensive-line measure failed** — `prior_line_yards` +0.0223,
  `prior_power_success` +0.0520, `prior_stuff_rate_allowed` −0.0290. The
  trenches thesis is supported for the DEFENSIVE front and unsupported for the
  offensive front, as measured by prior-season line play. The two
  `havoc__front_seven` splits are **untestable**, not null: they exist for only
  17.4% of team-seasons.
- **The screen computed `recruiting_points_3yr` and `blue_chip_pipeline` with
  `COALESCE(..., 0)`.** This table does not (see §1i) — so the shipped columns
  are marginally *cleaner* than the measurement that justified them, not
  dirtier.

**Preseason SP+ decision:** use **prior-season (S−1) final SP+** as the
"preseason SP" proxy. The `ratings.sp_ratings` loader merges one value per
`(year, team)`; for any completed season that value is the *final/in-season*
SP+ (it knows the whole season → leaky if used for season S). CFBD does publish
a genuine preseason SP+, but our loader captures no preseason-specific
snapshot, so last season's final rating is the only leak-free SP signal
available at the start of season S. Documented as a proxy in the column
comment; a true-preseason snapshot loader is a follow-up.

### 1g. Bookkeeping

| Column | Type | Source |
|---|---|---|
| `computed_at` | TIMESTAMPTZ NOT NULL DEFAULT now() | write time |
| `feature_build_version` | VARCHAR | `build_features.py` version tag (audit) |

**Column count: 39** (8 identity + 1 Elo + 5 adj-EPA + 7 season-to-date + 2
havoc + **15** preseason + 2 bookkeeping). Was 31 before migration 042 added
five screened preseason columns, 36 before migration 046 added
`hc_first_year_unproven`, and 37 before migration 047 added the draft pair.
The fitted vector is 22: 046 SWAPPED slot 17 rather than extending, 047 adds
two (§2a).

### 1h. Explicitly EXCLUDED

`neutral_site` is a **game** property, not a team-week property (the same team
can play neutral one week, home the next). It is **not** stored in
`team_week`; it joins in at scoring time from `core.games.neutral_site` as a
game-level model term (§2).

### 1i. Per-family NULL semantics (what `build_features.py` writes)

| Family | Week-1 row (no prior games) | Rule |
|---|---|---|
| identity/spine | populated; `games_played_to_date = 0` | count is genuinely 0, **not** NULL |
| `elo_pregame` | populated (carryover / SEED) | never NULL |
| adj-EPA (`adj_epa_*`) | **prior-season (S−1) fallback**, `adj_epa_source='prior_season'`; NULL only if S−1 fit also absent | see §1c predicate |
| season-to-date (§1d) | **NULL** | no plays with `week_index<WI` exist |
| havoc (§1e) | **NULL** | no games with `week_index<WI` exist |
| preseason constants (§1f) | **populated** (constant all season) | NULL only if the source row is absent |
| migration-042 preseason (§1f) | **populated**, `NULL` where the source is absent | see below — never zero-filled |

**Decision — the migration-042 columns are NULL, not 0, when their source is
absent**, which is the §1i rule applied consistently rather than a new one.

It is worth spelling out because the screen that justified these columns got it
wrong in both directions and had to be corrected twice. `blue_chip_pipeline` is
`blue_chips / signees` and `prior_def_line_yards` is a rate: a team-season with
no prior FBS season does not have *zero* line yards, it has *unknown* line
yards, and no team ever posts 0.000. Worse, the teams whose source rows are
missing — new FBS entrants, programs up from FCS — are disproportionately weak
in season S, so zero-filling would plant the floor value exactly where the
outcome is low and manufacture signal out of nothing.

Rough absence rates measured 2015–2025: `recruiting_points_3yr` 0.8%,
`blue_chip_pipeline` 1.5%, `hc_first_year` 0.9% (no coach record),
`prior_def_*` 1.1% (no prior-season row). §2b's train-window mean imputation
resolves all of them centrally and leak-free.

**`hc_first_year` is genuinely 0.0, not NULL, for an established coach** — the
value is known and it is zero. NULL is reserved for "we have no coach record
for this school-year at all", which is a different statement.

**`hc_first_year_unproven` (migration 046) carries the same 0.0-is-real rule
and one more NULL case.** 0.0 is true for a continuing staff — there was no
hire, so the hire was not unproven — and true for a first-year hire whose
previous teams averaged at or above average. NULL covers two genuinely unknown
situations, and holding them apart is load-bearing: a coach with **zero** prior
head-coaching seasons is a real first-timer and *is* unproven (1.0), while a
coach with prior seasons that carry **no SP+ row** — an FCS stop, or a year
older than ratings coverage — is unknown (NULL). Collapsing the second into
"unproven" would put a fabricated category where a missing value belongs, which
is the error the zero-filled `recruiting_points_regime` column already made
once. Absence rate 2015–2025: 8.1% (115 school-years with no unambiguous coach
record, plus 2 with an unratable prior career).

**Both coach columns are 100% NULL for 2026** as of late July — CFBD has not
published 2026 coaching records — so they contribute nothing to the current
outlook until roughly August, and teams with new head coaches are projected as
though nothing changed until then.

**Decision — NULL, not 0, for empty season-to-date aggregates.** `0` is a false
signal (0 EPA/play is an *average-team* value, not "unknown"; 0 plays/game
would read as an extreme slow-tempo team). NULL correctly encodes "unknown";
the **model layer** (train/score) imputes with a train-window league mean (§2),
so the honest "unknown" is resolved once, centrally, with a leak-free statistic
— never silently inside the feature table.

---

## 2. `fitted_v1` feature vector spec (`train_model.py` / `score_fitted.py`)

`fitted_v1` = ridge linear **margin** model (normal equations, α≈5–10, intercept
unpenalized) + logistic **win-prob** via IRLS + **Platt** scaling. Target for
the margin model is `y = home_points − away_points`; for the logistic model
`y = 1[home wins]`.

### 2a. Ordered feature vector (design-matrix columns)

All non-game-level features are **home-minus-away diffs** of a single
`team_week` column, evaluated on the home and away team-week rows for the game's
`(season, season_type, week)`. Order is fixed (index = column position).

| # | Feature name | Kind | team_week column(s) | Penalized? | Standardized? |
|---|---|---|---|---|---|
| 0 | `intercept` | constant 1.0 | — | **no** | no |
| 1 | `neutral_site` | game-level 0/1 | `core.games.neutral_site` | yes | no (raw 0/1) |
| 2 | `d_elo` | diff | `elo_pregame` | yes | yes (z) |
| 3 | `d_adj_epa_off` | diff | `adj_epa_off` | yes | yes |
| 4 | `d_adj_epa_def` | diff | `adj_epa_def` | yes | yes |
| 5 | `d_off_epa_per_play` | diff | `off_epa_per_play` | yes | yes |
| 6 | `d_def_epa_per_play_allowed` | diff | `def_epa_per_play_allowed` | yes | yes |
| 7 | `d_off_success_rate` | diff | `off_success_rate` | yes | yes |
| 8 | `d_def_success_rate_allowed` | diff | `def_success_rate_allowed` | yes | yes |
| 9 | `d_off_explosiveness_rate` | diff | `off_explosiveness_rate` | yes | yes |
| 10 | `d_off_plays_per_game` | diff | `off_plays_per_game` | yes | yes |
| 11 | `d_havoc_rate_defense` | diff | `havoc_rate_defense` | yes | yes |
| 12 | `d_havoc_rate_offense_allowed` | diff | `havoc_rate_offense_allowed` | yes | yes |
| 13 | `d_returning_ppa_pct` | diff | `returning_ppa_pct` | yes | yes |
| 14 | `d_preseason_sp_rating` | diff | `preseason_sp_rating` | yes | yes |
| 15 | `d_recruiting_points_3yr` | diff | `recruiting_points_3yr` | yes | yes |
| 16 | `d_blue_chip_pipeline` | diff | `blue_chip_pipeline` | yes | yes |
| 17 | `d_hc_first_year_unproven` | diff | `hc_first_year_unproven` | yes | yes |
| 18 | `d_draft_picks_3yr` | diff | `draft_picks_3yr` | yes | yes |
| 19 | `d_draft_departures` | diff | `draft_departures` | yes | yes |
| 20 | `d_prior_def_line_yards` | diff | `prior_def_line_yards` | yes | yes |
| 21 | `d_prior_def_stuff_rate` | diff | `prior_def_stuff_rate` | yes | yes |

**22 features + intercept** (was 15 before migration 042, 20 before 047). Same vector feeds
both the ridge-margin and the IRLS-logistic fit.

**Why these five are diffed like everything else.** They are team properties,
so home-minus-away is the same construction used throughout.
`d_hc_first_year_unproven` takes values in {−1, 0, +1}: an unproven first-year
coach on one side only is a relative disadvantage, and two of them correctly
cancel — the model term is about *relative* disruption, not about how many new
coaches are in the game.

**Migration 046 — slot 17 swaps, the vector does not grow.** `d_hc_first_year`
is replaced by `d_hc_first_year_unproven`; the count stays at 20 and
`hc_first_year` stays populated in `features.team_week` for downstream
consumers, it simply no longer enters the fit. The reason is that the flat
binary averages two different populations. Second-order partial against
season-S SP+, controlling prior SP+ and `recruiting_points_3yr` (screen runs
138/139/140):

| subgroup | positives | 2015–2025 | 2015–2020 | 2021–2025 |
|---|---|---|---|---|
| unproven hire | 184 | **−0.1844** | −0.1417 | **−0.2257** |
| proven hire | 80 | +0.0096 | −0.0155 | +0.0368 |
| flat `hc_first_year` | 266 | −0.1548 | −0.1332 | −0.1738 |

A hire whose previous teams averaged at or above average costs essentially
nothing in year one; at n=1,322 the standard error is ~0.028, so that is a
*powered* null, not an underpowered shrug. The finer split (`rookie` −0.1340,
`prior_below` −0.1345) also cleared the floor but is pooled rather than
shipped: shipping components alongside their own sum is an exact linear
dependence, and `prior_below`'s implied 0.86-SD per-hire effect rests on 33
team-seasons.

**This swap invalidates every stored fit** for the same positional reason
migration 042 did — see below. Every `train_through_season` vintage is
retrained in the same deploy.

**Adding these invalidates every stored fit.** `FEATURE_NAMES` is positional
and `score_fitted.load_fit` builds its coefficient vector by name lookup over
it, so a fit trained before migration 042 raises `KeyError` on the new names
rather than degrading. Every `train_through_season` vintage must be retrained in
the same deploy that ships the columns; there is no partial-rollout path.

**`adj_epa_net` deliberately omitted from the model vector.** `net = off − def`
is an *exact* linear combination of `d_adj_epa_off` and `d_adj_epa_def`;
including it adds a perfect collinearity that contributes nothing to prediction
(ridge would merely split weight between the three). It stays a `team_week`
column for human-facing transparency but is not a model feature. Likewise
`def_explosiveness_rate_allowed`, `returning_passing/rushing_ppa_pct`,
`returning_usage`, `preseason_sp_offense/defense`, `adj_epa_hfa` are carried in
`team_week` for transparency/future variants but are **not** in `fitted_v1` —
kept lean to the plan's named set. `market_*` is **excluded by design** so
`edge` stays meaningful (a `fitted_market_v1` is the documented follow-up).

### 2b. Imputation (leak-free, TRAIN-window only)

1. For each `team_week` source column `c` used by a diff feature, compute
   `mean_c` = mean of `c` over **all team-week rows of the TRAIN games only**
   (both home and away sides), ignoring NULLs. (`elo_pregame`, preseason
   constants rarely NULL; season-to-date/havoc NULL for early weeks.)
2. When vectorizing any game (train **or** score), replace a NULL home-side or
   away-side value of `c` with `mean_c` **before** differencing. Equivalent to
   imputing the missing side to the league-average team.
3. `mean_c` is computed once per fit (per `train_through_season`) and **frozen**
   in `model_metadata.feature_means`; scoring never recomputes it. This is the
   single reason NULLs are safe (§1i).

### 2c. Standardization (z-score, TRAIN-window stats, stored with the model)

For each standardized diff feature `f` (all of #2–#14): compute
`diff_mean_f`, `diff_std_f` over the **TRAIN** design rows (after imputation),
and transform `f ← (f − diff_mean_f) / diff_std_f` at both train and score
time. `intercept` and `neutral_site` are not standardized. Freeze
`diff_mean_f`/`diff_std_f` in `model_metadata`. **Decision: z-score using
train-window stats only, persisted with the model** — no test-season statistic
ever touches the transform.

### 2d. Persisted artifacts (migration 028 tables)

**`features.model_coefficients`** — one row per feature per component per fit:

| Column | Type |
|---|---|
| `model_version` | VARCHAR NOT NULL (`'fitted_v1'`) |
| `train_through_season` | BIGINT NOT NULL |
| `model_component` | VARCHAR NOT NULL (`'margin'` \| `'winprob'`) |
| `feature_order` | BIGINT NOT NULL |
| `feature_name` | VARCHAR NOT NULL |
| `coefficient` | NUMERIC(12,6) NOT NULL |

`UNIQUE (model_version, train_through_season, model_component, feature_name)`.

**`features.model_metadata`** — one row per fit:

| Column | Type | Contents |
|---|---|---|
| `model_version` | VARCHAR NOT NULL | `'fitted_v1'` |
| `train_through_season` | BIGINT NOT NULL | S−1 for score season S |
| `ridge_alpha` | NUMERIC(8,3) | margin ridge α |
| `winprob_ridge_alpha` | NUMERIC(8,3) | IRLS Hessian ridge stabilizer |
| `platt_a` | NUMERIC(12,6) | Platt slope |
| `platt_b` | NUMERIC(12,6) | Platt intercept |
| `train_seasons` | BIGINT[] | explicit list of seasons in the train window |
| `n_train_games` | BIGINT | training game count |
| `feature_means` | JSONB | `{team_week_column: mean_c}` (imputation, §2b) |
| `feature_diff_means` | JSONB | `{feature_name: diff_mean_f}` (scaling, §2c) |
| `feature_diff_stds` | JSONB | `{feature_name: diff_std_f}` |
| `fit_at` | TIMESTAMPTZ NOT NULL DEFAULT now() | fit timestamp |

`UNIQUE (model_version, train_through_season)`.

`score_fitted.py` selects the frozen fit by
`(model_version='fitted_v1', train_through_season = S−1)` for backfill season S,
and by `MAX(train_through_season)` for daily upcoming scoring. Grants follow
`predictions` house style: `GRANT USAGE ON SCHEMA features` + `SELECT` to
`anon, authenticated`; revoke write.

---

## 3. Walk-forward protocol (implementable checklist)

Expanding window, **min 3 train seasons**, score `S ∈ 2018..2025` with the model
trained **through S−1**. Feature availability starts 2015, so the earliest
score season 2018 trains on {2015, 2016, 2017}.

For each `S` in `2018..2025`:

1. `train_seasons ← [2015 .. S−1]` (expanding; assert `len ≥ 3`).
2. **Train rows:** every completed `core.games` in `train_seasons` where both
   the home and away `team_week` rows exist for the game's `(season,
   season_type, week)`. Attach `y_margin = home_points − away_points`,
   `y_win = 1[home_points > away_points]`, and `neutral_site`.
3. **Impute means:** compute `feature_means` over the TRAIN team-week rows
   (§2b). Vectorize train rows with imputation.
4. **Scale:** compute `feature_diff_means/stds` over the imputed TRAIN design
   rows (§2c); apply.
5. **Fit margin:** ridge normal equations `(XᵀX + αP)β = Xᵀy_margin`, `P` zeroed
   at the intercept index (α from `model_metadata.ridge_alpha`).
6. **Fit win-prob:** IRLS logistic (~8 Newton steps) on the same `X`, target
   `y_win`, ridge-stabilized Hessian (`winprob_ridge_alpha`).
7. **Platt:** fit `(a, b)` mapping raw logistic logits → calibrated probability
   on the train predictions (or an in-train holdout); store `platt_a/b`.
8. **Persist** coefficients (both components) + metadata for
   `train_through_season = S−1`.
9. **Score S (backfill):** for every completed game in `S`, vectorize with the
   **frozen S−1** `feature_means`/scaling, apply β → `expected_home_margin`,
   apply logistic+Platt → `home_win_prob`, and write a
   `model_version='fitted_v1'` row into `predictions.game_predictions`
   (`prediction_date = start_date::date`, so re-runs are idempotent under the
   `(game_id, model_version, prediction_date)` key). `marts/038` scores it
   automatically.
10. **Daily upcoming:** score pending games with the latest frozen fit
    (`MAX(train_through_season)`).

**Gate B:** `fitted_v1` must beat `elo_v1` on walk-forward **MAE and Brier**
(target Brier ≲ 0.168 vs elo's 0.187). Fail → stays advisory, not wired into
daily.

---

## 4. Leak-audit checklist (Phase 3 gate spot-checks)

Per-family assertions the gate runs against the freshly-built `team_week`
(and the scored predictions):

- **Elo (walk-forward):** week-1 2019 Alabama `elo_pregame` equals its
  `analytics.house_elo_game` week-1 pregame Elo (2018 carry-over), and is **not**
  its end-of-2019 rating.
- **Adj EPA (fallback):** week-1 2019 Alabama `adj_epa_off/def` equal the **2018**
  full-season `analytics.adjusted_epa_build` coefficients and
  `adj_epa_source='prior_season'`. A week-8 2019 row uses the week-8 as-of fit
  and differs from both the 2019 full-season fit and the 2018 fallback;
  `adj_epa_source='week'`.
- **Season-to-date NULL:** every week-1 row has NULL `off_epa_per_play`,
  `def_epa_per_play_allowed`, `*_success_rate`, `*_explosiveness_rate`,
  `off_plays_per_game`, and `games_played_to_date = 0`.
- **Season-to-date window:** a week-6 row's `off_epa_per_play` aggregates only
  weeks 1–5 plays — the team's week-6 opponent's plays are provably absent
  (spot-check a known game_id is excluded).
- **Havoc:** week-1 rows NULL for `havoc_rate_defense` /
  `havoc_rate_offense_allowed`; later weeks aggregate only `week_index < WI`
  games.
- **Preseason constants:** `returning_ppa_pct` and `preseason_sp_rating` are
  **identical across all weeks** of a given (season, team); `preseason_sp_rating`
  for 2019 equals 2018 final SP+ and is **not** 2019 SP+.
- **Postseason ordering:** a 2019 bowl row (`week_index = 101`) has
  `games_played_to_date` = the full 2019 regular-season game count, season-to-date
  aggregates spanning all regular-season weeks, and adj-EPA = the last
  regular-week as-of fit.
- **No-future invariant:** for every row, `MAX(week_index)` of any contributing
  play/game/coefficient `< row.week_index` (same season) — a single assertion
  over the whole build.
- **Model leak (train/score):** `feature_means` and `feature_diff_means/stds`
  used to score season S are byte-identical to those stored under
  `train_through_season = S−1`; no S-season statistic appears in them.

---

## Decisions made

- **Grain key:** `UNIQUE (season, season_type, week, team)` (not `(season,
  week, team)`) — regular/postseason both number week 1; `season_type`
  disambiguates and keeps natural joins to games/predictions.
- **Ordering:** added derived monotone `week_index` (`+100` offset for
  postseason); all as-of logic keyed on it, raw `week` preserved for joins.
- **Spine driver:** `core.games` team-sides (completed + scheduled);
  `ref.calendar` is current-season metadata only (loader is single-season), not
  the backfill spine.
- **Adj-EPA fallback predicate:** use the as-of week fit iff `wk.plays ≥ 150`
  (`MIN_TEAM_PLAYS`, team offensive plays) else prior-season (S−1) full fit else
  NULL → routes entering weeks 1–2 to fallback.
- **Havoc sourcing:** `stats.game_havoc` (authoritative, per-game, joined by
  `game_id` for week window), not recomputed from `play_epa`; both
  defense-generated and offense-allowed rates.
- **Preseason SP+:** prior-season (S−1) **final** SP+ as the leak-free proxy —
  no preseason-specific snapshot exists in the loaded data.
- **Empty season-to-date aggregates = NULL, not 0**; model layer imputes with a
  frozen train-window league mean.
- **`neutral_site` excluded** from `team_week` (game property); joins in at
  scoring time as a game-level model term.
- **`adj_epa_net` and secondary columns excluded from the `fitted_v1` vector**
  (net is an exact off−def combo); kept in `team_week` for transparency.
- **Standardization:** z-score with **train-window** diff mean/std, frozen in
  `model_metadata`; intercept + `neutral_site` left unscaled.
- **Imputation:** league-mean per source column over **train games only**,
  frozen in `model_metadata.feature_means`, applied before differencing.
- **Migration 028 = whole `features` schema:** `team_week` +
  `model_coefficients` + `model_metadata`, `predictions`-style grants.
- **Migration 042 = five screened preseason columns** (`recruiting_points_3yr`,
  `blue_chip_pipeline`, `hc_first_year`, `prior_def_line_yards`,
  `prior_def_stuff_rate`), all in the `fitted_v1` vector, taking it to 20
  features. Every column cleared the §2.5 screen except `blue_chip_pipeline`,
  which is a recorded override rather than a measurement.
- **Migration 046 = `hc_first_year_unproven`, which SWAPS into slot 17** in
  place of `d_hc_first_year`. The vector stays at 20 and `hc_first_year` stays
  populated in the table. The first-year penalty is not a penalty for changing
  coaches: split by the incoming coach's career SP+ it sits entirely on
  unproven hires (−0.1844), while proven hires screen as a *powered* null
  (+0.0096, p=0.73, SE ≈ 0.028). Confirmed in both halves of a split-window
  re-run, and stronger in the portal era than before it.
- **Migration 047 = `draft_picks_3yr` + `draft_departures`**, two columns
  rather than one net because the signs are opposite (+0.0834 / −0.0925) and
  netting them cancels most of both. Both had been *rejected*; those rejections
  were void, measured while `draft.draft_picks` held 2020–2026 against a
  configured 2000–2026, so `COALESCE(…,0)` read an unloaded draft as "produced
  zero picks" on 54.2% of the frame. After backfilling 2000–2019 the same screen
  on the same frame reverses both findings, and every non-draft candidate
  reproduces to four decimals.
- **The draft columns are the documented exception to "NULL, never 0."** They
  are counts, and a program that sent nobody to the NFL genuinely produced zero
  — NULLing that discards signal. The zero is written *only where the source
  drafts exist*, via an `EXISTS` over draft years with **no team predicate**; a
  team absent from a draft that was loaded is a true zero, and that has to stay
  distinguishable from a draft nobody loaded.
- **Migration-042 and -046 columns are NULL when their source is absent, never 0** —
  the §1i rule, restated because these are rates and counts whose zero is a
  fabricated extreme rather than a neutral value.
- **The trenches thesis is supported on defense only.** Prior-season defensive
  line yards and stuff rate carry signal past prior SP+ and recruiting; every
  offensive-line measure failed the screen. Recorded as a finding, not dropped
  — the null is on prior-season line play as an instrument, and the
  front-seven havoc splits remain untestable at 17.4% coverage.
- **Feature vector size:** 15 features + unpenalized intercept; `market_*`
  excluded so `edge` stays meaningful.
- **2026-07-28 in-season screen = all six drive/trajectory candidates REJECTED;
  no migration shipped.** Pre-registered in `scripts/screen_week_features.py`
  (run once, 2015–2025, n≈8k completed FBS-involved games): |partial_r| ≥ 0.08
  AND BH-FDR q ≤ 0.10 against home margin, controlling home-minus-away
  `elo_pregame` and `adj_epa_net` diffs. Candidates and results — `off_ppd`
  +0.0700 (p=3.6e-10), `def_ppd_allowed` −0.0583 (p=1.8e-07), `off_field_pos`
  −0.0547 (p=9.8e-07), `def_field_pos_allowed` +0.0469 (p=2.7e-05),
  `form_net_epa_last4` +0.0163 (p=0.21, n=5,804), `vol_net_epa` −0.0058
  (p=0.63, n=7,203). The four drive candidates are significant but all below
  the pre-registered floor: drive efficiency is real signal the vector already
  absorbs through Elo and adjusted EPA. The two trajectory candidates are
  nulls — recent form and volatility add nothing to margin once
  opponent-adjusted ratings are controlled. Consequence: `features.team_week`
  gains no drive or trajectory columns; the planned volatility-modulated
  calibration experiment was dropped with its substrate; do not re-test these
  six without a new pre-registration that changes the frame or controls
  (source plan: `docs/plans/2026-07-28-001-feat-starter-pack-model-features-plan.md`).
  Disclosed deviation (cross-model review): the screen operationalized
  trajectory maturity as week-row gates (>= 4 prior weeks for form, >= 2 for
  volatility) rather than the substrate's `MIN_TEAM_PLAYS = 150` play-count
  gate. For form the week gate is effectively stricter (4 games is ~260+
  offensive plays); for volatility it admits some 2-game teams the substrate
  would NULL. Not verdict-relevant — both trajectory partials sit an order of
  magnitude below the 0.08 floor — but a future re-registration should carry
  play counts through the weekly aggregates and gate on them.
