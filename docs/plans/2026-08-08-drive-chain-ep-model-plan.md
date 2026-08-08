# Drive-State Markov Chain & House Expected Points — Design

**Date:** 2026-08-08
**Status:** Design (pre-implementation). Companion brainstorm:
`docs/brainstorms/2026-08-08-play-sequence-derivatives.md` (Tier 1).
**Anchors:** Goldner (2012, JQAS) absorbing-chain drive model (v1);
net-next-score handoff recursion (v1.5); Chan, Fernandes & Puterman (2021,
Operations Research) Markov-reward "points gained" (v2 aspiration).
**Cost:** zero CFBD API calls (one optional call to `/ppa/predicted` as a
validation benchmark).

## 1. Objective

An in-house expected-points model built from our own 2.7M plays:
`EP(state)` for every drive state, drive-outcome probabilities from any state,
and a house EPA per play. Today every EPA-derived number in the warehouse
inherits CFBD's PPA model; this makes the EPA layer self-sufficient,
benchmarkable, and extensible (4th-down model, sunburst, sustain/finish all
fall out of the same artifact).

## 2. Non-goals (v1)

- No win probability (the live WP model stays as is).
- No overtime states (OT drives excluded; `POSSESSION (FOR OT DRIVES)` rows
  dropped).
- No replacement of `ppa` anywhere downstream until validation gates pass —
  house EPA lands as a *parallel* column, never a silent swap
  (SCHEMA_CONTRACT discipline).

## 3. Data foundation (verified 2026-08-08, seasons ≥2014 unless noted)

- **Scrimmage-play vocabulary** (`core.plays.play_type`): Rush 840k,
  Pass Reception 428k, Pass Incompletion 295k, Sack 49k, Rushing TD 42k,
  Passing TD 41k, Interception(+return) 19k, Fumble recoveries 21k. Special
  teams: Punt 118k, Kickoff 117k, FG Good 27.8k / Missed 8.8k / Blocked 1.1k.
  Non-plays to exclude from transitions: Penalty 113k, Timeout 86k,
  End Period/Half/Game rows.
- **Drive outcomes** (`core.drives.drive_result`): PUNT 117k, TD 85k, FG 27k,
  DOWNS 20k, INT 19k, FUMBLE 13k, MISSED FG 10k, END OF HALF/GAME 18k, plus a
  defensive-score tail (INT TD 2.0k, FUMBLE TD ~1.1k, PUNT RETURN TD ~0.9k)
  and `Uncategorized` 2.7k (~1%% — dropped, count logged).
- **State support:** a flat 160-state grid (4 downs × 4 distance buckets ×
  10 field zones) has median support 4,861 plays but **38 states under 500**
  (min 1) — the structurally rare corners (e.g. 1st-and-short away from the
  goal line). A flat grid is not estimable; smoothing is a requirement, not a
  refinement (section 5).
- **FG curve sanity:** make rate declines monotonically 94.2% (≤20yd bucket)
  → 54.3% (50) → 45.0% (55), n=37k attempts. House FG submodel is feasible
  on counts alone.
- **Ordering:** `play_number` within (`game_id`, `drive_id`) plus each play's
  own (down, distance, yards_to_goal) snapshot. Penalties are handled
  implicitly: a transition is defined snapshot→snapshot between consecutive
  *scrimmage* plays in a drive, so a penalty's state change is absorbed into
  the observed next snapshot without modeling the penalty row itself.
  `NO PLAY` rows (penalty-nullified, visible in 2025 NCAA text) carry the
  pre-snap state of the replayed down and are excluded by the same rule.

## 4. State space

**Transient states:** `(down 1–4, distance bucket, field zone)` with
down-aware distance buckets (this is what fixes the starved corners):

- Down 1: `goal_to_go` | `standard` (distance ≈ 10) | `short` (<10, after
  penalty) | `long` (>10)
- Downs 2–4: `short` (≤3) | `med` (4–6) | `long` (7–10) | `xlong` (>10),
  with `goal_to_go` overriding when distance ≥ yards_to_goal
- Field zone: 10-yard bands of `yards_to_goal` (1–10 … 91–99)

**Absorbing states** (mapped from `drive_result`): `TD`, `FG`,
`MISSED_FG`, `PUNT`, `TURNOVER` (INT + FUMBLE), `TURNOVER_TD` (defensive
score, valued negatively), `DOWNS`, `SAFETY`, `END_OF_HALF`.
`END OF GAME`/`END OF 4TH QUARTER` fold into `END_OF_HALF`; blocked kicks fold
into their missed/turnover equivalents; `Uncategorized` dropped with a count
assertion (<2%% of drives or the build fails).

## 5. Estimation

1. **League matrix per era.** Raw transition counts → row-normalized, with
   empirical-Bayes shrinkage toward a parent state (drop the distance
   dimension first, then the zone) so starved cells inherit their parent's
   distribution. Concentration parameter chosen by held-out log-likelihood
   (one season left out).
2. **Eras.** Scoring environment and rules drift 2004→2025 (kickoff rules,
   clock rules, pace). v1 estimates three era matrices: 2004–2013, 2014–2020,
   2021–present, and validates stability by comparing EP curves across eras;
   if adjacent eras agree within bootstrap CIs they may be pooled. EPA is
   always computed against the play's own era curve.
3. **Garbage time** excluded using the same `is_garbage_time` definition as
   `marts.play_epa` — one definition in the codebase, not two.
4. **Team-level (later phase):** team matrices as shrunk deviations from the
   era-league matrix (Dirichlet prior centered on league rows); never
   estimated free-standing.

## 6. The EP ladder

- **v1 — drive EP (Goldner).** `EP_drive(s) = A(s,·) · v` where `A` is the
  absorption-probability matrix and `v` = {TD: 6.97 (TD + league XP/2pt
  expectation), FG: 3, SAFETY: −2, TURNOVER_TD: −6.97, else 0}. Simple,
  interpretable, sufficient for the sunburst and sustain/finish.
- **v1.5 — net next-score EP.** Non-scoring absorptions are worth
  `−EP_opponent(handoff state)`: punts through a net-punt-distance model,
  turnovers through observed return-spot distributions, scores through
  kickoff start position (era-dependent: touchback rules changed). Solve by
  fixed-point iteration over field position (converges fast; both Goldner and
  nflfastR-style EP land here). **This is the basis comparable to CFBD PPA**
  and the one house EPA uses.
- **v2 — Chan/Fernandes/Puterman MRP.** Full-game Markov reward process with
  asymmetric teams and play-level "points gained" from the value function.
  Recorded as the aspiration; not in scope until v1.5 validates.

## 7. Special-teams submodels (needed by v1.5)

- **FG:** logistic make-probability on attempt distance (era-pooled unless
  the curve drifts); the empirical curve above is the sanity anchor.
- **Punt:** net field-position distribution given punt spot (includes
  touchback compression near the goal line).
- **Kickoff:** empirical start-position distribution per era.

Byproducts shipped for free: FG-attempt EPA, punter field-position value.

## 8. Deliverables & schema

| Artifact | Shape |
|---|---|
| `scripts/compute_drive_chain.py` | `compute_house_elo.py` conventions; `--era`, `--validate`, idempotent rebuild |
| `analytics.drive_chain_transitions` | (era, state, next_state, n, p_raw, p_shrunk) — league grain first |
| `analytics.ep_states` | (era, state, ep_drive, ep_net, p_td, p_fg, p_punt, p_turnover, se_boot) |
| `marts.play_epa.house_epa` | parallel column: `EP(next state) − EP(state)` (+ scoring adjustments), NULL where CFBD ppa is also NULL |
| `marts.drive_sequences` | Tier 2 sunburst rollup (start zone → play-1 → play-2 → result) + `api.` view |

Bootstrap SEs (cluster-resample by game) stored alongside every EP value —
the Brill/Yurko/Wyner point: publish intervals, not verdicts.

## 9. Validation gates (all must pass before anything consumes house EPA)

1. **Shape sanity:** EP monotone in yards_to_goal within each down; 1st ≥ 2nd
   ≥ 3rd ≥ 4th at fixed (distance, zone) for standard distances.
2. **Benchmark vs CFBD:** play-level corr(house_epa, ppa) on rush/pass plays
   — expect ≈0.9; a low value means a definition mismatch to explain, not a
   tweak to hide. Optional single call to `/ppa/predicted` for the
   state-level comparison.
3. **Calibration:** predicted vs realized drive-outcome rates by state decile
   (reliability curves for P(TD), P(punt), P(turnover)).
4. **Era stability:** EP curves per era with bootstrap bands; pooling
   decisions documented from this, not assumed.
5. **Plausibility review** by modeling-scientist conventions: team
   sustain/finish decomposition must rank known offenses sensibly before the
   mart ships.

## 10. Phasing

- **P1:** league chain per era + EP v1 + gates 1/3/4 → `ep_states`,
  `drive_chain_transitions`.
- **P2:** special-teams submodels + v1.5 net EP + gate 2 → `house_epa`
  column.
- **P3:** team-level shrunk deviations → sustain/finish mart + Tier 2
  sunburst mart + api views.
- **P4 (separate plan):** 4th-down decision model on top of v1.5, with
  bootstrap decision intervals per Brill et al.; coach aggressiveness above
  expectation.

Feature adoption into `features.team_week` is out of scope for all phases —
any candidate goes through the section 2.5 screen on its own merits later.

## 10.5 First real-chain run (2026-08-08, era 2021+, offline via MCP counts)

Corrected engine after PR #66 review (grandparent-target emission, no
play_epa join, drive-exact unmapped guard, realized-outcome calibration):
872,678 scrimmage plays, 158,649 mapped drives (0.90% unmapped), 6,687
unique transitions, every shrunk row summing to exactly 1. Realized-
outcome calibration MAE for P(TD) from drive-start states: **0.0076**.
Gate 1a
(zone monotonicity) PASSED: 1st-and-10 drive EP declines smoothly 4.75
(z2) -> 1.17 (z10); 1st-and-goal inside the 10 = 5.65 with P(TD) 0.758;
own-25 = 1.80 -- all consistent with published CFB EP curves. Gate 1b
surfaced a structural property, not an error: d4 transient states are
GO-FOR-IT-CONDITIONAL (punts/FGs exit the chain from the 3rd-down play), so
d4 can price above d3 (observed: d4|med|z7 1.40 vs d3|med|z7 1.37). The
gate now checks downs 2-3 only, with the exemption documented in code; P4's
4th-down model uses the conditionality directly (EP(go | state) IS the
d4-state value).

## 10.6 P2 run (2026-08-08, deploy 31261093115)

v1.5 delivered with ONE estimator instead of three submodels: handoff
distributions are the opponent's observed same-half next-drive start
positions per (outcome, exit zone), per era -- net punt distance, returns,
and spot-of-kick rules baked into what actually happened next. The
recursion closes over the ten first-and-10 handoff states (a 10x10 linear
system, exact). All eras: monotone_net pass, net_sane pass. Era 2021+:
own-25 net EP **+0.90** (published CFB ~0.3-1.2); backed up inside the own
10 goes **negative** (-0.18), the signature the drive basis cannot show.

**Gate 2 verdict (2024 season, 174,117 plays):** corr(house_epa, CFBD ppa)
= **0.8565**, means 0.292 vs 0.303. Against the target "~0.9", the
definitional mismatch is quantified, not waved at: the maximum ANY model on
this 162-state grid can achieve is the cell-mean oracle's **r = 0.9340**
(within-cell ppa variance is invisible to a discrete grid), so house EPA
captures 92% of attainable correlation. The 0.93->1.0 gap is pure
discretization; 0.86->0.93 is within-cell model difference (CFBD uses
clock and continuous yardline). Recorded as PASSED-WITH-EXPLANATION:
house EPA is fit for mart use labeled as grid-resolution EP, and a finer
grid is the identified lever if the residual ever matters.

## 11. Open questions

1. Half-open drives: `END_OF_HALF` truncation censors long drives — v1
   treats it as absorbing with value 0 (net basis handles it better); check
   sensitivity by excluding final-2:00 drives.
2. 2-pt attempts: fold into the TD value constant (league rate) or model
   separately? v1 folds.
3. FCS opponents: include (they're in the data, ~both sides of 350-team
   schedules) but validate the EP curve on FBS-vs-FBS only.
4. Where does the era boundary for the 2024 clock-rule change land? Check
   drive-length distributions before freezing era cuts.
