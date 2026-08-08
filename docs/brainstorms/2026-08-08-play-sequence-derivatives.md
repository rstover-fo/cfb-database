# Play-Sequence Derivatives: What the PBP/Drives Data Still Owes Us

**Date:** 2026-08-08
**Status:** Brainstorm — nothing here is committed work; model-feature candidates
must pass the section 2.5 screen before entering `features.team_week`.
**Inspirations:**
- Ötting, *Predicting play calls in the NFL using hidden Markov models*
  (arXiv:2003.10791) — 2-state HMM over per-drive play sequences, covariates
  down/distance/shotgun/score-diff, 71.5% out-of-sample play-call accuracy.
- Mik Panko's NFL plays sunburst (Observable) — hierarchical
  drive-state → play → outcome visualization.

## The gap, in one sentence

Every play-derived mart we have is **marginal** — a situation bucket mapped to a
rate (`team_playcalling_tendencies`, `situational_splits`, `team_tempo_metrics`,
`team_style_profile`, `defensive_havoc`, `scoring_opportunities`) — while both
inspirations exploit **conditional and sequential structure**: what a team does
*given what just happened*, and how drives move through a state space. We have
2.7M plays (2004+) with EPA, ordering (`play_number`, `drive_id`), score state,
timeouts, and (2018+) wallclock timestamps, and we have never once conditioned
on the previous play.

## Spot-checks that motivated this (run 2026-08-08, season 2024, non-garbage)

1. **Sequence dependence is large.** On 2nd down, P(pass) after a successful
   rush is **0.355**; after a failed pass it is **0.527** — a 17-point swing
   invisible to down/distance conditioning. On 1st down, previous-play identity
   alone moves P(pass) by ~6 points (0.415 after rush vs 0.475–0.486 after
   pass).
2. **Real tempo is measurable.** `core.plays.wallclock` is ≥94.8% populated
   for every season 2018+ (99%+ most years, 0% in 2014) — actual elapsed
   seconds between snaps, strictly better than game-clock arithmetic for
   tempo, hurry-up detection, and end-of-half urgency.
3. **Unexploited columns already sitting in `core.plays`:**
   `offense_timeouts`/`defense_timeouts` (clock-management analysis),
   `play_number` × `drive_id` (sequencing), `wallclock` (real tempo),
   `play_text` (sack/scramble/screen/PA extraction via regex — never parsed).

## Tier 1 — Drive-state Markov chain (the biggest single unlock)

Model the drive as an absorbing Markov chain. States = (down × distance bucket
× field zone); absorbing states = TD, FG, punt, turnover, downs, end-of-half.
Estimate the league transition matrix from all ~2.7M plays; per-team-season
matrices as deviations (shrunk toward league).

What falls out of one artifact:

- **House expected-points curve.** EP(state) from the chain's absorption
  values. Today every EPA number we publish inherits CFBD's PPA model;
  a house EP model makes `marts.play_epa` self-sufficient and lets us define
  EPA on our own terms (and sanity-check CFBD's).
- **Drive outcome probabilities from any state** — P(TD | 2nd-and-7 at own 40)
  — which is exactly the data behind the sunburst (Tier 2).
- **Sustain vs finish decomposition.** Separate P(earn a new set of downs)
  from P(points | scoring opportunity). `scoring_opportunities` measures
  finishing; nothing measures sustaining as a chain property, and the two
  skills price differently in matchups.
- **Defense mirror.** The same chain conditioned on defense = a structural
  defensive profile (where drives die against you), richer than havoc rates.

Shape: `analytics.drive_chain_transitions` (state, next_state, n, p, season,
team nullable for league) + a compute script following the
`compute_house_elo.py` pattern. Walk-forward/as-of discipline applies if any
of it feeds features.

## Tier 2 — Drive sunburst mart (the visualization)

`marts.drive_sequences`: hierarchical aggregation
(start field zone → first-play category → second-play category → drive result)
with counts and points, per season and team, garbage-time filtered. Exposed via
an `api.` view so cfb-app can render the Observable-style sunburst directly —
the ring hierarchy is precisely a GROUP BY ROLLUP. Cheap (one aggregation over
`play_epa` × `core.drives`), zero API calls, high dashboard value.

## Tier 3 — Predictability & sequence identity (the HMM paper direction)

1. **Play-call predictability index.** Per team-season, fit a play-call model
   (logistic with situation + previous-play covariates is enough to start; the
   paper's 2-state HMM is the upgrade) and score *held-out cross-entropy*.
   Low entropy = predictable offense. Candidate model feature: does offensive
   predictability cost EPA/margin? Also a scouting product on its own
   ("Team X goes run 78% after a successful run on 1st down").
2. **Sequence entropy vs situation entropy.** We already measure situation mix;
   add first-order transition entropy P(next category | current category,
   down). Two teams with identical run/pass splits can be maximally different
   in sequencing.
3. **Abandon rate / discipline.** P(stay with run | consecutive failed runs) —
   how fast a coach abandons the plan. The 2024 spot-check shows the
   league-wide effect; the per-coach deviation is the interesting object, and
   it travels with the coach (we have `coaches` + tenure marts to join).
4. **Opening-script detection.** The HMM's latent-regime idea, minimally: do
   the first ~12 offensive plays follow a different (more scripted, less
   score-sensitive) distribution than the rest? Regime-switch frequency as a
   coach fingerprint.

## Tier 4 — Decision quality (needs Tier 1's EP curve)

1. **4th-down decision model.** With EP(state) in hand, compute
   go/punt/kick break-evens per state; score every actual 4th-down decision
   2004+ → coach "aggressiveness above expectation" and EP lost to
   conservatism. All inputs exist; the only prerequisite is the house EP curve.
2. **Clock management.** `offense_timeouts`/`defense_timeouts` + wallclock:
   end-of-half EP with 0/1/2/3 timeouts remaining, timeout-burn timing vs
   points on the ensuing two-minute drive.
3. **Real tempo (2018+).** Seconds-between-snaps from wallclock: true
   hurry-up rate, situational tempo (leading vs trailing), tempo-as-weapon
   vs tempo-as-identity. Would supersede the play-count proxies in
   `team_tempo_metrics` for the seasons where wallclock exists.

## Tier 5 — Player sequence structure (via `stats.play_stats`, 2.5M rows)

- **Usage trees:** who touches the ball by (down, leverage, sequence position) —
  a player-level sunburst; feeds cfb-scout.
- **Dependence index:** HHI concentration of team EPA across players; a
  candidate injury-sensitivity modifier when joined against availability
  reports (`raw.availability_reports`) and rosters.
- **`play_text` mining:** sack/scramble/screen/play-action extraction by regex
  — none of it currently parsed out of the free text.

## Ordering and guardrails

Build order that respects dependencies: Tier 1 → (2, 4) with 3 and 5
independent. Tier 2 is the cheapest shippable win; Tier 1 is the highest
leverage. Every candidate *feature* goes through
`screen_preseason_features.py`-style screening and the modeling-scientist
review gates before touching `features.team_week` — predictability and
discipline metrics in particular are exactly the kind of plausible-sounding
feature the pre-registration discipline exists to keep honest. Everything here
is derivable from data already in the warehouse: **zero CFBD API calls.**

---

# Addendum (same day): related work + the scheme/formation question

## Formation data: we have more than we thought

`play_text` was assumed to be bare "Player rush for X yds" text. Empirically
(2026-08-08):

| season | shotgun-tagged plays | no-huddle-tagged |
|---|---|---|
| 2014–2021 | 0.0% | 0.0% |
| 2022–2024 | ~0.5% | ~0.5% |
| **2025** | **32.6%** | **23.4%** |

The 2025 text is official NCAA live-stats format —
`(10:57) No Huddle-Shotgun Player rush middle for 0 yards ...` — and the
coverage is a **per-game split**: 720 of 1,653 games are fully tagged
(shotgun rate inside them: 72.3%, consistent with modern CFB), the rest have
zero tags. So:

- **Shotgun and no-huddle — the exact covariates in the Ötting HMM — are
  extractable for ~44% of 2025 games and presumably a growing share of 2026.**
- A cheap regex extraction (`is_shotgun`, `is_no_huddle`, `is_sack`, penalty
  structure) belongs in the play_epa pipeline regardless of anything else.
- **Selection-bias guard before any feature use:** which games carry NCAA text
  is not random (sampled tagged games skew non-P5). Any formation-derived
  feature needs a `game_is_tagged` indicator and a bias check on the tagged
  subsample before it goes near `features.team_week`.

## What is genuinely NOT derivable, and the honest substitutes

Alignment, motion, personnel packages (11/12/21), route concepts, and coverage
shells do not exist in any CFBD field. Options, in increasing cost:

1. **PROE / xpass** (industry standard): fit P(pass | down, distance, field
   position, score, clock) league-wide; a team's pass rate *over* expected is
   a scheme-intent fingerprint that needs no formation data at all. Fully
   derivable today, all seasons. The closest thing to "scheme in one number."
2. **Skill-personnel proxies from `stats.play_stats`:** co-occurrence of
   ball-touchers per play/drive approximates personnel rotation breadth
   (O-linemen never appear, so it is a proxy, not personnel).
3. **External charting** (PFF/SIS): real formation/coverage labels, paywalled,
   would enter via the flat-file lane if ever purchased.
4. **CV on broadcast film:** an active research area, out of scope for this
   warehouse.

## Reading list (verified links)

| Paper | Why it matters here |
|---|---|
| Goldner (2012), *A Markov Model of Football*, JQAS | The Tier 1 blueprint: absorbing chain over (down, distance, yardline), EP = absorption probs × state values. |
| Chan, Fernandes & Puterman (2021), *Points Gained in Football*, Operations Research | The Tier 1 upgrade path: Bellman/Markov-reward formulation with asymmetric teams; play-level value ("points gained") from the value function. |
| Brill, Yurko & Wyner (2025), *Analytics, have some humility*, The American Statistician (arXiv:2311.03490) | Mandatory Tier 4 guardrail: bootstrap uncertainty on EP/WP; many 4th-down "decisions" are statistical coin-flips. Report intervals, not verdicts. |
| Romer (2006), *Do Firms Maximize?* / NBER w9024 dynamic-programming 4th-down analysis | The classic decision baseline. |
| Ötting (2020), arXiv:2003.10791 | Tier 3 blueprint (HMM play-call regimes). |
| arXiv:2103.06939, *RL Based Approach to Play Calling* | Play-calling as sequential decision problem — Tier 3/4 bridge. |
| arXiv:2309.00756, *Learning Risk Preferences in MDPs* (4th down) | Inverse-RL take: infer coach risk preferences from observed decisions — richer than "aggressiveness above expectation." |
| arXiv:2102.01846, *NFLSimulatoR* | Simulation harness pattern for strategy evaluation. |
| Yurko, Ventura & Horowitz (2018), *nflWAR* (arXiv:1802.00998) | Tier 5 upgrade: multilevel player value from PBP alone — the road from player EPA to a WAR-style number. |
| PROE / xpass (industry, nflfastR ecosystem) | Scheme-intent without formation data; Tier 3 item 1 should produce this as its first output. |

## Other gaps surfaced while looking (not sequence-related, all derivable)

- **Rest & travel:** days of rest / bye weeks from the schedule; travel
  distance and altitude from `ref.venues` lat/lon. Classic HFA decomposition
  features; never built, zero API calls.
- **Special teams in the chain:** the Tier 1 EP model needs punt and FG
  submodels anyway (house FG make-probability curve from `core.plays` FG
  attempts); that yields FG-attempt EPA and punter field-position value as
  byproducts.
- **Weather:** `core.game_weather` is loaded and unscreened as a feature
  source (wind × pass-rate interactions are the plausible candidate).
- **QB continuity:** `stats.play_stats` identifies who took snaps; a
  week-over-week starting-QB-change indicator is derivable and is the single
  most prediction-relevant availability signal.
