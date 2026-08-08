# Handoff: `api.expected_points` — house drive EP for cfb-app / cfb_mcp

**Date:** 2026-08-08
**Producer:** cfb-database (PRs #66, #69; design
`docs/plans/2026-08-08-drive-chain-ep-model-plan.md`)
**Consumers:** cfb-app (dashboard), the cfb_mcp agent (new
`get_expected_points` tool ships alongside this handoff)

## What this is

The warehouse's first in-house expected-points model: an absorbing Markov
chain over drive states (down × distance bucket × field zone) estimated from
2.5M scrimmage plays, solved per rules era. One row per (era, state) with the
drive's expected points, its outcome probabilities, and a bootstrap standard
error. Validation status at publication: zone and down monotonicity pass in
all three eras; realized-outcome P(TD) calibration MAE 0.0072–0.0077.

```
GET /rest/v1/expected_points?era=eq.2021%2B&down=eq.1&distance_bucket=eq.standard&field_zone=eq.3
→ [{"era":"2021+","state":"d1|standard|z3","ep_drive":4.24,"p_td":0.521,"se_boot":0.016,...}]
```

## The four rules (getting any of these wrong produces confident nonsense)

1. **Drive basis, not net basis.** `ep_drive` = expected points of THIS
   possession (TD 6.97, FG 3, safety −2, defensive TD −6.97, everything else
   0). It ignores the field position handed to the opponent, so it is **not
   comparable to CFBD `ppa`**, nflfastR EP, or any "next score" number. The
   comparable basis is `ep_net`, which is **NULL on every row until P2** —
   render NULL as "not yet computed", never as 0, and do not build EPA-style
   deltas from `ep_drive` across possession changes.
2. **Era-scope every lookup.** Eras are `2004-2013`, `2014-2020`, `2021+`.
   A 1st-and-10 at your own 25 is 1.58 EP in the 2000s and 1.80 today — a
   ~15-SE gap. Join a game to its own era; never average eras. For "current"
   displays default to `2021+`. Mind the plus sign: PostgREST needs
   `era=eq.2021%2B` (an unencoded `+` decodes as a space and matches nothing).
3. **`down=4` rows are go-for-it-conditional.** A 4th-down state exists in
   the chain only when the offense lined up to go — punts and FGs exit from
   the 3rd-down play. So `EP(d4)` answers "what is this state worth GIVEN
   they go", which is exactly what a 4th-down decision UI wants and exactly
   what a naive down-ladder display gets wrong (d4 can price above d3; both
   production eras show it). Keep d4 out of d1–d3 ladders or caveat it.
4. **Show intervals.** `se_boot` is the game-cluster bootstrap SE of
   `ep_drive`. Anything user-facing should render `ep_drive ± 2·se_boot` or
   equivalent — the Brill/Yurko/Wyner "intervals, not verdicts" rule is part
   of this contract's spirit, especially for thin states (check `n_obs`).

## Column reference

| column | notes |
|---|---|
| `era` | `2004-2013` \| `2014-2020` \| `2021+` (open-ended: picks up new seasons) |
| `state` | canonical key `d{down}\|{distance_bucket}\|z{zone}` |
| `down` | 1–4, parsed from state |
| `distance_bucket` | down-aware: d1 uses `standard`(=10)/`short`(<10)/`long`(>10)/`goal`; d2–d4 use `short`(≤3)/`med`(4–6)/`long`(7–10)/`xlong`(>10)/`goal` |
| `field_zone` | 1 (opponent goal line) … 10 (own goal line), 10-yard bands of yards-to-goal |
| `yards_to_goal_min/max` | the band's bounds, for display |
| `n_obs` | scrimmage-play observations in this state (garbage time excluded) |
| `ep_drive` | drive-basis expected points (rule 1) |
| `ep_net` | NULL until P2; the future CFBD-comparable basis |
| `p_td` … `p_turnover` | drive-outcome absorption probabilities from this state (`p_turnover` includes defensive-TD turnovers) |
| `se_boot` | bootstrap SE of ep_drive, game-cluster resampled |
| `computed_at` | staleness check; rewritten by each compute run |

Not exposed (contract-internal): `analytics.ep_states`,
`analytics.drive_chain_transitions`. Never read them directly — the
transitions surface will arrive with the drive-sequences (sunburst) mart.

## Freshness & change policy

Recomputed on demand via the deploy workflow (`compute_drive_chain`), not on
the daily schedule; `computed_at` is the staleness signal. Additive changes
(new columns, `ep_net` filling in, a new open era rolling over) will NOT be
announced as breaking; the row grain (era, state) and existing column
semantics are stable. When `ep_net` populates, a contract changelog entry
will say so — until then any UI copy should label these numbers "drive EP
(house model v1)".

## Suggested first uses in cfb-app

- Field-position value strip on game pages (EP by zone for the current era).
- 4th-down context chip: `EP(d4 state)` vs `EP(punt-implied next state)`
  once P2's net basis lands — until then display d4 EP with the go caveat.
- Agent answers ("what's a drive from the 25 worth?") via the new
  `get_expected_points` MCP tool, which carries these caveats in its
  docstring so the model self-qualifies.
