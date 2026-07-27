# Handoff: model + outlook changes for cfb-app

**From:** cfb-database
**Date:** 2026-07-27
**PR:** rstover-fo/cfb-database#56
**Status:** deployed to prod and verified; contract changes are live

Your eight integration findings are all addressed. Two of them changed our
numbers rather than just our docs, and one thing you should stop displaying is
listed at the bottom.

---

## 1. Your four `api.season_outlook` defects are fixed

**`classification` (new column).** The view mixed FBS/FCS/DII/DIII — 350 rows for
2026 — so an unfiltered `ORDER BY projected_wins` compared teams playing entirely
different schedules. Now derived season-accurately from
`core.games.home_classification` / `away_classification`, so a team that changed
division keeps the right label per season.

Verified post-deploy for 2026: **138 fbs / 128 fcs / 38 ii / 33 iii / 13 NULL**.

**Filter on it before ranking.** NULL means *unplaceable*, not FBS — your
`classification = 'fbs'` filter correctly drops those 13.

This replaces the conference-name allowlist you were using as a workaround.

**`is_projection` (new column).** Every 2025 row had `projected_wins =
actual_wins`, `wins_p10 = wins_p90`, and `conf_title_prob` values like an exact
`0.2500` shared by four SEC teams — a tie split evenly among teams that finished
level, not a title race. A consumer defaulting to the current season was being
handed hindsight under projection column names.

Defined per row as `games_simulated > games_completed`. **Check it before calling
anything a forecast.** For a season-level answer use `bool_or(is_projection)`.

**`schedule_complete` is division-aware.** The flat 11-game rule flagged all 8 Ivy
League teams as incomplete when their 10-game seasons were fully described — so
your "these are floors" caveat was firing on a conference that needed no caveat.
The threshold is now the modal `games_scheduled` among the team's conference
peers, clamped per division.

Side effect worth knowing: an FBS team at 11 of 12 whose conference modally plays
12 now correctly reads *incomplete*.

**`p_bowl_eligible` is NULL outside FBS.** The 6-win threshold was applied to
every division, so Yale carried `0.888` for a postseason the Ivy League does not
have. `p_ten_plus` is untouched — 10 wins means the same thing everywhere.

Verified: `p_bowl_eligible` non-NULL on 138/138 FBS and 0/212 everything else.

---

## 2. Stop hardcoding the accuracy numbers — there is a view now

You hardcoded win MAE and the interval as a TypeScript constant. That is correct
right up until we re-run the backtest, at which point your shipped numbers
describe a model that no longer exists and **nothing anywhere fails**.

**`api.model_backtest`** (migration 045) persists every backtest run.

```sql
SELECT win_mae, resid_p10, resid_p90, baseline_prior_mae, n, run_date
FROM model_backtest
WHERE model_version = 'fitted_v1' AND scope = 'fbs'
ORDER BY run_date DESC LIMIT 1;
```

Four things to get right:

- **`n` counts TEAM-SEASONS, not games.** Current run is n=921.
- **Use `resid_p10`/`resid_p90` as the interval, never `± win_mae`.** MAE is an
  average loss, not a half-width; a ±MAE band spans ~58% of a normal error, not
  the ~80% a reader assumes. The band is asymmetric — read both ends.
- **Filter `scope = 'fbs'`.** `all_divisions` is a different measurement, not a
  superset.
- **No row means never backtested.** Render as unmeasured — not as zero error.

`run_date` exists precisely so a cached copy can be checked for staleness.

---

## 3. Numbers that changed — update anything you cached

| | old | **new** |
|---|---|---|
| win MAE | 1.743 | **1.738** |
| RMSE | 2.168 | **2.167** |
| bias | −0.126 | **−0.122** |
| coverage | 0.800 | **0.807** |
| 80% interval | [−2.68, +3.02] | **[−2.65, +3.02]** |

**Do not present this as an accuracy improvement.** Cumulative movement is 0.005
wins against a standard error of ~0.04, and RMSE went *up* while MAE went down.
It is noise. We changed the numbers because the model changed, not because it got
better. Prefer reading them live from `api.model_backtest` over copying this
table.

---

## 4. The coaching feature changed meaning — this one matters for prose

We replaced `hc_first_year` with **`hc_first_year_unproven`** in the vector
(migration 046). The distinction is not cosmetic and the bot will get it wrong if
it repeats the old framing.

**The first-year penalty is not a penalty for changing coaches.** Split by the
incoming coach's career SP+ at previous stops:

| subgroup | positives | partial | reading |
|---|---|---|---|
| unproven hire | 184 | **−0.1844** | the entire effect |
| proven hire | 80 | **+0.0096** (p=0.73) | essentially nothing |
| flat binary (old) | 266 | −0.1548 | the two averaged together |

At n=1,322 the standard error is ~0.028, so the proven-hire result is a
**powered null**, not an underpowered shrug — the interval excludes anything near
the unproven effect. It holds in both halves of a split-window re-run and is
stronger in the portal era.

So: **"new coach, therefore worse" is not what the model believes.** A program
hiring someone with a track record is projected roughly as though nothing
happened. If the bot narrates coaching changes, that is the sentence to get right.

**Still 100% NULL for 2026** — CFBD publishes no 2026 coaching records yet, so it
contributes nothing until roughly August. Teams with new head coaches are
currently projected as though nothing changed, and that caveat still needs
relaying.

---

## 5. Two new model features, and why the vector is now 22

`draft_picks_3yr` (+0.0834) and `draft_departures` (−0.0925), migration 047.
Opposite signs by construction: picks *produced* over S−3..S−1 measure a program
that develops talent, picks *lost* in year S measure the best players leaving.

Unlike the coaching term these **are populated for 2026**, so they do move the
current outlook.

Worth knowing why they are new: both had been *rejected*, and those rejections
were void. `draft.draft_picks` held 2020–2026 while the loader was configured for
2000–2026, and `COALESCE(..., 0)` read an uningested draft as "produced zero
picks" on 54% of the evaluation frame. Nothing errored. After backfilling
2000–2019 both reversed. Every non-draft candidate reproduced to four decimals,
which is how we know the backfill moved these and nothing else.

No action for you — this is context for why the projections shifted.

---

## 6. Correcting one thing in your findings

Your note said `fitted_v1` has **23,453 rows in both** `api.game_predictions` and
`api.scored_matchup_edges`. Only the first is right:

| surface | fitted_v1 rows | seasons |
|---|---|---|
| `api.game_predictions` | 23,453 | 2018–2026 |
| `api.scored_matchup_edges` | **1,638** | **2026 only** |

`scored_matchup_edges` is upcoming games with a market line, so it is
1,638 per model version and empty out of season **by design** — that is not a
failure state and should not render as an error. All three model versions carry
the same 1,638.

Everything else in your findings checked out, including the win-probability
divergence (`elo_v1` 0.7000 / `elo_epa_blend_v1` 0.7000 / `fitted_v1` 0.8772 on
the same game) — `fitted_v1` calibrates its own Platt-scaled probability while
the Elo pair share one.

---

## 7. Open, so you are not surprised later

- **DII reads 0/38 `schedule_complete` and DIII 3/33.** Plausible — 2026 non-FBS
  schedules are barely published in July — but it is also what a modal threshold
  failing to find conference peers would produce. We have not verified which.
  If you surface those divisions, treat the flag as unconfirmed there.
- **`conf_title_prob` is still naive v1** — highest conference win percentage per
  simulation, ties split evenly, **no tiebreakers and no championship game**. It
  remains the weakest number on the row. Prefer projected wins.
- **`playoff_prob` is still NULL by design.** Do not fill it in.

---

## Framing that survives all of this

The original refusal — "I won't invent a standings table" — was right in spirit
and should not come back as false confidence now that numbers exist. The honest
version is still:

> These are model output, not predictions I'd bet the house on. Preseason
> projections have missed by about 1.7 wins on average since 2019, and the 80%
> range on any single team is roughly −2.7/+3.0 wins. Conference title odds
> ignore tiebreakers and the title game.

A standings table with no error band is the same overconfidence the original
refusal was avoiding, just better dressed.

---

## Provenance

- PR: rstover-fo/cfb-database#56
- Migrations: `045_model_backtest.sql`, `046_team_week_hc_unproven.sql`,
  `047_team_week_draft_columns.sql`
- Views: `api/041_season_outlook.sql`, `api/042_model_backtest.sql`
- Model: `fitted_v1`, 22-feature vector, nine walk-forward vintages
  (`train_through_season` 2017–2025)
- Contract: `docs/SCHEMA_CONTRACT.md` changelog, 2026-07-26/27 entries
