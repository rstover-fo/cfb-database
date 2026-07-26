# Handoff: season outlook for the Discord bot (cfb-app)

**From:** cfb-database
**Date:** 2026-07-26
**Status:** `api.season_outlook` deployed; 2026 populated (see
SCHEMA_CONTRACT.md changelog 2026-07-26)

## What changed

A Discord user asked for projected final SEC standings with win/loss records.
The bot correctly refused, saying its engine "scores one scheduled game at a
time" and that inventing a standings table would be inventing numbers. That
refusal was right when it was written. **It is now wrong on two counts, and
the bot will keep declining a question the warehouse can answer.**

**1. It names the wrong model.** The bot cites `elo_epa_blend_v1` as "the
prediction engine". Measured on 2025 (3,829 games), `fitted_v1` is the better
model — margin MAE **14.69** vs `elo_epa_blend_v1`'s **15.69** — and it is
what scores upcoming games now. `elo_v1` was 15.88.

**2. Season-long projection now exists.** `predictions.season_projections`
runs 10,000 Monte Carlo simulations per team and `api.season_outlook` exposes
the latest snapshot per `(season, team, model_version)`.

| Need | Column |
|---|---|
| Projected record | `projected_wins`, `projected_losses` |
| Uncertainty band | `wins_p10`, `wins_p25`, `median_wins`, `wins_p75`, `wins_p90` |
| Full distribution | `p_win_dist` — `{"0": p, "1": p, …}`, sums to 1 |
| Milestones | `p_bowl_eligible`, `p_ten_plus` |
| Schedule strength | `sos_rating`, `sos_rank` |
| Conference race | `conf_title_prob` (**see caveats — weakest column here**) |
| Playoff | `playoff_prob` — **always NULL by design** |

## The query behind the question that started this

```sql
SELECT team,
       projected_wins,
       projected_losses,
       wins_p10,
       wins_p90,
       p_bowl_eligible,
       p_ten_plus,
       sos_rank,
       games_simulated,
       games_unscored
FROM season_outlook
WHERE season = 2026
  AND conference = 'SEC'
  AND model_version = 'fitted_v1'
ORDER BY projected_wins DESC;
```

## What cfb-app should do

1. **Correct the bot's self-description.** It should name `fitted_v1`, not
   `elo_epa_blend_v1`, and it should no longer say season projections are out
   of scope.
2. **Add `api.season_outlook` to the available-views list** for
   `run_analyst_query` / `run_sql`, so the model knows the surface exists.
3. **Consider a curated `get_season_outlook(team, season)` MCP tool.** The
   standings shape is one query, but the caveats below are easy to drop when
   a model writes raw SQL, and a tool can attach them structurally.

## Honest-data caveats the bot must relay

The bot's instinct — refuse rather than invent — was correct and should
survive this change. It now has real numbers, but a clean standings table
hides uncertainty that is large relative to the differences between teams.

- **Preseason accuracy is ~1.74 wins MAE.** Backtested on 2019–2025 week-1
  vectors with frozen prior-season fits (n=921): win MAE 1.743, RMSE 2.168,
  bias −0.126. Both baselines are worse (prior-season record 2.128, flat .500
  2.140), so the model beats the naive answers — but it is not precise.
- **Use the empirical interval, not ±MAE.** The residual quantiles are
  asymmetric: an 80% interval is **`[projected − 2.68, projected + 3.02]`**.
  Quoting ±1.74 spans only ~58% of outcomes and reads as far more confident
  than the model is.
- **`conf_title_prob` is naive v1** — the fraction of simulations in which a
  team has the highest *conference win percentage*, ties split evenly. It
  models **no real tiebreakers and no championship game**. For a conference-
  standings question this is the weakest number on the row; prefer projected
  wins and say the title odds are approximate.
- **`playoff_prob` is NULL by design.** The 12-team format's autobids and
  seeding are their own rules-modeling project. Never fill it in.
- **Read `games_unscored` before quoting `projected_losses`.** A scheduled
  game with no prediction is *excluded* from the simulation, not counted as a
  loss. Every projected quantity is over `games_simulated`, never
  `games_scheduled`.
- **Check `schedule_complete`.** 2026 schedules are still filling in — 68 of
  337 teams had fewer than 8 games listed as of late July. Projections are
  over *listed* games and never extrapolate to a hypothetical 12-game slate.
- **Coaching-change signal is absent for 2026 right now.** `hc_first_year`
  is one of the model's strongest preseason features (partial −0.1548), but
  CFBD publishes no 2026 coaching records yet, so it is 100% NULL for 2026
  and contributes nothing until roughly August. Teams with new head coaches
  are currently projected as though nothing changed.
- **FCS/D2:** CFBD labels non-FBS playoff bracket games `season_type =
  'regular'`, so `games_scheduled` for an FCS team can include a playoff run
  (completed seasons only). Do not rank FCS against FBS on `projected_wins`.

## Framing that keeps the honesty

The old answer's spirit was "I won't make up a table." The new answer should
be "here is the table, here is how wrong it usually is." Something like:

> Projected 2026 SEC finish from `fitted_v1` (10,000 simulations). These are
> model output, not predictions I'd bet the house on — preseason projections
> have missed by about 1.7 wins on average since 2019, and the 80% range on
> any single team is roughly −2.7/+3.0 wins. Conference title odds ignore
> tiebreakers and the title game.

A standings table with no error band is the same overconfidence the original
refusal was avoiding, just better dressed.

## Provenance

- View: `src/schemas/api/041_season_outlook.sql`
- Writer: `scripts/simulate_season.py` (correlated draws, `strength_share`
  0.15 calibrated against backtest coverage)
- Backtest: `scripts/backtest_preseason.py`
- Model: `fitted_v1`, 20-feature vector as of migration 042
