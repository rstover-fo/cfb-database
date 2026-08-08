# CORE ratings for cfb-app

**From:** cfb-database
**Date:** 2026-08-08
**Status:** Deployed (stage 1: dedicated view; stage 2: team-view embed)

## What changed

CFBD published a new team rating — CORE (Context & Opponent-Relative
Efficiency): PPA-based performance adjusted for game situation and opponent
strength, retrospective from 2016. It is now ingested daily
(`ratings.core_ratings`, internal) and surfaced two ways:

| Surface | Grain | What it carries |
|---|---|---|
| `api.core_ratings` (new) | (team, season) | Full column set: `overall`, `offense`, `defense`, `offense_plays`, `defense_plays`, `through_week`, `through_season_type`, `model_version`, plus in-season `overall_rank` / `offense_rank` / `defense_rank` |
| `api.team_detail` / `api.team_history` (additive) | unchanged | Three new columns next to sp/elo/fpi: `core_overall`, `core_offense`, `core_defense` |

## What cfb-app should do

- Team pages can show CORE alongside SP+/FPI immediately — the three embedded
  columns are already in the views the pages read. No query changes required;
  the additions are purely additive.
- For a ratings table/leaderboard, query `api.core_ratings` directly, e.g.
  PostgREST: `/api/core_ratings?season=eq.2025&order=overall_rank.asc&limit=25`.
- The bot reaches it via `run_analyst_query` automatically (api-schema default
  privileges cover `analyst_ro`).

## Honest-data caveats

1. **Coverage is 2016+ only.** NULL `core_*` on team views (or absent rows in
   `api.core_ratings`) for earlier seasons means not-rated — never zero.
2. **`defense` is lower-better.** "Best defense" is `defense_rank ASC`
   (already ranked ascending) or `ORDER BY defense ASC` — never
   `ORDER BY defense DESC`. `overall = offense - defense`.
3. **In-season rows are as-of snapshots.** `through_week` /
   `through_season_type` mark how much of the season the rating has seen; the
   daily load advances the row in place. Treat mid-season values as current
   form, not final ratings.
4. `offense`/`defense` are per-100-qualifying-plays point margins vs average;
   `offense_plays`/`defense_plays` are the qualifying volumes behind them
   (garbage time, OT, FCS games, special teams excluded by CFBD's model).

## Example

Top 10 of 2025 with components:

```sql
SELECT team, conference, overall, offense, defense,
       overall_rank, offense_rank, defense_rank
FROM api.core_ratings
WHERE season = 2025
ORDER BY overall_rank
LIMIT 10;
```
