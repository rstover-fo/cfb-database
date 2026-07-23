# Handoff: penalty views for the Discord bot (cfb-app)

**From:** cfb-database
**Date:** 2026-07-23
**Status:** views live once the penalty-layer deploy lands (see
SCHEMA_CONTRACT.md changelog 2026-07-23)

## What changed

Discord users asked the bot for holding-call counts and it correctly
reported the warehouse exposed no penalty data. That gap is closed — two new
`api` views (both already reachable through `run_analyst_query`, no cfb-app
code change required for raw SQL access):

| View | Grain | Use for |
|------|-------|---------|
| `api.team_penalties` | (game_id, team) | Official box-score counts: `penalties`, `penalty_yards`, `opponent_penalties`, `opponent_penalty_yards`, plus season/week/opponent/home_away. Aggregate for per-game averages. |
| `api.penalty_log` | one row per play carrying penalty text (2004+) | Infraction-level detail: `infraction` ('Holding', 'Pass Interference', …), `penalized_team`, `benefiting_team`, `penalty_yards`, `declined`, `offsetting`, `no_play`, `is_penalty_play_type`, `parse_ok`, raw `play_text`. |

## What cfb-app should do

1. Mention both views in the bot's tool/prompt surface (wherever the
   available-views list for `run_sql` lives) so the model knows penalty
   questions are now in-scope.
2. Consider a curated MCP tool for the common shape ("penalties by team X
   in season Y, split by infraction, vs opponents") if raw SQL proves
   clumsy — the views are designed so one query answers it.

## Honest-data caveats the bot should relay

- `api.penalty_log` is parsed from CFBD **free-text** `play_text` spanning
  four provider formats. `infraction = 'Unknown'` or
  `penalized_team IS NULL` means *unclassified, not absent* — infraction
  coverage is validated ≥90% for seasons ≥2022, team attribution ≥50%
  (floors enforced in prod by `validation_penalties.sql`; actual numbers in
  the deploy log). Treat filtered counts as floors and say so.
- `api.team_penalties` is the scorer's official tally (`totalPenaltiesYards`)
  — prefer it for totals; use the log only when infraction/type matters.
- Example, the question that started this ("holding calls against OU
  opponents vs their season average"):

```sql
WITH holding_vs_ou AS (
    SELECT penalized_team, COUNT(*) AS holding_calls
    FROM penalty_log
    WHERE season = 2025 AND infraction = 'Holding'
      AND (offense = 'Oklahoma' OR defense = 'Oklahoma')
      AND penalized_team IS NOT NULL AND penalized_team <> 'Oklahoma'
    GROUP BY penalized_team
),
season_holding AS (
    SELECT penalized_team AS team, COUNT(*)::numeric AS holdings
    FROM penalty_log
    WHERE season = 2025 AND infraction = 'Holding' AND penalized_team IS NOT NULL
    GROUP BY penalized_team
),
-- Denominator = ALL games the team played (from the box view), not just
-- games where a holding call happened -- a filtered denominator would
-- inflate the average for clean-game teams.
team_games AS (
    SELECT team, COUNT(DISTINCT game_id) AS games
    FROM team_penalties
    WHERE season = 2025
    GROUP BY team
)
SELECT h.penalized_team,
       h.holding_calls AS holding_vs_ou,
       ROUND(COALESCE(s.holdings, 0) / g.games, 2) AS their_per_game_avg
FROM holding_vs_ou h
JOIN team_games g ON g.team = h.penalized_team
LEFT JOIN season_holding s ON s.team = h.penalized_team;
```
