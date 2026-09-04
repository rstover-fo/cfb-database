# Handoff: rushing charting -- bot/agent enablement work order for cfb-app

**From:** cfb-database
**Date:** 2026-09-03
**Audience:** cfb-app (MCP tool surface, Discord bot, agent subagents)
**Status:** everything below is LIVE in production and was exercised
read-only through `run_sql` (as `analyst_ro`) on 2026-09-03. Nothing in
this doc is pending a deploy.
**Deliverable requested:** the rushing analogue of what cfb-app PR #56 did
for passing charting -- a `get_rushing_charting` curated tool, the three
views added to the `run_sql` schema card, and `get_player_detail`'s new
`rushing_charting` block surfaced in player answers.

Companion docs (semantics, not repeated here):
- `docs/handoffs/2026-09-03-rushing-charting-for-cfb-app.md` -- how to read
  the views: the denominators, `unknown` direction, `invalid` vs `partial`,
  player/team non-reconciliation (R10), text `entity_id`.
- `docs/SCHEMA_CONTRACT.md` 2026-09-03 changelog -- full column lists.

## What is live

| Surface | Grain | Rows (2025 / 2026 as of 2026-09-03) |
|---|---|---|
| `api.rushing_charting_player_season` | (season, player_id, team) | 1,622 / 76 |
| `api.rushing_charting_team_season` | (season, team) | 136 / 16 |
| `api.rushing_charting_direction_season` | (season, entity_type, entity_id, team, side, direction) | 4 rows per (entity, side) |
| `public.get_player_detail(p_player_id, p_season)` | one row | LAST column `rushing_charting jsonb` (NULL when no charting row) |

Also fixed today, and load-bearing for any "returning players" question:
**`api.roster_lookup` now has 2026 rows** (30,109 players, 284 teams, every
final-2025 AP Top 25 team present). The 2026 roster load had been landing
in an unread table since preseason (cfb-database PR #112); anything the bot
answered about 2026 rosters before 2026-09-03 23:40 UTC was answered from
2025 rosters.

## Coverage -- measured live, and it is NOT the passing story

Passing charting's trap is that the headline averages divide by a partial
charted count. Rushing is different, and the tool description must say so
or the model will over-hedge the metrics that are actually complete:

| What | 2025 | 2026 (Week 0 only) |
|---|---|---|
| Yardage-tier / rate metrics (`rushing_yards_available` / `attempts`, player rows) | **100%** on every position group | 100% |
| Same, team offense rows (`offense_rushing_yards_available` / `offense_attempts`) | 97.7% avg | 98.0% |
| Touchdown status (team only) | 100% | 100% |
| Direction resolved (`direction_available_attempts` / `direction_eligible_attempts`) | **37.7% RB, 41.5% QB**, 44.5% team-offense avg | 99.0-99.5% |

So for 2025:
- `yards_per_carry`, `success_rate`, `ppa`, `stuff_rate`, `power_success`,
  `explosiveness`, and the line/second-level/open-field yards are computed
  over **every** carry. They need a sample-size floor (35 carries is noise),
  not a coverage caveat. Every row has non-NULL `ppa` (722 of 722 RBs).
- Direction splits (`api.rushing_charting_direction_season`, and the
  `directions` object inside `get_player_detail.rushing_charting`) are the
  partial piece: roughly 40% of eligible carries are resolved to
  left/middle/right, the rest sit in `unknown`. Any 2025 direction answer
  must carry `direction_available_attempts` and say "of N charted carries".
- 2026 is near-complete same-day for both, with the two-day provisional
  window from the convergence-watch doc.

Floor guidance, from the live 2025 `api.rushing_charting_player_season`
distribution:

| Floor | `attempts` qualifiers | `direction_available_attempts` qualifiers |
|---|---|---|
| >= 20 | 629 | 323 |
| >= 50 | 376 | 130 |
| >= 100 | 173 | 30 (max is 160) |

Recommended defaults: **50 attempts** for headline sorts, **20 direction
charted carries** for direction sorts. Both must be applied server-side
before the row cap (same reasoning as `passing-charting.ts`), and the
enforced floor must be echoed back.

Position mix matters: 2025 has 722 RB, 388 QB, 377 WR, 55 TE player-seasons
plus a long tail (P, PK, LB, OL...). A leaderboard without a position
filter puts QBs (John Mateer, 145 carries, 18.3% stuff rate, sacks folded
into his `attempts`) next to RBs. Default the tool to `position = 'RB'`
with an override, and say in the description that QB rows include sacks
as attempts (individually attributed sacks and kneels are inside the
player's `attempts`; the separate `sacks`/`kneels` counters on player rows
are typically 0 -- see the R10 section of the companion doc).

## Work order

### 1. `run_sql` schema card (`src/lib/mcp/tools.ts`, next to the passing entries around line 1418)

Add three entries. Draft text, tuned to the measured coverage above:

```
- api.rushing_charting_player_season: RUSHER charting, 2025+ ONLY (season, player_id, team): position,
  attempts, total_rushing_yards, yards_per_carry, success_rate, ppa (per rush), total_ppa, stuff_rate,
  power_success, explosiveness, line_yards / second_level_yards / open_field_yards (per-carry) + *_total.
  UNLIKE passing, the rate metrics are over EVERY carry (rushing_yards_available = attempts in 2025),
  so they need a sample floor, not a coverage caveat: always WHERE attempts >= 50 (or say the floor).
  Direction is the partial part: direction_eligible_attempts vs direction_available_attempts (~40%
  resolved in 2025, ~99% in 2026). Filter position = 'RB' unless asked -- QB rows count sacks as
  attempts. NULL = not charted, never 0. Prefer the get_rushing_charting tool
- api.rushing_charting_team_season: (season, team_id, team) with offense_*/defense_* pairs of the same
  metrics plus offense_/defense_rushing_touchdowns and their own *_touchdown_status_available
  denominator. defense_* is THIS team's run defense. offense_attempts does NOT equal the sum of its
  players' attempts (CFBD keeps team-only/multi-carrier/unresolved carries off player rows) --
  never compute a player's share of team carries from these two views
- api.rushing_charting_direction_season: tall, 2025+ (season, entity_type 'player'|'team', entity_id
  TEXT, team, side 'offense'|'defense', direction 'left'|'middle'|'right'|'unknown') -- exactly 4 rows
  per entity+side, 15 metrics each (carries, yards, ypc, success_rate, ppa, stuff_rate, explosiveness...).
  'unknown' = eligible carries not yet resolved to a direction, NOT a charted bucket: share of resolved
  = carries / direction_available_attempts for left/middle/right only; unknown / eligible is the
  coverage gap. Join players on entity_id = player_id::text
```

And one line on the existing `api.roster_lookup` entry: 2026 rosters are
present as of 2026-09-03; the "returning in 2026" join is
`r.id = p.player_id AND r.team = p.team AND r.year = 2026`.

### 2. `get_rushing_charting` curated tool

Mirror `get_passing_charting` end to end: query module
`src/lib/queries/rushing-charting.ts`, tool + description + input shape in
`src/lib/mcp/tools.ts`, `withToolTelemetry('get_rushing_charting', ...)`,
`agent/tools/get_rushing_charting.ts` plus the advisor re-export, and a
`src/lib/mcp/__tests__/rushing-charting-tools.test.ts`.

Suggested input shape:

| Param | Type | Default | Notes |
|---|---|---|---|
| `season` | int | current season | 2025+ only; earlier returns the coverage-boundary message |
| `team` | string | -- | exact school name |
| `conference` | string | -- | |
| `position` | string | `'RB'` | pass `'ALL'` (or null) to drop the filter; describe the QB-sacks caveat |
| `sort` | enum | `'ppa'` | see table below, all DESC except `stuff_rate` ASC |
| `min_attempts` | int >= 1 | 50 | floors `attempts`, or `direction_available_attempts` for direction sorts |
| `limit` | int 1-100 | 25 | |

Sort keys and the floor column each one binds:

| `sort` | Column | Floor column | Direction |
|---|---|---|---|
| `ppa` | `ppa` | `attempts` | DESC |
| `success_rate` | `success_rate` | `attempts` | DESC |
| `explosiveness` | `explosiveness` | `attempts` | DESC |
| `ypc` | `yards_per_carry` | `attempts` | DESC |
| `stuff_rate` | `stuff_rate` | `attempts` | **ASC** (lower is better) |
| `power_success` | `power_success` | `attempts` | DESC |
| `yards` | `total_rushing_yards` | `attempts` | DESC |
| `attempts` | `attempts` | `attempts` | DESC |
| `line_yards` / `second_level_yards` / `open_field_yards` | per-carry columns | `attempts` | DESC |

Response: `{"_source", "count", "rows", "min_attempts", "position", "coverage_note"}`
with a derived `direction_coverage_pct = direction_available_attempts /
NULLIF(direction_eligible_attempts, 0)` per row (3dp, null-preserving) so
a follow-up direction question already has its denominator in context.
Deterministic tiebreak on `player_id`.

Description must state, in this order: rate metrics are over all carries
(floor for sample size, not coverage); default RB filter and why; direction
is ~40% resolved in 2025; NULL never renders as 0; player totals do not
reconcile to team totals; 2025 is provisional only via explicit re-pull.

### 3. Direction profile -- extend `get_player_detail` handling, defer a new tool

`get_player_detail` already returns `rushing_charting` with a nested
`directions` object (`left`/`middle`/`right`/`unknown`, 15 metrics each) and
all three player-grain denominators. Update the player-detail schema card
in `tools.ts` (the `top_hit_detail` block around line 616-629) to name the
block and its `directions` keys, and to say that direction shares are
`directions.<dir>.carries / direction_available_attempts` (never divide
`unknown` by `available`).

A dedicated `get_rushing_direction` team tool (offense/defense side of
`api.rushing_charting_direction_season`) is worth doing once 2026 has a few
weeks of data; on 2025 it would mostly rank teams by how much CFBD has
charted. Not requested now.

### 4. Bot cache invalidation -- content fingerprint over api views

The companion doc's invalidation tuple reads `stats.rushing_plays`, which
`analyst_ro` cannot see (cfb-app's own finding in
`docs/WAREHOUSE_EXPANSION_HANDOFF.md` §1). It is also count-based, and a
re-pull can correct a player's PPA, move a carry from `unknown` to
`middle`, or re-attribute a team-only rush without changing any count, so
counts alone would leave a cached answer stale. Fingerprint the answer-
bearing rows instead. This runs through `run_sql` in well under the 8 s
cap (verified 2026-09-04: 2 rows, one per season):

```sql
WITH d AS (
  SELECT season, 'player' AS v,
         md5(string_agg(p::text, '|' ORDER BY player_id, team)) AS digest
  FROM api.rushing_charting_player_season p GROUP BY season
  UNION ALL
  SELECT season, 'team', md5(string_agg(t::text, '|' ORDER BY team))
  FROM api.rushing_charting_team_season t GROUP BY season
  UNION ALL
  SELECT season, 'direction',
         md5(string_agg(x::text, '|' ORDER BY entity_type, entity_id, team, side, direction))
  FROM api.rushing_charting_direction_season x GROUP BY season
)
SELECT season, string_agg(v || ':' || digest, ',' ORDER BY v) AS fingerprint
FROM d GROUP BY season ORDER BY season;
```

Persist the per-season fingerprint alongside any cached rushing answer and
recompute when it changes. It covers every column of all three views
(player, team, and direction rows, including the team-only attribution
counters), so an in-place correction from a re-pull changes the digest
even when no count moves. The row `::text` cast is deterministic for a
fixed view definition; a redeploy that changes a view's column list
changes every digest once, which is the correct outcome. Do not watermark
on load ids.

## Worked queries -- all verified through `run_sql` on 2026-09-03

**One player's headline row** (Xavier Robinson, Oklahoma, 2025: 35 att,
6.1 ypc, 42.9% success, 0.319 ppa, 2.9% stuff, 1.466 explosiveness):

```sql
SELECT season, player, team, position, attempts, yards_per_carry, success_rate, ppa,
       stuff_rate, power_success, explosiveness,
       direction_available_attempts, direction_eligible_attempts
FROM api.rushing_charting_player_season
WHERE season = 2025 AND team = 'Oklahoma' AND position = 'RB'
ORDER BY attempts DESC LIMIT 10;
```

**Ranked within a peer pool** (RBs with 35+ carries on final-2025 AP Top 25
teams; 65 backs):

```sql
WITH top25 AS (
  SELECT school FROM api.poll_rankings
  WHERE season = 2025 AND season_type = 'postseason' AND week = 1 AND poll ILIKE 'AP%'
), pool AS (
  SELECT p.player, p.team, p.attempts, p.ppa, p.explosiveness, p.stuff_rate,
         p.yards_per_carry, p.success_rate
  FROM api.rushing_charting_player_season p
  JOIN top25 t ON t.school = p.team
  WHERE p.season = 2025 AND p.position = 'RB' AND p.attempts >= 35
)
SELECT *, RANK() OVER (ORDER BY ppa DESC) AS ppa_rk,
       RANK() OVER (ORDER BY stuff_rate ASC) AS stuff_rk,
       COUNT(*) OVER () AS n, ROUND(AVG(ppa) OVER ()::numeric, 3) AS pool_avg_ppa
FROM pool ORDER BY ppa DESC LIMIT 100;
```

Note the final AP poll is `season_type = 'postseason', week = 1`, and
`api.poll_rankings` uses `school`, not `team`.

**Same pool, restricted to backs on the same team's 2026 roster** (30 of the
65; this is the query that needed today's roster fix):

```sql
... FROM api.rushing_charting_player_season p
    JOIN top25 t ON t.school = p.team
    JOIN api.roster_lookup r
      ON r.id = p.player_id AND r.team = p.team AND r.year = 2026
    WHERE p.season = 2025 AND p.position = 'RB' AND p.attempts >= 35
```

Join on `player_id`, not name; `roster_lookup.id` is text and so is
`player_id`.

**Team run-game identity** (offense side, one row per team):

```sql
SELECT team, offense_attempts, offense_yards_per_carry, offense_success_rate, offense_ppa,
       offense_stuff_rate, offense_explosiveness,
       offense_line_yards, offense_second_level_yards, offense_open_field_yards,
       offense_rushing_touchdowns, offense_touchdown_status_available
FROM api.rushing_charting_team_season
WHERE season = 2025 AND offense_attempts >= 300
ORDER BY offense_ppa DESC LIMIT 25;
```

## What cfb-database will do on its side

- Daily load now carries `rushing` for the in-season year; the finished
  2025 season only moves via an explicit `--sources passing,rushing`
  re-pull (next planned ~2026-09-07). Tell us if a direction-coverage
  jump would help a specific feature and we will pull sooner.
- `verify_load.py` now fails a daily run that creates a new dlt
  `__v_double` twin on the charting tables, so a metric cannot silently go
  NULL in the api views (cfb-database PR #110).
- Row-count floors for the three rushing marts and a current-season
  `core.roster` floor are queued behind the first weekly re-pull.

Reply with a handoff doc or issue in either repo; questions about
semantics belong against the companion doc, not this one.
