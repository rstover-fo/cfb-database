# Rushing charting for cfb-app

**From:** cfb-database
**Date:** 2026-09-03
**Audience:** cfb-app (dashboard + bot)
**Status:** Stage A (raw tables) LIVE. Stage B (views + RPC extension) is
now **LIVE** too -- applied to production 2026-09-03 by Deploy Schema run
33783102034 (migration `059` applied; marts built 1,698 / 152 / 8,008 rows
for player_season/team_season/direction_season; validation passed).
Everything below is queryable against `api.*` now.

**Deploy sequencing.** `tests/test_api_views.py` and `tests/test_marts.py`
already asserted against these objects, and the daily mart refresh
(`scripts/refresh_marts.py`) already listed the three new rushing marts,
both ahead of the objects existing in production. The manifest
(`deploys/rushing_views-manifest.json`, covering marts/api definitions
`050`-`052`) and migration `059_rushing_grants_indexes.sql` (applied
separately via `run_migrations.py --file`, per its own header) were both
applied to production from `deploy/rushing-stage-b` **before** the Stage B
PR merged, per this repo's schema-migrations convention. `api.*` has been
queryable since that deploy completed.

## What shipped

**Stage A (live, PR #107, merge commit `dc6568d`).** CFBD's five `/rushing`
charting endpoints, ingested as their own dlt source (`rushing.py`),
mirroring the passing-charting unit shipped 2026-08-30. Backfill run
33776170498 loaded 2025+2026 with no failed jobs:

| Table | Rows (2025+2026) |
|---|---|
| `stats.rushing_plays` | 63,234 |
| `stats.rushing_player_games` | 8,205 |
| `stats.rushing_team_games` | 1,758 |
| `stats.rushing_player_season` | 1,698 |
| `stats.rushing_team_season` | 152 |

These are internal (`stats` schema); you read them only through the api
views below.

**Stage B (live, deployed 2026-09-03).** Three `api.*` views plus an additive
`get_player_detail` column, per `deploys/rushing_views-manifest.json`:

| View | Grain | What it carries |
|---|---|---|
| `api.rushing_charting_player_season` | (season, player_id, team) | Headline rushing charting per player-season: yardage tiers (line/second-level/open-field), success rate, PPA, stuff rate, power success, explosiveness, plus box-score attempts/yards. |
| `api.rushing_charting_team_season` | (season, team) | Same headline metrics, offense and defense sides (`offense_*`/`defense_*`), plus rushing touchdowns. |
| `api.rushing_charting_direction_season` | (season, entity_type, entity_id, team, side, direction) | Tall direction splits (left/middle/right/unknown) for both players and teams -- the per-direction detail behind the two headline views above. |

`get_player_detail` gains a LAST column, `rushing_charting jsonb`: NULL when
the requested player-season has no rushing charting row; otherwise the
headline metrics, the three player-grain coverage denominators
(`rushing_yards_available`, `direction_eligible_attempts`,
`direction_available_attempts`), the attribution counters
(`individual_attempts`, `unattributed_attempts`, `sacks`, `kneels`,
`team_rushes`, `multi_carrier_attempts`) as their own separate clause, and
a nested `directions` object keyed `left`/`middle`/`right`/`unknown` (15
metrics each, always all four keys once non-NULL). `touchdown_status_
available` is a team-season-only denominator (`api.rushing_charting_team_
season`) and does not appear on this player-grain block.

Full column lists and NULL/denominator semantics for each view are in
`docs/SCHEMA_CONTRACT.md` (2026-09-03 changelog entry and the api/marts
tables) -- this doc covers how to read them correctly, not the column list
itself.

## How to read these views

**Carry four coverage denominators, and never merge them.** Each view has
its own set, and none of them is interchangeable with another:

- `rushing_yards_available` (player-season) / `offense_rushing_yards_
  available` + `defense_rushing_yards_available` (team-season) -- the
  denominator for yardage-tier and rate metrics (line/second-level/
  open-field yards, success rate, PPA, stuff rate, power success,
  explosiveness).
- `direction_eligible_attempts` and `direction_available_attempts`
  (`available <= eligible`) -- present on all three views, the denominators
  for direction splits. `eligible` is the population CFBD considers
  chartable by direction (excludes kneels/sacks by construction);
  `available` is what has actually been resolved to a left/middle/right
  direction so far. The gap (`eligible - available`) is exactly
  `unknown`'s carries -- the unresolved remainder, not a fourth charted
  direction; never divide `unknown` by `available` (yields >100%) --
  `unknown / eligible` is the coverage gap, not a share. These names
  intentionally diverge from passing charting's `*_attempts_available`
  suffix (e.g. `air_yards_attempts_available`): rushing has two direction
  denominators, eligible vs. available, where passing charting has one per
  metric family, so a single shared suffix would not distinguish them.
- `offense_touchdown_status_available` / `defense_touchdown_status_
  available` (team-season only) -- the denominator for
  `offense_rushing_touchdowns` / `defense_rushing_touchdowns`.

NULL on a metric means the carries behind it were not charted yet; 0 is a
real observed value. Do not treat a NULL rate as zero, and do not compute a
share or rate without dividing by the matching denominator above.

**`unknown` direction is the unresolved remainder, not a charted bucket.**
`api.rushing_charting_direction_season` always carries exactly 4 rows per
(entity, side) -- left/middle/right/unknown -- even when every metric on a
row is NULL. `unknown`'s carries equal `direction_eligible_attempts -
direction_available_attempts`: the eligible carries CFBD has not yet
resolved to left/middle/right. It is read directly off the source rather
than subtracted here, but its status is "not yet charted by direction," the
same as `parse_status='partial'` on `stats.rushing_plays` -- keeping the row
visible (instead of dropping it) is exactly what makes that coverage gap
visible to a consumer. If you build a stacked-bar or pie chart of direction
mix, `unknown` still belongs in it as its own slice -- just don't read its
presence as "CFBD charted this carry as unknown" the way left/middle/right
are charted.

**`invalid` vs `partial` on `stats.rushing_plays`.** `parse_status` has
three values: `complete`, `partial`, and `invalid`. `invalid` is its own
bucket -- a play CFBD's own parser flagged as unusable -- and is never
counted as charted in any coverage denominator, distinct from `partial`
(charted but incomplete). If you build a coverage indicator on top of these
views, do not fold `invalid` into either "charted" or "not yet charted";
report it separately or exclude it explicitly.

**Player totals never reconcile to team totals.** This is upstream CFBD
attribution, not a warehouse bug: a team-season's `offense_attempts` will
NOT equal the sum of `attempts` across every player on that team in
`api.rushing_charting_player_season`, because CFBD attributes some carries
to team-only, multi-carrier, or unresolved buckets that never attach to an
individual player. The same holds one level down: a player entity's
carries summed across its 4 offense rows in `api.rushing_charting_
direction_season` will not equal the matching team entity's carries for the
same (season, team). Do not build a "player's share of team carries" metric
from these views without accounting for that gap -- and never surface a
computed share without the denominators alongside it.

**`entity_id` is text, `team_id` is numeric.** On
`api.rushing_charting_direction_season`, `entity_id` is always text (player
rows carry the CFBD athlete id as a string; team rows carry
`COALESCE(team_id::text, team)`, so it's never NULL even when `team_id`
itself is). `team_id` is a separate numeric column on all three views,
joining `ref.teams(id)`, derived from the (season, offense-name ->
offense_id) mapping in `stats.rushing_plays`; it is NULL when that mapping
is ambiguous (never-guess) rather than guessed.

## In-season data timing: two-day provisional window

Same finding as passing charting (`docs/handoffs/2026-09-01-charting-
convergence-watch.md`): in-season (2026+), `parse_status='partial'` is an
active re-charting queue that resolves within about a day, with roughly a
0.6% residue that may be terminal. Treat a rushing-charting answer for the
current week as provisional for about two days before caching it as final;
the residue is small enough that a longer hold isn't needed. 2025 is a
different regime -- see the frontier baseline in the convergence-watch
handoff -- charting improvements to already-finished 2025 games reach the
warehouse only via an explicit re-pull, never the daily load.

## Bot cache invalidation

Per season, persist alongside any rushing-charting-derived answer the tuple:

```sql
SELECT
  season,
  count(*) AS plays,
  count(*) FILTER (WHERE parse_status = 'partial') AS partial_count,
  count(*) FILTER (WHERE parse_status = 'invalid') AS invalid_count,
  count(*) FILTER (WHERE rush_direction IS NOT NULL) AS direction_charted_count
FROM stats.rushing_plays
GROUP BY season
ORDER BY season;
```

A changed tuple for a season means recompute any cached answer scoped to
that season. Do **NOT** use `max(_dlt_load_id)` as a watermark -- merge
rewrites rows on every re-pull even when values are identical, so a
load-id watermark produces false invalidations (same rule as the passing
convergence-watch doc).

## Suggested first uses

- **Team run game profile.** `api.rushing_charting_team_season` offense
  side: yardage-tier mix (line/second-level/open-field yards) plus success
  rate and explosiveness gives a team's run-blocking-vs-explosiveness
  identity without touching play-by-play.
- **RB production by direction.** `api.rushing_charting_player_season`
  joined to `api.rushing_charting_direction_season` (entity_type='player')
  for a back's left/middle/right/unknown split -- e.g. "does this back
  produce more off-tackle or between the tackles."
- **Run-defense scouting.** `api.rushing_charting_team_season` defense side
  (remember: this team's own run defense, not the opponent's offense) plus
  the defense-side rows of `api.rushing_charting_direction_season` for
  where a defense is vulnerable by direction.

## Related context

- Full column lists, NULL semantics, and the R10 non-reconciliation
  statement for each view: `docs/SCHEMA_CONTRACT.md`, 2026-09-03 changelog
  entry.
- Passing-charting precedent and the convergence-watch mechanism this doc
  extends to rushing: `docs/handoffs/2026-08-30-expansion-views-reply-to-
  cfb-app.md`, `docs/handoffs/2026-09-01-charting-convergence-watch.md`.
- Rushing charting coverage rules (denominators, `invalid` bucket,
  non-reconciliation) as repo-wide conventions: `.claude/skills/cfbd-api/
  SKILL.md`, "Rushing charting coverage".
