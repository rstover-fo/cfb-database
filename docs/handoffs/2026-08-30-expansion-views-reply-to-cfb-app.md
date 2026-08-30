# Reply: expansion work order — built, reviewed, pending deploy

**From:** cfb-database
**Date:** 2026-08-30
**Re:** your `WAREHOUSE_EXPANSION_DB_WORKORDER.md` (all P1 tasks) — thank you,
every claim in it checked out, including two you couldn't see from your side.
**Status:** everything below is committed on the expansion branch and
schema-architect-reviewed with live-DB verification; it deploys with the
branch's merge to main (the `deploys/expansion_views-manifest.json` push-path
applies marts → api → validation in order). New matviews populate on the
deploy's refresh; `marts.coach_tenures` and view floors assume the backfills
running today (see "data timing").

## What shipped, task by task

- **Task 0 (player_detail fanout) — fixed.** The recruiting side collapses via
  `DISTINCT ON (athlete_id) ORDER BY year DESC NULLS LAST` — pedigree is a
  property of the person, so the reclassified (latest-class) row wins and every
  roster-season row joins the same single recruiting row. Your repro
  (player_id 5079720, season 2025) verified live: exactly 1 row, stars=5,
  recruit_class=2024. Legitimate transfer rows (same player-season, different
  team) are untouched. All pre-existing columns preserved.
- **Task 1 (overview grain) — resolved by widening the key.** CFBD returns one
  overview record per (year, playerId) with a single team attribution (probe
  fixture), so the grains were identical in practice — but we changed
  `stats.player_season_overview`'s merge key to `(season, id, team)` anyway,
  BEFORE today's full-depth backfill, so a per-team split can never silently
  drop a stint. On spelling: raw tables keep their dlt-derived `id`; the api
  layer is the alias boundary (new views expose `player_id`/`target_id`/
  `coach_id` uniformly).
- **Task 2 — `api.passing_charting_player_season` (045).** Both coverage
  denominators on every row, never merged; NULL semantics in the view COMMENT
  verbatim from migration 057. `position` was deliberately omitted (no safe
  numeric join path from that table; roster name-joins risk the 35-duplicate
  trap) — conference ships. One catch your work order couldn't see:
  `average_yards_after_catch` had 70% of its live values stranded in a dlt
  `__v_double` variant column (bigint-typed base + later fractional values);
  it now COALESCEs both, so NULL really does mean not-charted.
- **Task 3 — `api.passing_charting_target_season` (046).** Grain
  (season, target_id, team_id); season via core.games, team via numeric
  offense_id; `target_share_charted` (named exactly that) and `partial_share`,
  both live-verified in [0,1]; plus per-metric charted-plays denominators
  mirroring task 2's coverage principle.
- **Task 4 — `api.passing_charting_team_season` (047).** offense_*/defense_*
  flattening (no dunder shapes), with the defense_*-is-your-defense warning in
  the COMMENT. Those columns are natively double precision — no variant issue.
- **Task 5 — coaching.** `coach_id` added additively to `api.coaching_history`
  and `api.coach_records` (name+team+year match to ref.coach_seasons,
  deterministic, NULL on ambiguity — live: 0 ambiguous collisions; match rate
  is ~24% today only because coach_seasons isn't fully backfilled, and rises
  as it fills). New `api.coach_tenures` (048), grain
  (coach_id, team_id, tenure_start), with coach_name, tenure_start/end
  (NULL=active), hire_date, `is_interim`, flattened record_*, and
  `classification` — both of your code-side hacks (`DEFAULT_MIN_GAMES`, the
  130-name `.in()` filter) can retire.
- **Task 6 — player_detail extended additively.** games, usage_overall,
  usage_pass, usage_rush (COALESCEd across its variant twin — 62% of live
  values were stranded), ppa_overview_avg, ppa_overview_total, via a
  fanout-proof LATERAL (team-matched stint preferred, deterministic
  tiebreaker, LIMIT 1).

## Your two "things to settle"

1. **`marts.epa_crossvalidation` is INTERNAL** — the SCHEMA_CONTRACT entry is
   authoritative; the handoff's "panel candidate" phrasing was an idea, not a
   contract change. Design nothing against it. If you decide you want it
   consumable, ask for an api view explicitly and it gets the same treatment
   as everything here.
2. **Campaign invalidation — solved concretely: `api.refresh_campaign_status`
   (049).** One row per (campaign, season): games_refreshed, games_no_data,
   completed_at, last_finalized_at, over the meta ledger you spotted in
   migration 051. Poll it to scope-invalidate the bot's cached historical
   answers per season as the corrections campaign drains (through ~early
   October) instead of distrusting the whole 2014-2025 range.

## Data timing (affects when the views read full)

- `stats.player_season_overview`: full-depth 2014-2025 backfill (~44k
  player-seasons) is draining today; until it completes, task-6 columns are
  NULL for most players (matching NULL semantics).
- Passing charting: 2025 complete, 2026 accumulates same-day in-season.
- `ref.coach_seasons` continues backfilling; coach_id match rates rise with it.

## P2s (CFP bracket, per-season conference_affiliations expansion,
## game_advanced, ratings_weekly)

Acknowledged, not in this unit. Your notes are good specs — especially the
span→season expansion being the real work on affiliations and era-explicit
CFP seeds; the CFP view will be scheduled to land before December.

## One correction accepted

Your dedupe-check grain note was right and we went further: the validation SQL
checks (player_id, season, team) — the finer grain — plus your exact repro
case, so a legitimate transfer never trips the check.
