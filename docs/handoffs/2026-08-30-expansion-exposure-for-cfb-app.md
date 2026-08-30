# Handoff: 2026-08 warehouse expansion — exposure plan wanted from cfb-app

**From:** cfb-database
**Date:** 2026-08-30
**Status:** all data live in the shared Supabase DB (or landing today — see
"In flight" below). Nothing in `api`/`public` changed yet; that is the ask.
**Deliverable requested:** a prioritized exposure plan from cfb-app covering
(a) dashboard/UI features, (b) the MCP/agent tool surface (run_sql views list,
curated tools), and (c) the list of new `api.*` views cfb-app wants
cfb-database to build — returned as a handoff doc or issue.

## Why

CFBD shipped a major pre-season API update (spec v5.24.2 → v5.25.0, 79
endpoints) and cfbfastR 3.0.0 exposed the sportsdataverse parquet releases.
cfb-database ingested all of it over 2026-08-29/30. The warehouse now carries
substantial player-grain, coaching, and charting data that neither the app nor
the bot can see yet. Player-grain depth is the platform's stated strategic
priority (future PFF/SIS joinability).

## Ground rules (unchanged)

- `docs/SCHEMA_CONTRACT.md` in cfb-database remains the contract. The stable
  surface is `api.*` views + `public` views/RPCs. Everything below in `stats`/
  `ref`/`core`/`ratings`/`metrics` schemas is **internal raw tables** — they
  are SELECT-granted to anon/authenticated (PostgREST-reachable) and fine for
  prototyping and `run_sql`, but a shipped app feature should sit on an
  `api.*` view requested from cfb-database, not raw-table coupling.
- Athlete ids are TEXT and **CFBD athlete/team/game ids ARE ESPN ids**
  (verified live) — `espn.*` joins need no crosswalk.
- Join teams by numeric id (`team_id`, `offense_id`, `defense_id`) wherever
  one exists: `ref.teams` has 35 legitimate duplicate school names, so
  name-string joins need `DISTINCT ON (school)` or accept fanout.
- Player-season PKs include `team` (transfer safety) — a player can carry two
  rows in one season.

## What's new, by feature area

### 1. Passing charting — the headline (data 2025+, live in-season)

Five tables in `stats`, from CFBD's new /passing endpoints (air yards, aDOT,
pass depth/direction/location, YAC — manually charted from film):

| Table | Grain / PK | Rows now |
|---|---|---|
| `stats.passing_plays` | (game_id, play_id) — per pass attempt | 53.5k (2025) + live 2026 |
| `stats.passing_player_games` | (game_id, player_id) | 3k+ |
| `stats.passing_player_season` | (season, player_id, team) | 820 (2025) |
| `stats.passing_team_games` | (game_id, team), offense__*/defense__* columns | 1.7k+ |
| `stats.passing_team_season` | (season, team) | 136 (2025) |

Key columns on `passing_plays`: `passer_id`, **`target_id`** (the warehouse's
first receiver-grain join surface — indexed), `outcome`, `air_yards`,
`pass_depth`, `pass_direction`, `pass_location`, `yards_after_catch`,
`is_spike`/`is_throwaway`/`is_intentional_grounding`, `parse_status`,
`offense_id`/`defense_id` (preferred team-join columns).

**NULL semantics (on the columns as COMMENTs, honor them in every surface):**
NULL charting value = not (yet) charted, never zero; 0 is a real observed
value; `*_attempts_available` columns are the charting-coverage denominators
(e.g. `total_air_yards IS NULL` when `air_yards_attempts_available = 0`).
`parse_status='partial'` marks incompletely-charted plays; partial rows may be
re-charted upstream later. Coverage is genuinely partial in 2025 (407 of 820
player-seasons have air-yards aggregates) — leaderboards must show the
denominator or filter on it, or they will rank on coverage, not skill.
2026 charts land the SAME DAY as games — this is live-season data.

Feature ideas worth planning: aDOT/air-yards leaderboards, receiver target
share and target quality (via `target_id`), QB depth-of-target profiles,
team pass-location tendencies, YAC-over-expected once modeled.

### 2. Player-season overview hub (2014-2025, ~44k rows landing today)

`stats.player_season_overview` — one row per (season, id, team): the CFBD
/player/season/overview payload (usage, PPA, stat lines) for every player in
`stats.player_usage` ∪ `metrics.ppa_players_season`. Currently draining ~44k
player-seasons at full depth 2014-2025 (complete within ~a day). This plus
passing charting plus `stats.player_success_season/game` (success rates,
2014+) is the new player-grain core the bot's player tools should sit on.

### 3. Coaching (new: real coach ids)

- `ref.coach_seasons` (3,066 rows, 2014+-ish): per coach-season detail with
  `coach__id` — CFBD's real coachId, ending name-string coach joins.
- `ref.coach_tenures` (2,738 rows): per (coach, team) tenure spans with
  `start_year`/`end_year`, `hire_date`, `is_interim`, `record__wins` etc.
- `ref.coach_profiles` (drainer, 200+ rows growing 250/day): career profile
  per coach id.
Existing `api.coaching_history`/`api.coach_records` still run on the old
name-keyed marts — a coach_id rekeying of those views is a natural ask.

### 4. CFP + conference history

- `core.cfp_bracket` / `cfp_games` / `cfp_participants` (2014+): structured
  playoff brackets, rounds, matchups, seeds.
- `ref.conference_affiliations` / `ref.conference_changes`: full realignment
  history to 1869 — replaces games-derived conference inference.

### 5. Ratings & metrics additions

- `ratings.srs_expanded` (2004+): FCS-inclusive SRS.
- `ratings.fpi_weekly` + `ratings.external_weekly` (2005+): **as-of weekly**
  rating snapshots (leak-free backtest inputs, unlike end-of-season tables).
- `ratings.massey_composite` (+ `massey_system_ratings` child): the Massey
  composite table; populates weekly once masseyratings.com starts publishing
  2026 (team names resolve via the now-seeded `ref.team_name_xwalk`).
- `metrics.ppa_predicted` (10,140 rows): static expected-points lookup by
  down/distance/yardline — useful as a reference layer in play views.
- `stats.game_advanced_team_stats` (2014+): year-scoped per-game advanced
  team stats with garbage-time-excluded variants.
- `marts.epa_crossvalidation`: house adjusted-EPA vs external systems
  (Spearman per season; baseline avg 0.881) — a data-quality panel candidate.

### 6. ESPN player splits + id crosswalks (sportsdataverse parquet)

New `espn` schema: `espn.player_passing/rushing/receiving/defense` (EPA-grain
advanced player splits, 2004+ — historical seasons loading today) and
`espn.play_participants` (2014+). Joins to CFBD players directly by id.
`ref.player_id_xwalk` / `team_id_xwalk` / `game_id_xwalk` add Fox/Yahoo ids.

### 7. Explicitly NOT for consumption

- **`ncaa` schema (sub-FBS 2013+) is deliberately UNGRANTED** — stats.ncaa.org
  re-issues player ids every season; it stays staging-only until an identity
  strategy exists. Do not plan features on it yet.
- `meta`, `raw`, dlt `_dlt_*` columns: internal.
- SBR betting history: table exists, no data until real SBR files arrive.

## Data-correctness context the bot should know

A historical-corrections refresh campaign (upstream garbage-time
reclassifications, 2014-2025 play stats + box scores, ~19k games) is draining
at ≤1,000 calls/day through ~early October. EPA-family numbers for past
seasons will shift slightly as it lands; `marts.epa_crossvalidation` is the
plausibility harness. Year-keyed corrected sources (metrics, wepa, recruiting,
rosters, advanced stats) were re-ingested 2026-08-30.

## What cfb-app should return

A prioritized plan (P1/P2/P3) with, per item: the consumer (dashboard page,
bot tool, run_sql prompt-surface line), the data source (existing table/view
or a requested new `api.*` view with its proposed columns and grain), and any
semantics the UI must carry (charting NULLs, coverage denominators,
transfer-split rows, partial parse_status). Send new-view requests back as a
handoff doc or issue to cfb-database — do not build on raw tables for shipped
features. Quick wins to consider first: add the new granted tables to the
bot's run_sql available-views prompt list (zero code), then curated tools for
air-yards leaderboards, target analysis, coach tenures, and CFP brackets.
