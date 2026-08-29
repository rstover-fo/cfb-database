# CFBD 2026 Expansion — Rollout Runbook

**Date:** 2026-08-29
**Branch:** `claude/cfbd-cfb-package-updates-7iabg2`
**Status:** Built and reviewed; nothing below has been applied to the live database yet.

## What shipped on the branch

- **12 new CFBD endpoints wired** (playoffs ×3, coach seasons/tenures/profiles,
  conference affiliations/changes, SRS-expanded, player success ×2, player season
  overview) plus `/stats/game/advanced` (closed the long-UNMAPPED row 23).
- **Two repairs**: `/game/box/advanced` lost its `year` param upstream and had been
  silently no-oping (now explicit `game_ids` mode for the refresh drainer);
  `/ppa/predicted` had never loaded a row (now the opt-in `metrics_ppa_predicted`
  source, down 1-4 × distance 1-30, ~120 one-time calls).
- **Historical-refresh mechanism** (`scripts/backfill_refresh.py`,
  `meta.refresh_campaigns`/`refresh_progress`, `historical-refresh.yml`): resumable,
  1,000 calls/run and 30,000/month caps, plays_stats drains before box_advanced,
  self-disables when complete.
- **16 flat-file sources, zero API cost**: 4 sportsdataverse (team/game ID
  crosswalks, `ratings.espn_fpi_weekly` 2005+, `ratings.sdv_ratings_weekly`),
  7 NCAA sub-FBS datasets (2013+, new UNGRANTED `ncaa` schema — ids re-issued
  every season, never join to CFBD ids without a crosswalk), 5 ESPN player-grain
  datasets (`stats.espn_*`, 2004+; `espn_play_participants` carries the shared
  CFBD/ESPN athlete-id join spine).
- **`marts.epa_crossvalidation`** — the refresh campaign's plausibility harness,
  with a pre-registered operator decision rule in its header.
- Housekeeping: `wepa` wired into the daily load (never ran before), docs/manifest
  regenerated (74 endpoints), dead config removed, Model Delegation section in
  CLAUDE.md.

Facts discovered en route (recorded in code/manifest comments): CFBD numeric
team/game/athlete ids ARE ESPN's ids (verified against the live warehouse);
no player crosswalk asset exists upstream and none is needed; ESPN pbp starts
2004 (the 2002-03 gap-fill idea was wrong); NCAA pbp is only ~12MB/season.

## Deploy + backfill order (schema-review-approved checklist)

Migrations 050-055 are `--file`-only (not in MIGRATION_ORDER). 050 and 054 apply
AFTER the loads that create their tables (the 026 pattern); 051/053/055 have no
load precondition.

1. [ ] Apply `051_refresh_ledger.sql`, `053_ncaa_tables.sql`, `055_espn_tables.sql`
       (no preconditions). Apply `052_sportsdataverse_xwalk_ratings.sql` — it MUST
       land in an earlier deploy run than mart 044 (deploy_schema.py runs marts
       before `--files`; 044 fail-fasts if 052's tables are absent).
2. [ ] **A2 backfills** via `backfill-sources.yml` dispatches (~800 calls total):
       playoffs 2014-2025; ratings 2004-2025 (srs_expanded rides along);
       `stats:player_success_season+player_success_game+game_advanced` 2014-2025;
       coaches + conferences (bulk + `--source conferences --mode backfill` for
       changes history). One-time manual runs: `--source coach_tenures` (~350
       per-team calls), `--source metrics_ppa_predicted` (~120 calls).
3. [ ] Let the daily load (or one manual `load_season.py` run) create
       `ref.coach_profiles` / `stats.player_season_overview` via the new capped
       drainers, then verify dlt column names via `pg_attribute` (coach endpoints
       were never observed live — every probe 400'd) and apply
       `050_expansion_grants_indexes.sql` and `054_fanout_grants_indexes.sql`.
4. [ ] **Cheap corrected-data re-runs** (~4,900 calls): backfill-sources.yml
       dispatches, seasons 2025→2014, sources
       `metrics,wepa,recruiting,rosters,stats:advanced_team_stats+game_havoc+player_usage+player_returning+game_advanced`,
       plus one `reference` run. Split 2-3 dispatches for the 120-min timeout.
5. [ ] **Flat-file first loads**: `flat-files.yml` picks the 16 new sources up via
       `--due`; historical seasons via explicit `--season` dispatches
       (ncaa 2013-2025, espn adv 2004-2025, participants 2014-2025 — ~190 fetches,
       no API budget).
6. [ ] Deploy mart `044_epa_crossvalidation.sql` (separate, later run than 052);
       refresh marts; confirm the `cfbd_fpi_season` anchor is NON-dark (it has
       data today — dark there means the team-name join broke). Snapshot the
       per-season Spearman columns — this is the "before" baseline.
7. [ ] **Launch the refresh campaign**: dispatch `historical-refresh.yml` with
       `create=true` (campaign `2026-08-upstream-corrections`, seasons 2014-2025,
       tasks `plays_stats,box_advanced`). ~38k calls at ≤1,000/day under the
       30k/month cap → ~5-6 weeks. The daily cron keeps it draining; it
       self-disables (green no-op) when complete — then remove the cron.
       After each week, compare `marts.epa_crossvalidation` against the baseline
       per the decision rule in the mart header.
8. [ ] **player_overview backfill 2021-2025** (~23k calls — live counts run
       4-5.5k players/season, higher than the ~3k planning estimate): dispatch
       `backfill-sources.yml --sources player_overview` per season, or let the
       daily drainer work at 250/day. Pace against the campaign so combined
       spend stays under ~30k/month of headroom. **2014-2020 (~30k more) is
       parked pending an explicit go-ahead.**
9. [ ] On merge to main: remove the temporary `push:` trigger from
       `probe-endpoints.yml` (workflow_dispatch works once it's on main).

## Budget picture

| Item | Calls | When |
|---|---|---|
| Daily in-season load (now 18 sources) | ~2,918/day cap | ongoing |
| A2 endpoint backfills + one-times | ~1,300 | step 2 |
| Cheap corrected-data re-runs | ~4,900 | step 4 |
| Refresh campaign (both per-game tasks) | ~38,000 | step 7, paced |
| player_overview 2021-2025 | ~23,000 | step 8, paced |
| Flat-file sources | 0 (GitHub fetches) | steps 5+ |

Worst-case months stay ≈100k of the 125k Tier-4 budget with both paced streams
active alongside the in-season daily load.

## Known parked items / follow-ups

- player_overview 2014-2020 (~30k calls) — explicit user go-ahead required.
- `ncaa` schema exposure — deliberately ungranted until a crosswalk exists;
  revisit when an FCS join path is built on the `espn_game_id` bridge columns.
- `ref.conference_affiliations` replacing the games-derived
  `public.team_season_class` workaround — contract-gated (cfb-app heads-up).
- Coach marts rekeying on `coach_id` (api.coaching_history/coach_records keep
  their name-keyed logic untouched).
- Participant-id indexes on `stats.espn_play_participants` — add on proven need.
- Pre-existing test env quirk: `test_seed_team_xwalk.py::TestEndToEndCli::
  test_sbr_fixture_run` spawns system `python` (no openpyxl) — unrelated to this
  branch.
