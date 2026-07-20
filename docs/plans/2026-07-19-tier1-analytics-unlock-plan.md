# Sprint Plan: Tier 1 Analytics Unlock

**Date:** 2026-07-19
**Branch:** `claude/database-2026-readiness-qss9fm`
**Status:** executing

## Context

The warehouse loads authoritative CFBD data that the analytics layer ignores, while several marts re-derive inferior local approximations of the same things. A capability audit (vs. how the CFBD analytics community actually works) found:

- **WEPA (opponent-adjusted EPA)** — team, player passing/rushing, kicker PAAR — is fully loaded (`metrics.wepa_*`, confirmed present) and referenced by zero marts/views/functions. Opponent adjustment is the #1 sophistication marker in this space.
- **Player EPA attribution** (`marts/011_player_game_epa.sql`) regex-parses `play_text` while `stats.play_stats` (CFBD's athlete-to-play link table) is loaded for exactly this purpose.
- **Defensive havoc** (`marts/005`) uses `ILIKE '%sack%'` heuristics while `stats.game_havoc` (authoritative) sits unused.
- **Returning production, player usage, ATS records** are loaded and surfaced nowhere.
- **Garbage time** is defined once in `functions/is_garbage_time.sql` (never called), re-inlined identically in 5 marts, and *not applied at all* in five public split RPCs — silent inconsistency across dashboard tabs.
- **Betting line movement is destroyed daily**: `betting.lines` PK `(game_id, provider)` merge-overwrites; history cannot be backfilled, so capture must start before the season.

This sprint unlocks already-loaded data (zero new modeling) plus starts the line-snapshot capture. Season starts late August.

**Data-presence gate:** `docs/db-snapshot-current.json` (stamped 2026-01-28) confirms `metrics.wepa_*` and `betting.lines` present, but does NOT contain `stats.play_stats`, `stats.game_havoc`, `stats.player_usage`, `stats.player_returning`, `betting.team_ats` — the manifest says WORKING but they may postdate the snapshot or were never backfilled. Phase 0 verifies live row counts before anything is built on them; marts on gate tables get loud empty-guards.

## Deploy mechanism (amended from original design)

The sandbox cannot reach Supabase; GitHub Actions can (`SUPABASE_DB_URL` secret). The original design used `workflow_dispatch`, but the session's GitHub integration cannot trigger dispatches (403) — and `workflow_dispatch` also requires the workflow on the default branch first. **Amended mechanism: push-triggered deploy branches.**

- **`.github/workflows/deploy-schema.yml`** triggers on `push: branches: ["deploy/**"]` (push-triggered workflows run from the pushed ref, so no main merge is needed first) *plus* `workflow_dispatch` inputs for human use.
- On push, a driver (`scripts/deploy_schema.py`) reads **`deploy-manifest.json`** committed at the repo root of the deploy branch: `{"action": "presence_check" | "apply" | "backfill", "marts_from": …, "marts_only": …, "files": […], "refresh": bool, "backfill": {"start": …, "end": …, "sources": …}}` and executes via the existing `run_marts.py` / `run_migrations.py --file` / `refresh_marts.py` / `load_season.py` / `check_presence.py`.
- The session pushes `deploy/tier1-…` branches cut from the working branch (with the manifest), then reads the run's job logs for results (`check_presence.py` prints row counts, `information_schema.columns` for gate tables, `ref.play_stat_types` contents, and an athlete_id↔roster overlap sample — the recon that feeds Phases 3–5).
- **`scripts/check_presence.py`** (new): prints counts/columns for gate tables; `--strict` exits non-zero if any expected-non-empty table is empty.
- CI is unaffected (`ci.yml` triggers only on main pushes + PRs). Deploy branches are deleted after use.

**Core sequencing rule:** never add a new mart to `tests/test_marts.py`'s inventory (or open the PR) until the object is CREATE'd + REFRESH'd live — otherwise PR CI goes red on `pg_matviews` existence tests.

## Phase 0 — Infrastructure + presence gate (BLOCKING)

1. Author `deploy-schema.yml`, `scripts/deploy_schema.py`, `scripts/check_presence.py` (+ unit tests for manifest parsing).
2. Push `deploy/tier1-presence` with `action=presence_check`; read logs.
3. Decision branch: gate tables present → proceed. Absent/thin → push `deploy/tier1-backfill` (`action=backfill`, seasons 2013/2014–2025, `sources=stats,betting`; ~14.5K API calls, dominated by play_stats at ~1 call/game — well under the 75K budget), re-check.
4. Extraction from the same logs: `ref.play_stat_types` (26 rows) for the stat_type→role mapping; gate-table columns; athlete_id key-space overlap.

## Phase 1 — Centralize garbage time

Pattern: `public.is_garbage_time()` stays the canonical definition; marts keep the inlined predicate for performance, enforced identical by a **drift-guard test**; the five split RPCs gain the filter.

- Canonical: `(period = 4 AND ABS(COALESCE(score_diff,0)) > 28) OR (period >= 3 AND ABS(COALESCE(score_diff,0)) > 35)` — already byte-identical at all 5 inline sites (marts 002, 004, 005, 010, 019).
- `functions/is_garbage_time.sql` gets a canonical-source block comment.
- New `tests/test_garbage_time_consistency.py`: (a) unit — regex-extract every inline site from `marts/*.sql`, assert byte-identical to the canonical constant; (b) DB (skips w/o creds) — grid-assert `public.is_garbage_time(p,d)` ≡ inline expression.
- Add `NOT public.is_garbage_time(...)` (or `NOT pe.is_garbage_time`) to: `get_home_away_splits`, `get_conference_splits` (public/005), `get_red_zone_splits`, `get_down_distance_splits`, `get_field_position_splits` (public/006). Behavioral contract change → logged (§7).

## Phase 2 — Surface WEPA

New dedicated surfaces; do NOT touch contracted `api.team_detail` / `api.leaderboard_teams`.

- **`marts/029_team_wepa_season.sql`** → `marts.team_wepa_season`: passthrough of `metrics.wepa_team_season` (`year → season`; all 21 analytic cols). Unique index `(team, season)`.
- **`marts/030_player_wepa_season.sql`** → `marts.player_wepa_season`: tall union, grain `(season, athlete_id, category)` for passing/rushing (wepa, plays) + kicking (paar, attempts), with `season_rank` per category. Unique index `(season, athlete_id, category)`.
- **`api/019_team_wepa_season.sql`**, **`api/020_player_wepa_leaders.sql`**: thin views.
- `functions/get_player_detail.sql`: additive `wepa_passing, wepa_rushing, paar` via LEFT JOIN (athlete_id join; name+team+season fallback if Phase 0 shows ID mismatch).

## Phase 3 — Rebuild player EPA attribution (athlete_id)

Rewrite `marts/011_player_game_epa.sql` **in place** (same matview name; dependents keep working). Preserve all existing columns; **add** `athlete_id` and a `receiving` category.

- Join `marts.play_epa` → `stats.play_stats` on `(game_id, play_id)`; roles via stat_type sets (passing: Completion/Incompletion/Passing TD/thrown INT; rushing: Rush/Rushing TD; receiving: Reception/Target/Receiving TD — finalized against the live `ref.play_stat_types` extract).
- **Double-count guard**: DISTINCT per (play, athlete, role) so one EPA contribution per role; cross-category credit (passer + receiver on same play) is intended.
- Keep `NOT is_garbage_time`, `HAVING COUNT(*) >= 3`. New unique index `(game_id, team, athlete_id, play_category)`.
- `012_player_season_epa`: passthrough `athlete_id`, unique index `(season, team, athlete_id, play_category)`.
- `functions/get_player_game_log.sql`: match on `athlete_id = p_player_id` (name path as fallback).
- Deploy order: `--only 011` (CASCADE drops 012) → `--only 012` → refresh → function file.
- Empty-guard `DO $$ … RAISE EXCEPTION … $$` appended (gate table).

## Phase 4 — Replace havoc heuristic

`marts.defensive_havoc` + `public.defensive_havoc` are contracted — preserve the column set.

- Havoc-rate family re-sourced from `stats.game_havoc` (season-aggregated); **add** `front_seven_havoc_rate`, `db_havoc_rate`.
- Opponent-EPA family stays plays-derived (it's real EPA, not heuristic).
- Disruptive counts (sacks/INTs/fumbles/stuffs/TFLs): re-source from `stats.team_season_stats` stat_names if Phase 0 confirms a clean mapping; else retain plays-derived counts documented as approximation. Do not block the sprint on this.
- Keep grain + `(team, season)` unique index; empty-guard appended.

## Phase 5 — New capability marts

- **`marts/031_returning_production.sql`** ← `stats.player_returning` (grain team-season). `api/021_team_returning_production.sql`.
- **`marts/032_player_usage.sql`** ← `stats.player_usage` (grain season-athlete). `api/022_player_usage_leaders.sql`.
- **`marts/033_team_ats_records.sql`** ← `betting.team_ats` (grain team-season). `api/023_team_ats.sql`.
- All: unique indexes, empty-guards, thin api views with season ranks.

## Phase 6 — Betting line snapshots

- New **`betting.line_snapshots`**, dlt `write_disposition="append"`: `captured_at` (one UTC stamp per run), game/provider/line fields, `line_hash` (md5 of the 5 line values).
- **Pending games only** (payload's home/away scores are null) — completed games are immutable.
- **No capture-time dedup** (decision): a no-movement day is signal; volume is bounded (hundreds of rows/day); `line_hash` lets consumers compress streaks.
- Resource added to `betting_source()` → existing daily workflow starts writing immediately, no workflow edit.
- Indexes (`IF NOT EXISTS`, applied after first load): `(game_id, provider, captured_at)`, `(season, week)`, `(captured_at)`.
- Optional `api/024_line_movement.sql`. Unit test: single stamp per run, hash emitted, pending-only filter.

## Phase 7 — Docs & contract

- `pipeline-manifest.md` row 10 `/games/players`: DEFERRED → WORKING (~6.4M rows, consumed by api/010 + api/011); resolve the investigation note; fix summary counts. Add `betting.line_snapshots`.
- `SCHEMA_CONTRACT.md`: add the 6 new api views + 5 new marts (with columns/keys); additive changes (player EPA `+athlete_id`/`+receiving`; havoc `+2` cols; `get_player_detail` `+3` cols); dated behavioral note — five split RPCs now exclude garbage time; refresh count 28→33; date bump. Note `supabase gen types` regen as a cfb-app follow-up.
- Register all 5 new marts in `scripts/refresh_marts.py` MARTS_VIEWS (Layer 1 except 011/012 already placed) **and** `functions/refresh_all_marts.sql`; fix its "28" comment.

## Phase 8 — Final deploy, verify, PR

Full clean-room deploy via a final `deploy/tier1-apply` push (all marts + files + refresh) → presence/refresh checks green in logs → add new marts to `tests/test_marts.py` inventory → PR to main → **verification gate = PR CI green against the now-populated live DB** → merge → daily workflow maintains everything.

## Delegation map

| Task class | Model |
|---|---|
| Log/row-count extraction, stat-type mapping verification, registry/inventory mechanical adds, manifest row edit, index SQL | haiku |
| Deploy infra (workflow + driver + presence script), WEPA marts/views, capability marts/views, RPC garbage-time edits, snapshot loader, `012`/game-log edits, all tests, docs/contract edits | sonnet |
| Player-EPA attribution SQL, havoc source reconciliation, garbage-time drift-guard design, deploy sequencing review, final contract review | opus |
| Orchestration, integration, commits, deploy-branch pushes, log reading, final PR | main loop |

## Commit breakdown

1. `Add push-triggered schema deploy workflow` (+ driver + check_presence + tests)
2. `Backfill stats and betting gate tables` (deploy dispatch only, if needed)
3. `Centralize garbage-time rule with drift guard`
4. `Add garbage-time filter to split RPCs`
5. `Add team and player WEPA marts and api views`
6. `Surface WEPA on player detail RPC`
7. `Rebuild player EPA on play_stats athlete_id`
8. `Key player season EPA and game log on athlete_id`
9. `Replace havoc heuristic with stats.game_havoc`
10. `Add returning-production, usage, ATS marts`
11. `Capture append-only betting line snapshots`
12. `Register new marts in refresh registries`
13. `Add mart existence and column tests`
14. `Correct game_player_stats manifest status`
15. `Update SCHEMA_CONTRACT for Tier 1 unlock`

## Out of scope

No new modeling or endpoints; no `betting.lines` schema change; no `api.team_detail`/`leaderboard_teams` churn; no intraday snapshots; no defensive-stats mart beyond de-heuristicking; no `analytics.*` changes; `supabase gen types` regen is cfb-app's follow-up. Tier 2 (scored matchup_edges, house Elo, ridge-adjusted EPA, features/predictions schema) is a separate September sprint once 2026 games flow.
