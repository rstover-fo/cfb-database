# dlt Write Dispositions Audit

> Sprint 0.3 — Audited from source code

## Summary

**All non-reference sources use `merge`.** This means Sprint 2 schema hardening (indexes, constraints) will survive pipeline runs. No changes needed.

| Source File | Resource | Write Disposition | Safe for Schema Hardening? |
|---|---|---|---|
| reference.py | conferences | `replace` | Yes (reference data, full refresh expected) |
| reference.py | teams | `replace` | Yes (reference data) |
| reference.py | venues | `replace` | Yes (reference data) |
| reference.py | coaches | `replace` | Yes (reference data) |
| reference.py | play_types | `replace` | Yes (reference data) |
| games.py | games | `merge` | Yes |
| games.py | drives | `merge` | Yes |
| games.py | game_media | `merge` | Yes (unwired — Task #2) |
| plays.py | plays | `merge` | Yes |
| stats.py | team_season_stats | `merge` | Yes |
| stats.py | player_season_stats | `merge` | Yes |
| stats.py | advanced_team_stats | `merge` | Yes (unwired — Task #2) |
| ratings.py | sp_ratings | `merge` | Yes |
| ratings.py | elo_ratings | `merge` | Yes |
| ratings.py | fpi_ratings | `merge` | Yes |
| ratings.py | srs_ratings | `merge` | Yes |
| recruiting.py | recruits | `merge` | Yes |
| recruiting.py | team_recruiting | `merge` | Yes |
| recruiting.py | transfer_portal | `merge` | Yes |
| betting.py | lines | `merge` | Yes |
| draft.py | draft_picks | `merge` | Yes |
| metrics.py | ppa_teams | `merge` | Yes |
| metrics.py | ppa_players_season | `merge` | Yes |
| metrics.py | pregame_win_probability | `merge` | Yes |

## Primary Key Mismatches

| Resource | PK in Code | PK in Config | Match? |
|---|---|---|---|
| coaches | `["first_name", "last_name"]` | `["first_name", "last_name", "seasons"]` | **NO** — config includes JSONB array |
| player_season_stats | `["player_id", "season", "category"]` | `["player_id", "season", "stat_type"]` | **NO** — field name mismatch |
| transfer_portal | `["season", "first_name", "last_name"]` | `["player_id", "season"]` | **NO** — different fields entirely |
| lines | `["game_id", "provider"]` | `["id"]` | **NO** — composite vs single key |
| draft_picks | `["year", "overall"]` | `["college_athlete_id", "year"]` | **NO** — different fields |

## Risk Assessment

- **Reference sources (`replace`)**: Will drop and recreate tables on each run. Any indexes or constraints on `ref.*` tables must be in the schema SQL files and re-applied after loads, OR we need to convert reference sources to `merge` and add PKs.
- **All other sources (`merge`)**: Safe. dlt will upsert using the primary key. Indexes and constraints survive.
- **Recommendation**: Consider converting reference sources from `replace` to `merge` to preserve indexes. Reference data changes rarely, so `merge` with proper PKs is more resilient.

## Note on dlt `replace` Behavior

dlt `replace` drops the table and recreates it. This means:
- Indexes on `ref.conferences`, `ref.teams`, `ref.venues`, `ref.coaches`, `ref.play_types` will be destroyed on each reference load
- `001_reference.sql` must be re-run after each reference load, OR convert to `merge`
- Foreign keys from other tables pointing to ref tables will also be dropped

**This is the #1 risk for Sprint 2.** Must decide: re-run schema after reference loads, or convert reference to `merge`.
