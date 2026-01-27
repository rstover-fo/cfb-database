# CFB Database — Sprint Plan

> Reviewed by plan-reviewer. Incorporates feedback on migration safety, testing timing, dlt disposition conflicts, partitioning complexity, and Sprint 3 decomposition.

## Project Status

- **Infrastructure**: API client, rate limiter, CLI, 9 dlt sources — all built
- **Data loaded**: Reference data, games, drives, plays (2004-2024), stats, ratings, recruiting, betting, draft, metrics all backfilled
- **Schema**: Only `001_reference.sql` exists as managed SQL; dlt auto-created the rest
- **Tests**: None
- **Known bugs**: 5 primary key mismatches, 3 dead/unwired resources

---

## Sprint 0: Investigation & Baseline

**Goal**: Resolve all open questions, document current state, take a snapshot before any changes.

### Tasks

#### 0.1 Database snapshot
- [ ] `pg_dump` full Supabase database before any schema changes
- [ ] Store dump in a versioned location (local or cloud storage)
- [ ] Document current table sizes: `SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')`

#### 0.2 Resolve PK ambiguities via API inspection
- [ ] **transfer_portal**: Call `/recruiting/transfer-portal?year=2024` and inspect response — does it return `player_id`? Document actual field names
- [ ] **picks**: Call `/draft/picks?year=2024` and inspect — is `college_athlete_id` present? Or `overall`? Document
- [ ] **lines**: Call `/lines?gameId=<known_id>` — inspect nested structure, determine natural key
- [ ] **player_season_stats**: Call `/stats/player/season?year=2024` — is the field `stat_type` or `category`?
- [ ] Document findings in a `docs/api-field-audit.md`

#### 0.3 Audit dlt write dispositions
- [ ] For every source, document the write disposition (`replace`, `merge`, `append`)
- [ ] **Critical question**: Do any sources use `replace`? If so, they will DROP and RECREATE tables, destroying indexes/FKs/constraints added in Sprint 2
- [ ] If `replace` sources exist beyond reference data, convert to `merge` with proper PKs, or guard schema objects with a post-load migration script
- [ ] Document findings in `docs/write-dispositions.md`

#### 0.4 Audit current table state
- [ ] Query `information_schema.columns` for all `__v_double` variant columns — list table + column
- [ ] Check for duplicate rows caused by incorrect PKs (coaches, stats, recruiting, betting, draft)
- [ ] Document row counts per table
- [ ] Document existing indexes and constraints dlt created automatically

#### 0.5 Pipeline manifest
- [ ] Create `docs/pipeline-manifest.md` mapping each endpoint to: source file, resource function, write disposition, table name, PK, year range, last loaded timestamp
- [ ] This becomes the single source of truth for all pipeline metadata

**Validation**: All open questions resolved. Snapshot taken. No code changes in this sprint.

---

## Sprint 1: Fix Bugs & Stabilize Pipelines

**Goal**: Every existing pipeline runs correctly with proper PKs and no dead code. Tests written alongside fixes.

### Tasks

#### 1.1 Fix primary key mismatches (Task #1)

Each fix follows the pattern: write a failing test → fix the code → test passes → check for duplicate data in DB → deduplicate if needed.

- [ ] **coaches** (`endpoints.py:44`): Remove `"seasons"` from PK. Fix to `["first_name", "last_name"]`. Query DB for duplicates created by the old PK
- [ ] **player_season_stats** (`stats.py:71` vs `endpoints.py:99`): Align field name based on Sprint 0.2 findings. Update both config and code to match
- [ ] **transfer_portal** (`recruiting.py:101` vs `endpoints.py:170`): Use Sprint 0.2 findings. Update to correct PK
- [ ] **lines** (`betting.py:44` vs `endpoints.py:177`): Align based on Sprint 0.2 findings
- [ ] **picks** (`draft.py` vs `endpoints.py:192`): Align based on Sprint 0.2 findings
- [ ] **Deduplication**: For each fixed PK, run dedup queries to remove duplicate rows created by old incorrect PKs

**Validation**: Run each source with `--dry-run`. No dlt PK errors. No duplicate rows in DB.

#### 1.2 Wire dead/unwired resources (Tasks #1, #2)
- [ ] **game_media** (`games.py`): Add `game_media_resource(years)` to `games_source()` return list
- [ ] **advanced_team_stats** (`stats.py`): Add `advanced_team_stats_resource(years)` to `stats_source()` return list
- [ ] **ppa_games** (`metrics.py`): Wire `ppa_games_resource()` into `metrics_source()`
- [ ] **ppa_players_games** (`metrics.py`): Wire into `metrics_source()`
- [ ] **win_probability** (`metrics.py`): Wire in-game win probability into `metrics_source()`

**Validation**: `python -m src.pipelines.run --source all --dry-run` succeeds.

#### 1.3 Initial test infrastructure (Task #9 — partial)

Start testing now, not in Sprint 4. Write tests alongside bug fixes.

- [ ] Create `tests/conftest.py` with shared fixtures
- [ ] `tests/test_endpoints_config.py` — validate all EndpointConfig entries: required fields, valid PKs, no JSONB in PKs, config-to-code PK alignment
- [ ] `tests/test_rate_limiter.py` — budget tracking, monthly reset, state persistence
- [ ] `tests/test_api_client.py` — auth headers, retry logic, 429 handling (mock httpx)
- [ ] `tests/test_years.py` — year range generation, current season logic

**Validation**: `pytest` passes. Config validation test catches the exact PK bugs we just fixed (regression tests).

#### 1.4 Add `--validate` CLI command
- [ ] New CLI subcommand that checks: all configured endpoints have wired resources, all PKs match between config and code, no orphan variant columns
- [ ] Runs without DB connection (code-level validation only)

**Validation**: `python -m src.pipelines.run --validate` reports clean.

---

## Sprint 2: Schema Hardening

**Goal**: Managed schema definitions with indexes and constraints. Partitioning deferred to Sprint 2B.

**Prerequisite**: Sprint 0.3 (write disposition audit) must confirm that `merge` is used for all non-reference sources, so schema objects survive pipeline runs.

### Tasks

#### 2.1 Create schema files — column definitions and simple indexes (Tasks #3, #4, #5, #6)

These are ALTER-based migration scripts that work against dlt's existing auto-created tables, NOT CREATE TABLE scripts for a blank DB. Each script:
1. Adds missing columns / fixes types if needed
2. Adds single-column B-tree indexes
3. Adds `updated_at` trigger if missing

- [ ] **002_core.sql** — `core` schema: games, drives, plays, game_media
- [ ] **003_stats.sql** — `stats` schema: team_season_stats, player_season_stats, game_team_stats, game_player_stats, advanced_team_stats
- [ ] **004_ratings.sql** — `ratings` schema: sp_ratings, elo_ratings, fpi_ratings, srs_ratings
- [ ] **005_recruiting.sql** — `recruiting` schema: recruits, team_recruiting, transfer_portal
- [ ] **006_betting.sql** — `betting` schema: lines
- [ ] **007_draft.sql** — `draft` schema: picks
- [ ] **008_metrics.sql** — `metrics` schema: ppa_teams, ppa_players_season, ppa_games, ppa_players_games, pregame_win_probability, win_probability

**Note**: Schema files should use `IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` to be idempotent.

**Validation**: All `.sql` files run cleanly against existing Supabase tables. Run twice — second run is a no-op.

#### 2.2 Add business indexes (Task #11)
- [ ] B-tree indexes on: `season`, `team`, `game_id`, `player_id`, `conference`, `week`, `game_date`, `position`
- [ ] All tables in core, stats, ratings, recruiting, betting, draft, metrics schemas

#### 2.3 Add composite indexes (Task #12)
- [ ] `(season, team)` on games, stats, ratings
- [ ] `(season, week)` on games, plays
- [ ] `(game_id, team)` on drives, game stats
- [ ] `(game_id, drive_id)` on plays
- [ ] `(season, position)` on recruiting
- [ ] `(team, season, category)` on player stats

#### 2.4 Add BRIN index (Task #13)
- [ ] BRIN on `core.plays(season)` — physically ordered by season insertion
- [ ] Evaluate BRIN on `core.games(start_date)` as well

**Note**: If plays table is partitioned later (Sprint 2B), BRIN applies within each partition. Still useful.

#### 2.5 Consolidate variant columns (Task #14)

Moved from Sprint 1 to Sprint 2 per review — this is a schema concern, not a bug fix. Depends on understanding the final target schema.

- [ ] Use Sprint 0.4 findings to identify all `__v_double` columns
- [ ] For each: determine correct type from API response
- [ ] Write migration script: `UPDATE table SET correct_col = __v_double_col::correct_type WHERE __v_double_col IS NOT NULL`
- [ ] Drop variant columns after migration
- [ ] Enable dlt `schema_contract` to prevent future type inference drift

**Validation**: `SELECT column_name FROM information_schema.columns WHERE column_name LIKE '%__v_%'` returns zero rows.

#### 2.6 Run ANALYZE (Task #16)
- [ ] `ANALYZE` all tables after index creation
- [ ] `EXPLAIN ANALYZE` on representative queries to verify index usage

**Validation**: Index scans (not seq scans) on common query patterns.

---

## Sprint 2B: Partitioning & Foreign Keys

**Goal**: Partition the plays table and add FK constraints. Separated from Sprint 2 because partitioning existing tables is a major migration.

### Tasks

#### 2B.1 Partition plays table by season (part of Task #3)

This is a multi-step migration on a 5-10M row table:

- [ ] Create new partitioned table: `CREATE TABLE core.plays_partitioned (...) PARTITION BY LIST (season)`
- [ ] Create partitions for each season (2004-2025 = 22 partitions): `CREATE TABLE core.plays_y2004 PARTITION OF core.plays_partitioned FOR VALUES IN (2004)` etc.
- [ ] Copy data: `INSERT INTO core.plays_partitioned SELECT * FROM core.plays`
- [ ] Verify row counts match
- [ ] Swap tables: `ALTER TABLE core.plays RENAME TO plays_old; ALTER TABLE core.plays_partitioned RENAME TO plays;`
- [ ] Recreate indexes on partitioned table (indexes must be created per-partition or on the parent)
- [ ] Drop `plays_old` after verification
- [ ] Test dlt loads against new partitioned table — confirm `merge` still works

**Risk**: dlt may not handle partitioned tables transparently. Test before dropping old table.

**Validation**: `SELECT COUNT(*) FROM core.plays` matches pre-migration count. `EXPLAIN ANALYZE` shows partition pruning. dlt test load succeeds.

#### 2B.2 Add foreign key constraints (Task #15)

Deferred until after partitioning because FKs on partitioned tables have Postgres-specific behavior (each partition needs its own FK).

- [ ] `core.games` → `ref.teams` (home_team_id, away_team_id)
- [ ] `core.games` → `ref.venues` (venue_id)
- [ ] `core.drives` → `core.games` (game_id)
- [ ] `core.plays` → `core.games` (game_id) — on partitioned table
- [ ] `stats.*` → `core.games`, `ref.teams`
- [ ] `ratings.*` → `ref.teams`
- [ ] `betting.lines` → `core.games`
- [ ] `recruiting.*` → `ref.teams`

**Note**: Verify dlt-created PKs are compatible. FKs require UNIQUE or PK on referenced columns. May need to add constraints on reference tables first.

**Validation**: All FK constraints created without errors. Test a pipeline load to ensure FKs don't block inserts.

---

## Sprint 3: Expand Endpoint Coverage

**Goal**: All 61 CFBD API endpoints configured, sourced, and loadable.

**Prerequisite**: Calculate expected API call count for full historical backfill of new endpoints. Must fit within 75k/month budget.

### Tasks

#### 3.1 Priority Tier 1 — Analytics-critical endpoints (Task #8)

These feed the Sprint 5 materialized views:

- [ ] `/rankings` — poll rankings by week (feeds conference standings view)
- [ ] `/roster` — team rosters by season (feeds player career view)
- [ ] `/stats/season/advanced` — advanced team stats (feeds team summary view)
- [ ] `/game/box/advanced` — advanced box scores (feeds game results view)
- [ ] `/teams/talent` — team talent composite (feeds recruiting trend view)
- [ ] `/recruiting/groups` — recruiting position groups

**Source files**: Create `rankings.py`, extend `stats.py`, `games.py`, `teams_extended.py`, `recruiting.py`

#### 3.2 Priority Tier 2 — High-value endpoints (Task #8)

- [ ] `/player/search` — player lookup
- [ ] `/player/usage` — player usage rates
- [ ] `/player/returning` — returning production
- [ ] `/teams/fbs` — FBS team list (reference-like)
- [ ] `/teams/matchup` — historical matchup records
- [ ] `/records` — team season records
- [ ] `/games/weather` — game weather data
- [ ] `/draft/positions`, `/draft/teams` — draft reference data

**Source files**: Create `players.py`, extend `teams_extended.py`, `games.py`, `draft.py`

#### 3.3 Priority Tier 3 — Nice-to-have endpoints (Task #8)

- [ ] `/wepa/*` (4 endpoints) — opponent-adjusted metrics
- [ ] `/ppa/predicted` — predicted PPA
- [ ] `/metrics/fg/ep` — field goal expected points
- [ ] `/stats/game/advanced`, `/stats/game/havoc` — game-level advanced stats
- [ ] `/stats/categories` — stat type reference
- [ ] `/plays/stats`, `/plays/stats/types` — play stat types
- [ ] `/live/plays` — live play feed (real-time only, no backfill)
- [ ] `/calendar` — season calendar
- [ ] `/scoreboard` — live scoreboard (real-time only)
- [ ] `/teams/ats` — against the spread records
- [ ] `/ratings/sp/conferences` — conference SP+ ratings

**Source files**: Create `adjusted_metrics.py`, extend `metrics.py`, `stats.py`, `plays.py`

#### 3.4 Register new sources in CLI (Task #7)
- [ ] Update `run.py` to include new source categories
- [ ] Update `--source all` to include new sources

**Validation**: `--dry-run` works for all sources. Load one year of each new Tier 1 endpoint to Supabase.

---

## Sprint 4: Remaining Tests & CI

**Goal**: Full test coverage across all sources, integration tests, and CI pipeline.

### Tasks

#### 4.1 Source integration tests (Task #9)
- [ ] `tests/test_sources/test_reference.py` — mock API, verify yield count and schema
- [ ] `tests/test_sources/test_games.py` — year iteration, media resource inclusion
- [ ] `tests/test_sources/test_plays.py` — week iteration, postseason handling
- [ ] `tests/test_sources/test_stats.py` — category iteration, PK correctness
- [ ] One test per source file covering happy path and edge cases
- [ ] Tests for all new Sprint 3 sources

#### 4.2 CLI tests (Task #9)
- [ ] `tests/test_cli.py` — `--dry-run`, `--status`, `--validate`, `--source` validation, `--years` parsing

#### 4.3 Pipeline health monitoring
- [ ] Add structured logging (JSON) for pipeline runs: source, records loaded, errors, duration
- [ ] Create a `pipeline_runs` table in Supabase to log each execution
- [ ] Add `--source <name> --status` to report last run, record counts, errors

**Validation**: `pytest` passes with 0 failures. 80%+ coverage on utilities, 60%+ on sources.

---

## Sprint 5: Analytics Materialized Views

**Goal**: Pre-computed views for common analytics queries.

### Tasks

#### 5.1 Design materialized views (Task #17)
- [ ] `analytics.team_season_summary` — W-L, points for/against, margin, conference record
- [ ] `analytics.player_career_stats` — aggregated across seasons
- [ ] `analytics.conference_standings` — by season, with tiebreakers
- [ ] `analytics.team_recruiting_trend` — multi-year recruiting class comparison
- [ ] `analytics.game_results` — denormalized game results with team names, scores, spreads, ratings

#### 5.2 Implement views (Task #17)
- [ ] Create `src/schemas/009_analytics.sql`
- [ ] Each view must have a UNIQUE INDEX for `REFRESH MATERIALIZED VIEW CONCURRENTLY`
- [ ] Add refresh script: `scripts/refresh_views.py` (or SQL function)
- [ ] Define refresh strategy: after pipeline loads, or on a schedule
- [ ] Add indexes on materialized view columns used in queries

**Validation**: Views return correct data for spot-checked seasons. Concurrent refresh completes without error.

---

## Dependency Graph

```
Sprint 0 (Investigation)
  └── Resolve all open questions, snapshot DB

Sprint 1 (Stabilize) — requires Sprint 0
  ├── #1  Fix PK mismatches + dedup
  ├── #2  Wire dead resources (after #1)
  ├── #9  Initial tests (alongside fixes)
  └── --validate CLI command

Sprint 2 (Schema Hardening) — requires Sprint 1
  ├── #3-#6   Schema files (ALTER-based, idempotent)
  ├── #11-#13 Indexes (business, composite, BRIN)
  ├── #14     Variant column consolidation
  └── #16     ANALYZE (after indexes)

Sprint 2B (Partitioning & FKs) — requires Sprint 2
  ├── Partition plays table (major migration)
  └── #15 Foreign key constraints (after partitioning)

Sprint 3 (Expand Coverage) — can start parallel to Sprint 2
  ├── #8  Endpoint configs (Tier 1 → 2 → 3)
  └── #7  Source files (after #8)

Sprint 4 (Remaining Tests) — after Sprints 1-3
  └── #9  Integration tests, CLI tests, monitoring

Sprint 5 (Analytics) — requires Sprint 2B
  └── #17 Materialized views
```

---

## Key Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| dlt `replace` disposition destroys indexes/FKs | Sprint 2 work wasted | Sprint 0.3 audit; convert all non-reference sources to `merge` |
| Partitioning breaks dlt loads | Can't load new data | Test dlt against partitioned table before dropping old table |
| Incorrect PKs caused duplicate data in backfill | Bad analytics | Sprint 1.1 includes dedup step; snapshot before changes |
| 31 new endpoints exceed API budget | Can't backfill | Calculate call count before Sprint 3; prioritize by analytics value |
| Schema files conflict with dlt auto-schema | Migration errors | Use ALTER-based idempotent scripts, not CREATE TABLE |
| No migration tooling | Manual SQL file management | Accept for now; evaluate Alembic/Flyway if schema files exceed 10 |

---

## Open Questions (Resolve in Sprint 0)

1. **dlt write dispositions**: Which sources use `replace` vs `merge`? This determines whether Sprint 2 schema work survives.
2. **Supabase tier**: Current plan? Storage limits? Connection pooling config?
3. **CI/CD**: Are loads run manually or scheduled? Affects monitoring strategy.
4. **Incremental load watermarks**: How does dlt track what's already loaded? `dlt_state`? Last-loaded year?
5. **Frontend dependency**: Is the CFB web app (expressive-painting-ocean tasks) consuming this DB directly? If so, schema changes need coordination.

---

## Constraints & Notes

- **API Budget**: 75,000 calls/month (Tier 3). Current backfill ~600 calls. Headroom for expansion.
- **Plays table**: Largest table (5-10M rows). Partitioning is a separate sprint (2B).
- **dlt auto-schema**: dlt creates tables on first load. Schema files are ALTER-based hardening, not initial creation.
- **Backfill complete**: Through 2024 for all existing sources. Schema work is about hardening, not initial load.
- **Snapshot required**: `pg_dump` before any Sprint 1 changes. Non-negotiable.
