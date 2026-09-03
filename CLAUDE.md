# CFB Database Project

## Project Overview

College Football Database -- a complete data warehouse for the CFBD (College Football Data) API, powered by Supabase Postgres and dlthub pipelines.

**Goals:**
- Ingest all 79 CFBD API endpoints into a well-designed Postgres schema
- Support both analytics (read-heavy, denormalized) and application (normalized, transactional) use cases
- Maintain working pipelines for ongoing 2026 season data
- Full historical data (no storage constraints -- will upgrade Supabase tier if needed)

## Related Projects

This is the schema source of truth for a three-repo college football platform:

| Repo | Role | Relationship |
|------|------|-------------|
| **cfb-app** | Next.js analytics dashboard | Reads from `public`, `core`, `core_staging` schemas |
| **cfb-scout** | FastAPI scouting agent | Reads from `core.roster`, `recruiting.recruits`; owns `scouting` schema |

Schema contract for downstream consumers: `docs/SCHEMA_CONTRACT.md`

**Warning:** Schema changes to `core`, `recruiting`, or `public` schemas can break cfb-app and cfb-scout. Check `SCHEMA_CONTRACT.md` before modifying.

## Tech Stack

| Component | Technology |
|-----------|------------|
| Database | Supabase Postgres |
| Pipelines | dlt (dlthub) with REST API source |
| API Source | CFBD API (collegefootballdata.com) |
| Language | Python 3.11+ |

## Key Constraints

- **125,000 API calls/month (Tier 4)** -- use incremental loading, cache reference data
- **No native pagination** -- CFBD uses year-based filtering, iterate programmatically
- **Storage** -- no hard limit; will upgrade Supabase Pro ($25/mo, 8GB) if needed

## Schema Architecture

The database uses multiple Postgres schemas organized by data domain:

| Schema | Purpose | Examples |
|--------|---------|---------|
| `ref` | Reference/lookup data | teams, venues, conferences |
| `core` | Normalized game data | roster, games, game_team_stats, line scores |
| `stats` | Player/team statistics | player_stats, team_stats, passing_plays, rushing_plays |
| `ratings` | Rankings and ratings | SP+, Elo, FPI, SRS |
| `recruiting` | Recruiting data | recruits, team_recruiting |
| `betting` | Betting lines | lines, spreads |
| `draft` | NFL draft data | picks, positions |
| `metrics` | Advanced metrics | PPA, win probability |
| `analytics` | Computed analytics | EPA, style profiles |
| `features` | Fitted-model substrate | team_week (as-of feature vector), model_coefficients, model_metadata |
| `live` | In-game polling | scoreboard_snapshots, wp_params (house live win prob) |
| `marts` | Materialized views (49) | Denormalized, query-optimized |
| `api` | API view layer (52) | Contract surface for cfb-app/cfb-scout |
| `predictions` | Prediction snapshots | game_predictions, season_projections (append-only daily) |
| `public` | Convenience views/RPCs (12) | Downstream consumer interface |
| `meta` | Flat-file load ledger | flat_file_loads |
| `raw` | Raw archived source files | availability_reports |

### Marts System

49 materialized views in the `marts` schema provide denormalized, query-optimized data (50 in the refresh registry counting the internal helper `marts._game_epa_calc`, plus the plain view `analytics.data_quality_dashboard`). Refresh via:
```bash
python scripts/refresh_marts.py        # Refresh all marts
```
Or use the `refresh_all_marts()` RPC.

### Daily Automation

`.github/workflows/daily-load.yml` runs daily at 10:00 UTC from `main`: loads the
current season (`scripts/load_season.py --weekly`, mart refresh included), refits house
Elo/adjusted EPA (including the as-of weekly EPA build), refits fitted_v1 when it is stale
(`train_model.py --refit-if-stale`, a no-op on all but one day a year) and writes the
model's upcoming scores, then runs post-load checks (`scripts/verify_load.py`). Failures
open/update a rolling GitHub issue.

- `verify_load.py` also runs the KTD7 variant-twin tripwire (`check_variant_twins`,
  backed by `src/pipelines/utils/variant_twins.py`): dlt sometimes splits a
  charting metric into a bigint base column plus a `<col>__v_double` twin, and
  every mart reading `stats.rushing_*`/`stats.passing_player_season` COALESCEs
  only the twins that existed when it was authored. A daily load that creates a
  NEW twin now FAILs the run naming the column instead of silently going NULL
  in the mart/api view/RPC until someone notices.

**Finished-season skip:** on the unattended path (no `--season`, no `--sources`)
`load_season.py` skips sources whose data cannot change once a season is complete
(`IMMUTABLE_ONCE_FINAL` -- plays, game_stats, ratings, recruiting, draft, ...).
Before this, every off-season run re-ingested the entire finished previous season
-- `get_current_season()` returns `year - 1` until August -- roughly 2,000 calls a
day against the then-75,000/month budget for immutable data. That is what exhausted
the quota behind the 2026-07-25 three-hour rate-limited run. `reference` and
`metrics_wp` are never skipped (cheap / already self-limiting), and the
upcoming-schedule refresh is unaffected. An explicit `--season` or `--sources`
disables the skip entirely, so a backfill is never silently turned into a no-op;
`--no-skip-final` forces it off on the daily path.

**Where the per-game fan-out actually lives:** `stats`, not `plays`. `plays` is
year+week (16 calls/season). The `stats` source's `play_stats` resource issues one
`/plays/stats` call **per game** (~1,640/season) and `rosters` one per team, which
is the cost that exhausted the quota. `play_stats` now requests **completed games
only** -- an unplayed game has no play stats, and from 2026-08-01 (when
`get_current_season()` rolled to 2026, a season that is not final, so nothing was
skipped) the daily load walked all 1,638 *scheduled* 2026 games every day, was
429'd partway through, and failed the whole `stats` extract package -- discarding
`player_returning`'s already-fetched payload and burst-blocking `ratings` and
`game_stats` behind it. Note that failure mode: a resource that dies inside a
source takes every sibling resource's data with it. Because the source is not uniformly priced --
its other seven resources are one call per year -- anything running daily must name
resources rather than take the whole source: `--sources stats:player_returning`
(one call), and `PRESEASON_STATS_RESOURCES` in `load_season.py` for the automated
path.

**Upcoming-season preseason inputs:** off-season the daily run targets `year - 1`,
so the finished-season skip drops every immutable source for it -- correct, but it
left the *upcoming* season with no ingest beyond the games/betting schedule refresh.
Returning production, preseason SP+, talent and team recruiting are published
progressively through spring/summer and were never requested at all (2026 had a
schedule loaded since spring and zero rows in all four on 2026-07-28). The
upcoming-season block now also refreshes `PRESEASON_INPUT_SOURCES` (~11 calls/day);
an unpublished endpoint returns empty and merges nothing, so it self-heals as each
lands. `rosters` stays out (one call per team, and it firms up in August).
`scripts/probe_offseason_availability.py` distinguishes "never asked" from "CFBD
has not published it yet".

**Season targeting:** the compute chain's `--incremental` resolves target seasons from
`core.games` via `get_projection_seasons()` -- the most recent season with completed games
plus every later season with a published schedule -- **not** from `get_current_season()`,
which is a calendar rule returning `year - 1` until August and is correct only for ingest
year windows. `verify_load.py` asserts fitted_v1 covers >=90% of pending games so a missing
feature substrate cannot fail silently. Requires repo secrets `CFBD_API_KEY` and `SUPABASE_DB_URL` (session
pooler). `.github/workflows/flat-files.yml` runs daily at 11:00 UTC to load flat-file
sources (massey ratings, nflverse draft/combine, SBR lines, availability reports) using a
hash-skip ledger in `meta.flat_file_loads` to avoid re-processing unchanged files. Requires
repo secret `SUPABASE_DB_URL` only. `.github/workflows/live-scoreboard.yml` separately polls
CFBD's `/scoreboard` every 5 minutes on Saturdays (games-today guard) to feed
`live.scoreboard_snapshots` and the house live win-probability model.

### dlt Table Conventions

Tables loaded via dlt use parent-child relationships with `_dlt_id`, `_dlt_parent_id`, `_dlt_list_idx` columns for nested data traversal.

## Project Structure

```
cfb-database/
├── CLAUDE.md
├── pyproject.toml                # Dependencies, CLI entry point, ruff/pytest config
├── docs/
│   ├── SCHEMA_CONTRACT.md        # API surface contract for downstream repos
│   ├── pipeline-manifest.md      # Endpoint-to-pipeline status (source of truth)
│   ├── cfbd-api-endpoints.md     # Complete CFBD API reference
│   ├── dlt-reference.md          # Pipeline configuration patterns
│   ├── plans/                    # Dated sprint/implementation plans (+ archive/)
│   ├── handoffs/                 # Cross-repo handoff docs
│   ├── brainstorms/              # Design exploration docs
│   └── solutions/                # Solved problem documentation
├── src/
│   ├── pipelines/
│   │   ├── config/               # RESTAPIConfig definitions
│   │   │   ├── endpoints.py
│   │   │   └── years.py
│   │   ├── sources/              # 19 endpoint-specific source modules (incl. playoffs.py,
│   │   │   │                     # coaches.py, conferences.py, player_overview.py, passing.py, rushing.py) + flat-file sources
│   │   │   ├── flat_files.py      # Flat-file source registry and orchestration
│   │   │   └── flatfile_parsers/  # Parsers for CSV, parquet, PDF flat-file formats
│   │   ├── utils/
│   │   │   ├── api_client.py     # Custom CFBD HTTP client (httpx)
│   │   │   └── rate_limiter.py   # Monthly budget tracking
│   │   └── run.py                # Pipeline orchestration
│   └── schemas/
│       ├── api/                  # 52 API view definitions (contract surface)
│       ├── functions/            # SQL functions
│       ├── marts/                # 49 materialized view definitions (+ _game_epa_calc helper, +1 plain view)
│       ├── public/               # 12 convenience views + RPCs
│       └── migrations/           # Schema migrations
├── scripts/
│   ├── load_season.py            # Orchestrate full season load + mart refresh
│   ├── verify_load.py            # Post-load verification checks
│   ├── refresh_marts.py          # Mart refresh script
│   ├── run_marts.py              # Apply mart definitions
│   ├── run_migrations.py         # Migration runner (--file for one-off SQL)
│   ├── compute_house_elo.py      # Compute house Elo ratings from game history
│   ├── compute_adjusted_epa.py   # Compute team adjusted EPA ratings
│   ├── compute_adjusted_epa_week.py # Walk-forward as-of weekly adjusted EPA (--incremental)
│   ├── compute_predictions.py    # Generate game predictions and edges
│   ├── simulate_season.py        # Monte Carlo season win totals + distributions
│   ├── screen_preseason_features.py # Partial-correlation screen for candidate features
│   ├── check_backtest.py         # Backtest prediction accuracy and scoring
│   ├── build_features.py         # Build features.team_week substrate (--incremental)
│   ├── train_model.py            # Fit fitted_v1 walk-forward coefficients (--refit-if-stale)
│   ├── tune_params.py            # Hyperparameter search for fitted_v1
│   ├── score_fitted.py           # Score games with fitted_v1 (--upcoming default)
│   ├── probe_offseason_availability.py # Report which CFBD offseason inputs a season has
│   ├── calibrate_live_wp.py      # Fit live.wp_params sigma against historical win prob
│   ├── poll_scoreboard.py        # Poll CFBD /scoreboard, write live.scoreboard_snapshots
│   ├── load_flat_files.py        # Load flat-file sources (massey, nflverse, SBR, availability)
│   └── seed_team_xwalk.py        # Generate team name crosswalk reference data
├── tests/                        # Test files + test_sources/
│   └── conftest.py               # DB connection + mock fixtures
├── .dlt/
│   ├── config.toml               # Runtime config (workers, rate limit budget)
│   └── secrets.toml              # API keys (not committed)
├── .github/workflows/            # CI + daily season load + Saturday live-scoreboard polling
└── .githooks/pre-push            # Runs ruff + pytest before push
```

## Key Documentation

| File | Purpose |
|------|---------|
| `docs/SCHEMA_CONTRACT.md` | Defines the public API surface for cfb-app and cfb-scout |
| `docs/pipeline-manifest.md` | Single source of truth for endpoint-to-pipeline mappings with row counts and statuses |
| `docs/cfbd-api-endpoints.md` | Complete CFBD API reference |
| `docs/dlt-reference.md` | Pipeline configuration patterns |

## Skills & Guidelines

In-repo skills (`.claude/skills/`): `cfbd-api` (API conventions and traps), `schema-migrations` (migration workflow, grants/RLS lessons, refresh chains, column contracts), `dlt-pipelines` (source conventions, budget discipline, test patterns). In-repo agents (`.claude/agents/`): `pipeline-engineer` (builds sources), `modeling-scientist` (feature/model work with leak-free and pre-registration guardrails, plus output plausibility review), `schema-architect` (read-only schema-diff reviewer). Cloud and local sessions both see these; `~/.claude/skills/cfbd-api` is a symlink to the main checkout's copy for sibling-repo sessions.

### Postgres Best Practices (Supabase)
Location: `.claude/skills/supabase-postgres-best-practices/` (Supabase's official skill, vendored via `npx skills add supabase/agent-skills`; pinned in `skills-lock.json` — re-run the install to upgrade)

The skill carries the full rules catalog with examples and query-plan analysis. The floor that always applies when designing schema and writing queries:

**CRITICAL priority:**
- Add indexes on WHERE and JOIN columns
- Use connection pooling (Supabase provides this)
- Enable RLS for any user-facing tables

**HIGH priority:**
- Use `bigint generated always as identity` for primary keys
- Index all foreign key columns
- Choose appropriate data types (`timestamptz`, `text`, `numeric`)
- Use lowercase snake_case identifiers

**MEDIUM priority:**
- Batch INSERT statements for bulk loading
- Use cursor-based pagination, not OFFSET
- Use UPSERT (`ON CONFLICT`) for idempotent loads

**For large tables (plays, drives, player_stats):**
- Partition by season for plays table (millions of rows)
- Use BRIN indexes for time-series data (created_at, game_date)
- Use partial indexes for filtered queries (e.g., active season only)

### CFBD API Skill
Location: `.claude/skills/cfbd-api/` (canonical, in-repo — available to cloud/CI sessions). `~/.claude/skills/cfbd-api` is a symlink to the main checkout's copy so local cfb-app/cfb-scout sessions get it too.

Use this skill when building or debugging anything that calls the CFBD API:

- **Conventions**: auth (no double `Bearer` prefix), camelCase raw vs snake_case client params, dlt column renames (verify via `pg_attribute`), `team` full name vs `conference` abbreviation, postseason week restart
- **Volume traps**: no native pagination (never invent `page`/`offset`), `/plays/stats` ~2,000-record cap (per-gameId fan-out), estimate-before-loop discipline
- **Failure classification**: empty-200 vs 403 semantics, Cloudflare burst 429s vs quota exhaustion, when auto-retry is wrong (one-off scripts) vs correct (the pipeline's Retry-After + circuit-breaker pattern)
- **Data-shape traps**: nullable fields, v2 breaking changes, completed games with NULL scores, duplicate school names

The complete 84-endpoint inventory stays in `docs/cfbd-api-endpoints.md`; the skill carries conventions and traps, not the endpoint list.

### dlt REST API Reference
Location: `docs/dlt-reference.md`

Consult when building or modifying dlt pipelines: `RESTAPIConfig` structure, bearer auth via `dlt.secrets`, incremental placeholder syntax (`{incremental.start_value}`), write dispositions (`merge` is this repo's default), pagination notes, and `.dlt/config.toml` / `secrets.toml` patterns. CFBD has no native pagination, so most generic pagination machinery is irrelevant here — this repo's sources iterate year/week programmatically instead. dlt child-table gotchas (`_dlt_parent_id` LATERAL joins, column renames) are documented in `docs/solutions/database-issues/`.

## Model Delegation

Orchestrator sessions (Fable/Mythos-class) write task specs, sequence work,
review diffs, and integrate -- they do not implement. Implementation goes to
subagents: `pipeline-engineer` and `schema-architect` on Sonnet-class models,
`modeling-scientist` on Opus-class (its leak-free and pre-registration
guardrails warrant the extra care), and exploration/scoping on Haiku- or
Sonnet-class. Independent units run in parallel rather than sequentially,
keeping the orchestrator's own context free for review instead of
implementation.

## CFBD API Categories

| Category | Endpoints | Refresh Strategy |
|----------|-----------|------------------|
| Reference (teams, venues, conferences) | 10 | Full refresh (rarely changes) |
| Games & Schedules | 9 | Incremental by year |
| Plays & Drives | 6 | Incremental by year (largest data) |
| Stats (player/team) | 6 | Incremental by year |
| Ratings (SP+, Elo, FPI, CORE) | 6 | Incremental by year |
| Recruiting | 3 | Incremental by year |
| Betting Lines | 1 | Incremental by year |
| Metrics (PPA, win prob) | 8 | Incremental by year |
| Draft | 3 | Incremental by year |
| Passing (air yards, aDOT, depth/direction/location, YAC) | 5 | Incremental by year, data 2025+ |

## Commands

```bash
# Setup
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,compute,flatfiles]"

# Run pipelines
python -m src.pipelines.run --source reference           # Load reference data
python -m src.pipelines.run --source games --year 2024   # Load games for a year

# Season orchestration (what the daily workflow runs)
python scripts/load_season.py --weekly    # Load current season + refresh marts
python scripts/verify_load.py             # Post-load checks (exit 1 on failure)

# Marts
python scripts/refresh_marts.py     # Refresh all materialized views

# Migrations
python scripts/run_migrations.py    # Apply pending migrations
python scripts/run_migrations.py --file <path>  # Apply one-off public/api/functions SQL

# Flat-file sources
python scripts/load_flat_files.py --due              # Load all due sources (cadence-based)
python scripts/load_flat_files.py --source massey    # Load specific source

# Testing & linting
.venv/bin/ruff check .              # Lint
.venv/bin/ruff format --check .     # Format check
.venv/bin/pytest -q                 # Run tests
```

## Testing

- `tests/` contains 8 test files and a `test_sources/` subdirectory
- `conftest.py` provides DB connection fixtures and mock fixtures
- Integration tests hit live Supabase (`test_api_views.py`, `test_marts.py`, `test_player_analytics.py`)
- Unit tests for config, rate limiter, year ranges
- Pre-push hook (`.githooks/pre-push`) runs `ruff check`, `ruff format --check`, `pytest`

## Environment Variables

Credentials live in `.dlt/secrets.toml` (not `.env`). See `.dlt/secrets.toml.example`.

```bash
# .dlt/secrets.toml
[sources.cfbd]
api_key = ""                # From collegefootballdata.com/key

[destination.postgres]
credentials = ""            # Supabase direct connection string

# .env (non-secret config only)
SUPABASE_DB_URL=            # Direct Postgres connection string
```

## Git Conventions

- Branch names: `feature/`, `fix/`, `refactor/`, `chore/` prefixes
- Commit messages: imperative mood, 50-char subject line

## Data Availability Notes

Historical depth varies by endpoint:
- Games/scores: 1869+
- Play-by-play: ~2004+
- Recruiting: ~2000+
- Advanced metrics (SP+, PPA): ~2014+

**Load strategy:** Full history where available. Play-by-play (2004+) will be the largest table -- estimate 5-10M rows, partitioned by season.

## Configuration

- `pyproject.toml` -- project metadata, dependencies, CLI entry point (`cfb-pipeline`), ruff config (line-length 100, py311), pytest config
- `.dlt/config.toml` -- runtime config: worker count, file chunking, monthly API budget (125,000). Year ranges live in `src/pipelines/config/years.py`
- Supabase MCP server defined in `.mcp.json` (project-scoped, committed; token via `SUPABASE_ACCESS_TOKEN` env var — never committed) and enabled per-machine in `.claude/settings.local.json`
