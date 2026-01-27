# CFB Database Project

## Project Overview

College Football Database — a complete data warehouse for the CFBD (College Football Data) API, powered by Supabase Postgres and dlthub pipelines.

**Goals:**
- Ingest all 61 CFBD API endpoints into a well-designed Postgres schema
- Support both analytics (read-heavy, denormalized) and application (normalized, transactional) use cases
- Maintain working pipelines for ongoing 2026 season data
- Full historical data (no storage constraints — will upgrade Supabase tier if needed)

## Tech Stack

| Component | Technology |
|-----------|------------|
| Database | Supabase Postgres (free tier) |
| Pipelines | dlt (dlthub) with REST API source |
| API Source | CFBD API (collegefootballdata.com) |
| Language | Python 3.11+ |

## Key Constraints

- **1,000 API calls/month (free tier)** — use incremental loading, cache reference data
- **No native pagination** — CFBD uses year-based filtering, iterate programmatically
- **Storage** — no hard limit; will upgrade Supabase Pro ($25/mo, 8GB) if needed

## Skills & Guidelines

### Postgres Best Practices (Supabase)
Location: `~/.claude/skills/postgres-best-practices/`

Apply these guidelines when designing schema and writing queries:

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

## Project Structure

```
cfb-database/
├── CLAUDE.md              # This file
├── docs/
│   ├── cfbd-api-endpoints.md   # Complete API reference
│   ├── dlt-reference.md        # Pipeline configuration patterns
│   └── SPRINT_PLAN.md          # Implementation plan (TBD)
├── src/
│   ├── pipelines/              # dlt pipeline code
│   │   ├── config.py           # RESTAPIConfig definitions
│   │   ├── sources/            # Endpoint-specific sources
│   │   └── run.py              # Pipeline orchestration
│   └── schemas/                # SQL schema definitions
├── scripts/                    # Utility scripts
└── .env                        # API keys (not committed)
```

## CFBD API Categories

| Category | Endpoints | Refresh Strategy |
|----------|-----------|------------------|
| Reference (teams, venues, conferences) | 10 | Full refresh (rarely changes) |
| Games & Schedules | 9 | Incremental by year |
| Plays & Drives | 6 | Incremental by year (largest data) |
| Stats (player/team) | 6 | Incremental by year |
| Ratings (SP+, Elo, FPI) | 5 | Incremental by year |
| Recruiting | 3 | Incremental by year |
| Betting Lines | 1 | Incremental by year |
| Metrics (PPA, win prob) | 8 | Incremental by year |
| Draft | 3 | Incremental by year |

## Environment Variables

```bash
CFBD_API_KEY=           # From collegefootballdata.com/key
SUPABASE_URL=           # Project URL
SUPABASE_DB_URL=        # Direct Postgres connection string
```

## Commands

```bash
# Setup
python -m venv .venv && source .venv/bin/activate
pip install "dlt[postgres]"

# Run pipelines
python src/pipelines/run.py --source reference  # Load reference data
python src/pipelines/run.py --source games --year 2024  # Load games

# Schema
psql $SUPABASE_DB_URL -f src/schemas/001_reference.sql
```

## Data Availability Notes

Historical depth varies by endpoint:
- Games/scores: 1869+
- Play-by-play: ~2004+
- Recruiting: ~2000+
- Advanced metrics (SP+, PPA): ~2014+

**Load strategy:** Full history where available. Play-by-play (2004+) will be the largest table — estimate 5-10M rows, partitioned by season.
