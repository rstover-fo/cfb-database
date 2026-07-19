# CFB Database

College Football Data warehouse using dlt pipelines and Supabase Postgres.

## Setup

1. Copy `.dlt/secrets.toml.example` to `.dlt/secrets.toml` and add your credentials:
   - CFBD API key from https://collegefootballdata.com/key
   - Supabase Postgres connection string

2. Install dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

3. Provision the database:
   ```bash
   python scripts/run_migrations.py     # Core DDL (src/schemas/001-018)
   python scripts/run_marts.py          # Materialized view definitions
   python scripts/refresh_marts.py      # Populate/refresh the marts
   ```
   One-off SQL in `src/schemas/public/`, `api/`, or `functions/` is applied with:
   ```bash
   python scripts/run_migrations.py --file src/schemas/public/008_trajectory_averages_function.sql
   ```

4. Load data:
   ```bash
   python -m src.pipelines.run --source reference
   ```

## In-Season Operations

A GitHub Actions workflow (`.github/workflows/daily-load.yml`) runs every day at
10:00 UTC from `main`: it loads the current season, refreshes all marts, and runs
`scripts/verify_load.py`. Failures open/update a rolling GitHub issue.

Required repo secrets:

| Secret | Value |
|--------|-------|
| `CFBD_API_KEY` | collegefootballdata.com API key (exported as `SOURCES__CFBD__API_KEY`) |
| `SUPABASE_DB_URL` | Supabase **session pooler** connection string (IPv4-capable; the transaction pooler rejects the `statement_timeout` startup option the mart refresh needs) |

The same flow can be run manually:

```bash
python scripts/load_season.py                    # Load current season + refresh marts
python scripts/load_season.py --season 2026 --weekly   # Explicit season, week-by-week game stats
python scripts/load_season.py --dry-run          # Show the plan and API call estimate
python scripts/verify_load.py                    # Post-load checks (exits 1 on failure)
```

`--weekly` loads game box scores week-by-week (~35K rows per merge) to stay under
Supabase statement timeouts; the daily workflow always uses it.

## CLI Usage

```bash
# Load reference data (full refresh)
python -m src.pipelines.run --source reference

# Load current season games (incremental)
python -m src.pipelines.run --source games --mode incremental

# Backfill historical data
python -m src.pipelines.run --source games --mode backfill --years 2020 2021 2022

# Check pipeline status
python -m src.pipelines.run --status
```

## Architecture

- **Source:** CFBD API (see `docs/pipeline-manifest.md` for endpoint-to-table coverage)
- **ETL:** dlt pipelines with year-based iteration
- **Destination:** Supabase Postgres (10 schemas, ~35 tables + marts/api/public layers)

## Rate Limits

Using Tier 3 (75,000 calls/month), read by the rate limiter from
`.dlt/config.toml` (`sources.cfbd.monthly_budget`). A full single-season
refresh is ~730 estimated calls (`scripts/load_season.py --dry-run` prints the
current estimate), so even daily loads stay well under budget.
