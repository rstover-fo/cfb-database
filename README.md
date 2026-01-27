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

3. Run the schemas:
   ```bash
   psql $DATABASE_URL -f src/schemas/001_reference.sql
   # ... additional schema files
   ```

4. Load data:
   ```bash
   python -m src.pipelines.run --source reference
   ```

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

- **Source:** CFBD API (61 endpoints)
- **ETL:** dlt pipelines with year-based iteration
- **Destination:** Supabase Postgres (10 schemas, ~35 tables)

## Rate Limits

Using Tier 3 (75,000 calls/month). Full historical backfill requires ~1,100 calls.
