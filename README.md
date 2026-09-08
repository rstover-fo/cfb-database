# CFB Database

College Football Data warehouse using dlt pipelines and Supabase Postgres.

## Setup

For agent/local/cloud development without warehouse credentials, run
`bash scripts/setup_dev.sh`. This installs the warehouse and MCP development
dependencies into `.venv`. See [agent setup](docs/agent-setup.md) for shared
instructions, cloud setup, and offline versus live test commands.

### Disposable warehouse development

The managed warehouse path requires PostgreSQL 17 with pgvector. It creates the
reviewed schema and static seeds without CFBD requests or production row data.
Use an explicitly selected empty database:

```bash
export WAREHOUSE_DB_URL='postgresql://postgres:local-password@127.0.0.1:55436/postgres'
.venv/bin/python scripts/bootstrap_warehouse.py bootstrap
.venv/bin/python scripts/bootstrap_warehouse.py status
```

`bootstrap` is repeatable: an unchanged manifest is a no-op. For a database
already managed by this manifest, inspect and apply forward changes with:

```bash
.venv/bin/python scripts/bootstrap_warehouse.py plan
.venv/bin/python scripts/bootstrap_warehouse.py upgrade
```

See [warehouse bootstrap](docs/warehouse-bootstrap.md) for the disposable Docker
command, prior-baseline upgrade exercise, fixture tests, and platform boundaries.
The managed CLI uses only `WAREHOUSE_DB_URL`; it does not discover production
credentials. Existing unledgered production databases require separately reviewed
catalog reconciliation and adoption. Do not point bootstrap at them.

### Ingestion and explicit operations

Mart replacement uses an explicit [dependency-aware release](docs/mart-releases.md).
Preview with `scripts/run_marts.py --release <manifest> --plan`, then apply the
same reviewed manifest. Real bare/number-selected mart deployments and raw mart
files through the per-file migration runner are rejected to preserve consumers.

Schema construction does not load game/roster data or certify warehouse coverage.
For an authorized ingestion target, configure `.dlt/secrets.toml` from its example
with a CFBD key and Supabase session-pooler connection, then run the selected
pipelines. Ingestion populates provider data before derived-data computation and
mart refresh. Secrets are unnecessary for installing dependencies or fixture tests.

Historical root migrations 001–018 are retained for incident/recovery reference;
they are not a clean-install recipe. Real replay requires `--legacy-history`.
Explicit reviewed SQL applications and diagnostics retain their existing path:

```bash
.venv/bin/python scripts/run_migrations.py --file path/to/reviewed.sql
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
- **Destination:** Supabase Postgres with domain tables and marts/api/public layers
  (see [warehouse operations](docs/warehouse-operations.md) and the schema contract)

## Rate Limits

The request budget comes from `.dlt/config.toml`
(`sources.cfbd.monthly_budget`). `scripts/load_season.py --dry-run` prints the
estimate for the selected sources. Per-game and per-team fanout can dominate
cost; check the requested scope and correction/reload policy before a backfill.
