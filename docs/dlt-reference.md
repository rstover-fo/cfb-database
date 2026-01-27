# dlt (dlthub) Reference for CFBD Pipeline

## Installation

```bash
pip install "dlt[postgres]"
```

## REST API Source Configuration

### Basic Structure

```python
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source

config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.collegefootballdata.com/",
        "auth": {
            "type": "bearer",
            "token": dlt.secrets["cfbd_api_key"],
        },
    },
    "resource_defaults": {
        "primary_key": "id",
        "write_disposition": "merge",
    },
    "resources": [
        # Simple endpoint (name = path)
        "conferences",
        "venues",

        # Configured endpoint
        {
            "name": "games",
            "endpoint": {
                "path": "games",
                "params": {
                    "year": 2024,
                    "seasonType": "regular",
                },
            },
        },
    ],
}
```

### Incremental Loading

For endpoints that support date/cursor-based filtering:

```python
{
    "name": "plays",
    "endpoint": {
        "path": "plays",
        "params": {
            "year": "{incremental.start_value}",
        },
        "incremental": {
            "cursor_path": "year",
            "initial_value": "2004",
        },
    },
}
```

### Pagination

dlt auto-detects common pagination patterns. For custom:

```python
"client": {
    "paginator": {
        "type": "page_number",
        "page_param": "page",
        "total_path": "response.total_pages",
    },
}
```

## Postgres Destination

### Connection (Supabase)

```python
# secrets.toml
[destination.postgres.credentials]
database = "postgres"
username = "postgres.xxxxx"
password = "<supabase-password>"
host = "db.xxxxx.supabase.co"
port = 5432

# Or connection string
pipeline = dlt.pipeline(
    destination=dlt.destinations.postgres(
        "postgresql://postgres.xxxxx:<password>@db.xxxxx.supabase.co:5432/postgres"
    ),
    dataset_name="cfbd"
)
```

### Write Dispositions

- `append` - Add new rows
- `replace` - Drop and recreate table
- `merge` - Upsert based on primary key (requires `primary_key`)

## Pipeline Structure for CFBD

```
src/pipelines/
├── __init__.py
├── config.py           # RESTAPIConfig definitions
├── sources/
│   ├── __init__.py
│   ├── reference.py    # teams, conferences, venues (full refresh)
│   ├── games.py        # games, drives, plays (incremental by year)
│   ├── stats.py        # player/team stats (incremental)
│   ├── recruiting.py   # recruiting data (incremental by year)
│   └── ratings.py      # SP+, Elo, FPI (incremental by year)
└── run.py              # Main pipeline orchestration
```

## Key Considerations for CFBD

1. **Rate Limits**: Free tier = 1,000 calls/month
   - Use incremental loading where possible
   - Cache reference data (teams, venues) - rarely changes

2. **No Native Pagination**: CFBD uses year-based filtering, not page-based
   - Will need to iterate years programmatically

3. **Nested Data**: dlt auto-unnests JSON, creates child tables
   - May need `processing_steps` for custom flattening

4. **Primary Keys**: Many CFBD endpoints lack natural PKs
   - May need composite keys (year + team_id + game_id)
   - Or generate surrogate keys
