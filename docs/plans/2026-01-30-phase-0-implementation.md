# Phase 0 + 0.5 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Get cfb-app working in production, create missing database views, and add DuckDB-WASM exploration capability.

**Architecture:** Fix deployment env vars, create 2 missing materialized views (`team_style_profile`, `team_season_trajectory`) that the app expects, verify the `get_drive_patterns` function is deployed, then add client-side data exploration with DuckDB-WASM.

**Tech Stack:** Next.js 14, Supabase (Postgres), Vercel, DuckDB-WASM, Parquet

---

## Phase 0: Get App Working

### Task 1: Add Environment Variables to Vercel

**Files:**
- External: Vercel Dashboard → cfb-app project → Settings → Environment Variables

**Step 1: Open Vercel dashboard**

Navigate to: https://vercel.com → cfb-app project → Settings → Environment Variables

**Step 2: Add the two required variables**

| Name | Value | Environments |
|------|-------|--------------|
| `NEXT_PUBLIC_SUPABASE_URL` | `https://ibobsbwlewpqslkqbrjd.supabase.co` | Production, Preview, Development |
| `NEXT_PUBLIC_SUPABASE_ANON_KEY` | (copy from `.env.local`) | Production, Preview, Development |

**Step 3: Redeploy the application**

In Vercel dashboard: Deployments → most recent → "..." menu → Redeploy

**Step 4: Verify the app loads**

Open: https://v0-production-data-application-a6aa5qzvd.vercel.app/

Expected: Team list loads without "Invalid API key" error

**Step 5: Commit a note (no code changes)**

```bash
# No code changes needed — this was a Vercel config fix
```

---

### Task 2: Verify `get_drive_patterns` Function Exists in Supabase

**Files:**
- Reference: `cfb-database/src/schemas/functions/get_drive_patterns.sql`

**Step 1: Check if function exists in Supabase**

Run in Supabase SQL Editor (https://supabase.com/dashboard → cfb-database project → SQL Editor):

```sql
SELECT routine_name, routine_type
FROM information_schema.routines
WHERE routine_schema = 'public'
  AND routine_name = 'get_drive_patterns';
```

Expected: One row returned with `routine_name = 'get_drive_patterns'`

**Step 2: If missing, deploy the function**

If Step 1 returns 0 rows, run this in Supabase SQL Editor:

```sql
-- Aggregate drive data for visualization
-- Returns bucketed start/end positions with outcome counts
CREATE OR REPLACE FUNCTION get_drive_patterns(
  p_team TEXT,
  p_season INT
)
RETURNS TABLE (
  start_yard INT,
  end_yard INT,
  outcome TEXT,
  count BIGINT,
  avg_plays NUMERIC,
  avg_yards NUMERIC
) AS $$
BEGIN
  RETURN QUERY
  WITH drive_outcomes AS (
    SELECT
      (100 - d.start_yards_to_goal)::INT AS start_yard,
      LEAST(100, (100 - d.start_yards_to_goal + d.yards))::INT AS end_yard,
      CASE
        WHEN d.drive_result IN ('TD', 'TOUCHDOWN', 'Touchdown') THEN 'touchdown'
        WHEN d.drive_result IN ('FG', 'FIELD GOAL', 'FG GOOD', 'Field Goal') THEN 'field_goal'
        WHEN d.drive_result IN ('PUNT', 'Punt') THEN 'punt'
        WHEN d.drive_result IN ('INT', 'INTERCEPTION', 'FUMBLE', 'FUMBLE LOST', 'INT TD', 'FUMBLE TD', 'Interception', 'Fumble', 'Fumble Lost', 'Interception Return') THEN 'turnover'
        WHEN d.drive_result IN ('END OF HALF', 'END OF GAME', 'END OF 4TH QUARTER', 'End of Half', 'End of Game') THEN 'end_of_half'
        WHEN d.drive_result IN ('DOWNS', 'TURNOVER ON DOWNS', 'Downs', 'Turnover on Downs') THEN 'downs'
        ELSE 'other'
      END AS outcome,
      d.plays,
      d.yards
    FROM core.drives d
    WHERE d.offense = p_team
      AND d.season = p_season
      AND d.start_yards_to_goal IS NOT NULL
  )
  SELECT
    (FLOOR(drv.start_yard / 10.0) * 10)::INT AS start_yard,
    (FLOOR(drv.end_yard / 10.0) * 10)::INT AS end_yard,
    drv.outcome,
    COUNT(*)::BIGINT AS count,
    ROUND(AVG(drv.plays), 1) AS avg_plays,
    ROUND(AVG(drv.yards), 1) AS avg_yards
  FROM drive_outcomes drv
  WHERE drv.outcome != 'other'
  GROUP BY 1, 2, drv.outcome
  HAVING COUNT(*) >= 2
  ORDER BY drv.outcome, 1, 2;
END;
$$ LANGUAGE plpgsql STABLE;
```

**Step 3: Test the function**

```sql
SELECT * FROM get_drive_patterns('Alabama', 2024) LIMIT 5;
```

Expected: Rows with start_yard, end_yard, outcome, count, avg_plays, avg_yards

---

### Task 3: Create `team_style_profile` Materialized View

**Files:**
- Create: `cfb-database/src/schemas/marts/010_team_style_profile.sql`
- Deploy to: Supabase SQL Editor

**Step 1: Write the SQL file**

Create `cfb-database/src/schemas/marts/010_team_style_profile.sql`:

```sql
-- Team offensive/defensive identity profile
-- Provides run/pass tendencies, EPA by play type, and tempo classification
-- Depends on: core.plays, core.games

DROP MATERIALIZED VIEW IF EXISTS marts.team_style_profile CASCADE;

CREATE MATERIALIZED VIEW marts.team_style_profile AS
WITH play_aggregates AS (
    SELECT
        p.offense AS team,
        g.season,
        -- Play counts by type
        COUNT(*) FILTER (WHERE p.play_type IN ('Rush', 'Rushing Touchdown')) AS rush_plays,
        COUNT(*) FILTER (WHERE p.play_type IN ('Pass Reception', 'Pass Incompletion', 'Passing Touchdown', 'Pass Interception', 'Sack')) AS pass_plays,
        COUNT(*) AS total_plays,
        -- EPA by play type
        AVG(p.ppa) FILTER (WHERE p.play_type IN ('Rush', 'Rushing Touchdown')) AS epa_rushing,
        AVG(p.ppa) FILTER (WHERE p.play_type IN ('Pass Reception', 'Pass Incompletion', 'Passing Touchdown', 'Pass Interception', 'Sack')) AS epa_passing,
        -- Game count for tempo
        COUNT(DISTINCT p.game_id) AS games
    FROM core.plays p
    JOIN core.games g ON p.game_id = g.id
    WHERE p.ppa IS NOT NULL
    GROUP BY p.offense, g.season
),
defensive_aggregates AS (
    SELECT
        p.defense AS team,
        g.season,
        -- Defensive EPA allowed by play type
        AVG(p.ppa) FILTER (WHERE p.play_type IN ('Rush', 'Rushing Touchdown')) AS def_epa_vs_run,
        AVG(p.ppa) FILTER (WHERE p.play_type IN ('Pass Reception', 'Pass Incompletion', 'Passing Touchdown', 'Pass Interception', 'Sack')) AS def_epa_vs_pass
    FROM core.plays p
    JOIN core.games g ON p.game_id = g.id
    WHERE p.ppa IS NOT NULL
    GROUP BY p.defense, g.season
)
SELECT
    o.team,
    o.season,
    -- Run/pass rates
    ROUND((o.rush_plays::numeric / NULLIF(o.total_plays, 0)), 3) AS run_rate,
    ROUND((o.pass_plays::numeric / NULLIF(o.total_plays, 0)), 3) AS pass_rate,
    -- EPA by play type
    ROUND(o.epa_rushing::numeric, 4) AS epa_rushing,
    ROUND(o.epa_passing::numeric, 4) AS epa_passing,
    -- Tempo (plays per game)
    ROUND((o.total_plays::numeric / NULLIF(o.games, 0)), 1) AS plays_per_game,
    -- Tempo category
    CASE
        WHEN (o.total_plays::numeric / NULLIF(o.games, 0)) >= 75 THEN 'up_tempo'
        WHEN (o.total_plays::numeric / NULLIF(o.games, 0)) >= 65 THEN 'balanced'
        ELSE 'slow'
    END AS tempo_category,
    -- Offensive identity
    CASE
        WHEN (o.rush_plays::numeric / NULLIF(o.total_plays, 0)) >= 0.55 THEN 'run_heavy'
        WHEN (o.pass_plays::numeric / NULLIF(o.total_plays, 0)) >= 0.55 THEN 'pass_heavy'
        ELSE 'balanced'
    END AS offensive_identity,
    -- Defensive EPA (positive = bad for defense, negative = good)
    ROUND(d.def_epa_vs_run::numeric, 4) AS def_epa_vs_run,
    ROUND(d.def_epa_vs_pass::numeric, 4) AS def_epa_vs_pass
FROM play_aggregates o
LEFT JOIN defensive_aggregates d ON o.team = d.team AND o.season = d.season;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.team_style_profile (team, season);

-- Query indexes
CREATE INDEX ON marts.team_style_profile (season);
CREATE INDEX ON marts.team_style_profile (offensive_identity);
CREATE INDEX ON marts.team_style_profile (tempo_category);
```

**Step 2: Run test to verify SQL is valid**

Run in Supabase SQL Editor (dry run with EXPLAIN):

```sql
EXPLAIN
WITH play_aggregates AS (
    SELECT p.offense AS team, g.season, COUNT(*) AS total_plays
    FROM core.plays p
    JOIN core.games g ON p.game_id = g.id
    WHERE p.ppa IS NOT NULL
    GROUP BY p.offense, g.season
)
SELECT * FROM play_aggregates LIMIT 1;
```

Expected: Query plan output (no errors)

**Step 3: Deploy the materialized view**

Run the full SQL from Step 1 in Supabase SQL Editor.

Expected: `CREATE MATERIALIZED VIEW` success message

**Step 4: Verify data**

```sql
SELECT * FROM marts.team_style_profile WHERE team = 'Alabama' AND season = 2024;
```

Expected: One row with run_rate, pass_rate, epa_rushing, epa_passing, etc.

**Step 5: Create public schema view for Supabase API access**

```sql
-- Supabase auto-exposes public schema via REST API
CREATE OR REPLACE VIEW public.team_style_profile AS
SELECT * FROM marts.team_style_profile;
```

**Step 6: Commit the SQL file**

```bash
cd /Users/robstover/Development/personal/cfb-database
git add src/schemas/marts/010_team_style_profile.sql
git commit -m "feat: add team_style_profile materialized view"
```

---

### Task 4: Create `team_season_trajectory` Materialized View

**Files:**
- Create: `cfb-database/src/schemas/marts/011_team_season_trajectory.sql`
- Deploy to: Supabase SQL Editor

**Step 1: Write the SQL file**

Create `cfb-database/src/schemas/marts/011_team_season_trajectory.sql`:

```sql
-- Team historical trajectory for year-over-year comparison
-- Combines EPA, win %, recruiting rank, and era classification
-- Depends on: marts.team_epa_season, core.team_records, recruiting.team_recruiting

DROP MATERIALIZED VIEW IF EXISTS marts.team_season_trajectory CASCADE;

CREATE MATERIALIZED VIEW marts.team_season_trajectory AS
WITH era_definitions AS (
    SELECT
        season,
        CASE
            WHEN season BETWEEN 2004 AND 2013 THEN 'BCS'
            WHEN season BETWEEN 2014 AND 2023 THEN 'CFP_4'
            WHEN season >= 2024 THEN 'CFP_12'
        END AS era_code,
        CASE
            WHEN season BETWEEN 2004 AND 2013 THEN 'BCS Era'
            WHEN season BETWEEN 2014 AND 2023 THEN '4-Team Playoff'
            WHEN season >= 2024 THEN '12-Team Playoff'
        END AS era_name
    FROM generate_series(2004, 2026) AS season
),
team_records AS (
    SELECT
        team,
        year AS season,
        total_wins AS wins,
        total_games AS games,
        ROUND(total_wins::numeric / NULLIF(total_games, 0), 3) AS win_pct
    FROM core.team_records
),
recruiting_ranks AS (
    SELECT
        team,
        year AS season,
        rank AS recruiting_rank
    FROM recruiting.team_recruiting
)
SELECT
    epa.team,
    epa.season,
    -- EPA metrics
    epa.epa_per_play,
    epa.success_rate,
    -- Rankings (would need window function for actual rank, using tier for now)
    RANK() OVER (PARTITION BY epa.season ORDER BY epa.epa_per_play DESC) AS off_epa_rank,
    RANK() OVER (PARTITION BY epa.season ORDER BY epa.epa_per_play ASC) AS def_epa_rank,
    -- Win/loss record
    tr.win_pct,
    tr.wins,
    tr.games,
    -- Recruiting
    rr.recruiting_rank,
    -- Era
    e.era_code,
    e.era_name,
    -- Year-over-year delta
    LAG(epa.epa_per_play) OVER (PARTITION BY epa.team ORDER BY epa.season) AS prev_epa,
    ROUND(epa.epa_per_play - LAG(epa.epa_per_play) OVER (PARTITION BY epa.team ORDER BY epa.season), 4) AS epa_delta
FROM marts.team_epa_season epa
LEFT JOIN team_records tr ON epa.team = tr.team AND epa.season = tr.season
LEFT JOIN recruiting_ranks rr ON epa.team = rr.team AND epa.season = rr.season
JOIN era_definitions e ON epa.season = e.season;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.team_season_trajectory (team, season);

-- Query indexes
CREATE INDEX ON marts.team_season_trajectory (season);
CREATE INDEX ON marts.team_season_trajectory (team);
CREATE INDEX ON marts.team_season_trajectory (era_code);
```

**Step 2: Deploy the materialized view**

Run the full SQL in Supabase SQL Editor.

Expected: `CREATE MATERIALIZED VIEW` success message

**Step 3: Verify data**

```sql
SELECT * FROM marts.team_season_trajectory
WHERE team = 'Alabama'
ORDER BY season DESC
LIMIT 5;
```

Expected: Rows with epa_per_play, win_pct, recruiting_rank, era_code, epa_delta

**Step 4: Create public schema view for Supabase API access**

```sql
CREATE OR REPLACE VIEW public.team_season_trajectory AS
SELECT * FROM marts.team_season_trajectory;
```

**Step 5: Commit the SQL file**

```bash
cd /Users/robstover/Development/personal/cfb-database
git add src/schemas/marts/011_team_season_trajectory.sql
git commit -m "feat: add team_season_trajectory materialized view"
```

---

### Task 5: Create Public Views for Existing Marts

**Files:**
- Deploy to: Supabase SQL Editor

The app queries `public.team_season_epa` but the view is in `marts` schema. Create public views.

**Step 1: Create public views for all app-facing marts**

Run in Supabase SQL Editor:

```sql
-- Expose marts views to Supabase REST API via public schema
CREATE OR REPLACE VIEW public.team_season_epa AS
SELECT * FROM marts.team_epa_season;

-- Already created in Tasks 3 and 4:
-- public.team_style_profile
-- public.team_season_trajectory
```

**Step 2: Verify API access**

Test in app or via curl:

```bash
curl "https://ibobsbwlewpqslkqbrjd.supabase.co/rest/v1/team_season_epa?team=eq.Alabama&season=eq.2024" \
  -H "apikey: <your-anon-key>" \
  -H "Authorization: Bearer <your-anon-key>"
```

Expected: JSON array with EPA data

---

### Task 6: Verify App Works End-to-End

**Step 1: Open the deployed app**

Navigate to: https://v0-production-data-application-a6aa5qzvd.vercel.app/

Expected: Team list loads

**Step 2: Click on a team (e.g., Alabama)**

Expected: Team detail page loads with:
- Drive Patterns visualization
- Performance Metrics cards
- Style Profile section
- Historical Trajectory JSON

**Step 3: Check browser console for errors**

Open DevTools → Console

Expected: No Supabase errors, no missing data warnings

---

## Phase 0.5: DuckDB-WASM Exploration

### Task 7: Export Parquet Files from Supabase

**Files:**
- Create: `cfb-database/scripts/export_parquet.py`

**Step 1: Install dependencies**

```bash
cd /Users/robstover/Development/personal/cfb-database
pip install pyarrow pandas psycopg2-binary
```

**Step 2: Write the export script**

Create `cfb-database/scripts/export_parquet.py`:

```python
"""Export Supabase tables to Parquet files for DuckDB-WASM."""

import os
from pathlib import Path

import pandas as pd
import psycopg2

# Supabase connection (use service role for direct DB access)
DATABASE_URL = os.environ.get("SUPABASE_DB_URL")

EXPORT_DIR = Path("exports/parquet")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

TABLES_TO_EXPORT = [
    ("core.plays", "plays.parquet"),
    ("core.drives", "drives.parquet"),
    ("core.games", "games.parquet"),
    ("ref.teams", "teams.parquet"),
    ("marts.team_epa_season", "team_epa_season.parquet"),
    ("marts.team_style_profile", "team_style_profile.parquet"),
    ("marts.team_season_trajectory", "team_season_trajectory.parquet"),
]


def export_table(conn, table: str, filename: str) -> None:
    """Export a single table to Parquet."""
    print(f"Exporting {table}...")
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    output_path = EXPORT_DIR / filename
    df.to_parquet(output_path, index=False, compression="snappy")
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  → {filename}: {len(df):,} rows, {size_mb:.1f} MB")


def main() -> None:
    if not DATABASE_URL:
        print("Error: Set SUPABASE_DB_URL environment variable")
        print("Format: postgresql://postgres:<password>@<host>:5432/postgres")
        return

    conn = psycopg2.connect(DATABASE_URL)

    for table, filename in TABLES_TO_EXPORT:
        try:
            export_table(conn, table, filename)
        except Exception as e:
            print(f"  ✗ Failed: {e}")

    conn.close()
    print(f"\nExported to {EXPORT_DIR.absolute()}")


if __name__ == "__main__":
    main()
```

**Step 3: Run the export**

```bash
export SUPABASE_DB_URL="postgresql://postgres:<password>@db.ibobsbwlewpqslkqbrjd.supabase.co:5432/postgres"
python scripts/export_parquet.py
```

Expected: Parquet files created in `exports/parquet/`

**Step 4: Commit the script**

```bash
git add scripts/export_parquet.py
git commit -m "feat: add Parquet export script for DuckDB-WASM"
```

---

### Task 8: Add DuckDB-WASM to cfb-app

**Files:**
- Modify: `cfb-app/package.json`
- Create: `cfb-app/src/lib/duckdb/client.ts`
- Create: `cfb-app/src/lib/duckdb/queries.ts`

**Step 1: Install DuckDB-WASM**

```bash
cd /Users/robstover/Development/personal/cfb-app
npm install @duckdb/duckdb-wasm apache-arrow
```

**Step 2: Create DuckDB client wrapper**

Create `cfb-app/src/lib/duckdb/client.ts`:

```typescript
import * as duckdb from '@duckdb/duckdb-wasm'

let db: duckdb.AsyncDuckDB | null = null
let conn: duckdb.AsyncDuckDBConnection | null = null

const PARQUET_BASE_URL = process.env.NEXT_PUBLIC_PARQUET_URL || '/data'

export async function initDuckDB(): Promise<duckdb.AsyncDuckDBConnection> {
  if (conn) return conn

  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles()
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES)

  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
  )

  const worker = new Worker(worker_url)
  const logger = new duckdb.ConsoleLogger()
  db = new duckdb.AsyncDuckDB(logger, worker)
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker)

  conn = await db.connect()

  // Register Parquet files as tables
  await conn.query(`
    CREATE TABLE IF NOT EXISTS plays AS
    SELECT * FROM '${PARQUET_BASE_URL}/plays.parquet'
  `)
  await conn.query(`
    CREATE TABLE IF NOT EXISTS teams AS
    SELECT * FROM '${PARQUET_BASE_URL}/teams.parquet'
  `)

  URL.revokeObjectURL(worker_url)
  return conn
}

export async function query<T>(sql: string): Promise<T[]> {
  const connection = await initDuckDB()
  const result = await connection.query(sql)
  return result.toArray().map((row) => row.toJSON() as T)
}

export async function closeConnection(): Promise<void> {
  if (conn) {
    await conn.close()
    conn = null
  }
  if (db) {
    await db.terminate()
    db = null
  }
}
```

**Step 3: Create example queries**

Create `cfb-app/src/lib/duckdb/queries.ts`:

```typescript
import { query } from './client'

export interface PlayCount {
  team: string
  play_count: number
}

export async function getPlayCountsByTeam(season: number): Promise<PlayCount[]> {
  return query<PlayCount>(`
    SELECT offense as team, COUNT(*) as play_count
    FROM plays
    WHERE season = ${season}
    GROUP BY offense
    ORDER BY play_count DESC
    LIMIT 20
  `)
}

export interface EPAByDown {
  down: number
  avg_epa: number
  play_count: number
}

export async function getEPAByDown(team: string, season: number): Promise<EPAByDown[]> {
  return query<EPAByDown>(`
    SELECT
      down,
      AVG(ppa) as avg_epa,
      COUNT(*) as play_count
    FROM plays
    WHERE offense = '${team}' AND season = ${season} AND down IS NOT NULL
    GROUP BY down
    ORDER BY down
  `)
}
```

**Step 4: Commit the DuckDB setup**

```bash
cd /Users/robstover/Development/personal/cfb-app
git add src/lib/duckdb/
git commit -m "feat: add DuckDB-WASM client for client-side analytics"
```

---

### Task 9: Create Exploration Page

**Files:**
- Create: `cfb-app/src/app/explore/page.tsx`
- Create: `cfb-app/src/components/explore/QueryRunner.tsx`

**Step 1: Create the exploration page**

Create `cfb-app/src/app/explore/page.tsx`:

```tsx
'use client'

import { useState } from 'react'
import { QueryRunner } from '@/components/explore/QueryRunner'

export default function ExplorePage() {
  return (
    <main className="max-w-6xl mx-auto p-8">
      <h1 className="text-3xl font-bold mb-2">Data Explorer</h1>
      <p className="text-gray-600 mb-8">
        Query CFB data directly in your browser using DuckDB-WASM
      </p>

      <QueryRunner />
    </main>
  )
}
```

**Step 2: Create the QueryRunner component**

Create `cfb-app/src/components/explore/QueryRunner.tsx`:

```tsx
'use client'

import { useState } from 'react'
import { query } from '@/lib/duckdb/client'

const EXAMPLE_QUERIES = [
  {
    name: 'Top 10 teams by plays (2024)',
    sql: `SELECT offense as team, COUNT(*) as plays
FROM plays
WHERE season = 2024
GROUP BY offense
ORDER BY plays DESC
LIMIT 10`
  },
  {
    name: 'EPA by down for Alabama',
    sql: `SELECT down, ROUND(AVG(ppa), 3) as avg_epa, COUNT(*) as plays
FROM plays
WHERE offense = 'Alabama' AND season = 2024 AND down IS NOT NULL
GROUP BY down
ORDER BY down`
  },
  {
    name: 'Red zone efficiency',
    sql: `SELECT
  offense as team,
  COUNT(*) FILTER (WHERE yards_to_goal <= 20) as rz_plays,
  ROUND(AVG(ppa) FILTER (WHERE yards_to_goal <= 20), 3) as rz_epa
FROM plays
WHERE season = 2024
GROUP BY offense
HAVING COUNT(*) FILTER (WHERE yards_to_goal <= 20) > 50
ORDER BY rz_epa DESC
LIMIT 10`
  }
]

export function QueryRunner() {
  const [sql, setSql] = useState(EXAMPLE_QUERIES[0].sql)
  const [results, setResults] = useState<Record<string, unknown>[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  async function runQuery() {
    setLoading(true)
    setError(null)
    setResults(null)

    try {
      const data = await query<Record<string, unknown>>(sql)
      setResults(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Query failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="space-y-4">
      {/* Example query buttons */}
      <div className="flex gap-2 flex-wrap">
        {EXAMPLE_QUERIES.map((q) => (
          <button
            key={q.name}
            onClick={() => setSql(q.sql)}
            className="px-3 py-1 text-sm bg-gray-100 hover:bg-gray-200 rounded"
          >
            {q.name}
          </button>
        ))}
      </div>

      {/* SQL input */}
      <textarea
        value={sql}
        onChange={(e) => setSql(e.target.value)}
        className="w-full h-40 p-4 font-mono text-sm border rounded bg-gray-900 text-gray-100"
        placeholder="Enter SQL query..."
      />

      {/* Run button */}
      <button
        onClick={runQuery}
        disabled={loading}
        className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
      >
        {loading ? 'Running...' : 'Run Query'}
      </button>

      {/* Error display */}
      {error && (
        <div className="p-4 bg-red-50 border border-red-200 rounded text-red-700">
          {error}
        </div>
      )}

      {/* Results table */}
      {results && results.length > 0 && (
        <div className="overflow-x-auto">
          <table className="min-w-full border-collapse border">
            <thead>
              <tr className="bg-gray-100">
                {Object.keys(results[0]).map((col) => (
                  <th key={col} className="border p-2 text-left font-medium">
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {results.map((row, i) => (
                <tr key={i} className="hover:bg-gray-50">
                  {Object.values(row).map((val, j) => (
                    <td key={j} className="border p-2">
                      {val === null ? <span className="text-gray-400">null</span> : String(val)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          <p className="text-sm text-gray-500 mt-2">{results.length} rows</p>
        </div>
      )}

      {results && results.length === 0 && (
        <p className="text-gray-500">No results</p>
      )}
    </div>
  )
}
```

**Step 3: Add link in header**

Modify `cfb-app/src/components/Header.tsx` to add Explore link:

```tsx
// Add to nav links:
<Link href="/explore" className="hover:text-gray-300">Explore</Link>
```

**Step 4: Commit the exploration page**

```bash
cd /Users/robstover/Development/personal/cfb-app
git add src/app/explore/ src/components/explore/
git commit -m "feat: add data exploration page with DuckDB-WASM"
```

---

### Task 10: Upload Parquet Files to R2 (or serve locally)

**Files:**
- Parquet files in `cfb-database/exports/parquet/`

**Option A: Local Development (serve from public folder)**

```bash
# Copy parquet files to Next.js public folder
cp -r /Users/robstover/Development/personal/cfb-database/exports/parquet \
      /Users/robstover/Development/personal/cfb-app/public/data
```

Update `.env.local`:
```
NEXT_PUBLIC_PARQUET_URL=/data
```

**Option B: Production (Cloudflare R2)**

1. Create R2 bucket in Cloudflare dashboard
2. Upload parquet files
3. Enable public access or use signed URLs
4. Set `NEXT_PUBLIC_PARQUET_URL` to R2 URL

**Step 1: For now, use local serving**

```bash
mkdir -p /Users/robstover/Development/personal/cfb-app/public/data
# Copy after running export script
```

**Step 2: Add to .gitignore**

Add to `cfb-app/.gitignore`:
```
public/data/*.parquet
```

---

## Summary Checklist

### Phase 0: Get App Working
- [ ] Task 1: Add env vars to Vercel, redeploy
- [ ] Task 2: Verify `get_drive_patterns` function exists
- [ ] Task 3: Create `team_style_profile` materialized view
- [ ] Task 4: Create `team_season_trajectory` materialized view
- [ ] Task 5: Create public schema views for API access
- [ ] Task 6: Verify app works end-to-end

### Phase 0.5: DuckDB-WASM Exploration
- [ ] Task 7: Export Parquet files from Supabase
- [ ] Task 8: Add DuckDB-WASM to cfb-app
- [ ] Task 9: Create exploration page
- [ ] Task 10: Serve Parquet files (local or R2)

---

## References

- Design doc: `docs/plans/2026-01-30-analytics-architecture-design.md`
- Sprint 4 design: `docs/plans/2026-01-29-sprint-4-design.md`
- DuckDB-WASM docs: https://duckdb.org/docs/api/wasm/overview
- Supabase SQL Editor: https://supabase.com/dashboard
