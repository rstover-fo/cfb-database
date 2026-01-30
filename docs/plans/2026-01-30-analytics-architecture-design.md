# CFB Analytics Architecture Design

> **Status:** Draft
> **Date:** 2026-01-30
> **Author:** Rob

## Problem Statement

### Current State
- Supabase Postgres with ~4.1M rows (3.6M plays, 1.7GB)
- 14 materialized views across `analytics` and `marts` schemas (CONCURRENT refresh)
- Direct queries from Next.js Server Components
- Works fine for current scale and off-season usage

### Why This Won't Scale Long-Term
1. **MVs are snapshots** — Require full rebuild on refresh, can't handle real-time
2. **Postgres row-oriented storage** — Slow for analytical aggregations across millions of rows
3. **Connection limits** — Supabase plans cap concurrent connections; complex queries compete
4. **Game-day spikes** — CFB Saturdays could see 100x normal traffic

### Target Workloads

| Workload | Example | Latency | Concurrency | Future Solution |
|----------|---------|---------|-------------|-----------------|
| **App dashboards** | Team page, standings, EPA charts | <200ms | High (1000+) | ClickHouse |
| **Real-time games** | Live play-by-play during Saturday | <5s freshness | High | ClickHouse streaming |
| **User exploration** | Hex-style "drag fields, build charts" | <2s acceptable | Low (power users) | DuckDB-WASM |

### Success Criteria (Future State)
- Dashboard queries: P95 < 200ms
- Real-time data: Available within 5 seconds of source
- Exploration: Arbitrary queries on full dataset in-browser
- Handle 1000+ concurrent users on game days
- No server load for exploration features

---

## Target Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                   │
│                    CFBD API (historical + live games)                       │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           INGESTION LAYER                                   │
│                                                                             │
│   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐        │
│   │   dlt Pipeline  │    │  Live Ingest    │    │  Parquet Export │        │
│   │   (historical)  │    │  (game days)    │    │  (nightly)      │        │
│   └────────┬────────┘    └────────┬────────┘    └────────┬────────┘        │
└────────────┼──────────────────────┼──────────────────────┼──────────────────┘
             │                      │                      │
             ▼                      ▼                      ▼
┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐
│     SUPABASE       │  │    CLICKHOUSE      │  │    OBJECT STORE    │
│     (OLTP)         │  │    (OLAP)          │  │    (R2)            │
│                    │  │                    │  │                    │
│ • Users/auth       │  │ • Plays (billions) │  │ • Parquet files    │
│ • App state        │  │ • Real-time ingest │  │ • Versioned by     │
│ • Reference data   │  │ • Pre-aggregated   │  │   season/week      │
│ • Teams/coaches    │  │   dashboards       │  │                    │
└─────────┬──────────┘  └─────────┬──────────┘  └─────────┬──────────┘
          │                       │                       │
          ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            CFB-APP (Next.js)                                │
│                                                                             │
│   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐        │
│   │  Server Comp.   │    │   API Routes    │    │  Client Comp.   │        │
│   │  (Supabase)     │    │  (ClickHouse)   │    │  (DuckDB-WASM)  │        │
│   │                 │    │                 │    │                 │        │
│   │ • Auth/session  │    │ • Dashboards    │    │ • Exploration   │        │
│   │ • User prefs    │    │ • Live games    │    │ • Ad-hoc query  │        │
│   │ • Reference     │    │ • Leaderboards  │    │ • Export/share  │        │
│   └─────────────────┘    └─────────────────┘    └─────────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Ownership

| Store | Data | Why Here |
|-------|------|----------|
| **Supabase** | Users, auth, preferences, reference tables (teams, coaches, venues) | OLTP workload, Row-level security, Auth integration |
| **ClickHouse** | Plays, drives, stats, aggregations, live game data | Columnar storage, real-time ingest, fast aggregations |
| **Cloudflare R2** | Parquet exports of analytical tables | Zero egress fees, cheap storage, DuckDB-WASM compatible |

---

## Data Flow & Synchronization

### Path 1: Historical Data (dlt → Supabase → ClickHouse)

```
CFBD API ──► dlt pipeline ──► Supabase ──► CDC ──► ClickHouse
                              (staging)           (analytics)
```

- **Trigger:** Manual or scheduled (off-season backfills, weekly during season)
- **Latency:** Hours acceptable — this is historical data
- **Keep existing dlt pipelines**, add ClickHouse as downstream consumer

### Path 2: Real-Time Games (API → ClickHouse direct)

```
CFBD API ──► Live Ingest Service ──► ClickHouse
             (Vercel Cron + Edge)     (direct insert)
```

- **Trigger:** Game days only (Saturdays, bowl season)
- **Latency:** <5 seconds from source
- **Bypasses Supabase entirely** — no need to hit OLTP for live analytics

### Path 3: Exploration Export (ClickHouse → Parquet → R2)

```
ClickHouse ──► Nightly Job ──► Parquet ──► R2
                               (compressed)
```

- **Trigger:** Nightly (or after game days)
- **Size:** ~4M plays ≈ 50-100MB compressed Parquet
- **DuckDB-WASM fetches** these files on-demand in browser

---

## Technology Choices

| Component | Choice | Rationale |
|-----------|--------|-----------|
| OLAP Database | ClickHouse Cloud | Managed, fast, real-time native, incremental MVs |
| Object Storage | Cloudflare R2 | Zero egress fees (critical for client-side fetching) |
| Client Engine | DuckDB-WASM | Browser-native, no server load, infinite scale |
| CDC | ClickHouse Postgres connector | Native, no middleware |
| Live Ingest | Vercel Cron + Edge Function | Serverless, fits existing stack |

---

## Implementation Plan

### Current Priority: Iteration Over Scale

Right now, the priority is rapid iteration with a single user (Rob) to explore the data and refine app capabilities. The full architecture above is the target state, but we'll build toward it incrementally.

### Phase 0: Get It Working

- [ ] Add Supabase env vars to Vercel project (`NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_ANON_KEY`)
- [ ] Verify app loads teams correctly
- [ ] Fix 5 PK bugs in dlt config:
  - `coaches`: Should be (first_name, last_name), not include seasons
  - `player_season_stats`: Missing stat_type in PK
  - `transfer_portal`: PK mismatch in config
  - `lines`: Config says `id`, should be (game_id, provider)
  - `picks`: Config says college_athlete_id, should be (year, overall)
- [ ] Create missing views:
  - `team_season_epa`
  - `team_style_profile`
  - `team_season_trajectory`
  - `get_drive_patterns()` function

**Outcome:** App actually works in production

### Phase 0.5: Personal Data Exploration

- [ ] Export plays/drives/stats to Parquet (one-time, local or R2)
- [ ] Add DuckDB-WASM to cfb-app (`@duckdb/duckdb-wasm`, `apache-arrow`)
- [ ] Build minimal exploration UI:
  - Field/dimension picker
  - Table view with sorting/filtering
  - Basic chart output
- [ ] Iterate on what questions you want to answer

**Outcome:** Ability to interrogate data without pre-building every query

### Phase 1: ClickHouse for Dashboards (When Needed)

**Trigger:** Query latency >500ms with multiple users

- Stand up ClickHouse Cloud instance
- Migrate `plays`, `drives`, `stats` tables
- Set up CDC from Supabase → ClickHouse
- Update cfb-app API routes to query ClickHouse
- Keep Supabase MVs as fallback during transition

### Phase 2: DuckDB-WASM at Scale (When Needed)

**Trigger:** Exploration feature validated, ready for other users

- Set up R2 bucket with automated Parquet exports
- Build nightly export job (ClickHouse → Parquet → R2)
- Expand exploration UI to full Hex-style interface

### Phase 3: Real-Time Game Day (When Needed)

**Trigger:** Actual demand for live game data

- Build live ingest service (Vercel Cron + Edge Function)
- Direct insert to ClickHouse, bypassing Supabase
- Add real-time UI components (live play feed, updating stats)

---

## Open Questions

Parking lot for decisions to make later:

1. **Exploration UI scope** — Full Hex-style drag-and-drop, or simpler SQL-in-browser?
2. **Parquet refresh cadence** — Manual for now, automate when it becomes friction?
3. **ClickHouse timing** — Wait for performance pain, or set up early to learn the tool?
4. **Authentication for exploration** — Public data exploration, or require login?

---

## References

- [ClickHouse Cloud](https://clickhouse.com/cloud)
- [DuckDB-WASM](https://duckdb.org/docs/api/wasm/overview)
- [Cloudflare R2](https://developers.cloudflare.com/r2/)
- [Hex Explorations](https://hex.tech/) — Inspiration for exploration UI
