# CFB Scout Agent Design

**Goal:** Build a scouting intelligence platform that crawls recruiting sites, social media, and news sources, then uses Claude to synthesize player/team sentiment and store structured data for cfb-app consumption.

## System Overview

The **CFB Scout Agent** is a scheduled Python service that:
1. Crawls scouting sources (247Sports, Reddit, X, etc.)
2. Extracts and tags player/team entities
3. Synthesizes content using Claude into structured grades, traits, and sentiment
4. Stores results in Supabase for cfb-app to query

### Core Entities

- **Player profiles** - Draft prospects and transfer portal players with aggregated scouting grades, sentiment, and source citations
- **Player timeline** - Longitudinal tracking from recruit → college career → draft prospect
- **Team rosters** - Position group analysis, depth chart projections, and overall roster sentiment
- **Scouting reports** - Raw collected articles/posts with source, date, player/team tags

### Data Flow

```
Sources (247, Rivals, Reddit, X)
    → Crawlers (scheduled)
    → Raw content table (scouting.reports)
    → Claude summarization
    → Structured entities (players, teams, timelines)
    → Supabase tables
    → cfb-app queries via API
```

### Tech Stack

- Python + Claude API for agent logic
- Supabase Postgres for storage (same instance as cfb-database)
- Scheduled via cron or Supabase Edge Functions
- Web scraping: httpx + BeautifulSoup / Playwright for JS-heavy sites
- APIs: Reddit (PRAW), X (tweepy), news APIs

---

## Data Model

New schema: `scouting`

### scouting.players

| Column | Type | Description |
|--------|------|-------------|
| id | serial | Primary key |
| player_id | bigint | Links to core.roster or recruiting.recruits |
| name | text | Player name |
| position | text | Position |
| team | text | Current team |
| class_year | int | Eligibility year |
| current_status | text | recruit, active, transfer, draft_eligible, drafted |
| composite_grade | int | AI-synthesized 0-100 grade |
| traits | jsonb | {athleticism, technique, football_iq, leadership, durability} |
| draft_projection | text | Day 1, Day 2, Day 3, UDFA, etc. |
| comps | text[] | Player comparisons |
| last_updated | timestamptz | Last refresh |

### scouting.player_timeline

| Column | Type | Description |
|--------|------|-------------|
| id | serial | Primary key |
| player_id | bigint | FK to scouting.players |
| snapshot_date | date | When snapshot was taken |
| status | text | recruit, freshman, sophomore, etc. |
| sentiment_score | numeric | -1 to 1 |
| grade_at_time | int | Grade at this point |
| traits_at_time | jsonb | Traits snapshot |
| key_narratives | text[] | "raw but explosive", "injury concerns" |
| sources_count | int | Number of sources in this period |

### scouting.reports

| Column | Type | Description |
|--------|------|-------------|
| id | serial | Primary key |
| source_url | text | Original URL |
| source_name | text | 247Sports, Reddit, X, etc. |
| published_at | timestamptz | When content was published |
| crawled_at | timestamptz | When we crawled it |
| content_type | text | article, social, forum |
| player_ids | bigint[] | Tagged players |
| team_ids | text[] | Tagged teams |
| raw_text | text | Original content |
| summary | text | Claude-generated summary |
| sentiment_score | numeric | -1 to 1 |

### scouting.team_rosters

| Column | Type | Description |
|--------|------|-------------|
| team | text | Team name |
| season | int | Season year |
| position_groups | jsonb | Depth + sentiment per group |
| overall_sentiment | numeric | -1 to 1 |
| trajectory | text | improving, stable, declining |
| key_storylines | text[] | Notable narratives |
| last_updated | timestamptz | Last refresh |

---

## Sources & Crawlers

### Tier 1 - High-value scouting sites (daily crawl)

| Source | Content Type | Method | Cost | Notes |
|--------|--------------|--------|------|-------|
| 247Sports | Recruiting, scouting reports | Scrape | Free tier | Player pages, team boards |
| Rivals | Recruiting, player evals | Scrape | Free tier | Similar structure to 247 |
| On3 | NIL, transfer portal, recruiting | Scrape | Free tier | Strong portal coverage, consider upgrade later |
| PFF | Per-play grades, position rankings | Scrape | Free/~$40/mo | Objective film-based grades, complements sentiment |

### Tier 2 - Community sentiment (hourly for active topics)

| Source | Content Type | Method | Cost | Notes |
|--------|--------------|--------|------|-------|
| Reddit | r/CFB, team subreddits | API (PRAW) | Free | Great for fan sentiment, rumors |

### Tier 3 - Supplementary (weekly)

| Source | Content Type | Method | Notes |
|--------|--------------|--------|-------|
| Team blogs (SB Nation) | Team-specific analysis | RSS + scrape | Good for depth chart speculation |
| Draft analysts | Draft grades | Scrape | Seasonal - ramps up Nov-April |
| ESPN | Draft rankings, scouting reports | Scrape | Insider content limited |

### Deferred Sources

| Source | Reason | Revisit When |
|--------|--------|--------------|
| X/Twitter | Poor cost/value ($100/mo for limited access) | If Reddit + scouting sites leave gaps |
| The Athletic | Paywall (~$8/mo) | If quality analysis needed |

### Crawler Architecture

- Each source gets its own crawler module
- Shared `BaseCrawler` class handles rate limiting, retries, deduplication
- Content stored in `scouting.reports` with source tagging
- Separate summarization job processes raw content → structured entities

---

## AI Processing Pipeline

### Stage 1: Entity Extraction

When raw content is crawled, Claude identifies:
- Player names → matched to `core.roster` or `recruiting.recruits`
- Teams mentioned
- Content type (scouting report, rumor, opinion, news)

### Stage 2: Player Summarization

For each player with new content, Claude generates:

```json
{
    "grade": 78,
    "sentiment": 0.6,
    "traits": {
        "athleticism": {"score": 85, "trend": "stable"},
        "technique": {"score": 70, "trend": "improving"},
        "football_iq": {"score": 80, "trend": "stable"},
        "leadership": {"score": 75, "trend": "unknown"},
        "durability": {"score": 65, "trend": "concerning"}
    },
    "key_narratives": [
        "Elite burst and acceleration",
        "Route running has improved significantly since freshman year",
        "Some concerns about contested catch ability"
    ],
    "draft_projection": "Day 2 (Rounds 2-3)",
    "comps": ["Garrett Wilson", "Chris Olave"]
}
```

### Stage 3: Team Roster Rollup

Aggregate player data into position group assessments:

```json
{
    "position_groups": {
        "QB": {"depth": 2, "sentiment": 0.7, "outlook": "strong starter, thin backup"},
        "WR": {"depth": 5, "sentiment": 0.8, "outlook": "deepest in conference"},
        "OL": {"depth": 4, "sentiment": 0.3, "outlook": "concerning after 2 transfers out"}
    },
    "overall_sentiment": 0.6,
    "trajectory": "improving",
    "key_storylines": [
        "QB competition resolved in favor of Smith",
        "O-line depth is biggest question mark",
        "Defensive backfield could be elite"
    ]
}
```

### Prompt Engineering

Each summarization uses structured prompts with CFB-specific context:
- Position-specific trait expectations
- Conference strength adjustments
- Historical context from player timeline
- Consistent grading rubrics

---

## Project Structure

Repository: `cfb-scout` (standalone under Development/personal)

```
cfb-scout/
├── src/
│   ├── crawlers/
│   │   ├── base.py              # BaseCrawler with rate limiting, retries
│   │   ├── recruiting/          # 247, Rivals, On3
│   │   ├── social/              # Reddit, X
│   │   └── news/                # ESPN, Athletic, blogs
│   ├── processing/
│   │   ├── entity_extraction.py
│   │   ├── player_summarizer.py
│   │   ├── team_rollup.py
│   │   └── prompts/             # Structured prompts for Claude
│   ├── storage/
│   │   ├── models.py            # SQLAlchemy/Supabase models
│   │   └── db.py                # Connection, upsert helpers
│   └── scheduler/
│       └── jobs.py              # Cron job definitions
├── scripts/
│   ├── run_crawl.py             # Manual crawl trigger
│   └── backfill_player.py       # Seed historical data for a player
├── tests/
├── pyproject.toml
└── README.md
```

---

## Implementation Phases

| Phase | Scope | Outcome |
|-------|-------|---------|
| **1** | Schema + Reddit crawler | Prove the loop: crawl → store → summarize → query |
| **2** | 247Sports + player entity linking | Real scouting content, matched to existing roster data |
| **3** | PFF grades integration | Objective performance data alongside sentiment |
| **4** | Player timeline + longitudinal tracking | Historical snapshots working |
| **5** | Team roster rollups | Position group analysis |
| **6** | cfb-app integration | API endpoints for UI consumption |

---

## API Requirements (for cfb-app)

The following endpoints/queries will be needed:

- `GET /players/{id}` - Full player profile with current grade, traits, timeline
- `GET /players/{id}/timeline` - Historical sentiment/grade progression
- `GET /players?status=draft_eligible&position=WR` - Filter/search players
- `GET /teams/{team}/roster?season=2025` - Team roster with position group analysis
- `GET /reports?player_id={id}` - Raw scouting reports for a player

These can be implemented as Supabase RPC functions or a thin API layer.

---

## Dependencies & Credentials Needed

- **Anthropic API key** - Claude for summarization
- **Reddit API credentials** - For PRAW (free tier)
- **Supabase connection** - Same as cfb-database
- **Proxy service (optional)** - For scraping at scale without blocks

---

## Decisions Made

1. **Premium access:** Start with free tiers for all sources. Consider On3 + Athletic (~$20-30/mo) later if gaps identified.
2. **X/Twitter:** Skip for now - poor cost/value ratio. Reddit + scouting sites provide 80% of signal.
3. **PFF:** Added as key source for objective, film-based player grades.
4. **Backfill scope:** Active roster players only (~11,000 total, focus on ~3,000 key contributors). Pull recruiting history from existing `recruiting.recruits` table, scrape current-season scouting content only.
5. **Crawl frequency:** Daily for scouting sites, hourly for Reddit during active periods.

---

## Success Criteria

Phase 1 is complete when:
- [ ] Schema deployed to Supabase
- [ ] Reddit crawler running on schedule
- [ ] At least one player profile populated with sentiment data
- [ ] Data queryable from Supabase dashboard
