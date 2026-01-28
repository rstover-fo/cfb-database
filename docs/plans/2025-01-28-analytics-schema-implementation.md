# Analytics Schema Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Expand endpoint coverage, build analytics marts layer, and create app-friendly API views for a CFB analytics web application.

**Architecture:** Layered schema (raw → marts → api → features) on Supabase Postgres. New endpoints loaded via dlt pipelines. Materialized views for pre-computed analytics. Regular views for API consumption via PostgREST.

**Tech Stack:** Python 3.11+, dlt (dlthub), Supabase Postgres, SQL (materialized views, functions)

---

## Current State Summary

**Loaded in Supabase:**
- `ref.*`: conferences (106), teams (1,899), venues (837), coaches (1,790), play_types (49)
- `core.*`: games (18,650), drives (183,603), plays (3,611,707 with PPA)
- `stats.*`: team_season_stats (49,819), player_season_stats (131,268)
- `ratings.*`: sp (800), elo (791), fpi (791), srs (1,258)
- `recruiting.*`: recruits (16,086), team_recruiting (1,184), transfer_portal (14,356)
- `betting.*`: lines (20,192)
- `draft.*`: draft_picks (1,549)
- `metrics.*`: ppa_teams (792), ppa_players_season (24,475), pregame_win_probability (5,080)

**Known Issues:**
- 3 variant columns need cleanup
- No business indexes (only dlt's `_dlt_id`)
- Teams reference conferences by name, not FK

**Critical Gaps:**
- No `core.rosters` (blocks player-team linkage)
- No `ratings.poll_rankings` (AP/Coaches polls)
- No advanced team stats, WEPA, or game-level player stats

---

## Phase 1: Endpoint Coverage (Pipeline Work)

### Task 1.1: Add Roster Endpoint

**Files:**
- Create: `src/pipelines/sources/rosters.py`
- Modify: `src/pipelines/config/endpoints.py`
- Modify: `src/pipelines/run.py`
- Test: `tests/test_sources/test_rosters.py`

**Step 1: Write endpoint config**

Add to `src/pipelines/config/endpoints.py`:

```python
EndpointConfig(
    name="rosters",
    path="/roster",
    primary_key=["id", "team", "year"],
    params={"team": None, "year": None},  # Required params
    schema="core",
    table_name="rosters",
    write_disposition="merge",
    year_range=(2004, 2026),
)
```

**Step 2: Write the failing test**

Create `tests/test_sources/test_rosters.py`:

```python
import pytest
from unittest.mock import patch, MagicMock

def test_rosters_resource_yields_players():
    """Roster endpoint should yield player records with team/year context."""
    from src.pipelines.sources.rosters import rosters_resource

    mock_response = [
        {"id": 12345, "first_name": "Jalen", "last_name": "Milroe", "position": "QB", "jersey": 4},
        {"id": 12346, "first_name": "Ryan", "last_name": "Williams", "position": "WR", "jersey": 2},
    ]

    with patch("src.pipelines.sources.rosters.get_api_client") as mock_client:
        mock_client.return_value.get.return_value = mock_response

        results = list(rosters_resource(teams=["Alabama"], years=[2024]))

        assert len(results) == 2
        assert results[0]["id"] == 12345
        assert results[0]["team"] == "Alabama"
        assert results[0]["year"] == 2024


def test_rosters_resource_iterates_teams_and_years():
    """Should call API for each team/year combination."""
    from src.pipelines.sources.rosters import rosters_resource

    with patch("src.pipelines.sources.rosters.get_api_client") as mock_client:
        mock_client.return_value.get.return_value = []

        list(rosters_resource(teams=["Alabama", "Georgia"], years=[2023, 2024]))

        # 2 teams x 2 years = 4 API calls
        assert mock_client.return_value.get.call_count == 4
```

**Step 3: Run test to verify it fails**

Run: `pytest tests/test_sources/test_rosters.py -v`
Expected: FAIL with "No module named 'src.pipelines.sources.rosters'"

**Step 4: Write the implementation**

Create `src/pipelines/sources/rosters.py`:

```python
"""Roster data source - team rosters by season."""
import dlt
from typing import Iterator, List

from src.pipelines.utils.api_client import get_api_client
from src.pipelines.utils.rate_limiter import RateLimiter


@dlt.resource(
    name="rosters",
    write_disposition="merge",
    primary_key=["id", "team", "year"],
)
def rosters_resource(
    teams: List[str],
    years: List[int],
    rate_limiter: RateLimiter = None,
) -> Iterator[dict]:
    """
    Fetch team rosters for given teams and years.

    Args:
        teams: List of team names (e.g., ["Alabama", "Georgia"])
        years: List of seasons (e.g., [2023, 2024])
        rate_limiter: Optional rate limiter instance

    Yields:
        Player roster records with team/year context added
    """
    client = get_api_client()

    for team in teams:
        for year in years:
            if rate_limiter:
                rate_limiter.wait()

            try:
                players = client.get("/roster", params={"team": team, "year": year})

                for player in players:
                    # Add context fields
                    player["team"] = team
                    player["year"] = year
                    yield player

            except Exception as e:
                print(f"Error fetching roster for {team} {year}: {e}")
                continue


@dlt.source(name="rosters")
def rosters_source(
    teams: List[str] = None,
    years: List[int] = None,
    rate_limiter: RateLimiter = None,
):
    """
    Roster data source.

    If teams not provided, fetches all FBS teams from ref.teams.
    If years not provided, uses default year range.
    """
    from src.pipelines.utils.years import get_year_range

    if years is None:
        years = get_year_range(2004, 2026)

    if teams is None:
        # Load FBS teams from database or config
        from src.pipelines.config.teams import get_fbs_teams
        teams = get_fbs_teams()

    return rosters_resource(teams=teams, years=years, rate_limiter=rate_limiter)
```

**Step 5: Run test to verify it passes**

Run: `pytest tests/test_sources/test_rosters.py -v`
Expected: PASS

**Step 6: Wire into CLI**

Modify `src/pipelines/run.py` to add rosters source:

```python
# In the source registry
SOURCES = {
    # ... existing sources
    "rosters": rosters_source,
}
```

**Step 7: Commit**

```bash
git add src/pipelines/sources/rosters.py tests/test_sources/test_rosters.py
git add src/pipelines/config/endpoints.py src/pipelines/run.py
git commit -m "feat: add roster endpoint for player-team linkage"
```

---

### Task 1.2: Add Rankings Endpoint

**Files:**
- Create: `src/pipelines/sources/rankings.py`
- Modify: `src/pipelines/config/endpoints.py`
- Modify: `src/pipelines/run.py`
- Test: `tests/test_sources/test_rankings.py`

**Step 1: Write the failing test**

Create `tests/test_sources/test_rankings.py`:

```python
import pytest
from unittest.mock import patch

def test_rankings_resource_yields_poll_data():
    """Rankings endpoint should yield poll rankings by week."""
    from src.pipelines.sources.rankings import rankings_resource

    mock_response = [
        {
            "season": 2024,
            "seasonType": "regular",
            "week": 1,
            "polls": [
                {
                    "poll": "AP Top 25",
                    "ranks": [
                        {"rank": 1, "school": "Georgia", "conference": "SEC", "firstPlaceVotes": 55, "points": 1550},
                        {"rank": 2, "school": "Ohio State", "conference": "Big Ten", "firstPlaceVotes": 8, "points": 1492},
                    ]
                }
            ]
        }
    ]

    with patch("src.pipelines.sources.rankings.get_api_client") as mock_client:
        mock_client.return_value.get.return_value = mock_response

        results = list(rankings_resource(years=[2024]))

        # Should flatten polls and ranks
        assert len(results) >= 2
        assert results[0]["season"] == 2024
        assert results[0]["week"] == 1
        assert results[0]["poll_type"] == "AP Top 25"
        assert results[0]["rank"] == 1
        assert results[0]["school"] == "Georgia"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_sources/test_rankings.py -v`
Expected: FAIL

**Step 3: Write the implementation**

Create `src/pipelines/sources/rankings.py`:

```python
"""Rankings data source - AP/Coaches poll rankings by week."""
import dlt
from typing import Iterator, List

from src.pipelines.utils.api_client import get_api_client
from src.pipelines.utils.rate_limiter import RateLimiter


@dlt.resource(
    name="poll_rankings",
    write_disposition="merge",
    primary_key=["season", "week", "poll_type", "school"],
)
def rankings_resource(
    years: List[int],
    rate_limiter: RateLimiter = None,
) -> Iterator[dict]:
    """
    Fetch poll rankings for given years.

    Flattens the nested poll structure into individual rank records.
    """
    client = get_api_client()

    for year in years:
        if rate_limiter:
            rate_limiter.wait()

        try:
            weeks_data = client.get("/rankings", params={"year": year})

            for week_data in weeks_data:
                season = week_data.get("season")
                week = week_data.get("week")
                season_type = week_data.get("seasonType")

                for poll in week_data.get("polls", []):
                    poll_type = poll.get("poll")

                    for rank_entry in poll.get("ranks", []):
                        yield {
                            "season": season,
                            "week": week,
                            "season_type": season_type,
                            "poll_type": poll_type,
                            "rank": rank_entry.get("rank"),
                            "school": rank_entry.get("school"),
                            "conference": rank_entry.get("conference"),
                            "first_place_votes": rank_entry.get("firstPlaceVotes"),
                            "points": rank_entry.get("points"),
                        }

        except Exception as e:
            print(f"Error fetching rankings for {year}: {e}")
            continue


@dlt.source(name="rankings")
def rankings_source(years: List[int] = None, rate_limiter: RateLimiter = None):
    """Rankings data source."""
    from src.pipelines.utils.years import get_year_range

    if years is None:
        years = get_year_range(2004, 2026)

    return rankings_resource(years=years, rate_limiter=rate_limiter)
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_sources/test_rankings.py -v`
Expected: PASS

**Step 5: Wire into CLI and commit**

```bash
git add src/pipelines/sources/rankings.py tests/test_sources/test_rankings.py
git commit -m "feat: add rankings endpoint for poll data"
```

---

### Task 1.3: Wire Existing CONFIG_ONLY Endpoints

These endpoints have configs but aren't wired into their source functions.

**Files:**
- Modify: `src/pipelines/sources/stats.py` (advanced_team_stats)
- Modify: `src/pipelines/sources/metrics.py` (ppa_games, ppa_players_games, win_probability)
- Modify: `src/pipelines/sources/games.py` (game_media)
- Test: Add tests for each

**Step 1: Wire advanced_team_stats in stats.py**

Find the `stats_source()` function and add `advanced_team_stats_resource` to the return list.

**Step 2: Wire ppa_games, ppa_players_games, win_probability in metrics.py**

Add these to `metrics_source()` return list.

**Step 3: Wire game_media in games.py**

Add to `games_source()` return list.

**Step 4: Test each with --dry-run**

```bash
python -m src.pipelines.run --source stats --dry-run
python -m src.pipelines.run --source metrics --dry-run
python -m src.pipelines.run --source games --dry-run
```

**Step 5: Commit**

```bash
git add src/pipelines/sources/*.py
git commit -m "feat: wire CONFIG_ONLY endpoints (advanced_stats, ppa_games, game_media)"
```

---

### Task 1.4: Add WEPA Endpoint

**Files:**
- Create: `src/pipelines/sources/wepa.py`
- Test: `tests/test_sources/test_wepa.py`

**Step 1: Write test**

```python
def test_wepa_team_season_resource():
    """WEPA endpoint should yield opponent-adjusted EPA by team/season."""
    from src.pipelines.sources.wepa import wepa_team_season_resource

    mock_response = [
        {"team": "Alabama", "year": 2024, "offense": {"overall": 0.25}, "defense": {"overall": -0.15}},
    ]

    with patch("src.pipelines.sources.wepa.get_api_client") as mock_client:
        mock_client.return_value.get.return_value = mock_response

        results = list(wepa_team_season_resource(years=[2024]))

        assert len(results) == 1
        assert results[0]["team"] == "Alabama"
        assert "offense__overall" in results[0] or results[0].get("offense", {}).get("overall")
```

**Step 2: Implement and wire**

Similar pattern to rankings. Write disposition: merge. PK: (team, year).

**Step 3: Commit**

```bash
git commit -m "feat: add WEPA endpoint for opponent-adjusted EPA"
```

---

### Task 1.5: Add Team Talent Endpoint

**Files:**
- Modify: `src/pipelines/sources/recruiting.py`
- Test: `tests/test_sources/test_recruiting.py`

**Step 1: Add talent resource**

```python
@dlt.resource(
    name="team_talent",
    write_disposition="merge",
    primary_key=["year", "school"],
)
def team_talent_resource(years: List[int], rate_limiter: RateLimiter = None):
    """Fetch team talent composite scores."""
    client = get_api_client()

    for year in years:
        if rate_limiter:
            rate_limiter.wait()

        data = client.get("/talent", params={"year": year})
        for team in data:
            team["year"] = year
            yield team
```

**Step 2: Wire into recruiting_source and commit**

---

## Phase 2: Schema Hardening

### Task 2.1: Create Utility Function for Garbage Time

**Files:**
- Create: `src/schemas/functions/is_garbage_time.sql`

**Step 1: Write the SQL function**

```sql
-- Function to detect garbage time plays
-- Garbage time: margin > 28 in 4th quarter, or > 35 in 3rd+
CREATE OR REPLACE FUNCTION is_garbage_time(
    period integer,
    score_diff integer
) RETURNS boolean AS $$
BEGIN
    RETURN (
        (period = 4 AND ABS(COALESCE(score_diff, 0)) > 28) OR
        (period >= 3 AND ABS(COALESCE(score_diff, 0)) > 35)
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION is_garbage_time IS 'Returns true if play occurred in garbage time (blowout situations)';
```

**Step 2: Test locally**

```sql
SELECT is_garbage_time(4, 30);  -- true
SELECT is_garbage_time(4, 14);  -- false
SELECT is_garbage_time(3, 40);  -- true
```

**Step 3: Commit**

```bash
git add src/schemas/functions/
git commit -m "feat: add is_garbage_time SQL function"
```

---

### Task 2.2: Create Positions Reference Table

**Files:**
- Create: `src/schemas/014_positions.sql`

**Step 1: Write the schema**

```sql
-- Position reference table for grouping
CREATE TABLE IF NOT EXISTS ref.positions (
    id text PRIMARY KEY,
    name text NOT NULL,
    side text NOT NULL CHECK (side IN ('offense', 'defense', 'special_teams')),
    position_group text NOT NULL
);

INSERT INTO ref.positions (id, name, side, position_group) VALUES
    ('QB', 'Quarterback', 'offense', 'passer'),
    ('RB', 'Running Back', 'offense', 'rusher'),
    ('FB', 'Fullback', 'offense', 'rusher'),
    ('WR', 'Wide Receiver', 'offense', 'receiver'),
    ('TE', 'Tight End', 'offense', 'receiver'),
    ('OT', 'Offensive Tackle', 'offense', 'lineman'),
    ('OG', 'Offensive Guard', 'offense', 'lineman'),
    ('OC', 'Center', 'offense', 'lineman'),
    ('OL', 'Offensive Line', 'offense', 'lineman'),
    ('DE', 'Defensive End', 'defense', 'dline'),
    ('DT', 'Defensive Tackle', 'defense', 'dline'),
    ('DL', 'Defensive Line', 'defense', 'dline'),
    ('EDGE', 'Edge Rusher', 'defense', 'dline'),
    ('ILB', 'Inside Linebacker', 'defense', 'linebacker'),
    ('OLB', 'Outside Linebacker', 'defense', 'linebacker'),
    ('LB', 'Linebacker', 'defense', 'linebacker'),
    ('CB', 'Cornerback', 'defense', 'db'),
    ('S', 'Safety', 'defense', 'db'),
    ('DB', 'Defensive Back', 'defense', 'db'),
    ('K', 'Kicker', 'special_teams', 'specialist'),
    ('P', 'Punter', 'special_teams', 'specialist'),
    ('LS', 'Long Snapper', 'special_teams', 'specialist'),
    ('ATH', 'Athlete', 'offense', 'athlete')
ON CONFLICT (id) DO NOTHING;
```

**Step 2: Commit**

```bash
git commit -m "feat: add positions reference table"
```

---

### Task 2.3: Add Score Diff Column to Plays

The plays table needs `score_diff` for garbage time filtering. Currently has `offense_score` and `defense_score`.

**Files:**
- Create: `src/schemas/015_plays_score_diff.sql`

**Step 1: Add computed column**

```sql
-- Add score_diff as a generated column
ALTER TABLE core.plays
ADD COLUMN IF NOT EXISTS score_diff integer
GENERATED ALWAYS AS (offense_score - defense_score) STORED;

-- Index for garbage time queries
CREATE INDEX IF NOT EXISTS idx_plays_score_diff ON core.plays (score_diff);
```

**Step 2: Commit**

```bash
git commit -m "feat: add score_diff generated column to plays"
```

---

## Phase 3: Marts Layer (Materialized Views)

### Task 3.1: Create team_season_summary Mart

**Files:**
- Create: `src/schemas/marts/001_team_season_summary.sql`

**Step 1: Write the materialized view**

```sql
-- Team season summary: record, scoring, ratings, recruiting
CREATE SCHEMA IF NOT EXISTS marts;

DROP MATERIALIZED VIEW IF EXISTS marts.team_season_summary CASCADE;

CREATE MATERIALIZED VIEW marts.team_season_summary AS
WITH game_results AS (
    SELECT
        CASE WHEN home_id = t.id THEN home_id ELSE away_id END AS team_id,
        t.school,
        g.season,
        g.conference_game,
        CASE
            WHEN home_id = t.id AND home_points > away_points THEN 1
            WHEN away_id = t.id AND away_points > home_points THEN 1
            ELSE 0
        END AS won,
        CASE WHEN home_id = t.id THEN home_points ELSE away_points END AS points_for,
        CASE WHEN home_id = t.id THEN away_points ELSE home_points END AS points_against
    FROM core.games g
    CROSS JOIN LATERAL (
        SELECT id, school FROM ref.teams WHERE id IN (g.home_id, g.away_id)
    ) t
    WHERE g.completed = true
)
SELECT
    gr.team_id,
    gr.school,
    t.conference,
    gr.season,

    -- Record
    COUNT(*) AS games,
    SUM(gr.won) AS wins,
    COUNT(*) - SUM(gr.won) AS losses,

    -- Conference record
    SUM(gr.won) FILTER (WHERE gr.conference_game) AS conf_wins,
    COUNT(*) FILTER (WHERE gr.conference_game) - SUM(gr.won) FILTER (WHERE gr.conference_game) AS conf_losses,

    -- Scoring
    ROUND(AVG(gr.points_for)::numeric, 1) AS ppg,
    ROUND(AVG(gr.points_against)::numeric, 1) AS opp_ppg,
    ROUND(AVG(gr.points_for - gr.points_against)::numeric, 1) AS avg_margin,

    -- Ratings (joined)
    sp.rating AS sp_rating,
    sp.ranking AS sp_rank,
    sp."offense__rating" AS sp_offense,
    sp."defense__rating" AS sp_defense,
    elo.elo,
    fpi.fpi,

    -- Recruiting
    tr.rank AS recruiting_rank,
    tr.points AS recruiting_points

FROM game_results gr
JOIN ref.teams t ON gr.team_id = t.id
LEFT JOIN ratings.sp_ratings sp ON t.school = sp.team AND gr.season = sp.year
LEFT JOIN ratings.elo_ratings elo ON t.school = elo.team AND gr.season = elo.year
LEFT JOIN ratings.fpi_ratings fpi ON t.school = fpi.team AND gr.season = fpi.year
LEFT JOIN recruiting.team_recruiting tr ON t.school = tr.team AND gr.season = tr.year
GROUP BY
    gr.team_id, gr.school, t.conference, gr.season,
    sp.rating, sp.ranking, sp."offense__rating", sp."defense__rating",
    elo.elo, fpi.fpi, tr.rank, tr.points;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.team_season_summary (team_id, season);

-- Query indexes
CREATE INDEX ON marts.team_season_summary (season);
CREATE INDEX ON marts.team_season_summary (conference);
CREATE INDEX ON marts.team_season_summary (sp_rank);
```

**Step 2: Test the view**

```sql
SELECT * FROM marts.team_season_summary
WHERE school = 'Alabama' AND season = 2024;
```

**Step 3: Commit**

```bash
git commit -m "feat: add team_season_summary materialized view"
```

---

### Task 3.2: Create _game_epa_calc Helper Mart

**Files:**
- Create: `src/schemas/marts/002_game_epa_calc.sql`

**Step 1: Write the helper view**

```sql
-- Helper: EPA calculations per game/team (excluding garbage time)
CREATE MATERIALIZED VIEW marts._game_epa_calc AS
SELECT
    p.game_id,
    t.id AS team_id,

    -- EPA/play (excluding garbage time)
    ROUND(AVG(p.ppa) FILTER (
        WHERE NOT is_garbage_time(p.period::integer, (p.offense_score - p.defense_score)::integer)
    )::numeric, 4) AS epa_per_play,

    -- Success rate: % of plays with positive EPA
    ROUND(AVG(CASE WHEN p.ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (
        WHERE NOT is_garbage_time(p.period::integer, (p.offense_score - p.defense_score)::integer)
    )::numeric, 4) AS success_rate,

    -- Explosiveness: avg EPA on successful plays only
    ROUND(AVG(p.ppa) FILTER (
        WHERE p.ppa > 0
        AND NOT is_garbage_time(p.period::integer, (p.offense_score - p.defense_score)::integer)
    )::numeric, 4) AS explosiveness,

    -- Play counts
    COUNT(*) FILTER (
        WHERE NOT is_garbage_time(p.period::integer, (p.offense_score - p.defense_score)::integer)
    ) AS plays_non_garbage,
    COUNT(*) AS plays_total

FROM core.plays p
JOIN ref.teams t ON p.offense = t.school
GROUP BY p.game_id, t.id;

CREATE UNIQUE INDEX ON marts._game_epa_calc (game_id, team_id);
```

**Step 2: Commit**

```bash
git commit -m "feat: add _game_epa_calc helper materialized view"
```

---

### Task 3.3: Create team_epa_season Mart

**Files:**
- Create: `src/schemas/marts/003_team_epa_season.sql`

**Step 1: Write the view**

```sql
-- Team EPA season summary with benchmarks
CREATE MATERIALIZED VIEW marts.team_epa_season AS
SELECT
    epa.team_id,
    t.school,
    g.season,

    -- Aggregated EPA metrics
    ROUND(AVG(epa.epa_per_play)::numeric, 4) AS epa_per_play,
    ROUND(AVG(epa.success_rate)::numeric, 4) AS success_rate,
    ROUND(AVG(epa.explosiveness)::numeric, 4) AS explosiveness,

    -- EPA tier benchmark
    CASE
        WHEN AVG(epa.epa_per_play) >= 0.16 THEN 'elite'
        WHEN AVG(epa.epa_per_play) >= 0.05 THEN 'above_avg'
        WHEN AVG(epa.epa_per_play) >= -0.05 THEN 'average'
        WHEN AVG(epa.epa_per_play) >= -0.15 THEN 'below_avg'
        ELSE 'struggling'
    END AS epa_tier,

    SUM(epa.plays_non_garbage) AS total_plays

FROM marts._game_epa_calc epa
JOIN core.games g ON epa.game_id = g.id
JOIN ref.teams t ON epa.team_id = t.id
GROUP BY epa.team_id, t.school, g.season;

CREATE UNIQUE INDEX ON marts.team_epa_season (team_id, season);
CREATE INDEX ON marts.team_epa_season (season, epa_tier);
```

**Step 2: Commit**

```bash
git commit -m "feat: add team_epa_season materialized view"
```

---

### Task 3.4: Create situational_splits Mart

**Files:**
- Create: `src/schemas/marts/004_situational_splits.sql`

This is the expanded situational metrics view from the design. Include:
- Down & distance splits
- Red zone metrics
- Field position metrics
- Late & close
- Two-minute drill
- Play type efficiency
- Power & stuff metrics

**Step 1: Write the full view (see design document for complete SQL)**

**Step 2: Commit**

```bash
git commit -m "feat: add situational_splits materialized view"
```

---

### Task 3.5: Create Remaining Marts

For each mart from the design document:
- `marts.defensive_havoc`
- `marts.scoring_opportunities`
- `marts.game_results`
- `marts.matchup_history`
- `marts.recruiting_class`
- `marts.coach_record`
- `marts.conference_standings`

Follow the same pattern:
1. Create SQL file in `src/schemas/marts/`
2. Include unique index for concurrent refresh
3. Add query indexes
4. Test with sample queries
5. Commit

---

## Phase 4: API Views Layer

### Task 4.1: Create team_detail API View

**Files:**
- Create: `src/schemas/api/001_team_detail.sql`

**Step 1: Write the view**

```sql
CREATE SCHEMA IF NOT EXISTS api;

CREATE OR REPLACE VIEW api.team_detail AS
SELECT
    t.id,
    t.school,
    t.mascot,
    t.abbreviation,
    t.color,
    t.alternate_color AS alt_color,
    logos.value AS logo_url,
    t.conference,

    -- Current season summary
    tss.season AS current_season,
    tss.wins,
    tss.losses,
    tss.conf_wins,
    tss.conf_losses,
    tss.ppg,
    tss.opp_ppg,
    tss.sp_rating,
    tss.sp_rank,

    -- EPA metrics
    epa.epa_per_play,
    epa.epa_tier,
    epa.success_rate,

    -- Recruiting
    tss.recruiting_rank,
    tss.recruiting_points

FROM ref.teams t
LEFT JOIN ref.teams__logos logos ON t._dlt_id = logos._dlt_parent_id AND logos._dlt_list_idx = 0
LEFT JOIN LATERAL (
    SELECT * FROM marts.team_season_summary
    WHERE team_id = t.id
    ORDER BY season DESC LIMIT 1
) tss ON true
LEFT JOIN LATERAL (
    SELECT * FROM marts.team_epa_season
    WHERE team_id = t.id
    ORDER BY season DESC LIMIT 1
) epa ON true;
```

**Step 2: Commit**

```bash
git commit -m "feat: add team_detail API view"
```

---

### Task 4.2: Create Remaining API Views

For each API view from the design:
- `api.team_history`
- `api.game_detail`
- `api.player_detail` (depends on rosters being loaded)
- `api.matchup`
- `api.leaderboard_teams`

---

## Phase 5: Refresh Script

### Task 5.1: Create Mart Refresh Script

**Files:**
- Create: `scripts/refresh_marts.py`

**Step 1: Write the script**

```python
#!/usr/bin/env python3
"""Refresh all materialized views in dependency order."""
import os
import psycopg2
from datetime import datetime

DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL")

# Order matters: dependencies first
MARTS = [
    "marts._game_epa_calc",
    "marts.team_season_summary",
    "marts.team_epa_season",
    "marts.situational_splits",
    "marts.defensive_havoc",
    "marts.scoring_opportunities",
    "marts.game_results",
    "marts.matchup_history",
    "marts.recruiting_class",
    "marts.coach_record",
    "marts.conference_standings",
]

def refresh_marts(concurrently: bool = True):
    """Refresh all materialized views."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    refresh_type = "CONCURRENTLY" if concurrently else ""

    for mart in MARTS:
        print(f"[{datetime.now()}] Refreshing {mart}...")
        try:
            cur.execute(f"REFRESH MATERIALIZED VIEW {refresh_type} {mart}")
            conn.commit()
            print(f"  ✓ {mart} refreshed")
        except Exception as e:
            print(f"  ✗ {mart} failed: {e}")
            conn.rollback()

    cur.close()
    conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-concurrent", action="store_true", help="Don't use CONCURRENTLY")
    args = parser.parse_args()

    refresh_marts(concurrently=not args.no_concurrent)
```

**Step 2: Commit**

```bash
git commit -m "feat: add mart refresh script"
```

---

## Phase 6: Backfill Data

### Task 6.1: Backfill Rosters

**Run:**
```bash
python -m src.pipelines.run --source rosters --years 2004 2005 2006 ... 2024
```

Note: This will make ~2,860 API calls (130 teams × 22 years). Budget allows.

### Task 6.2: Backfill Rankings

**Run:**
```bash
python -m src.pipelines.run --source rankings --years 2004 2005 ... 2024
```

### Task 6.3: Backfill Other Endpoints

For each newly wired endpoint, run the backfill.

### Task 6.4: Refresh All Marts

**Run:**
```bash
python scripts/refresh_marts.py
```

---

## Phase 7: Validation

### Task 7.1: Validate Team Season Summary

```sql
-- Spot check Alabama 2024
SELECT * FROM marts.team_season_summary
WHERE school = 'Alabama' AND season = 2024;

-- Should have wins, losses, ppg, sp_rating, etc.
```

### Task 7.2: Validate API Views

```sql
-- Test team detail
SELECT * FROM api.team_detail WHERE school = 'Georgia';

-- Test leaderboard
SELECT school, epa_per_play, epa_tier
FROM api.leaderboard_teams
WHERE season = 2024
ORDER BY epa_per_play DESC
LIMIT 10;
```

### Task 7.3: Test from Supabase Client

```typescript
// In a test script or app
const { data, error } = await supabase
  .from('team_detail')
  .select('*')
  .eq('school', 'Alabama')
  .single();

console.log(data);
```

---

## Summary

| Phase | Tasks | Est. Commits |
|-------|-------|--------------|
| 1. Endpoint Coverage | 5 tasks | ~8 commits |
| 2. Schema Hardening | 3 tasks | ~3 commits |
| 3. Marts Layer | 5+ tasks | ~10 commits |
| 4. API Views | 2+ tasks | ~5 commits |
| 5. Refresh Script | 1 task | 1 commit |
| 6. Backfill | 4 tasks | 0 commits (data only) |
| 7. Validation | 3 tasks | 0 commits |

**Total:** ~27 commits, organized into logical phases.

Each phase can be demoed independently:
- Phase 1: New data available in raw tables
- Phase 2-3: Analytics queries work
- Phase 4: API views available via PostgREST
- Phase 5-6: Full data loaded and marts populated
