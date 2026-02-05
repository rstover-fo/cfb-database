# CFB Scout Agent Phase 1 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build the foundational crawl → store → summarize → query loop using Reddit as the first data source.

**Architecture:** Python project with PRAW for Reddit API, Claude for summarization, and Supabase Postgres for storage. Scheduled crawling stores raw content, then a separate summarization job processes it into structured player/team data.

**Tech Stack:** Python 3.12, PRAW (Reddit API), Anthropic SDK (Claude), psycopg2/SQLAlchemy, httpx

---

## Task 1: Create Project Scaffold

**Files:**
- Create: `/Users/robstover/Development/personal/cfb-scout/pyproject.toml`
- Create: `/Users/robstover/Development/personal/cfb-scout/src/__init__.py`
- Create: `/Users/robstover/Development/personal/cfb-scout/src/config.py`
- Create: `/Users/robstover/Development/personal/cfb-scout/.env.example`

**Step 1: Create project directory structure**

```bash
mkdir -p /Users/robstover/Development/personal/cfb-scout/{src/{crawlers,processing,storage},tests,scripts}
touch /Users/robstover/Development/personal/cfb-scout/src/__init__.py
touch /Users/robstover/Development/personal/cfb-scout/src/crawlers/__init__.py
touch /Users/robstover/Development/personal/cfb-scout/src/processing/__init__.py
touch /Users/robstover/Development/personal/cfb-scout/src/storage/__init__.py
touch /Users/robstover/Development/personal/cfb-scout/tests/__init__.py
```

**Step 2: Create pyproject.toml**

```toml
[project]
name = "cfb-scout"
version = "0.1.0"
description = "CFB scouting intelligence agent"
requires-python = ">=3.12"
dependencies = [
    "praw>=7.7.0",
    "anthropic>=0.18.0",
    "psycopg2-binary>=2.9.9",
    "httpx>=0.27.0",
    "python-dotenv>=1.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "ruff>=0.3.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.pytest.ini_options]
testpaths = ["tests"]
```

**Step 3: Create config.py**

```python
"""Configuration management for CFB Scout."""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    """Application configuration."""

    # Supabase
    database_url: str

    # Reddit
    reddit_client_id: str
    reddit_client_secret: str
    reddit_user_agent: str

    # Anthropic
    anthropic_api_key: str

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        return cls(
            database_url=os.environ["DATABASE_URL"],
            reddit_client_id=os.environ["REDDIT_CLIENT_ID"],
            reddit_client_secret=os.environ["REDDIT_CLIENT_SECRET"],
            reddit_user_agent=os.environ.get("REDDIT_USER_AGENT", "cfb-scout:v0.1.0"),
            anthropic_api_key=os.environ["ANTHROPIC_API_KEY"],
        )


def get_config() -> Config:
    """Get application configuration."""
    return Config.from_env()
```

**Step 4: Create .env.example**

```bash
# Supabase (same as cfb-database)
DATABASE_URL=postgres://postgres:xxx@db.xxx.supabase.co:5432/postgres

# Reddit API (https://www.reddit.com/prefs/apps)
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=cfb-scout:v0.1.0

# Anthropic
ANTHROPIC_API_KEY=your_api_key
```

**Step 5: Initialize git and commit**

```bash
cd /Users/robstover/Development/personal/cfb-scout
git init
cp .env.example .env
echo ".env" >> .gitignore
echo "__pycache__" >> .gitignore
echo "*.pyc" >> .gitignore
echo ".venv" >> .gitignore
git add .
git commit -m "feat: initialize cfb-scout project scaffold"
```

---

## Task 2: Deploy Scouting Schema to Supabase

**Files:**
- Create: `/Users/robstover/Development/personal/cfb-scout/src/storage/schema.sql`

**Step 1: Create schema SQL file**

```sql
-- CFB Scout Schema
-- Deploy to Supabase using SQL Editor or migration tool

CREATE SCHEMA IF NOT EXISTS scouting;

-- Raw crawled content
CREATE TABLE IF NOT EXISTS scouting.reports (
    id SERIAL PRIMARY KEY,
    source_url TEXT NOT NULL,
    source_name TEXT NOT NULL,
    published_at TIMESTAMPTZ,
    crawled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    content_type TEXT NOT NULL CHECK (content_type IN ('article', 'social', 'forum')),
    player_ids BIGINT[] DEFAULT '{}',
    team_ids TEXT[] DEFAULT '{}',
    raw_text TEXT NOT NULL,
    summary TEXT,
    sentiment_score NUMERIC(3,2) CHECK (sentiment_score BETWEEN -1 AND 1),
    processed_at TIMESTAMPTZ,
    UNIQUE (source_url)
);

CREATE INDEX idx_reports_source ON scouting.reports (source_name);
CREATE INDEX idx_reports_crawled ON scouting.reports (crawled_at DESC);
CREATE INDEX idx_reports_unprocessed ON scouting.reports (id) WHERE processed_at IS NULL;
CREATE INDEX idx_reports_players ON scouting.reports USING GIN (player_ids);
CREATE INDEX idx_reports_teams ON scouting.reports USING GIN (team_ids);

-- Player scouting profiles
CREATE TABLE IF NOT EXISTS scouting.players (
    id SERIAL PRIMARY KEY,
    roster_player_id BIGINT,  -- Links to core.roster.id
    recruit_id BIGINT,        -- Links to recruiting.recruits.id
    name TEXT NOT NULL,
    position TEXT,
    team TEXT,
    class_year INT,
    current_status TEXT CHECK (current_status IN ('recruit', 'active', 'transfer', 'draft_eligible', 'drafted')),
    composite_grade INT CHECK (composite_grade BETWEEN 0 AND 100),
    traits JSONB DEFAULT '{}',
    draft_projection TEXT,
    comps TEXT[] DEFAULT '{}',
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (name, team, class_year)
);

CREATE INDEX idx_players_team ON scouting.players (team);
CREATE INDEX idx_players_status ON scouting.players (current_status);
CREATE INDEX idx_players_grade ON scouting.players (composite_grade DESC NULLS LAST);

-- Player timeline for longitudinal tracking
CREATE TABLE IF NOT EXISTS scouting.player_timeline (
    id SERIAL PRIMARY KEY,
    player_id INT NOT NULL REFERENCES scouting.players(id) ON DELETE CASCADE,
    snapshot_date DATE NOT NULL,
    status TEXT,
    sentiment_score NUMERIC(3,2),
    grade_at_time INT,
    traits_at_time JSONB,
    key_narratives TEXT[] DEFAULT '{}',
    sources_count INT DEFAULT 0,
    UNIQUE (player_id, snapshot_date)
);

CREATE INDEX idx_timeline_player ON scouting.player_timeline (player_id);
CREATE INDEX idx_timeline_date ON scouting.player_timeline (snapshot_date DESC);

-- Team roster analysis
CREATE TABLE IF NOT EXISTS scouting.team_rosters (
    id SERIAL PRIMARY KEY,
    team TEXT NOT NULL,
    season INT NOT NULL,
    position_groups JSONB DEFAULT '{}',
    overall_sentiment NUMERIC(3,2),
    trajectory TEXT CHECK (trajectory IN ('improving', 'stable', 'declining')),
    key_storylines TEXT[] DEFAULT '{}',
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (team, season)
);

CREATE INDEX idx_team_rosters_team ON scouting.team_rosters (team);
CREATE INDEX idx_team_rosters_season ON scouting.team_rosters (season DESC);

-- Crawl job tracking
CREATE TABLE IF NOT EXISTS scouting.crawl_jobs (
    id SERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed')),
    records_crawled INT DEFAULT 0,
    records_new INT DEFAULT 0,
    error_message TEXT
);

CREATE INDEX idx_crawl_jobs_source ON scouting.crawl_jobs (source_name, started_at DESC);
```

**Step 2: Deploy schema to Supabase**

Run in Supabase SQL Editor or via psql:
```bash
cd /Users/robstover/Development/personal/cfb-scout
psql "postgres://postgres:sittar-3fIzgy-horkak@db.ibobsbwlewpqslkqbrjd.supabase.co:5432/postgres" -f src/storage/schema.sql
```

Expected: Schema created successfully, no errors.

**Step 3: Verify tables exist**

```sql
SELECT table_name FROM information_schema.tables WHERE table_schema = 'scouting';
```

Expected: reports, players, player_timeline, team_rosters, crawl_jobs

**Step 4: Commit**

```bash
git add src/storage/schema.sql
git commit -m "feat: add scouting schema for Supabase"
```

---

## Task 3: Create Database Connection Module

**Files:**
- Create: `/Users/robstover/Development/personal/cfb-scout/src/storage/db.py`
- Create: `/Users/robstover/Development/personal/cfb-scout/tests/test_db.py`

**Step 1: Write the failing test**

```python
# tests/test_db.py
"""Tests for database connection."""

import pytest
from src.storage.db import get_connection, insert_report, get_unprocessed_reports


def test_get_connection_returns_connection():
    """Test that we can connect to the database."""
    conn = get_connection()
    assert conn is not None
    cur = conn.cursor()
    cur.execute("SELECT 1")
    result = cur.fetchone()
    assert result[0] == 1
    conn.close()


def test_insert_report_creates_record():
    """Test that we can insert a report."""
    conn = get_connection()

    report_id = insert_report(
        conn,
        source_url="https://reddit.com/r/CFB/test123",
        source_name="reddit",
        content_type="forum",
        raw_text="Test content about Texas football",
        team_ids=["Texas"],
    )

    assert report_id is not None
    assert report_id > 0

    # Clean up
    cur = conn.cursor()
    cur.execute("DELETE FROM scouting.reports WHERE id = %s", (report_id,))
    conn.commit()
    conn.close()
```

**Step 2: Run test to verify it fails**

```bash
cd /Users/robstover/Development/personal/cfb-scout
python -m pytest tests/test_db.py -v
```

Expected: FAIL with "ModuleNotFoundError: No module named 'src.storage.db'"

**Step 3: Write minimal implementation**

```python
# src/storage/db.py
"""Database connection and operations for CFB Scout."""

import os
from contextlib import contextmanager
from typing import Iterator

import psycopg2
from psycopg2.extensions import connection


def get_connection() -> connection:
    """Get a database connection."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    return psycopg2.connect(database_url)


@contextmanager
def get_db() -> Iterator[connection]:
    """Context manager for database connections."""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


def insert_report(
    conn: connection,
    source_url: str,
    source_name: str,
    content_type: str,
    raw_text: str,
    player_ids: list[int] | None = None,
    team_ids: list[str] | None = None,
    published_at: str | None = None,
) -> int:
    """Insert a scouting report and return its ID.

    Uses ON CONFLICT to handle duplicates (same URL).
    """
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO scouting.reports
            (source_url, source_name, content_type, raw_text, player_ids, team_ids, published_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_url) DO UPDATE SET
            raw_text = EXCLUDED.raw_text,
            crawled_at = NOW()
        RETURNING id
        """,
        (
            source_url,
            source_name,
            content_type,
            raw_text,
            player_ids or [],
            team_ids or [],
            published_at,
        ),
    )
    report_id = cur.fetchone()[0]
    conn.commit()
    return report_id


def get_unprocessed_reports(conn: connection, limit: int = 100) -> list[dict]:
    """Get reports that haven't been processed yet."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, source_url, source_name, content_type, raw_text, player_ids, team_ids
        FROM scouting.reports
        WHERE processed_at IS NULL
        ORDER BY crawled_at ASC
        LIMIT %s
        """,
        (limit,),
    )
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def mark_report_processed(
    conn: connection,
    report_id: int,
    summary: str | None = None,
    sentiment_score: float | None = None,
    player_ids: list[int] | None = None,
    team_ids: list[str] | None = None,
) -> None:
    """Mark a report as processed with optional extracted data."""
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE scouting.reports
        SET processed_at = NOW(),
            summary = COALESCE(%s, summary),
            sentiment_score = COALESCE(%s, sentiment_score),
            player_ids = COALESCE(%s, player_ids),
            team_ids = COALESCE(%s, team_ids)
        WHERE id = %s
        """,
        (summary, sentiment_score, player_ids, team_ids, report_id),
    )
    conn.commit()
```

**Step 4: Set up environment and run test**

```bash
# Create .env with real credentials
cp .env.example .env
# Edit .env with actual DATABASE_URL

# Create venv and install
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run tests
python -m pytest tests/test_db.py -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add src/storage/db.py tests/test_db.py
git commit -m "feat: add database connection module with report operations"
```

---

## Task 4: Create Reddit Crawler

**Files:**
- Create: `/Users/robstover/Development/personal/cfb-scout/src/crawlers/base.py`
- Create: `/Users/robstover/Development/personal/cfb-scout/src/crawlers/reddit.py`
- Create: `/Users/robstover/Development/personal/cfb-scout/tests/test_reddit_crawler.py`

**Step 1: Create base crawler class**

```python
# src/crawlers/base.py
"""Base crawler class with common functionality."""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class CrawlResult:
    """Result of a crawl operation."""
    source_name: str
    records_crawled: int
    records_new: int
    errors: list[str]
    started_at: datetime
    completed_at: datetime


class BaseCrawler(ABC):
    """Base class for all crawlers."""

    source_name: str = "unknown"

    @abstractmethod
    def crawl(self) -> CrawlResult:
        """Execute the crawl and return results."""
        pass

    def log_start(self) -> datetime:
        """Log crawl start."""
        started = datetime.now()
        logger.info(f"Starting {self.source_name} crawl at {started}")
        return started

    def log_complete(self, result: CrawlResult) -> None:
        """Log crawl completion."""
        logger.info(
            f"Completed {self.source_name} crawl: "
            f"{result.records_new} new / {result.records_crawled} total"
        )
```

**Step 2: Write failing test for Reddit crawler**

```python
# tests/test_reddit_crawler.py
"""Tests for Reddit crawler."""

import pytest
from unittest.mock import Mock, patch

from src.crawlers.reddit import RedditCrawler, extract_team_mentions


def test_extract_team_mentions_finds_teams():
    """Test that team mentions are extracted from text."""
    text = "Texas is looking strong this year. Ohio State will be tough to beat."
    teams = extract_team_mentions(text)
    assert "Texas" in teams
    assert "Ohio State" in teams


def test_extract_team_mentions_handles_variations():
    """Test that team name variations are handled."""
    text = "The Longhorns are playing the Buckeyes this weekend."
    teams = extract_team_mentions(text)
    assert "Texas" in teams
    assert "Ohio State" in teams


def test_reddit_crawler_parses_submission():
    """Test that Reddit submissions are parsed correctly."""
    crawler = RedditCrawler(subreddits=["CFB"])

    mock_submission = Mock()
    mock_submission.id = "abc123"
    mock_submission.title = "Texas QB situation looking good"
    mock_submission.selftext = "After spring practice, the QB room is stacked."
    mock_submission.url = "https://reddit.com/r/CFB/comments/abc123"
    mock_submission.created_utc = 1706745600.0
    mock_submission.score = 150

    result = crawler._parse_submission(mock_submission)

    assert result["source_url"] == "https://reddit.com/r/CFB/comments/abc123"
    assert result["content_type"] == "forum"
    assert "Texas" in result["raw_text"]
    assert "Texas" in result["team_ids"]
```

**Step 3: Run test to verify it fails**

```bash
python -m pytest tests/test_reddit_crawler.py -v
```

Expected: FAIL with "ModuleNotFoundError"

**Step 4: Write Reddit crawler implementation**

```python
# src/crawlers/reddit.py
"""Reddit crawler for r/CFB and team subreddits."""

import logging
import os
import re
from datetime import datetime, timezone

import praw
from praw.models import Submission

from .base import BaseCrawler, CrawlResult
from ..storage.db import get_connection, insert_report

logger = logging.getLogger(__name__)

# Team name mappings (common variations)
TEAM_ALIASES = {
    "longhorns": "Texas",
    "horns": "Texas",
    "buckeyes": "Ohio State",
    "osu": "Ohio State",
    "bucks": "Ohio State",
    "dawgs": "Georgia",
    "bulldogs": "Georgia",
    "uga": "Georgia",
    "tide": "Alabama",
    "bama": "Alabama",
    "crimson tide": "Alabama",
    "wolverines": "Michigan",
    "umich": "Michigan",
    "tigers": "LSU",  # Could also be Clemson, Auburn - context needed
    "sooners": "Oklahoma",
    "ou": "Oklahoma",
    "aggies": "Texas A&M",
    "tamu": "Texas A&M",
    "nittany lions": "Penn State",
    "psu": "Penn State",
    "ducks": "Oregon",
    "trojans": "USC",
    "irish": "Notre Dame",
    "nd": "Notre Dame",
    "gators": "Florida",
    "uf": "Florida",
    "seminoles": "Florida State",
    "fsu": "Florida State",
    "hurricanes": "Miami",
    "canes": "Miami",
}

# Canonical team names to look for
FBS_TEAMS = [
    "Alabama", "Arizona", "Arizona State", "Arkansas", "Auburn",
    "Baylor", "BYU", "California", "Cincinnati", "Clemson",
    "Colorado", "Duke", "Florida", "Florida State", "Georgia",
    "Georgia Tech", "Houston", "Illinois", "Indiana", "Iowa",
    "Iowa State", "Kansas", "Kansas State", "Kentucky", "Louisville",
    "LSU", "Maryland", "Memphis", "Miami", "Michigan",
    "Michigan State", "Minnesota", "Mississippi State", "Missouri", "Nebraska",
    "North Carolina", "NC State", "Notre Dame", "Ohio State", "Oklahoma",
    "Oklahoma State", "Ole Miss", "Oregon", "Oregon State", "Penn State",
    "Pittsburgh", "Purdue", "Rutgers", "SMU", "South Carolina",
    "Stanford", "Syracuse", "TCU", "Tennessee", "Texas",
    "Texas A&M", "Texas Tech", "UCF", "UCLA", "USC",
    "Utah", "Vanderbilt", "Virginia", "Virginia Tech", "Wake Forest",
    "Washington", "Washington State", "West Virginia", "Wisconsin",
]


def extract_team_mentions(text: str) -> list[str]:
    """Extract team mentions from text.

    Looks for canonical team names and common aliases.
    """
    text_lower = text.lower()
    teams_found = set()

    # Check aliases first
    for alias, canonical in TEAM_ALIASES.items():
        if re.search(rf'\b{re.escape(alias)}\b', text_lower):
            teams_found.add(canonical)

    # Check canonical names
    for team in FBS_TEAMS:
        if re.search(rf'\b{re.escape(team.lower())}\b', text_lower):
            teams_found.add(team)

    return list(teams_found)


class RedditCrawler(BaseCrawler):
    """Crawler for Reddit CFB content."""

    source_name = "reddit"

    def __init__(
        self,
        subreddits: list[str] | None = None,
        post_limit: int = 100,
    ):
        """Initialize Reddit crawler.

        Args:
            subreddits: List of subreddits to crawl. Defaults to ["CFB"].
            post_limit: Max posts to fetch per subreddit.
        """
        self.subreddits = subreddits or ["CFB"]
        self.post_limit = post_limit
        self._reddit = None

    @property
    def reddit(self) -> praw.Reddit:
        """Lazy-load Reddit client."""
        if self._reddit is None:
            self._reddit = praw.Reddit(
                client_id=os.environ["REDDIT_CLIENT_ID"],
                client_secret=os.environ["REDDIT_CLIENT_SECRET"],
                user_agent=os.environ.get("REDDIT_USER_AGENT", "cfb-scout:v0.1.0"),
            )
        return self._reddit

    def _parse_submission(self, submission: Submission) -> dict:
        """Parse a Reddit submission into report format."""
        # Combine title and body
        raw_text = f"{submission.title}\n\n{submission.selftext or ''}"

        # Extract teams mentioned
        team_ids = extract_team_mentions(raw_text)

        # Convert timestamp
        published_at = datetime.fromtimestamp(
            submission.created_utc, tz=timezone.utc
        ).isoformat()

        return {
            "source_url": f"https://reddit.com{submission.permalink}",
            "source_name": self.source_name,
            "content_type": "forum",
            "raw_text": raw_text,
            "team_ids": team_ids,
            "published_at": published_at,
        }

    def crawl(self) -> CrawlResult:
        """Crawl configured subreddits for CFB content."""
        started = self.log_start()
        errors = []
        records_crawled = 0
        records_new = 0

        conn = get_connection()

        try:
            for subreddit_name in self.subreddits:
                logger.info(f"Crawling r/{subreddit_name}")
                subreddit = self.reddit.subreddit(subreddit_name)

                # Get hot posts
                for submission in subreddit.hot(limit=self.post_limit):
                    try:
                        parsed = self._parse_submission(submission)
                        records_crawled += 1

                        # Only store if it mentions a team
                        if parsed["team_ids"]:
                            report_id = insert_report(conn, **parsed)
                            if report_id:
                                records_new += 1

                    except Exception as e:
                        errors.append(f"Error parsing {submission.id}: {e}")
                        logger.warning(f"Error parsing submission: {e}")

        finally:
            conn.close()

        completed = datetime.now()
        result = CrawlResult(
            source_name=self.source_name,
            records_crawled=records_crawled,
            records_new=records_new,
            errors=errors,
            started_at=started,
            completed_at=completed,
        )

        self.log_complete(result)
        return result
```

**Step 5: Run tests**

```bash
python -m pytest tests/test_reddit_crawler.py -v
```

Expected: PASS

**Step 6: Commit**

```bash
git add src/crawlers/base.py src/crawlers/reddit.py tests/test_reddit_crawler.py
git commit -m "feat: add Reddit crawler with team mention extraction"
```

---

## Task 5: Create Claude Summarization Module

**Files:**
- Create: `/Users/robstover/Development/personal/cfb-scout/src/processing/summarizer.py`
- Create: `/Users/robstover/Development/personal/cfb-scout/tests/test_summarizer.py`

**Step 1: Write failing test**

```python
# tests/test_summarizer.py
"""Tests for Claude summarization."""

import pytest
from unittest.mock import Mock, patch

from src.processing.summarizer import extract_sentiment, summarize_report


def test_extract_sentiment_positive():
    """Test sentiment extraction for positive content."""
    text = "Texas looks amazing this year. The offense is explosive and the defense is elite."
    sentiment = extract_sentiment(text)
    assert sentiment > 0.3  # Clearly positive


def test_extract_sentiment_negative():
    """Test sentiment extraction for negative content."""
    text = "Ohio State is struggling. The injuries are piling up and morale is low."
    sentiment = extract_sentiment(text)
    assert sentiment < -0.3  # Clearly negative


def test_extract_sentiment_neutral():
    """Test sentiment extraction for neutral content."""
    text = "The game is scheduled for Saturday at 3pm. Weather looks clear."
    sentiment = extract_sentiment(text)
    assert -0.3 <= sentiment <= 0.3  # Neutral range
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_summarizer.py -v
```

Expected: FAIL

**Step 3: Write summarizer implementation**

```python
# src/processing/summarizer.py
"""Claude-powered summarization for scouting content."""

import json
import logging
import os
from typing import TypedDict

import anthropic

logger = logging.getLogger(__name__)


class SummaryResult(TypedDict):
    """Result of summarization."""
    summary: str
    sentiment_score: float
    player_mentions: list[str]
    team_mentions: list[str]
    key_topics: list[str]


def get_client() -> anthropic.Anthropic:
    """Get Anthropic client."""
    return anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


def extract_sentiment(text: str) -> float:
    """Extract sentiment score from text using Claude.

    Returns a score from -1 (very negative) to 1 (very positive).
    """
    client = get_client()

    response = client.messages.create(
        model="claude-3-haiku-20240307",  # Fast/cheap for simple tasks
        max_tokens=50,
        messages=[
            {
                "role": "user",
                "content": f"""Analyze the sentiment of this college football text.
Return ONLY a number between -1.0 (very negative) and 1.0 (very positive).

Text: {text[:1000]}

Sentiment score:"""
            }
        ],
    )

    try:
        score = float(response.content[0].text.strip())
        return max(-1.0, min(1.0, score))  # Clamp to valid range
    except (ValueError, IndexError):
        logger.warning(f"Failed to parse sentiment: {response.content}")
        return 0.0


def summarize_report(text: str, team_context: list[str] | None = None) -> SummaryResult:
    """Summarize a scouting report using Claude.

    Args:
        text: The raw report text.
        team_context: Optional list of teams mentioned for context.

    Returns:
        SummaryResult with summary, sentiment, and extracted entities.
    """
    client = get_client()

    context = ""
    if team_context:
        context = f"Teams mentioned: {', '.join(team_context)}\n\n"

    response = client.messages.create(
        model="claude-3-haiku-20240307",
        max_tokens=500,
        messages=[
            {
                "role": "user",
                "content": f"""Analyze this college football content and extract key information.

{context}Text:
{text[:2000]}

Respond with JSON only:
{{
    "summary": "2-3 sentence summary of the key points",
    "sentiment_score": <float from -1.0 to 1.0>,
    "player_mentions": ["list", "of", "player", "names"],
    "team_mentions": ["list", "of", "team", "names"],
    "key_topics": ["recruiting", "transfer_portal", "injury", "performance", etc.]
}}"""
            }
        ],
    )

    try:
        # Extract JSON from response
        response_text = response.content[0].text.strip()
        # Handle potential markdown code blocks
        if response_text.startswith("```"):
            response_text = response_text.split("```")[1]
            if response_text.startswith("json"):
                response_text = response_text[4:]

        result = json.loads(response_text)
        return SummaryResult(
            summary=result.get("summary", ""),
            sentiment_score=float(result.get("sentiment_score", 0)),
            player_mentions=result.get("player_mentions", []),
            team_mentions=result.get("team_mentions", []),
            key_topics=result.get("key_topics", []),
        )
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning(f"Failed to parse summary response: {e}")
        return SummaryResult(
            summary="",
            sentiment_score=0.0,
            player_mentions=[],
            team_mentions=[],
            key_topics=[],
        )
```

**Step 4: Run tests**

```bash
python -m pytest tests/test_summarizer.py -v
```

Expected: PASS (requires valid ANTHROPIC_API_KEY in .env)

**Step 5: Commit**

```bash
git add src/processing/summarizer.py tests/test_summarizer.py
git commit -m "feat: add Claude-powered summarization module"
```

---

## Task 6: Create Processing Pipeline

**Files:**
- Create: `/Users/robstover/Development/personal/cfb-scout/src/processing/pipeline.py`
- Create: `/Users/robstover/Development/personal/cfb-scout/scripts/run_pipeline.py`

**Step 1: Create processing pipeline**

```python
# src/processing/pipeline.py
"""Processing pipeline to summarize crawled reports."""

import logging
from datetime import datetime

from ..storage.db import get_connection, get_unprocessed_reports, mark_report_processed
from .summarizer import summarize_report

logger = logging.getLogger(__name__)


def process_reports(batch_size: int = 50) -> dict:
    """Process unprocessed reports through Claude summarization.

    Args:
        batch_size: Number of reports to process in this run.

    Returns:
        Dict with processing stats.
    """
    conn = get_connection()

    try:
        reports = get_unprocessed_reports(conn, limit=batch_size)
        logger.info(f"Found {len(reports)} unprocessed reports")

        processed = 0
        errors = 0

        for report in reports:
            try:
                result = summarize_report(
                    text=report["raw_text"],
                    team_context=report["team_ids"],
                )

                mark_report_processed(
                    conn,
                    report_id=report["id"],
                    summary=result["summary"],
                    sentiment_score=result["sentiment_score"],
                )

                processed += 1
                logger.debug(f"Processed report {report['id']}")

            except Exception as e:
                errors += 1
                logger.error(f"Error processing report {report['id']}: {e}")

        return {
            "total": len(reports),
            "processed": processed,
            "errors": errors,
            "timestamp": datetime.now().isoformat(),
        }

    finally:
        conn.close()
```

**Step 2: Create run script**

```python
#!/usr/bin/env python3
# scripts/run_pipeline.py
"""Run the CFB Scout pipeline."""

import argparse
import logging
import sys

from dotenv import load_dotenv

load_dotenv()

from src.crawlers.reddit import RedditCrawler
from src.processing.pipeline import process_reports

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run CFB Scout pipeline")
    parser.add_argument(
        "--crawl",
        action="store_true",
        help="Run Reddit crawler",
    )
    parser.add_argument(
        "--process",
        action="store_true",
        help="Process unprocessed reports",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run full pipeline (crawl + process)",
    )
    parser.add_argument(
        "--subreddits",
        nargs="+",
        default=["CFB"],
        help="Subreddits to crawl (default: CFB)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max posts to crawl per subreddit",
    )

    args = parser.parse_args()

    if not any([args.crawl, args.process, args.all]):
        parser.print_help()
        sys.exit(1)

    if args.crawl or args.all:
        logger.info("Starting Reddit crawl...")
        crawler = RedditCrawler(
            subreddits=args.subreddits,
            post_limit=args.limit,
        )
        result = crawler.crawl()
        logger.info(f"Crawl complete: {result.records_new} new records")

    if args.process or args.all:
        logger.info("Processing reports...")
        result = process_reports()
        logger.info(f"Processing complete: {result['processed']}/{result['total']} reports")


if __name__ == "__main__":
    main()
```

**Step 3: Make script executable and test**

```bash
chmod +x scripts/run_pipeline.py

# Test crawl only
python scripts/run_pipeline.py --crawl --limit 10

# Test process only
python scripts/run_pipeline.py --process

# Test full pipeline
python scripts/run_pipeline.py --all --limit 10
```

Expected: Pipeline runs successfully, reports crawled and processed.

**Step 4: Commit**

```bash
git add src/processing/pipeline.py scripts/run_pipeline.py
git commit -m "feat: add processing pipeline and CLI runner"
```

---

## Task 7: Verify End-to-End and Document

**Files:**
- Create: `/Users/robstover/Development/personal/cfb-scout/README.md`

**Step 1: Run full pipeline test**

```bash
cd /Users/robstover/Development/personal/cfb-scout
source .venv/bin/activate

# Run full pipeline
python scripts/run_pipeline.py --all --subreddits CFB LonghornNation --limit 25
```

**Step 2: Verify data in Supabase**

```sql
-- Check reports were crawled
SELECT COUNT(*), source_name FROM scouting.reports GROUP BY source_name;

-- Check some were processed
SELECT COUNT(*) FROM scouting.reports WHERE processed_at IS NOT NULL;

-- View a processed report
SELECT source_url, team_ids, summary, sentiment_score
FROM scouting.reports
WHERE processed_at IS NOT NULL
LIMIT 5;
```

**Step 3: Create README**

```markdown
# CFB Scout

AI-powered college football scouting intelligence agent.

## Setup

1. Create virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -e ".[dev]"
   ```

2. Configure environment:
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

3. Deploy schema (first time only):
   ```bash
   psql "$DATABASE_URL" -f src/storage/schema.sql
   ```

## Usage

```bash
# Crawl Reddit
python scripts/run_pipeline.py --crawl --subreddits CFB LonghornNation

# Process unprocessed reports
python scripts/run_pipeline.py --process

# Run full pipeline
python scripts/run_pipeline.py --all
```

## Testing

```bash
pytest tests/ -v
```

## Project Structure

```
cfb-scout/
├── src/
│   ├── crawlers/       # Data source crawlers
│   ├── processing/     # Claude summarization
│   └── storage/        # Database operations
├── scripts/            # CLI tools
└── tests/              # Test suite
```
```

**Step 4: Final commit**

```bash
git add README.md
git commit -m "docs: add README with setup and usage instructions"
```

---

## Success Criteria Checklist

- [ ] Schema deployed to Supabase (`scouting.*` tables exist)
- [ ] Reddit crawler running (`python scripts/run_pipeline.py --crawl` succeeds)
- [ ] Reports stored with team tags (`SELECT * FROM scouting.reports LIMIT 5`)
- [ ] Claude summarization working (`processed_at IS NOT NULL` for some reports)
- [ ] End-to-end pipeline runs (`--all` flag works)
- [ ] Tests pass (`pytest tests/ -v`)
