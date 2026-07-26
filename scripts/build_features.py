#!/usr/bin/env python3
"""Build features.team_week, the as-of feature vector for fitted_v1.

Design doc: docs/brainstorms/2026-07-21-team-week-feature-design.md (section
1 fixes every column/source/leak-rule; section 0 fixes the week_index/as-of
convention). Target DDL: src/schemas/migrations/028_features_schema.sql.

Grain: one row per (season, season_type, week, team) -- a team plays <= 1
game/week, so both the home and away side of every core.games row (completed
or scheduled) get a row. week_index is the derived monotone ordering key
(week_index = week for season_type='regular', 100 + week for 'postseason',
since CFBD restarts week numbering at 1 for bowls -- see compute_week_index
below and migration 027's identical convention). As-of rule: the row keyed to
week_index = WI may only use data with week_index < WI within the same
season, plus explicitly leak-free preseason constants and prior-season (S-1)
fallbacks (design doc section 0).

Architecture: most of the build is one big SQL INSERT ... SELECT per season
(FEATURE_ROWS_QUERY) -- spine, games_played_to_date, season-to-date EPA/havoc
aggregates (both via pre-aggregated-by-week-index CTEs + a LATERAL "sum over
week_index < WI" join, so no row-wise Python loop is needed for those), and
the preseason constants (marts.returning_production, ratings.sp_ratings).
Two things are deliberately done in Python instead, because their leak-rule
fallback ladders are branchy per-row logic that reads far more clearly (and
is far more testable, see tests/test_build_features.py) as plain functions
than as nested SQL CASE/COALESCE chains:

  - Adjusted-EPA resolution (design doc section 1c): resolve_adj_epa() picks
    the greatest as-of-week fit with enough plays, else the prior-season
    full fit, else NULL. The SQL query does not touch
    analytics.adjusted_epa_week_build / analytics.adjusted_epa_build at all;
    build_season_rows() fetches both tables once per season and resolves
    each spine row against them in Python.
  - Elo fallback for upcoming games (design doc section 1b): the SQL query
    reads analytics.house_elo_game's pregame Elo for completed games (walk-
    forward by construction); when no house_elo_game row exists yet (games
    not yet computed by compute_house_elo.py -- typically upcoming/scheduled
    games), resolve_team_week_elo() falls back to
    scripts.compute_predictions.resolve_elo, the SAME carryover/SEED
    function compute_predictions.py uses for upcoming-game scoring, imported
    here rather than re-derived.

Write: idempotent per-season DELETE + INSERT, one commit per season (same
pattern as scripts/compute_house_elo.py / scripts/compute_adjusted_epa.py).

Usage:
    python scripts/build_features.py --from 2015
        Backfill every season from YYYY through the latest season with a
        published schedule (get_projection_seasons, NOT the calendar-based
        get_current_season -- which returns year-1 until August and would
        silently stop a July backfill one season short of the one being asked
        about). The design doc's 2015+ backfill scope. A season with no
        core.games rows is a clean no-op (not a failure).

    python scripts/build_features.py --season 2024
        Build a single season.

    python scripts/build_features.py --incremental
        Shorthand for `--season <current season>`. What the daily workflow
        runs.

Each season prints a machine-readable gate line after writing:
    FEATURES_GATE season={s} rows={n} null_elo={a} null_adj_epa={b}
    adj_src_week={c} adj_src_prior={d} null_std={e} week1_rows={f}
"""

import argparse
import logging
import sys
from collections import defaultdict

from scripts.compute_predictions import fetch_elo_current, resolve_elo

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Postseason week numbering restarts at 1 (bowls are week 1 of the
# postseason), so raw `week` cannot order a season monotonically -- shared
# convention with migration 027 / analytics.adjusted_epa_week_build (design
# doc section 0).
POSTSEASON_WEEK_OFFSET = 100

# Adjusted-EPA as-of-week fallback predicate (design doc section 1c): use the
# as-of week fit iff its `plays` (team offensive play count) is at least
# this many; below it, the prior-season fallback is used instead. In
# practice this routes entering weeks 1-2 to the fallback and entering week
# 3+ to the as-of week fit.
MIN_TEAM_PLAYS = 150

# Written to features.team_week.feature_build_version for every row this
# script writes (audit trail for which build produced a row).
FEATURE_BUILD_VERSION = "tw_v2"

# Recruiting-class window for the migration-042 columns. These MUST match
# scripts/screen_preseason_features.py: the partial correlations that justified
# shipping these columns were measured under exactly this decay and window, so
# a drift here would leave the feature table populated with a quantity the
# screen never evaluated. Duplicated rather than imported so a production build
# script does not depend on a research script; tests/test_build_features.py
# asserts the two stay equal.
CLASS_DECAY = 0.8
CLASS_WINDOW = 4

# Career-prior boundary separating a "proven" incoming head coach from an
# unproven one, on the SP+ scale where 0.0 is an average FBS team. Same
# duplicate-and-test rule as CLASS_DECAY above: it MUST equal the screen's
# HC_PROVEN_SP_PLUS, because hc_first_year_unproven's -0.1844 partial was
# measured at exactly this cut.
HC_PROVEN_SP_PLUS = 0.0

# The migration-042 and -046 preseason columns, reported individually in
# FEATURES_GATE.
PRESEASON_042_COLUMNS = (
    "recruiting_points_3yr",
    "blue_chip_pipeline",
    "hc_first_year",
    "hc_first_year_unproven",
    "draft_picks_3yr",
    "draft_departures",
    "prior_def_line_yards",
    "prior_def_stuff_rate",
)


# =============================================================================
# Pure helpers -- no I/O, no DB, unit-tested directly (tests/test_build_features.py).
# =============================================================================


def compute_week_index(week: int, season_type: str) -> int:
    """Derive the monotone `week_index` ordering key (design doc section 0).

    Mirrors the SQL `CASE WHEN season_type = 'postseason' THEN 100 + week
    ELSE week END` used throughout FEATURE_ROWS_QUERY below and in migration
    026's analytics.adjusted_epa_week_build -- kept here as the single
    documented, testable statement of the convention.
    """
    if season_type == "postseason":
        return POSTSEASON_WEEK_OFFSET + week
    return week


def leak_free_week_index(row_week_index: int, as_of_week_index: int) -> bool:
    """As-of predicate (design doc section 0): a row (a play, a game, a
    fitted coefficient) is safe to fold into a team-week keyed at
    `as_of_week_index` iff it happened strictly before that week, within the
    same season. `row_week_index == as_of_week_index` -- the team's own game
    that week -- is excluded; that is precisely what stops a game's own
    result from leaking into its own pregame feature row. Documents the
    predicate FEATURE_ROWS_QUERY's LATERAL joins implement in SQL
    (`week_index < s.week_index`) as a standalone, testable statement.
    """
    return row_week_index < as_of_week_index


def games_played_to_date(team_games: list[dict], week_index: int) -> int:
    """Count of COMPLETED games for one team with `week_index < week_index`,
    same season (design doc section 1a). Each dict in `team_games` needs
    `week_index` and `completed`. Genuinely 0 (never None/NULL) when the
    team has no qualifying prior games -- e.g. every team's week-1 row.
    """
    return sum(1 for g in team_games if g["completed"] and g["week_index"] < week_index)


def resolve_adj_epa(
    team: str,
    season: int,
    week_index: int,
    week_rows_by_team: dict[str, list[dict]],
    prior_season_rows_by_team: dict[str, dict],
    min_team_plays: int = MIN_TEAM_PLAYS,
) -> dict:
    """Adjusted-EPA lookup order for (team, season, week_index) -- design doc
    section 1c:

    1. As-of week fit: the analytics.adjusted_epa_week_build row for `team`
       with the greatest stored `week_index <= week_index`, provided that
       row's `plays >= min_team_plays` -- source 'week'.
    2. Else the prior-season (season - 1) analytics.adjusted_epa_build full
       fit -- source 'prior_season' (known before the season starts, so
       leak-free by construction).
    3. Else all-NULL, source None (the model layer imputes).

    `week_rows_by_team[team]` is that team's THIS-season rows (any order),
    each a dict with week_index/off_coef/def_coef/hfa_coef/plays.
    `prior_season_rows_by_team[team]` is that team's SEASON-1 full-season
    row, a dict with off_coef/def_coef/hfa_coef.

    Returns {"off", "def", "net", "hfa", "source"}; `net` = off - def
    (higher = better; subtracting a more-negative def coefficient adds).
    """
    best = None
    for row in week_rows_by_team.get(team, ()):
        if row["week_index"] > week_index:
            continue
        if row.get("plays") is None or row["plays"] < min_team_plays:
            continue
        if best is None or row["week_index"] > best["week_index"]:
            best = row

    if best is not None:
        return {
            "off": best["off_coef"],
            "def": best["def_coef"],
            "net": best["off_coef"] - best["def_coef"],
            "hfa": best["hfa_coef"],
            "source": "week",
        }

    prior = prior_season_rows_by_team.get(team)
    if prior is not None:
        return {
            "off": prior["off_coef"],
            "def": prior["def_coef"],
            "net": prior["off_coef"] - prior["def_coef"],
            "hfa": prior["hfa_coef"],
            "source": "prior_season",
        }

    return {"off": None, "def": None, "net": None, "hfa": None, "source": None}


def resolve_team_week_elo(
    elo_pregame_raw: float | None,
    team: str,
    season: int,
    elo_current: dict[str, tuple[float, int]],
) -> float:
    """House Elo fallback ladder (design doc section 1b): a completed game's
    stored analytics.house_elo_game pregame Elo (`elo_pregame_raw`, read by
    FEATURE_ROWS_QUERY) wins outright -- it is already walk-forward by
    construction. Otherwise (no house_elo_game row yet -- typically an
    upcoming/scheduled game), fall back to
    scripts.compute_predictions.resolve_elo against the live
    analytics.house_elo_current snapshot, which itself applies season
    carryover and finally defaults to EloEngine.SEED (1500) for a team with
    no snapshot at all. Never returns None.
    """
    if elo_pregame_raw is not None:
        return float(elo_pregame_raw)
    return resolve_elo(team, season, elo_current)


# =============================================================================
# --- I/O layer --- (SQL does the heavy set ops; Python resolves adj-EPA +
# upcoming-game Elo per row, then writes).
# =============================================================================


def get_db_url() -> str:
    """Get database URL from dlt secrets or environment.

    Copied from scripts/compute_house_elo.py's get_db_url pattern (each
    compute_*.py script keeps its own copy rather than importing across
    scripts for this one utility).
    """
    import os

    import dlt

    url = None
    try:
        creds = dlt.secrets.get("destination.postgres.credentials")
        if creds:
            url = str(creds)
    except Exception:
        pass

    if not url:
        url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")

    if not url:
        raise RuntimeError(
            "No database URL found. Set destination.postgres.credentials in "
            ".dlt/secrets.toml or SUPABASE_DB_URL environment variable."
        )

    return url


def _resolve_projection_seasons() -> list[int]:
    """Open a short-lived connection to resolve `--incremental`'s target
    seasons via src.pipelines.config.years.get_projection_seasons (which takes
    a connection so the config module stays DB-free)."""
    import psycopg2

    from src.pipelines.config.years import get_projection_seasons

    conn = psycopg2.connect(get_db_url())
    try:
        return get_projection_seasons(conn)
    finally:
        conn.close()


# One big per-season SELECT: spine (both sides of every core.games row for
# the season) plus every SQL-computable feature family (design doc sections
# 1a, 1b-completed-games-only, 1d, 1e, 1f). Adjusted-EPA (1c) and the
# upcoming-game Elo fallback (1b) are resolved in Python afterward -- see
# build_season_rows().
#
# Season-to-date EPA (1d) and havoc (1e) are each pre-aggregated to
# (team, week_index) grain first (off_week_agg/def_week_agg/havoc_week_agg),
# then summed for week_index < s.week_index via a LATERAL join -- far
# cheaper than a LATERAL scanning raw per-play rows, while computing exactly
# the same weighted average (sum/sum, not an average-of-averages).
FEATURE_ROWS_QUERY = f"""
WITH coach_counts AS (
    -- Coaches CFBD lists per school-year. >1 means a mid-season change; those
    -- school-years are excluded in coach_tenure below (leak guard).
    SELECT school, year, COUNT(*) AS n_coaches
    FROM ref.coaches__seasons
    GROUP BY school, year
),
coach_year AS (
    -- Deliberately NOT filtered to the target season: the gaps-and-islands
    -- grouping below needs a school's whole coaching history to tell a second
    -- stint from a continuation. ref.coaches__seasons is small. Filtering it
    -- would also left-censor tenure, making every long-tenured coach look like
    -- he started in the first year of the window.
    SELECT DISTINCT ON (c.school, c.year)
           c.school, c.year, c._dlt_parent_id AS coach_id
    FROM ref.coaches__seasons c
    ORDER BY c.school, c.year, c.games DESC NULLS LAST
),
coach_islands AS (
    SELECT school, year, coach_id,
           year - ROW_NUMBER() OVER (PARTITION BY school, coach_id ORDER BY year) AS grp
    FROM coach_year
),
coach_tenure AS (
    -- LEAK GUARD (PR #55 review, P1) -- MUST match the screen's
    -- _COACH_TENURE_CTE, which carries the full rationale.
    --
    -- ref.coaches__seasons cannot split a mid-season coaching change
    -- (src/schemas/api/038_coach_records.sql), so picking the most-games coach
    -- resolves an EARLY firing to the replacement and marks week 1 as
    -- "first-year coach" when it was not. A mid-season firing is caused by
    -- season-S performance, so that would carry the season's own outcome into a
    -- feature that claims to be preseason-known.
    SELECT i.school, i.year, i.coach_id,
           CASE WHEN cc.n_coaches > 1 THEN NULL
                ELSE MIN(i.year) OVER (PARTITION BY i.school, i.coach_id, i.grp)
           END AS tenure_start
    FROM coach_islands i
    JOIN coach_counts cc ON cc.school = i.school AND cc.year = i.year
),
coach_season AS (
    -- Raw material for an incoming coach's CAREER PRIOR. MUST match the
    -- screen's coach_season, which carries the full rationale.
    --
    -- Ambiguous school-years are excluded here for attribution rather than
    -- leak reasons: an interim promoted into a collapse would otherwise
    -- inherit the whole season's rating. The cost is that a coach whose entire
    -- prior record is ambiguous reads as having no prior seasons, which
    -- classifies him as a rookie -- the intended trade, because the
    -- alternative credits him with somebody else's season.
    --
    -- rating is LEFT JOINed, not required: a season with no SP+ row (an FCS
    -- stop, or older than ratings coverage) is still head-coaching EXPERIENCE
    -- and must not be confused with having none.
    SELECT cy.coach_id, cy.school, cy.year,
           sp.rating::double precision AS rating
    FROM coach_year cy
    JOIN coach_counts cc ON cc.school = cy.school AND cc.year = cy.year
    LEFT JOIN ratings.sp_ratings sp
           ON sp.team = cy.school AND sp.year = cy.year
    WHERE cc.n_coaches = 1
),
coach_prior AS (
    -- What the coach listed at (school, year) did in seasons STRICTLY BEFORE
    -- that year, at any school.
    --
    -- LEAK RULE. `prev.year < cy.year` is strict, so season S can never enter.
    -- S-1 IS included and that is correct: S-1 ended in January of year S and
    -- its final SP+ is published months before week 1, so the hiring school
    -- knew it.
    SELECT cy.school, cy.year,
           COUNT(prev.year) AS prior_seasons,
           AVG(prev.rating) AS prior_sp_mean
    FROM coach_year cy
    LEFT JOIN coach_season prev
           ON prev.coach_id = cy.coach_id
          AND prev.year < cy.year
    GROUP BY cy.school, cy.year
),
draft_out AS (
    -- Draft picks by (draft year, college team). MUST match the screen's
    -- draft_out: the partials that justified these columns were measured on
    -- exactly this aggregation.
    SELECT dp.year AS season, dp.college_team AS team,
           COUNT(*)::double precision AS picks
    FROM draft.draft_picks dp
    WHERE dp.college_team IS NOT NULL
    GROUP BY 1, 2
),
class_points AS (
    -- The four recruiting classes signed BEFORE season S (design doc 1f).
    SELECT tr.team, tr.year, tr.points::double precision AS points
    FROM recruiting.team_recruiting tr
    WHERE tr.points IS NOT NULL
      AND tr.year BETWEEN %(season)s - {CLASS_WINDOW} AND %(season)s - 1
),
blue_chip_classes AS (
    SELECT rc.committed_to AS team,
           COUNT(*) FILTER (WHERE rc.stars >= 4)::numeric AS blue_chips,
           COUNT(*)::numeric AS signees
    FROM recruiting.recruits rc
    WHERE rc.committed_to IS NOT NULL
      AND rc.year BETWEEN %(season)s - {CLASS_WINDOW} AND %(season)s - 1
    GROUP BY 1
),
plays_wi AS (
    SELECT
        pe.offense,
        pe.defense,
        pe.epa,
        pe.success,
        pe.explosive,
        CASE WHEN g.season_type = 'postseason' THEN 100 + g.week ELSE g.week END AS week_index
    FROM marts.play_epa pe
    JOIN core.games g ON g.id = pe.game_id
    WHERE pe.season = %(season)s
      AND NOT pe.is_garbage_time
),
off_week_agg AS (
    SELECT
        offense AS team,
        week_index,
        SUM(epa) AS sum_epa,
        SUM(success) AS sum_success,
        SUM(explosive) AS sum_explosive,
        COUNT(*) AS n_plays
    FROM plays_wi
    GROUP BY offense, week_index
),
def_week_agg AS (
    SELECT
        defense AS team,
        week_index,
        SUM(epa) AS sum_epa,
        SUM(success) AS sum_success,
        SUM(explosive) AS sum_explosive,
        COUNT(*) AS n_plays
    FROM plays_wi
    GROUP BY defense, week_index
),
-- stats.game_havoc, joined by game_id for the week_index window (design doc
-- section 1e). defense__* = havoc that team's DEFENSE generated;
-- offense__* = havoc generated AGAINST that team's OFFENSE (havoc allowed).
-- offense__* mirrors mart 005's defense__* naming exactly -- dlt flattens
-- the CFBD response's nested "offense" object the same way it flattens
-- "defense" -- but is UNVERIFIED against a live information_schema check as
-- of this writing (mart 005 only live-verified defense__*, noting
-- offense__* "is present but is unused here"). If this query fails with
-- "column offense__... does not exist", re-run mart 005's presence check
-- against stats.game_havoc and fix the two offense__* refs below.
havoc_wi AS (
    SELECT
        gh.team,
        CASE WHEN g.season_type = 'postseason' THEN 100 + g.week ELSE g.week END AS week_index,
        COALESCE(
            gh.defense__total_havoc_events::double precision,
            gh.defense__total_havoc_events__v_double
        ) AS def_havoc_events,
        gh.defense__total_plays AS def_plays,
        COALESCE(
            gh.offense__total_havoc_events::double precision,
            gh.offense__total_havoc_events__v_double
        ) AS off_havoc_events_allowed,
        gh.offense__total_plays AS off_plays_allowed
    FROM stats.game_havoc gh
    JOIN core.games g ON g.id = gh.game_id
    WHERE g.season = %(season)s
),
havoc_week_agg AS (
    SELECT
        team,
        week_index,
        SUM(def_havoc_events) AS def_havoc_events,
        SUM(def_plays) AS def_plays,
        SUM(off_havoc_events_allowed) AS off_havoc_events_allowed,
        SUM(off_plays_allowed) AS off_plays_allowed
    FROM havoc_wi
    GROUP BY team, week_index
),
-- Spine (design doc section 0): both team-sides of every core.games row for
-- the season, completed or scheduled.
spine AS (
    SELECT
        g.id AS game_id, g.season, g.season_type, g.week,
        CASE WHEN g.season_type = 'postseason' THEN 100 + g.week ELSE g.week END AS week_index,
        g.home_team AS team, g.home_conference AS conference, true AS is_home
    FROM core.games g
    WHERE g.season = %(season)s
    UNION ALL
    SELECT
        g.id, g.season, g.season_type, g.week,
        CASE WHEN g.season_type = 'postseason' THEN 100 + g.week ELSE g.week END,
        g.away_team, g.away_conference, false
    FROM core.games g
    WHERE g.season = %(season)s
)
SELECT
    s.season,
    s.season_type,
    s.week,
    s.week_index,
    s.team,
    s.conference,
    s.game_id,
    gpd.games_played_to_date,
    -- Completed-game pregame Elo only (analytics.house_elo_game has no row
    -- for a game compute_house_elo.py hasn't processed yet -- typically an
    -- upcoming/scheduled game); NULL here triggers the Python fallback in
    -- build_season_rows() (design doc section 1b).
    CASE WHEN s.is_home THEN he.home_pregame_elo ELSE he.away_pregame_elo END AS elo_pregame_raw,
    std.off_epa_per_play,
    std.off_success_rate,
    std.off_explosiveness_rate,
    ROUND(std.off_plays_count::numeric / NULLIF(gpd.games_played_to_date, 0), 3)
        AS off_plays_per_game,
    stdd.def_epa_per_play_allowed,
    stdd.def_success_rate_allowed,
    stdd.def_explosiveness_rate_allowed,
    ROUND((hv.def_havoc_events / NULLIF(hv.def_plays, 0))::numeric, 5) AS havoc_rate_defense,
    ROUND((hv.off_havoc_events_allowed / NULLIF(hv.off_plays_allowed, 0))::numeric, 5)
        AS havoc_rate_offense_allowed,
    rp.returning_ppa_pct,
    rp.returning_passing_ppa_pct,
    rp.returning_rushing_ppa_pct,
    rp."usage" AS returning_usage,
    sp.rating AS preseason_sp_rating,
    sp."offense__rating" AS preseason_sp_offense,
    sp."defense__rating" AS preseason_sp_defense,
    -- Migration 042 (screened, plan section 2.5). NULL and never 0 where a
    -- source row is absent -- see the design doc's section 1i decision: these
    -- are rates and decayed sums whose zero is a fabricated extreme, and the
    -- teams that are missing skew weak, so zero-filling would invent signal.
    -- train_model.py imputes with a frozen train-window mean instead.
    (
        SELECT SUM(cp.points * POWER({CLASS_DECAY}, %(season)s - cp.year - 1))
        FROM class_points cp
        WHERE cp.team = s.team
    ) AS recruiting_points_3yr,
    bc.blue_chips / NULLIF(bc.signees, 0) AS blue_chip_pipeline,
    -- 0.0 is a real value here ("established coach"); NULL means we have no
    -- coach record for this school-year at all.
    CASE WHEN ct.tenure_start IS NULL THEN NULL
         WHEN ct.tenure_start >= s.season THEN 1.0
         ELSE 0.0 END AS hc_first_year,
    -- The first-year penalty is not a penalty for changing coaches: it lives
    -- entirely on hires without a record. Screen runs 138/139/140, second-order
    -- partial vs prior SP+ and recruiting, over 2015-2025:
    --
    --     unproven hire  -0.1844   |  proven hire  +0.0096 (p=0.73)
    --
    -- and unproven beats the flat binary in BOTH half-windows. See
    -- scripts/screen_preseason_features.py finding 8.
    --
    -- NULL, never 0.0, wherever the classification is genuinely unknown: no
    -- coach record at all, or a hire whose head-coaching seasons the warehouse
    -- cannot rate. 0.0 IS a true value for a continuing staff -- there was no
    -- hire, so the hire was not unproven. Guarding prior_seasons separately
    -- from prior_sp_mean is what keeps a genuine first-time head coach (a
    -- rookie, and unproven) apart from one whose record cannot be scored
    -- (unknown, and NULL).
    CASE WHEN ct.tenure_start IS NULL OR cpr.prior_seasons IS NULL THEN NULL
         WHEN ct.tenure_start < s.season THEN 0.0
         WHEN cpr.prior_seasons = 0 THEN 1.0
         WHEN cpr.prior_sp_mean IS NULL THEN NULL
         WHEN cpr.prior_sp_mean < {HC_PROVEN_SP_PLUS} THEN 1.0
         ELSE 0.0 END AS hc_first_year_unproven,
    -- MIGRATION 047. Counts, so 0.0 is a REAL value here -- a program that
    -- sent nobody to the NFL produced zero picks, and NULLing that would throw
    -- away signal. This is the documented exception to section 1i's blanket
    -- never-zero rule, and it is safe ONLY because the zero is guarded.
    --
    -- The guard is the lesson of this column's own history. Before the
    -- 2000-2019 backfill, draft.draft_picks held 2020-2026 while years.py
    -- configured 2000-2026, and a bare COALESCE(...,0) turned "this draft was
    -- never ingested" into "this program produced zero picks" on 54.2%% of the
    -- screening frame. Nothing errored and the column screened as a null.
    --
    -- So the zero is only written where the SOURCE YEARS EXIST. The EXISTS
    -- deliberately has NO team predicate: a team absent from a draft that was
    -- loaded really did produce nothing, and that true zero has to stay
    -- distinguishable from a draft nobody loaded.
    CASE WHEN EXISTS (
             SELECT 1 FROM draft_out d
             WHERE d.season BETWEEN s.season - 3 AND s.season - 1
         )
         THEN COALESCE((
             SELECT SUM(d2.picks) FROM draft_out d2
             WHERE d2.team = s.team
               AND d2.season BETWEEN s.season - 3 AND s.season - 1
         ), 0)
    END AS draft_picks_3yr,
    CASE WHEN EXISTS (SELECT 1 FROM draft_out d WHERE d.season = s.season)
         THEN COALESCE((
             SELECT d3.picks FROM draft_out d3
             WHERE d3.team = s.team AND d3.season = s.season
         ), 0)
    END AS draft_departures,
    ats."defense__line_yards" AS prior_def_line_yards,
    ats."defense__stuff_rate" AS prior_def_stuff_rate
FROM spine s
LEFT JOIN analytics.house_elo_game he ON he.game_id = s.game_id
-- Always returns exactly one row (COUNT(*) is an aggregate with no
-- GROUP BY), so games_played_to_date is 0 -- never NULL -- for a team's
-- first game of the season (design doc section 1a).
LEFT JOIN LATERAL (
    SELECT COUNT(*) AS games_played_to_date
    FROM core.games gp
    WHERE COALESCE(gp.completed, false)
      AND gp.season = s.season
      AND (gp.home_team = s.team OR gp.away_team = s.team)
      AND (CASE WHEN gp.season_type = 'postseason' THEN 100 + gp.week ELSE gp.week END)
          < s.week_index
) gpd ON true
LEFT JOIN LATERAL (
    SELECT
        SUM(owa.sum_epa) / NULLIF(SUM(owa.n_plays), 0) AS off_epa_per_play,
        SUM(owa.sum_success)::numeric / NULLIF(SUM(owa.n_plays), 0) AS off_success_rate,
        SUM(owa.sum_explosive)::numeric / NULLIF(SUM(owa.n_plays), 0) AS off_explosiveness_rate,
        SUM(owa.n_plays) AS off_plays_count
    FROM off_week_agg owa
    WHERE owa.team = s.team AND owa.week_index < s.week_index
) std ON true
LEFT JOIN LATERAL (
    SELECT
        SUM(dwa.sum_epa) / NULLIF(SUM(dwa.n_plays), 0) AS def_epa_per_play_allowed,
        SUM(dwa.sum_success)::numeric / NULLIF(SUM(dwa.n_plays), 0) AS def_success_rate_allowed,
        SUM(dwa.sum_explosive)::numeric / NULLIF(SUM(dwa.n_plays), 0)
            AS def_explosiveness_rate_allowed
    FROM def_week_agg dwa
    WHERE dwa.team = s.team AND dwa.week_index < s.week_index
) stdd ON true
LEFT JOIN LATERAL (
    SELECT
        SUM(hwa.def_havoc_events) AS def_havoc_events,
        SUM(hwa.def_plays) AS def_plays,
        SUM(hwa.off_havoc_events_allowed) AS off_havoc_events_allowed,
        SUM(hwa.off_plays_allowed) AS off_plays_allowed
    FROM havoc_week_agg hwa
    WHERE hwa.team = s.team AND hwa.week_index < s.week_index
) hv ON true
-- Preseason-known constants (design doc section 1f): same for every week of
-- a season, so a plain equality join (no as-of window) is correct.
LEFT JOIN marts.returning_production rp ON rp.season = s.season AND rp.team = s.team
LEFT JOIN ratings.sp_ratings sp ON sp.year = s.season - 1 AND sp.team = s.team
LEFT JOIN blue_chip_classes bc ON bc.team = s.team
LEFT JOIN coach_tenure ct ON ct.school = s.team AND ct.year = s.season
LEFT JOIN coach_prior cpr ON cpr.school = s.team AND cpr.year = s.season
-- Season S-1 trench performance: complete before S starts, so leak-free.
LEFT JOIN stats.advanced_team_stats ats
       ON ats.team = s.team AND ats.season = s.season - 1
ORDER BY s.team, s.week_index
"""

_INSERT_COLUMNS = [
    "season",
    "season_type",
    "week",
    "week_index",
    "team",
    "conference",
    "game_id",
    "games_played_to_date",
    "elo_pregame",
    "adj_epa_off",
    "adj_epa_def",
    "adj_epa_net",
    "adj_epa_hfa",
    "adj_epa_source",
    "off_epa_per_play",
    "off_success_rate",
    "off_explosiveness_rate",
    "off_plays_per_game",
    "def_epa_per_play_allowed",
    "def_success_rate_allowed",
    "def_explosiveness_rate_allowed",
    "havoc_rate_defense",
    "havoc_rate_offense_allowed",
    "returning_ppa_pct",
    "returning_passing_ppa_pct",
    "returning_rushing_ppa_pct",
    "returning_usage",
    "preseason_sp_rating",
    "preseason_sp_offense",
    "preseason_sp_defense",
    # Migration 042 -- screened preseason columns (plan section 2.5).
    "recruiting_points_3yr",
    "blue_chip_pipeline",
    "hc_first_year",
    # Migration 046 -- the screened replacement for hc_first_year in the
    # fitted vector; hc_first_year stays populated for continuity.
    "hc_first_year_unproven",
    # Migration 047 -- adjudicated only after the draft backfill.
    "draft_picks_3yr",
    "draft_departures",
    "prior_def_line_yards",
    "prior_def_stuff_rate",
    "feature_build_version",
]

# computed_at is deliberately excluded -- features.team_week.computed_at
# DEFAULT now() (migration 028) fills it in, one value per row at insert time.
_INSERT_SQL = f"INSERT INTO features.team_week ({', '.join(_INSERT_COLUMNS)}) VALUES %s"


def fetch_feature_rows(conn, season: int) -> list[dict]:
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(FEATURE_ROWS_QUERY, {"season": season})
        return [dict(row) for row in cur.fetchall()]


def fetch_adj_epa_week_rows(conn, season: int) -> dict[str, list[dict]]:
    """analytics.adjusted_epa_week_build rows for `season`, grouped by team,
    for resolve_adj_epa's as-of-week lookup."""
    result: dict[str, list[dict]] = defaultdict(list)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT team, week_index, off_coef, def_coef, hfa_coef, plays
            FROM analytics.adjusted_epa_week_build
            WHERE season = %s
            """,
            (season,),
        )
        for team, week_index, off_coef, def_coef, hfa_coef, plays in cur.fetchall():
            result[team].append(
                {
                    "week_index": int(week_index),
                    "off_coef": float(off_coef) if off_coef is not None else None,
                    "def_coef": float(def_coef) if def_coef is not None else None,
                    "hfa_coef": float(hfa_coef) if hfa_coef is not None else None,
                    "plays": int(plays) if plays is not None else None,
                }
            )
    return dict(result)


def fetch_adj_epa_full_rows(conn, season: int) -> dict[str, dict]:
    """analytics.adjusted_epa_build rows for `season` (the prior-season
    fallback, so callers pass season - 1), one row per team."""
    result: dict[str, dict] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT team, off_coef, def_coef, hfa_coef
            FROM analytics.adjusted_epa_build
            WHERE season = %s
            """,
            (season,),
        )
        for team, off_coef, def_coef, hfa_coef in cur.fetchall():
            if off_coef is None or def_coef is None:
                continue
            result[team] = {
                "off_coef": float(off_coef),
                "def_coef": float(def_coef),
                "hfa_coef": float(hfa_coef) if hfa_coef is not None else None,
            }
    return result


def build_season_rows(conn, season: int, elo_current: dict[str, tuple[float, int]]) -> list[dict]:
    """Fetch one season's SQL-computed rows and resolve adj-EPA + Elo
    fallback in Python (see module docstring). Empty list = clean no-op
    (no core.games rows for this season)."""
    raw_rows = fetch_feature_rows(conn, season)
    if not raw_rows:
        return []

    week_rows_by_team = fetch_adj_epa_week_rows(conn, season)
    prior_rows_by_team = fetch_adj_epa_full_rows(conn, season - 1)

    built: list[dict] = []
    for row in raw_rows:
        team = row["team"]
        week_index = row["week_index"]

        elo_pregame = resolve_team_week_elo(row["elo_pregame_raw"], team, season, elo_current)
        adj = resolve_adj_epa(team, season, week_index, week_rows_by_team, prior_rows_by_team)

        built.append(
            {
                "season": row["season"],
                "season_type": row["season_type"],
                "week": row["week"],
                "week_index": week_index,
                "team": team,
                "conference": row["conference"],
                "game_id": row["game_id"],
                "games_played_to_date": row["games_played_to_date"],
                "elo_pregame": elo_pregame,
                "adj_epa_off": adj["off"],
                "adj_epa_def": adj["def"],
                "adj_epa_net": adj["net"],
                "adj_epa_hfa": adj["hfa"],
                "adj_epa_source": adj["source"],
                "off_epa_per_play": row["off_epa_per_play"],
                "off_success_rate": row["off_success_rate"],
                "off_explosiveness_rate": row["off_explosiveness_rate"],
                "off_plays_per_game": row["off_plays_per_game"],
                "def_epa_per_play_allowed": row["def_epa_per_play_allowed"],
                "def_success_rate_allowed": row["def_success_rate_allowed"],
                "def_explosiveness_rate_allowed": row["def_explosiveness_rate_allowed"],
                "havoc_rate_defense": row["havoc_rate_defense"],
                "havoc_rate_offense_allowed": row["havoc_rate_offense_allowed"],
                "returning_ppa_pct": row["returning_ppa_pct"],
                "returning_passing_ppa_pct": row["returning_passing_ppa_pct"],
                "returning_rushing_ppa_pct": row["returning_rushing_ppa_pct"],
                "returning_usage": row["returning_usage"],
                "preseason_sp_rating": row["preseason_sp_rating"],
                "preseason_sp_offense": row["preseason_sp_offense"],
                "preseason_sp_defense": row["preseason_sp_defense"],
                "recruiting_points_3yr": row["recruiting_points_3yr"],
                "blue_chip_pipeline": row["blue_chip_pipeline"],
                "hc_first_year": row["hc_first_year"],
                "hc_first_year_unproven": row["hc_first_year_unproven"],
                "draft_picks_3yr": row["draft_picks_3yr"],
                "draft_departures": row["draft_departures"],
                "prior_def_line_yards": row["prior_def_line_yards"],
                "prior_def_stuff_rate": row["prior_def_stuff_rate"],
                "feature_build_version": FEATURE_BUILD_VERSION,
            }
        )

    return built


def write_season(conn, season: int, rows: list[dict]) -> None:
    """Idempotent per-season write: DELETE then bulk INSERT, one commit."""
    from psycopg2.extras import execute_values

    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM features.team_week WHERE season = %s", (season,))
        if rows:
            values = [tuple(r[c] for c in _INSERT_COLUMNS) for r in rows]
            execute_values(cur, _INSERT_SQL, values)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def summarize(rows: list[dict]) -> dict:
    """Counts backing the FEATURES_GATE log line -- a leak-audit spot check,
    not a hard gate (design doc section 1i's NULL-semantics table)."""
    return {
        "rows": len(rows),
        "null_elo": sum(1 for r in rows if r["elo_pregame"] is None),
        "null_adj_epa": sum(1 for r in rows if r["adj_epa_off"] is None),
        "adj_src_week": sum(1 for r in rows if r["adj_epa_source"] == "week"),
        "adj_src_prior": sum(1 for r in rows if r["adj_epa_source"] == "prior_season"),
        "null_std": sum(1 for r in rows if r["off_epa_per_play"] is None),
        "week1_rows": sum(1 for r in rows if r["games_played_to_date"] == 0),
        # Migration 042. Reported per column because these are the ONLY
        # features carrying information in week 1 -- if one silently arrives
        # all-NULL it imputes to the train-window mean, its diff is exactly
        # zero, and the preseason prediction is unchanged while every other
        # number on this line looks healthy. A broken join and genuinely sparse
        # data are indistinguishable without the count.
        **{f"null_{col}": sum(1 for r in rows if r[col] is None) for col in PRESEASON_042_COLUMNS},
    }


def print_gate(season: int, rows: list[dict]) -> None:
    s = summarize(rows)
    extra = " ".join(f"null_{col}={s[f'null_{col}']}" for col in PRESEASON_042_COLUMNS)
    print(
        f"FEATURES_GATE season={season} rows={s['rows']} null_elo={s['null_elo']} "
        f"null_adj_epa={s['null_adj_epa']} adj_src_week={s['adj_src_week']} "
        f"adj_src_prior={s['adj_src_prior']} null_std={s['null_std']} "
        f"week1_rows={s['week1_rows']} {extra}"
    )


def compute_seasons(seasons: list[int]) -> int:
    """Build and write each season. Returns count of failed seasons (an
    empty/no-op season is NOT a failure)."""
    import psycopg2

    conn = psycopg2.connect(get_db_url())
    # The Supabase session pooler (PgBouncer) drops the `options` startup
    # parameter, so a URL-level statement_timeout never reaches the backend;
    # set it explicitly on the session. The per-season INSERT..SELECT can
    # legitimately exceed the 2-minute default on 2021+ play volumes.
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '900s'")
    conn.commit()
    failures = 0
    try:
        elo_current = fetch_elo_current(conn)
        for season in seasons:
            try:
                rows = build_season_rows(conn, season, elo_current)
                if not rows:
                    logger.info(f"season={season}: no core.games rows found, clean no-op")
                    print(
                        f"FEATURES_GATE season={season} rows=0 null_elo=0 null_adj_epa=0 "
                        "adj_src_week=0 adj_src_prior=0 null_std=0 week1_rows=0"
                    )
                    continue
                write_season(conn, season, rows)
                print_gate(season, rows)
                logger.info(f"season={season}: wrote {len(rows)} features.team_week row(s)")
            except Exception as e:
                conn.rollback()
                logger.error(f"season={season}: FAILED: {e}")
                failures += 1
    finally:
        conn.close()

    return failures


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build features.team_week per "
        "docs/brainstorms/2026-07-21-team-week-feature-design.md"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--from",
        dest="from_season",
        type=int,
        help="Backfill every season from YYYY through the current season "
        "(src.pipelines.config.years.get_current_season())",
    )
    group.add_argument(
        "--season",
        dest="season",
        type=int,
        help="Build a single season",
    )
    group.add_argument(
        "--incremental",
        action="store_true",
        help="Build every projection season -- the most recent season with completed "
        "games plus every later season with a published schedule "
        "(src.pipelines.config.years.get_projection_seasons()). What the daily "
        "workflow runs.",
    )
    args = parser.parse_args()

    from src.pipelines.config.years import get_current_season

    if args.season is not None:
        seasons = [args.season]
    elif args.incremental:
        # Resolved from core.games, not the calendar: get_current_season()
        # returns year-1 until August, which left features.team_week with zero
        # rows for the upcoming season all offseason (see
        # get_projection_seasons' docstring).
        seasons = _resolve_projection_seasons()
    else:
        # Upper bound from core.games, NOT get_current_season(). The calendar
        # rule returns year-1 until August, so a July `--from 2015` silently
        # stopped at 2025 and left the upcoming season untouched -- exactly the
        # defect Phase 1 fixed for --incremental and missed here.
        #
        # It is the worse half of that bug, because a backfill that skips the
        # only season anyone is asking about still exits 0 and still prints a
        # gate line per season it DID build. The 2026-07-26 migration-042
        # rebuild hit this: eleven seasons got the new preseason columns, 2026
        # kept NULLs, and 2026 predictions would have been unchanged while
        # every surface reported success.
        projection_seasons = _resolve_projection_seasons()
        last_season = max([*projection_seasons, get_current_season()])
        if args.from_season > last_season:
            logger.error(f"--from {args.from_season} is after the last season ({last_season})")
            sys.exit(1)
        seasons = list(range(args.from_season, last_season + 1))

    logger.info(f"Building features.team_week for {len(seasons)} season(s): {seasons}")
    failures = compute_seasons(seasons)

    if failures:
        logger.warning(f"{failures} season(s) failed")
    else:
        logger.info("All seasons built successfully")

    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
