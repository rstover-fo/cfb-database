-- Sprint 5: Analytics materialized views
--
-- Pre-computed denormalized views for common analytical queries.
-- Each view supports REFRESH MATERIALIZED VIEW CONCURRENTLY via a UNIQUE INDEX.
-- Additional indexes cover common query patterns.
--
-- Refresh strategy: run the refresh function after each pipeline load.

-- =============================================================================
-- Schema setup
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- =============================================================================
-- 1. team_season_summary
--    One row per team per season — win/loss splits, point margins
-- =============================================================================

DROP MATERIALIZED VIEW IF EXISTS analytics.team_season_summary;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.team_season_summary AS
WITH home_games AS (
    SELECT
        season,
        home_team AS team,
        home_conference AS conference,
        home_points AS points_for,
        away_points AS points_against,
        CASE WHEN home_points > away_points THEN 1 ELSE 0 END AS win,
        CASE WHEN home_points < away_points THEN 1 ELSE 0 END AS loss,
        1 AS home_game,
        0 AS away_game,
        CASE
            WHEN home_conference IS NOT NULL
                AND home_conference = away_conference THEN 1
            ELSE 0
        END AS is_conference_game,
        CASE
            WHEN home_conference IS NOT NULL
                AND home_conference = away_conference
                AND home_points > away_points THEN 1
            ELSE 0
        END AS conf_win,
        CASE
            WHEN home_conference IS NOT NULL
                AND home_conference = away_conference
                AND home_points < away_points THEN 1
            ELSE 0
        END AS conf_loss,
        excitement_index
    FROM core.games
    WHERE completed = true
),
away_games AS (
    SELECT
        season,
        away_team AS team,
        away_conference AS conference,
        away_points AS points_for,
        home_points AS points_against,
        CASE WHEN away_points > home_points THEN 1 ELSE 0 END AS win,
        CASE WHEN away_points < home_points THEN 1 ELSE 0 END AS loss,
        0 AS home_game,
        1 AS away_game,
        CASE
            WHEN away_conference IS NOT NULL
                AND home_conference = away_conference THEN 1
            ELSE 0
        END AS is_conference_game,
        CASE
            WHEN away_conference IS NOT NULL
                AND home_conference = away_conference
                AND away_points > home_points THEN 1
            ELSE 0
        END AS conf_win,
        CASE
            WHEN away_conference IS NOT NULL
                AND home_conference = away_conference
                AND away_points < home_points THEN 1
            ELSE 0
        END AS conf_loss,
        excitement_index
    FROM core.games
    WHERE completed = true
),
all_games AS (
    SELECT * FROM home_games
    UNION ALL
    SELECT * FROM away_games
)
SELECT
    season,
    team,
    MAX(conference) AS conference,
    SUM(win)::int AS wins,
    SUM(loss)::int AS losses,
    COUNT(*)::int AS total_games,
    SUM(points_for)::int AS points_for,
    SUM(points_against)::int AS points_against,
    (SUM(points_for) - SUM(points_against))::int AS point_margin,
    SUM(CASE WHEN home_game = 1 THEN win ELSE 0 END)::int AS home_wins,
    SUM(CASE WHEN home_game = 1 THEN loss ELSE 0 END)::int AS home_losses,
    SUM(CASE WHEN away_game = 1 THEN win ELSE 0 END)::int AS away_wins,
    SUM(CASE WHEN away_game = 1 THEN loss ELSE 0 END)::int AS away_losses,
    SUM(conf_win)::int AS conference_wins,
    SUM(conf_loss)::int AS conference_losses,
    ROUND(AVG(excitement_index)::numeric, 4) AS avg_excitement_index
FROM all_games
GROUP BY season, team;

-- Unique index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX IF NOT EXISTS ux_team_season_summary
    ON analytics.team_season_summary(season, team);

-- Query indexes
CREATE INDEX IF NOT EXISTS ix_team_season_summary_team
    ON analytics.team_season_summary(team);
CREATE INDEX IF NOT EXISTS ix_team_season_summary_conference
    ON analytics.team_season_summary(conference);
CREATE INDEX IF NOT EXISTS ix_team_season_summary_wins
    ON analytics.team_season_summary(season, wins DESC);

-- =============================================================================
-- 2. player_career_stats
--    One row per player per category/stat_type — aggregated across seasons
-- =============================================================================

DROP MATERIALIZED VIEW IF EXISTS analytics.player_career_stats;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.player_career_stats AS
WITH ranked AS (
    SELECT
        player_id,
        player,
        position,
        team,
        conference,
        season,
        category,
        stat_type,
        stat,
        ROW_NUMBER() OVER (
            PARTITION BY player_id, category, stat_type
            ORDER BY season DESC
        ) AS rn
    FROM stats.player_season_stats
)
SELECT
    r.player_id,
    r.player,
    r.position,
    -- last team / conference (most recent season)
    MAX(CASE WHEN r.rn = 1 THEN r.team END) AS team,
    MAX(CASE WHEN r.rn = 1 THEN r.conference END) AS conference,
    COUNT(DISTINCT r.season)::int AS seasons_played,
    MIN(r.season)::int AS first_season,
    MAX(r.season)::int AS last_season,
    r.category,
    r.stat_type,
    SUM(r.stat::numeric) AS total_stat,
    ROUND(SUM(r.stat::numeric) / NULLIF(COUNT(DISTINCT r.season), 0), 2) AS avg_stat_per_season
FROM ranked r
GROUP BY r.player_id, r.player, r.position, r.category, r.stat_type;

-- Unique index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX IF NOT EXISTS ux_player_career_stats
    ON analytics.player_career_stats(player_id, category, stat_type);

-- Query indexes
CREATE INDEX IF NOT EXISTS ix_player_career_stats_player
    ON analytics.player_career_stats(player);
CREATE INDEX IF NOT EXISTS ix_player_career_stats_team
    ON analytics.player_career_stats(team);
CREATE INDEX IF NOT EXISTS ix_player_career_stats_position
    ON analytics.player_career_stats(position);
CREATE INDEX IF NOT EXISTS ix_player_career_stats_category
    ON analytics.player_career_stats(category, stat_type);

-- =============================================================================
-- 3. conference_standings
--    One row per team per season within their conference
-- =============================================================================

DROP MATERIALIZED VIEW IF EXISTS analytics.conference_standings;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.conference_standings AS
SELECT
    tss.season,
    tss.conference,
    tss.team,
    tss.conference_wins AS conf_wins,
    tss.conference_losses AS conf_losses,
    tss.wins AS overall_wins,
    tss.losses AS overall_losses,
    ROUND(
        tss.conference_wins::numeric
        / NULLIF(tss.conference_wins + tss.conference_losses, 0),
        4
    ) AS conf_win_pct,
    sp.rating AS sp_rating,
    elo.elo AS elo_rating,
    tr.rank AS recruiting_rank
FROM analytics.team_season_summary tss
LEFT JOIN ratings.sp_ratings sp
    ON sp.year = tss.season AND sp.team = tss.team
LEFT JOIN ratings.elo_ratings elo
    ON elo.year = tss.season AND elo.team = tss.team
LEFT JOIN recruiting.team_recruiting tr
    ON tr.year = tss.season AND tr.team = tss.team
WHERE tss.conference IS NOT NULL;

-- Unique index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX IF NOT EXISTS ux_conference_standings
    ON analytics.conference_standings(season, conference, team);

-- Query indexes
CREATE INDEX IF NOT EXISTS ix_conference_standings_conf_season
    ON analytics.conference_standings(conference, season);
CREATE INDEX IF NOT EXISTS ix_conference_standings_win_pct
    ON analytics.conference_standings(season, conference, conf_win_pct DESC);
CREATE INDEX IF NOT EXISTS ix_conference_standings_team
    ON analytics.conference_standings(team);

-- =============================================================================
-- 4. team_recruiting_trend
--    One row per team per year — commit breakdown, rolling averages
-- =============================================================================

DROP MATERIALIZED VIEW IF EXISTS analytics.team_recruiting_trend;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.team_recruiting_trend AS
WITH commit_stats AS (
    SELECT
        year,
        committed_to AS team,
        COUNT(*)::int AS num_commits,
        ROUND(AVG(rating)::numeric, 4) AS avg_rating,
        COUNT(*) FILTER (WHERE stars = 5)::int AS five_stars,
        COUNT(*) FILTER (WHERE stars = 4)::int AS four_stars,
        COUNT(*) FILTER (WHERE stars = 3)::int AS three_stars
    FROM recruiting.recruits
    WHERE committed_to IS NOT NULL
    GROUP BY year, committed_to
),
top_positions AS (
    SELECT DISTINCT ON (year, committed_to)
        year,
        committed_to AS team,
        position AS top_position
    FROM (
        SELECT
            year,
            committed_to,
            position,
            COUNT(*) AS pos_count
        FROM recruiting.recruits
        WHERE committed_to IS NOT NULL AND position IS NOT NULL
        GROUP BY year, committed_to, position
    ) pos_counts
    ORDER BY year, committed_to, pos_count DESC
)
SELECT
    tr.year,
    tr.team,
    tr.rank AS recruiting_rank,
    tr.points AS recruiting_points,
    COALESCE(cs.num_commits, 0) AS num_commits,
    cs.avg_rating,
    COALESCE(cs.five_stars, 0) AS five_stars,
    COALESCE(cs.four_stars, 0) AS four_stars,
    COALESCE(cs.three_stars, 0) AS three_stars,
    tp.top_position,
    ROUND(
        AVG(tr.rank::numeric) OVER (
            PARTITION BY tr.team
            ORDER BY tr.year
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS recruiting_rank_3yr_avg
FROM recruiting.team_recruiting tr
LEFT JOIN commit_stats cs
    ON cs.year = tr.year AND cs.team = tr.team
LEFT JOIN top_positions tp
    ON tp.year = tr.year AND tp.team = tr.team;

-- Unique index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX IF NOT EXISTS ux_team_recruiting_trend
    ON analytics.team_recruiting_trend(year, team);

-- Query indexes
CREATE INDEX IF NOT EXISTS ix_team_recruiting_trend_team
    ON analytics.team_recruiting_trend(team);
CREATE INDEX IF NOT EXISTS ix_team_recruiting_trend_rank
    ON analytics.team_recruiting_trend(year, recruiting_rank);

-- =============================================================================
-- 5. game_results
--    Denormalized game results — everything analysts need in one row
-- =============================================================================

DROP MATERIALIZED VIEW IF EXISTS analytics.game_results;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.game_results AS
WITH consensus_lines AS (
    -- Pick 'consensus' provider first; fall back to the first available provider
    SELECT DISTINCT ON (game_id)
        game_id,
        spread,
        over_under
    FROM betting.lines
    ORDER BY game_id, CASE WHEN provider = 'consensus' THEN 0 ELSE 1 END, provider
)
SELECT
    g.id AS game_id,
    g.season,
    g.week,
    g.season_type,
    g.start_date AS game_date,
    g.home_team,
    g.home_conference,
    g.away_team,
    g.away_conference,
    g.home_points,
    g.away_points,
    CASE
        WHEN g.home_points > g.away_points THEN g.home_team
        WHEN g.away_points > g.home_points THEN g.away_team
        ELSE NULL
    END AS winner,
    CASE
        WHEN g.home_points > g.away_points THEN g.away_team
        WHEN g.away_points > g.home_points THEN g.home_team
        ELSE NULL
    END AS loser,
    ABS(g.home_points - g.away_points)::int AS point_diff,
    cl.spread AS home_spread,
    cl.over_under,
    wp.home_win_probability AS home_win_prob,
    g.home_pregame_elo AS home_elo,
    g.away_pregame_elo AS away_elo,
    g.excitement_index,
    g.venue,
    g.attendance
FROM core.games g
LEFT JOIN consensus_lines cl ON cl.game_id = g.id
LEFT JOIN metrics.pregame_win_probability wp ON wp.game_id = g.id
WHERE g.completed = true;

-- Unique index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX IF NOT EXISTS ux_game_results
    ON analytics.game_results(game_id);

-- Query indexes
CREATE INDEX IF NOT EXISTS ix_game_results_season_week
    ON analytics.game_results(season, week);
CREATE INDEX IF NOT EXISTS ix_game_results_home_team
    ON analytics.game_results(home_team);
CREATE INDEX IF NOT EXISTS ix_game_results_away_team
    ON analytics.game_results(away_team);
CREATE INDEX IF NOT EXISTS ix_game_results_winner
    ON analytics.game_results(winner);
CREATE INDEX IF NOT EXISTS ix_game_results_season_type
    ON analytics.game_results(season, season_type);

-- =============================================================================
-- Refresh function
--
-- Call analytics.refresh_all_views() after pipeline loads to update all views.
-- Uses CONCURRENTLY so reads are not blocked during refresh.
-- Order matters: team_season_summary must refresh before conference_standings.
-- =============================================================================

CREATE OR REPLACE FUNCTION analytics.refresh_all_views()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Refreshing analytics.team_season_summary...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.team_season_summary;

    RAISE NOTICE 'Refreshing analytics.player_career_stats...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.player_career_stats;

    RAISE NOTICE 'Refreshing analytics.conference_standings...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.conference_standings;

    RAISE NOTICE 'Refreshing analytics.team_recruiting_trend...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.team_recruiting_trend;

    RAISE NOTICE 'Refreshing analytics.game_results...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.game_results;

    RAISE NOTICE 'All analytics views refreshed.';
END;
$$;
