-- Conference comparison: per-conference per-season aggregate metrics
-- Grain: Conference × Season
-- Enables "SEC vs Big Ten" style comparisons

DROP MATERIALIZED VIEW IF EXISTS marts.conference_comparison CASCADE;

CREATE MATERIALIZED VIEW marts.conference_comparison AS
WITH team_conf AS (
    -- Get conference for each team, avoiding ref.teams fanout (35 duplicate school names)
    SELECT DISTINCT ON (school)
        school, conference
    FROM ref.teams
    WHERE conference IS NOT NULL
    ORDER BY school, classification NULLS LAST
),
non_conf_games AS (
    -- Games between teams in different conferences
    SELECT
        g.season,
        CASE
            WHEN g.home_conference != g.away_conference AND g.home_points > g.away_points
                THEN g.home_conference
            WHEN g.home_conference != g.away_conference AND g.away_points > g.home_points
                THEN g.away_conference
        END AS winning_conference,
        CASE
            WHEN g.home_conference != g.away_conference THEN g.home_conference
        END AS home_conf,
        CASE
            WHEN g.home_conference != g.away_conference THEN g.away_conference
        END AS away_conf
    FROM core.games g
    WHERE g.completed = true
      AND g.home_conference IS NOT NULL
      AND g.away_conference IS NOT NULL
      AND g.home_conference != g.away_conference
),
non_conf_record AS (
    SELECT
        conf,
        season,
        COUNT(*) AS non_conf_games,
        SUM(wins)::int AS non_conf_wins
    FROM (
        SELECT home_conf AS conf, season,
            CASE WHEN winning_conference = home_conf THEN 1 ELSE 0 END AS wins
        FROM non_conf_games
        WHERE home_conf IS NOT NULL
        UNION ALL
        SELECT away_conf AS conf, season,
            CASE WHEN winning_conference = away_conf THEN 1 ELSE 0 END AS wins
        FROM non_conf_games
        WHERE away_conf IS NOT NULL
    ) x
    GROUP BY conf, season
),
conf_agg AS (
    SELECT
        tc.conference,
        tss.season,
        COUNT(DISTINCT tss.team) AS member_count,

        -- Performance
        ROUND(AVG(tss.wins)::numeric, 2) AS avg_wins,
        ROUND(AVG(tss.sp_rating)::numeric, 2) AS avg_sp_rating,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tss.sp_rating)::numeric, 2) AS median_sp_rating,

        -- Best/worst team
        (ARRAY_AGG(tss.team ORDER BY tss.sp_rating DESC NULLS LAST))[1] AS best_team,
        MAX(tss.sp_rating) AS best_team_sp,
        (ARRAY_AGG(tss.team ORDER BY tss.sp_rating ASC NULLS LAST))[1] AS worst_team,
        MIN(tss.sp_rating) AS worst_team_sp,

        -- Parity
        ROUND(STDDEV(tss.sp_rating)::numeric, 2) AS std_dev_sp,

        -- EPA
        ROUND(AVG(epa.epa_per_play)::numeric, 4) AS avg_epa_per_play,
        ROUND(AVG(epa.success_rate)::numeric, 4) AS avg_success_rate,

        -- Recruiting
        ROUND(AVG(rc.national_rank)::numeric, 1) AS avg_recruiting_rank,
        SUM(rc.five_stars + rc.four_stars)::int AS total_blue_chips,
        ROUND(AVG(rc.blue_chip_ratio)::numeric, 4) AS avg_blue_chip_ratio,

        -- Ranked teams (SP+ rank <= 25)
        COUNT(*) FILTER (WHERE tss.sp_rank IS NOT NULL AND tss.sp_rank <= 25) AS ranked_team_count

    FROM marts.team_season_summary tss
    JOIN team_conf tc ON tc.school = tss.team
    LEFT JOIN marts.team_epa_season epa ON epa.team = tss.team AND epa.season = tss.season
    LEFT JOIN marts.recruiting_class rc ON rc.team = tss.team AND rc.year = tss.season
    WHERE tc.conference IS NOT NULL
    GROUP BY tc.conference, tss.season
    HAVING COUNT(DISTINCT tss.team) >= 4  -- meaningful conference size
),
combined AS (
    SELECT
        ca.*,

        -- Non-conference record
        ROUND(
            ncr.non_conf_wins::numeric / NULLIF(ncr.non_conf_games, 0),
            4
        ) AS non_conf_win_pct

    FROM conf_agg ca
    LEFT JOIN non_conf_record ncr
        ON ncr.conf = ca.conference AND ncr.season = ca.season
)
SELECT
    c.*,

    -- Percentiles across conferences (per season)
    CASE WHEN c.avg_sp_rating IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.avg_sp_rating)
    END AS avg_sp_pctl,
    CASE WHEN c.avg_epa_per_play IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.avg_epa_per_play)
    END AS avg_epa_pctl,
    CASE WHEN c.avg_recruiting_rank IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.avg_recruiting_rank DESC)
    END AS avg_recruiting_pctl,
    CASE WHEN c.non_conf_win_pct IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.non_conf_win_pct)
    END AS non_conf_win_pct_pctl

FROM combined c;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.conference_comparison (conference, season);

-- Query indexes
CREATE INDEX ON marts.conference_comparison (season);
CREATE INDEX ON marts.conference_comparison (season, avg_sp_rating DESC);
