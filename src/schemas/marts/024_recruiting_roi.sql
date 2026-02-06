-- Recruiting ROI: connects 4-year rolling recruiting investment to on-field outcomes
-- Grain: Team × Season
-- Measures: blue chip ratio, wins over expected, recruiting efficiency, draft capital

DROP MATERIALIZED VIEW IF EXISTS marts.recruiting_roi CASCADE;

CREATE MATERIALIZED VIEW marts.recruiting_roi AS
WITH rolling_recruiting AS (
    -- 4-year rolling average for recruiting (matches scholarship cycle)
    SELECT
        tss.team,
        tss.season,
        AVG(rc.national_rank) AS avg_class_rank_4yr,
        AVG(rc.total_points) AS avg_class_points_4yr,
        SUM(rc.five_stars + rc.four_stars) AS total_blue_chips_4yr,
        SUM(rc.total_commits) AS total_recruits_4yr,
        ROUND(
            SUM(rc.five_stars + rc.four_stars)::numeric
            / NULLIF(SUM(rc.total_commits), 0),
            4
        ) AS blue_chip_ratio
    FROM marts.team_season_summary tss
    LEFT JOIN marts.recruiting_class rc
        ON rc.team = tss.team
        AND rc.year BETWEEN tss.season - 4 AND tss.season - 1
    GROUP BY tss.team, tss.season
    HAVING COUNT(rc.year) >= 2  -- need at least 2 years of recruiting data
),
draft_output AS (
    -- Draft picks from players at this school in this season
    SELECT
        dp.college_team AS team,
        dp.year AS season,
        COUNT(*) AS players_drafted,
        SUM(GREATEST(260 - COALESCE(dp.overall, 260), 0))::int AS draft_picks_value
    FROM draft.draft_picks dp
    WHERE dp.college_team IS NOT NULL
    GROUP BY dp.college_team, dp.year
),
-- Compute expected wins/EPA based on recruiting rank bucket
-- Using median performance for teams with similar recruiting
expected_perf AS (
    SELECT
        tss.team,
        tss.season,
        -- Bucket teams by recruiting rank: top 10, 11-25, 26-50, 51+
        CASE
            WHEN rr.avg_class_rank_4yr <= 10 THEN 'elite'
            WHEN rr.avg_class_rank_4yr <= 25 THEN 'high'
            WHEN rr.avg_class_rank_4yr <= 50 THEN 'mid'
            ELSE 'low'
        END AS talent_bucket,
        tss.wins,
        tss.sp_rating,
        epa.epa_per_play,
        epa.success_rate,
        rr.avg_class_rank_4yr,
        rr.avg_class_points_4yr,
        rr.total_blue_chips_4yr,
        rr.blue_chip_ratio,
        ROUND(tss.wins::numeric / NULLIF(tss.games, 0), 4) AS win_pct
    FROM marts.team_season_summary tss
    JOIN rolling_recruiting rr ON rr.team = tss.team AND rr.season = tss.season
    LEFT JOIN marts.team_epa_season epa ON epa.team = tss.team AND epa.season = tss.season
),
bucket_medians AS (
    SELECT
        season,
        talent_bucket,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY wins) AS expected_wins,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY epa_per_play) AS expected_epa
    FROM expected_perf
    GROUP BY season, talent_bucket
),
combined AS (
    SELECT
        ep.team,
        ep.season,
        tss.conference,

        -- Recruiting inputs
        ep.avg_class_rank_4yr,
        ep.avg_class_points_4yr,
        ep.total_blue_chips_4yr,
        ep.blue_chip_ratio,

        -- On-field outputs
        tss.wins,
        tss.losses,
        ep.win_pct,
        tss.sp_rating,
        tss.sp_rank,
        ep.epa_per_play,
        ep.success_rate,

        -- Draft output
        COALESCE(do2.players_drafted, 0)::int AS players_drafted,
        COALESCE(do2.draft_picks_value, 0)::int AS draft_picks_value,

        -- ROI metrics
        ROUND((tss.wins - bm.expected_wins)::numeric, 2) AS wins_over_expected,
        ROUND((ep.epa_per_play - bm.expected_epa)::numeric, 4) AS epa_over_expected,
        ROUND(
            tss.wins::numeric / NULLIF(ep.avg_class_rank_4yr, 0),
            4
        ) AS recruiting_efficiency

    FROM expected_perf ep
    JOIN marts.team_season_summary tss ON tss.team = ep.team AND tss.season = ep.season
    LEFT JOIN bucket_medians bm ON bm.season = ep.season AND bm.talent_bucket = ep.talent_bucket
    LEFT JOIN draft_output do2 ON do2.team = ep.team AND do2.season = ep.season
)
SELECT
    c.*,

    -- Percentiles (per season)
    CASE WHEN c.win_pct IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.win_pct)
    END AS win_pct_pctl,
    CASE WHEN c.epa_per_play IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.epa_per_play)
    END AS epa_pctl,
    CASE WHEN c.recruiting_efficiency IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.recruiting_efficiency)
    END AS recruiting_efficiency_pctl

FROM combined c;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.recruiting_roi (team, season);

-- Query indexes
CREATE INDEX ON marts.recruiting_roi (season, recruiting_efficiency DESC);
CREATE INDEX ON marts.recruiting_roi (season, blue_chip_ratio DESC);
CREATE INDEX ON marts.recruiting_roi (conference, season);
