-- Recruiting class breakdown: star distribution and position groups
-- Grain: Team × Year
-- Includes: star counts, blue chip ratio, top positions

DROP MATERIALIZED VIEW IF EXISTS marts.recruiting_class CASCADE;

CREATE MATERIALIZED VIEW marts.recruiting_class AS
WITH recruit_details AS (
    SELECT
        r.committed_to AS team,
        r.year,
        r.stars,
        r.rating,
        r.position,
        r.ranking,
        -- Position groupings
        CASE
            WHEN r.position IN ('QB') THEN 'qb'
            WHEN r.position IN ('RB', 'FB', 'APB') THEN 'rb'
            WHEN r.position IN ('WR', 'TE') THEN 'receiver'
            WHEN r.position IN ('OT', 'OG', 'OC', 'OL') THEN 'oline'
            WHEN r.position IN ('DE', 'DT', 'DL', 'EDGE', 'SDE', 'WDE') THEN 'dline'
            WHEN r.position IN ('ILB', 'OLB', 'LB') THEN 'lb'
            WHEN r.position IN ('CB', 'S', 'DB') THEN 'db'
            WHEN r.position IN ('K', 'P', 'LS') THEN 'specialist'
            ELSE 'ath'
        END AS position_group
    FROM recruiting.recruits r
    WHERE r.committed_to IS NOT NULL
),
position_counts AS (
    SELECT
        team,
        year,
        position_group,
        COUNT(*) AS cnt
    FROM recruit_details
    GROUP BY team, year, position_group
),
top_position AS (
    SELECT DISTINCT ON (team, year)
        team,
        year,
        position_group AS top_position_group
    FROM position_counts
    ORDER BY team, year, cnt DESC
)
SELECT
    rd.team,
    rd.year,

    -- Team recruiting summary (from team_recruiting table)
    tr.rank AS national_rank,
    tr.points AS total_points,

    -- Commit counts
    COUNT(*) AS total_commits,

    -- Star breakdown
    COUNT(*) FILTER (WHERE rd.stars = 5)::int AS five_stars,
    COUNT(*) FILTER (WHERE rd.stars = 4)::int AS four_stars,
    COUNT(*) FILTER (WHERE rd.stars = 3)::int AS three_stars,
    COUNT(*) FILTER (WHERE rd.stars = 2)::int AS two_stars,
    COUNT(*) FILTER (WHERE rd.stars IS NULL OR rd.stars < 2)::int AS unrated,

    -- Blue chip ratio (4* and 5* / total)
    ROUND(
        (COUNT(*) FILTER (WHERE rd.stars >= 4))::numeric /
        NULLIF(COUNT(*), 0),
        4
    ) AS blue_chip_ratio,

    -- Average rating
    ROUND(AVG(rd.rating)::numeric, 4) AS avg_rating,
    MAX(rd.rating) AS top_rating,

    -- Best recruit rank
    MIN(rd.ranking) FILTER (WHERE rd.ranking IS NOT NULL) AS best_recruit_rank,

    -- Position group counts
    COUNT(*) FILTER (WHERE position_group = 'qb')::int AS qb_commits,
    COUNT(*) FILTER (WHERE position_group = 'rb')::int AS rb_commits,
    COUNT(*) FILTER (WHERE position_group = 'receiver')::int AS receiver_commits,
    COUNT(*) FILTER (WHERE position_group = 'oline')::int AS oline_commits,
    COUNT(*) FILTER (WHERE position_group = 'dline')::int AS dline_commits,
    COUNT(*) FILTER (WHERE position_group = 'lb')::int AS lb_commits,
    COUNT(*) FILTER (WHERE position_group = 'db')::int AS db_commits,

    -- Top position group
    tp.top_position_group

FROM recruit_details rd
LEFT JOIN recruiting.team_recruiting tr ON tr.team = rd.team AND tr.year = rd.year
LEFT JOIN top_position tp ON tp.team = rd.team AND tp.year = rd.year
GROUP BY rd.team, rd.year, tr.rank, tr.points, tp.top_position_group;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.recruiting_class (team, year);

-- Query indexes
CREATE INDEX ON marts.recruiting_class (year);
CREATE INDEX ON marts.recruiting_class (national_rank);
CREATE INDEX ON marts.recruiting_class (blue_chip_ratio DESC);
