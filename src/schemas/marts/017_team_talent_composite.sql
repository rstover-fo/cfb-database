-- Team Talent Composite Mart
-- Combines recruiting stars + transfer portal to create roster talent score
-- Depends on: recruiting.recruits, recruiting.transfer_portal, core.roster
--
-- Usage: Analyze roster composition by talent level, track blue chip ratios,
--        compare transfer portal impact across programs

CREATE SCHEMA IF NOT EXISTS marts;

DROP MATERIALIZED VIEW IF EXISTS marts.team_talent_composite CASCADE;

CREATE MATERIALIZED VIEW marts.team_talent_composite AS
WITH roster_talent AS (
    SELECT
        r.team,
        r.year as season,
        COUNT(*) as roster_size,
        -- Count by star rating (from original recruiting)
        COUNT(*) FILTER (WHERE rec.stars = 5) as five_stars,
        COUNT(*) FILTER (WHERE rec.stars = 4) as four_stars,
        COUNT(*) FILTER (WHERE rec.stars = 3) as three_stars,
        COUNT(*) FILTER (WHERE rec.stars <= 2 OR rec.stars IS NULL) as low_stars,
        -- Average rating (NULL-safe)
        ROUND(AVG(rec.rating)::numeric, 4) as avg_recruit_rating,
        -- Blue chip ratio (4* and 5*)
        ROUND(
            COUNT(*) FILTER (WHERE rec.stars >= 4)::numeric
            / NULLIF(COUNT(*), 0),
            3
        ) as blue_chip_ratio
    FROM core.roster r
    LEFT JOIN recruiting.recruits rec ON
        r.id = rec.athlete_id
        AND rec.year BETWEEN r.year - 5 AND r.year  -- Within eligibility window
    GROUP BY r.team, r.year
),
transfer_impact AS (
    SELECT
        destination as team,
        season,
        COUNT(*) as transfers_in,
        ROUND(AVG(stars)::numeric, 2) as avg_transfer_stars
    FROM recruiting.transfer_portal
    WHERE destination IS NOT NULL
    GROUP BY destination, season
)
SELECT
    rt.team,
    rt.season,
    rt.roster_size,
    rt.five_stars,
    rt.four_stars,
    rt.three_stars,
    rt.low_stars,
    rt.blue_chip_ratio,
    rt.avg_recruit_rating,
    COALESCE(ti.transfers_in, 0) as transfers_in,
    ti.avg_transfer_stars,
    -- Composite talent score (weighted average of star ratings)
    ROUND(
        (COALESCE(rt.five_stars, 0) * 5
         + COALESCE(rt.four_stars, 0) * 4
         + COALESCE(rt.three_stars, 0) * 3
         + COALESCE(rt.low_stars, 0) * 2)::numeric
        / NULLIF(rt.roster_size, 0),
        2
    ) as talent_score
FROM roster_talent rt
LEFT JOIN transfer_impact ti ON rt.team = ti.team AND rt.season = ti.season
WHERE rt.roster_size > 0;

-- Indexes for common query patterns
CREATE UNIQUE INDEX ON marts.team_talent_composite (team, season);
CREATE INDEX ON marts.team_talent_composite (season);
CREATE INDEX ON marts.team_talent_composite (talent_score DESC);
CREATE INDEX ON marts.team_talent_composite (blue_chip_ratio DESC);

COMMENT ON MATERIALIZED VIEW marts.team_talent_composite IS
'Team roster talent composition by season. Combines recruiting ratings with current rosters.';
