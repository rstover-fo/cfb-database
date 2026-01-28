-- Coach record: tenure and performance by team
-- Grain: Coach × Team
-- Note: ref.coaches stores seasons as JSONB array

DROP MATERIALIZED VIEW IF EXISTS marts.coach_record CASCADE;

CREATE MATERIALIZED VIEW marts.coach_record AS
WITH coach_seasons AS (
    -- Unnest the seasons JSONB array from coaches table
    SELECT
        c.first_name,
        c.last_name,
        c.first_name || ' ' || c.last_name AS coach_name,
        s->>'school' AS team,
        (s->>'year')::int AS season,
        (s->>'games')::int AS games,
        (s->>'wins')::int AS wins,
        (s->>'losses')::int AS losses,
        (s->>'ties')::int AS ties,
        (s->>'preseason_rank')::int AS preseason_rank,
        (s->>'postseason_rank')::int AS postseason_rank,
        s->>'srs' AS srs,
        s->>'sp_overall' AS sp_overall,
        s->>'sp_offense' AS sp_offense,
        s->>'sp_defense' AS sp_defense
    FROM ref.coaches c,
    LATERAL jsonb_array_elements(c.seasons) AS s
    WHERE s->>'school' IS NOT NULL
),
coach_team_agg AS (
    SELECT
        coach_name,
        first_name,
        last_name,
        team,
        MIN(season) AS first_season,
        MAX(season) AS last_season,
        COUNT(DISTINCT season) AS seasons_at_team,
        SUM(games) AS total_games,
        SUM(wins) AS total_wins,
        SUM(losses) AS total_losses,
        SUM(COALESCE(ties, 0)) AS total_ties,
        -- First and last season rankings
        MIN(preseason_rank) FILTER (WHERE season = (SELECT MIN(season) FROM coach_seasons cs2 WHERE cs2.coach_name = coach_seasons.coach_name AND cs2.team = coach_seasons.team)) AS first_preseason_rank,
        MIN(postseason_rank) FILTER (WHERE season = (SELECT MAX(season) FROM coach_seasons cs2 WHERE cs2.coach_name = coach_seasons.coach_name AND cs2.team = coach_seasons.team)) AS last_postseason_rank
    FROM coach_seasons
    GROUP BY coach_name, first_name, last_name, team
)
SELECT
    cta.coach_name,
    cta.first_name,
    cta.last_name,
    cta.team,
    cta.first_season,
    cta.last_season,
    cta.seasons_at_team,

    -- Record
    cta.total_games,
    cta.total_wins,
    cta.total_losses,
    cta.total_ties,

    -- Win percentage
    ROUND(
        cta.total_wins::numeric /
        NULLIF(cta.total_wins + cta.total_losses, 0),
        4
    ) AS win_pct,

    -- Ranking trajectory
    cta.first_preseason_rank,
    cta.last_postseason_rank,

    -- Average recruiting rank during tenure
    ROUND(AVG(tr.rank)::numeric, 1) AS avg_recruiting_rank

FROM coach_team_agg cta
LEFT JOIN recruiting.team_recruiting tr
    ON tr.team = cta.team
    AND tr.year BETWEEN cta.first_season AND cta.last_season;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.coach_record (coach_name, team);

-- Query indexes
CREATE INDEX ON marts.coach_record (team);
CREATE INDEX ON marts.coach_record (coach_name);
CREATE INDEX ON marts.coach_record (win_pct DESC);
CREATE INDEX ON marts.coach_record (total_wins DESC);
