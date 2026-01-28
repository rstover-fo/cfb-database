-- Coach record: tenure and performance by team
-- Grain: Coach × Team
-- Note: ref.coaches is flattened by dlt into coaches + coaches__seasons tables

DROP MATERIALIZED VIEW IF EXISTS marts.coach_record CASCADE;

CREATE MATERIALIZED VIEW marts.coach_record AS
WITH coach_seasons AS (
    -- Join coaches with their seasons via dlt parent/child relationship
    SELECT
        c.first_name,
        c.last_name,
        c.first_name || ' ' || c.last_name AS coach_name,
        cs.school AS team,
        cs.year AS season,
        cs.games,
        cs.wins,
        cs.losses,
        cs.ties,
        cs.preseason_rank,
        cs.postseason_rank,
        cs.srs,
        cs.sp_overall,
        cs.sp_offense,
        cs.sp_defense
    FROM ref.coaches c
    JOIN ref.coaches__seasons cs ON cs._dlt_parent_id = c._dlt_id
    WHERE cs.school IS NOT NULL
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
        -- Best rankings during tenure
        MIN(preseason_rank) FILTER (WHERE preseason_rank IS NOT NULL) AS best_preseason_rank,
        MIN(postseason_rank) FILTER (WHERE postseason_rank IS NOT NULL) AS best_postseason_rank,
        -- Average SP rating during tenure
        ROUND(AVG(sp_overall)::numeric, 2) AS avg_sp_rating
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
    cta.total_games::int,
    cta.total_wins::int,
    cta.total_losses::int,
    cta.total_ties::int,

    -- Win percentage
    ROUND(
        cta.total_wins::numeric /
        NULLIF(cta.total_wins + cta.total_losses, 0),
        4
    ) AS win_pct,

    -- Ranking trajectory
    cta.best_preseason_rank,
    cta.best_postseason_rank,
    cta.avg_sp_rating,

    -- Average recruiting rank during tenure
    ROUND(AVG(tr.rank)::numeric, 1) AS avg_recruiting_rank

FROM coach_team_agg cta
LEFT JOIN recruiting.team_recruiting tr
    ON tr.team = cta.team
    AND tr.year BETWEEN cta.first_season AND cta.last_season
GROUP BY
    cta.coach_name, cta.first_name, cta.last_name, cta.team,
    cta.first_season, cta.last_season, cta.seasons_at_team,
    cta.total_games, cta.total_wins, cta.total_losses, cta.total_ties,
    cta.best_preseason_rank, cta.best_postseason_rank, cta.avg_sp_rating;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.coach_record (coach_name, team);

-- Query indexes
CREATE INDEX ON marts.coach_record (team);
CREATE INDEX ON marts.coach_record (coach_name);
CREATE INDEX ON marts.coach_record (win_pct DESC);
CREATE INDEX ON marts.coach_record (total_wins DESC);
