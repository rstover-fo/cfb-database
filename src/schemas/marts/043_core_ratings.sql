-- marts.core_ratings
-- CFBD CORE (Context & Opponent-Relative Efficiency) ratings by team-season:
-- passthrough of ratings.core_ratings (year -> season).
-- Grain: (team, season) -- one row per team per season, 2016+ only (CFBD
-- publishes retrospective CORE from 2016; earlier seasons are absent, not zero).
-- Semantics: offense = points created above average per 100 qualifying plays
-- (higher is better); defense = points allowed above average per 100 plays
-- (LOWER is better); overall = offense - defense. through_week /
-- through_season_type are as-of markers: in-season the row is a snapshot
-- through that week, advanced in place by the daily merge load.

DROP MATERIALIZED VIEW IF EXISTS marts.core_ratings CASCADE;

CREATE MATERIALIZED VIEW marts.core_ratings AS
SELECT
    r.year AS season,
    r.team,
    r.conference,

    -- Ratings
    ROUND(r.overall::numeric, 2) AS overall,
    ROUND(r.offense::numeric, 2) AS offense,
    ROUND(r.defense::numeric, 2) AS defense,

    -- Qualifying-play volume behind the ratings
    r.offense_plays,
    r.defense_plays,

    -- As-of markers + model provenance
    r.through_week,
    r.through_season_type,
    r.model_version,

    -- Computed rankings within season. Defense ranked ASC: lower is better.
    RANK() OVER (PARTITION BY r.year ORDER BY r.overall DESC NULLS LAST) AS overall_rank,
    RANK() OVER (PARTITION BY r.year ORDER BY r.offense DESC NULLS LAST) AS offense_rank,
    RANK() OVER (PARTITION BY r.year ORDER BY r.defense ASC NULLS LAST) AS defense_rank

FROM ratings.core_ratings r;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.core_ratings (team, season);

-- Query indexes
CREATE INDEX ON marts.core_ratings (season);
CREATE INDEX ON marts.core_ratings (season, overall_rank);

-- Empty-guard: ratings.core_ratings backs this mart. If it ever refreshes to
-- zero rows, fail loudly at deploy time instead of silently serving an empty
-- mart downstream.
DO $$
BEGIN
    IF (SELECT count(*) FROM marts.core_ratings) = 0 THEN
        RAISE EXCEPTION 'marts.core_ratings is empty: ratings.core_ratings has no rows. Run the ratings backfill (2016+, e.g. deploy-schema action=backfill sources=ratings) and re-apply before use.';
    END IF;
END $$;
