-- marts.player_wepa_season
-- Player-level WEPA (opponent-adjusted EPA) and kicker PAAR, unioned into a tall shape.
-- Grain: (season, athlete_id, category) -- one row per player per season per category
-- category = 'passing' | 'rushing' -> metric is wepa (wepa set, paar NULL)
-- category = 'kicking'             -> metric is paar (paar set, wepa NULL, position NULL,
--                                      plays = attempts)
-- Sources: metrics.wepa_players_passing, metrics.wepa_players_rushing,
--          metrics.wepa_players_kicking

DROP MATERIALIZED VIEW IF EXISTS marts.player_wepa_season CASCADE;

CREATE MATERIALIZED VIEW marts.player_wepa_season AS
WITH combined AS (
    SELECT
        year AS season,
        athlete_id,
        athlete_name,
        position,
        team,
        conference,
        'passing'::text AS category,
        wepa,
        NULL::double precision AS paar,
        wepa AS metric,
        plays
    FROM metrics.wepa_players_passing

    UNION ALL

    SELECT
        year AS season,
        athlete_id,
        athlete_name,
        position,
        team,
        conference,
        'rushing'::text AS category,
        wepa,
        NULL::double precision AS paar,
        wepa AS metric,
        plays
    FROM metrics.wepa_players_rushing

    UNION ALL

    SELECT
        year AS season,
        athlete_id,
        athlete_name,
        NULL::character varying AS position,
        team,
        conference,
        'kicking'::text AS category,
        NULL::double precision AS wepa,
        paar,
        paar AS metric,
        attempts AS plays
    FROM metrics.wepa_players_kicking
)
SELECT
    season,
    athlete_id,
    athlete_name,
    position,
    team,
    conference,
    category,
    wepa,
    paar,
    metric,
    plays,
    RANK() OVER (PARTITION BY season, category ORDER BY metric DESC NULLS LAST) AS season_rank
FROM combined;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.player_wepa_season (season, athlete_id, category);

-- Query indexes
CREATE INDEX ON marts.player_wepa_season (season, category, season_rank);

-- Empty-guard: metrics.wepa_players_passing/rushing/kicking back this mart. If they ever
-- refresh to zero rows, fail loudly at deploy time instead of silently serving an empty
-- mart downstream.
DO $$
BEGIN
    IF (SELECT count(*) FROM marts.player_wepa_season) = 0 THEN
        RAISE EXCEPTION 'marts.player_wepa_season is empty: metrics.wepa_players_passing/rushing/kicking have no rows. Run the metrics backfill (deploy/tier1-backfill, action=backfill, sources=metrics) and refresh this mart before use.';
    END IF;
END $$;
