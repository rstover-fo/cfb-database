-- marts.team_wepa_season
-- Opponent-adjusted EPA (WEPA) by team-season: passthrough of metrics.wepa_team_season
-- with double-underscore dlt column names flattened to single-underscore friendly names.
-- Grain: (team, season) -- one row per team per season
-- Source: metrics.wepa_team_season (year -> season)

DROP MATERIALIZED VIEW IF EXISTS marts.team_wepa_season CASCADE;

CREATE MATERIALIZED VIEW marts.team_wepa_season AS
SELECT
    w.year AS season,
    w.team_id,
    w.team,
    w.conference,

    -- EPA (opponent-adjusted)
    ROUND(w."epa__total"::numeric, 4) AS epa_total,
    ROUND(w."epa__passing"::numeric, 4) AS epa_passing,
    ROUND(w."epa__rushing"::numeric, 4) AS epa_rushing,
    ROUND(w."epa_allowed__total"::numeric, 4) AS epa_allowed_total,
    ROUND(w."epa_allowed__passing"::numeric, 4) AS epa_allowed_passing,
    ROUND(w."epa_allowed__rushing"::numeric, 4) AS epa_allowed_rushing,

    -- Success rate (opponent-adjusted)
    ROUND(w."success_rate__total"::numeric, 4) AS success_rate_total,
    ROUND(w."success_rate__standard_downs"::numeric, 4) AS success_rate_standard_downs,
    ROUND(w."success_rate__passing_downs"::numeric, 4) AS success_rate_passing_downs,
    ROUND(w."success_rate_allowed__total"::numeric, 4) AS success_rate_allowed_total,
    ROUND(w."success_rate_allowed__standard_downs"::numeric, 4) AS success_rate_allowed_standard_downs,
    ROUND(w."success_rate_allowed__passing_downs"::numeric, 4) AS success_rate_allowed_passing_downs,

    -- Rushing yardage splits (line/second-level/open-field/highlight yards)
    ROUND(w."rushing__line_yards"::numeric, 2) AS rushing_line_yards,
    ROUND(w."rushing__second_level_yards"::numeric, 2) AS rushing_second_level_yards,
    ROUND(w."rushing__open_field_yards"::numeric, 2) AS rushing_open_field_yards,
    ROUND(w."rushing__highlight_yards"::numeric, 2) AS rushing_highlight_yards,
    ROUND(w."rushing_allowed__line_yards"::numeric, 2) AS rushing_allowed_line_yards,
    ROUND(w."rushing_allowed__second_level_yards"::numeric, 2) AS rushing_allowed_second_level_yards,
    ROUND(w."rushing_allowed__open_field_yards"::numeric, 2) AS rushing_allowed_open_field_yards,
    ROUND(w."rushing_allowed__highlight_yards"::numeric, 2) AS rushing_allowed_highlight_yards,

    -- Explosiveness
    ROUND(w.explosiveness::numeric, 4) AS explosiveness,
    ROUND(w.explosiveness_allowed::numeric, 4) AS explosiveness_allowed,

    -- Computed rankings within season
    RANK() OVER (PARTITION BY w.year ORDER BY w."epa__total" DESC NULLS LAST) AS epa_rank,
    RANK() OVER (PARTITION BY w.year ORDER BY w."epa_allowed__total" ASC NULLS LAST) AS defense_rank

FROM metrics.wepa_team_season w;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.team_wepa_season (team, season);

-- Query indexes
CREATE INDEX ON marts.team_wepa_season (season);
CREATE INDEX ON marts.team_wepa_season (season, epa_rank);

-- Empty-guard: metrics.wepa_team_season backs this mart. If it ever refreshes to zero
-- rows, fail loudly at deploy time instead of silently serving an empty mart downstream.
DO $$
BEGIN
    IF (SELECT count(*) FROM marts.team_wepa_season) = 0 THEN
        RAISE EXCEPTION 'marts.team_wepa_season is empty: metrics.wepa_team_season has no rows. Run the metrics backfill (deploy/tier1-backfill, action=backfill, sources=metrics) and refresh this mart before use.';
    END IF;
END $$;
