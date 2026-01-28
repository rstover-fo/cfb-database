-- Team EPA season summary with benchmarks
-- Aggregates game-level EPA from _game_epa_calc into season totals
-- Depends on: marts._game_epa_calc

DROP MATERIALIZED VIEW IF EXISTS marts.team_epa_season CASCADE;

CREATE MATERIALIZED VIEW marts.team_epa_season AS
SELECT
    epa.team,
    g.season,

    -- Aggregated EPA metrics (averaged across games)
    ROUND(AVG(epa.epa_per_play)::numeric, 4) AS epa_per_play,
    ROUND(AVG(epa.success_rate)::numeric, 4) AS success_rate,
    ROUND(AVG(epa.explosiveness)::numeric, 4) AS explosiveness,

    -- EPA tier benchmark
    -- Based on historical CFB EPA distributions:
    -- Elite: >= 0.16, Above Avg: >= 0.05, Avg: >= -0.05, Below Avg: >= -0.15
    CASE
        WHEN AVG(epa.epa_per_play) >= 0.16 THEN 'elite'
        WHEN AVG(epa.epa_per_play) >= 0.05 THEN 'above_avg'
        WHEN AVG(epa.epa_per_play) >= -0.05 THEN 'average'
        WHEN AVG(epa.epa_per_play) >= -0.15 THEN 'below_avg'
        ELSE 'struggling'
    END AS epa_tier,

    -- Total plays (non-garbage time)
    SUM(epa.plays_non_garbage)::bigint AS total_plays,

    -- Game count
    COUNT(*)::int AS games_played

FROM marts._game_epa_calc epa
JOIN core.games g ON epa.game_id = g.id
GROUP BY epa.team, g.season;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.team_epa_season (team, season);

-- Query indexes
CREATE INDEX ON marts.team_epa_season (season);
CREATE INDEX ON marts.team_epa_season (season, epa_tier);
CREATE INDEX ON marts.team_epa_season (epa_per_play DESC);
