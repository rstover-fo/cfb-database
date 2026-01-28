-- Helper: EPA calculations per game/team (excluding garbage time)
-- This is a building block for team_epa_season and other EPA-based views
--
-- Garbage time definition (inlined for performance):
--   - Q4 with margin > 28
--   - Q3+ with margin > 35

DROP MATERIALIZED VIEW IF EXISTS marts._game_epa_calc CASCADE;

CREATE MATERIALIZED VIEW marts._game_epa_calc AS
SELECT
    p.game_id,
    p.offense AS team,

    -- EPA/play (excluding garbage time)
    -- Inline garbage time check for performance (avoids PL/pgSQL function call per row)
    ROUND(AVG(p.ppa) FILTER (
        WHERE NOT (
            (p.period = 4 AND ABS(COALESCE(p.score_diff, 0)) > 28) OR
            (p.period >= 3 AND ABS(COALESCE(p.score_diff, 0)) > 35)
        )
    )::numeric, 4) AS epa_per_play,

    -- Success rate: % of plays with positive EPA (excluding garbage time)
    ROUND(AVG(CASE WHEN p.ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (
        WHERE NOT (
            (p.period = 4 AND ABS(COALESCE(p.score_diff, 0)) > 28) OR
            (p.period >= 3 AND ABS(COALESCE(p.score_diff, 0)) > 35)
        )
    )::numeric, 4) AS success_rate,

    -- Explosiveness: avg EPA on successful plays only (excluding garbage time)
    ROUND(AVG(p.ppa) FILTER (
        WHERE p.ppa > 0
        AND NOT (
            (p.period = 4 AND ABS(COALESCE(p.score_diff, 0)) > 28) OR
            (p.period >= 3 AND ABS(COALESCE(p.score_diff, 0)) > 35)
        )
    )::numeric, 4) AS explosiveness,

    -- Play counts
    COUNT(*) FILTER (
        WHERE NOT (
            (p.period = 4 AND ABS(COALESCE(p.score_diff, 0)) > 28) OR
            (p.period >= 3 AND ABS(COALESCE(p.score_diff, 0)) > 35)
        )
    ) AS plays_non_garbage,
    COUNT(*) AS plays_total

FROM core.plays p
WHERE p.ppa IS NOT NULL  -- Only include plays with EPA values
GROUP BY p.game_id, p.offense;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts._game_epa_calc (game_id, team);

-- Query indexes
CREATE INDEX ON marts._game_epa_calc (team);
