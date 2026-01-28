-- Situational splits: EPA and efficiency by game situation
-- Grain: Team × Season
-- Includes: down/distance, red zone, field position, late & close, play type

DROP MATERIALIZED VIEW IF EXISTS marts.situational_splits CASCADE;

CREATE MATERIALIZED VIEW marts.situational_splits AS
WITH play_situations AS (
    SELECT
        p.offense AS team,
        g.season,
        p.ppa,
        p.play_type,
        p.down,
        p.distance,
        p.yards_to_goal,
        p.period,
        p.clock_minutes,
        p.clock_seconds,
        p.score_diff,

        -- Situation flags
        NOT is_garbage_time(p.period::integer, p.score_diff::integer) AS is_competitive,

        -- Down classifications
        CASE
            WHEN p.down = 1 THEN 'first'
            WHEN p.down = 2 AND p.distance <= 4 THEN 'second_short'
            WHEN p.down = 2 AND p.distance > 4 THEN 'second_long'
            WHEN p.down = 3 AND p.distance <= 3 THEN 'third_short'
            WHEN p.down = 3 AND p.distance BETWEEN 4 AND 7 THEN 'third_medium'
            WHEN p.down = 3 AND p.distance > 7 THEN 'third_long'
            WHEN p.down = 4 THEN 'fourth'
            ELSE 'other'
        END AS down_situation,

        -- Standard vs passing downs (Connelly definition)
        -- Passing down: 2nd & 8+, 3rd & 5+
        CASE
            WHEN (p.down = 2 AND p.distance >= 8) OR (p.down = 3 AND p.distance >= 5) THEN true
            ELSE false
        END AS is_passing_down,

        -- Field position zones
        CASE
            WHEN p.yards_to_goal <= 20 THEN 'red_zone'
            WHEN p.yards_to_goal <= 40 THEN 'scoring_position'
            WHEN p.yards_to_goal >= 80 THEN 'backed_up'
            ELSE 'between_20s'
        END AS field_zone,

        -- Late & close: 4th quarter, margin <= 8
        CASE
            WHEN p.period = 4 AND ABS(COALESCE(p.score_diff, 0)) <= 8 THEN true
            ELSE false
        END AS is_late_and_close,

        -- Two-minute drill: last 2 mins of half
        CASE
            WHEN (p.period = 2 OR p.period = 4)
                AND COALESCE(p.clock_minutes, 0) < 2 THEN true
            ELSE false
        END AS is_two_minute,

        -- Play type classification
        CASE
            WHEN p.play_type ILIKE '%rush%' OR p.play_type ILIKE '%run%' THEN 'rush'
            WHEN p.play_type ILIKE '%pass%' OR p.play_type ILIKE '%sack%' THEN 'pass'
            ELSE 'other'
        END AS play_category

    FROM core.plays p
    JOIN core.games g ON p.game_id = g.id
    WHERE p.ppa IS NOT NULL
)
SELECT
    team,
    season,

    -- === OVERALL (non-garbage time) ===
    COUNT(*) FILTER (WHERE is_competitive) AS total_plays,
    ROUND(AVG(ppa) FILTER (WHERE is_competitive)::numeric, 4) AS epa_per_play,
    ROUND(AVG(CASE WHEN ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE is_competitive)::numeric, 4) AS success_rate,

    -- === BY DOWN ===
    -- First down
    ROUND(AVG(ppa) FILTER (WHERE is_competitive AND down = 1)::numeric, 4) AS first_down_epa,
    ROUND(AVG(CASE WHEN ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE is_competitive AND down = 1)::numeric, 4) AS first_down_success,

    -- Second down
    ROUND(AVG(ppa) FILTER (WHERE is_competitive AND down = 2)::numeric, 4) AS second_down_epa,
    ROUND(AVG(CASE WHEN ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE is_competitive AND down = 2)::numeric, 4) AS second_down_success,

    -- Third down
    ROUND(AVG(ppa) FILTER (WHERE is_competitive AND down = 3)::numeric, 4) AS third_down_epa,
    ROUND(AVG(CASE WHEN ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE is_competitive AND down = 3)::numeric, 4) AS third_down_success,
    COUNT(*) FILTER (WHERE is_competitive AND down = 3) AS third_down_attempts,

    -- Third down by distance
    ROUND(AVG(CASE WHEN ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE is_competitive AND down_situation = 'third_short')::numeric, 4) AS third_short_success,
    ROUND(AVG(CASE WHEN ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE is_competitive AND down_situation = 'third_medium')::numeric, 4) AS third_medium_success,
    ROUND(AVG(CASE WHEN ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE is_competitive AND down_situation = 'third_long')::numeric, 4) AS third_long_success,

    -- === STANDARD vs PASSING DOWNS ===
    ROUND(AVG(ppa) FILTER (WHERE is_competitive AND NOT is_passing_down)::numeric, 4) AS standard_down_epa,
    ROUND(AVG(ppa) FILTER (WHERE is_competitive AND is_passing_down)::numeric, 4) AS passing_down_epa,
    ROUND(AVG(CASE WHEN ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE is_competitive AND NOT is_passing_down)::numeric, 4) AS standard_down_success,
    ROUND(AVG(CASE WHEN ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE is_competitive AND is_passing_down)::numeric, 4) AS passing_down_success,

    -- === RED ZONE ===
    COUNT(*) FILTER (WHERE is_competitive AND field_zone = 'red_zone') AS red_zone_plays,
    ROUND(AVG(ppa) FILTER (WHERE is_competitive AND field_zone = 'red_zone')::numeric, 4) AS red_zone_epa,
    ROUND(AVG(CASE WHEN ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE is_competitive AND field_zone = 'red_zone')::numeric, 4) AS red_zone_success,

    -- === FIELD POSITION ===
    ROUND(AVG(ppa) FILTER (WHERE is_competitive AND field_zone = 'backed_up')::numeric, 4) AS backed_up_epa,
    ROUND(AVG(ppa) FILTER (WHERE is_competitive AND field_zone = 'scoring_position')::numeric, 4) AS scoring_position_epa,

    -- === LATE & CLOSE ===
    COUNT(*) FILTER (WHERE is_competitive AND is_late_and_close) AS late_close_plays,
    ROUND(AVG(ppa) FILTER (WHERE is_competitive AND is_late_and_close)::numeric, 4) AS late_close_epa,
    ROUND(AVG(CASE WHEN ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE is_competitive AND is_late_and_close)::numeric, 4) AS late_close_success,

    -- === TWO-MINUTE DRILL ===
    COUNT(*) FILTER (WHERE is_competitive AND is_two_minute) AS two_minute_plays,
    ROUND(AVG(ppa) FILTER (WHERE is_competitive AND is_two_minute)::numeric, 4) AS two_minute_epa,

    -- === PLAY TYPE ===
    -- Rush
    COUNT(*) FILTER (WHERE is_competitive AND play_category = 'rush') AS rush_plays,
    ROUND(AVG(ppa) FILTER (WHERE is_competitive AND play_category = 'rush')::numeric, 4) AS rush_epa,
    ROUND(AVG(CASE WHEN ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE is_competitive AND play_category = 'rush')::numeric, 4) AS rush_success,

    -- Pass
    COUNT(*) FILTER (WHERE is_competitive AND play_category = 'pass') AS pass_plays,
    ROUND(AVG(ppa) FILTER (WHERE is_competitive AND play_category = 'pass')::numeric, 4) AS pass_epa,
    ROUND(AVG(CASE WHEN ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE is_competitive AND play_category = 'pass')::numeric, 4) AS pass_success,

    -- Play calling tendency (rush rate)
    ROUND(
        COUNT(*) FILTER (WHERE is_competitive AND play_category = 'rush')::numeric /
        NULLIF(COUNT(*) FILTER (WHERE is_competitive AND play_category IN ('rush', 'pass')), 0),
        4
    ) AS rush_rate

FROM play_situations
GROUP BY team, season;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.situational_splits (team, season);

-- Query indexes
CREATE INDEX ON marts.situational_splits (season);
CREATE INDEX ON marts.situational_splits (third_down_success DESC);
CREATE INDEX ON marts.situational_splits (red_zone_success DESC);
