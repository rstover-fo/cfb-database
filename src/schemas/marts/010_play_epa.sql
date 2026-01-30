-- Per-play EPA metrics with situational flags
-- Foundation for player attribution and advanced situational analysis
-- Filters out non-scrimmage plays and null EPA

DROP MATERIALIZED VIEW IF EXISTS marts.play_epa CASCADE;

CREATE MATERIALIZED VIEW marts.play_epa AS
SELECT
    p.id AS play_id,
    p.game_id,
    p.drive_id,
    p.season,
    p.offense,
    p.defense,
    p.down,
    p.distance,
    p.yards_to_goal,
    p.yards_gained,
    p.play_type,
    p.play_text,
    p.scoring,
    p.period,
    -- EPA from CFBD API
    p.ppa AS epa,
    -- Success: positive EPA
    CASE WHEN p.ppa > 0 THEN 1 ELSE 0 END AS success,
    -- Explosive: EPA > 0.5 on successful plays
    CASE WHEN p.ppa > 0.5 THEN 1 ELSE 0 END AS explosive,
    -- Down classification
    CASE
        WHEN p.down = 1 THEN 'first'
        WHEN p.down = 2 THEN 'second'
        WHEN p.down = 3 THEN 'third'
        WHEN p.down = 4 THEN 'fourth'
    END AS down_name,
    -- Distance bucket
    CASE
        WHEN p.distance <= 3 THEN 'short'
        WHEN p.distance <= 7 THEN 'medium'
        ELSE 'long'
    END AS distance_bucket,
    -- Field position zone
    CASE
        WHEN p.yards_to_goal <= 20 THEN 'red_zone'
        WHEN p.yards_to_goal <= 40 THEN 'opponent_territory'
        WHEN p.yards_to_goal <= 60 THEN 'midfield'
        ELSE 'own_territory'
    END AS field_position,
    -- Garbage time flag (inlined for performance)
    CASE
        WHEN (p.period = 4 AND ABS(COALESCE(p.score_diff, 0)) > 28) OR
             (p.period >= 3 AND ABS(COALESCE(p.score_diff, 0)) > 35) THEN true
        ELSE false
    END AS is_garbage_time,
    -- Play type category
    CASE
        WHEN p.play_type ILIKE '%rush%' OR p.play_type ILIKE '%run%' THEN 'rush'
        WHEN p.play_type ILIKE '%pass%' OR p.play_type ILIKE '%sack%' THEN 'pass'
        ELSE 'other'
    END AS play_category
FROM core.plays p
WHERE p.ppa IS NOT NULL
  AND p.play_type NOT IN ('Timeout', 'End Period', 'End of Half', 'End of Game', 'Kickoff', 'Kickoff Return (Offense)');

-- Indexes for aggregation queries
CREATE UNIQUE INDEX ON marts.play_epa (play_id);
CREATE INDEX ON marts.play_epa (game_id);
CREATE INDEX ON marts.play_epa (offense, season);
CREATE INDEX ON marts.play_epa (defense, season);
CREATE INDEX ON marts.play_epa (down_name, distance_bucket);
CREATE INDEX ON marts.play_epa (field_position);
CREATE INDEX ON marts.play_epa (play_category);
CREATE INDEX ON marts.play_epa (is_garbage_time) WHERE NOT is_garbage_time;
