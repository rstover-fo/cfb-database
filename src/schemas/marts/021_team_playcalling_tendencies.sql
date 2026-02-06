-- marts.team_playcalling_tendencies
-- Team play-calling mix by situation (down, distance, field position, score state).
-- Grain: team + season + down + distance_bucket + field_position + score_diff_bucket
--
-- Sources: marts.play_epa, core.plays (for score_diff)
-- Unique key: (team, season, down, distance_bucket, field_position, score_diff_bucket)
-- Refresh layer: 2 (depends on play_epa from Layer 1)

DROP MATERIALIZED VIEW IF EXISTS marts.team_playcalling_tendencies CASCADE;

CREATE MATERIALIZED VIEW marts.team_playcalling_tendencies AS
WITH base_plays AS (
    SELECT
        pe.offense AS team,
        pe.season,
        pe.down,
        pe.distance_bucket,
        pe.field_position,
        pe.play_category,
        CASE
            WHEN COALESCE(p.score_diff, 0) >= 14 THEN 'big_lead'
            WHEN COALESCE(p.score_diff, 0) >= 1 THEN 'small_lead'
            WHEN COALESCE(p.score_diff, 0) = 0 THEN 'tied'
            WHEN COALESCE(p.score_diff, 0) >= -13 THEN 'small_deficit'
            ELSE 'big_deficit'
        END AS score_diff_bucket
    FROM marts.play_epa pe
    JOIN core.plays p ON p.id = pe.play_id
    WHERE NOT pe.is_garbage_time
      AND pe.play_category IN ('rush', 'pass')
)
SELECT
    team,
    season,
    down,
    distance_bucket,
    field_position,
    score_diff_bucket,
    COUNT(*)::bigint AS total_plays,
    COUNT(*) FILTER (WHERE play_category = 'rush')::bigint AS rush_plays,
    COUNT(*) FILTER (WHERE play_category = 'pass')::bigint AS pass_plays,
    ROUND(COUNT(*) FILTER (WHERE play_category = 'rush')::numeric
        / NULLIF(COUNT(*), 0), 4) AS run_rate,
    ROUND(COUNT(*) FILTER (WHERE play_category = 'pass')::numeric
        / NULLIF(COUNT(*), 0), 4) AS pass_rate
FROM base_plays
GROUP BY team, season, down, distance_bucket, field_position, score_diff_bucket
WITH DATA;

-- Indexes
CREATE UNIQUE INDEX idx_playcalling_tendencies_pk
    ON marts.team_playcalling_tendencies (team, season, down, distance_bucket, field_position, score_diff_bucket);
CREATE INDEX idx_playcalling_tendencies_team_season
    ON marts.team_playcalling_tendencies (team, season);
CREATE INDEX idx_playcalling_tendencies_leaderboard
    ON marts.team_playcalling_tendencies (season, down, distance_bucket, run_rate DESC);
