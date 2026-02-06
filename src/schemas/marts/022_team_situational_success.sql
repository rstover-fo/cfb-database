-- marts.team_situational_success
-- Team situational effectiveness (success rate, EPA, explosiveness) by context.
-- Grain: team + season + down + distance_bucket + field_position + score_diff_bucket
--
-- Sources: marts.play_epa, core.plays (for score_diff)
-- Unique key: (team, season, down, distance_bucket, field_position, score_diff_bucket)
-- Refresh layer: 2 (depends on play_epa from Layer 1)

DROP MATERIALIZED VIEW IF EXISTS marts.team_situational_success CASCADE;

CREATE MATERIALIZED VIEW marts.team_situational_success AS
WITH base_plays AS (
    SELECT
        pe.offense AS team,
        pe.season,
        pe.down,
        pe.distance_bucket,
        pe.field_position,
        pe.play_category,
        pe.success,
        pe.explosive,
        pe.epa,
        pe.yards_gained,
        pe.distance,
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
    -- Overall success metrics (NULL if < 10 plays)
    CASE WHEN COUNT(*) >= 10 THEN
        ROUND(AVG(success)::numeric, 4)
    END AS success_rate,
    CASE WHEN COUNT(*) >= 10 THEN
        ROUND(AVG(epa)::numeric, 4)
    END AS avg_epa,
    CASE WHEN COUNT(*) >= 10 THEN
        ROUND(AVG(explosive)::numeric, 4)
    END AS explosive_rate,
    -- Rush-specific
    CASE WHEN COUNT(*) FILTER (WHERE play_category = 'rush') >= 10 THEN
        ROUND(AVG(success) FILTER (WHERE play_category = 'rush')::numeric, 4)
    END AS rush_success_rate,
    CASE WHEN COUNT(*) FILTER (WHERE play_category = 'rush') >= 10 THEN
        ROUND(AVG(epa) FILTER (WHERE play_category = 'rush')::numeric, 4)
    END AS rush_avg_epa,
    -- Pass-specific
    CASE WHEN COUNT(*) FILTER (WHERE play_category = 'pass') >= 10 THEN
        ROUND(AVG(success) FILTER (WHERE play_category = 'pass')::numeric, 4)
    END AS pass_success_rate,
    CASE WHEN COUNT(*) FILTER (WHERE play_category = 'pass') >= 10 THEN
        ROUND(AVG(epa) FILTER (WHERE play_category = 'pass')::numeric, 4)
    END AS pass_avg_epa,
    -- Yardage success rate: only meaningful on 3rd/4th down
    CASE WHEN down IN (3, 4) AND COUNT(*) >= 10 THEN
        ROUND(AVG(CASE WHEN yards_gained >= distance THEN 1.0 ELSE 0.0 END)::numeric, 4)
    END AS yardage_success_rate
FROM base_plays
GROUP BY team, season, down, distance_bucket, field_position, score_diff_bucket
WITH DATA;

-- Indexes
CREATE UNIQUE INDEX idx_situational_success_pk
    ON marts.team_situational_success (team, season, down, distance_bucket, field_position, score_diff_bucket);
CREATE INDEX idx_situational_success_team_season
    ON marts.team_situational_success (team, season);
CREATE INDEX idx_situational_success_season
    ON marts.team_situational_success (season);
