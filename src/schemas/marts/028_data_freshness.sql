-- Data freshness: track when each key table was last loaded and whether it's stale
-- Grain: schema_name + table_name (one row per tracked table)
-- Sources: pg_stat_user_tables activity timestamps + reltuples estimates

DROP MATERIALIZED VIEW IF EXISTS marts.data_freshness CASCADE;

CREATE MATERIALIZED VIEW marts.data_freshness AS
WITH tracked_tables AS (
    SELECT * FROM (VALUES
        ('ref', 'teams', 'static'),
        ('ref', 'conferences', 'static'),
        ('ref', 'venues', 'static'),
        ('ref', 'coaches', 'seasonal'),
        ('core', 'games', 'weekly'),
        ('core', 'drives', 'weekly'),
        ('core', 'plays', 'weekly'),
        ('core', 'game_team_stats', 'weekly'),
        ('core', 'game_player_stats', 'weekly'),
        ('core', 'roster', 'seasonal'),
        ('stats', 'player_season_stats', 'seasonal'),
        ('stats', 'team_season_stats', 'seasonal'),
        ('ratings', 'sp_ratings', 'weekly'),
        ('ratings', 'elo_ratings', 'weekly'),
        ('ratings', 'fpi_ratings', 'seasonal'),
        ('ratings', 'srs_ratings', 'seasonal'),
        ('recruiting', 'recruits', 'seasonal'),
        ('recruiting', 'team_recruiting', 'seasonal'),
        ('recruiting', 'transfer_portal', 'seasonal'),
        ('betting', 'lines', 'weekly'),
        ('draft', 'draft_picks', 'seasonal'),
        ('metrics', 'predicted_points', 'weekly'),
        ('metrics', 'win_probability', 'weekly')
    ) AS t(schema_name, table_name, expected_refresh_frequency)
),
table_stats AS (
    SELECT
        s.schemaname AS schema_name,
        s.relname AS table_name,
        c.reltuples::bigint AS row_count,
        GREATEST(
            s.last_vacuum, s.last_autovacuum,
            s.last_analyze, s.last_autoanalyze
        ) AS last_activity
    FROM pg_stat_user_tables s
    JOIN pg_class c ON s.relid = c.oid
)
SELECT
    tt.schema_name,
    tt.table_name,
    COALESCE(ts.row_count, 0) AS row_count,
    ts.last_activity,
    tt.expected_refresh_frequency,
    CASE
        WHEN ts.last_activity IS NOT NULL
        THEN ROUND(EXTRACT(EPOCH FROM (now() - ts.last_activity)) / 86400.0, 1)
    END AS days_since_activity,
    CASE
        WHEN tt.expected_refresh_frequency = 'static' THEN false
        WHEN tt.expected_refresh_frequency = 'weekly'
            AND (ts.last_activity IS NULL
                 OR ts.last_activity < now() - interval '14 days') THEN true
        WHEN tt.expected_refresh_frequency = 'seasonal'
            AND (ts.last_activity IS NULL
                 OR ts.last_activity < now() - interval '90 days') THEN true
        ELSE false
    END AS is_stale
FROM tracked_tables tt
LEFT JOIN table_stats ts
    ON ts.schema_name = tt.schema_name
    AND ts.table_name = tt.table_name
ORDER BY tt.schema_name, tt.table_name;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.data_freshness (schema_name, table_name);
