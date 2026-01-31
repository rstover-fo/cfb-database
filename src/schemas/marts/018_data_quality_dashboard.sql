-- Data Quality Dashboard
-- Monitors data coverage, freshness, and integrity across all schemas
--
-- Usage: Run periodically to verify data pipeline health
-- Query: SELECT * FROM analytics.data_quality_dashboard ORDER BY check_type, metric;

-- Create analytics schema if not exists
CREATE SCHEMA IF NOT EXISTS analytics;

-- Drop and recreate view (views don't have IF NOT EXISTS for CREATE OR REPLACE)
DROP VIEW IF EXISTS analytics.data_quality_dashboard CASCADE;

CREATE VIEW analytics.data_quality_dashboard AS

-- Coverage metrics: what years do we have data for?
WITH coverage AS (
    SELECT 'games' as metric,
           MIN(season)::int as min_year,
           MAX(season)::int as max_year,
           COUNT(*)::bigint as row_count
    FROM core.games
    UNION ALL
    SELECT 'plays', MIN(season), MAX(season), COUNT(*) FROM core.plays
    UNION ALL
    SELECT 'drives', MIN(season), MAX(season), COUNT(*) FROM core.drives
    UNION ALL
    SELECT 'team_season_stats', MIN(season), MAX(season), COUNT(*) FROM stats.team_season_stats
    UNION ALL
    SELECT 'player_season_stats', MIN(season), MAX(season), COUNT(*) FROM stats.player_season_stats
    UNION ALL
    SELECT 'sp_ratings', MIN(year), MAX(year), COUNT(*) FROM ratings.sp_ratings
    UNION ALL
    SELECT 'recruits', MIN(year), MAX(year), COUNT(*) FROM recruiting.recruits
    UNION ALL
    SELECT 'rosters', MIN(year), MAX(year), COUNT(*) FROM core.roster
),

-- Orphan check: plays without matching games
orphan_plays AS (
    SELECT
        COUNT(*) FILTER (WHERE NOT EXISTS (
            SELECT 1 FROM core.games g WHERE g.id = p.game_id
        )) as orphan_count,
        COUNT(*) as total_count
    FROM core.plays p
),

-- Orphan check: drives without matching games
orphan_drives AS (
    SELECT
        COUNT(*) FILTER (WHERE NOT EXISTS (
            SELECT 1 FROM core.games g WHERE g.id = d.game_id
        )) as orphan_count,
        COUNT(*) as total_count
    FROM core.drives d
),

-- Table freshness: when was data last updated?
freshness AS (
    SELECT
        schemaname || '.' || relname as table_name,
        n_live_tup as row_count,
        last_analyze,
        last_autoanalyze
    FROM pg_stat_user_tables
    WHERE schemaname IN ('core', 'stats', 'ratings', 'recruiting', 'betting', 'draft', 'metrics', 'ref')
    ORDER BY n_live_tup DESC
    LIMIT 20
)

-- Output: Coverage metrics
SELECT
    'coverage'::text as check_type,
    metric,
    min_year::text as value_1,
    max_year::text as value_2,
    row_count::text as value_3,
    NULL::text as notes
FROM coverage

UNION ALL

-- Output: Orphan checks
SELECT
    'integrity'::text as check_type,
    'plays_without_games' as metric,
    orphan_count::text as value_1,
    total_count::text as value_2,
    CASE
        WHEN total_count > 0
        THEN ROUND(100.0 * orphan_count / total_count, 2)::text || '%'
        ELSE '0%'
    END as value_3,
    CASE
        WHEN orphan_count > 0 THEN 'WARNING: Orphan plays detected'
        ELSE 'OK'
    END as notes
FROM orphan_plays

UNION ALL

SELECT
    'integrity'::text as check_type,
    'drives_without_games' as metric,
    orphan_count::text as value_1,
    total_count::text as value_2,
    CASE
        WHEN total_count > 0
        THEN ROUND(100.0 * orphan_count / total_count, 2)::text || '%'
        ELSE '0%'
    END as value_3,
    CASE
        WHEN orphan_count > 0 THEN 'WARNING: Orphan drives detected'
        ELSE 'OK'
    END as notes
FROM orphan_drives

UNION ALL

-- Output: Table sizes (top tables)
SELECT
    'table_size'::text as check_type,
    table_name as metric,
    row_count::text as value_1,
    COALESCE(last_analyze::text, 'never') as value_2,
    COALESCE(last_autoanalyze::text, 'never') as value_3,
    NULL::text as notes
FROM freshness;

COMMENT ON VIEW analytics.data_quality_dashboard IS
'Data quality monitoring: coverage by year, orphan record detection, table freshness';
