-- get_data_freshness: return current data freshness status for all tracked tables
--
-- Usage:
--   SELECT * FROM get_data_freshness();
--   SELECT * FROM get_data_freshness() WHERE is_stale = true;

CREATE OR REPLACE FUNCTION get_data_freshness()
RETURNS TABLE(
    schema_name text,
    table_name text,
    row_count bigint,
    expected_refresh_frequency text,
    days_since_activity numeric,
    is_stale boolean
)
LANGUAGE sql
STABLE
AS $$
    SELECT
        f.schema_name,
        f.table_name,
        f.row_count,
        f.expected_refresh_frequency,
        f.days_since_activity,
        f.is_stale
    FROM marts.data_freshness f
    ORDER BY f.is_stale DESC, f.schema_name, f.table_name;
$$;

COMMENT ON FUNCTION get_data_freshness IS
'Returns data freshness status for all tracked tables. Use WHERE is_stale = true to find tables needing refresh.';
