-- Refresh all materialized views in dependency order.
-- Calls marts.refresh_all() which handles layered refresh with error handling.
--
-- Usage:
--   psql $SUPABASE_DB_URL -f scripts/refresh_marts.sql

SELECT * FROM marts.refresh_all();
