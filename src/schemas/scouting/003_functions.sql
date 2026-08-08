-- scouting schema: functions (codified from live, 2026-08-08)
-- =============================================================================
-- Consolidated home for scouting.* functions after the cub-scout merge.
-- Supersedes src/schemas/functions/refresh_player_mart.sql (removed in the same
-- commit that added this file); definition matches pg_get_functiondef in
-- production exactly.
--
-- Applied via: python scripts/run_migrations.py --file src/schemas/scouting/003_functions.sql

-- Refreshes scouting.player_mart. SECURITY DEFINER so callers need no direct
-- privileges on the matview; search_path pinned per this repo's function rules.
-- While the scout service is parked nothing schedules this -- see the revival
-- note in 002_player_mart.sql.
CREATE OR REPLACE FUNCTION scouting.refresh_player_mart()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO ''
AS $function$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY scouting.player_mart;
END;
$function$;
