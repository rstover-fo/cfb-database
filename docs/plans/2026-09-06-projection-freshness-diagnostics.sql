-- Read-only cloud diagnostics. RAISE NOTICE exposes results in Deploy Schema logs.
SET TRANSACTION READ ONLY;
SET LOCAL statement_timeout = '60s';
DO $probe$
DECLARE result json;
BEGIN
  SELECT json_agg(row_to_json(r)) INTO result FROM (SELECT team, season, model_version, computed_at, games_scheduled,
       games_simulated, games_unscored
FROM api.season_outlook
WHERE season = 2026 AND team IN ('Campbell', 'Western Carolina')) r;
  RAISE NOTICE 'recovery_probe_1: %', result;
  SELECT json_agg(row_to_json(r)) INTO result FROM (SELECT id, week, season_type, start_date, home_team, away_team, completed
FROM core.games
WHERE season = 2026
  AND (home_team IN ('Campbell', 'Western Carolina')
       OR away_team IN ('Campbell', 'Western Carolina'))
ORDER BY start_date, id) r;
  RAISE NOTICE 'recovery_probe_2: %', result;
  SELECT json_agg(row_to_json(r)) INTO result FROM (SELECT player_id, player, team, position, season, COUNT(*) AS stat_rows
FROM stats.player_season_stats
WHERE player_id::text = '5302440' AND season = 2026 AND team = 'Alabama State'
GROUP BY player_id, player, team, position, season) r;
  RAISE NOTICE 'recovery_probe_3: %', result;
  SELECT json_agg(row_to_json(r)) INTO result FROM (SELECT id, team, year, height, weight, jersey
FROM core.roster
WHERE id::text = '5302440' AND year = 2026 AND team = 'Alabama State') r;
  RAISE NOTICE 'recovery_probe_4: %', result;
  SELECT json_agg(row_to_json(r)) INTO result FROM (SELECT *
FROM metrics.ppa_players_season
WHERE id::text = '5302440' AND season = 2026) r;
  RAISE NOTICE 'recovery_probe_5: %', result;
  SELECT json_agg(row_to_json(r)) INTO result FROM (WITH RECURSIVE deps AS (
 SELECT 'marts.player_comparison'::regclass::oid AS oid
 UNION
 SELECT r.ev_class FROM deps p JOIN pg_depend d ON d.refobjid=p.oid
 JOIN pg_rewrite r ON r.oid=d.objid WHERE r.ev_class<>p.oid
) SELECT n.nspname AS schema, c.relname, c.relkind, pg_get_userbyid(c.relowner) AS owner,
 c.relacl::text AS acl, c.reloptions,
 CASE WHEN c.relkind='v' THEN pg_get_viewdef(c.oid,true) END AS definition
FROM deps JOIN pg_class c USING(oid) JOIN pg_namespace n ON n.oid=c.relnamespace) r;
  RAISE NOTICE 'recovery_probe_6: %', result;
  SELECT json_agg(row_to_json(r)) INTO result FROM (SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema='core' AND table_name='games' AND column_name LIKE '%dlt%') r;
  RAISE NOTICE 'recovery_probe_7: %', result;
END
$probe$;
