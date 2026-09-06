-- Read-only post-migration and post-compute checks through Deploy Schema.
SET TRANSACTION READ ONLY;
SET LOCAL statement_timeout = '60s';
DO $verify$
DECLARE n bigint; result json;
BEGIN
 SELECT count(*) INTO n FROM (
   SELECT player_id,team,season FROM marts.player_comparison
   GROUP BY player_id,team,season HAVING count(*)>1
 ) duplicates;
 IF n<>0 THEN RAISE EXCEPTION 'Duplicate player keys remain: %',n; END IF;
 SELECT count(*) INTO n FROM marts.player_comparison
 WHERE player_id::text='5302440' AND team='Alabama State' AND season=2026;
 IF n<>1 THEN RAISE EXCEPTION 'Affected player expected once, found %',n; END IF;
 RAISE NOTICE 'Player comparison uniqueness and affected key passed';
 SELECT json_agg(row_to_json(r)) INTO result FROM (
   SELECT team,season,model_version,computed_at,games_scheduled,
          games_simulated,games_unscored,projected_wins,projected_losses
   FROM api.season_outlook
   WHERE season=2026 AND team IN ('Campbell','Western Carolina')
 ) r;
 RAISE NOTICE 'Recovered outlooks: %',result;
 SELECT count(*) INTO n FROM api.season_outlook
 WHERE season=2026 AND team IN ('Campbell','Western Carolina')
   AND model_version='fitted_v1' AND computed_at::date=CURRENT_DATE
   AND games_scheduled=12 AND games_simulated<=games_scheduled
   AND games_unscored=games_scheduled-games_simulated
   AND projected_wins BETWEEN 0 AND games_simulated
   AND projected_losses BETWEEN 0 AND games_simulated;
 IF n<>2 THEN RAISE EXCEPTION 'Expected two fresh, valid 12-game outlooks; found %',n; END IF;
END $verify$;
SET LOCAL ROLE anon;
SELECT count(*) FROM api.player_comparison WHERE player_id::text='5302440';
RESET ROLE;
SET LOCAL ROLE authenticated;
SELECT count(*) FROM api.player_comparison WHERE player_id::text='5302440';
RESET ROLE;
SET LOCAL ROLE analyst_ro;
SELECT count(*) FROM api.player_comparison WHERE player_id::text='5302440';
RESET ROLE;
