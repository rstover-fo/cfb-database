-- Read-only, bounded lifecycle diagnostics for the approved F03 production rebuild.
SET TRANSACTION READ ONLY;
SET LOCAL statement_timeout = '60s';
DO $diagnostic$
DECLARE result json;
BEGIN
 SELECT json_agg(row_to_json(r)) INTO result FROM (
   SELECT season,season_type,count(*) AS games,
          count(*) FILTER (WHERE NOT COALESCE(completed,false)) AS incomplete,
          count(*) FILTER (WHERE start_date IS NULL) AS missing_date,
          count(*) FILTER (WHERE home_points IS NULL OR away_points IS NULL) AS missing_score
   FROM core.games WHERE season BETWEEN 2020 AND 2025 GROUP BY season,season_type
   ORDER BY season,season_type
 ) r;
 RAISE NOTICE 'Lifecycle shape: %',result;
 SELECT json_agg(row_to_json(r)) INTO result FROM (
   SELECT id,season,season_type,week,start_date,completed,home_id,home_team,
          away_id,away_team,home_points,away_points
   FROM core.games
   WHERE id IN (401773541,401833535,401640992,401677463,401540999,401545766,
                401545768,401545773,401545780,401545781,401549719,401550299,
                401552878,401552884,401279029,401279033)
   ORDER BY season,id
 ) r;
 RAISE NOTICE 'Unresolved examples: %',result;
 SELECT json_agg(row_to_json(r)) INTO result FROM (
   SELECT model_version,count(*) AS invalid_predictions
   FROM predictions.game_predictions WHERE game_id=401866625 GROUP BY model_version
 ) r;
 RAISE NOTICE 'Superseded prediction rows: %',result;
 SELECT json_agg(row_to_json(r)) INTO result FROM (
   SELECT id,season,season_type,week,start_date,completed,home_id,home_team,
          away_id,away_team,home_points,away_points
   FROM core.games
   WHERE (season=2023 AND (home_id,away_id) IN
          ((340,2977),(121,2731),(33,2394),(402,2967)))
      OR (season=2024 AND (home_id,away_id)=(620,190))
      OR (season=2025 AND (home_id,away_id)=(2834,2025))
   ORDER BY season,home_id,start_date,id
 ) r;
 RAISE NOTICE 'Replacement candidates: %',result;
END $diagnostic$;
