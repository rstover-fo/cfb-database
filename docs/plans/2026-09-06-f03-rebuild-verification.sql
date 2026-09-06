-- Read-only postflight for the approved F03 recovery; execute after the runner.
SET TRANSACTION READ ONLY;
SET LOCAL statement_timeout = '90s';
DO $verify$
DECLARE n bigint; baseline timestamptz; old_fit jsonb; current_fit jsonb; result json;
        caller text;
BEGIN
 FOREACH caller IN ARRAY ARRAY['anon','authenticated','analyst_ro'] LOOP
   IF has_schema_privilege(caller,'recovery','USAGE')
      OR has_table_privilege(caller,'recovery.f03_row_archive','SELECT') THEN
     RAISE EXCEPTION 'Recovery archive exposed to %',caller;
   END IF;
 END LOOP;
 SELECT min(archived_at) INTO baseline FROM recovery.f03_row_archive;
 IF baseline IS NULL THEN RAISE EXCEPTION 'Missing pre-recovery journal'; END IF;
 -- Require outputs from the corrected replay, not the earlier intermediate run.
 -- https://github.com/rstover-fo/cfb-database/actions/runs/34064735040
 baseline := GREATEST(baseline,'2026-09-06 22:41:07+00'::timestamptz);
 SELECT jsonb_agg(v ORDER BY v::text) INTO old_fit
 FROM recovery.f03_row_archive a CROSS JOIN LATERAL jsonb_array_elements(a.payload) v
 WHERE a.source_table='features.model_metadata';
 SELECT jsonb_agg(to_jsonb(m) ORDER BY to_jsonb(m)::text) INTO current_fit
 FROM features.model_metadata m;
 IF old_fit IS DISTINCT FROM current_fit THEN RAISE EXCEPTION 'Model metadata changed'; END IF;
 SELECT jsonb_agg(v ORDER BY v::text) INTO old_fit
 FROM recovery.f03_row_archive a CROSS JOIN LATERAL jsonb_array_elements(a.payload) v
 WHERE a.source_table='features.model_coefficients';
 SELECT jsonb_agg(to_jsonb(c) ORDER BY to_jsonb(c)::text) INTO current_fit
 FROM features.model_coefficients c;
 IF old_fit IS DISTINCT FROM current_fit THEN RAISE EXCEPTION 'Model coefficients changed'; END IF;

 SELECT count(*) INTO n FROM (
   SELECT game_id FROM predictions.game_predictions WHERE game_id=401866625
   UNION ALL SELECT game_id FROM api.game_predictions WHERE game_id=401866625
   UNION ALL SELECT game_id FROM marts.scored_matchup_edges WHERE game_id=401866625
   UNION ALL SELECT game_id FROM features.team_week WHERE game_id=401866625
   UNION ALL SELECT game_id FROM analytics.house_elo_game WHERE game_id=401866625
 ) invalid;
 IF n<>0 THEN RAISE EXCEPTION 'Superseded derived rows remain: %',n; END IF;
 -- prediction_accuracy is aggregated by model/season/threshold, not game ID.
 -- Verify its unconditional population against current game-grain API inputs.
 SELECT count(*) INTO n FROM (
   SELECT p.model_version,p.season,count(*) AS n_games
   FROM api.game_predictions p JOIN core.games g ON g.id=p.game_id
   WHERE p.season=2026 AND g.completed AND g.home_points IS NOT NULL
     AND g.away_points IS NOT NULL GROUP BY p.model_version,p.season
 ) expected FULL JOIN (
   SELECT model_version,season,n_games FROM marts.prediction_accuracy
   WHERE season=2026 AND edge_threshold=0
 ) actual USING(model_version,season)
 WHERE expected.n_games IS DISTINCT FROM actual.n_games;
 IF n<>0 THEN RAISE EXCEPTION 'Accuracy mart population differs from current inputs'; END IF;
 SELECT count(*) INTO n FROM core.games WHERE id IN (401866625,401917058);
 IF n<>2 THEN RAISE EXCEPTION 'Original/replacement raw records not retained'; END IF;
 SELECT count(*) INTO n FROM analytics.house_elo_game
 WHERE (game_id=401677463 AND season=2024 AND actual_home_margin=51)
    OR (game_id=401773541 AND season=2025 AND actual_home_margin=-62);
 IF n<>2 THEN RAISE EXCEPTION 'Repaired historical outcomes missing from Elo replay'; END IF;

 SELECT count(*) INTO n FROM core.games g
 WHERE g.season=2026 AND NOT COALESCE(g.completed,false) AND g.id<>401866625
 AND NOT EXISTS (SELECT 1 FROM api.game_predictions p
   WHERE p.game_id=g.id AND p.model_version='fitted_v1' AND p.computed_at>=baseline
     AND p.home_win_prob BETWEEN 0 AND 1
     AND p.expected_home_margin::text NOT IN ('NaN','Infinity','-Infinity'));
 IF n<>0 THEN RAISE EXCEPTION 'Pending games without fresh valid fitted scores: %',n; END IF;

 SELECT count(*) INTO n FROM api.season_outlook
 WHERE season=2026 AND model_version='fitted_v1'
   AND team IN ('Campbell','Western Carolina') AND computed_at>=baseline
   AND games_scheduled=12 AND games_simulated=12 AND games_unscored=0
   AND projected_wins BETWEEN 0 AND 12 AND projected_losses BETWEEN 0 AND 12;
 IF n<>2 THEN RAISE EXCEPTION 'Expected two fresh complete 12-game outlooks; found %',n; END IF;
 SELECT json_agg(row_to_json(r)) INTO result FROM (
   SELECT team,computed_at,games_scheduled,games_simulated,games_unscored,
          projected_wins,projected_losses FROM api.season_outlook
   WHERE season=2026 AND model_version='fitted_v1'
     AND team IN ('Campbell','Western Carolina')
 ) r;
 RAISE NOTICE 'F03 rebuilt outlooks: %',result;
 SELECT row_to_json(r) INTO result FROM (
   SELECT count(*) AS teams,
          count(*) FILTER (WHERE computed_at>=baseline) AS fresh_teams,
          count(*) FILTER (WHERE games_unscored=0) AS complete_schedules,
          sum(games_unscored) AS unscored_team_games
   FROM api.season_outlook WHERE season=2026 AND model_version='fitted_v1'
 ) r;
 RAISE NOTICE 'F03 all-team outlook coverage: %',result;
 RAISE NOTICE 'F03 fit equality, source retention, exclusion and scoring checks passed';
END $verify$;
SET LOCAL ROLE anon;
SELECT count(*) FROM api.season_outlook WHERE season=2026 AND model_version='fitted_v1';
SELECT count(*) FROM api.game_predictions WHERE game_id=401866625;
SELECT has_schema_privilege(current_user,'recovery','USAGE') AS archive_access;
RESET ROLE;
SET LOCAL ROLE authenticated;
SELECT count(*) FROM api.season_outlook WHERE season=2026 AND model_version='fitted_v1';
SELECT has_schema_privilege(current_user,'recovery','USAGE') AS archive_access;
RESET ROLE;
SET LOCAL ROLE analyst_ro;
SELECT count(*) FROM api.season_outlook WHERE season=2026 AND model_version='fitted_v1';
SELECT has_schema_privilege(current_user,'recovery','USAGE') AS archive_access;
RESET ROLE;
