-- Read-only, after authorized upcoming fitted scoring and mart refresh.
DO $verify$
DECLARE r RECORD; n BIGINT; seasons INTEGER := 0;
BEGIN
    SELECT count(*) INTO n FROM predictions.game_predictions WHERE prediction_id>545692;
    IF n=0 THEN RAISE EXCEPTION 'No new F05 predictions'; END IF;
    IF EXISTS (
        SELECT 1 FROM predictions.game_predictions p
        LEFT JOIN predictions.model_artifacts a ON a.fit_id=p.fit_id AND a.model_version=p.model_version
        LEFT JOIN features.training_fits f ON f.training_fit_id=a.artifact->>'training_fit_id'
        LEFT JOIN features.model_deployments d ON d.training_fit_id=f.training_fit_id
             AND d.model_version=p.model_version AND d.train_through_season=f.train_through_season
        LEFT JOIN core.games g ON g.id=p.game_id
        WHERE p.prediction_id>545692 AND (
            p.model_version IS DISTINCT FROM 'fitted_v1'
            OR p.evaluation_mode IS DISTINCT FROM 'published_forecast'
            OR p.created_at IS NULL OR p.published_at IS DISTINCT FROM p.created_at
            OR p.input_hash IS NULL OR p.input_snapshot IS NULL OR p.expected_home_margin IS NULL
            OR p.home_win_prob IS NULL OR p.home_win_prob NOT BETWEEN 0 AND 1
            OR f.training_fit_id IS NULL OR d.training_fit_id IS NULL
            OR f.manifest->>'lineage' IS DISTINCT FROM 'known'
            OR g.id IS NULL OR f.train_through_season>=g.season
            OR (a.artifact->>'train_through_season')::bigint IS DISTINCT FROM f.train_through_season
            OR f.train_through_season IS DISTINCT FROM (
                SELECT max(train_through_season) FROM features.model_deployments
                WHERE model_version='fitted_v1' AND train_through_season<g.season)))
        THEN RAISE EXCEPTION 'New F05 prediction lineage/selection invalid'; END IF;
    RAISE NOTICE 'F05 new immutable published predictions: %',n;
    FOR r IN
        WITH pending AS (SELECT g.id,g.season,g.start_date FROM core.games g WHERE NOT COALESCE(g.completed, false) AND g.id NOT IN (401540999, 401545766, 401545768, 401545773, 401545780, 401545781, 401549719, 401550299, 401552878, 401552884, 401640992, 401833535, 401866625) AND g.season >= (SELECT COALESCE(MAX(season), 0) FROM core.games WHERE completed AND id NOT IN (401540999, 401545766, 401545768, 401545773, 401545780, 401545781, 401549719, 401550299, 401552878, 401552884, 401640992, 401833535, 401866625)))
        SELECT p.season,count(*) AS pending,
            count(*) FILTER(WHERE EXISTS(SELECT 1 FROM predictions.game_predictions gp
                WHERE gp.game_id=p.id AND gp.prediction_id>545692 AND gp.model_version='fitted_v1')) AS scored,
            count(*) FILTER(WHERE EXISTS(SELECT 1 FROM predictions.game_predictions gp
                WHERE gp.game_id=p.id AND gp.prediction_id>545692 AND gp.model_version='fitted_v1'
                  AND gp.evaluation_mode='published_forecast' AND gp.expected_home_margin IS NOT NULL
                  AND p.start_date IS NOT NULL AND gp.published_at<p.start_date)) AS eligible
        FROM pending p GROUP BY p.season ORDER BY p.season
    LOOP
        seasons := seasons+1;
        RAISE NOTICE 'F05 pending coverage: %',row_to_json(r);
        IF r.scored::numeric/r.pending<0.9 OR r.eligible::numeric/r.pending<0.9
            THEN RAISE EXCEPTION 'F05 per-season coverage below 90%%: %',row_to_json(r); END IF;
    END LOOP;
    IF seasons=0 THEN RAISE EXCEPTION 'No pending season verified'; END IF;
    FOR r IN SELECT a.fit_id,a.artifact->>'training_fit_id' AS training_fit_id,count(*) AS rows
        FROM predictions.game_predictions p JOIN predictions.model_artifacts a USING(fit_id)
        WHERE p.prediction_id>545692 GROUP BY a.fit_id,a.artifact->>'training_fit_id'
    LOOP RAISE NOTICE 'F05 scoring artifact: %',row_to_json(r); END LOOP;
    SELECT count(*) INTO n FROM api.prediction_accuracy;
    RAISE NOTICE 'F05 published accuracy rows: %',n;
    RAISE NOTICE 'F05 post-scoring verification complete';
END
$verify$;
