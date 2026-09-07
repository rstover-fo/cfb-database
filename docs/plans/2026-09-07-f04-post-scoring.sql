-- Authorized refresh plus read-only assertions after both upcoming scorers.
SET LOCAL statement_timeout = '300s';
REFRESH MATERIALIZED VIEW marts.scored_matchup_edges;
REFRESH MATERIALIZED VIEW marts.prediction_accuracy;
DO $verify$
DECLARE n bigint; digest text; r record; role_name text;
BEGIN
    SELECT count(*),md5(COALESCE(string_agg(md5((to_jsonb(p)-ARRAY['evaluation_mode',
      'created_at','published_at','simulated_as_of_at','experiment_label','fit_id',
      'input_hash','input_snapshot'])::text),'' ORDER BY prediction_id),''))
    INTO n,digest FROM predictions.game_predictions p WHERE evaluation_mode='legacy_unknown';
    IF n<>364959 OR digest<>'596053a3eb85ac927166c7a6712bb581' THEN
        RAISE EXCEPTION 'Legacy history was changed'; END IF;
    SELECT count(*),md5(COALESCE(string_agg(md5(to_jsonb(p)::text),''
                       ORDER BY to_jsonb(p)::text),''))
    INTO n,digest FROM predictions.season_projections p;
    IF n<>39860 OR digest<>'6c8f33295dac29003d056500bd8e5154' THEN
        RAISE EXCEPTION 'Stored outlook changed during cold start'; END IF;
    RAISE NOTICE 'F04 preserved 364959 legacy rows and 39860 outlook rows after scoring';
    IF EXISTS (
        SELECT 1 FROM predictions.game_predictions p
        LEFT JOIN predictions.model_artifacts a ON a.fit_id=p.fit_id AND a.model_version=p.model_version
        WHERE p.evaluation_mode='published_forecast' AND
            (a.fit_id IS NULL OR p.created_at IS NULL OR p.published_at IS NULL
             OR p.published_at<>p.created_at OR p.input_snapshot IS NULL
             OR p.input_hash IS NULL OR p.simulated_as_of_at IS NOT NULL)
    ) THEN RAISE EXCEPTION 'Published lineage incomplete'; END IF;
    FOR r IN SELECT model_version,count(*) AS published_rows,count(DISTINCT fit_id) AS artifacts,
                    min(published_at) AS first_publication,max(published_at) AS last_publication
             FROM predictions.game_predictions WHERE evaluation_mode='published_forecast'
             GROUP BY model_version ORDER BY model_version
    LOOP RAISE NOTICE 'F04 publications: %',row_to_json(r); END LOOP;
    IF (SELECT count(DISTINCT model_version) FROM api.game_predictions WHERE model_version IN
        ('elo_v1','elo_epa_blend_v1','fitted_v1'))<>3 THEN
        RAISE EXCEPTION 'Missing published model'; END IF;
    FOR r IN
        WITH pending AS (SELECT g.id,g.season,g.start_date FROM core.games g WHERE NOT COALESCE(g.completed, false) AND g.id NOT IN (401540999, 401545766, 401545768, 401545773, 401545780, 401545781, 401549719, 401550299, 401552878, 401552884, 401640992, 401833535, 401866625) AND g.season >= (SELECT COALESCE(MAX(season), 0) FROM core.games WHERE completed AND id NOT IN (401540999, 401545766, 401545768, 401545773, 401545780, 401545781, 401549719, 401550299, 401552878, 401552884, 401640992, 401833535, 401866625)))
        SELECT p.season,count(*) AS pending,
            count(*) FILTER(WHERE EXISTS(SELECT 1 FROM predictions.game_predictions gp
                WHERE gp.game_id=p.id AND gp.model_version='fitted_v1'
                  AND gp.evaluation_mode='published_forecast'
                  AND (p.start_date IS NULL OR gp.published_at<p.start_date))) AS covered
        FROM pending p GROUP BY p.season ORDER BY p.season
    LOOP
        RAISE NOTICE 'F04 eligible fitted coverage: %',row_to_json(r);
        IF r.covered::numeric/r.pending < 0.9 THEN
            RAISE EXCEPTION 'Insufficient eligible fitted coverage: %',row_to_json(r); END IF;
    END LOOP;
    FOR r IN 
    WITH latest AS (
        SELECT DISTINCT ON (p.game_id)
               p.game_id,
               p.expected_home_margin,
               g.home_points - g.away_points AS actual_margin
        FROM predictions.game_predictions p
        JOIN core.games g ON g.id = p.game_id
        WHERE p.model_version = 'fitted_v1'
          AND g.id NOT IN (401540999, 401545766, 401545768, 401545773, 401545780, 401545781, 401549719, 401550299, 401552878, 401552884, 401640992, 401833535, 401866625)
          AND COALESCE(g.completed, false)
          AND g.home_points IS NOT NULL AND g.away_points IS NOT NULL
          AND p.expected_home_margin IS NOT NULL
          AND p.evaluation_mode = 'published_forecast'
          AND p.published_at IS NOT NULL
          AND g.start_date IS NOT NULL
          AND p.published_at < g.start_date
        ORDER BY p.game_id, p.published_at DESC, p.prediction_id DESC
    )
    SELECT stddev_pop(actual_margin::double precision - expected_home_margin),
           COUNT(*)
    FROM latest
 LOOP RAISE NOTICE 'F04 prospective calibration: %',row_to_json(r); END LOOP;
    SELECT count(*) INTO n FROM api.prediction_accuracy;
    RAISE NOTICE 'F04 published accuracy rows: %',n;
    FOREACH role_name IN ARRAY ARRAY['anon','authenticated','analyst_ro'] LOOP
        EXECUTE format('SET LOCAL ROLE %I',role_name);
        SELECT count(*) INTO n FROM api.game_predictions;
        RAISE NOTICE 'F04 caller % reads % published forecasts',role_name,n;
        SELECT count(*) INTO n FROM api.prediction_history;
        RAISE NOTICE 'F04 caller % reads % history rows',role_name,n;
        RESET ROLE;
    END LOOP;
    RAISE NOTICE 'F04 post-scoring verification complete';
END
$verify$;
