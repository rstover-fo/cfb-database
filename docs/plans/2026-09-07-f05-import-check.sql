-- Assert exact import, without treating unknown legacy lineage as current.
DO $verify$
DECLARE m RECORD; p JSONB; n BIGINT; role_name TEXT; table_name TEXT;
BEGIN
    IF EXISTS(SELECT 1 FROM features.model_deployments) OR EXISTS(SELECT 1 FROM features.model_deployment_history)
        THEN RAISE EXCEPTION 'Legacy import unexpectedly promoted fits'; END IF;
    IF EXISTS (SELECT 1 FROM features.model_coefficients c LEFT JOIN features.model_metadata legacy_meta
        USING(model_version,train_through_season) WHERE legacy_meta.model_version IS NULL)
    THEN RAISE EXCEPTION 'Orphan legacy coefficients require investigation'; END IF;
    SELECT count(*) INTO n FROM features.training_fits WHERE manifest->>'lineage'='legacy_unknown';
    IF n<>9 THEN RAISE EXCEPTION 'Expected 9 imported fits, found %',n; END IF;
    FOR m IN SELECT * FROM features.model_metadata LOOP
        SELECT count(*) INTO n FROM features.training_fits f WHERE f.model_version=m.model_version
            AND f.train_through_season=m.train_through_season AND manifest->>'lineage'='legacy_unknown';
        IF n<>1 THEN RAISE EXCEPTION 'Legacy fit import missing/duplicated at %',m.train_through_season; END IF;
        SELECT parameters INTO p FROM features.training_fits f WHERE f.model_version=m.model_version
            AND f.train_through_season=m.train_through_season AND manifest->>'lineage'='legacy_unknown';
        IF (p->>'platt_a')::numeric IS DISTINCT FROM m.platt_a
          OR (p->>'platt_b')::numeric IS DISTINCT FROM m.platt_b
          OR p->'feature_means' IS DISTINCT FROM m.feature_means
          OR p->'diff_means' IS DISTINCT FROM m.feature_diff_means
          OR p->'diff_stds' IS DISTINCT FROM m.feature_diff_stds
          OR p->'feature_names' IS DISTINCT FROM (SELECT jsonb_agg(feature_name ORDER BY feature_order)
            FROM features.model_coefficients WHERE model_version=m.model_version
              AND train_through_season=m.train_through_season AND model_component='margin')
          OR p->'beta_margin' IS DISTINCT FROM (SELECT jsonb_agg(coefficient::text ORDER BY feature_order)
            FROM features.model_coefficients WHERE model_version=m.model_version
              AND train_through_season=m.train_through_season AND model_component='margin')
          OR p->'beta_winprob' IS DISTINCT FROM (SELECT jsonb_agg(coefficient::text ORDER BY feature_order)
            FROM features.model_coefficients WHERE model_version=m.model_version
              AND train_through_season=m.train_through_season AND model_component='winprob')
        THEN RAISE EXCEPTION 'Legacy fit differs at %',m.train_through_season; END IF;
    END LOOP;
    FOREACH role_name IN ARRAY ARRAY['anon','authenticated'] LOOP
        EXECUTE format('SET LOCAL ROLE %I',role_name);
        PERFORM 1 FROM features.training_fits LIMIT 1;
        PERFORM 1 FROM features.model_deployments LIMIT 1;
        PERFORM 1 FROM features.model_deployment_history LIMIT 1;
        IF has_table_privilege(current_user,'features.training_fits','INSERT,UPDATE,DELETE,TRUNCATE')
          OR has_table_privilege(current_user,'features.model_deployments','INSERT,UPDATE,DELETE,TRUNCATE')
          OR has_table_privilege(current_user,'features.model_deployment_history','INSERT,UPDATE,DELETE,TRUNCATE')
        THEN RAISE EXCEPTION 'Unexpected consumer write access'; END IF;
        RESET ROLE;
        RAISE NOTICE 'F05 actual role % reads registry; consumer DML revoked',role_name;
    END LOOP;
    SET LOCAL ROLE analyst_ro;
    PERFORM 1 FROM api.game_predictions LIMIT 1;
    FOREACH table_name IN ARRAY ARRAY['training_fits','model_deployments','model_deployment_history'] LOOP
        BEGIN
            EXECUTE format('SELECT 1 FROM features.%I LIMIT 1',table_name);
            RAISE EXCEPTION 'analyst_ro unexpectedly reads %',table_name;
        EXCEPTION WHEN insufficient_privilege THEN NULL;
        END;
    END LOOP;
    RESET ROLE;
    RAISE NOTICE 'F05 all 9 imports exactly match legacy parameters; actual analyst_ro API access and raw denial verified';
END
$verify$;
