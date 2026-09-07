-- Run only after the 2017..2025 closed-window refit and a fresh no-op check.
DO $verify$
DECLARE r RECORD; actual BIGINT[];
BEGIN
    SELECT array_agg(train_through_season ORDER BY train_through_season) INTO actual
        FROM features.model_deployments WHERE model_version='fitted_v1';
    IF actual IS DISTINCT FROM ARRAY[2017,2018,2019,2020,2021,2022,2023,2024,2025]::bigint[]
        THEN RAISE EXCEPTION 'Incomplete selected fit ladder: %',actual; END IF;
    IF EXISTS (
        SELECT 1 FROM features.model_deployments d JOIN features.training_fits f USING(training_fit_id)
        WHERE d.model_version='fitted_v1' AND (
            f.manifest->>'lineage' IS DISTINCT FROM 'known'
            OR (f.manifest->'training_data'->>'row_count')::bigint IS NULL
            OR (f.manifest->'training_data'->>'row_count')::bigint<=0
            OR f.manifest->'training_data'->>'digest' IS NULL
            OR (f.manifest->'training_window'->>'train_through_season')::bigint IS DISTINCT FROM d.train_through_season
            OR f.manifest->'calibration'->>'population' IS DISTINCT FROM 'training_logits'))
        THEN RAISE EXCEPTION 'Selected manifest lineage/window/calibration invalid'; END IF;
    FOR r IN SELECT d.train_through_season,d.training_fit_id,
        f.manifest->'training_data'->>'row_count' AS training_rows,
        f.manifest->'runtime' AS runtime, f.manifest->>'code_revision' AS revision,
        (SELECT max(abs(c.coefficient - (CASE c.model_component WHEN 'margin' THEN
            f.parameters->'beta_margin' ELSE f.parameters->'beta_winprob' END ->>c.feature_order::int)::numeric))
         FROM features.model_coefficients c WHERE c.model_version=d.model_version
            AND c.train_through_season=d.train_through_season) AS max_legacy_coefficient_delta
        FROM features.model_deployments d JOIN features.training_fits f USING(training_fit_id)
        WHERE d.model_version='fitted_v1' ORDER BY d.train_through_season
    LOOP RAISE NOTICE 'F05 selected fit: %',row_to_json(r); END LOOP;
    RAISE NOTICE 'F05 selected complete 2017..2025 known-lineage ladder';
END
$verify$;
