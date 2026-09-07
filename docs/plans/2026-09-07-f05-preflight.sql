-- Approved F05 rollout: aggregate preservation baseline, no row payloads logged.
SET LOCAL lock_timeout = '10s';
LOCK TABLE features.model_metadata, features.model_coefficients,
    predictions.game_predictions, predictions.model_artifacts IN SHARE MODE;
DO $preflight$
DECLARE v JSONB;
BEGIN
    SELECT jsonb_build_object(
        'metadata_count', (SELECT count(*) FROM features.model_metadata),
        'metadata_hash', (SELECT md5(string_agg(md5(to_jsonb(m)::text), ''
            ORDER BY model_version, train_through_season)) FROM features.model_metadata m),
        'coefficients_count', (SELECT count(*) FROM features.model_coefficients),
        'coefficients_hash', (SELECT md5(string_agg(md5(to_jsonb(c)::text), ''
            ORDER BY model_version, train_through_season, model_component, feature_order))
            FROM features.model_coefficients c),
        'prediction_count', (SELECT count(*) FROM predictions.game_predictions),
        'prediction_max_id', (SELECT max(prediction_id) FROM predictions.game_predictions),
        'prediction_hash', (SELECT md5(string_agg(md5(to_jsonb(p)::text), ''
            ORDER BY prediction_id)) FROM predictions.game_predictions p),
        'artifact_count', (SELECT count(*) FROM predictions.model_artifacts),
        'artifact_ids', (SELECT jsonb_agg(fit_id ORDER BY fit_id) FROM predictions.model_artifacts),
        'artifact_hash', (SELECT md5(string_agg(md5(to_jsonb(a)::text), '' ORDER BY fit_id))
            FROM predictions.model_artifacts a),
        'registry_exists', to_regclass('features.training_fits') IS NOT NULL,
        'legacy_vintages', (SELECT jsonb_agg(train_through_season ORDER BY train_through_season)
            FROM features.model_metadata WHERE model_version='fitted_v1')
    ) INTO v;
    RAISE NOTICE 'F05_BASELINE %', v;
END
$preflight$;
