-- Baseline: approved diagnostic run 34127237645. Append-only new rows are allowed.
SET LOCAL statement_timeout = '300s';
DO $verify$
DECLARE n BIGINT; h TEXT;
BEGIN
    SELECT count(*),md5(string_agg(md5(to_jsonb(m)::text),'' ORDER BY model_version,train_through_season))
    INTO n,h FROM features.model_metadata m;
    IF n<>9 OR h<>'08e75096672a7fc358bff8a52d69135a' THEN RAISE EXCEPTION 'Legacy metadata changed'; END IF;
    SELECT count(*),md5(string_agg(md5(to_jsonb(c)::text),'' ORDER BY model_version,train_through_season,model_component,feature_order))
    INTO n,h FROM features.model_coefficients c;
    IF n<>414 OR h<>'a643a1a58f8470d5bc8efa2ef71b9f65' THEN RAISE EXCEPTION 'Legacy coefficients changed'; END IF;
    SELECT count(*),md5(string_agg(md5(to_jsonb(p)::text),'' ORDER BY prediction_id))
    INTO n,h FROM predictions.game_predictions p WHERE prediction_id<=545692;
    IF n<>374700 OR h<>'10d6118d58b5d125146de10f2fae0ab1' THEN RAISE EXCEPTION 'Original predictions changed'; END IF;
    SELECT count(*),md5(string_agg(md5(to_jsonb(a)::text),'' ORDER BY fit_id))
    INTO n,h FROM predictions.model_artifacts a WHERE fit_id IN (
        '28403a33849812e68ff0a052902428535b2de1a1d61eaf552e3ce8ba85e978d9',
        '28e7ef31a583c4f2fd0a24fec3eb78da88724c464ed1764a800ec971077c4919',
        'c4ebd955f68e9fa835728e122ff2ba4cee0fe2b33195d93f0d1ecc891b587433');
    IF n<>3 OR h<>'aa3791dfde3ba8b574884e8d9db0f79c' THEN RAISE EXCEPTION 'Original artifacts changed'; END IF;
    RAISE NOTICE 'F05 preserved all 9 legacy fits, 414 coefficients, 374700 original predictions and 3 original artifacts';
END
$verify$;
