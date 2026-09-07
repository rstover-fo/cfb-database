-- F04 prediction ledger. Explicit-file migration (not MIGRATION_ORDER).
-- Pause prediction writers for the coordinated schema/code cutover. Then apply
-- marts/037, marts/038, api/030, api/031 and api/032 in that order.
-- Existing computed_at/prediction_date values are NOT publication evidence.

CREATE TABLE IF NOT EXISTS predictions.model_artifacts (
    fit_id TEXT PRIMARY KEY CHECK (fit_id ~ '^[0-9a-f]{64}$'),
    model_version TEXT NOT NULL,
    artifact JSONB NOT NULL CHECK (jsonb_typeof(artifact) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    UNIQUE (fit_id, model_version)
);

ALTER TABLE predictions.game_predictions
    ADD COLUMN IF NOT EXISTS evaluation_mode TEXT NOT NULL DEFAULT 'legacy_unknown',
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS simulated_as_of_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS experiment_label TEXT,
    ADD COLUMN IF NOT EXISTS fit_id TEXT,
    ADD COLUMN IF NOT EXISTS input_hash TEXT,
    ADD COLUMN IF NOT EXISTS input_snapshot JSONB;

-- No default mode for new writes: each producer must state its intent.
ALTER TABLE predictions.game_predictions ALTER COLUMN evaluation_mode DROP DEFAULT;
DROP INDEX IF EXISTS predictions.game_predictions_daily_key;

DO $migration$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'predictions.game_predictions'::regclass
          AND conname = 'game_predictions_provenance_check'
    ) THEN
        ALTER TABLE predictions.game_predictions ADD CONSTRAINT game_predictions_provenance_check
        CHECK (
            (evaluation_mode = 'legacy_unknown'
                AND created_at IS NULL AND published_at IS NULL
                AND simulated_as_of_at IS NULL AND experiment_label IS NULL
                AND fit_id IS NULL AND input_hash IS NULL AND input_snapshot IS NULL)
            OR
            (evaluation_mode IN ('published_forecast', 'walk_forward_reconstruction', 'hindsight_experiment')
                AND created_at IS NOT NULL
                AND fit_id IS NOT NULL AND fit_id ~ '^[0-9a-f]{64}$'
                AND input_hash IS NOT NULL AND input_hash ~ '^[0-9a-f]{64}$'
                AND input_snapshot IS NOT NULL AND jsonb_typeof(input_snapshot) = 'object'
                AND (
                    (evaluation_mode = 'published_forecast'
                        AND published_at IS NOT NULL AND published_at = created_at
                        AND simulated_as_of_at IS NULL AND experiment_label IS NULL)
                    OR
                    (evaluation_mode = 'walk_forward_reconstruction'
                        AND published_at IS NULL AND simulated_as_of_at IS NOT NULL
                        AND experiment_label IS NULL)
                    OR
                    (evaluation_mode = 'hindsight_experiment'
                        AND published_at IS NULL AND simulated_as_of_at IS NOT NULL
                        AND experiment_label IS NOT NULL AND length(btrim(experiment_label)) > 0)
                ))
        );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'predictions.game_predictions'::regclass
          AND conname = 'game_predictions_artifact_fk'
    ) THEN
        ALTER TABLE predictions.game_predictions ADD CONSTRAINT game_predictions_artifact_fk
            FOREIGN KEY (fit_id, model_version)
            REFERENCES predictions.model_artifacts (fit_id, model_version);
    END IF;
END
$migration$;

CREATE OR REPLACE FUNCTION predictions.stamp_prediction_provenance()
RETURNS trigger LANGUAGE plpgsql SET search_path = '' AS $function$
BEGIN
    IF NEW.evaluation_mode IS NULL OR NEW.evaluation_mode = 'legacy_unknown' THEN
        RAISE EXCEPTION 'New predictions require explicit non-legacy provenance';
    END IF;
    -- statement_timestamp, unlike now(), is not the start of a transaction
    -- which may have spent minutes loading/scoring. Never trust a supplied time.
    NEW.created_at := statement_timestamp();
    NEW.published_at := CASE WHEN NEW.evaluation_mode = 'published_forecast'
        THEN NEW.created_at ELSE NULL END;
    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION predictions.reject_snapshot_update()
RETURNS trigger LANGUAGE plpgsql SET search_path = '' AS $function$
BEGIN
    RAISE EXCEPTION 'Prediction and model artifact snapshots are immutable; append a new snapshot';
END
$function$;

DROP TRIGGER IF EXISTS stamp_prediction_provenance ON predictions.game_predictions;
CREATE TRIGGER stamp_prediction_provenance BEFORE INSERT ON predictions.game_predictions
    FOR EACH ROW EXECUTE FUNCTION predictions.stamp_prediction_provenance();
DROP TRIGGER IF EXISTS reject_prediction_update ON predictions.game_predictions;
CREATE TRIGGER reject_prediction_update BEFORE UPDATE ON predictions.game_predictions
    FOR EACH ROW EXECUTE FUNCTION predictions.reject_snapshot_update();
DROP TRIGGER IF EXISTS reject_artifact_update ON predictions.model_artifacts;
CREATE TRIGGER reject_artifact_update BEFORE UPDATE ON predictions.model_artifacts
    FOR EACH ROW EXECUTE FUNCTION predictions.reject_snapshot_update();

CREATE INDEX IF NOT EXISTS game_predictions_published_idx
    ON predictions.game_predictions (game_id, model_version, published_at DESC, prediction_id DESC)
    WHERE evaluation_mode = 'published_forecast';
CREATE INDEX IF NOT EXISTS game_predictions_fit_idx ON predictions.game_predictions (fit_id);

COMMENT ON TABLE predictions.game_predictions IS
    'Immutable prediction ledger keyed by prediction_id. New rows explicitly distinguish published_forecast, walk_forward_reconstruction and hindsight_experiment. Legacy rows retain unknown provenance; prediction_date and computed_at are not publication evidence.';
COMMENT ON COLUMN predictions.game_predictions.created_at IS
    'Actual database INSERT statement time for new ledger entries; NULL for unverifiable legacy rows.';
COMMENT ON COLUMN predictions.game_predictions.published_at IS
    'Database write-time publication, set only for published_forecast. Prospective scoring requires known kickoff and published_at strictly before kickoff.';
COMMENT ON COLUMN predictions.game_predictions.simulated_as_of_at IS
    'Nominal reconstructed kickoff, not evidence that corrected inputs or closing market lines were available then.';
COMMENT ON TABLE predictions.model_artifacts IS
    'Content-addressed immutable snapshot of the artifact actually consumed for scoring, including implementation fingerprint. Does not reconstruct upstream training history.';

GRANT USAGE ON SCHEMA predictions TO anon, authenticated;
GRANT SELECT ON predictions.model_artifacts, predictions.game_predictions TO anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON predictions.model_artifacts, predictions.game_predictions
    FROM anon, authenticated;
REVOKE ALL ON FUNCTION predictions.stamp_prediction_provenance() FROM PUBLIC;
REVOKE ALL ON FUNCTION predictions.reject_snapshot_update() FROM PUBLIC;
