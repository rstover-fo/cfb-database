-- F05 immutable training-fit registry. Explicit-file migration (not
-- MIGRATION_ORDER). Training writes candidates without changing production;
-- features.model_deployments is the explicit per-vintage selection pointer.

CREATE TABLE IF NOT EXISTS features.training_fits (
    training_fit_id TEXT PRIMARY KEY CHECK (training_fit_id ~ '^[0-9a-f]{64}$'),
    model_version TEXT NOT NULL CHECK (length(btrim(model_version)) > 0),
    train_through_season BIGINT NOT NULL,
    manifest JSONB NOT NULL CHECK (jsonb_typeof(manifest) = 'object'),
    parameters JSONB NOT NULL CHECK (
        jsonb_typeof(parameters) = 'object'
        AND parameters ?& ARRAY[
            'feature_names', 'feature_means', 'diff_means', 'diff_stds',
            'beta_margin', 'beta_winprob', 'platt_a', 'platt_b'
        ]
        AND parameters - ARRAY[
            'feature_names', 'feature_means', 'diff_means', 'diff_stds',
            'beta_margin', 'beta_winprob', 'platt_a', 'platt_b'
        ]::TEXT[] = '{}'::JSONB
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    UNIQUE (training_fit_id, model_version),
    UNIQUE (training_fit_id, model_version, train_through_season)
);

CREATE TABLE IF NOT EXISTS features.model_deployments (
    model_version TEXT NOT NULL CHECK (length(btrim(model_version)) > 0),
    train_through_season BIGINT NOT NULL,
    training_fit_id TEXT NOT NULL,
    promotion_reason TEXT NOT NULL CHECK (length(btrim(promotion_reason)) > 0),
    promoted_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (model_version, train_through_season),
    CONSTRAINT model_deployments_fit_fk FOREIGN KEY
        (training_fit_id, model_version, train_through_season)
        REFERENCES features.training_fits
        (training_fit_id, model_version, train_through_season)
);

CREATE TABLE IF NOT EXISTS features.model_deployment_history (
    deployment_history_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    model_version TEXT NOT NULL,
    train_through_season BIGINT NOT NULL,
    previous_training_fit_id TEXT,
    training_fit_id TEXT,
    action TEXT NOT NULL CHECK (action IN ('promote', 'delete')),
    reason TEXT NOT NULL CHECK (length(btrim(reason)) > 0),
    changed_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    changed_by TEXT NOT NULL,
    CONSTRAINT model_deployment_history_shape_check CHECK (
        (action = 'promote' AND training_fit_id IS NOT NULL)
        OR (action = 'delete' AND previous_training_fit_id IS NOT NULL
            AND training_fit_id IS NULL)
    ),
    CONSTRAINT model_deployment_history_previous_fit_fk FOREIGN KEY
        (previous_training_fit_id, model_version, train_through_season)
        REFERENCES features.training_fits
        (training_fit_id, model_version, train_through_season),
    CONSTRAINT model_deployment_history_fit_fk FOREIGN KEY
        (training_fit_id, model_version, train_through_season)
        REFERENCES features.training_fits
        (training_fit_id, model_version, train_through_season)
);

COMMENT ON TABLE features.training_fits IS
    'F05 content-addressed immutable training snapshots. The SHA-256 address covers model version, vintage, explicit training manifest and exact frozen parameters. Source digests identify consumed inputs; they do not archive upstream warehouse revisions.';
COMMENT ON TABLE features.model_deployments IS
    'Explicit selected training fit for each (model_version, train_through_season). Candidate training and legacy import do not move this pointer.';
COMMENT ON TABLE features.model_deployment_history IS
    'Immutable database-generated audit trail for every deployment promotion/re-promotion and pointer deletion.';
COMMENT ON TABLE features.model_coefficients IS
    'Legacy historical compatibility rows. F05 writers append to features.training_fits; these rows remain available for exact legacy import and readers during coordinated cutover.';
COMMENT ON TABLE features.model_metadata IS
    'Legacy historical compatibility rows. F05 writers append to features.training_fits; these rows remain available for exact legacy import and readers during coordinated cutover.';

CREATE OR REPLACE FUNCTION features.reject_training_registry_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = ''
AS $function$
BEGIN
    RAISE EXCEPTION 'Training-fit and deployment-history snapshots are immutable; append a new fit or promotion';
END
$function$;

DROP TRIGGER IF EXISTS reject_training_fit_update ON features.training_fits;
CREATE TRIGGER reject_training_fit_update
    BEFORE UPDATE OR DELETE ON features.training_fits
    FOR EACH ROW EXECUTE FUNCTION features.reject_training_registry_mutation();
DROP TRIGGER IF EXISTS reject_training_fit_truncate ON features.training_fits;
CREATE TRIGGER reject_training_fit_truncate
    BEFORE TRUNCATE ON features.training_fits
    FOR EACH STATEMENT EXECUTE FUNCTION features.reject_training_registry_mutation();

DROP TRIGGER IF EXISTS reject_deployment_history_update
    ON features.model_deployment_history;
CREATE TRIGGER reject_deployment_history_update
    BEFORE UPDATE OR DELETE ON features.model_deployment_history
    FOR EACH ROW EXECUTE FUNCTION features.reject_training_registry_mutation();
DROP TRIGGER IF EXISTS reject_deployment_history_truncate
    ON features.model_deployment_history;
CREATE TRIGGER reject_deployment_history_truncate
    BEFORE TRUNCATE ON features.model_deployment_history
    FOR EACH STATEMENT EXECUTE FUNCTION features.reject_training_registry_mutation();

DROP TRIGGER IF EXISTS reject_model_deployments_truncate
    ON features.model_deployments;
CREATE TRIGGER reject_model_deployments_truncate
    BEFORE TRUNCATE ON features.model_deployments
    FOR EACH STATEMENT EXECUTE FUNCTION features.reject_training_registry_mutation();

CREATE OR REPLACE FUNCTION features.record_model_deployment_change()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO features.model_deployment_history
            (model_version, train_through_season, previous_training_fit_id,
             training_fit_id, action, reason, changed_by)
        VALUES
            (OLD.model_version, OLD.train_through_season, OLD.training_fit_id,
             NULL, 'delete',
             COALESCE(NULLIF(current_setting('features.deployment_delete_reason', true), ''),
                      'deployment pointer deleted'),
             session_user);
        RETURN OLD;
    END IF;

    INSERT INTO features.model_deployment_history
        (model_version, train_through_season, previous_training_fit_id,
         training_fit_id, action, reason, changed_by)
    VALUES
        (NEW.model_version, NEW.train_through_season,
         CASE WHEN TG_OP = 'UPDATE' THEN OLD.training_fit_id ELSE NULL END,
         NEW.training_fit_id, 'promote', NEW.promotion_reason, session_user);
    RETURN NEW;
END
$function$;

DROP TRIGGER IF EXISTS record_model_deployment_change
    ON features.model_deployments;
CREATE TRIGGER record_model_deployment_change
    AFTER INSERT OR UPDATE OR DELETE ON features.model_deployments
    FOR EACH ROW EXECUTE FUNCTION features.record_model_deployment_change();

GRANT USAGE ON SCHEMA features TO anon, authenticated;
GRANT SELECT ON features.training_fits, features.model_deployments,
    features.model_deployment_history TO anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON features.training_fits,
    features.model_deployments, features.model_deployment_history
    FROM anon, authenticated;

DO $migration$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst_ro') THEN
        REVOKE ALL ON TABLE features.training_fits,
            features.model_deployments,
            features.model_deployment_history FROM analyst_ro;
        REVOKE ALL ON SEQUENCE features.model_deployment_history_deployment_history_id_seq
            FROM analyst_ro;
    END IF;
END
$migration$;

REVOKE ALL ON FUNCTION features.reject_training_registry_mutation() FROM PUBLIC;
REVOKE ALL ON FUNCTION features.record_model_deployment_change() FROM PUBLIC;
