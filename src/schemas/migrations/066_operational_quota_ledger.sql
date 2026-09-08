-- Migration: 066_operational_quota_ledger
--
-- F14 quota foundation. This creates private operational audit and conservative
-- local admission records only; no pipeline caller is changed by this migration.
-- Apply through the reviewed forward-migration manifest. Preparing this file
-- does not authorize a production rollout.
--
-- The caller must commit reserve_cfbd_attempt(), then commit
-- mark_cfbd_attempt_dispatched(), before sending HTTP bytes. PostgreSQL cannot
-- make that cross-system ordering automatic. A committed reservation is always
-- charged locally, including a crash/unknown outcome. Provider reconciliation
-- is separate because provider billing may treat a response differently.

CREATE SCHEMA IF NOT EXISTS meta;

-- This namespace is dedicated to trusted quota routines. Existing meta writers
-- may keep CREATE there; placing RPCs alongside them would permit hostile
-- overloads to capture unknown/string arguments even with qualified calls.
DO $namespace$
DECLARE
    schema_row record;
BEGIN
    SELECT oid, nspowner, nspacl INTO schema_row
    FROM pg_catalog.pg_namespace WHERE nspname = 'warehouse_quota';
    IF NOT FOUND THEN
        CREATE SCHEMA warehouse_quota;
    ELSE
        IF schema_row.nspowner <> (SELECT oid FROM pg_catalog.pg_roles
                WHERE rolname = current_user) THEN
            RAISE EXCEPTION 'warehouse_quota must be owned by the migration role';
        END IF;
        IF EXISTS (
            SELECT 1 FROM pg_catalog.aclexplode(schema_row.nspacl) acl
            WHERE acl.grantee <> schema_row.nspowner AND acl.privilege_type = 'CREATE'
        ) THEN
            RAISE EXCEPTION 'existing warehouse_quota has untrusted CREATE grants';
        END IF;
        -- A same-named domain can turn a one-argument RPC call into a cast.
        -- This dedicated routines-only namespace has no legitimate types.
        IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_type WHERE typnamespace = schema_row.oid
        ) THEN
            RAISE EXCEPTION 'warehouse_quota contains an unexpected type';
        END IF;
        -- Do not adopt an already-compromised namespace by merely removing ACLs:
        -- an attacker-owned routine can restore its own EXECUTE grants later.
        IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc p
            WHERE p.pronamespace = schema_row.oid AND (
                p.proowner <> schema_row.nspowner OR p.prokind <> 'f'
                OR p.provariadic <> 0 OR p.pronargdefaults <> 0
                OR NOT COALESCE(p.oid = ANY(ARRAY[
                    pg_catalog.to_regprocedure('warehouse_quota.guard_api_request_attempt_update()'),
                    pg_catalog.to_regprocedure('warehouse_quota.reject_api_request_attempt_removal()'),
                    pg_catalog.to_regprocedure('warehouse_quota.start_operation_run(uuid,text,text,jsonb,text,text)'),
                    pg_catalog.to_regprocedure('warehouse_quota.finish_operation_run(uuid,text,text)'),
                    pg_catalog.to_regprocedure('warehouse_quota.reserve_cfbd_attempt(uuid,uuid,text,timestamptz,text,integer,jsonb)'),
                    pg_catalog.to_regprocedure('warehouse_quota.mark_cfbd_attempt_dispatched(uuid)'),
                    pg_catalog.to_regprocedure('warehouse_quota.record_cfbd_attempt_result(uuid,text,integer,text)')
                ]::oid[]), false)
            )
        ) THEN
            RAISE EXCEPTION 'warehouse_quota contains an unexpected or untrusted routine';
        END IF;
    END IF;
END
$namespace$;

-- Text equality in the account-scoped exclusion constraint comes from
-- btree_gist. The DO block below discovers its actual installation schema, so
-- this works both on plain PostgreSQL (normally public) and hosts that install
-- extensions into a dedicated schema.
CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;

DO $role$
DECLARE
    existing_role_is_unsafe boolean;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'warehouse_ingest') THEN
        CREATE ROLE warehouse_ingest NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB
            NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    ELSE
        SELECT r.rolcanlogin OR r.rolsuper OR r.rolcreatedb OR r.rolcreaterole
            OR r.rolreplication OR r.rolbypassrls OR r.rolinherit
            OR EXISTS (
                SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member = r.oid
            )
        INTO existing_role_is_unsafe
        FROM pg_catalog.pg_roles AS r
        WHERE r.rolname = 'warehouse_ingest';
        IF existing_role_is_unsafe THEN
            RAISE EXCEPTION 'existing warehouse_ingest role is not the required bounded NOLOGIN role';
        END IF;
    END IF;
END
$role$;

CREATE TABLE IF NOT EXISTS meta.operation_runs (
    operation_run_id uuid PRIMARY KEY,
    operation_kind text NOT NULL CHECK (
        operation_kind IN ('extract', 'load', 'compute', 'refresh', 'verify')
    ),
    initiator text NOT NULL CHECK (length(btrim(initiator)) > 0),
    requested_scope jsonb NOT NULL CHECK (jsonb_typeof(requested_scope) = 'object'),
    plan_digest text CHECK (plan_digest IS NULL OR length(btrim(plan_digest)) > 0),
    code_revision text CHECK (code_revision IS NULL OR length(btrim(code_revision)) > 0),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    outcome text NOT NULL DEFAULT 'running' CHECK (
        outcome IN ('running', 'succeeded', 'failed', 'partial', 'blocked', 'cancelled')
    ),
    error_summary text,
    recorded_by text NOT NULL DEFAULT session_user,
    CONSTRAINT operation_runs_lifecycle_check CHECK (
        (outcome = 'running' AND finished_at IS NULL AND error_summary IS NULL)
        OR
        (outcome <> 'running' AND finished_at IS NOT NULL
            AND (outcome <> 'succeeded' OR error_summary IS NULL))
    )
);

COMMENT ON TABLE meta.operation_runs IS
    'Private audit parent for one warehouse operation invocation. A successful run is not by itself a source-freshness or publication claim.';
COMMENT ON COLUMN meta.operation_runs.requested_scope IS
    'Caller-supplied non-secret JSON scope such as selected sources, assets, years, or work units.';
COMMENT ON COLUMN meta.operation_runs.recorded_by IS
    'Database session identity that invoked start_operation_run; initiator is the scheduler/CLI identity.';

CREATE TABLE IF NOT EXISTS meta.api_quota_periods (
    account_key text NOT NULL CHECK (length(btrim(account_key)) > 0),
    period_start_at timestamptz NOT NULL,
    period_end_at timestamptz NOT NULL,
    attempt_limit integer NOT NULL CHECK (attempt_limit > 0),
    reserved_attempts integer NOT NULL DEFAULT 0 CHECK (
        reserved_attempts >= 0 AND reserved_attempts <= attempt_limit
    ),
    provider_observed_used integer CHECK (provider_observed_used >= 0),
    provider_observed_at timestamptz,
    reconciliation_note text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (account_key, period_start_at),
    CONSTRAINT api_quota_periods_bounds_check CHECK (
        pg_catalog.isfinite(period_start_at)
        AND pg_catalog.isfinite(period_end_at)
        AND period_start_at < period_end_at
    ),
    CONSTRAINT api_quota_periods_observation_check CHECK (
        (provider_observed_used IS NULL) = (provider_observed_at IS NULL)
    )
);

DO $constraint$
DECLARE
    extension_schema text;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint
        WHERE conrelid = 'meta.api_quota_periods'::pg_catalog.regclass
          AND conname = 'api_quota_periods_no_overlap'
    ) THEN
        SELECT n.nspname
        INTO STRICT extension_schema
        FROM pg_catalog.pg_extension e
        JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
        WHERE e.extname = 'btree_gist';

        EXECUTE pg_catalog.format(
            'ALTER TABLE meta.api_quota_periods '
            'ADD CONSTRAINT api_quota_periods_no_overlap '
            'EXCLUDE USING gist '
            '(account_key %I.gist_text_ops WITH =, '
            'tstzrange(period_start_at, period_end_at, ''[)'') WITH &&)',
            extension_schema
        );
    END IF;
END
$constraint$;

COMMENT ON TABLE meta.api_quota_periods IS
    'Owner-configured local admission periods. account_key is an opaque non-secret identifier for one shared provider pool, including shared CFBD/CBBD products; it is never a token or token fingerprint.';
COMMENT ON COLUMN meta.api_quota_periods.attempt_limit IS
    'Conservative local reservation cap for this explicit period. At mid-period it must be configured from verified remaining capacity minus headroom, not copied from the full provider monthly limit.';
COMMENT ON COLUMN meta.api_quota_periods.reserved_attempts IS
    'Monotone local count maintained by reserve_cfbd_attempt. Every committed transport reservation stays charged regardless of HTTP/result status.';
COMMENT ON COLUMN meta.api_quota_periods.provider_observed_used IS
    'Optional owner-recorded provider observation for reconciliation; it never rewrites local immutable attempt history.';

CREATE TABLE IF NOT EXISTS meta.api_request_attempts (
    attempt_id uuid PRIMARY KEY,
    operation_run_id uuid NOT NULL REFERENCES meta.operation_runs (operation_run_id),
    account_key text NOT NULL,
    period_start_at timestamptz NOT NULL,
    period_attempt_number integer NOT NULL CHECK (period_attempt_number > 0),
    endpoint_class text NOT NULL CHECK (length(btrim(endpoint_class)) > 0),
    retry_ordinal integer NOT NULL CHECK (retry_ordinal >= 0),
    request_context jsonb NOT NULL CHECK (jsonb_typeof(request_context) = 'object'),
    reserved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    state text NOT NULL DEFAULT 'reserved' CHECK (
        state IN (
            'reserved', 'dispatched', 'succeeded', 'expected_no_data',
            'http_error', 'transport_error', 'unknown'
        )
    ),
    dispatched_at timestamptz,
    result_recorded_at timestamptz,
    http_status integer CHECK (http_status BETWEEN 100 AND 599),
    error_category text CHECK (
        error_category IS NULL OR length(btrim(error_category)) > 0
    ),
    CONSTRAINT api_request_attempts_period_fk FOREIGN KEY (account_key, period_start_at)
        REFERENCES meta.api_quota_periods (account_key, period_start_at),
    CONSTRAINT api_request_attempts_period_number_key UNIQUE (
        account_key, period_start_at, period_attempt_number
    ),
    CONSTRAINT api_request_attempts_lifecycle_check CHECK (
        (state = 'reserved'
            AND dispatched_at IS NULL AND result_recorded_at IS NULL
            AND http_status IS NULL AND error_category IS NULL)
        OR
        (state = 'dispatched'
            AND dispatched_at IS NOT NULL AND result_recorded_at IS NULL
            AND http_status IS NULL AND error_category IS NULL)
        OR
        (state IN ('succeeded', 'expected_no_data')
            AND dispatched_at IS NOT NULL AND result_recorded_at IS NOT NULL
            AND http_status IS NOT NULL AND http_status BETWEEN 200 AND 299
            AND error_category IS NULL)
        OR
        (state = 'http_error'
            AND dispatched_at IS NOT NULL AND result_recorded_at IS NOT NULL
            AND http_status IS NOT NULL AND (http_status < 200 OR http_status >= 300)
            AND error_category IS NOT NULL)
        OR
        (state = 'transport_error'
            AND dispatched_at IS NOT NULL AND result_recorded_at IS NOT NULL
            AND http_status IS NULL AND error_category IS NOT NULL)
        OR
        (state = 'unknown'
            AND result_recorded_at IS NOT NULL AND http_status IS NULL
            AND error_category IS NOT NULL
            AND (
                (dispatched_at IS NULL AND error_category = 'dispatch_unconfirmed')
                OR (dispatched_at IS NOT NULL AND error_category = 'response_unobserved')
            ))
    )
);

CREATE INDEX IF NOT EXISTS idx_api_request_attempts_operation_state
    ON meta.api_request_attempts (operation_run_id, state);

COMMENT ON TABLE meta.api_request_attempts IS
    'One private row per locally admitted HTTP transport attempt. Reservation identity and charge are immutable; terminal failure and unknown states never refund quota.';
COMMENT ON COLUMN meta.api_request_attempts.request_context IS
    'Non-secret immutable logical context used to reject an attempt UUID replayed for a different request. Callers must never include credentials or authorization material.';
COMMENT ON COLUMN meta.api_request_attempts.state IS
    'Transport lifecycle, separate from source-level publication/freshness. reserved/dispatched can remain pending after a crash and remain locally charged.';

CREATE OR REPLACE FUNCTION warehouse_quota.guard_api_request_attempt_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = ''
AS $function$
BEGIN
    IF ROW(
        NEW.attempt_id, NEW.operation_run_id, NEW.account_key, NEW.period_start_at,
        NEW.period_attempt_number, NEW.endpoint_class, NEW.retry_ordinal,
        NEW.request_context, NEW.reserved_at
    ) IS DISTINCT FROM ROW(
        OLD.attempt_id, OLD.operation_run_id, OLD.account_key, OLD.period_start_at,
        OLD.period_attempt_number, OLD.endpoint_class, OLD.retry_ordinal,
        OLD.request_context, OLD.reserved_at
    ) THEN
        RAISE EXCEPTION 'API attempt reservation identity is immutable';
    END IF;

    IF NOT (
        (OLD.state = 'reserved' AND NEW.state IN ('dispatched', 'unknown'))
        OR (OLD.state = 'dispatched' AND NEW.state IN (
            'succeeded', 'expected_no_data', 'http_error', 'transport_error', 'unknown'
        ))
    ) THEN
        RAISE EXCEPTION 'Invalid API attempt state transition from % to %',
            OLD.state, NEW.state;
    END IF;
    RETURN NEW;
END
$function$;

DROP TRIGGER IF EXISTS guard_api_request_attempt_update ON meta.api_request_attempts;
CREATE TRIGGER guard_api_request_attempt_update
    BEFORE UPDATE ON meta.api_request_attempts
    FOR EACH ROW EXECUTE FUNCTION warehouse_quota.guard_api_request_attempt_update();

CREATE OR REPLACE FUNCTION warehouse_quota.reject_api_request_attempt_removal()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = ''
AS $function$
BEGIN
    RAISE EXCEPTION 'API attempt history is append-only and cannot be removed';
END
$function$;

DROP TRIGGER IF EXISTS reject_api_request_attempt_delete ON meta.api_request_attempts;
CREATE TRIGGER reject_api_request_attempt_delete
    BEFORE DELETE OR TRUNCATE ON meta.api_request_attempts
    FOR EACH STATEMENT EXECUTE FUNCTION warehouse_quota.reject_api_request_attempt_removal();

CREATE OR REPLACE FUNCTION warehouse_quota.start_operation_run(
    p_operation_run_id uuid,
    p_operation_kind text,
    p_initiator text,
    p_requested_scope jsonb,
    p_plan_digest text,
    p_code_revision text
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
    existing_run meta.operation_runs%ROWTYPE;
BEGIN
    IF p_operation_run_id IS NULL THEN
        RAISE EXCEPTION 'operation_run_id is required' USING ERRCODE = '22023';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(p_operation_run_id::text, 0)
    );
    SELECT r.* INTO existing_run
    FROM meta.operation_runs AS r
    WHERE r.operation_run_id = p_operation_run_id;

    IF FOUND THEN
        IF existing_run.operation_kind IS DISTINCT FROM p_operation_kind
            OR existing_run.initiator IS DISTINCT FROM p_initiator
            OR existing_run.requested_scope IS DISTINCT FROM p_requested_scope
            OR existing_run.plan_digest IS DISTINCT FROM p_plan_digest
            OR existing_run.code_revision IS DISTINCT FROM p_code_revision THEN
            RAISE EXCEPTION 'operation_run_id already exists with different context'
                USING ERRCODE = '22023';
        END IF;
        RETURN p_operation_run_id;
    END IF;

    INSERT INTO meta.operation_runs (
        operation_run_id, operation_kind, initiator, requested_scope,
        plan_digest, code_revision
    ) VALUES (
        p_operation_run_id, p_operation_kind, p_initiator, p_requested_scope,
        p_plan_digest, p_code_revision
    );
    RETURN p_operation_run_id;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_quota.finish_operation_run(
    p_operation_run_id uuid,
    p_outcome text,
    p_error_summary text
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
    run_row meta.operation_runs%ROWTYPE;
    pending_attempts integer;
BEGIN
    IF p_outcome IS NULL OR p_outcome NOT IN (
        'succeeded', 'failed', 'partial', 'blocked', 'cancelled'
    ) THEN
        RAISE EXCEPTION 'terminal operation outcome is invalid' USING ERRCODE = '22023';
    END IF;
    IF p_outcome = 'succeeded' AND p_error_summary IS NOT NULL THEN
        RAISE EXCEPTION 'a succeeded operation cannot have an error summary'
            USING ERRCODE = '22023';
    END IF;

    SELECT r.* INTO run_row
    FROM meta.operation_runs AS r
    WHERE r.operation_run_id = p_operation_run_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'operation run is not configured' USING ERRCODE = '22023';
    END IF;

    SELECT count(*)::integer INTO pending_attempts
    FROM meta.api_request_attempts AS a
    WHERE a.operation_run_id = p_operation_run_id
      AND a.state IN ('reserved', 'dispatched');

    IF run_row.outcome <> 'running' THEN
        IF run_row.outcome IS DISTINCT FROM p_outcome
            OR run_row.error_summary IS DISTINCT FROM p_error_summary THEN
            RAISE EXCEPTION 'operation run already finished with a different result'
                USING ERRCODE = '22023';
        END IF;
        RETURN pending_attempts;
    END IF;

    IF p_outcome = 'succeeded' AND pending_attempts > 0 THEN
        RAISE EXCEPTION 'cannot succeed operation with % pending API attempt(s)',
            pending_attempts USING ERRCODE = '22023';
    END IF;

    UPDATE meta.operation_runs
    SET outcome = p_outcome,
        error_summary = p_error_summary,
        finished_at = pg_catalog.clock_timestamp()
    WHERE operation_run_id = p_operation_run_id;
    RETURN pending_attempts;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_quota.reserve_cfbd_attempt(
    p_attempt_id uuid,
    p_operation_run_id uuid,
    p_account_key text,
    p_period_start_at timestamptz,
    p_endpoint_class text,
    p_retry_ordinal integer,
    p_request_context jsonb
)
RETURNS TABLE (
    attempt_id uuid,
    reserved_at timestamptz,
    period_attempt_number integer,
    attempt_limit integer,
    reused boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
    existing_attempt meta.api_request_attempts%ROWTYPE;
    run_outcome text;
    period_row meta.api_quota_periods%ROWTYPE;
    reservation_time timestamptz;
    new_period_count integer;
BEGIN
    IF p_attempt_id IS NULL OR p_operation_run_id IS NULL
        OR p_account_key IS NULL OR length(pg_catalog.btrim(p_account_key)) = 0
        OR p_period_start_at IS NULL
        OR p_endpoint_class IS NULL OR length(pg_catalog.btrim(p_endpoint_class)) = 0
        OR p_retry_ordinal IS NULL OR p_retry_ordinal < 0
        OR p_request_context IS NULL OR pg_catalog.jsonb_typeof(p_request_context) <> 'object' THEN
        RAISE EXCEPTION 'attempt reservation context is invalid' USING ERRCODE = '22023';
    END IF;

    -- Serialize on the logical attempt before any period counter is touched.
    -- Hash collisions only add harmless serialization.
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(p_attempt_id::text, 1)
    );
    SELECT a.* INTO existing_attempt
    FROM meta.api_request_attempts AS a
    WHERE a.attempt_id = p_attempt_id;

    IF FOUND THEN
        IF existing_attempt.operation_run_id IS DISTINCT FROM p_operation_run_id
            OR existing_attempt.account_key IS DISTINCT FROM p_account_key
            OR existing_attempt.period_start_at IS DISTINCT FROM p_period_start_at
            OR existing_attempt.endpoint_class IS DISTINCT FROM p_endpoint_class
            OR existing_attempt.retry_ordinal IS DISTINCT FROM p_retry_ordinal
            OR existing_attempt.request_context IS DISTINCT FROM p_request_context THEN
            RAISE EXCEPTION 'attempt_id already reserved with different context'
                USING ERRCODE = '22023';
        END IF;

        RETURN QUERY
        SELECT existing_attempt.attempt_id, existing_attempt.reserved_at,
            existing_attempt.period_attempt_number, p.attempt_limit, true
        FROM meta.api_quota_periods AS p
        WHERE p.account_key = existing_attempt.account_key
          AND p.period_start_at = existing_attempt.period_start_at;
        RETURN;
    END IF;

    SELECT r.outcome INTO run_outcome
    FROM meta.operation_runs AS r
    WHERE r.operation_run_id = p_operation_run_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'operation run is not configured' USING ERRCODE = '22023';
    END IF;
    IF run_outcome <> 'running' THEN
        RAISE EXCEPTION 'operation run is already terminal' USING ERRCODE = '22023';
    END IF;

    SELECT p.* INTO period_row
    FROM meta.api_quota_periods AS p
    WHERE p.account_key = p_account_key
      AND p.period_start_at = p_period_start_at
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'quota period is not configured' USING ERRCODE = '22023';
    END IF;

    -- Use a fresh wall clock only after the potentially blocking period lock.
    reservation_time := pg_catalog.clock_timestamp();
    IF reservation_time < period_row.period_start_at
        OR reservation_time >= period_row.period_end_at THEN
        RAISE EXCEPTION 'quota period is not active' USING ERRCODE = '22023';
    END IF;
    IF period_row.reserved_attempts >= period_row.attempt_limit THEN
        RAISE EXCEPTION 'quota period is exhausted' USING ERRCODE = 'P0001';
    END IF;

    UPDATE meta.api_quota_periods AS p
    SET reserved_attempts = p.reserved_attempts + 1,
        updated_at = reservation_time
    WHERE p.account_key = p_account_key
      AND p.period_start_at = p_period_start_at
    RETURNING p.reserved_attempts INTO new_period_count;

    INSERT INTO meta.api_request_attempts (
        attempt_id, operation_run_id, account_key, period_start_at,
        period_attempt_number, endpoint_class, retry_ordinal, request_context,
        reserved_at
    ) VALUES (
        p_attempt_id, p_operation_run_id, p_account_key, p_period_start_at,
        new_period_count, p_endpoint_class, p_retry_ordinal, p_request_context,
        reservation_time
    );

    RETURN QUERY SELECT p_attempt_id, reservation_time, new_period_count,
        period_row.attempt_limit, false;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_quota.mark_cfbd_attempt_dispatched(p_attempt_id uuid)
RETURNS timestamptz
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
    attempt_row meta.api_request_attempts%ROWTYPE;
    run_outcome text;
    period_row meta.api_quota_periods%ROWTYPE;
    dispatch_time timestamptz;
BEGIN
    -- Read identity, then lock in the same run -> attempt -> period order used
    -- by operation finish/reservation to avoid lock-order inversions.
    SELECT a.* INTO attempt_row
    FROM meta.api_request_attempts AS a
    WHERE a.attempt_id = p_attempt_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'attempt is not reserved' USING ERRCODE = '22023';
    END IF;

    SELECT r.outcome INTO run_outcome
    FROM meta.operation_runs AS r
    WHERE r.operation_run_id = attempt_row.operation_run_id
    FOR UPDATE;

    SELECT a.* INTO attempt_row
    FROM meta.api_request_attempts AS a
    WHERE a.attempt_id = p_attempt_id
    FOR UPDATE;
    IF attempt_row.state <> 'reserved' THEN
        RAISE EXCEPTION 'attempt is already dispatched or terminal; do not send it again'
            USING ERRCODE = '22023';
    END IF;
    IF run_outcome <> 'running' THEN
        RAISE EXCEPTION 'operation run is already terminal' USING ERRCODE = '22023';
    END IF;

    SELECT p.* INTO period_row
    FROM meta.api_quota_periods AS p
    WHERE p.account_key = attempt_row.account_key
      AND p.period_start_at = attempt_row.period_start_at
    FOR UPDATE;
    dispatch_time := pg_catalog.clock_timestamp();
    IF dispatch_time < period_row.period_start_at
        OR dispatch_time >= period_row.period_end_at THEN
        RAISE EXCEPTION 'quota period is not active at dispatch' USING ERRCODE = '22023';
    END IF;

    UPDATE meta.api_request_attempts
    SET state = 'dispatched', dispatched_at = dispatch_time
    WHERE attempt_id = p_attempt_id;
    RETURN dispatch_time;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_quota.record_cfbd_attempt_result(
    p_attempt_id uuid,
    p_state text,
    p_http_status integer,
    p_error_category text
)
RETURNS timestamptz
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
    attempt_row meta.api_request_attempts%ROWTYPE;
    result_time timestamptz;
BEGIN
    IF p_state IS NULL OR p_state NOT IN (
        'succeeded', 'expected_no_data', 'http_error', 'transport_error', 'unknown'
    ) THEN
        RAISE EXCEPTION 'terminal attempt state is invalid' USING ERRCODE = '22023';
    END IF;
    IF p_http_status IS NOT NULL AND (p_http_status < 100 OR p_http_status > 599) THEN
        RAISE EXCEPTION 'HTTP status is invalid' USING ERRCODE = '22023';
    END IF;
    IF p_state IN ('succeeded', 'expected_no_data') AND (
        p_http_status IS NULL OR p_http_status < 200 OR p_http_status >= 300
        OR p_error_category IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'successful/logical no-data transport result requires 2xx status and no error category'
            USING ERRCODE = '22023';
    END IF;
    IF p_state = 'http_error' AND (
        p_http_status IS NULL OR (p_http_status >= 200 AND p_http_status < 300)
        OR p_error_category IS NULL OR length(pg_catalog.btrim(p_error_category)) = 0
    ) THEN
        RAISE EXCEPTION 'HTTP error result requires non-2xx status and error category'
            USING ERRCODE = '22023';
    END IF;
    IF p_state = 'transport_error' AND (
        p_http_status IS NOT NULL OR p_error_category IS NULL OR length(pg_catalog.btrim(p_error_category)) = 0
    ) THEN
        RAISE EXCEPTION 'transport error requires no HTTP status and an error category'
            USING ERRCODE = '22023';
    END IF;

    SELECT a.* INTO attempt_row
    FROM meta.api_request_attempts AS a
    WHERE a.attempt_id = p_attempt_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'attempt is not reserved' USING ERRCODE = '22023';
    END IF;

    IF p_state = 'unknown' AND (
        p_http_status IS NOT NULL OR p_error_category IS DISTINCT FROM
            CASE WHEN attempt_row.dispatched_at IS NULL THEN 'dispatch_unconfirmed'
                 ELSE 'response_unobserved' END
    ) THEN
        RAISE EXCEPTION 'unknown result requires no HTTP status and the matching dispatch phase category'
            USING ERRCODE = '22023';
    END IF;

    IF attempt_row.state IN (
        'succeeded', 'expected_no_data', 'http_error', 'transport_error', 'unknown'
    ) THEN
        IF attempt_row.state IS DISTINCT FROM p_state
            OR attempt_row.http_status IS DISTINCT FROM p_http_status
            OR attempt_row.error_category IS DISTINCT FROM p_error_category THEN
            RAISE EXCEPTION 'attempt already has a different terminal result'
                USING ERRCODE = '22023';
        END IF;
        RETURN attempt_row.result_recorded_at;
    END IF;

    IF attempt_row.state = 'reserved' AND p_state <> 'unknown' THEN
        RAISE EXCEPTION 'attempt must be marked dispatched before recording this result'
            USING ERRCODE = '22023';
    END IF;

    result_time := pg_catalog.clock_timestamp();
    UPDATE meta.api_request_attempts
    SET state = p_state,
        result_recorded_at = result_time,
        http_status = p_http_status,
        error_category = p_error_category
    WHERE attempt_id = p_attempt_id;
    RETURN result_time;
END
$function$;

-- Host defaults can be deliberately broad and can name arbitrary roles.
-- Remove their ACL entries from only the new private objects/functions. Do not
-- rewrite named roles' pre-existing meta schema privileges: older ledgers and
-- loaders already use that schema.
DO $quota_acl$
DECLARE
    entry record;
    recipient text;
BEGIN
    FOR entry IN
        SELECT DISTINCT 'SCHEMA' AS object_kind,
            pg_catalog.format('%I', n.nspname) AS object_name,
            acl.grantee, 'ALL' AS privileges
        FROM pg_catalog.pg_namespace n
        CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) acl
        WHERE n.nspname = 'warehouse_quota' AND acl.grantee <> n.nspowner
        UNION
        SELECT DISTINCT 'TABLE',
            pg_catalog.format('%I.%I', n.nspname, c.relname), acl.grantee, 'ALL'
        FROM pg_catalog.pg_class AS c
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) AS acl
        WHERE n.nspname = 'meta'
          AND c.relname IN ('operation_runs', 'api_quota_periods', 'api_request_attempts')
          AND c.relkind IN ('r', 'p') AND acl.grantee <> c.relowner
        UNION
        SELECT DISTINCT 'FUNCTION',
            pg_catalog.format('%I.%I(%s)', n.nspname, p.proname,
                pg_catalog.pg_get_function_identity_arguments(p.oid)),
            acl.grantee, 'ALL'
        FROM pg_catalog.pg_proc AS p
        JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))
        ) AS acl
        WHERE n.nspname = 'warehouse_quota'
          AND p.proname IN (
              'guard_api_request_attempt_update', 'reject_api_request_attempt_removal',
              'start_operation_run', 'finish_operation_run', 'reserve_cfbd_attempt',
              'mark_cfbd_attempt_dispatched', 'record_cfbd_attempt_result'
          )
          AND acl.grantee <> p.proowner
    LOOP
        recipient := CASE WHEN entry.grantee = 0 THEN 'PUBLIC'
            ELSE pg_catalog.format('%I', pg_catalog.pg_get_userbyid(entry.grantee)) END;
        EXECUTE pg_catalog.format('REVOKE %s ON %s %s FROM %s',
            entry.privileges, entry.object_kind, entry.object_name, recipient);
    END LOOP;
END
$quota_acl$;

GRANT USAGE ON SCHEMA warehouse_quota TO warehouse_ingest;

GRANT EXECUTE ON FUNCTION warehouse_quota.start_operation_run(uuid, text, text, jsonb, text, text),
    warehouse_quota.finish_operation_run(uuid, text, text),
    warehouse_quota.reserve_cfbd_attempt(uuid, uuid, text, timestamptz, text, integer, jsonb),
    warehouse_quota.mark_cfbd_attempt_dispatched(uuid),
    warehouse_quota.record_cfbd_attempt_result(uuid, text, integer, text)
    TO warehouse_ingest;
