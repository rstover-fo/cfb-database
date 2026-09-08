-- F14 transport admission. Follow-up to immutable migration 066.
-- Apply only through the managed manifests after separate rollout approval.
-- No budgets or runtime memberships are configured here.
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
                    pg_catalog.to_regprocedure('warehouse_quota.record_cfbd_attempt_result(uuid,text,integer,text)'),
                    pg_catalog.to_regprocedure('warehouse_quota.reserve_cfbd_budget_attempt(uuid,uuid,text,timestamptz,text,integer,jsonb,text)'),
                    pg_catalog.to_regprocedure('warehouse_quota.reserve_cfbd_transport_attempt(uuid,uuid,text,text,integer,jsonb)')
                ]::oid[]), false)
            )
        ) THEN
            RAISE EXCEPTION 'warehouse_quota contains an unexpected or untrusted routine';
        END IF;
    END IF;
END
$namespace$;

ALTER TABLE meta.api_quota_periods ADD COLUMN IF NOT EXISTS budget_class text
    NOT NULL DEFAULT 'extraction' CHECK (budget_class IN ('extraction', 'reconciliation'));
ALTER TABLE meta.api_request_attempts ADD COLUMN IF NOT EXISTS budget_class text
    NOT NULL DEFAULT 'extraction' CHECK (budget_class IN ('extraction', 'reconciliation'));
ALTER TABLE meta.api_request_attempts DROP CONSTRAINT IF EXISTS api_request_attempts_period_fk;
ALTER TABLE meta.api_request_attempts DROP CONSTRAINT IF EXISTS api_request_attempts_period_number_key;
ALTER TABLE meta.api_quota_periods DROP CONSTRAINT IF EXISTS api_quota_periods_pkey;
ALTER TABLE meta.api_quota_periods DROP CONSTRAINT IF EXISTS api_quota_periods_no_overlap;
ALTER TABLE meta.api_quota_periods ADD PRIMARY KEY (account_key, budget_class, period_start_at);
ALTER TABLE meta.api_request_attempts ADD CONSTRAINT api_request_attempts_period_fk
    FOREIGN KEY (account_key, budget_class, period_start_at)
    REFERENCES meta.api_quota_periods(account_key, budget_class, period_start_at);
ALTER TABLE meta.api_request_attempts ADD CONSTRAINT api_request_attempts_period_number_key
    UNIQUE (account_key, budget_class, period_start_at, period_attempt_number);
COMMENT ON COLUMN meta.api_quota_periods.budget_class IS
    'Independent owner-configured finite allowances. Reconciliation is only for exact /info and /info/usage paths.';


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
            'budget_class %I.gist_text_ops WITH =, '
            'tstzrange(period_start_at, period_end_at, ''[)'') WITH &&)',
            extension_schema, extension_schema
        );
    END IF;
END
$constraint$;



CREATE OR REPLACE FUNCTION warehouse_quota.guard_api_request_attempt_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = ''
AS $function$
BEGIN
    IF ROW(
        NEW.attempt_id, NEW.operation_run_id, NEW.account_key, NEW.period_start_at,
        NEW.period_attempt_number, NEW.endpoint_class, NEW.retry_ordinal,
        NEW.request_context, NEW.reserved_at, NEW.budget_class
    ) IS DISTINCT FROM ROW(
        OLD.attempt_id, OLD.operation_run_id, OLD.account_key, OLD.period_start_at,
        OLD.period_attempt_number, OLD.endpoint_class, OLD.retry_ordinal,
        OLD.request_context, OLD.reserved_at, OLD.budget_class
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

CREATE OR REPLACE FUNCTION warehouse_quota.reserve_cfbd_budget_attempt(
    p_attempt_id uuid,
    p_operation_run_id uuid,
    p_account_key text,
    p_period_start_at timestamptz,
    p_endpoint_class text,
    p_retry_ordinal integer,
    p_request_context jsonb,
    p_budget_class text
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
    IF p_budget_class IS NULL OR p_budget_class NOT IN ('extraction', 'reconciliation')
        OR p_attempt_id IS NULL OR p_operation_run_id IS NULL
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
            OR existing_attempt.budget_class IS DISTINCT FROM p_budget_class
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
        WHERE p.budget_class = existing_attempt.budget_class
          AND p.account_key = existing_attempt.account_key
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
    WHERE p.budget_class = p_budget_class
      AND p.account_key = p_account_key
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
    WHERE p.budget_class = p_budget_class
      AND p.account_key = p_account_key
      AND p.period_start_at = p_period_start_at
    RETURNING p.reserved_attempts INTO new_period_count;

    INSERT INTO meta.api_request_attempts (
        attempt_id, operation_run_id, account_key, period_start_at,
        period_attempt_number, endpoint_class, retry_ordinal, request_context,
        reserved_at, budget_class
    ) VALUES (
        p_attempt_id, p_operation_run_id, p_account_key, p_period_start_at,
        new_period_count, p_endpoint_class, p_retry_ordinal, p_request_context,
        reservation_time, p_budget_class
    );

    RETURN QUERY SELECT p_attempt_id, reservation_time, new_period_count,
        period_row.attempt_limit, false;
END
$function$;

-- Retain the original explicit-period RPC as extraction-only.
CREATE OR REPLACE FUNCTION warehouse_quota.reserve_cfbd_attempt(
    p_attempt_id uuid, p_operation_run_id uuid, p_account_key text,
    p_period_start_at timestamptz, p_endpoint_class text, p_retry_ordinal integer,
    p_request_context jsonb
)
RETURNS TABLE (attempt_id uuid, reserved_at timestamptz, period_attempt_number integer,
    attempt_limit integer, reused boolean)
LANGUAGE sql SECURITY DEFINER SET search_path = '' AS $function$
    SELECT * FROM warehouse_quota.reserve_cfbd_budget_attempt(
        p_attempt_id, p_operation_run_id, p_account_key, p_period_start_at,
        p_endpoint_class, p_retry_ordinal, p_request_context, 'extraction');
$function$;

CREATE OR REPLACE FUNCTION warehouse_quota.reserve_cfbd_transport_attempt(
    p_attempt_id uuid, p_operation_run_id uuid, p_account_key text,
    p_endpoint text, p_retry_ordinal integer, p_request_context jsonb
)
RETURNS TABLE (attempt_id uuid, reserved_at timestamptz, period_attempt_number integer,
    attempt_limit integer, reused boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE
    selected_class text;
    selected_start timestamptz;
BEGIN
    -- Absolute URLs, query strings, dot segments, escapes and fragments cannot
    -- masquerade as trusted control endpoints or escape the configured API host.
    IF p_endpoint IS NULL OR p_endpoint !~ '^/[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*$' THEN
        RAISE EXCEPTION 'endpoint must be a canonical API path' USING ERRCODE = '22023';
    END IF;
    selected_class := CASE WHEN p_endpoint IN ('/info', '/info/usage')
        THEN 'reconciliation' ELSE 'extraction' END;
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(p_attempt_id::text, 1));
    -- Replays use their original period, including after its expiry. The helper
    -- verifies every context field and class before returning the old charge.
    SELECT a.period_start_at INTO selected_start FROM meta.api_request_attempts a
        WHERE a.attempt_id = p_attempt_id;
    IF NOT FOUND THEN
        SELECT p.period_start_at INTO selected_start FROM meta.api_quota_periods p
        WHERE p.account_key = p_account_key AND p.budget_class = selected_class
          AND p.period_start_at <= pg_catalog.clock_timestamp()
          AND p.period_end_at > pg_catalog.clock_timestamp();
        IF NOT FOUND THEN
            RAISE EXCEPTION 'active quota allowance is not configured' USING ERRCODE = '22023';
        END IF;
    END IF;
    RETURN QUERY SELECT * FROM warehouse_quota.reserve_cfbd_budget_attempt(
        p_attempt_id, p_operation_run_id, p_account_key, selected_start,
        p_endpoint, p_retry_ordinal, p_request_context, selected_class);
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
    WHERE p.budget_class = attempt_row.budget_class
      AND p.account_key = attempt_row.account_key
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

-- New routines may inherit broad host defaults: scrub every non-owner grantee,
-- including implicit PUBLIC, before granting only the transport entry point.
DO $acl$
DECLARE r record; recipient text;
BEGIN
    FOR r IN
        SELECT p.oid::pg_catalog.regprocedure AS signature, a.grantee
        FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(p.proacl, pg_catalog.acldefault('f',p.proowner))) a
        WHERE n.nspname='warehouse_quota'
          AND p.proname IN ('reserve_cfbd_budget_attempt','reserve_cfbd_transport_attempt')
          AND a.grantee <> p.proowner
    LOOP
        recipient := CASE WHEN r.grantee=0 THEN 'PUBLIC'
            ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(r.grantee)) END;
        EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM %s',r.signature,recipient);
    END LOOP;
END
$acl$;
GRANT EXECUTE ON FUNCTION warehouse_quota.reserve_cfbd_transport_attempt(uuid,uuid,text,text,integer,jsonb)
    TO warehouse_ingest;
