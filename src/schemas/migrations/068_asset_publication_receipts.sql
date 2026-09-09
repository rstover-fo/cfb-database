-- Migration: 068_asset_publication_receipts
--
-- F14/F19 controlled publication foundation. Prepared, not a production
-- rollout. This bounded publisher owns only the full-history house Elo game
-- table, its current-team snapshot, and marts.house_elo_game.
--
-- The producer computes from one REPEATABLE READ snapshot, commits that read
-- transaction, then calls publish_house_elo() in a fresh READ COMMITTED
-- transaction. The RPC locks and re-digests all of core.games before replacing
-- either target. Data, ordinary mart refresh, receipts and current pointers
-- become visible together. published_at is an in-transaction clock reading;
-- PostgreSQL does not expose an exact commit timestamp here.

CREATE SCHEMA IF NOT EXISTS meta;

-- A routines-only namespace prevents hostile overloads in broader schemas.
DO $namespace$
DECLARE schema_row record;
BEGIN
    SELECT oid, nspowner, nspacl INTO schema_row
    FROM pg_catalog.pg_namespace WHERE nspname = 'warehouse_publication';
    IF NOT FOUND THEN
        CREATE SCHEMA warehouse_publication;
    ELSE
        IF schema_row.nspowner <> (SELECT oid FROM pg_catalog.pg_roles
                WHERE rolname = current_user) THEN
            RAISE EXCEPTION 'warehouse_publication must be owned by the migration role';
        END IF;
        IF EXISTS (
            SELECT 1 FROM pg_catalog.aclexplode(schema_row.nspacl) acl
            WHERE acl.grantee <> schema_row.nspowner AND acl.privilege_type = 'CREATE'
        ) THEN
            RAISE EXCEPTION 'warehouse_publication has untrusted CREATE grants';
        END IF;
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_type WHERE typnamespace = schema_row.oid) THEN
            RAISE EXCEPTION 'warehouse_publication contains an unexpected type';
        END IF;
        IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc p
            WHERE p.pronamespace = schema_row.oid AND (
                p.proowner <> schema_row.nspowner OR p.prokind <> 'f'
                OR p.provariadic <> 0 OR p.pronargdefaults <> 0
                OR NOT COALESCE(p.oid = ANY(ARRAY[
                    pg_catalog.to_regprocedure('warehouse_publication.reject_append_only_mutation()'),
                    pg_catalog.to_regprocedure('warehouse_publication.guard_current_generation()'),
                    pg_catalog.to_regprocedure('warehouse_publication.guard_receipt_input()'),
                    pg_catalog.to_regprocedure('warehouse_publication.invalidate_house_elo_pointers()'),
                    pg_catalog.to_regprocedure('warehouse_publication.invalidate_asset_pointer_ddl()'),
                    pg_catalog.to_regprocedure('warehouse_publication.invalidate_asset_pointer_drop()'),
                    pg_catalog.to_regprocedure('warehouse_publication.start_house_elo_run(uuid)'),
                    pg_catalog.to_regprocedure('warehouse_publication.finish_house_elo_run(uuid,text)'),
                    pg_catalog.to_regprocedure('warehouse_publication.get_house_elo_state(bigint,bigint)'),
                    pg_catalog.to_regprocedure('warehouse_publication.publish_house_elo(uuid,uuid,uuid,uuid,uuid,text,jsonb,jsonb,bigint,bigint,bigint[])'),
                    pg_catalog.to_regprocedure('warehouse_publication.record_house_elo_failure(uuid,uuid,uuid,text,text)')
                ]::oid[]), false)
            )
        ) THEN
            RAISE EXCEPTION 'warehouse_publication contains an unexpected or untrusted routine';
        END IF;
    END IF;
END
$namespace$;

DO $role$
DECLARE unsafe boolean;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'warehouse_publisher') THEN
        CREATE ROLE warehouse_publisher NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB
            NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    ELSE
        SELECT r.rolcanlogin OR r.rolsuper OR r.rolcreatedb OR r.rolcreaterole
            OR r.rolreplication OR r.rolbypassrls OR r.rolinherit
            OR EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member = r.oid)
        INTO unsafe FROM pg_catalog.pg_roles r WHERE r.rolname = 'warehouse_publisher';
        IF unsafe THEN
            RAISE EXCEPTION 'existing warehouse_publisher is not the required bounded NOLOGIN role';
        END IF;
    END IF;
END
$role$;

CREATE TABLE IF NOT EXISTS meta.asset_publication_locks (
    asset_key text PRIMARY KEY CHECK (asset_key IN (
        'analytics.house_elo_game', 'marts.house_elo_game'
    )),
    relation_schema text NOT NULL,
    relation_name text NOT NULL,
    relation_oid oid NOT NULL UNIQUE,
    relation_kind "char" NOT NULL CHECK (relation_kind IN ('r', 'm')),
    CHECK (asset_key = relation_schema || '.' || relation_name)
);

CREATE TABLE IF NOT EXISTS meta.asset_receipts (
    generation_id uuid PRIMARY KEY,
    operation_run_id uuid NOT NULL REFERENCES meta.operation_runs(operation_run_id),
    asset_key text NOT NULL REFERENCES meta.asset_publication_locks(asset_key),
    coverage_key text NOT NULL DEFAULT 'source-wide' CHECK (coverage_key = 'source-wide'),
    outcome text NOT NULL CHECK (outcome IN (
        'succeeded', 'expected_no_data', 'failed', 'deferred', 'partial', 'blocked'
    )),
    coverage jsonb NOT NULL CHECK (
        pg_catalog.jsonb_typeof(coverage) = 'object'
        AND coverage ? 'complete'
        AND coverage->'complete' IN ('true'::jsonb, 'false'::jsonb)
    ),
    source_watermark text,
    input_observations jsonb NOT NULL DEFAULT '{}' CHECK (
        pg_catalog.jsonb_typeof(input_observations) = 'object'
    ),
    row_delta jsonb NOT NULL DEFAULT '{}' CHECK (pg_catalog.jsonb_typeof(row_delta) = 'object'),
    request_digest text NOT NULL CHECK (request_digest ~ '^[0-9a-f]{32}$'),
    recorded_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    published_at timestamptz,
    committed_at timestamptz CHECK (committed_at IS NULL),
    error_summary text,
    UNIQUE (asset_key, coverage_key, generation_id),
    CHECK (
        (outcome IN ('succeeded', 'expected_no_data')
            AND published_at IS NOT NULL AND coverage->'complete' = 'true'::jsonb
            AND error_summary IS NULL)
        OR (outcome NOT IN ('succeeded', 'expected_no_data')
            AND published_at IS NULL AND coverage->'complete' = 'false'::jsonb
            AND error_summary IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS asset_receipts_operation_run_idx
    ON meta.asset_receipts(operation_run_id);
CREATE INDEX IF NOT EXISTS asset_receipts_asset_published_idx
    ON meta.asset_receipts(asset_key, published_at DESC) WHERE published_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS meta.asset_current_generations (
    asset_key text NOT NULL,
    coverage_key text NOT NULL DEFAULT 'source-wide' CHECK (coverage_key = 'source-wide'),
    generation_id uuid NOT NULL,
    PRIMARY KEY (asset_key, coverage_key),
    FOREIGN KEY (asset_key, coverage_key, generation_id)
        REFERENCES meta.asset_receipts(asset_key, coverage_key, generation_id)
);

CREATE TABLE IF NOT EXISTS meta.asset_receipt_inputs (
    generation_id uuid NOT NULL REFERENCES meta.asset_receipts(generation_id),
    input_asset_key text NOT NULL,
    input_coverage_key text NOT NULL DEFAULT 'source-wide' CHECK (
        input_coverage_key = 'source-wide'
    ),
    input_generation_id uuid NOT NULL,
    PRIMARY KEY (generation_id, input_asset_key, input_coverage_key),
    FOREIGN KEY (input_asset_key, input_coverage_key, input_generation_id)
        REFERENCES meta.asset_receipts(asset_key, coverage_key, generation_id),
    CHECK (generation_id <> input_generation_id)
);

CREATE INDEX IF NOT EXISTS asset_receipt_inputs_input_idx
    ON meta.asset_receipt_inputs(input_generation_id);

DO $private_objects$
DECLARE object_name text; object_row record;
BEGIN
    FOREACH object_name IN ARRAY ARRAY[
        'asset_publication_locks', 'asset_receipts',
        'asset_current_generations', 'asset_receipt_inputs'
    ] LOOP
        SELECT c.relowner, c.relkind INTO object_row
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'meta' AND c.relname = object_name;
        IF NOT FOUND OR object_row.relkind <> 'r'
            OR object_row.relowner <> (SELECT oid FROM pg_catalog.pg_roles
                WHERE rolname = current_user) THEN
            RAISE EXCEPTION 'meta.% must be a migration-owned ordinary table', object_name;
        END IF;
    END LOOP;
END
$private_objects$;

-- Register exact OIDs. Re-adoption after drop/recreate is allowed only after
-- its current pointer was invalidated.
DO $assets$
DECLARE
    item record;
    existing meta.asset_publication_locks%ROWTYPE;
    current_oid oid;
    current_kind "char";
BEGIN
    FOR item IN SELECT * FROM (VALUES
        ('analytics.house_elo_game', 'analytics', 'house_elo_game', 'r'::"char"),
        ('marts.house_elo_game', 'marts', 'house_elo_game', 'm'::"char")
    ) AS v(asset_key, relation_schema, relation_name, relation_kind) LOOP
        current_oid := pg_catalog.to_regclass(
            pg_catalog.format('%I.%I', item.relation_schema, item.relation_name));
        IF current_oid IS NULL THEN
            RAISE EXCEPTION 'required publication relation % is missing', item.asset_key;
        END IF;
        SELECT c.relkind INTO current_kind FROM pg_catalog.pg_class c WHERE c.oid = current_oid;
        IF current_kind <> item.relation_kind THEN
            RAISE EXCEPTION 'publication relation % has unexpected kind', item.asset_key;
        END IF;
        SELECT * INTO existing FROM meta.asset_publication_locks l
        WHERE l.asset_key = item.asset_key;
        IF NOT FOUND THEN
            INSERT INTO meta.asset_publication_locks(
                asset_key, relation_schema, relation_name, relation_oid, relation_kind
            ) VALUES (item.asset_key, item.relation_schema, item.relation_name,
                current_oid, item.relation_kind);
        ELSIF existing.relation_oid IS DISTINCT FROM current_oid
            OR existing.relation_schema IS DISTINCT FROM item.relation_schema
            OR existing.relation_name IS DISTINCT FROM item.relation_name
            OR existing.relation_kind IS DISTINCT FROM item.relation_kind THEN
            IF EXISTS (SELECT 1 FROM meta.asset_current_generations p
                WHERE p.asset_key = item.asset_key) THEN
                RAISE EXCEPTION 'cannot adopt replacement relation % while a current pointer exists',
                    item.asset_key;
            END IF;
            UPDATE meta.asset_publication_locks
            SET relation_schema = item.relation_schema, relation_name = item.relation_name,
                relation_oid = current_oid, relation_kind = item.relation_kind
            WHERE asset_key = item.asset_key;
        END IF;
    END LOOP;
    IF pg_catalog.to_regclass('analytics.house_elo_current') IS NULL
        OR (SELECT relkind FROM pg_catalog.pg_class
            WHERE oid = pg_catalog.to_regclass('analytics.house_elo_current')) <> 'r' THEN
        RAISE EXCEPTION 'required publication relation analytics.house_elo_current is missing or invalid';
    END IF;
END
$assets$;

COMMENT ON TABLE meta.asset_receipts IS
    'Append-only private publication evidence. Unversioned input_observations are observations, not dependency-generation proof.';
COMMENT ON COLUMN meta.asset_receipts.published_at IS
    'Wall-clock observation inside the publishing transaction; not an exact commit timestamp.';
COMMENT ON COLUMN meta.asset_receipts.committed_at IS
    'Reserved for a future verified commit-time source; migration 068 always leaves it NULL.';
COMMENT ON TABLE meta.asset_current_generations IS
    'One current complete receipt per asset and source-wide scope. Read data and pointer in one snapshot.';

CREATE OR REPLACE FUNCTION warehouse_publication.reject_append_only_mutation()
RETURNS trigger LANGUAGE plpgsql SET search_path = '' AS $function$
BEGIN
    RAISE EXCEPTION 'publication receipt evidence is append-only';
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_publication.record_house_elo_failure(
    p_operation_run_id uuid,
    p_source_generation_id uuid,
    p_mart_generation_id uuid,
    p_outcome text,
    p_error_category text
)
RETURNS TABLE (source_generation uuid, mart_generation uuid, replayed boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE
    run_row meta.operation_runs%ROWTYPE;
    source_receipt meta.asset_receipts%ROWTYPE;
    mart_receipt meta.asset_receipts%ROWTYPE;
    digest text;
    failure_coverage jsonb := '{"complete":false,"mode":"full","scope":"source-wide"}'::jsonb;
    observations jsonb := '{"publisher":"compute_house_elo","mode":"full"}'::jsonb;
BEGIN
    IF p_operation_run_id IS NULL OR p_source_generation_id IS NULL
        OR p_mart_generation_id IS NULL OR p_source_generation_id = p_mart_generation_id
        OR p_outcome NOT IN ('failed', 'deferred', 'partial', 'blocked')
        OR p_error_category IS DISTINCT FROM 'publication_failed' THEN
        RAISE EXCEPTION 'house Elo failure context is invalid' USING ERRCODE = '22023';
    END IF;
    digest := pg_catalog.md5(pg_catalog.jsonb_build_object(
        'operation_run_id', p_operation_run_id,
        'source_generation_id', p_source_generation_id,
        'mart_generation_id', p_mart_generation_id,
        'outcome', p_outcome,
        'error_category', p_error_category
    )::text);

    SELECT r.* INTO run_row FROM meta.operation_runs r
    WHERE r.operation_run_id = p_operation_run_id FOR UPDATE;
    IF NOT FOUND OR run_row.operation_kind <> 'compute'
        OR run_row.recorded_by <> session_user
        OR run_row.initiator <> 'compute_house_elo'
        OR run_row.requested_scope->>'mode' IS DISTINCT FROM 'full'
        OR run_row.requested_scope->>'start_season' IS DISTINCT FROM '1869'
        OR run_row.requested_scope->'assets' IS DISTINCT FROM
            '["analytics.house_elo_game", "marts.house_elo_game"]'::jsonb THEN
        RAISE EXCEPTION 'operation run does not belong to this house Elo publisher'
            USING ERRCODE = '22023';
    END IF;

    SELECT r.* INTO source_receipt FROM meta.asset_receipts r
    WHERE r.generation_id = p_source_generation_id;
    SELECT r.* INTO mart_receipt FROM meta.asset_receipts r
    WHERE r.generation_id = p_mart_generation_id;
    IF source_receipt.generation_id IS NOT NULL OR mart_receipt.generation_id IS NOT NULL THEN
        IF source_receipt.generation_id IS NULL OR mart_receipt.generation_id IS NULL
            OR source_receipt.operation_run_id <> p_operation_run_id
            OR mart_receipt.operation_run_id <> p_operation_run_id
            OR source_receipt.asset_key <> 'analytics.house_elo_game'
            OR mart_receipt.asset_key <> 'marts.house_elo_game'
            OR source_receipt.outcome <> p_outcome OR mart_receipt.outcome <> p_outcome
            OR source_receipt.error_summary <> p_error_category
            OR mart_receipt.error_summary <> p_error_category
            OR source_receipt.request_digest <> digest OR mart_receipt.request_digest <> digest THEN
            RAISE EXCEPTION 'generation ID already has different failure context'
                USING ERRCODE = '22023';
        END IF;
        RETURN QUERY SELECT p_source_generation_id, p_mart_generation_id, true;
        RETURN;
    END IF;
    IF run_row.outcome <> 'running' THEN
        RAISE EXCEPTION 'operation run is already terminal' USING ERRCODE = '22023';
    END IF;

    INSERT INTO meta.asset_receipts(
        generation_id, operation_run_id, asset_key, outcome, coverage,
        input_observations, row_delta, request_digest, error_summary
    ) VALUES
        (p_source_generation_id, p_operation_run_id, 'analytics.house_elo_game',
         p_outcome, failure_coverage, observations, '{}'::jsonb, digest, p_error_category),
        (p_mart_generation_id, p_operation_run_id, 'marts.house_elo_game',
         p_outcome, failure_coverage, observations, '{}'::jsonb, digest, p_error_category);

    RETURN QUERY SELECT p_source_generation_id, p_mart_generation_id, false;
END
$function$;

-- Bounded lifecycle wrappers keep the runtime role away from generic quota
-- helpers that can create arbitrary operation kinds/scopes.
CREATE OR REPLACE FUNCTION warehouse_publication.start_house_elo_run(
    p_operation_run_id uuid
)
RETURNS uuid
LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE run_row meta.operation_runs%ROWTYPE;
BEGIN
    IF p_operation_run_id IS NULL THEN
        RAISE EXCEPTION 'operation_run_id is required' USING ERRCODE = '22023';
    END IF;
    PERFORM warehouse_quota.start_operation_run(
        p_operation_run_id,
        'compute',
        'compute_house_elo',
        '{"assets":["analytics.house_elo_game","marts.house_elo_game"],"mode":"full","start_season":1869}'::jsonb,
        NULL,
        NULL
    );
    -- start_operation_run retains its advisory transaction lock. Validate the
    -- actor after an insert or replay so another login cannot adopt the UUID.
    SELECT r.* INTO STRICT run_row FROM meta.operation_runs r
    WHERE r.operation_run_id = p_operation_run_id;
    IF run_row.recorded_by <> session_user
        OR run_row.operation_kind <> 'compute'
        OR run_row.initiator <> 'compute_house_elo'
        OR run_row.requested_scope IS DISTINCT FROM
            '{"assets":["analytics.house_elo_game","marts.house_elo_game"],"mode":"full","start_season":1869}'::jsonb
        OR run_row.plan_digest IS NOT NULL OR run_row.code_revision IS NOT NULL THEN
        RAISE EXCEPTION 'operation run does not belong to this house Elo publisher'
            USING ERRCODE = '22023';
    END IF;
    RETURN p_operation_run_id;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_publication.finish_house_elo_run(
    p_operation_run_id uuid,
    p_outcome text
)
RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE run_row meta.operation_runs%ROWTYPE;
BEGIN
    IF p_operation_run_id IS NULL OR p_outcome NOT IN ('succeeded', 'failed') THEN
        RAISE EXCEPTION 'house Elo operation outcome is invalid' USING ERRCODE = '22023';
    END IF;
    SELECT r.* INTO run_row FROM meta.operation_runs r
    WHERE r.operation_run_id = p_operation_run_id FOR UPDATE;
    IF NOT FOUND OR run_row.recorded_by <> session_user
        OR run_row.operation_kind <> 'compute'
        OR run_row.initiator <> 'compute_house_elo'
        OR run_row.requested_scope IS DISTINCT FROM
            '{"assets":["analytics.house_elo_game","marts.house_elo_game"],"mode":"full","start_season":1869}'::jsonb
        OR run_row.plan_digest IS NOT NULL OR run_row.code_revision IS NOT NULL THEN
        RAISE EXCEPTION 'operation run does not belong to this house Elo publisher'
            USING ERRCODE = '22023';
    END IF;
    RETURN warehouse_quota.finish_operation_run(
        p_operation_run_id,
        p_outcome,
        CASE WHEN p_outcome = 'failed' THEN 'publication_failed' ELSE NULL END
    );
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_publication.publish_house_elo(
    p_operation_run_id uuid,
    p_source_generation_id uuid,
    p_mart_generation_id uuid,
    p_expected_source_generation_id uuid,
    p_expected_mart_generation_id uuid,
    p_input_digest text,
    p_rows jsonb,
    p_snapshot jsonb,
    p_start_season bigint,
    p_end_season bigint,
    p_excluded_game_ids bigint[]
)
RETURNS TABLE (
    source_generation uuid,
    mart_generation uuid,
    replayed boolean,
    source_rows bigint,
    mart_rows bigint
)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE
    reviewed_exclusions constant bigint[] := ARRAY[
        401540999,401545766,401545768,401545773,401545780,401545781,
        401549719,401550299,401552878,401552884,401640992,401833535,
        401866625
    ]::bigint[];
    run_row meta.operation_runs%ROWTYPE;
    source_receipt meta.asset_receipts%ROWTYPE;
    mart_receipt meta.asset_receipts%ROWTYPE;
    request_digest_value text;
    fresh_input_digest text;
    actual_source_generation uuid;
    actual_mart_generation uuid;
    old_source_rows bigint;
    old_mart_rows bigint;
    eligible_scheduled_rows bigint;
    eligible_completed_rows bigint;
    parsed_source_rows bigint;
    parsed_snapshot_rows bigint;
    refreshed_mart_rows bigint;
    mismatch_rows bigint;
    max_season bigint;
    publication_time timestamptz;
    publication_outcome text;
    source_coverage jsonb;
    mart_coverage jsonb;
    observations jsonb;
BEGIN
    IF p_operation_run_id IS NULL OR p_source_generation_id IS NULL
        OR p_mart_generation_id IS NULL OR p_source_generation_id = p_mart_generation_id
        OR p_input_digest IS NULL OR p_input_digest !~ '^[0-9a-f]{32}$'
        OR p_rows IS NULL OR pg_catalog.jsonb_typeof(p_rows) <> 'array'
        OR p_snapshot IS NULL OR pg_catalog.jsonb_typeof(p_snapshot) <> 'array'
        OR p_start_season IS DISTINCT FROM 1869 OR p_end_season IS NULL
        OR p_end_season < p_start_season
        OR p_excluded_game_ids IS DISTINCT FROM reviewed_exclusions THEN
        RAISE EXCEPTION 'house Elo publication context is invalid' USING ERRCODE = '22023';
    END IF;

    request_digest_value := pg_catalog.md5(pg_catalog.jsonb_build_object(
        'protocol', 'house-elo-full-v1',
        'operation_run_id', p_operation_run_id,
        'source_generation_id', p_source_generation_id,
        'mart_generation_id', p_mart_generation_id,
        'expected_source_generation_id', p_expected_source_generation_id,
        'expected_mart_generation_id', p_expected_mart_generation_id,
        'input_digest', p_input_digest, 'rows', p_rows, 'snapshot', p_snapshot,
        'start_season', p_start_season, 'end_season', p_end_season,
        'excluded_game_ids', pg_catalog.to_jsonb(p_excluded_game_ids)
    )::text);

    SELECT r.* INTO run_row FROM meta.operation_runs r
    WHERE r.operation_run_id = p_operation_run_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'operation run is not configured' USING ERRCODE = '22023';
    END IF;
    IF run_row.operation_kind <> 'compute' OR run_row.recorded_by <> session_user
        OR run_row.initiator <> 'compute_house_elo'
        OR run_row.requested_scope->>'mode' IS DISTINCT FROM 'full'
        OR run_row.requested_scope->>'start_season' IS DISTINCT FROM '1869'
        OR run_row.requested_scope->'assets' IS DISTINCT FROM
            '["analytics.house_elo_game", "marts.house_elo_game"]'::jsonb THEN
        RAISE EXCEPTION 'operation run does not belong to this house Elo publisher'
            USING ERRCODE = '22023';
    END IF;

    -- A matching immutable pair is a pure lookup. It never repoints after a
    -- later correction and is allowed after the operation run became terminal.
    SELECT r.* INTO source_receipt FROM meta.asset_receipts r
    WHERE r.generation_id = p_source_generation_id;
    SELECT r.* INTO mart_receipt FROM meta.asset_receipts r
    WHERE r.generation_id = p_mart_generation_id;
    IF source_receipt.generation_id IS NOT NULL OR mart_receipt.generation_id IS NOT NULL THEN
        IF source_receipt.generation_id IS NULL OR mart_receipt.generation_id IS NULL
            OR source_receipt.operation_run_id <> p_operation_run_id
            OR mart_receipt.operation_run_id <> p_operation_run_id
            OR source_receipt.asset_key <> 'analytics.house_elo_game'
            OR mart_receipt.asset_key <> 'marts.house_elo_game'
            OR source_receipt.outcome NOT IN ('succeeded', 'expected_no_data')
            OR mart_receipt.outcome <> source_receipt.outcome
            OR source_receipt.request_digest <> request_digest_value
            OR mart_receipt.request_digest <> request_digest_value
            OR NOT EXISTS (
                SELECT 1 FROM meta.asset_receipt_inputs i
                WHERE i.generation_id = p_mart_generation_id
                  AND i.input_asset_key = 'analytics.house_elo_game'
                  AND i.input_coverage_key = 'source-wide'
                  AND i.input_generation_id = p_source_generation_id
            ) THEN
            RAISE EXCEPTION 'generation ID already has different publication context'
                USING ERRCODE = '22023';
        END IF;
        RETURN QUERY SELECT p_source_generation_id, p_mart_generation_id, true,
            (source_receipt.row_delta->>'published_rows')::bigint,
            (mart_receipt.row_delta->>'published_rows')::bigint;
        RETURN;
    END IF;
    IF run_row.outcome <> 'running' THEN
        RAISE EXCEPTION 'operation run is already terminal' USING ERRCODE = '22023';
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed'
        OR current_setting('session_replication_role') <> 'origin'
        OR COALESCE(current_setting('event_triggers', true), 'on') <> 'on' THEN
        RAISE EXCEPTION 'house Elo publication requires a fresh READ COMMITTED transaction'
            USING ERRCODE = '25001';
    END IF;

    -- Lock order: operation run -> unversioned input -> targets -> asset rows.
    LOCK TABLE core.games IN SHARE MODE;
    LOCK TABLE analytics.house_elo_game, analytics.house_elo_current IN EXCLUSIVE MODE;
    -- PostgreSQL LOCK TABLE excludes materialized views. The ordinary REFRESH
    -- below takes its AccessExclusiveLock; source/asset serialization makes it
    -- the final target lock in the protocol.
    PERFORM 1 FROM meta.asset_publication_locks l
    WHERE l.asset_key IN ('analytics.house_elo_game', 'marts.house_elo_game')
    ORDER BY l.asset_key FOR UPDATE;

    -- Refuse to install future proof if any invalidation guard was altered.
    IF EXISTS (
        SELECT 1 FROM meta.asset_publication_locks l
        WHERE l.relation_oid <> pg_catalog.to_regclass(
            pg_catalog.format('%I.%I', l.relation_schema, l.relation_name))
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_trigger t
        WHERE t.tgrelid = pg_catalog.to_regclass('analytics.house_elo_game')
          AND t.tgname = 'invalidate_house_elo_game_pointer'
          AND t.tgfoid = pg_catalog.to_regprocedure(
              'warehouse_publication.invalidate_house_elo_pointers()')
          AND t.tgtype = 62 AND t.tgenabled IN ('O', 'A') AND NOT t.tgisinternal
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_trigger t
        WHERE t.tgrelid = pg_catalog.to_regclass('analytics.house_elo_current')
          AND t.tgname = 'invalidate_house_elo_current_pointer'
          AND t.tgfoid = pg_catalog.to_regprocedure(
              'warehouse_publication.invalidate_house_elo_pointers()')
          AND t.tgtype = 62 AND t.tgenabled IN ('O', 'A') AND NOT t.tgisinternal
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_event_trigger e
        WHERE e.evtname = 'warehouse_publication_invalidate_ddl'
          AND e.evtevent = 'ddl_command_end' AND e.evtenabled IN ('O', 'A')
          AND e.evtfoid = pg_catalog.to_regprocedure(
              'warehouse_publication.invalidate_asset_pointer_ddl()')
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_event_trigger e
        WHERE e.evtname = 'warehouse_publication_invalidate_drop'
          AND e.evtevent = 'sql_drop' AND e.evtenabled IN ('O', 'A')
          AND e.evtfoid = pg_catalog.to_regprocedure(
              'warehouse_publication.invalidate_asset_pointer_drop()')
    ) THEN
        RAISE EXCEPTION 'house Elo publication invalidation guards are missing or stale';
    END IF;

    SELECT p.generation_id INTO actual_source_generation
    FROM meta.asset_current_generations p
    WHERE p.asset_key = 'analytics.house_elo_game' AND p.coverage_key = 'source-wide';
    SELECT p.generation_id INTO actual_mart_generation
    FROM meta.asset_current_generations p
    WHERE p.asset_key = 'marts.house_elo_game' AND p.coverage_key = 'source-wide';
    IF actual_source_generation IS DISTINCT FROM p_expected_source_generation_id
        OR actual_mart_generation IS DISTINCT FROM p_expected_mart_generation_id THEN
        RAISE EXCEPTION 'house Elo current generation changed during preparation'
            USING ERRCODE = '40001';
    END IF;

    PERFORM pg_catalog.set_config('TimeZone', 'UTC', true);
    SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
        pg_catalog.md5(pg_catalog.to_jsonb(g)::text), '' ORDER BY g.id), ''))
    INTO fresh_input_digest FROM core.games g;
    IF fresh_input_digest IS DISTINCT FROM p_input_digest THEN
        RAISE EXCEPTION 'core.games changed during house Elo preparation'
            USING ERRCODE = '40001';
    END IF;

    -- Excluding a reviewed postponed original is safe only while its reviewed
    -- replacement is present and carries the same non-NULL game identity.
    -- This mirrors select_canonical_game_rows() without changing raw rows.
    IF EXISTS (
        SELECT 1
        FROM (VALUES
            (401866625::bigint, 401917058::bigint),
            (401549719::bigint, 401611307::bigint),
            (401550299::bigint, 401611308::bigint),
            (401552878::bigint, 401611306::bigint),
            (401552884::bigint, 401612659::bigint)
        ) mapping(original_id, replacement_id)
        JOIN core.games original_game ON original_game.id = mapping.original_id
        LEFT JOIN core.games replacement_game ON replacement_game.id = mapping.replacement_id
        WHERE replacement_game.id IS NULL
           OR original_game.season IS NULL
           OR original_game.home_team IS NULL
           OR original_game.away_team IS NULL
           OR original_game.season IS DISTINCT FROM replacement_game.season
           OR original_game.home_team IS DISTINCT FROM replacement_game.home_team
           OR original_game.away_team IS DISTINCT FROM replacement_game.away_team
    ) THEN
        RAISE EXCEPTION 'reviewed superseded game is missing a matching replacement'
            USING ERRCODE = '22023';
    END IF;

    IF EXISTS (
        SELECT 1 FROM core.games g WHERE g.id <> ALL(reviewed_exclusions)
          AND (g.season IS NULL OR g.season < p_start_season OR g.season > p_end_season)
    ) THEN
        RAISE EXCEPTION 'house Elo publication scope does not cover every eligible game'
            USING ERRCODE = '22023';
    END IF;
    SELECT max(g.season), count(*) INTO max_season, eligible_scheduled_rows
    FROM core.games g WHERE g.id <> ALL(reviewed_exclusions);
    IF max_season IS NOT NULL AND p_end_season < max_season THEN
        RAISE EXCEPTION 'house Elo publication end season does not cover current schedule'
            USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1 FROM core.games g
        WHERE g.id <> ALL(reviewed_exclusions) AND g.completed = true
          AND (g.home_points IS NULL OR g.away_points IS NULL
               OR g.home_team IS NULL OR g.away_team IS NULL)
    ) THEN
        RAISE EXCEPTION 'completed eligible games require teams and final scores'
            USING ERRCODE = '22023';
    END IF;
    SELECT count(*) INTO eligible_completed_rows FROM core.games g
    WHERE g.id <> ALL(reviewed_exclusions) AND g.completed = true
      AND g.home_points IS NOT NULL AND g.away_points IS NOT NULL;

    IF EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(p_rows) e(value)
        WHERE pg_catalog.jsonb_typeof(value) <> 'object'
           OR NOT value ?& ARRAY[
                'game_id','season','week','season_type','start_date','neutral_site',
                'home_team','away_team','home_pregame_elo','away_pregame_elo',
                'home_postgame_elo','away_postgame_elo','home_win_prob',
                'expected_home_margin','actual_home_margin','mov_multiplier',
                'cfbd_home_pregame_elo','cfbd_away_pregame_elo'
              ]
           OR value - ARRAY[
                'game_id','season','week','season_type','start_date','neutral_site',
                'home_team','away_team','home_pregame_elo','away_pregame_elo',
                'home_postgame_elo','away_postgame_elo','home_win_prob',
                'expected_home_margin','actual_home_margin','mov_multiplier',
                'cfbd_home_pregame_elo','cfbd_away_pregame_elo'
              ] <> '{}'::jsonb
    ) THEN
        RAISE EXCEPTION 'house Elo row payload has an invalid object shape'
            USING ERRCODE = '22023';
    END IF;

    SELECT count(*), count(*) - count(DISTINCT r.game_id)
    INTO parsed_source_rows, mismatch_rows
    FROM pg_catalog.jsonb_populate_recordset(NULL::analytics.house_elo_game, p_rows) r;
    IF mismatch_rows <> 0 OR parsed_source_rows <> eligible_completed_rows THEN
        RAISE EXCEPTION 'house Elo row payload has duplicate or missing game IDs'
            USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_populate_recordset(NULL::analytics.house_elo_game, p_rows) r
        WHERE r.game_id IS NULL OR r.season IS NULL OR r.neutral_site IS NULL
           OR r.home_team IS NULL OR length(pg_catalog.btrim(r.home_team)) = 0
           OR r.away_team IS NULL OR length(pg_catalog.btrim(r.away_team)) = 0
           OR r.home_pregame_elo IS NULL OR r.away_pregame_elo IS NULL
           OR r.home_postgame_elo IS NULL OR r.away_postgame_elo IS NULL
           OR r.home_win_prob IS NULL OR r.home_win_prob < 0 OR r.home_win_prob > 1
           OR r.expected_home_margin IS NULL OR r.actual_home_margin IS NULL
           OR r.mov_multiplier IS NULL OR r.mov_multiplier < 0
           OR r.home_pregame_elo::text IN ('NaN','Infinity','-Infinity')
           OR r.away_pregame_elo::text IN ('NaN','Infinity','-Infinity')
           OR r.home_postgame_elo::text IN ('NaN','Infinity','-Infinity')
           OR r.away_postgame_elo::text IN ('NaN','Infinity','-Infinity')
           OR r.home_win_prob::text IN ('NaN','Infinity','-Infinity')
           OR r.expected_home_margin::text IN ('NaN','Infinity','-Infinity')
           OR r.mov_multiplier::text IN ('NaN','Infinity','-Infinity')
    ) THEN
        RAISE EXCEPTION 'house Elo row payload contains invalid values'
            USING ERRCODE = '22023';
    END IF;

    SELECT count(*) INTO mismatch_rows
    FROM pg_catalog.jsonb_populate_recordset(NULL::analytics.house_elo_game, p_rows) r
    FULL JOIN (
        SELECT * FROM core.games source_game
        WHERE source_game.id <> ALL(reviewed_exclusions)
          AND source_game.completed = true
          AND source_game.home_points IS NOT NULL
          AND source_game.away_points IS NOT NULL
    ) g ON g.id = r.game_id
    WHERE r.game_id IS NULL OR g.id IS NULL
       OR r.season IS DISTINCT FROM g.season OR r.week IS DISTINCT FROM g.week
       OR r.season_type IS DISTINCT FROM g.season_type
       OR r.start_date IS DISTINCT FROM g.start_date
       OR r.neutral_site IS DISTINCT FROM COALESCE(g.neutral_site, false)
       OR r.home_team IS DISTINCT FROM g.home_team OR r.away_team IS DISTINCT FROM g.away_team
       OR r.actual_home_margin IS DISTINCT FROM (g.home_points - g.away_points)
       OR r.cfbd_home_pregame_elo IS DISTINCT FROM g.home_pregame_elo
       OR r.cfbd_away_pregame_elo IS DISTINCT FROM g.away_pregame_elo;
    IF mismatch_rows <> 0 THEN
        RAISE EXCEPTION 'house Elo row payload does not match the locked source rows'
            USING ERRCODE = '22023';
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(p_snapshot) e(value)
        WHERE pg_catalog.jsonb_typeof(value) <> 'object'
           OR NOT value ?& ARRAY[
                'team','season','rating','games_played','last_game_id',
                'last_game_date','low_confidence'
              ]
           OR value - ARRAY[
                'team','season','rating','games_played','last_game_id',
                'last_game_date','low_confidence'
              ] <> '{}'::jsonb
    ) THEN
        RAISE EXCEPTION 'house Elo snapshot payload has an invalid object shape'
            USING ERRCODE = '22023';
    END IF;
    SELECT count(*), count(*) - count(DISTINCT s.team)
    INTO parsed_snapshot_rows, mismatch_rows
    FROM pg_catalog.jsonb_to_recordset(p_snapshot) AS s(
        team text, season bigint, rating numeric, games_played bigint,
        last_game_id bigint, last_game_date timestamptz, low_confidence boolean
    );
    IF mismatch_rows <> 0 THEN
        RAISE EXCEPTION 'house Elo snapshot payload has duplicate teams'
            USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_to_recordset(p_snapshot) AS s(
            team text, season bigint, rating numeric, games_played bigint,
            last_game_id bigint, last_game_date timestamptz, low_confidence boolean
        )
        WHERE s.team IS NULL OR length(pg_catalog.btrim(s.team)) = 0
           OR s.season IS NULL OR s.rating IS NULL
           OR s.rating::text IN ('NaN','Infinity','-Infinity')
           OR s.games_played IS NULL OR s.games_played < 0
           OR s.last_game_id IS NULL OR s.low_confidence IS NULL
    ) THEN
        RAISE EXCEPTION 'house Elo snapshot payload contains invalid values'
            USING ERRCODE = '22023';
    END IF;

    WITH payload_teams AS (
        SELECT s.team, s.season
        FROM pg_catalog.jsonb_to_recordset(p_snapshot) AS s(
            team text, season bigint, rating numeric, games_played bigint,
            last_game_id bigint, last_game_date timestamptz, low_confidence boolean
        )
    ), source_teams AS (
        SELECT team, max(season) AS season FROM (
            SELECT r.home_team AS team, r.season
            FROM pg_catalog.jsonb_populate_recordset(NULL::analytics.house_elo_game, p_rows) r
            UNION ALL
            SELECT r.away_team AS team, r.season
            FROM pg_catalog.jsonb_populate_recordset(NULL::analytics.house_elo_game, p_rows) r
        ) sides GROUP BY team
    )
    SELECT count(*) INTO mismatch_rows
    FROM payload_teams p FULL JOIN source_teams s USING (team)
    WHERE p.team IS NULL OR s.team IS NULL OR p.season IS DISTINCT FROM s.season;
    IF mismatch_rows <> 0 THEN
        RAISE EXCEPTION 'house Elo snapshot does not cover the computed team set'
            USING ERRCODE = '22023';
    END IF;

    SELECT count(*) INTO old_source_rows FROM analytics.house_elo_game;
    SELECT count(*) INTO old_mart_rows FROM marts.house_elo_game;

    DELETE FROM analytics.house_elo_game;
    INSERT INTO analytics.house_elo_game(
        game_id, season, week, season_type, start_date, neutral_site,
        home_team, away_team, home_pregame_elo, away_pregame_elo,
        home_postgame_elo, away_postgame_elo, home_win_prob,
        expected_home_margin, actual_home_margin, mov_multiplier,
        cfbd_home_pregame_elo, cfbd_away_pregame_elo
    )
    SELECT game_id, season, week, season_type, start_date, neutral_site,
        home_team, away_team, home_pregame_elo, away_pregame_elo,
        home_postgame_elo, away_postgame_elo, home_win_prob,
        expected_home_margin, actual_home_margin, mov_multiplier,
        cfbd_home_pregame_elo, cfbd_away_pregame_elo
    FROM pg_catalog.jsonb_populate_recordset(NULL::analytics.house_elo_game, p_rows);

    DELETE FROM analytics.house_elo_current;
    INSERT INTO analytics.house_elo_current(
        team, season, rating, games_played, last_game_id,
        last_game_date, low_confidence, updated_at
    )
    SELECT s.team, s.season, s.rating, s.games_played, s.last_game_id,
        s.last_game_date, s.low_confidence, pg_catalog.clock_timestamp()
    FROM pg_catalog.jsonb_to_recordset(p_snapshot) AS s(
        team text, season bigint, rating numeric, games_played bigint,
        last_game_id bigint, last_game_date timestamptz, low_confidence boolean
    );

    REFRESH MATERIALIZED VIEW marts.house_elo_game;
    -- REFRESH now retains AccessExclusiveLock through commit. Recheck the
    -- registered identity to close the pre-refresh DROP/rename/recreate race.
    IF (SELECT l.relation_oid FROM meta.asset_publication_locks l
            WHERE l.asset_key = 'marts.house_elo_game')
        IS DISTINCT FROM pg_catalog.to_regclass('marts.house_elo_game') THEN
        RAISE EXCEPTION 'house Elo mart relation changed during publication'
            USING ERRCODE = '40001';
    END IF;
    SELECT count(*) INTO refreshed_mart_rows FROM marts.house_elo_game;
    IF refreshed_mart_rows <> parsed_source_rows THEN
        RAISE EXCEPTION 'house Elo mart row count differs from its source';
    END IF;

    publication_time := pg_catalog.clock_timestamp();
    publication_outcome := CASE WHEN eligible_completed_rows = 0
        THEN 'expected_no_data' ELSE 'succeeded' END;
    source_coverage := pg_catalog.jsonb_build_object(
        'complete', true, 'mode', 'full', 'scope', 'source-wide',
        'start_season', p_start_season, 'end_season', p_end_season,
        'eligible_scheduled_games', eligible_scheduled_rows,
        'eligible_completed_games', eligible_completed_rows,
        'excluded_game_ids', pg_catalog.to_jsonb(p_excluded_game_ids));
    mart_coverage := source_coverage || pg_catalog.jsonb_build_object(
        'input_generation_id', p_source_generation_id);
    observations := pg_catalog.jsonb_build_object(
        'publisher', 'compute_house_elo', 'mode', 'full',
        'input_asset', 'core.games', 'input_digest', p_input_digest,
        'expected_source_generation_id', p_expected_source_generation_id,
        'expected_mart_generation_id', p_expected_mart_generation_id,
        'excluded_game_ids', pg_catalog.to_jsonb(p_excluded_game_ids));

    INSERT INTO meta.asset_receipts(
        generation_id, operation_run_id, asset_key, outcome, coverage,
        source_watermark, input_observations, row_delta, request_digest, published_at
    ) VALUES (
        p_source_generation_id, p_operation_run_id, 'analytics.house_elo_game',
        publication_outcome, source_coverage, p_input_digest, observations,
        pg_catalog.jsonb_build_object('previous_rows', old_source_rows,
            'published_rows', parsed_source_rows, 'snapshot_rows', parsed_snapshot_rows),
        request_digest_value, publication_time
    ), (
        p_mart_generation_id, p_operation_run_id, 'marts.house_elo_game',
        publication_outcome, mart_coverage, p_input_digest, observations,
        pg_catalog.jsonb_build_object('observed_previous_rows', old_mart_rows,
            'published_rows', refreshed_mart_rows), request_digest_value, publication_time
    );

    INSERT INTO meta.asset_receipt_inputs(
        generation_id, input_asset_key, input_coverage_key, input_generation_id
    ) VALUES (p_mart_generation_id, 'analytics.house_elo_game', 'source-wide',
        p_source_generation_id);

    INSERT INTO meta.asset_current_generations(asset_key, coverage_key, generation_id)
    VALUES
        ('analytics.house_elo_game', 'source-wide', p_source_generation_id),
        ('marts.house_elo_game', 'source-wide', p_mart_generation_id)
    ON CONFLICT (asset_key, coverage_key) DO UPDATE
    SET generation_id = EXCLUDED.generation_id;

    RETURN QUERY SELECT p_source_generation_id, p_mart_generation_id, false,
        parsed_source_rows, refreshed_mart_rows;
END
$function$;

DROP TRIGGER IF EXISTS reject_asset_receipt_mutation ON meta.asset_receipts;
CREATE TRIGGER reject_asset_receipt_mutation
    BEFORE UPDATE OR DELETE OR TRUNCATE ON meta.asset_receipts
    FOR EACH STATEMENT EXECUTE FUNCTION warehouse_publication.reject_append_only_mutation();
DROP TRIGGER IF EXISTS reject_asset_receipt_input_mutation ON meta.asset_receipt_inputs;
CREATE TRIGGER reject_asset_receipt_input_mutation
    BEFORE UPDATE OR DELETE OR TRUNCATE ON meta.asset_receipt_inputs
    FOR EACH STATEMENT EXECUTE FUNCTION warehouse_publication.reject_append_only_mutation();

CREATE OR REPLACE FUNCTION warehouse_publication.guard_current_generation()
RETURNS trigger LANGUAGE plpgsql SET search_path = '' AS $function$
DECLARE receipt meta.asset_receipts%ROWTYPE;
BEGIN
    SELECT r.* INTO receipt FROM meta.asset_receipts r
    WHERE r.asset_key = NEW.asset_key AND r.coverage_key = NEW.coverage_key
      AND r.generation_id = NEW.generation_id;
    IF NOT FOUND OR receipt.outcome NOT IN ('succeeded', 'expected_no_data')
        OR receipt.coverage->'complete' <> 'true'::jsonb OR receipt.published_at IS NULL THEN
        RAISE EXCEPTION 'current publication pointer requires a complete successful receipt';
    END IF;
    RETURN NEW;
END
$function$;

DROP TRIGGER IF EXISTS guard_asset_current_generation ON meta.asset_current_generations;
CREATE TRIGGER guard_asset_current_generation BEFORE INSERT OR UPDATE
    ON meta.asset_current_generations FOR EACH ROW
    EXECUTE FUNCTION warehouse_publication.guard_current_generation();

CREATE OR REPLACE FUNCTION warehouse_publication.guard_receipt_input()
RETURNS trigger LANGUAGE plpgsql SET search_path = '' AS $function$
DECLARE output_ok boolean; input_ok boolean;
BEGIN
    SELECT r.outcome IN ('succeeded', 'expected_no_data')
        AND r.coverage->'complete' = 'true'::jsonb INTO output_ok
    FROM meta.asset_receipts r WHERE r.generation_id = NEW.generation_id;
    SELECT r.outcome IN ('succeeded', 'expected_no_data')
        AND r.coverage->'complete' = 'true'::jsonb INTO input_ok
    FROM meta.asset_receipts r WHERE r.asset_key = NEW.input_asset_key
      AND r.coverage_key = NEW.input_coverage_key
      AND r.generation_id = NEW.input_generation_id;
    IF NOT COALESCE(output_ok, false) OR NOT COALESCE(input_ok, false) THEN
        RAISE EXCEPTION 'receipt dependency edges require complete successful receipts';
    END IF;
    RETURN NEW;
END
$function$;

DROP TRIGGER IF EXISTS guard_asset_receipt_input ON meta.asset_receipt_inputs;
CREATE TRIGGER guard_asset_receipt_input BEFORE INSERT ON meta.asset_receipt_inputs
    FOR EACH ROW EXECUTE FUNCTION warehouse_publication.guard_receipt_input();

-- Legacy writes invalidate both proofs before any target changes.
CREATE OR REPLACE FUNCTION warehouse_publication.invalidate_house_elo_pointers()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
BEGIN
    DELETE FROM meta.asset_current_generations p
    WHERE p.asset_key IN ('analytics.house_elo_game', 'marts.house_elo_game');
    RETURN NULL;
END
$function$;

DROP TRIGGER IF EXISTS invalidate_house_elo_game_pointer ON analytics.house_elo_game;
CREATE TRIGGER invalidate_house_elo_game_pointer
    BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON analytics.house_elo_game
    FOR EACH STATEMENT EXECUTE FUNCTION warehouse_publication.invalidate_house_elo_pointers();
DROP TRIGGER IF EXISTS invalidate_house_elo_current_pointer ON analytics.house_elo_current;
CREATE TRIGGER invalidate_house_elo_current_pointer
    BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON analytics.house_elo_current
    FOR EACH STATEMENT EXECUTE FUNCTION warehouse_publication.invalidate_house_elo_pointers();

-- DDL invalidation matches stored OIDs and canonical names. It covers relation
-- rename/schema move/drop/recreate, direct mart refresh, and relation-trigger
-- changes. PostgreSQL does not fire event triggers for DDL targeting event
-- triggers themselves; those are trusted-administrator operations. The RPC
-- refuses to publish while either event trigger is missing or disabled.
CREATE OR REPLACE FUNCTION warehouse_publication.invalidate_asset_pointer_ddl()
RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE
    command record;
    source_changed boolean := false;
    mart_changed boolean := false;
    protocol_changed boolean := false;
BEGIN
    FOR command IN SELECT * FROM pg_catalog.pg_event_trigger_ddl_commands() LOOP
        IF command.object_identity IN (
            'warehouse_publication_invalidate_ddl',
            'warehouse_publication_invalidate_drop'
        ) OR command.object_identity LIKE '%invalidate_house_elo_%pointer%'
          OR command.object_identity LIKE 'warehouse_publication.invalidate_%' THEN
            protocol_changed := true;
        END IF;
        IF EXISTS (
            SELECT 1 FROM meta.asset_publication_locks l
            WHERE l.asset_key = 'analytics.house_elo_game'
              AND (command.objid = l.relation_oid OR command.objid =
                    pg_catalog.to_regclass(pg_catalog.format('%I.%I',
                        l.relation_schema, l.relation_name)))
        ) OR command.objid = pg_catalog.to_regclass('analytics.house_elo_current')
          OR EXISTS (
              SELECT 1 FROM pg_catalog.pg_trigger t
              WHERE t.tgrelid = command.objid
                AND t.tgname = 'invalidate_house_elo_current_pointer'
                AND t.tgfoid = pg_catalog.to_regprocedure(
                    'warehouse_publication.invalidate_house_elo_pointers()')
          ) THEN
            source_changed := true;
        END IF;
        IF EXISTS (
            SELECT 1 FROM meta.asset_publication_locks l
            WHERE l.asset_key = 'marts.house_elo_game'
              AND (command.objid = l.relation_oid OR command.objid =
                    pg_catalog.to_regclass(pg_catalog.format('%I.%I',
                        l.relation_schema, l.relation_name)))
        ) THEN
            mart_changed := true;
        END IF;
    END LOOP;
    -- ALTER SCHEMA ... RENAME identifies only the namespace. Resolve the
    -- canonical bindings after the command to cover whole-schema changes.
    IF EXISTS (
        SELECT 1 FROM meta.asset_publication_locks l
        WHERE l.asset_key = 'analytics.house_elo_game'
          AND l.relation_oid IS DISTINCT FROM pg_catalog.to_regclass(
              pg_catalog.format('%I.%I', l.relation_schema, l.relation_name))
    ) OR pg_catalog.to_regclass('analytics.house_elo_current') IS NULL THEN
        source_changed := true;
    END IF;
    IF EXISTS (
        SELECT 1 FROM meta.asset_publication_locks l
        WHERE l.asset_key = 'marts.house_elo_game'
          AND l.relation_oid IS DISTINCT FROM pg_catalog.to_regclass(
              pg_catalog.format('%I.%I', l.relation_schema, l.relation_name))
    ) THEN
        mart_changed := true;
    END IF;
    IF source_changed OR protocol_changed THEN
        DELETE FROM meta.asset_current_generations p
        WHERE p.asset_key IN ('analytics.house_elo_game', 'marts.house_elo_game');
    ELSIF mart_changed THEN
        DELETE FROM meta.asset_current_generations p WHERE p.asset_key = 'marts.house_elo_game';
    END IF;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_publication.invalidate_asset_pointer_drop()
RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE
    dropped record;
    source_changed boolean := false;
    mart_changed boolean := false;
    protocol_changed boolean := false;
BEGIN
    FOR dropped IN SELECT * FROM pg_catalog.pg_event_trigger_dropped_objects() LOOP
        IF dropped.object_identity IN (
            'warehouse_publication_invalidate_ddl',
            'warehouse_publication_invalidate_drop'
        ) OR 'invalidate_house_elo_game_pointer' = ANY(dropped.address_names)
          OR 'invalidate_house_elo_current_pointer' = ANY(dropped.address_names)
          OR dropped.object_identity LIKE 'warehouse_publication.invalidate_%' THEN
            protocol_changed := true;
        END IF;
        IF EXISTS (
            SELECT 1 FROM meta.asset_publication_locks l
            WHERE l.asset_key = 'analytics.house_elo_game'
              AND (dropped.objid = l.relation_oid OR
                   (dropped.address_names[1] = l.relation_schema
                    AND dropped.address_names[2] = l.relation_name))
        ) OR (dropped.address_names[1] = 'analytics'
              AND dropped.address_names[2] = 'house_elo_current') THEN
            source_changed := true;
        END IF;
        IF EXISTS (
            SELECT 1 FROM meta.asset_publication_locks l
            WHERE l.asset_key = 'marts.house_elo_game'
              AND (dropped.objid = l.relation_oid OR
                   (dropped.address_names[1] = l.relation_schema
                    AND dropped.address_names[2] = l.relation_name))
        ) THEN
            mart_changed := true;
        END IF;
    END LOOP;
    IF EXISTS (
        SELECT 1 FROM meta.asset_publication_locks l
        WHERE l.asset_key = 'analytics.house_elo_game'
          AND l.relation_oid IS DISTINCT FROM pg_catalog.to_regclass(
              pg_catalog.format('%I.%I', l.relation_schema, l.relation_name))
    ) OR pg_catalog.to_regclass('analytics.house_elo_current') IS NULL THEN
        source_changed := true;
    END IF;
    IF EXISTS (
        SELECT 1 FROM meta.asset_publication_locks l
        WHERE l.asset_key = 'marts.house_elo_game'
          AND l.relation_oid IS DISTINCT FROM pg_catalog.to_regclass(
              pg_catalog.format('%I.%I', l.relation_schema, l.relation_name))
    ) THEN
        mart_changed := true;
    END IF;
    IF source_changed OR protocol_changed THEN
        DELETE FROM meta.asset_current_generations p
        WHERE p.asset_key IN ('analytics.house_elo_game', 'marts.house_elo_game');
    ELSIF mart_changed THEN
        DELETE FROM meta.asset_current_generations p WHERE p.asset_key = 'marts.house_elo_game';
    END IF;
END
$function$;

DROP EVENT TRIGGER IF EXISTS warehouse_publication_invalidate_ddl;
CREATE EVENT TRIGGER warehouse_publication_invalidate_ddl ON ddl_command_end
    EXECUTE FUNCTION warehouse_publication.invalidate_asset_pointer_ddl();
DROP EVENT TRIGGER IF EXISTS warehouse_publication_invalidate_drop;
CREATE EVENT TRIGGER warehouse_publication_invalidate_drop ON sql_drop
    EXECUTE FUNCTION warehouse_publication.invalidate_asset_pointer_drop();

CREATE OR REPLACE FUNCTION warehouse_publication.get_house_elo_state(
    p_start_season bigint,
    p_end_season bigint
)
RETURNS TABLE (source_generation uuid, mart_generation uuid, input_digest text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE max_season bigint;
BEGIN
    IF p_start_season IS DISTINCT FROM 1869 OR p_end_season IS NULL
        OR p_end_season < p_start_season THEN
        RAISE EXCEPTION 'house Elo publication requires full history beginning in 1869'
            USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1 FROM core.games g
        WHERE g.id <> ALL(ARRAY[
            401540999,401545766,401545768,401545773,401545780,401545781,
            401549719,401550299,401552878,401552884,401640992,401833535,
            401866625
        ]::bigint[]) AND g.season IS NULL
    ) THEN
        RAISE EXCEPTION 'eligible core.games rows require a season' USING ERRCODE = '22023';
    END IF;
    SELECT max(g.season) INTO max_season FROM core.games g
    WHERE g.id <> ALL(ARRAY[
        401540999,401545766,401545768,401545773,401545780,401545781,
        401549719,401550299,401552878,401552884,401640992,401833535,
        401866625
    ]::bigint[]);
    IF max_season IS NOT NULL AND p_end_season < max_season THEN
        RAISE EXCEPTION 'house Elo publication end season does not cover current schedule'
            USING ERRCODE = '22023';
    END IF;
    PERFORM pg_catalog.set_config('TimeZone', 'UTC', true);
    RETURN QUERY SELECT
        (SELECT p.generation_id FROM meta.asset_current_generations p
         WHERE p.asset_key = 'analytics.house_elo_game' AND p.coverage_key = 'source-wide'),
        (SELECT p.generation_id FROM meta.asset_current_generations p
         WHERE p.asset_key = 'marts.house_elo_game' AND p.coverage_key = 'source-wide'),
        (SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
                    pg_catalog.md5(pg_catalog.to_jsonb(g)::text), '' ORDER BY g.id), ''))
         FROM core.games g);
END
$function$;

-- Scrub broad host defaults from the new private objects and every routine.
DO $acl$
DECLARE entry record; recipient text;
BEGIN
    FOR entry IN
        SELECT DISTINCT 'SCHEMA' AS object_kind,
            pg_catalog.format('%I', n.nspname) AS object_name, acl.grantee
        FROM pg_catalog.pg_namespace n
        CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) acl
        WHERE n.nspname = 'warehouse_publication' AND acl.grantee <> n.nspowner
        UNION
        SELECT DISTINCT 'TABLE', pg_catalog.format('%I.%I', n.nspname, c.relname), acl.grantee
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) acl
        WHERE n.nspname = 'meta'
          AND c.relname IN ('asset_publication_locks', 'asset_receipts',
              'asset_current_generations', 'asset_receipt_inputs')
          AND c.relkind = 'r' AND acl.grantee <> c.relowner
        UNION
        SELECT DISTINCT 'FUNCTION', p.oid::pg_catalog.regprocedure::text, acl.grantee
        FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))) acl
        WHERE n.nspname = 'warehouse_publication' AND acl.grantee <> p.proowner
    LOOP
        recipient := CASE WHEN entry.grantee = 0 THEN 'PUBLIC'
            ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(entry.grantee)) END;
        EXECUTE pg_catalog.format('REVOKE ALL ON %s %s FROM %s',
            entry.object_kind, entry.object_name, recipient);
    END LOOP;
END
$acl$;

-- No direct target mutation is part of the runtime role contract.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE
    ON analytics.house_elo_game, analytics.house_elo_current
    FROM warehouse_publisher;
REVOKE EXECUTE ON FUNCTION
    warehouse_quota.start_operation_run(uuid, text, text, jsonb, text, text),
    warehouse_quota.finish_operation_run(uuid, text, text)
FROM warehouse_publisher;
REVOKE USAGE ON SCHEMA warehouse_quota FROM warehouse_publisher;

DO $publisher_boundary$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        WHERE c.oid IN (
            pg_catalog.to_regclass('analytics.house_elo_game'),
            pg_catalog.to_regclass('analytics.house_elo_current'),
            pg_catalog.to_regclass('marts.house_elo_game')
        ) AND c.relowner = (SELECT oid FROM pg_catalog.pg_roles
            WHERE rolname = 'warehouse_publisher')
    ) OR pg_catalog.has_table_privilege(
            'warehouse_publisher', 'analytics.house_elo_game', 'INSERT,UPDATE,DELETE,TRUNCATE')
       OR pg_catalog.has_table_privilege(
            'warehouse_publisher', 'analytics.house_elo_current', 'INSERT,UPDATE,DELETE,TRUNCATE')
       OR pg_catalog.has_function_privilege(
            'warehouse_publisher',
            'warehouse_quota.start_operation_run(uuid,text,text,jsonb,text,text)',
            'EXECUTE')
       OR pg_catalog.has_function_privilege(
            'warehouse_publisher',
            'warehouse_quota.finish_operation_run(uuid,text,text)',
            'EXECUTE') THEN
        RAISE EXCEPTION 'warehouse_publisher has unsafe direct target privileges';
    END IF;
END
$publisher_boundary$;

GRANT USAGE ON SCHEMA warehouse_publication TO warehouse_publisher;
GRANT EXECUTE ON FUNCTION
    warehouse_publication.start_house_elo_run(uuid),
    warehouse_publication.finish_house_elo_run(uuid, text),
    warehouse_publication.get_house_elo_state(bigint, bigint),
    warehouse_publication.publish_house_elo(
        uuid, uuid, uuid, uuid, uuid, text, jsonb, jsonb, bigint, bigint, bigint[]
    ),
    warehouse_publication.record_house_elo_failure(uuid, uuid, uuid, text, text)
TO warehouse_publisher;
