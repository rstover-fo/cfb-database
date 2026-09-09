-- F11 generation-enforced refresh: managed forward migration after 069.
-- Opt-in standalone refresh of marts.house_elo_game only. The validated input
-- boundary is analytics.house_elo_game, not unversioned upstream core.games.

DO $namespace$
DECLARE ns record;
BEGIN
    SELECT oid,nspowner,nspacl INTO ns FROM pg_catalog.pg_namespace
    WHERE nspname='warehouse_refresh';
    IF NOT FOUND THEN
        CREATE SCHEMA warehouse_refresh;
    ELSE
        IF ns.nspowner <> (SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user)
            OR EXISTS (SELECT 1 FROM pg_catalog.aclexplode(ns.nspacl) a
                WHERE a.grantee<>ns.nspowner AND a.privilege_type='CREATE')
            OR EXISTS (SELECT 1 FROM pg_catalog.pg_type WHERE typnamespace=ns.oid)
            OR EXISTS (SELECT 1 FROM pg_catalog.pg_proc p WHERE p.pronamespace=ns.oid
                AND (p.proowner<>ns.nspowner OR p.prokind<>'f' OR p.provariadic<>0
                    OR p.pronargdefaults<>0 OR NOT COALESCE(p.oid=ANY(ARRAY[
                        pg_catalog.to_regprocedure('warehouse_refresh.get_house_elo_game_plan()'),
                        pg_catalog.to_regprocedure('warehouse_refresh.start_house_elo_game_refresh(uuid,jsonb)'),
                        pg_catalog.to_regprocedure('warehouse_refresh.require_house_elo_refresh_run(uuid)'),
                        pg_catalog.to_regprocedure('warehouse_refresh.publish_house_elo_game_refresh(uuid,uuid)'),
                        pg_catalog.to_regprocedure('warehouse_refresh.fail_house_elo_game_refresh(uuid,uuid,text)')
                    ]::oid[]),false))) THEN
            RAISE EXCEPTION 'warehouse_refresh must be a trusted migration-owned routines namespace';
        END IF;
    END IF;
END
$namespace$;

DO $role$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='warehouse_refresher') THEN
        CREATE ROLE warehouse_refresher NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB
            NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    ELSIF EXISTS (SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname='warehouse_refresher'
        AND (r.rolcanlogin OR r.rolinherit OR r.rolsuper OR r.rolcreatedb
            OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls
            OR EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member=r.oid))) THEN
        RAISE EXCEPTION 'warehouse_refresher must be a bounded NOLOGIN role';
    END IF;
END
$role$;

CREATE OR REPLACE FUNCTION warehouse_refresh.get_house_elo_game_plan()
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path='' AS $function$
DECLARE source_receipt meta.asset_receipts%ROWTYPE; target_generation uuid;
    cadence interval; latest_outcome text;
BEGIN
    SELECT r.* INTO source_receipt FROM meta.asset_current_generations p
    JOIN meta.asset_receipts r USING(asset_key,coverage_key,generation_id)
    WHERE p.asset_key='analytics.house_elo_game' AND p.coverage_key='source-wide';
    IF NOT FOUND OR source_receipt.outcome NOT IN ('succeeded','expected_no_data')
        OR source_receipt.coverage->'complete' IS DISTINCT FROM 'true'::jsonb
        OR source_receipt.published_at IS NULL THEN
        RAISE EXCEPTION 'required source has no current complete generation' USING ERRCODE='55000';
    END IF;
    SELECT r.outcome INTO latest_outcome FROM meta.asset_receipts r
    WHERE r.asset_key='analytics.house_elo_game' AND r.coverage_key='source-wide'
    ORDER BY r.recorded_at DESC,r.generation_id DESC LIMIT 1;
    IF latest_outcome IS NULL OR latest_outcome NOT IN ('succeeded','expected_no_data') THEN
        RAISE EXCEPTION 'latest source attempt is unsuccessful' USING ERRCODE='55000';
    END IF;
    SELECT expected_refresh_interval INTO cadence FROM meta.asset_freshness_policies
    WHERE asset_key='analytics.house_elo_game' AND coverage_key='source-wide';
    IF cadence IS NULL OR cadence<=interval '0 seconds' THEN
        RAISE EXCEPTION 'source publication cadence is undeclared' USING ERRCODE='55000';
    END IF;
    IF pg_catalog.clock_timestamp()-source_receipt.published_at>cadence THEN
        RAISE EXCEPTION 'required source generation is stale' USING ERRCODE='55000';
    END IF;
    SELECT generation_id INTO target_generation FROM meta.asset_current_generations
    WHERE asset_key='marts.house_elo_game' AND coverage_key='source-wide';
    RETURN pg_catalog.jsonb_build_object(
        'protocol','house-elo-game-refresh-v1','assets','["marts.house_elo_game"]'::jsonb,
        'coverage_key','source-wide','input_asset','analytics.house_elo_game',
        'source_generation_id',source_receipt.generation_id,'mart_generation_id',target_generation,
        'expected_refresh_interval',cadence::text,'unversioned_input_assets','["core.games"]'::jsonb);
END
$function$;

-- Private shared validator. Fixed scope prevents use of these wrappers to
-- finish another job or impersonate another session's refresh invocation.
CREATE OR REPLACE FUNCTION warehouse_refresh.require_house_elo_refresh_run(p_run_id uuid)
RETURNS meta.operation_runs LANGUAGE plpgsql SET search_path='' AS $function$
DECLARE r meta.operation_runs%ROWTYPE; expected jsonb; cadence interval;
BEGIN
    SELECT * INTO r FROM meta.operation_runs WHERE operation_run_id=p_run_id FOR UPDATE;
    IF NOT FOUND OR r.recorded_by<>session_user OR r.operation_kind<>'refresh'
        OR r.initiator<>'refresh_marts' OR r.code_revision IS NOT NULL THEN
        RAISE EXCEPTION 'operation does not belong to this refresher' USING ERRCODE='22023';
    END IF;
    cadence := (r.requested_scope->>'expected_refresh_interval')::interval;
    IF cadence IS NULL OR cadence<=interval '0 seconds'
        OR r.requested_scope->>'source_generation_id' IS NULL THEN
        RAISE EXCEPTION 'invalid pinned refresh input' USING ERRCODE='22023';
    END IF;
    expected := pg_catalog.jsonb_build_object(
        'protocol','house-elo-game-refresh-v1','assets','["marts.house_elo_game"]'::jsonb,
        'coverage_key','source-wide','input_asset','analytics.house_elo_game',
        'source_generation_id',(r.requested_scope->>'source_generation_id')::uuid,
        'mart_generation_id',(r.requested_scope->>'mart_generation_id')::uuid,
        'expected_refresh_interval',r.requested_scope->>'expected_refresh_interval',
        'unversioned_input_assets','["core.games"]'::jsonb);
    IF r.requested_scope IS DISTINCT FROM expected
        OR r.plan_digest IS DISTINCT FROM pg_catalog.md5(expected::text) THEN
        RAISE EXCEPTION 'invalid pinned refresh scope' USING ERRCODE='22023';
    END IF;
    RETURN r;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_refresh.start_house_elo_game_refresh(p_run_id uuid,p_plan jsonb)
RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path='' AS $function$
BEGIN
    PERFORM warehouse_quota.start_operation_run(p_run_id,'refresh','refresh_marts',p_plan,
        pg_catalog.md5(p_plan::text),NULL);
    PERFORM warehouse_refresh.require_house_elo_refresh_run(p_run_id);
    RETURN p_run_id;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_refresh.publish_house_elo_game_refresh(p_run_id uuid,p_generation uuid)
RETURNS TABLE(generation_id uuid,replayed boolean,published_rows bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path='' AS $function$
DECLARE run_row meta.operation_runs%ROWTYPE; existing meta.asset_receipts%ROWTYPE;
    source_receipt meta.asset_receipts%ROWTYPE; expected_source uuid; expected_mart uuid;
    current_source uuid; plan_now jsonb; request_digest_value text; source_rows bigint;
    previous_rows bigint; result_rows bigint; publication_time timestamptz;
BEGIN
    IF p_generation IS NULL THEN
        RAISE EXCEPTION 'generation ID is required' USING ERRCODE='22023';
    END IF;
    run_row := warehouse_refresh.require_house_elo_refresh_run(p_run_id);
    expected_source := (run_row.requested_scope->>'source_generation_id')::uuid;
    expected_mart := (run_row.requested_scope->>'mart_generation_id')::uuid;
    request_digest_value := pg_catalog.md5(pg_catalog.jsonb_build_object(
        'run_id',p_run_id,'generation_id',p_generation,'plan',run_row.requested_scope)::text);
    SELECT * INTO existing FROM meta.asset_receipts r WHERE r.generation_id=p_generation;
    IF FOUND THEN
        IF existing.operation_run_id<>p_run_id OR existing.asset_key<>'marts.house_elo_game'
            OR existing.request_digest<>request_digest_value
            OR existing.outcome NOT IN ('succeeded','expected_no_data')
            OR run_row.outcome<>'succeeded'
            OR NOT EXISTS (SELECT 1 FROM meta.asset_receipt_inputs i
                WHERE i.generation_id=p_generation AND i.input_asset_key='analytics.house_elo_game'
                    AND i.input_coverage_key='source-wide' AND i.input_generation_id=expected_source) THEN
            RAISE EXCEPTION 'generation ID already has different refresh context' USING ERRCODE='22023';
        END IF;
        RETURN QUERY SELECT p_generation,true,(existing.row_delta->>'published_rows')::bigint;
        RETURN;
    END IF;
    IF run_row.outcome<>'running' THEN
        RAISE EXCEPTION 'refresh operation is terminal' USING ERRCODE='22023';
    END IF;
    IF pg_catalog.current_setting('transaction_isolation')<>'read committed'
        OR pg_catalog.current_setting('session_replication_role')<>'origin'
        OR COALESCE(pg_catalog.current_setting('event_triggers',true),'on')<>'on' THEN
        RAISE EXCEPTION 'refresh publication requires READ COMMITTED and active guards' USING ERRCODE='25001';
    END IF;

    -- Matches 068's source-before-asset order. Snapshot writes also invalidate
    -- source evidence, so both source targets are pinned against ordinary DML.
    LOCK TABLE analytics.house_elo_game,analytics.house_elo_current IN SHARE MODE;
    PERFORM 1 FROM meta.asset_publication_locks
    WHERE asset_key IN ('analytics.house_elo_game','marts.house_elo_game') ORDER BY asset_key FOR UPDATE;
    PERFORM 1 FROM meta.asset_freshness_policies WHERE asset_key='analytics.house_elo_game'
        AND coverage_key='source-wide' FOR SHARE;
    -- 068 failure receipts do not lock asset rows. Block those inserts too,
    -- so a newly failed input cannot race the latest-outcome eligibility check.
    LOCK TABLE meta.asset_receipts IN SHARE MODE;
    SELECT p.generation_id INTO current_source FROM meta.asset_current_generations p
    WHERE p.asset_key='analytics.house_elo_game' AND p.coverage_key='source-wide' FOR SHARE;

    IF (SELECT count(*) FROM meta.asset_publication_locks l
            WHERE l.asset_key IN ('analytics.house_elo_game','marts.house_elo_game')
                AND l.relation_oid=pg_catalog.to_regclass(l.asset_key))<>2
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t
            WHERE t.tgrelid='analytics.house_elo_game'::regclass
                AND t.tgname='invalidate_house_elo_game_pointer' AND t.tgtype=62
                AND t.tgenabled IN ('O','A') AND NOT t.tgisinternal
                AND t.tgfoid=pg_catalog.to_regprocedure('warehouse_publication.invalidate_house_elo_pointers()'))
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t
            WHERE t.tgrelid='analytics.house_elo_current'::regclass
                AND t.tgname='invalidate_house_elo_current_pointer' AND t.tgtype=62
                AND t.tgenabled IN ('O','A') AND NOT t.tgisinternal
                AND t.tgfoid=pg_catalog.to_regprocedure('warehouse_publication.invalidate_house_elo_pointers()'))
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger e
            WHERE e.evtname='warehouse_publication_invalidate_ddl' AND e.evtevent='ddl_command_end'
                AND e.evtenabled IN ('O','A') AND e.evtfoid=pg_catalog.to_regprocedure(
                    'warehouse_publication.invalidate_asset_pointer_ddl()'))
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger e
            WHERE e.evtname='warehouse_publication_invalidate_drop' AND e.evtevent='sql_drop'
                AND e.evtenabled IN ('O','A') AND e.evtfoid=pg_catalog.to_regprocedure(
                    'warehouse_publication.invalidate_asset_pointer_drop()')) THEN
        RAISE EXCEPTION 'publication guards or asset identities are invalid' USING ERRCODE='55000';
    END IF;
    plan_now := warehouse_refresh.get_house_elo_game_plan();
    IF plan_now IS DISTINCT FROM run_row.requested_scope OR current_source IS DISTINCT FROM expected_source THEN
        RAISE EXCEPTION 'pinned generation or cadence changed; replan required' USING ERRCODE='40001';
    END IF;
    SELECT * INTO STRICT source_receipt FROM meta.asset_receipts r WHERE r.generation_id=expected_source;
    SELECT count(*) INTO source_rows FROM analytics.house_elo_game;
    IF source_rows IS DISTINCT FROM (source_receipt.row_delta->>'published_rows')::bigint
        OR (source_receipt.outcome='expected_no_data') IS DISTINCT FROM (source_rows=0) THEN
        RAISE EXCEPTION 'source coverage and data disagree' USING ERRCODE='55000';
    END IF;
    SELECT count(*) INTO previous_rows FROM marts.house_elo_game;
    -- Do not lock the mart pointer before this: a queued legacy refresh holds
    -- the mart lock while its event guard deletes that pointer.
    REFRESH MATERIALIZED VIEW marts.house_elo_game;
    IF (SELECT relation_oid FROM meta.asset_publication_locks WHERE asset_key='marts.house_elo_game')
            IS DISTINCT FROM pg_catalog.to_regclass('marts.house_elo_game') THEN
        RAISE EXCEPTION 'mart identity changed during refresh' USING ERRCODE='40001';
    END IF;
    SELECT count(*) INTO result_rows FROM marts.house_elo_game;
    publication_time := pg_catalog.clock_timestamp();
    IF result_rows<>source_rows THEN
        RAISE EXCEPTION 'refreshed coverage differs from source' USING ERRCODE='55000';
    END IF;
    IF publication_time-source_receipt.published_at>
            (run_row.requested_scope->>'expected_refresh_interval')::interval THEN
        RAISE EXCEPTION 'source became stale during refresh' USING ERRCODE='55000';
    END IF;
    INSERT INTO meta.asset_receipts(generation_id,operation_run_id,asset_key,outcome,coverage,
        source_watermark,input_observations,row_delta,request_digest,published_at)
    VALUES(p_generation,p_run_id,'marts.house_elo_game',source_receipt.outcome,
        source_receipt.coverage||pg_catalog.jsonb_build_object('input_generation_id',expected_source),
        source_receipt.source_watermark,
        pg_catalog.jsonb_build_object('publisher','refresh_marts','protocol','house-elo-game-refresh-v1',
            'input_boundary','analytics.house_elo_game','unversioned_input_assets','["core.games"]'::jsonb,
            'expected_refresh_interval',run_row.requested_scope->>'expected_refresh_interval'),
        pg_catalog.jsonb_build_object('observed_previous_rows',previous_rows,'published_rows',result_rows),
        request_digest_value,publication_time);
    INSERT INTO meta.asset_receipt_inputs(generation_id,input_asset_key,input_generation_id)
    VALUES(p_generation,'analytics.house_elo_game',expected_source);
    INSERT INTO meta.asset_current_generations(asset_key,generation_id)
    VALUES('marts.house_elo_game',p_generation)
    ON CONFLICT(asset_key,coverage_key) DO UPDATE SET generation_id=EXCLUDED.generation_id;
    PERFORM warehouse_quota.finish_operation_run(p_run_id,'succeeded',NULL);
    RETURN QUERY SELECT p_generation,false,result_rows;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_refresh.fail_house_elo_game_refresh(
    p_run_id uuid,p_generation uuid,p_outcome text)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path='' AS $function$
DECLARE run_row meta.operation_runs%ROWTYPE; existing meta.asset_receipts%ROWTYPE; digest text;
BEGIN
    IF p_generation IS NULL OR p_outcome IS NULL OR p_outcome NOT IN ('failed','blocked') THEN
        RAISE EXCEPTION 'invalid refresh failure outcome' USING ERRCODE='22023';
    END IF;
    run_row := warehouse_refresh.require_house_elo_refresh_run(p_run_id);
    digest := pg_catalog.md5(pg_catalog.jsonb_build_object('run_id',p_run_id,
        'generation_id',p_generation,'plan',run_row.requested_scope,'outcome',p_outcome)::text);
    SELECT * INTO existing FROM meta.asset_receipts r WHERE r.generation_id=p_generation;
    IF FOUND THEN
        IF existing.operation_run_id<>p_run_id OR existing.asset_key<>'marts.house_elo_game'
            OR existing.outcome<>p_outcome OR existing.request_digest<>digest THEN
            RAISE EXCEPTION 'generation ID has different failure context' USING ERRCODE='22023';
        END IF;
        RETURN 0;
    END IF;
    IF run_row.outcome<>'running' THEN
        RAISE EXCEPTION 'refresh operation is terminal' USING ERRCODE='22023';
    END IF;
    INSERT INTO meta.asset_receipts(generation_id,operation_run_id,asset_key,outcome,coverage,
        input_observations,request_digest,error_summary)
    VALUES(p_generation,p_run_id,'marts.house_elo_game',p_outcome,
        '{"complete":false,"scope":"source-wide"}'::jsonb,run_row.requested_scope,digest,'publication_failed');
    RETURN warehouse_quota.finish_operation_run(p_run_id,p_outcome,'publication_failed');
END
$function$;

DO $acl$
DECLARE item record; recipient text;
BEGIN
    FOR item IN
        SELECT 'SCHEMA' AS kind,'warehouse_refresh' AS name,a.grantee
        FROM pg_catalog.pg_namespace n CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) a
        WHERE n.nspname='warehouse_refresh' AND a.grantee<>n.nspowner
        UNION
        SELECT 'FUNCTION',p.oid::pg_catalog.regprocedure::text,a.grantee
        FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(p.proacl,pg_catalog.acldefault('f',p.proowner))) a
        WHERE n.nspname='warehouse_refresh' AND a.grantee<>p.proowner
    LOOP
        recipient := CASE WHEN item.grantee=0 THEN 'PUBLIC'
            ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(item.grantee)) END;
        EXECUTE pg_catalog.format('REVOKE ALL ON %s %s FROM %s',item.kind,item.name,recipient);
    END LOOP;
END
$acl$;
GRANT USAGE ON SCHEMA warehouse_refresh TO warehouse_refresher;
GRANT EXECUTE ON FUNCTION warehouse_refresh.get_house_elo_game_plan(),
    warehouse_refresh.start_house_elo_game_refresh(uuid,jsonb),
    warehouse_refresh.publish_house_elo_game_refresh(uuid,uuid),
    warehouse_refresh.fail_house_elo_game_refresh(uuid,uuid,text) TO warehouse_refresher;

-- A pre-existing role can have unsafe direct grants even without memberships.
-- Refuse activation rather than silently retaining a route around the gates.
DO $boundary$
DECLARE relation_name text;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'analytics.house_elo_game','analytics.house_elo_current','marts.house_elo_game',
        'meta.asset_receipts','meta.asset_current_generations','meta.asset_receipt_inputs',
        'meta.asset_freshness_policies','meta.operation_runs'
    ] LOOP
        -- PostgreSQL 17 permits direct REFRESH with MAINTAIN even without
        -- ownership or DML grants. Earlier servers require ownership instead.
        IF pg_catalog.current_setting('server_version_num')::integer>=170000 THEN
            IF pg_catalog.has_table_privilege('warehouse_refresher',relation_name,'MAINTAIN') THEN
                RAISE EXCEPTION 'warehouse_refresher has unsafe maintenance access to %',relation_name;
            END IF;
        END IF;
        IF pg_catalog.has_table_privilege('warehouse_refresher',relation_name,
                'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
            OR (SELECT relowner FROM pg_catalog.pg_class
                WHERE oid=pg_catalog.to_regclass(relation_name)) =
                    (SELECT oid FROM pg_catalog.pg_roles WHERE rolname='warehouse_refresher') THEN
            RAISE EXCEPTION 'warehouse_refresher has unsafe direct access to %',relation_name;
        END IF;
    END LOOP;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname IN ('warehouse_quota','warehouse_publication')
            AND pg_catalog.has_function_privilege('warehouse_refresher',p.oid,'EXECUTE')
    ) THEN
        RAISE EXCEPTION 'warehouse_refresher has unrelated quota or publication access';
    END IF;
END
$boundary$;
