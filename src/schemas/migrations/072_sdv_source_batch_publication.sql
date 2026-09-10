-- F14/F28 source publication: extend the opt-in SDV season protocol to the
-- remaining three sportsdataverse season files.  Migration 071's dedicated
-- ratings.sdv_ratings_weekly entrypoints remain unchanged.

ALTER TABLE meta.asset_publication_locks
    DROP CONSTRAINT IF EXISTS asset_publication_locks_asset_key_check;
ALTER TABLE meta.asset_publication_locks
    ADD CONSTRAINT asset_publication_locks_asset_key_check CHECK (asset_key IN (
        'analytics.house_elo_game',
        'marts.house_elo_game',
        'ratings.sdv_ratings_weekly',
        'ratings.espn_fpi_weekly',
        'ref.team_id_xwalk',
        'ref.game_id_xwalk'
    ));

ALTER TABLE meta.asset_receipts
    DROP CONSTRAINT IF EXISTS asset_receipts_coverage_key_check;
ALTER TABLE meta.asset_receipts
    ADD CONSTRAINT asset_receipts_coverage_key_check CHECK (
        (asset_key IN ('analytics.house_elo_game', 'marts.house_elo_game')
            AND coverage_key = 'source-wide')
        OR
        (asset_key IN (
                'ratings.sdv_ratings_weekly', 'ratings.espn_fpi_weekly',
                'ref.team_id_xwalk', 'ref.game_id_xwalk'
            )
            AND coverage_key ~
                '^season:(18(69|[7-9][0-9])|19[0-9]{2}|20[0-9]{2}|21[0-9]{2}|2200)$')
    );

ALTER TABLE meta.asset_current_generations
    DROP CONSTRAINT IF EXISTS asset_current_generations_coverage_key_check;
ALTER TABLE meta.asset_current_generations
    ADD CONSTRAINT asset_current_generations_coverage_key_check CHECK (
        (asset_key IN ('analytics.house_elo_game', 'marts.house_elo_game')
            AND coverage_key = 'source-wide')
        OR
        (asset_key IN (
                'ratings.sdv_ratings_weekly', 'ratings.espn_fpi_weekly',
                'ref.team_id_xwalk', 'ref.game_id_xwalk'
            )
            AND coverage_key ~
                '^season:(18(69|[7-9][0-9])|19[0-9]{2}|20[0-9]{2}|21[0-9]{2}|2200)$')
    );

ALTER TABLE meta.asset_receipt_inputs
    DROP CONSTRAINT IF EXISTS asset_receipt_inputs_input_coverage_key_check;
ALTER TABLE meta.asset_receipt_inputs
    ADD CONSTRAINT asset_receipt_inputs_input_coverage_key_check CHECK (
        (input_asset_key IN ('analytics.house_elo_game', 'marts.house_elo_game')
            AND input_coverage_key = 'source-wide')
        OR
        (input_asset_key IN (
                'ratings.sdv_ratings_weekly', 'ratings.espn_fpi_weekly',
                'ref.team_id_xwalk', 'ref.game_id_xwalk'
            )
            AND input_coverage_key ~
                '^season:(18(69|[7-9][0-9])|19[0-9]{2}|20[0-9]{2}|21[0-9]{2}|2200)$')
    );

-- Keep the new protocol in its own routines-only namespace so neither the
-- original publication nor migration 071 routine whitelist changes.
DO $namespace$
DECLARE ns record;
BEGIN
    SELECT oid, nspowner, nspacl INTO ns FROM pg_catalog.pg_namespace
    WHERE nspname = 'warehouse_source_batch';
    IF NOT FOUND THEN
        CREATE SCHEMA warehouse_source_batch;
    ELSIF ns.nspowner <> (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user)
        OR EXISTS (SELECT 1 FROM pg_catalog.aclexplode(ns.nspacl) a
            WHERE a.grantee <> ns.nspowner AND a.privilege_type = 'CREATE')
        OR EXISTS (SELECT 1 FROM pg_catalog.pg_type WHERE typnamespace = ns.oid)
        OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc p WHERE p.pronamespace = ns.oid
              AND (p.proowner <> ns.nspowner OR p.prokind <> 'f'
                OR p.provariadic <> 0 OR p.pronargdefaults <> 0
                OR NOT COALESCE(p.oid = ANY(ARRAY[
                    pg_catalog.to_regprocedure(
                        'warehouse_source_batch.invalidate_row_pointer()'),
                    pg_catalog.to_regprocedure(
                        'warehouse_source_batch.invalidate_truncate_pointer()'),
                    pg_catalog.to_regprocedure(
                        'warehouse_source_batch.invalidate_ddl()'),
                    pg_catalog.to_regprocedure(
                        'warehouse_source_batch.invalidate_drop()'),
                    pg_catalog.to_regprocedure(
                        'warehouse_source_batch.get_source_plan(text,bigint)'),
                    pg_catalog.to_regprocedure(
                        'warehouse_source_batch.require_source_load(uuid)'),
                    pg_catalog.to_regprocedure(
                        'warehouse_source_batch.start_source_load(uuid,jsonb)'),
                    pg_catalog.to_regprocedure(
                        'warehouse_source_batch.publish_source_load(uuid,uuid,text,jsonb,jsonb)'),
                    pg_catalog.to_regprocedure(
                        'warehouse_source_batch.fail_source_load(uuid,uuid,text)')
                ]::oid[]), false))
        ) THEN
        RAISE EXCEPTION
            'warehouse_source_batch must be a trusted migration-owned routines namespace';
    END IF;
END
$namespace$;

DO $dependencies$
DECLARE stage_ns record; ledger_oid oid := pg_catalog.to_regclass('meta.flat_file_loads');
BEGIN
    SELECT oid, nspowner, nspacl INTO stage_ns FROM pg_catalog.pg_namespace
    WHERE nspname = 'warehouse_source_stage';
    IF NOT FOUND
        OR stage_ns.nspowner <>
            (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user)
        OR EXISTS (SELECT 1 FROM pg_catalog.aclexplode(stage_ns.nspacl) a
            WHERE a.grantee <> stage_ns.nspowner) THEN
        RAISE EXCEPTION 'warehouse_source_stage is not the trusted private staging namespace';
    END IF;
    IF ledger_oid IS NULL OR NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        WHERE c.oid = ledger_oid AND c.relkind = 'r'
          AND c.relowner = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user)
    ) OR EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i
        WHERE i.inhrelid = ledger_oid OR i.inhparent = ledger_oid)
      OR EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t
        WHERE t.tgrelid = ledger_oid AND NOT t.tgisinternal) THEN
        RAISE EXCEPTION 'meta.flat_file_loads is not a trusted migration-owned ordinary table';
    END IF;
    -- 071 and 072 are both pending in production.  Under PostgreSQL 17 a
    -- non-superuser CREATEROLE migration owner receives an automatic inbound
    -- ADMIN membership in the role created by 071; stock PG17 records the
    -- bootstrap superuser as grantor.  Accept only that exact
    -- creator edge while SET and INHERIT remain false.  Any runtime
    -- activation, unrelated member or outbound membership still fails closed.
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles r
        WHERE r.rolname = 'warehouse_source_publisher'
          AND NOT r.rolcanlogin AND NOT r.rolinherit AND NOT r.rolsuper
          AND NOT r.rolcreatedb AND NOT r.rolcreaterole AND NOT r.rolreplication
          AND NOT r.rolbypassrls
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m
              WHERE m.member = r.oid
                 OR (m.roleid = r.oid AND NOT (
                    m.member = (SELECT oid FROM pg_catalog.pg_roles
                        WHERE rolname = current_user)
                    AND m.admin_option AND NOT m.inherit_option AND NOT m.set_option
                )))) THEN
        RAISE EXCEPTION
            'warehouse_source_publisher must be a bounded NOLOGIN role';
    END IF;
END
$dependencies$;

-- Register exact relation identities.  Replacing a target is allowed only
-- after its DDL guard has removed every current pointer.
DO $assets$
DECLARE
    target record;
    current_oid oid;
    existing meta.asset_publication_locks%ROWTYPE;
BEGIN
    FOR target IN SELECT * FROM (VALUES
        ('ratings.espn_fpi_weekly', 'ratings', 'espn_fpi_weekly'),
        ('ref.team_id_xwalk', 'ref', 'team_id_xwalk'),
        ('ref.game_id_xwalk', 'ref', 'game_id_xwalk')
    ) AS t(asset_key, relation_schema, relation_name)
    LOOP
        current_oid := pg_catalog.to_regclass(pg_catalog.format(
            '%I.%I', target.relation_schema, target.relation_name));
        IF current_oid IS NULL OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class c
            WHERE c.oid = current_oid AND c.relkind = 'r'
              AND c.relowner = (SELECT oid FROM pg_catalog.pg_roles
                  WHERE rolname = current_user)
              AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
              AND NOT c.relhasrules
        ) OR EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i
            WHERE i.inhrelid = current_oid OR i.inhparent = current_oid)
          OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_trigger tr
            WHERE tr.tgrelid = current_oid AND NOT tr.tgisinternal
              AND NOT (
                tr.tgname = 'invalidate_sdv_source_batch_row_pointer'
                AND tr.tgtype = 31
                AND tr.tgqual IS NULL AND tr.tgnargs = 0
                AND pg_catalog.octet_length(tr.tgargs) = 0
                AND tr.tgattr::text = '' AND tr.tgconstraint = 0
                AND NOT tr.tgdeferrable AND NOT tr.tginitdeferred
                AND tr.tgparentid = 0
                AND tr.tgfoid = pg_catalog.to_regprocedure(
                    'warehouse_source_batch.invalidate_row_pointer()')
                OR tr.tgname = 'invalidate_sdv_source_batch_truncate_pointer'
                AND tr.tgtype = 34
                AND tr.tgqual IS NULL AND tr.tgnargs = 0
                AND pg_catalog.octet_length(tr.tgargs) = 0
                AND tr.tgattr::text = '' AND tr.tgconstraint = 0
                AND NOT tr.tgdeferrable AND NOT tr.tginitdeferred
                AND tr.tgparentid = 0
                AND tr.tgfoid = pg_catalog.to_regprocedure(
                    'warehouse_source_batch.invalidate_truncate_pointer()')
              )
          ) THEN
            RAISE EXCEPTION '% must be a trusted migration-owned ordinary table',
                target.asset_key;
        END IF;

        SELECT * INTO existing FROM meta.asset_publication_locks l
        WHERE l.asset_key = target.asset_key;
        IF NOT FOUND THEN
            INSERT INTO meta.asset_publication_locks(
                asset_key, relation_schema, relation_name, relation_oid, relation_kind
            ) VALUES (target.asset_key, target.relation_schema, target.relation_name,
                current_oid, 'r'::"char");
        ELSIF existing.relation_schema IS DISTINCT FROM target.relation_schema
            OR existing.relation_name IS DISTINCT FROM target.relation_name
            OR existing.relation_oid IS DISTINCT FROM current_oid
            OR existing.relation_kind IS DISTINCT FROM 'r'::"char" THEN
            IF EXISTS (SELECT 1 FROM meta.asset_current_generations p
                WHERE p.asset_key = target.asset_key) THEN
                RAISE EXCEPTION 'cannot adopt replacement % while a current pointer exists',
                    target.asset_key;
            END IF;
            UPDATE meta.asset_publication_locks
            SET relation_schema = target.relation_schema,
                relation_name = target.relation_name,
                relation_oid = current_oid,
                relation_kind = 'r'::"char"
            WHERE asset_key = target.asset_key;
        END IF;
    END LOOP;
END
$assets$;

CREATE OR REPLACE FUNCTION warehouse_source_batch.invalidate_row_pointer()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE asset text;
BEGIN
    asset := CASE TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
        WHEN 'ratings.espn_fpi_weekly' THEN 'ratings.espn_fpi_weekly'
        WHEN 'ref.team_id_xwalk' THEN 'ref.team_id_xwalk'
        WHEN 'ref.game_id_xwalk' THEN 'ref.game_id_xwalk'
        ELSE NULL END;
    IF asset IS NULL THEN
        RAISE EXCEPTION 'source invalidation trigger is attached to an untrusted target';
    END IF;
    DELETE FROM meta.asset_current_generations p
    WHERE p.asset_key = asset
      AND p.coverage_key IN (
          CASE WHEN TG_OP IN ('UPDATE', 'DELETE') THEN 'season:' || OLD.season::text END,
          CASE WHEN TG_OP IN ('UPDATE', 'INSERT') THEN 'season:' || NEW.season::text END
      );
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_source_batch.invalidate_truncate_pointer()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE asset text;
BEGIN
    asset := CASE TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
        WHEN 'ratings.espn_fpi_weekly' THEN 'ratings.espn_fpi_weekly'
        WHEN 'ref.team_id_xwalk' THEN 'ref.team_id_xwalk'
        WHEN 'ref.game_id_xwalk' THEN 'ref.game_id_xwalk'
        ELSE NULL END;
    IF asset IS NULL THEN
        RAISE EXCEPTION 'source invalidation trigger is attached to an untrusted target';
    END IF;
    DELETE FROM meta.asset_current_generations p WHERE p.asset_key = asset;
    RETURN NULL;
END
$function$;

DROP TRIGGER IF EXISTS invalidate_sdv_source_batch_row_pointer
    ON ratings.espn_fpi_weekly;
CREATE TRIGGER invalidate_sdv_source_batch_row_pointer
    BEFORE INSERT OR UPDATE OR DELETE ON ratings.espn_fpi_weekly
    FOR EACH ROW EXECUTE FUNCTION warehouse_source_batch.invalidate_row_pointer();
DROP TRIGGER IF EXISTS invalidate_sdv_source_batch_truncate_pointer
    ON ratings.espn_fpi_weekly;
CREATE TRIGGER invalidate_sdv_source_batch_truncate_pointer
    BEFORE TRUNCATE ON ratings.espn_fpi_weekly
    FOR EACH STATEMENT EXECUTE FUNCTION warehouse_source_batch.invalidate_truncate_pointer();

DROP TRIGGER IF EXISTS invalidate_sdv_source_batch_row_pointer ON ref.team_id_xwalk;
CREATE TRIGGER invalidate_sdv_source_batch_row_pointer
    BEFORE INSERT OR UPDATE OR DELETE ON ref.team_id_xwalk
    FOR EACH ROW EXECUTE FUNCTION warehouse_source_batch.invalidate_row_pointer();
DROP TRIGGER IF EXISTS invalidate_sdv_source_batch_truncate_pointer ON ref.team_id_xwalk;
CREATE TRIGGER invalidate_sdv_source_batch_truncate_pointer
    BEFORE TRUNCATE ON ref.team_id_xwalk
    FOR EACH STATEMENT EXECUTE FUNCTION warehouse_source_batch.invalidate_truncate_pointer();

DROP TRIGGER IF EXISTS invalidate_sdv_source_batch_row_pointer ON ref.game_id_xwalk;
CREATE TRIGGER invalidate_sdv_source_batch_row_pointer
    BEFORE INSERT OR UPDATE OR DELETE ON ref.game_id_xwalk
    FOR EACH ROW EXECUTE FUNCTION warehouse_source_batch.invalidate_row_pointer();
DROP TRIGGER IF EXISTS invalidate_sdv_source_batch_truncate_pointer ON ref.game_id_xwalk;
CREATE TRIGGER invalidate_sdv_source_batch_truncate_pointer
    BEFORE TRUNCATE ON ref.game_id_xwalk
    FOR EACH STATEMENT EXECUTE FUNCTION warehouse_source_batch.invalidate_truncate_pointer();

CREATE OR REPLACE FUNCTION warehouse_source_batch.invalidate_ddl()
RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE command record; target record; invalidate_all boolean := false; changed boolean;
BEGIN
    FOR command IN SELECT * FROM pg_catalog.pg_event_trigger_ddl_commands() LOOP
        IF command.object_identity LIKE 'warehouse_source_batch.%'
            OR command.object_identity IN (
                'warehouse_source_batch_invalidate_ddl',
                'warehouse_source_batch_invalidate_drop') THEN
            invalidate_all := true;
        END IF;
        FOR target IN SELECT * FROM (VALUES
            ('ratings.espn_fpi_weekly', 'ratings', 'espn_fpi_weekly'),
            ('ref.team_id_xwalk', 'ref', 'team_id_xwalk'),
            ('ref.game_id_xwalk', 'ref', 'game_id_xwalk')
        ) AS t(asset_key, relation_schema, relation_name)
        LOOP
            changed := EXISTS (
                SELECT 1 FROM meta.asset_publication_locks l
                WHERE l.asset_key = target.asset_key
                  AND (command.objid = l.relation_oid OR command.objid =
                    pg_catalog.to_regclass(pg_catalog.format('%I.%I',
                        target.relation_schema, target.relation_name)))
            ) OR EXISTS (
                SELECT 1 FROM pg_catalog.pg_trigger tr
                JOIN meta.asset_publication_locks l ON l.relation_oid = tr.tgrelid
                WHERE l.asset_key = target.asset_key AND tr.oid = command.objid
            ) OR EXISTS (
                SELECT 1 FROM pg_catalog.pg_index ix
                JOIN meta.asset_publication_locks l ON l.relation_oid = ix.indrelid
                WHERE l.asset_key = target.asset_key AND ix.indexrelid = command.objid
            ) OR EXISTS (
                SELECT 1 FROM pg_catalog.pg_constraint con
                JOIN meta.asset_publication_locks l ON l.relation_oid = con.conrelid
                WHERE l.asset_key = target.asset_key AND con.oid = command.objid
            ) OR EXISTS (
                SELECT 1 FROM pg_catalog.pg_rewrite rw
                JOIN meta.asset_publication_locks l ON l.relation_oid = rw.ev_class
                WHERE l.asset_key = target.asset_key AND rw.oid = command.objid
            ) OR EXISTS (
                SELECT 1 FROM pg_catalog.pg_inherits i
                WHERE i.inhrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I',
                        target.relation_schema, target.relation_name))
                   OR i.inhparent = pg_catalog.to_regclass(pg_catalog.format('%I.%I',
                        target.relation_schema, target.relation_name))
            ) OR EXISTS (
                SELECT 1 FROM meta.asset_publication_locks l
                WHERE l.asset_key = target.asset_key
                  AND l.relation_oid IS DISTINCT FROM pg_catalog.to_regclass(
                    pg_catalog.format('%I.%I', target.relation_schema,
                        target.relation_name))
            );
            IF changed THEN
                DELETE FROM meta.asset_current_generations p
                WHERE p.asset_key = target.asset_key;
            END IF;
        END LOOP;
    END LOOP;
    IF invalidate_all THEN
        DELETE FROM meta.asset_current_generations p
        WHERE p.asset_key IN (
            'ratings.espn_fpi_weekly', 'ref.team_id_xwalk', 'ref.game_id_xwalk');
    END IF;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_source_batch.invalidate_drop()
RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE dropped record; target record; invalidate_all boolean := false; changed boolean;
BEGIN
    FOR dropped IN SELECT * FROM pg_catalog.pg_event_trigger_dropped_objects() LOOP
        IF dropped.object_identity LIKE 'warehouse_source_batch.%'
            OR dropped.object_identity IN (
                'warehouse_source_batch_invalidate_ddl',
                'warehouse_source_batch_invalidate_drop') THEN
            invalidate_all := true;
        END IF;
        FOR target IN SELECT * FROM (VALUES
            ('ratings.espn_fpi_weekly', 'ratings', 'espn_fpi_weekly'),
            ('ref.team_id_xwalk', 'ref', 'team_id_xwalk'),
            ('ref.game_id_xwalk', 'ref', 'game_id_xwalk')
        ) AS t(asset_key, relation_schema, relation_name)
        LOOP
            changed := EXISTS (
                SELECT 1 FROM meta.asset_publication_locks l
                WHERE l.asset_key = target.asset_key
                  AND (dropped.objid = l.relation_oid OR
                    (dropped.address_names[1] = target.relation_schema
                        AND dropped.address_names[2] = target.relation_name))
            ) OR (
                dropped.object_type = 'trigger'
                AND target.relation_schema = ANY(dropped.address_names)
                AND target.relation_name = ANY(dropped.address_names)
            ) OR EXISTS (
                SELECT 1 FROM meta.asset_publication_locks l
                WHERE l.asset_key = target.asset_key
                  AND l.relation_oid IS DISTINCT FROM pg_catalog.to_regclass(
                    pg_catalog.format('%I.%I', target.relation_schema,
                        target.relation_name))
            );
            IF changed THEN
                DELETE FROM meta.asset_current_generations p
                WHERE p.asset_key = target.asset_key;
            END IF;
        END LOOP;
    END LOOP;
    IF invalidate_all THEN
        DELETE FROM meta.asset_current_generations p
        WHERE p.asset_key IN (
            'ratings.espn_fpi_weekly', 'ref.team_id_xwalk', 'ref.game_id_xwalk');
    END IF;
END
$function$;

DROP EVENT TRIGGER IF EXISTS warehouse_source_batch_invalidate_ddl;
CREATE EVENT TRIGGER warehouse_source_batch_invalidate_ddl ON ddl_command_end
    EXECUTE FUNCTION warehouse_source_batch.invalidate_ddl();
DROP EVENT TRIGGER IF EXISTS warehouse_source_batch_invalidate_drop;
CREATE EVENT TRIGGER warehouse_source_batch_invalidate_drop ON sql_drop
    EXECUTE FUNCTION warehouse_source_batch.invalidate_drop();

CREATE OR REPLACE FUNCTION warehouse_source_batch.get_source_plan(
    p_source_name text, p_season bigint
)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = '' AS $function$
DECLARE asset text; parser text; coverage text; current_generation uuid;
BEGIN
    IF p_season IS NULL OR p_season < 1869 OR p_season > 2200 THEN
        RAISE EXCEPTION 'SDV source season must be between 1869 and 2200'
            USING ERRCODE = '22023';
    END IF;
    SELECT CASE p_source_name
        WHEN 'sdv_fpi_weekly' THEN 'ratings.espn_fpi_weekly'
        WHEN 'sdv_team_xwalk' THEN 'ref.team_id_xwalk'
        WHEN 'sdv_game_xwalk' THEN 'ref.game_id_xwalk'
        END,
        CASE p_source_name
        WHEN 'sdv_fpi_weekly' THEN 'sdv-fpi-v1'
        WHEN 'sdv_team_xwalk' THEN 'sdv-team-xwalk-v1'
        WHEN 'sdv_game_xwalk' THEN 'sdv-game-xwalk-v1'
        END
    INTO asset, parser;
    IF asset IS NULL THEN
        RAISE EXCEPTION 'source is not enrolled in the SDV season protocol'
            USING ERRCODE = '22023';
    END IF;
    coverage := 'season:' || p_season::text;
    SELECT p.generation_id INTO current_generation
    FROM meta.asset_current_generations p
    WHERE p.asset_key = asset AND p.coverage_key = coverage;
    RETURN pg_catalog.jsonb_build_object(
        'protocol', 'sdv-season-file-v1',
        'source_name', p_source_name,
        'asset_key', asset,
        'coverage_key', coverage,
        'season', p_season,
        'expected_generation_id', current_generation,
        'parser_contract', parser
    );
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_source_batch.require_source_load(p_run_id uuid)
RETURNS meta.operation_runs LANGUAGE plpgsql SET search_path = '' AS $function$
DECLARE
    run_row meta.operation_runs%ROWTYPE;
    source_name text;
    asset text;
    parser text;
    season_value bigint;
    expected_generation uuid;
    expected jsonb;
BEGIN
    IF p_run_id IS NULL THEN
        RAISE EXCEPTION 'operation run ID is required' USING ERRCODE = '22023';
    END IF;
    SELECT * INTO run_row FROM meta.operation_runs r
    WHERE r.operation_run_id = p_run_id FOR UPDATE;
    IF NOT FOUND OR run_row.recorded_by <> session_user
        OR run_row.operation_kind <> 'load' OR run_row.initiator <> 'load_flat_files'
        OR run_row.code_revision IS NOT NULL THEN
        RAISE EXCEPTION 'operation does not belong to this source publisher'
            USING ERRCODE = '22023';
    END IF;
    BEGIN
        source_name := run_row.requested_scope->>'source_name';
        season_value := (run_row.requested_scope->>'season')::bigint;
        expected_generation := CASE
            WHEN run_row.requested_scope->>'expected_generation_id' IS NULL THEN NULL::uuid
            ELSE (run_row.requested_scope->>'expected_generation_id')::uuid END;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'stored source publication scope is invalid'
            USING ERRCODE = '22023';
    END;
    SELECT CASE source_name
        WHEN 'sdv_fpi_weekly' THEN 'ratings.espn_fpi_weekly'
        WHEN 'sdv_team_xwalk' THEN 'ref.team_id_xwalk'
        WHEN 'sdv_game_xwalk' THEN 'ref.game_id_xwalk' END,
        CASE source_name
        WHEN 'sdv_fpi_weekly' THEN 'sdv-fpi-v1'
        WHEN 'sdv_team_xwalk' THEN 'sdv-team-xwalk-v1'
        WHEN 'sdv_game_xwalk' THEN 'sdv-game-xwalk-v1' END
    INTO asset, parser;
    expected := pg_catalog.jsonb_build_object(
        'protocol', 'sdv-season-file-v1', 'source_name', source_name,
        'asset_key', asset, 'coverage_key', 'season:' || season_value::text,
        'season', season_value, 'expected_generation_id', expected_generation,
        'parser_contract', parser
    );
    IF source_name IS NULL OR asset IS NULL OR parser IS NULL
        OR season_value < 1869 OR season_value > 2200
        OR run_row.requested_scope IS DISTINCT FROM expected
        OR run_row.plan_digest IS DISTINCT FROM pg_catalog.md5(expected::text) THEN
        RAISE EXCEPTION 'stored source publication scope is invalid'
            USING ERRCODE = '22023';
    END IF;
    RETURN run_row;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_source_batch.start_source_load(
    p_run_id uuid, p_plan jsonb
)
RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE source_name text; season_value bigint; current_plan jsonb;
BEGIN
    IF p_run_id IS NULL OR p_plan IS NULL OR pg_catalog.jsonb_typeof(p_plan) <> 'object' THEN
        RAISE EXCEPTION 'source publication start context is invalid'
            USING ERRCODE = '22023';
    END IF;
    BEGIN
        source_name := p_plan->>'source_name';
        season_value := (p_plan->>'season')::bigint;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'source publication plan is invalid' USING ERRCODE = '22023';
    END;
    current_plan := warehouse_source_batch.get_source_plan(source_name, season_value);
    IF p_plan IS DISTINCT FROM current_plan THEN
        RAISE EXCEPTION 'source publication plan is stale or invalid'
            USING ERRCODE = '40001';
    END IF;
    PERFORM warehouse_quota.start_operation_run(
        p_run_id, 'load', 'load_flat_files', p_plan, pg_catalog.md5(p_plan::text), NULL
    );
    PERFORM warehouse_source_batch.require_source_load(p_run_id);
    RETURN p_run_id;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_source_batch.publish_source_load(
    p_run_id uuid,
    p_generation_id uuid,
    p_source_sha256 text,
    p_rows jsonb,
    p_dlt_evidence jsonb
)
RETURNS TABLE (generation_id uuid, replayed boolean, published_rows bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE
    run_row meta.operation_runs%ROWTYPE;
    existing meta.asset_receipts%ROWTYPE;
    source_name text;
    asset text;
    parser text;
    season_basis text;
    season_value bigint;
    coverage_value text;
    expected_generation uuid;
    actual_generation uuid;
    request_digest_value text;
    row_count_value bigint;
    previous_rows bigint;
    inserted_rows bigint;
    deleted_rows bigint;
    changed_rows bigint;
    publication_time timestamptz;
    actual_load_ids jsonb;
    catalog_shape jsonb;
    expected_catalog jsonb;
    expected_keys text[];
    ledger_inserted integer;
    ledger_source_url text;
BEGIN
    IF p_run_id IS NULL OR p_generation_id IS NULL
        OR p_source_sha256 IS NULL OR p_source_sha256 !~ '^[0-9a-f]{64}$'
        OR p_rows IS NULL OR pg_catalog.jsonb_typeof(p_rows) <> 'array'
        OR p_dlt_evidence IS NULL OR pg_catalog.jsonb_typeof(p_dlt_evidence) <> 'object' THEN
        RAISE EXCEPTION 'source publication input is invalid' USING ERRCODE = '22023';
    END IF;
    IF pg_catalog.octet_length(p_rows::text) > 134217728 THEN
        RAISE EXCEPTION 'source publication payload exceeds 128 MiB'
            USING ERRCODE = '54000';
    END IF;

    run_row := warehouse_source_batch.require_source_load(p_run_id);
    source_name := run_row.requested_scope->>'source_name';
    asset := run_row.requested_scope->>'asset_key';
    parser := run_row.requested_scope->>'parser_contract';
    season_value := (run_row.requested_scope->>'season')::bigint;
    coverage_value := run_row.requested_scope->>'coverage_key';
    expected_generation := (run_row.requested_scope->>'expected_generation_id')::uuid;
    season_basis := CASE source_name
        WHEN 'sdv_fpi_weekly' THEN 'artifact_field'
        WHEN 'sdv_team_xwalk' THEN CASE p_dlt_evidence->>'artifact_origin'
            WHEN 'registered_url' THEN 'registered_artifact_name'
            WHEN 'local_file' THEN 'caller_declared' END
        WHEN 'sdv_game_xwalk' THEN CASE p_dlt_evidence->>'artifact_origin'
            WHEN 'registered_url' THEN 'registered_artifact_name'
            WHEN 'local_file' THEN 'caller_declared' END
        END;
    request_digest_value := pg_catalog.md5(pg_catalog.jsonb_build_object(
        'run_id', p_run_id, 'generation_id', p_generation_id,
        'source_sha256', p_source_sha256, 'plan', run_row.requested_scope,
        'rows', p_rows, 'dlt_evidence', p_dlt_evidence
    )::text);

    SELECT * INTO existing FROM meta.asset_receipts r
    WHERE r.generation_id = p_generation_id;
    IF FOUND THEN
        IF existing.operation_run_id <> p_run_id
            OR existing.asset_key <> asset
            OR existing.coverage_key <> coverage_value
            OR existing.outcome <> 'succeeded'
            OR existing.source_watermark <> p_source_sha256
            OR existing.request_digest <> request_digest_value
            OR run_row.outcome <> 'succeeded' THEN
            RAISE EXCEPTION
                'generation ID already has different source publication context'
                USING ERRCODE = '22023';
        END IF;
        RETURN QUERY SELECT p_generation_id, true,
            (existing.row_delta->>'published_rows')::bigint;
        RETURN;
    END IF;
    IF run_row.outcome <> 'running' THEN
        RAISE EXCEPTION 'source publication operation is terminal' USING ERRCODE = '22023';
    END IF;

    SELECT count(*) INTO row_count_value FROM pg_catalog.jsonb_array_elements(p_rows);
    IF row_count_value < 1 OR row_count_value > 100000 THEN
        RAISE EXCEPTION 'source publication requires between 1 and 100000 rows'
            USING ERRCODE = '22023';
    END IF;

    expected_keys := CASE source_name
        WHEN 'sdv_fpi_weekly' THEN ARRAY[
            '_dlt_id','_dlt_load_id','accomplishment','accomplishmentrank',
            'adjavgingamewp','adjavgingamewprank','adjlosses','adjwinpctrank',
            'adjwins','avgingamewp','avgingamewprank','avgsosrank','defefficiency',
            'defefficiencyrank','epadefense','epaoffense','epaspecialteams','fpi',
            'fpirank','gamecontrol','gamecontrolrank','last_updated','numlosses',
            'numties','numwins','offefficiency','offefficiencyrank','prob6wins',
            'probmakeplayoffs','probmaketitlegame','probwinconf','probwindiv',
            'probwinout','probwintitle','projectedl','projectedt','projectedw',
            'projectedwpctrank','rank','rankchange7days','run_date_time_key','season',
            'season_type','snapshot_is_contemporaneous','snapshot_out_of_sequence',
            'sosremainingrank','stefficiency','stefficiencyrank','team_id','topsosrank',
            'totefficiency','totefficiencyrank','week'
        ]::text[]
        WHEN 'sdv_team_xwalk' THEN ARRAY[
            '_dlt_id','_dlt_load_id','espn_abbreviation','espn_team','espn_team_id',
            'fox_abbreviation','fox_team','fox_team_id','matched_sources','norm_key',
            'season','xwalk_key','yahoo_abbreviation','yahoo_team','yahoo_team_id'
        ]::text[]
        WHEN 'sdv_game_xwalk' THEN ARRAY[
            '_dlt_id','_dlt_load_id','away_team','espn_date','espn_game_id','fox_date',
            'fox_game_id','home_team','matched_sources','matchup_key','season','yahoo_date',
            'yahoo_game_id','yahoo_global_game_id'
        ]::text[] END;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(p_rows) item
        WHERE pg_catalog.jsonb_typeof(item) <> 'object'
           OR ARRAY(SELECT k FROM pg_catalog.jsonb_object_keys(item) k
                    ORDER BY k COLLATE "C") IS DISTINCT FROM expected_keys
    ) THEN
        RAISE EXCEPTION 'source publication rows have unexpected or missing fields'
            USING ERRCODE = '22023';
    END IF;

    -- Validate the raw JSON shape before typed conversion.  PostgreSQL's
    -- bigint JSON conversion can round fractional numerics, so every integer
    -- is required to have an integral canonical representation first.
    IF EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(p_rows) item
        WHERE pg_catalog.jsonb_typeof(item->'season') <> 'number'
           OR item->>'season' !~ '^-?(0|[1-9][0-9]*)$'
           OR pg_catalog.jsonb_typeof(item->'_dlt_id') <> 'string'
           OR pg_catalog.jsonb_typeof(item->'_dlt_load_id') <> 'string'
           OR item->>'_dlt_id' = '' OR item->>'_dlt_load_id' = ''
           OR pg_catalog.length(item->>'_dlt_id') > 256
           OR pg_catalog.length(item->>'_dlt_load_id') > 256
    ) THEN
        RAISE EXCEPTION 'source publication row types are invalid' USING ERRCODE = '22023';
    END IF;

    IF source_name = 'sdv_fpi_weekly' THEN
        IF EXISTS (
            SELECT 1 FROM pg_catalog.jsonb_array_elements(p_rows) item
            WHERE pg_catalog.jsonb_typeof(item->'season_type') <> 'number'
               OR item->>'season_type' !~ '^-?(0|[1-9][0-9]*)$'
               OR pg_catalog.jsonb_typeof(item->'week') <> 'number'
               OR item->>'week' !~ '^-?(0|[1-9][0-9]*)$'
               OR pg_catalog.jsonb_typeof(item->'team_id') <> 'number'
               OR item->>'team_id' !~ '^-?(0|[1-9][0-9]*)$'
               OR (pg_catalog.jsonb_typeof(item->'run_date_time_key') = 'number'
                   AND item->>'run_date_time_key' !~ '^-?(0|[1-9][0-9]*)$')
               OR pg_catalog.jsonb_typeof(item->'run_date_time_key')
                    NOT IN ('number','null')
               OR (pg_catalog.jsonb_typeof(item->'last_updated') = 'string'
                   AND item->>'last_updated' !~
                    '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]{1,6})?)?(Z|[+-][0-9]{2}:[0-9]{2})$')
               OR pg_catalog.jsonb_typeof(item->'last_updated') NOT IN ('string','null')
               OR pg_catalog.jsonb_typeof(item->'snapshot_out_of_sequence')
                    NOT IN ('boolean','null')
               OR pg_catalog.jsonb_typeof(item->'snapshot_is_contemporaneous')
                    NOT IN ('boolean','null')
               OR EXISTS (
                    SELECT 1 FROM pg_catalog.jsonb_each(item) field
                    WHERE field.key = ANY(ARRAY[
                        'fpi','fpirank','projectedw','projectedl','projectedt',
                        'projectedwpctrank','probwinout','probwinconf',
                        'sosremainingrank','accomplishment','accomplishmentrank',
                        'adjwins','adjlosses','adjwinpctrank','gamecontrol',
                        'gamecontrolrank','adjavgingamewp','adjavgingamewprank',
                        'avgingamewp','avgingamewprank','avgsosrank','topsosrank',
                        'epaoffense','epadefense','epaspecialteams','probwindiv',
                        'probmakeplayoffs','probmaketitlegame','numwins','numlosses',
                        'numties','probwintitle','rankchange7days','prob6wins','rank',
                        'offefficiency','offefficiencyrank','defefficiency',
                        'defefficiencyrank','stefficiency','stefficiencyrank',
                        'totefficiency','totefficiencyrank']::text[])
                      AND pg_catalog.jsonb_typeof(field.value) NOT IN ('number','null')
               )
        ) THEN
            RAISE EXCEPTION 'FPI source publication row types are invalid'
                USING ERRCODE = '22023';
        END IF;
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_catalog.jsonb_to_recordset(p_rows) AS r(
                    season bigint, season_type bigint, week bigint, team_id bigint,
                    last_updated timestamptz, run_date_time_key bigint,
                    snapshot_out_of_sequence boolean, fpi double precision,
                    fpirank double precision, projectedw double precision,
                    projectedl double precision, projectedt double precision,
                    projectedwpctrank double precision, probwinout double precision,
                    probwinconf double precision, sosremainingrank double precision,
                    accomplishment double precision, accomplishmentrank double precision,
                    adjwins double precision, adjlosses double precision,
                    adjwinpctrank double precision, gamecontrol double precision,
                    gamecontrolrank double precision, adjavgingamewp double precision,
                    adjavgingamewprank double precision, avgingamewp double precision,
                    avgingamewprank double precision, avgsosrank double precision,
                    topsosrank double precision, epaoffense double precision,
                    epadefense double precision, epaspecialteams double precision,
                    probwindiv double precision, probmakeplayoffs double precision,
                    probmaketitlegame double precision, numwins double precision,
                    numlosses double precision, numties double precision,
                    probwintitle double precision, rankchange7days double precision,
                    prob6wins double precision, rank double precision,
                    offefficiency double precision, offefficiencyrank double precision,
                    defefficiency double precision, defefficiencyrank double precision,
                    stefficiency double precision, stefficiencyrank double precision,
                    totefficiency double precision, totefficiencyrank double precision,
                    snapshot_is_contemporaneous boolean, _dlt_load_id text, _dlt_id text
                )
                WHERE r.season IS DISTINCT FROM season_value
                   OR r.season_type NOT IN (2,3) OR r.week < 0 OR r.team_id <= 0
                   OR (r.last_updated IS NOT NULL AND NOT pg_catalog.isfinite(r.last_updated))
                   OR 'NaN' = ANY(ARRAY[
                        r.fpi::text,r.fpirank::text,r.projectedw::text,r.projectedl::text,
                        r.projectedt::text,r.projectedwpctrank::text,r.probwinout::text,
                        r.probwinconf::text,r.sosremainingrank::text,r.accomplishment::text,
                        r.accomplishmentrank::text,r.adjwins::text,r.adjlosses::text,
                        r.adjwinpctrank::text,r.gamecontrol::text,r.gamecontrolrank::text,
                        r.adjavgingamewp::text,r.adjavgingamewprank::text,
                        r.avgingamewp::text,r.avgingamewprank::text,r.avgsosrank::text,
                        r.topsosrank::text,r.epaoffense::text,r.epadefense::text,
                        r.epaspecialteams::text,r.probwindiv::text,r.probmakeplayoffs::text,
                        r.probmaketitlegame::text,r.numwins::text,r.numlosses::text,
                        r.numties::text,r.probwintitle::text,r.rankchange7days::text,
                        r.prob6wins::text,r.rank::text,r.offefficiency::text,
                        r.offefficiencyrank::text,r.defefficiency::text,
                        r.defefficiencyrank::text,r.stefficiency::text,
                        r.stefficiencyrank::text,r.totefficiency::text,
                        r.totefficiencyrank::text]::text[])
                   OR 'Infinity' = ANY(ARRAY[
                        r.fpi::text,r.fpirank::text,r.projectedw::text,r.projectedl::text,
                        r.projectedt::text,r.projectedwpctrank::text,r.probwinout::text,
                        r.probwinconf::text,r.sosremainingrank::text,r.accomplishment::text,
                        r.accomplishmentrank::text,r.adjwins::text,r.adjlosses::text,
                        r.adjwinpctrank::text,r.gamecontrol::text,r.gamecontrolrank::text,
                        r.adjavgingamewp::text,r.adjavgingamewprank::text,
                        r.avgingamewp::text,r.avgingamewprank::text,r.avgsosrank::text,
                        r.topsosrank::text,r.epaoffense::text,r.epadefense::text,
                        r.epaspecialteams::text,r.probwindiv::text,r.probmakeplayoffs::text,
                        r.probmaketitlegame::text,r.numwins::text,r.numlosses::text,
                        r.numties::text,r.probwintitle::text,r.rankchange7days::text,
                        r.prob6wins::text,r.rank::text,r.offefficiency::text,
                        r.offefficiencyrank::text,r.defefficiency::text,
                        r.defefficiencyrank::text,r.stefficiency::text,
                        r.stefficiencyrank::text,r.totefficiency::text,
                        r.totefficiencyrank::text]::text[])
                   OR '-Infinity' = ANY(ARRAY[
                        r.fpi::text,r.fpirank::text,r.projectedw::text,r.projectedl::text,
                        r.projectedt::text,r.projectedwpctrank::text,r.probwinout::text,
                        r.probwinconf::text,r.sosremainingrank::text,r.accomplishment::text,
                        r.accomplishmentrank::text,r.adjwins::text,r.adjlosses::text,
                        r.adjwinpctrank::text,r.gamecontrol::text,r.gamecontrolrank::text,
                        r.adjavgingamewp::text,r.adjavgingamewprank::text,
                        r.avgingamewp::text,r.avgingamewprank::text,r.avgsosrank::text,
                        r.topsosrank::text,r.epaoffense::text,r.epadefense::text,
                        r.epaspecialteams::text,r.probwindiv::text,r.probmakeplayoffs::text,
                        r.probmaketitlegame::text,r.numwins::text,r.numlosses::text,
                        r.numties::text,r.probwintitle::text,r.rankchange7days::text,
                        r.prob6wins::text,r.rank::text,r.offefficiency::text,
                        r.offefficiencyrank::text,r.defefficiency::text,
                        r.defefficiencyrank::text,r.stefficiency::text,
                        r.stefficiencyrank::text,r.totefficiency::text,
                        r.totefficiencyrank::text]::text[])
            ) THEN
                RAISE EXCEPTION 'FPI source publication contains an invalid value'
                    USING ERRCODE = '22023';
            END IF;
        EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range
            OR datetime_field_overflow THEN
            RAISE EXCEPTION 'FPI source publication contains an invalid typed value'
                USING ERRCODE = '22023';
        END;
    ELSIF source_name = 'sdv_team_xwalk' THEN
        IF EXISTS (
            SELECT 1 FROM pg_catalog.jsonb_array_elements(p_rows) item
            WHERE pg_catalog.jsonb_typeof(item->'norm_key') <> 'string'
               OR pg_catalog.jsonb_typeof(item->'xwalk_key') <> 'string'
               OR (pg_catalog.jsonb_typeof(item->'espn_team_id') = 'number'
                   AND item->>'espn_team_id' !~ '^-?(0|[1-9][0-9]*)$')
               OR pg_catalog.jsonb_typeof(item->'espn_team_id') NOT IN ('number','null')
               OR EXISTS (
                    SELECT 1 FROM pg_catalog.jsonb_each(item) field
                    WHERE field.key = ANY(ARRAY[
                        'espn_team','espn_abbreviation','fox_team_id','fox_team',
                        'fox_abbreviation','yahoo_team_id','yahoo_team',
                        'yahoo_abbreviation','matched_sources']::text[])
                      AND pg_catalog.jsonb_typeof(field.value) NOT IN ('string','null')
               )
        ) THEN
            RAISE EXCEPTION 'team crosswalk source publication row types are invalid'
                USING ERRCODE = '22023';
        END IF;
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_catalog.jsonb_to_recordset(p_rows) AS r(
                    season bigint, norm_key text, xwalk_key text, espn_team_id bigint,
                    espn_team text, espn_abbreviation text, fox_team_id text,
                    fox_team text, fox_abbreviation text, yahoo_team_id text,
                    yahoo_team text, yahoo_abbreviation text, matched_sources text,
                    _dlt_load_id text, _dlt_id text
                )
                WHERE r.season IS DISTINCT FROM season_value
                   OR pg_catalog.btrim(r.norm_key) = ''
                   OR pg_catalog.btrim(r.xwalk_key) = ''
                   OR (r.espn_team_id IS NOT NULL AND r.espn_team_id <= 0)
                   OR r.xwalk_key IS DISTINCT FROM r.norm_key || '#' ||
                        COALESCE(r.espn_team_id::text, r.fox_team_id,
                            r.yahoo_team_id, 'None')
            ) THEN
                RAISE EXCEPTION 'team crosswalk publication contains an invalid key or ID'
                    USING ERRCODE = '22023';
            END IF;
        EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
            RAISE EXCEPTION 'team crosswalk publication contains an invalid typed value'
                USING ERRCODE = '22023';
        END;
    ELSIF source_name = 'sdv_game_xwalk' THEN
        IF EXISTS (
            SELECT 1 FROM pg_catalog.jsonb_array_elements(p_rows) item
            WHERE pg_catalog.jsonb_typeof(item->'matchup_key') <> 'string'
               OR pg_catalog.jsonb_typeof(item->'yahoo_date') <> 'string'
               OR item->>'yahoo_date' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
               OR (pg_catalog.jsonb_typeof(item->'espn_game_id') = 'number'
                   AND item->>'espn_game_id' !~ '^-?(0|[1-9][0-9]*)$')
               OR pg_catalog.jsonb_typeof(item->'espn_game_id') NOT IN ('number','null')
               OR (pg_catalog.jsonb_typeof(item->'espn_date') = 'string'
                   AND item->>'espn_date' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
               OR pg_catalog.jsonb_typeof(item->'espn_date') NOT IN ('string','null')
               OR (pg_catalog.jsonb_typeof(item->'fox_date') = 'string'
                   AND item->>'fox_date' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
               OR pg_catalog.jsonb_typeof(item->'fox_date') NOT IN ('string','null')
               OR EXISTS (
                    SELECT 1 FROM pg_catalog.jsonb_each(item) field
                    WHERE field.key = ANY(ARRAY[
                        'fox_game_id','yahoo_game_id','yahoo_global_game_id',
                        'home_team','away_team','matched_sources']::text[])
                      AND pg_catalog.jsonb_typeof(field.value) NOT IN ('string','null')
               )
        ) THEN
            RAISE EXCEPTION 'game crosswalk source publication row types are invalid'
                USING ERRCODE = '22023';
        END IF;
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_catalog.jsonb_to_recordset(p_rows) AS r(
                    season bigint, matchup_key text, yahoo_date date,
                    espn_game_id bigint, fox_game_id text, yahoo_game_id text,
                    yahoo_global_game_id text, home_team text, away_team text,
                    espn_date date, fox_date date, matched_sources text,
                    _dlt_load_id text, _dlt_id text
                )
                WHERE r.season IS DISTINCT FROM season_value
                   OR pg_catalog.btrim(r.matchup_key) = ''
                   OR (r.espn_game_id IS NOT NULL AND r.espn_game_id <= 0)
            ) THEN
                RAISE EXCEPTION 'game crosswalk publication contains an invalid key or ID'
                    USING ERRCODE = '22023';
            END IF;
        EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range
            OR datetime_field_overflow THEN
            RAISE EXCEPTION 'game crosswalk publication contains an invalid typed value'
                USING ERRCODE = '22023';
        END;
    END IF;

    -- Check both the target grain and dlt's global row identity.
    IF source_name = 'sdv_fpi_weekly' THEN
        IF EXISTS (SELECT 1 FROM pg_catalog.jsonb_to_recordset(p_rows) AS r(
                season bigint, season_type bigint, week bigint, team_id bigint)
            GROUP BY season, season_type, week, team_id HAVING count(*) > 1) THEN
            RAISE EXCEPTION 'source publication payload contains duplicate keys'
                USING ERRCODE = '22023';
        END IF;
    ELSIF source_name = 'sdv_team_xwalk' THEN
        IF EXISTS (SELECT 1 FROM pg_catalog.jsonb_to_recordset(p_rows) AS r(
                season bigint, xwalk_key text)
            GROUP BY season, xwalk_key HAVING count(*) > 1) THEN
            RAISE EXCEPTION 'source publication payload contains duplicate keys'
                USING ERRCODE = '22023';
        END IF;
    ELSE
        IF EXISTS (SELECT 1 FROM pg_catalog.jsonb_to_recordset(p_rows) AS r(
                season bigint, matchup_key text, yahoo_date date)
            GROUP BY season, matchup_key, yahoo_date HAVING count(*) > 1) THEN
            RAISE EXCEPTION 'source publication payload contains duplicate keys'
                USING ERRCODE = '22023';
        END IF;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.jsonb_to_recordset(p_rows) AS r(_dlt_id text)
        GROUP BY _dlt_id HAVING count(*) > 1) THEN
        RAISE EXCEPTION 'source publication payload contains duplicate dlt identities'
            USING ERRCODE = '22023';
    END IF;

    SELECT pg_catalog.jsonb_agg(v ORDER BY v COLLATE "C") INTO actual_load_ids
    FROM (SELECT DISTINCT item->>'_dlt_load_id' AS v
        FROM pg_catalog.jsonb_array_elements(p_rows) item) ids;
    IF ARRAY(SELECT k FROM pg_catalog.jsonb_object_keys(p_dlt_evidence) k
            ORDER BY k COLLATE "C")
            IS DISTINCT FROM ARRAY[
                'artifact_origin','dlt_load_ids','parser_contract','season_basis',
                'source_rows','stage_schema'
            ]::text[]
        OR pg_catalog.jsonb_typeof(p_dlt_evidence->'artifact_origin') <> 'string'
        OR p_dlt_evidence->>'artifact_origin' NOT IN ('registered_url','local_file')
        OR pg_catalog.jsonb_typeof(p_dlt_evidence->'season_basis') <> 'string'
        OR p_dlt_evidence->>'season_basis' IS DISTINCT FROM season_basis
        OR p_dlt_evidence->>'stage_schema' IS DISTINCT FROM 'warehouse_source_stage'
        OR p_dlt_evidence->>'parser_contract' IS DISTINCT FROM parser
        OR pg_catalog.jsonb_typeof(p_dlt_evidence->'dlt_load_ids') <> 'array'
        OR p_dlt_evidence->'dlt_load_ids' IS DISTINCT FROM actual_load_ids
        OR pg_catalog.jsonb_typeof(p_dlt_evidence->'source_rows') <> 'number'
        OR p_dlt_evidence->>'source_rows' !~ '^(0|[1-9][0-9]*)$'
        OR (p_dlt_evidence->>'source_rows')::numeric <> row_count_value::numeric THEN
        RAISE EXCEPTION 'dlt evidence does not match the normalized payload'
            USING ERRCODE = '22023';
    END IF;

    IF pg_catalog.current_setting('transaction_isolation') <> 'read committed'
        OR pg_catalog.current_setting('session_replication_role') <> 'origin'
        OR COALESCE(pg_catalog.current_setting('event_triggers', true), 'on') <> 'on' THEN
        RAISE EXCEPTION 'source publication requires READ COMMITTED and active guards'
            USING ERRCODE = '25001';
    END IF;

    IF source_name = 'sdv_fpi_weekly' THEN
        LOCK TABLE ratings.espn_fpi_weekly IN EXCLUSIVE MODE;
    ELSIF source_name = 'sdv_team_xwalk' THEN
        LOCK TABLE ref.team_id_xwalk IN EXCLUSIVE MODE;
    ELSE
        LOCK TABLE ref.game_id_xwalk IN EXCLUSIVE MODE;
    END IF;
    PERFORM 1 FROM meta.asset_publication_locks l
    WHERE l.asset_key = asset FOR UPDATE;

    SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'name', a.attname, 'type', pg_catalog.format_type(a.atttypid, a.atttypmod),
        'not_null', a.attnotnull) ORDER BY a.attnum)
    INTO catalog_shape
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = pg_catalog.to_regclass(asset)
      AND a.attnum > 0 AND NOT a.attisdropped;

    expected_catalog := CASE source_name
        WHEN 'sdv_fpi_weekly' THEN '[
            {"name":"season","type":"bigint","not_null":true},
            {"name":"season_type","type":"bigint","not_null":true},
            {"name":"week","type":"bigint","not_null":true},
            {"name":"team_id","type":"bigint","not_null":true},
            {"name":"last_updated","type":"timestamp with time zone","not_null":false},
            {"name":"run_date_time_key","type":"bigint","not_null":false},
            {"name":"snapshot_out_of_sequence","type":"boolean","not_null":false},
            {"name":"fpi","type":"double precision","not_null":false},
            {"name":"fpirank","type":"double precision","not_null":false},
            {"name":"projectedw","type":"double precision","not_null":false},
            {"name":"projectedl","type":"double precision","not_null":false},
            {"name":"projectedt","type":"double precision","not_null":false},
            {"name":"projectedwpctrank","type":"double precision","not_null":false},
            {"name":"probwinout","type":"double precision","not_null":false},
            {"name":"probwinconf","type":"double precision","not_null":false},
            {"name":"sosremainingrank","type":"double precision","not_null":false},
            {"name":"accomplishment","type":"double precision","not_null":false},
            {"name":"accomplishmentrank","type":"double precision","not_null":false},
            {"name":"adjwins","type":"double precision","not_null":false},
            {"name":"adjlosses","type":"double precision","not_null":false},
            {"name":"adjwinpctrank","type":"double precision","not_null":false},
            {"name":"gamecontrol","type":"double precision","not_null":false},
            {"name":"gamecontrolrank","type":"double precision","not_null":false},
            {"name":"adjavgingamewp","type":"double precision","not_null":false},
            {"name":"adjavgingamewprank","type":"double precision","not_null":false},
            {"name":"avgingamewp","type":"double precision","not_null":false},
            {"name":"avgingamewprank","type":"double precision","not_null":false},
            {"name":"avgsosrank","type":"double precision","not_null":false},
            {"name":"topsosrank","type":"double precision","not_null":false},
            {"name":"epaoffense","type":"double precision","not_null":false},
            {"name":"epadefense","type":"double precision","not_null":false},
            {"name":"epaspecialteams","type":"double precision","not_null":false},
            {"name":"probwindiv","type":"double precision","not_null":false},
            {"name":"probmakeplayoffs","type":"double precision","not_null":false},
            {"name":"probmaketitlegame","type":"double precision","not_null":false},
            {"name":"numwins","type":"double precision","not_null":false},
            {"name":"numlosses","type":"double precision","not_null":false},
            {"name":"numties","type":"double precision","not_null":false},
            {"name":"probwintitle","type":"double precision","not_null":false},
            {"name":"rankchange7days","type":"double precision","not_null":false},
            {"name":"prob6wins","type":"double precision","not_null":false},
            {"name":"rank","type":"double precision","not_null":false},
            {"name":"offefficiency","type":"double precision","not_null":false},
            {"name":"offefficiencyrank","type":"double precision","not_null":false},
            {"name":"defefficiency","type":"double precision","not_null":false},
            {"name":"defefficiencyrank","type":"double precision","not_null":false},
            {"name":"stefficiency","type":"double precision","not_null":false},
            {"name":"stefficiencyrank","type":"double precision","not_null":false},
            {"name":"totefficiency","type":"double precision","not_null":false},
            {"name":"totefficiencyrank","type":"double precision","not_null":false},
            {"name":"snapshot_is_contemporaneous","type":"boolean","not_null":false},
            {"name":"loaded_at","type":"timestamp with time zone","not_null":true},
            {"name":"_dlt_load_id","type":"character varying","not_null":true},
            {"name":"_dlt_id","type":"character varying","not_null":true}
        ]'::jsonb
        WHEN 'sdv_team_xwalk' THEN '[
            {"name":"season","type":"bigint","not_null":true},
            {"name":"norm_key","type":"text","not_null":true},
            {"name":"xwalk_key","type":"text","not_null":true},
            {"name":"espn_team_id","type":"bigint","not_null":false},
            {"name":"espn_team","type":"text","not_null":false},
            {"name":"espn_abbreviation","type":"text","not_null":false},
            {"name":"fox_team_id","type":"text","not_null":false},
            {"name":"fox_team","type":"text","not_null":false},
            {"name":"fox_abbreviation","type":"text","not_null":false},
            {"name":"yahoo_team_id","type":"text","not_null":false},
            {"name":"yahoo_team","type":"text","not_null":false},
            {"name":"yahoo_abbreviation","type":"text","not_null":false},
            {"name":"matched_sources","type":"text","not_null":false},
            {"name":"loaded_at","type":"timestamp with time zone","not_null":true},
            {"name":"_dlt_load_id","type":"character varying","not_null":true},
            {"name":"_dlt_id","type":"character varying","not_null":true}
        ]'::jsonb
        WHEN 'sdv_game_xwalk' THEN '[
            {"name":"season","type":"bigint","not_null":true},
            {"name":"matchup_key","type":"text","not_null":true},
            {"name":"yahoo_date","type":"date","not_null":true},
            {"name":"espn_game_id","type":"bigint","not_null":false},
            {"name":"fox_game_id","type":"text","not_null":false},
            {"name":"yahoo_game_id","type":"text","not_null":false},
            {"name":"yahoo_global_game_id","type":"text","not_null":false},
            {"name":"home_team","type":"text","not_null":false},
            {"name":"away_team","type":"text","not_null":false},
            {"name":"espn_date","type":"date","not_null":false},
            {"name":"fox_date","type":"date","not_null":false},
            {"name":"matched_sources","type":"text","not_null":false},
            {"name":"loaded_at","type":"timestamp with time zone","not_null":true},
            {"name":"_dlt_load_id","type":"character varying","not_null":true},
            {"name":"_dlt_id","type":"character varying","not_null":true}
        ]'::jsonb END;

    IF catalog_shape IS DISTINCT FROM expected_catalog
        OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class c
            WHERE c.oid = pg_catalog.to_regclass(asset) AND c.relkind = 'r'
              AND c.relowner = (SELECT oid FROM pg_catalog.pg_roles
                  WHERE rolname = current_user)
              AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
              AND NOT c.relhasrules
        ) OR EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i
            WHERE i.inhrelid = pg_catalog.to_regclass(asset)
               OR i.inhparent = pg_catalog.to_regclass(asset))
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint c
            WHERE c.conrelid = pg_catalog.to_regclass(asset) AND c.contype = 'u'
              AND pg_catalog.pg_get_constraintdef(c.oid) = 'UNIQUE (_dlt_id)')
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint c
            WHERE c.conrelid = pg_catalog.to_regclass(asset) AND c.contype = 'p'
              AND pg_catalog.pg_get_constraintdef(c.oid) = CASE source_name
                  WHEN 'sdv_fpi_weekly' THEN
                      'PRIMARY KEY (season, season_type, week, team_id)'
                  WHEN 'sdv_team_xwalk' THEN 'PRIMARY KEY (season, xwalk_key)'
                  WHEN 'sdv_game_xwalk' THEN
                      'PRIMARY KEY (season, matchup_key, yahoo_date)' END) THEN
        RAISE EXCEPTION '% catalog differs from the reviewed contract', asset
            USING ERRCODE = '55000';
    END IF;
    IF (SELECT l.relation_oid FROM meta.asset_publication_locks l
            WHERE l.asset_key = asset) IS DISTINCT FROM pg_catalog.to_regclass(asset)
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t
            WHERE t.tgrelid = pg_catalog.to_regclass(asset)
              AND t.tgname = 'invalidate_sdv_source_batch_row_pointer'
              AND t.tgtype = 31 AND t.tgenabled IN ('O','A') AND NOT t.tgisinternal
              AND t.tgqual IS NULL AND t.tgnargs = 0
              AND pg_catalog.octet_length(t.tgargs) = 0
              AND t.tgattr::text = '' AND t.tgconstraint = 0
              AND NOT t.tgdeferrable AND NOT t.tginitdeferred AND t.tgparentid = 0
              AND t.tgfoid = pg_catalog.to_regprocedure(
                  'warehouse_source_batch.invalidate_row_pointer()'))
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t
            WHERE t.tgrelid = pg_catalog.to_regclass(asset)
              AND t.tgname = 'invalidate_sdv_source_batch_truncate_pointer'
              AND t.tgtype = 34 AND t.tgenabled IN ('O','A') AND NOT t.tgisinternal
              AND t.tgqual IS NULL AND t.tgnargs = 0
              AND pg_catalog.octet_length(t.tgargs) = 0
              AND t.tgattr::text = '' AND t.tgconstraint = 0
              AND NOT t.tgdeferrable AND NOT t.tginitdeferred AND t.tgparentid = 0
              AND t.tgfoid = pg_catalog.to_regprocedure(
                  'warehouse_source_batch.invalidate_truncate_pointer()'))
        OR EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t
            WHERE t.tgrelid = pg_catalog.to_regclass(asset) AND NOT t.tgisinternal
              AND t.tgname NOT IN (
                  'invalidate_sdv_source_batch_row_pointer',
                  'invalidate_sdv_source_batch_truncate_pointer'))
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger e
            WHERE e.evtname = 'warehouse_source_batch_invalidate_ddl'
              AND e.evtevent = 'ddl_command_end' AND e.evtenabled IN ('O','A')
              AND e.evttags IS NULL
              AND e.evtfoid = pg_catalog.to_regprocedure(
                  'warehouse_source_batch.invalidate_ddl()'))
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger e
            WHERE e.evtname = 'warehouse_source_batch_invalidate_drop'
              AND e.evtevent = 'sql_drop' AND e.evtenabled IN ('O','A')
              AND e.evttags IS NULL
              AND e.evtfoid = pg_catalog.to_regprocedure(
                  'warehouse_source_batch.invalidate_drop()')) THEN
        RAISE EXCEPTION 'source publication guards or asset identity are invalid'
            USING ERRCODE = '55000';
    END IF;

    SELECT p.generation_id INTO actual_generation
    FROM meta.asset_current_generations p
    WHERE p.asset_key = asset AND p.coverage_key = coverage_value FOR UPDATE;
    IF actual_generation IS DISTINCT FROM expected_generation THEN
        RAISE EXCEPTION 'source generation changed; replan required' USING ERRCODE = '40001';
    END IF;

    LOCK TABLE meta.flat_file_loads IN ROW EXCLUSIVE MODE;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
        WHERE c.oid = 'meta.flat_file_loads'::pg_catalog.regclass
          AND c.relkind = 'r'
          AND c.relowner = (SELECT oid FROM pg_catalog.pg_roles
              WHERE rolname = current_user)
          AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity)
        OR EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i
            WHERE i.inhrelid = 'meta.flat_file_loads'::pg_catalog.regclass
               OR i.inhparent = 'meta.flat_file_loads'::pg_catalog.regclass)
        OR EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t
            WHERE t.tgrelid = 'meta.flat_file_loads'::pg_catalog.regclass
              AND NOT t.tgisinternal) THEN
        RAISE EXCEPTION 'meta.flat_file_loads is not a trusted publication ledger'
            USING ERRCODE = '55000';
    END IF;

    publication_time := pg_catalog.clock_timestamp();
    IF source_name = 'sdv_fpi_weekly' THEN
        SELECT count(*) INTO previous_rows FROM ratings.espn_fpi_weekly old
        WHERE old.season = season_value;
        WITH incoming AS (
            SELECT * FROM pg_catalog.jsonb_populate_recordset(
                NULL::ratings.espn_fpi_weekly, p_rows)
        )
        SELECT
            count(*) FILTER (WHERE old.season IS NULL),
            count(*) FILTER (WHERE new.season IS NULL),
            count(*) FILTER (
                WHERE old.season IS NOT NULL AND new.season IS NOT NULL
                  AND (pg_catalog.to_jsonb(old) -
                        ARRAY['loaded_at','_dlt_load_id','_dlt_id']::text[])
                      IS DISTINCT FROM
                      (pg_catalog.to_jsonb(new) -
                        ARRAY['loaded_at','_dlt_load_id','_dlt_id']::text[]))
        INTO inserted_rows, deleted_rows, changed_rows
        FROM ratings.espn_fpi_weekly old
        FULL JOIN incoming new USING (season, season_type, week, team_id)
        WHERE COALESCE(old.season, new.season) = season_value;

        DELETE FROM ratings.espn_fpi_weekly WHERE season = season_value;
        INSERT INTO ratings.espn_fpi_weekly
        SELECT r.* FROM pg_catalog.jsonb_populate_recordset(
            NULL::ratings.espn_fpi_weekly,
            (SELECT pg_catalog.jsonb_agg(item || pg_catalog.jsonb_build_object(
                    'loaded_at', publication_time))
             FROM pg_catalog.jsonb_array_elements(p_rows) item)
        ) r;
        IF (SELECT count(*) FROM ratings.espn_fpi_weekly
                WHERE season = season_value) <> row_count_value THEN
            RAISE EXCEPTION 'published season differs from the normalized FPI file';
        END IF;
    ELSIF source_name = 'sdv_team_xwalk' THEN
        SELECT count(*) INTO previous_rows FROM ref.team_id_xwalk old
        WHERE old.season = season_value;
        WITH incoming AS (
            SELECT * FROM pg_catalog.jsonb_populate_recordset(
                NULL::ref.team_id_xwalk, p_rows)
        )
        SELECT
            count(*) FILTER (WHERE old.season IS NULL),
            count(*) FILTER (WHERE new.season IS NULL),
            count(*) FILTER (
                WHERE old.season IS NOT NULL AND new.season IS NOT NULL
                  AND (pg_catalog.to_jsonb(old) -
                        ARRAY['loaded_at','_dlt_load_id','_dlt_id']::text[])
                      IS DISTINCT FROM
                      (pg_catalog.to_jsonb(new) -
                        ARRAY['loaded_at','_dlt_load_id','_dlt_id']::text[]))
        INTO inserted_rows, deleted_rows, changed_rows
        FROM ref.team_id_xwalk old
        FULL JOIN incoming new USING (season, xwalk_key)
        WHERE COALESCE(old.season, new.season) = season_value;

        DELETE FROM ref.team_id_xwalk WHERE season = season_value;
        INSERT INTO ref.team_id_xwalk
        SELECT r.* FROM pg_catalog.jsonb_populate_recordset(
            NULL::ref.team_id_xwalk,
            (SELECT pg_catalog.jsonb_agg(item || pg_catalog.jsonb_build_object(
                    'loaded_at', publication_time))
             FROM pg_catalog.jsonb_array_elements(p_rows) item)
        ) r;
        IF (SELECT count(*) FROM ref.team_id_xwalk
                WHERE season = season_value) <> row_count_value THEN
            RAISE EXCEPTION 'published season differs from the normalized team crosswalk file';
        END IF;
    ELSE
        SELECT count(*) INTO previous_rows FROM ref.game_id_xwalk old
        WHERE old.season = season_value;
        WITH incoming AS (
            SELECT * FROM pg_catalog.jsonb_populate_recordset(
                NULL::ref.game_id_xwalk, p_rows)
        )
        SELECT
            count(*) FILTER (WHERE old.season IS NULL),
            count(*) FILTER (WHERE new.season IS NULL),
            count(*) FILTER (
                WHERE old.season IS NOT NULL AND new.season IS NOT NULL
                  AND (pg_catalog.to_jsonb(old) -
                        ARRAY['loaded_at','_dlt_load_id','_dlt_id']::text[])
                      IS DISTINCT FROM
                      (pg_catalog.to_jsonb(new) -
                        ARRAY['loaded_at','_dlt_load_id','_dlt_id']::text[]))
        INTO inserted_rows, deleted_rows, changed_rows
        FROM ref.game_id_xwalk old
        FULL JOIN incoming new USING (season, matchup_key, yahoo_date)
        WHERE COALESCE(old.season, new.season) = season_value;

        DELETE FROM ref.game_id_xwalk WHERE season = season_value;
        INSERT INTO ref.game_id_xwalk
        SELECT r.* FROM pg_catalog.jsonb_populate_recordset(
            NULL::ref.game_id_xwalk,
            (SELECT pg_catalog.jsonb_agg(item || pg_catalog.jsonb_build_object(
                    'loaded_at', publication_time))
             FROM pg_catalog.jsonb_array_elements(p_rows) item)
        ) r;
        IF (SELECT count(*) FROM ref.game_id_xwalk
                WHERE season = season_value) <> row_count_value THEN
            RAISE EXCEPTION 'published season differs from the normalized game crosswalk file';
        END IF;
    END IF;

    INSERT INTO meta.asset_receipts(
        generation_id, operation_run_id, asset_key, coverage_key, outcome, coverage,
        source_watermark, input_observations, row_delta, request_digest, published_at
    ) VALUES (
        p_generation_id, p_run_id, asset, coverage_value, 'succeeded',
        pg_catalog.jsonb_build_object(
            'complete', true, 'scope', 'season', 'season', season_value,
            'mode', 'full_file_for_season', 'source_rows', row_count_value,
            'published_rows', row_count_value),
        p_source_sha256, pg_catalog.jsonb_build_object(
            'publisher', 'load_flat_files', 'protocol', 'sdv-season-file-v1',
            'source_name', source_name, 'parser_contract', parser,
            'stage_schema', 'warehouse_source_stage',
            'artifact_origin', p_dlt_evidence->>'artifact_origin',
            'season_basis', season_basis, 'dlt_load_ids', actual_load_ids),
        pg_catalog.jsonb_build_object(
            'previous_rows', previous_rows, 'published_rows', row_count_value,
            'inserted_rows', inserted_rows, 'deleted_rows', deleted_rows,
            'changed_rows', changed_rows),
        request_digest_value, publication_time
    );
    INSERT INTO meta.asset_current_generations(asset_key, coverage_key, generation_id)
    VALUES (asset, coverage_value, p_generation_id)
    ON CONFLICT (asset_key, coverage_key) DO UPDATE
        SET generation_id = EXCLUDED.generation_id;

    ledger_source_url := CASE
        WHEN p_dlt_evidence->>'artifact_origin' = 'local_file' THEN NULL
        WHEN source_name = 'sdv_fpi_weekly' THEN
            'https://github.com/sportsdataverse/sportsdataverse-data/releases/download/'
            || 'cfb_fpi_weekly/cfb_fpi_weekly_' || season_value::text || '.parquet'
        WHEN source_name = 'sdv_team_xwalk' THEN
            'https://github.com/sportsdataverse/sportsdataverse-data/releases/download/'
            || 'cfb_crosswalk/cfb_teams_crosswalk_' || season_value::text || '.parquet'
        WHEN source_name = 'sdv_game_xwalk' THEN
            'https://github.com/sportsdataverse/sportsdataverse-data/releases/download/'
            || 'cfb_crosswalk/cfb_schedule_crosswalk_' || season_value::text || '.parquet'
        END;
    INSERT INTO meta.flat_file_loads(
        source, file_sha256, source_url, row_count, status, error
    ) VALUES (source_name || ':' || season_value::text, p_source_sha256,
        ledger_source_url, row_count_value::integer, 'loaded', NULL)
    ON CONFLICT (source, file_sha256) WHERE status = 'loaded' DO NOTHING;
    GET DIAGNOSTICS ledger_inserted = ROW_COUNT;
    IF ledger_inserted = 0 THEN
        INSERT INTO meta.flat_file_loads(
            source, file_sha256, source_url, row_count, status, error
        ) VALUES (source_name || ':' || season_value::text, p_source_sha256,
            ledger_source_url, row_count_value::integer, 'skipped', NULL);
    END IF;

    PERFORM warehouse_quota.finish_operation_run(p_run_id, 'succeeded', NULL);
    RETURN QUERY SELECT p_generation_id, false, row_count_value;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_source_batch.fail_source_load(
    p_run_id uuid, p_generation_id uuid, p_outcome text
)
RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE
    run_row meta.operation_runs%ROWTYPE;
    existing meta.asset_receipts%ROWTYPE;
    source_name text;
    asset text;
    parser text;
    season_value bigint;
    coverage_value text;
    operation_outcome text;
    digest text;
BEGIN
    IF p_generation_id IS NULL OR p_outcome IS NULL
        OR p_outcome NOT IN ('failed','deferred','blocked') THEN
        RAISE EXCEPTION 'invalid source publication failure outcome'
            USING ERRCODE = '22023';
    END IF;
    run_row := warehouse_source_batch.require_source_load(p_run_id);
    source_name := run_row.requested_scope->>'source_name';
    asset := run_row.requested_scope->>'asset_key';
    parser := run_row.requested_scope->>'parser_contract';
    season_value := (run_row.requested_scope->>'season')::bigint;
    coverage_value := run_row.requested_scope->>'coverage_key';
    operation_outcome := CASE WHEN p_outcome = 'deferred' THEN 'blocked' ELSE p_outcome END;
    digest := pg_catalog.md5(pg_catalog.jsonb_build_object(
        'run_id', p_run_id, 'generation_id', p_generation_id,
        'plan', run_row.requested_scope, 'outcome', p_outcome,
        'error_category', 'source_publication_failed')::text);

    SELECT * INTO existing FROM meta.asset_receipts r
    WHERE r.generation_id = p_generation_id;
    IF FOUND THEN
        IF existing.operation_run_id <> p_run_id OR existing.asset_key <> asset
            OR existing.coverage_key <> coverage_value OR existing.outcome <> p_outcome
            OR existing.request_digest <> digest
            OR existing.error_summary <> 'source_publication_failed'
            OR run_row.outcome <> operation_outcome THEN
            RAISE EXCEPTION 'generation ID already has different source failure context'
                USING ERRCODE = '22023';
        END IF;
        RETURN p_generation_id;
    END IF;
    IF run_row.outcome <> 'running' THEN
        RAISE EXCEPTION 'source publication operation is terminal' USING ERRCODE = '22023';
    END IF;

    INSERT INTO meta.asset_receipts(
        generation_id, operation_run_id, asset_key, coverage_key, outcome,
        coverage, input_observations, request_digest, error_summary
    ) VALUES (
        p_generation_id, p_run_id, asset, coverage_value, p_outcome,
        pg_catalog.jsonb_build_object(
            'complete', false, 'scope', 'season', 'season', season_value,
            'mode', 'full_file_for_season'),
        pg_catalog.jsonb_build_object(
            'publisher', 'load_flat_files', 'protocol', 'sdv-season-file-v1',
            'source_name', source_name, 'parser_contract', parser),
        digest, 'source_publication_failed'
    );
    PERFORM warehouse_quota.finish_operation_run(
        p_run_id, operation_outcome, 'source_publication_failed');
    RETURN p_generation_id;
END
$function$;

-- Scrub host defaults before exposing exactly the four public entrypoints.
DO $acl$
DECLARE item record; recipient text;
BEGIN
    FOR item IN
        SELECT 'SCHEMA' AS kind, pg_catalog.format('%I', n.nspname) AS name, a.grantee
        FROM pg_catalog.pg_namespace n
        CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) a
        WHERE n.nspname = 'warehouse_source_batch' AND a.grantee <> n.nspowner
        UNION
        SELECT 'FUNCTION', p.oid::pg_catalog.regprocedure::text, a.grantee
        FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))) a
        WHERE n.nspname = 'warehouse_source_batch' AND a.grantee <> p.proowner
    LOOP
        recipient := CASE WHEN item.grantee = 0 THEN 'PUBLIC'
            ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(item.grantee)) END;
        EXECUTE pg_catalog.format(
            'REVOKE ALL ON %s %s FROM %s', item.kind, item.name, recipient);
    END LOOP;
END
$acl$;

-- Host default privileges may grant PUBLIC execution to newly created routines.
-- Make the post-creation boundary explicit in addition to scrubbing every
-- catalog-visible direct grantee above.
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA warehouse_source_batch FROM PUBLIC;

REVOKE ALL ON ratings.espn_fpi_weekly, ref.team_id_xwalk, ref.game_id_xwalk,
    meta.asset_publication_locks, meta.asset_receipts,
    meta.asset_current_generations, meta.asset_receipt_inputs,
    meta.operation_runs, meta.flat_file_loads
FROM warehouse_source_publisher;
REVOKE CREATE ON SCHEMA warehouse_source_batch, warehouse_source
FROM warehouse_source_publisher;
REVOKE USAGE ON SCHEMA warehouse_source_stage, warehouse_quota,
    warehouse_publication, warehouse_refresh
FROM warehouse_source_publisher;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA warehouse_quota,
    warehouse_publication, warehouse_refresh
FROM warehouse_source_publisher;

GRANT USAGE ON SCHEMA warehouse_source, warehouse_source_batch
TO warehouse_source_publisher;
GRANT EXECUTE ON FUNCTION
    warehouse_source.get_sdv_ratings_plan(bigint),
    warehouse_source.start_sdv_ratings_load(uuid,jsonb),
    warehouse_source.publish_sdv_ratings_load(uuid,uuid,text,jsonb,jsonb),
    warehouse_source.fail_sdv_ratings_load(uuid,uuid,text),
    warehouse_source_batch.get_source_plan(text,bigint),
    warehouse_source_batch.start_source_load(uuid,jsonb),
    warehouse_source_batch.publish_source_load(uuid,uuid,text,jsonb,jsonb),
    warehouse_source_batch.fail_source_load(uuid,uuid,text)
TO warehouse_source_publisher;

DO $boundary$
DECLARE relation_name text;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'ratings.sdv_ratings_weekly','ratings.espn_fpi_weekly',
        'ref.team_id_xwalk','ref.game_id_xwalk',
        'meta.asset_publication_locks','meta.asset_receipts',
        'meta.asset_current_generations','meta.asset_receipt_inputs',
        'meta.operation_runs','meta.flat_file_loads'
    ] LOOP
        IF pg_catalog.current_setting('server_version_num')::integer >= 170000 THEN
            IF pg_catalog.has_table_privilege(
                    'warehouse_source_publisher', relation_name, 'MAINTAIN') THEN
                RAISE EXCEPTION 'warehouse_source_publisher has unsafe maintenance access to %',
                    relation_name;
            END IF;
        END IF;
        IF (SELECT c.relowner FROM pg_catalog.pg_class c
                WHERE c.oid = pg_catalog.to_regclass(relation_name)) =
                (SELECT oid FROM pg_catalog.pg_roles
                    WHERE rolname = 'warehouse_source_publisher')
            OR pg_catalog.has_table_privilege(
                'warehouse_source_publisher', relation_name,
                'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') THEN
            RAISE EXCEPTION 'warehouse_source_publisher has unsafe direct access to %',
                relation_name;
        END IF;
    END LOOP;
    IF pg_catalog.has_schema_privilege(
            'warehouse_source_publisher','warehouse_source_batch','CREATE')
        OR pg_catalog.has_schema_privilege(
            'warehouse_source_publisher','warehouse_source','CREATE')
        OR NOT pg_catalog.has_schema_privilege(
            'warehouse_source_publisher','warehouse_source_batch','USAGE')
        OR NOT pg_catalog.has_schema_privilege(
            'warehouse_source_publisher','warehouse_source','USAGE')
        OR pg_catalog.has_schema_privilege(
            'warehouse_source_publisher','warehouse_source_stage','USAGE,CREATE')
        OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname IN (
                    'warehouse_quota','warehouse_publication','warehouse_refresh')
              AND pg_catalog.has_function_privilege(
                  'warehouse_source_publisher', p.oid, 'EXECUTE'))
        OR NOT pg_catalog.has_function_privilege(
            'warehouse_source_publisher',
            'warehouse_source_batch.get_source_plan(text,bigint)', 'EXECUTE')
        OR NOT pg_catalog.has_function_privilege(
            'warehouse_source_publisher',
            'warehouse_source_batch.start_source_load(uuid,jsonb)', 'EXECUTE')
        OR NOT pg_catalog.has_function_privilege(
            'warehouse_source_publisher',
            'warehouse_source_batch.publish_source_load(uuid,uuid,text,jsonb,jsonb)',
            'EXECUTE')
        OR NOT pg_catalog.has_function_privilege(
            'warehouse_source_publisher',
            'warehouse_source_batch.fail_source_load(uuid,uuid,text)', 'EXECUTE')
        OR NOT pg_catalog.has_function_privilege(
            'warehouse_source_publisher',
            'warehouse_source.get_sdv_ratings_plan(bigint)', 'EXECUTE')
        OR NOT pg_catalog.has_function_privilege(
            'warehouse_source_publisher',
            'warehouse_source.start_sdv_ratings_load(uuid,jsonb)', 'EXECUTE')
        OR NOT pg_catalog.has_function_privilege(
            'warehouse_source_publisher',
            'warehouse_source.publish_sdv_ratings_load(uuid,uuid,text,jsonb,jsonb)',
            'EXECUTE')
        OR NOT pg_catalog.has_function_privilege(
            'warehouse_source_publisher',
            'warehouse_source.fail_sdv_ratings_load(uuid,uuid,text)', 'EXECUTE')
        OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'warehouse_source_batch'
              AND p.oid <> ALL(ARRAY[
                  'warehouse_source_batch.get_source_plan(text,bigint)'::pg_catalog.regprocedure,
                  'warehouse_source_batch.start_source_load(uuid,jsonb)'::pg_catalog.regprocedure,
                  'warehouse_source_batch.publish_source_load(uuid,uuid,text,jsonb,jsonb)'::pg_catalog.regprocedure,
                  'warehouse_source_batch.fail_source_load(uuid,uuid,text)'::pg_catalog.regprocedure
              ]::oid[])
              AND pg_catalog.has_function_privilege(
                  'warehouse_source_publisher', p.oid, 'EXECUTE'))
        OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'warehouse_source'
              AND p.oid <> ALL(ARRAY[
                  'warehouse_source.get_sdv_ratings_plan(bigint)'::pg_catalog.regprocedure,
                  'warehouse_source.start_sdv_ratings_load(uuid,jsonb)'::pg_catalog.regprocedure,
                  'warehouse_source.publish_sdv_ratings_load(uuid,uuid,text,jsonb,jsonb)'::pg_catalog.regprocedure,
                  'warehouse_source.fail_sdv_ratings_load(uuid,uuid,text)'::pg_catalog.regprocedure
              ]::oid[])
              AND pg_catalog.has_function_privilege(
                  'warehouse_source_publisher', p.oid, 'EXECUTE')) THEN
        RAISE EXCEPTION 'warehouse_source_publisher has unrelated or internal access';
    END IF;
END
$boundary$;

COMMENT ON SCHEMA warehouse_source_batch IS
    'Private fixed-allowlist publication RPCs for the three remaining SDV season files.';
COMMENT ON FUNCTION warehouse_source_batch.get_source_plan(text,bigint) IS
    'Returns the exact current-generation CAS plan for one enrolled SDV source season.';
COMMENT ON FUNCTION warehouse_source_batch.publish_source_load(uuid,uuid,text,jsonb,jsonb) IS
    'Atomically replaces one complete nonempty season for a fixed enrolled SDV source.';
