-- F14/F28 source publication: managed forward migration after 070.
--
-- This is an opt-in, season-scoped publisher for the cumulative contents of one
-- sportsdataverse season file.  The existing F19 public freshness surface stays
-- limited to its two house-Elo rows.  Runtime membership and workflow activation
-- remain separate rollout steps.

-- 068 deliberately admitted only source-wide house-Elo receipts.  Preserve that
-- rule for those assets while admitting one canonical season key for this source.
ALTER TABLE meta.asset_publication_locks
    DROP CONSTRAINT IF EXISTS asset_publication_locks_asset_key_check;
ALTER TABLE meta.asset_publication_locks
    ADD CONSTRAINT asset_publication_locks_asset_key_check CHECK (asset_key IN (
        'analytics.house_elo_game',
        'marts.house_elo_game',
        'ratings.sdv_ratings_weekly'
    ));

ALTER TABLE meta.asset_receipts
    DROP CONSTRAINT IF EXISTS asset_receipts_coverage_key_check;
ALTER TABLE meta.asset_receipts
    ADD CONSTRAINT asset_receipts_coverage_key_check CHECK (
        (asset_key IN ('analytics.house_elo_game', 'marts.house_elo_game')
            AND coverage_key = 'source-wide')
        OR
        (asset_key = 'ratings.sdv_ratings_weekly'
            AND coverage_key ~ '^season:(18(69|[7-9][0-9])|19[0-9]{2}|20[0-9]{2}|21[0-9]{2}|2200)$')
    );

ALTER TABLE meta.asset_current_generations
    DROP CONSTRAINT IF EXISTS asset_current_generations_coverage_key_check;
ALTER TABLE meta.asset_current_generations
    ADD CONSTRAINT asset_current_generations_coverage_key_check CHECK (
        (asset_key IN ('analytics.house_elo_game', 'marts.house_elo_game')
            AND coverage_key = 'source-wide')
        OR
        (asset_key = 'ratings.sdv_ratings_weekly'
            AND coverage_key ~ '^season:(18(69|[7-9][0-9])|19[0-9]{2}|20[0-9]{2}|21[0-9]{2}|2200)$')
    );

ALTER TABLE meta.asset_receipt_inputs
    DROP CONSTRAINT IF EXISTS asset_receipt_inputs_input_coverage_key_check;
ALTER TABLE meta.asset_receipt_inputs
    ADD CONSTRAINT asset_receipt_inputs_input_coverage_key_check CHECK (
        (input_asset_key IN ('analytics.house_elo_game', 'marts.house_elo_game')
            AND input_coverage_key = 'source-wide')
        OR
        (input_asset_key = 'ratings.sdv_ratings_weekly'
            AND input_coverage_key ~ '^season:(18(69|[7-9][0-9])|19[0-9]{2}|20[0-9]{2}|21[0-9]{2}|2200)$')
    );

-- Store the exact target identity.  A replacement relation may be adopted only
-- after DDL invalidation has removed every current season pointer.
DO $asset$
DECLARE
    current_oid oid := pg_catalog.to_regclass('ratings.sdv_ratings_weekly');
    current_kind "char";
    current_owner oid;
    existing meta.asset_publication_locks%ROWTYPE;
BEGIN
    IF current_oid IS NULL THEN
        RAISE EXCEPTION 'required publication relation ratings.sdv_ratings_weekly is missing';
    END IF;
    SELECT c.relkind, c.relowner INTO current_kind, current_owner
    FROM pg_catalog.pg_class c WHERE c.oid = current_oid;
    IF current_kind <> 'r'::"char"
        OR current_owner <> (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user)
        OR EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid = current_oid OR i.inhparent = current_oid)
        OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_trigger t
            WHERE t.tgrelid = current_oid AND NOT t.tgisinternal
              AND NOT (
                (t.tgname = 'invalidate_sdv_ratings_row_pointer' AND t.tgtype = 31
                    AND t.tgfoid = pg_catalog.to_regprocedure(
                        'warehouse_source.invalidate_sdv_ratings_row_pointer()'))
                OR
                (t.tgname = 'invalidate_sdv_ratings_truncate_pointer' AND t.tgtype = 34
                    AND t.tgfoid = pg_catalog.to_regprocedure(
                        'warehouse_source.invalidate_sdv_ratings_truncate_pointer()'))
              )
        ) THEN
        RAISE EXCEPTION 'ratings.sdv_ratings_weekly must be a trusted migration-owned ordinary table';
    END IF;

    SELECT * INTO existing FROM meta.asset_publication_locks l
    WHERE l.asset_key = 'ratings.sdv_ratings_weekly';
    IF NOT FOUND THEN
        INSERT INTO meta.asset_publication_locks(
            asset_key, relation_schema, relation_name, relation_oid, relation_kind
        ) VALUES ('ratings.sdv_ratings_weekly', 'ratings', 'sdv_ratings_weekly',
            current_oid, 'r'::"char");
    ELSIF existing.relation_schema IS DISTINCT FROM 'ratings'
        OR existing.relation_name IS DISTINCT FROM 'sdv_ratings_weekly'
        OR existing.relation_oid IS DISTINCT FROM current_oid
        OR existing.relation_kind IS DISTINCT FROM 'r'::"char" THEN
        IF EXISTS (SELECT 1 FROM meta.asset_current_generations p
            WHERE p.asset_key = 'ratings.sdv_ratings_weekly') THEN
            RAISE EXCEPTION 'cannot adopt replacement ratings.sdv_ratings_weekly while a current pointer exists';
        END IF;
        UPDATE meta.asset_publication_locks
        SET relation_schema = 'ratings', relation_name = 'sdv_ratings_weekly',
            relation_oid = current_oid, relation_kind = 'r'::"char"
        WHERE asset_key = 'ratings.sdv_ratings_weekly';
    END IF;
END
$asset$;

DO $flat_file_ledger$
DECLARE ledger_oid oid := pg_catalog.to_regclass('meta.flat_file_loads');
BEGIN
    IF ledger_oid IS NULL OR NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        WHERE c.oid = ledger_oid AND c.relkind = 'r'
          AND c.relowner = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user)
    ) OR EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid = ledger_oid OR i.inhparent = ledger_oid)
      OR EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t
          WHERE t.tgrelid = ledger_oid AND NOT t.tgisinternal) THEN
        RAISE EXCEPTION 'meta.flat_file_loads must be a trusted migration-owned ordinary table';
    END IF;
END
$flat_file_ledger$;

-- dlt may materialize and inspect its own staging objects here using the existing
-- wide pipeline credential.  The bounded publisher receives normalized JSON and
-- never dynamically reads a caller-selected relation from this schema.
DO $stage_namespace$
DECLARE ns record; acl record; recipient text;
BEGIN
    SELECT oid, nspowner, nspacl INTO ns FROM pg_catalog.pg_namespace
    WHERE nspname = 'warehouse_source_stage';
    IF NOT FOUND THEN
        CREATE SCHEMA warehouse_source_stage;
        SELECT oid, nspowner, nspacl INTO ns FROM pg_catalog.pg_namespace
        WHERE nspname = 'warehouse_source_stage';
    ELSIF ns.nspowner <> (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user) THEN
        RAISE EXCEPTION 'warehouse_source_stage must be owned by the migration role';
    END IF;
    FOR acl IN SELECT DISTINCT a.grantee FROM pg_catalog.aclexplode(ns.nspacl) a
        WHERE a.grantee <> ns.nspowner LOOP
        recipient := CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
            ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(acl.grantee)) END;
        EXECUTE pg_catalog.format('REVOKE ALL ON SCHEMA warehouse_source_stage FROM %s', recipient);
    END LOOP;
END
$stage_namespace$;

-- Separate routines-only namespace: adding routines to warehouse_publication
-- would violate migration 068's exact routine whitelist on reapplication.
DO $namespace$
DECLARE ns record;
BEGIN
    SELECT oid, nspowner, nspacl INTO ns FROM pg_catalog.pg_namespace
    WHERE nspname = 'warehouse_source';
    IF NOT FOUND THEN
        CREATE SCHEMA warehouse_source;
    ELSE
        IF ns.nspowner <> (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user)
            OR EXISTS (SELECT 1 FROM pg_catalog.aclexplode(ns.nspacl) a
                WHERE a.grantee <> ns.nspowner AND a.privilege_type = 'CREATE')
            OR EXISTS (SELECT 1 FROM pg_catalog.pg_type WHERE typnamespace = ns.oid)
            OR EXISTS (
                SELECT 1 FROM pg_catalog.pg_proc p WHERE p.pronamespace = ns.oid
                AND (p.proowner <> ns.nspowner OR p.prokind <> 'f'
                    OR p.provariadic <> 0 OR p.pronargdefaults <> 0
                    OR NOT COALESCE(p.oid = ANY(ARRAY[
                        pg_catalog.to_regprocedure('warehouse_source.invalidate_sdv_ratings_row_pointer()'),
                        pg_catalog.to_regprocedure('warehouse_source.invalidate_sdv_ratings_truncate_pointer()'),
                        pg_catalog.to_regprocedure('warehouse_source.invalidate_sdv_ratings_ddl()'),
                        pg_catalog.to_regprocedure('warehouse_source.invalidate_sdv_ratings_drop()'),
                        pg_catalog.to_regprocedure('warehouse_source.get_sdv_ratings_plan(bigint)'),
                        pg_catalog.to_regprocedure('warehouse_source.require_sdv_ratings_load(uuid)'),
                        pg_catalog.to_regprocedure('warehouse_source.start_sdv_ratings_load(uuid,jsonb)'),
                        pg_catalog.to_regprocedure('warehouse_source.publish_sdv_ratings_load(uuid,uuid,text,jsonb,jsonb)'),
                        pg_catalog.to_regprocedure('warehouse_source.fail_sdv_ratings_load(uuid,uuid,text)')
                    ]::oid[]), false))
            ) THEN
            RAISE EXCEPTION 'warehouse_source must be a trusted migration-owned routines namespace';
        END IF;
    END IF;
END
$namespace$;

DO $role$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles
        WHERE rolname = 'warehouse_source_publisher') THEN
        CREATE ROLE warehouse_source_publisher NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB
            NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    ELSIF EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles r
        WHERE r.rolname = 'warehouse_source_publisher'
          AND (r.rolcanlogin OR r.rolinherit OR r.rolsuper OR r.rolcreatedb
            OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls
            OR EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m
                WHERE m.member = r.oid OR m.roleid = r.oid))
    ) THEN
        RAISE EXCEPTION 'warehouse_source_publisher must be a bounded membership-free NOLOGIN role';
    END IF;
END
$role$;

-- Migration 068 originally checked every later-registered asset identity in the
-- house-Elo publisher.  Keep that protocol bounded to its own two assets so
-- SDV target DDL cannot make an unrelated house-Elo publication impossible.
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
        WHERE l.asset_key IN ('analytics.house_elo_game', 'marts.house_elo_game')
          AND l.relation_oid <> pg_catalog.to_regclass(
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
CREATE OR REPLACE FUNCTION warehouse_source.invalidate_sdv_ratings_row_pointer()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
BEGIN
    DELETE FROM meta.asset_current_generations p
    WHERE p.asset_key = 'ratings.sdv_ratings_weekly'
      AND p.coverage_key IN (
          CASE WHEN TG_OP IN ('UPDATE', 'DELETE') THEN 'season:' || OLD.season::text END,
          CASE WHEN TG_OP IN ('UPDATE', 'INSERT') THEN 'season:' || NEW.season::text END
      );
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_source.invalidate_sdv_ratings_truncate_pointer()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
BEGIN
    DELETE FROM meta.asset_current_generations p
    WHERE p.asset_key = 'ratings.sdv_ratings_weekly';
    RETURN NULL;
END
$function$;

DROP TRIGGER IF EXISTS invalidate_sdv_ratings_row_pointer ON ratings.sdv_ratings_weekly;
CREATE TRIGGER invalidate_sdv_ratings_row_pointer
    BEFORE INSERT OR UPDATE OR DELETE ON ratings.sdv_ratings_weekly
    FOR EACH ROW EXECUTE FUNCTION warehouse_source.invalidate_sdv_ratings_row_pointer();
DROP TRIGGER IF EXISTS invalidate_sdv_ratings_truncate_pointer ON ratings.sdv_ratings_weekly;
CREATE TRIGGER invalidate_sdv_ratings_truncate_pointer
    BEFORE TRUNCATE ON ratings.sdv_ratings_weekly
    FOR EACH STATEMENT EXECUTE FUNCTION warehouse_source.invalidate_sdv_ratings_truncate_pointer();

CREATE OR REPLACE FUNCTION warehouse_source.invalidate_sdv_ratings_ddl()
RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE command record; changed boolean := false;
BEGIN
    FOR command IN SELECT * FROM pg_catalog.pg_event_trigger_ddl_commands() LOOP
        IF command.object_identity LIKE 'warehouse_source.%'
            OR command.object_identity IN (
                'warehouse_source_invalidate_ddl', 'warehouse_source_invalidate_drop')
            OR command.object_identity LIKE '%invalidate_sdv_ratings_%pointer%'
            OR EXISTS (
                SELECT 1 FROM meta.asset_publication_locks l
                WHERE l.asset_key = 'ratings.sdv_ratings_weekly'
                  AND (command.objid = l.relation_oid OR command.objid =
                    pg_catalog.to_regclass(pg_catalog.format('%I.%I',
                        l.relation_schema, l.relation_name)))
            ) THEN
            changed := true;
        END IF;
    END LOOP;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i
        WHERE i.inhrelid = pg_catalog.to_regclass('ratings.sdv_ratings_weekly')
           OR i.inhparent = pg_catalog.to_regclass('ratings.sdv_ratings_weekly')) THEN
        changed := true;
    END IF;
    IF EXISTS (
        SELECT 1 FROM meta.asset_publication_locks l
        WHERE l.asset_key = 'ratings.sdv_ratings_weekly'
          AND l.relation_oid IS DISTINCT FROM pg_catalog.to_regclass(
              pg_catalog.format('%I.%I', l.relation_schema, l.relation_name))
    ) THEN
        changed := true;
    END IF;
    IF changed THEN
        DELETE FROM meta.asset_current_generations p
        WHERE p.asset_key = 'ratings.sdv_ratings_weekly';
    END IF;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_source.invalidate_sdv_ratings_drop()
RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE dropped record; changed boolean := false;
BEGIN
    FOR dropped IN SELECT * FROM pg_catalog.pg_event_trigger_dropped_objects() LOOP
        IF dropped.object_identity LIKE 'warehouse_source.%'
            OR dropped.object_identity IN (
                'warehouse_source_invalidate_ddl', 'warehouse_source_invalidate_drop')
            OR 'invalidate_sdv_ratings_row_pointer' = ANY(dropped.address_names)
            OR 'invalidate_sdv_ratings_truncate_pointer' = ANY(dropped.address_names)
            OR EXISTS (
                SELECT 1 FROM meta.asset_publication_locks l
                WHERE l.asset_key = 'ratings.sdv_ratings_weekly'
                  AND (dropped.objid = l.relation_oid OR
                    (dropped.address_names[1] = l.relation_schema
                        AND dropped.address_names[2] = l.relation_name))
            ) THEN
            changed := true;
        END IF;
    END LOOP;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i
        WHERE i.inhrelid = pg_catalog.to_regclass('ratings.sdv_ratings_weekly')
           OR i.inhparent = pg_catalog.to_regclass('ratings.sdv_ratings_weekly')) THEN
        changed := true;
    END IF;
    IF EXISTS (
        SELECT 1 FROM meta.asset_publication_locks l
        WHERE l.asset_key = 'ratings.sdv_ratings_weekly'
          AND l.relation_oid IS DISTINCT FROM pg_catalog.to_regclass(
              pg_catalog.format('%I.%I', l.relation_schema, l.relation_name))
    ) THEN
        changed := true;
    END IF;
    IF changed THEN
        DELETE FROM meta.asset_current_generations p
        WHERE p.asset_key = 'ratings.sdv_ratings_weekly';
    END IF;
END
$function$;

DROP EVENT TRIGGER IF EXISTS warehouse_source_invalidate_ddl;
CREATE EVENT TRIGGER warehouse_source_invalidate_ddl ON ddl_command_end
    EXECUTE FUNCTION warehouse_source.invalidate_sdv_ratings_ddl();
DROP EVENT TRIGGER IF EXISTS warehouse_source_invalidate_drop;
CREATE EVENT TRIGGER warehouse_source_invalidate_drop ON sql_drop
    EXECUTE FUNCTION warehouse_source.invalidate_sdv_ratings_drop();

CREATE OR REPLACE FUNCTION warehouse_source.get_sdv_ratings_plan(p_season bigint)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = '' AS $function$
DECLARE coverage text; current_generation uuid;
BEGIN
    IF p_season IS NULL OR p_season < 1869 OR p_season > 2200 THEN
        RAISE EXCEPTION 'sdv ratings season must be between 1869 and 2200'
            USING ERRCODE = '22023';
    END IF;
    coverage := 'season:' || p_season::text;
    SELECT p.generation_id INTO current_generation
    FROM meta.asset_current_generations p
    WHERE p.asset_key = 'ratings.sdv_ratings_weekly' AND p.coverage_key = coverage;
    RETURN pg_catalog.jsonb_build_object(
        'protocol', 'sdv-ratings-season-v1',
        'asset_key', 'ratings.sdv_ratings_weekly',
        'coverage_key', coverage,
        'season', p_season,
        'expected_generation_id', current_generation,
        'parser_contract', 'sdv-ratings-v1'
    );
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_source.require_sdv_ratings_load(p_run_id uuid)
RETURNS meta.operation_runs LANGUAGE plpgsql SET search_path = '' AS $function$
DECLARE run_row meta.operation_runs%ROWTYPE; expected jsonb; season_value bigint;
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
        season_value := (run_row.requested_scope->>'season')::bigint;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'stored source publication scope is invalid' USING ERRCODE = '22023';
    END;
    expected := pg_catalog.jsonb_build_object(
        'protocol', 'sdv-ratings-season-v1',
        'asset_key', 'ratings.sdv_ratings_weekly',
        'coverage_key', 'season:' || season_value::text,
        'season', season_value,
        'expected_generation_id',
            CASE WHEN run_row.requested_scope->>'expected_generation_id' IS NULL
                THEN NULL::uuid
                ELSE (run_row.requested_scope->>'expected_generation_id')::uuid END,
        'parser_contract', 'sdv-ratings-v1'
    );
    IF season_value < 1869 OR season_value > 2200
        OR run_row.requested_scope IS DISTINCT FROM expected
        OR run_row.plan_digest IS DISTINCT FROM pg_catalog.md5(expected::text) THEN
        RAISE EXCEPTION 'stored source publication scope is invalid' USING ERRCODE = '22023';
    END IF;
    RETURN run_row;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_source.start_sdv_ratings_load(
    p_run_id uuid, p_plan jsonb
)
RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE season_value bigint; current_plan jsonb;
BEGIN
    IF p_run_id IS NULL OR p_plan IS NULL OR pg_catalog.jsonb_typeof(p_plan) <> 'object' THEN
        RAISE EXCEPTION 'source publication start context is invalid' USING ERRCODE = '22023';
    END IF;
    BEGIN
        season_value := (p_plan->>'season')::bigint;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'source publication plan is invalid' USING ERRCODE = '22023';
    END;
    current_plan := warehouse_source.get_sdv_ratings_plan(season_value);
    IF p_plan IS DISTINCT FROM current_plan THEN
        RAISE EXCEPTION 'source publication plan is stale or invalid' USING ERRCODE = '40001';
    END IF;
    PERFORM warehouse_quota.start_operation_run(
        p_run_id, 'load', 'load_flat_files', p_plan, pg_catalog.md5(p_plan::text), NULL
    );
    PERFORM warehouse_source.require_sdv_ratings_load(p_run_id);
    RETURN p_run_id;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_source.publish_sdv_ratings_load(
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
    ledger_inserted integer;
    ledger_source_url text;
    expected_keys constant text[] := ARRAY[
        '_dlt_id','_dlt_load_id','adj_def_epa','adj_net','adj_off_epa','adj_st_epa',
        'def_rank','fei_def','fei_net','fei_off','games','net_rank','net_z','off_pace',
        'off_rank','season','team_id','through_week'
    ]::text[];
BEGIN
    IF p_run_id IS NULL OR p_generation_id IS NULL
        OR p_source_sha256 IS NULL OR p_source_sha256 !~ '^[0-9a-f]{64}$'
        OR p_rows IS NULL OR pg_catalog.jsonb_typeof(p_rows) <> 'array'
        OR p_dlt_evidence IS NULL OR pg_catalog.jsonb_typeof(p_dlt_evidence) <> 'object' THEN
        RAISE EXCEPTION 'source publication input is invalid' USING ERRCODE = '22023';
    END IF;
    IF pg_catalog.octet_length(p_rows::text) > 134217728 THEN
        RAISE EXCEPTION 'source publication payload exceeds 128 MiB' USING ERRCODE = '54000';
    END IF;

    run_row := warehouse_source.require_sdv_ratings_load(p_run_id);
    season_value := (run_row.requested_scope->>'season')::bigint;
    coverage_value := run_row.requested_scope->>'coverage_key';
    expected_generation := (run_row.requested_scope->>'expected_generation_id')::uuid;
    request_digest_value := pg_catalog.md5(pg_catalog.jsonb_build_object(
        'run_id', p_run_id, 'generation_id', p_generation_id,
        'source_sha256', p_source_sha256, 'plan', run_row.requested_scope,
        'rows', p_rows, 'dlt_evidence', p_dlt_evidence
    )::text);

    SELECT * INTO existing FROM meta.asset_receipts r
    WHERE r.generation_id = p_generation_id;
    IF FOUND THEN
        IF existing.operation_run_id <> p_run_id
            OR existing.asset_key <> 'ratings.sdv_ratings_weekly'
            OR existing.coverage_key <> coverage_value
            OR existing.outcome <> 'succeeded'
            OR existing.source_watermark <> p_source_sha256
            OR existing.request_digest <> request_digest_value
            OR run_row.outcome <> 'succeeded' THEN
            RAISE EXCEPTION 'generation ID already has different source publication context'
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
    IF EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(p_rows) item
        WHERE pg_catalog.jsonb_typeof(item) <> 'object'
           OR ARRAY(SELECT k FROM pg_catalog.jsonb_object_keys(item) k ORDER BY k COLLATE "C")
                IS DISTINCT FROM expected_keys
    ) THEN
        RAISE EXCEPTION 'source publication rows have unexpected or missing fields'
            USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(p_rows) item
        WHERE pg_catalog.jsonb_typeof(item->'season') <> 'number'
           OR pg_catalog.jsonb_typeof(item->'through_week') <> 'number'
           OR pg_catalog.jsonb_typeof(item->'team_id') <> 'number'
           OR pg_catalog.jsonb_typeof(item->'_dlt_id') <> 'string'
           OR pg_catalog.jsonb_typeof(item->'_dlt_load_id') <> 'string'
           OR item->>'_dlt_id' = '' OR item->>'_dlt_load_id' = ''
           OR pg_catalog.length(item->>'_dlt_id') > 256
           OR pg_catalog.length(item->>'_dlt_load_id') > 256
           OR EXISTS (
               SELECT 1 FROM pg_catalog.jsonb_each(item) field
               WHERE field.key IN ('adj_off_epa','adj_def_epa','adj_st_epa','adj_net',
                       'fei_off','fei_def','fei_net','games','off_pace','off_rank',
                       'def_rank','net_rank','net_z')
                 AND pg_catalog.jsonb_typeof(field.value) NOT IN ('number','null')
           )
    ) THEN
        RAISE EXCEPTION 'source publication row types are invalid' USING ERRCODE = '22023';
    END IF;

    BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_catalog.jsonb_to_recordset(p_rows) AS r(
                season bigint, through_week bigint, team_id bigint,
                adj_off_epa double precision, adj_def_epa double precision,
                adj_st_epa double precision, adj_net double precision,
                fei_off double precision, fei_def double precision,
                fei_net double precision, games bigint, off_pace double precision,
                off_rank bigint, def_rank bigint, net_rank bigint, net_z double precision,
                _dlt_id text, _dlt_load_id text
            )
            WHERE r.season IS DISTINCT FROM season_value OR r.through_week < 0
                OR r.team_id <= 0 OR r.games < 0
                OR r.adj_off_epa IN ('Infinity'::double precision, '-Infinity'::double precision, 'NaN'::double precision)
                OR r.adj_def_epa IN ('Infinity'::double precision, '-Infinity'::double precision, 'NaN'::double precision)
                OR r.adj_st_epa IN ('Infinity'::double precision, '-Infinity'::double precision, 'NaN'::double precision)
                OR r.adj_net IN ('Infinity'::double precision, '-Infinity'::double precision, 'NaN'::double precision)
                OR r.fei_off IN ('Infinity'::double precision, '-Infinity'::double precision, 'NaN'::double precision)
                OR r.fei_def IN ('Infinity'::double precision, '-Infinity'::double precision, 'NaN'::double precision)
                OR r.fei_net IN ('Infinity'::double precision, '-Infinity'::double precision, 'NaN'::double precision)
                OR r.off_pace IN ('Infinity'::double precision, '-Infinity'::double precision, 'NaN'::double precision)
                OR r.net_z IN ('Infinity'::double precision, '-Infinity'::double precision, 'NaN'::double precision)
        ) THEN
            RAISE EXCEPTION 'source publication contains an invalid season, key, or numeric value'
                USING ERRCODE = '22023';
        END IF;
    EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
        RAISE EXCEPTION 'source publication contains an invalid numeric value'
            USING ERRCODE = '22023';
    END;

    IF EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_to_recordset(p_rows) AS r(
            season bigint, through_week bigint, team_id bigint, _dlt_id text
        ) GROUP BY season, through_week, team_id HAVING count(*) > 1
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_to_recordset(p_rows) AS r(_dlt_id text)
        GROUP BY _dlt_id HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'source publication payload contains duplicate keys'
            USING ERRCODE = '22023';
    END IF;

    SELECT pg_catalog.jsonb_agg(v ORDER BY v COLLATE "C") INTO actual_load_ids
    FROM (SELECT DISTINCT item->>'_dlt_load_id' AS v
        FROM pg_catalog.jsonb_array_elements(p_rows) item) ids;
    IF ARRAY(SELECT k FROM pg_catalog.jsonb_object_keys(p_dlt_evidence) k
            ORDER BY k COLLATE "C")
            IS DISTINCT FROM ARRAY[
                'artifact_origin','dlt_load_ids','parser_contract','source_rows','stage_schema'
            ]::text[]
        OR pg_catalog.jsonb_typeof(p_dlt_evidence->'artifact_origin') <> 'string'
        OR p_dlt_evidence->>'artifact_origin' NOT IN ('registered_url','local_file')
        OR p_dlt_evidence->>'stage_schema' IS DISTINCT FROM 'warehouse_source_stage'
        OR p_dlt_evidence->>'parser_contract' IS DISTINCT FROM 'sdv-ratings-v1'
        OR pg_catalog.jsonb_typeof(p_dlt_evidence->'dlt_load_ids') <> 'array'
        OR p_dlt_evidence->'dlt_load_ids' IS DISTINCT FROM actual_load_ids
        OR pg_catalog.jsonb_typeof(p_dlt_evidence->'source_rows') <> 'number'
        OR (p_dlt_evidence->>'source_rows')::bigint IS DISTINCT FROM row_count_value THEN
        RAISE EXCEPTION 'dlt evidence does not match the normalized payload'
            USING ERRCODE = '22023';
    END IF;

    IF pg_catalog.current_setting('transaction_isolation') <> 'read committed'
        OR pg_catalog.current_setting('session_replication_role') <> 'origin'
        OR COALESCE(pg_catalog.current_setting('event_triggers', true), 'on') <> 'on' THEN
        RAISE EXCEPTION 'source publication requires READ COMMITTED and active guards'
            USING ERRCODE = '25001';
    END IF;

    LOCK TABLE ratings.sdv_ratings_weekly IN EXCLUSIVE MODE;
    PERFORM 1 FROM meta.asset_publication_locks l
    WHERE l.asset_key = 'ratings.sdv_ratings_weekly' FOR UPDATE;

    SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'name', a.attname, 'type', pg_catalog.format_type(a.atttypid, a.atttypmod),
        'not_null', a.attnotnull) ORDER BY a.attnum)
    INTO catalog_shape
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = 'ratings.sdv_ratings_weekly'::pg_catalog.regclass
      AND a.attnum > 0 AND NOT a.attisdropped;
    IF catalog_shape IS DISTINCT FROM '[
        {"name":"season","type":"bigint","not_null":true},
        {"name":"through_week","type":"bigint","not_null":true},
        {"name":"team_id","type":"bigint","not_null":true},
        {"name":"adj_off_epa","type":"double precision","not_null":false},
        {"name":"adj_def_epa","type":"double precision","not_null":false},
        {"name":"adj_st_epa","type":"double precision","not_null":false},
        {"name":"adj_net","type":"double precision","not_null":false},
        {"name":"fei_off","type":"double precision","not_null":false},
        {"name":"fei_def","type":"double precision","not_null":false},
        {"name":"fei_net","type":"double precision","not_null":false},
        {"name":"games","type":"bigint","not_null":false},
        {"name":"off_pace","type":"double precision","not_null":false},
        {"name":"off_rank","type":"bigint","not_null":false},
        {"name":"def_rank","type":"bigint","not_null":false},
        {"name":"net_rank","type":"bigint","not_null":false},
        {"name":"net_z","type":"double precision","not_null":false},
        {"name":"loaded_at","type":"timestamp with time zone","not_null":true},
        {"name":"_dlt_load_id","type":"character varying","not_null":true},
        {"name":"_dlt_id","type":"character varying","not_null":true}
    ]'::jsonb
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
            WHERE c.oid = 'ratings.sdv_ratings_weekly'::pg_catalog.regclass
              AND c.relkind = 'r'
              AND c.relowner = (SELECT oid FROM pg_catalog.pg_roles
                  WHERE rolname = current_user))
        OR EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i
            WHERE i.inhrelid = 'ratings.sdv_ratings_weekly'::pg_catalog.regclass
              OR i.inhparent = 'ratings.sdv_ratings_weekly'::pg_catalog.regclass)
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint c
            WHERE c.conrelid = 'ratings.sdv_ratings_weekly'::pg_catalog.regclass
              AND c.contype = 'p'
              AND pg_catalog.pg_get_constraintdef(c.oid) =
                  'PRIMARY KEY (season, through_week, team_id)')
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint c
            WHERE c.conrelid = 'ratings.sdv_ratings_weekly'::pg_catalog.regclass
              AND c.contype = 'u'
              AND pg_catalog.pg_get_constraintdef(c.oid) = 'UNIQUE (_dlt_id)') THEN
        RAISE EXCEPTION 'ratings.sdv_ratings_weekly catalog differs from the reviewed contract'
            USING ERRCODE = '55000';
    END IF;
    IF (SELECT l.relation_oid FROM meta.asset_publication_locks l
            WHERE l.asset_key = 'ratings.sdv_ratings_weekly')
            IS DISTINCT FROM pg_catalog.to_regclass('ratings.sdv_ratings_weekly')
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t
            WHERE t.tgrelid = 'ratings.sdv_ratings_weekly'::pg_catalog.regclass
              AND t.tgname = 'invalidate_sdv_ratings_row_pointer' AND t.tgtype = 31
              AND t.tgenabled IN ('O','A') AND NOT t.tgisinternal
              AND t.tgfoid = pg_catalog.to_regprocedure(
                  'warehouse_source.invalidate_sdv_ratings_row_pointer()'))
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t
            WHERE t.tgrelid = 'ratings.sdv_ratings_weekly'::pg_catalog.regclass
              AND t.tgname = 'invalidate_sdv_ratings_truncate_pointer' AND t.tgtype = 34
              AND t.tgenabled IN ('O','A') AND NOT t.tgisinternal
              AND t.tgfoid = pg_catalog.to_regprocedure(
                  'warehouse_source.invalidate_sdv_ratings_truncate_pointer()'))
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger e
            WHERE e.evtname = 'warehouse_source_invalidate_ddl'
              AND e.evtevent = 'ddl_command_end' AND e.evtenabled IN ('O','A')
              AND e.evtfoid = pg_catalog.to_regprocedure(
                  'warehouse_source.invalidate_sdv_ratings_ddl()'))
        OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger e
            WHERE e.evtname = 'warehouse_source_invalidate_drop'
              AND e.evtevent = 'sql_drop' AND e.evtenabled IN ('O','A')
              AND e.evtfoid = pg_catalog.to_regprocedure(
                  'warehouse_source.invalidate_sdv_ratings_drop()')) THEN
        RAISE EXCEPTION 'source publication guards or asset identity are invalid'
            USING ERRCODE = '55000';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t
        WHERE t.tgrelid = 'ratings.sdv_ratings_weekly'::pg_catalog.regclass
          AND NOT t.tgisinternal
          AND t.tgname NOT IN (
              'invalidate_sdv_ratings_row_pointer',
              'invalidate_sdv_ratings_truncate_pointer'
          )) THEN
        RAISE EXCEPTION 'ratings.sdv_ratings_weekly has an unexpected trigger'
            USING ERRCODE = '55000';
    END IF;

    SELECT p.generation_id INTO actual_generation
    FROM meta.asset_current_generations p
    WHERE p.asset_key = 'ratings.sdv_ratings_weekly'
      AND p.coverage_key = coverage_value FOR UPDATE;
    IF actual_generation IS DISTINCT FROM expected_generation THEN
        RAISE EXCEPTION 'source generation changed; replan required' USING ERRCODE = '40001';
    END IF;

    SELECT count(*) INTO previous_rows FROM ratings.sdv_ratings_weekly r
    WHERE r.season = season_value;
    WITH incoming AS (
        SELECT * FROM pg_catalog.jsonb_to_recordset(p_rows) AS r(
            season bigint, through_week bigint, team_id bigint,
            adj_off_epa double precision, adj_def_epa double precision,
            adj_st_epa double precision, adj_net double precision,
            fei_off double precision, fei_def double precision,
            fei_net double precision, games bigint, off_pace double precision,
            off_rank bigint, def_rank bigint, net_rank bigint, net_z double precision,
            _dlt_id text, _dlt_load_id text
        )
    )
    SELECT
        count(*) FILTER (WHERE old.season IS NULL),
        count(*) FILTER (WHERE new.season IS NULL),
        count(*) FILTER (WHERE old.season IS NOT NULL AND new.season IS NOT NULL
            AND ROW(old.adj_off_epa,old.adj_def_epa,old.adj_st_epa,old.adj_net,
                    old.fei_off,old.fei_def,old.fei_net,old.games,old.off_pace,
                    old.off_rank,old.def_rank,old.net_rank,old.net_z)
                IS DISTINCT FROM
                ROW(new.adj_off_epa,new.adj_def_epa,new.adj_st_epa,new.adj_net,
                    new.fei_off,new.fei_def,new.fei_net,new.games,new.off_pace,
                    new.off_rank,new.def_rank,new.net_rank,new.net_z))
    INTO inserted_rows, deleted_rows, changed_rows
    FROM ratings.sdv_ratings_weekly old
    FULL JOIN incoming new USING (season, through_week, team_id)
    WHERE COALESCE(old.season, new.season) = season_value;

    DELETE FROM ratings.sdv_ratings_weekly WHERE season = season_value;
    publication_time := pg_catalog.clock_timestamp();
    INSERT INTO ratings.sdv_ratings_weekly(
        season, through_week, team_id, adj_off_epa, adj_def_epa, adj_st_epa,
        adj_net, fei_off, fei_def, fei_net, games, off_pace, off_rank, def_rank,
        net_rank, net_z, loaded_at, _dlt_load_id, _dlt_id
    )
    SELECT season, through_week, team_id, adj_off_epa, adj_def_epa, adj_st_epa,
        adj_net, fei_off, fei_def, fei_net, games, off_pace, off_rank, def_rank,
        net_rank, net_z, publication_time, _dlt_load_id, _dlt_id
    FROM pg_catalog.jsonb_to_recordset(p_rows) AS r(
        season bigint, through_week bigint, team_id bigint,
        adj_off_epa double precision, adj_def_epa double precision,
        adj_st_epa double precision, adj_net double precision,
        fei_off double precision, fei_def double precision,
        fei_net double precision, games bigint, off_pace double precision,
        off_rank bigint, def_rank bigint, net_rank bigint, net_z double precision,
        _dlt_id text, _dlt_load_id text
    );
    IF (SELECT count(*) FROM ratings.sdv_ratings_weekly
            WHERE season = season_value) <> row_count_value THEN
        RAISE EXCEPTION 'published season coverage differs from the normalized file';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
        WHERE c.oid = 'meta.flat_file_loads'::pg_catalog.regclass
          AND c.relkind = 'r'
          AND c.relowner = (SELECT oid FROM pg_catalog.pg_roles
              WHERE rolname = current_user))
        OR EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i
            WHERE i.inhrelid = 'meta.flat_file_loads'::pg_catalog.regclass
              OR i.inhparent = 'meta.flat_file_loads'::pg_catalog.regclass)
        OR EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t
            WHERE t.tgrelid = 'meta.flat_file_loads'::pg_catalog.regclass
              AND NOT t.tgisinternal) THEN
        RAISE EXCEPTION 'meta.flat_file_loads is not a trusted publication ledger'
            USING ERRCODE = '55000';
    END IF;

    INSERT INTO meta.asset_receipts(
        generation_id, operation_run_id, asset_key, coverage_key, outcome, coverage,
        source_watermark, input_observations, row_delta, request_digest, published_at
    ) VALUES (
        p_generation_id, p_run_id, 'ratings.sdv_ratings_weekly', coverage_value,
        'succeeded', pg_catalog.jsonb_build_object(
            'complete', true, 'scope', 'season', 'season', season_value,
            'mode', 'full_file_for_season', 'source_rows', row_count_value,
            'published_rows', row_count_value),
        p_source_sha256, pg_catalog.jsonb_build_object(
            'publisher', 'load_flat_files', 'protocol', 'sdv-ratings-season-v1',
            'parser_contract', 'sdv-ratings-v1',
            'stage_schema', 'warehouse_source_stage',
            'artifact_origin', p_dlt_evidence->>'artifact_origin',
            'dlt_load_ids', actual_load_ids),
        pg_catalog.jsonb_build_object(
            'previous_rows', previous_rows, 'published_rows', row_count_value,
            'inserted_rows', inserted_rows, 'deleted_rows', deleted_rows,
            'changed_rows', changed_rows),
        request_digest_value, publication_time
    );
    INSERT INTO meta.asset_current_generations(asset_key, coverage_key, generation_id)
    VALUES ('ratings.sdv_ratings_weekly', coverage_value, p_generation_id)
    ON CONFLICT (asset_key, coverage_key) DO UPDATE
        SET generation_id = EXCLUDED.generation_id;

    ledger_source_url := CASE WHEN p_dlt_evidence->>'artifact_origin' = 'registered_url'
        THEN 'https://github.com/sportsdataverse/sportsdataverse-data/releases/download/'
            || 'cfb_ratings_weekly/cfb_ratings_weekly_' || season_value::text || '.parquet'
        ELSE NULL END;
    INSERT INTO meta.flat_file_loads(source, file_sha256, source_url, row_count, status, error)
    VALUES ('sdv_ratings_weekly:' || season_value::text, p_source_sha256,
        ledger_source_url,
        row_count_value::integer, 'loaded', NULL)
    ON CONFLICT (source, file_sha256) WHERE status = 'loaded' DO NOTHING;
    GET DIAGNOSTICS ledger_inserted = ROW_COUNT;
    IF ledger_inserted = 0 THEN
        INSERT INTO meta.flat_file_loads(
            source, file_sha256, source_url, row_count, status, error
        ) VALUES ('sdv_ratings_weekly:' || season_value::text, p_source_sha256,
            ledger_source_url,
            row_count_value::integer, 'skipped', NULL);
    END IF;

    PERFORM warehouse_quota.finish_operation_run(p_run_id, 'succeeded', NULL);
    RETURN QUERY SELECT p_generation_id, false, row_count_value;
END
$function$;

CREATE OR REPLACE FUNCTION warehouse_source.fail_sdv_ratings_load(
    p_run_id uuid, p_generation_id uuid, p_outcome text
)
RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $function$
DECLARE
    run_row meta.operation_runs%ROWTYPE;
    existing meta.asset_receipts%ROWTYPE;
    season_value bigint;
    coverage_value text;
    operation_outcome text;
    digest text;
BEGIN
    IF p_generation_id IS NULL OR p_outcome IS NULL
        OR p_outcome NOT IN ('failed','deferred','blocked') THEN
        RAISE EXCEPTION 'invalid source publication failure outcome' USING ERRCODE = '22023';
    END IF;
    run_row := warehouse_source.require_sdv_ratings_load(p_run_id);
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
        IF existing.operation_run_id <> p_run_id
            OR existing.asset_key <> 'ratings.sdv_ratings_weekly'
            OR existing.coverage_key <> coverage_value
            OR existing.outcome <> p_outcome
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
        p_generation_id, p_run_id, 'ratings.sdv_ratings_weekly', coverage_value,
        p_outcome, pg_catalog.jsonb_build_object(
            'complete', false, 'scope', 'season', 'season', season_value,
            'mode', 'full_file_for_season'),
        pg_catalog.jsonb_build_object(
            'publisher', 'load_flat_files', 'protocol', 'sdv-ratings-season-v1',
            'parser_contract', 'sdv-ratings-v1'),
        digest, 'source_publication_failed'
    );
    PERFORM warehouse_quota.finish_operation_run(
        p_run_id, operation_outcome, 'source_publication_failed');
    RETURN p_generation_id;
END
$function$;

-- Remove host defaults and any pre-existing grants before exposing only the four
-- bounded protocol entrypoints to the NOLOGIN runtime role.
DO $acl$
DECLARE item record; recipient text;
BEGIN
    FOR item IN
        SELECT 'SCHEMA' AS kind, pg_catalog.format('%I', n.nspname) AS name, a.grantee
        FROM pg_catalog.pg_namespace n
        CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) a
        WHERE n.nspname IN ('warehouse_source','warehouse_source_stage')
          AND a.grantee <> n.nspowner
        UNION
        SELECT 'FUNCTION', p.oid::pg_catalog.regprocedure::text, a.grantee
        FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))) a
        WHERE n.nspname = 'warehouse_source' AND a.grantee <> p.proowner
    LOOP
        recipient := CASE WHEN item.grantee = 0 THEN 'PUBLIC'
            ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(item.grantee)) END;
        EXECUTE pg_catalog.format('REVOKE ALL ON %s %s FROM %s', item.kind, item.name, recipient);
    END LOOP;
END
$acl$;

REVOKE ALL ON ratings.sdv_ratings_weekly,
    meta.asset_publication_locks, meta.asset_receipts,
    meta.asset_current_generations, meta.asset_receipt_inputs,
    meta.operation_runs, meta.flat_file_loads
FROM warehouse_source_publisher;
REVOKE USAGE ON SCHEMA warehouse_quota, warehouse_publication, warehouse_refresh,
    warehouse_source_stage FROM warehouse_source_publisher;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA warehouse_quota,
    warehouse_publication, warehouse_refresh FROM warehouse_source_publisher;

GRANT USAGE ON SCHEMA warehouse_source TO warehouse_source_publisher;
GRANT EXECUTE ON FUNCTION
    warehouse_source.get_sdv_ratings_plan(bigint),
    warehouse_source.start_sdv_ratings_load(uuid,jsonb),
    warehouse_source.publish_sdv_ratings_load(uuid,uuid,text,jsonb,jsonb),
    warehouse_source.fail_sdv_ratings_load(uuid,uuid,text)
TO warehouse_source_publisher;

DO $boundary$
DECLARE relation_name text;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'ratings.sdv_ratings_weekly','meta.asset_publication_locks',
        'meta.asset_receipts','meta.asset_current_generations',
        'meta.asset_receipt_inputs','meta.operation_runs','meta.flat_file_loads'
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
            OR pg_catalog.has_table_privilege('warehouse_source_publisher', relation_name,
                'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') THEN
            RAISE EXCEPTION 'warehouse_source_publisher has unsafe direct access to %',
                relation_name;
        END IF;
    END LOOP;
    IF pg_catalog.has_schema_privilege(
            'warehouse_source_publisher','warehouse_source','CREATE')
        OR pg_catalog.has_schema_privilege(
            'warehouse_source_publisher','warehouse_source_stage','USAGE,CREATE')
        OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname IN ('warehouse_quota','warehouse_publication','warehouse_refresh')
              AND pg_catalog.has_function_privilege(
                  'warehouse_source_publisher', p.oid, 'EXECUTE')
        ) OR EXISTS (
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
                  'warehouse_source_publisher', p.oid, 'EXECUTE')
        ) THEN
        RAISE EXCEPTION 'warehouse_source_publisher has unrelated or internal access';
    END IF;
END
$boundary$;

COMMENT ON SCHEMA warehouse_source_stage IS
    'Private dlt staging namespace for the SDV season publisher; bounded RPCs never dynamically read it.';
COMMENT ON FUNCTION warehouse_source.get_sdv_ratings_plan(bigint) IS
    'Returns the exact current generation CAS plan for one canonical SDV ratings season.';
COMMENT ON FUNCTION warehouse_source.publish_sdv_ratings_load(uuid,uuid,text,jsonb,jsonb) IS
    'Atomically replaces one complete nonempty SDV ratings season and records its private source generation.';
