-- Read-only post-adoption assertions. No table rows are emitted to Actions.
BEGIN;
SET TRANSACTION READ ONLY;
SET LOCAL statement_timeout = '60s';
DO $verify$
DECLARE caller text; relation_name text;
BEGIN
    IF (SELECT count(*) FROM warehouse_control.schema_migrations) <> 1
       OR NOT EXISTS (
           SELECT 1 FROM warehouse_control.schema_migrations
           WHERE migration_id='warehouse.production.catalog-adopted.20260907.288b4eaf817b'
             AND path='src/schemas/adoptions/20260907_production_catalog_receipt.sql'
             AND kind='immutable' AND manifest_order=1
             AND checksum='d621e7026d135f6e16aede646628624357cbc51c734dc3b381b4d441d8ab7dcb'
       ) THEN
        RAISE EXCEPTION 'Production ledger is not the single reviewed adoption receipt';
    END IF;
    IF EXISTS (SELECT 1 FROM warehouse_control.repeatable_migration_executions) THEN
        RAISE EXCEPTION 'Unexpected repeatable migration history at adoption';
    END IF;
    FOREACH caller IN ARRAY ARRAY['anon','authenticated','analyst_ro'] LOOP
        EXECUTE format('SET LOCAL ROLE %I', caller);
        IF has_schema_privilege(current_user, 'warehouse_control', 'USAGE,CREATE') THEN
            RAISE EXCEPTION 'Unexpected ledger schema access for %', caller;
        END IF;
        FOREACH relation_name IN ARRAY ARRAY[
            'schema_migrations',
            'repeatable_migration_executions',
            'repeatable_migration_executions_execution_id_seq'
        ] LOOP
            BEGIN
                EXECUTE format('SELECT * FROM warehouse_control.%I LIMIT 1', relation_name);
                RAISE EXCEPTION 'Unexpected ledger read for %', caller;
            EXCEPTION WHEN insufficient_privilege THEN NULL;
            END;
        END LOOP;
        EXECUTE 'RESET ROLE';
        RAISE NOTICE 'F06 private ledger boundary passed: role=%', caller;
    END LOOP;
    RAISE NOTICE 'F06 ledger contains exactly one verified catalog-adoption receipt; no historical migrations fabricated';
END
$verify$;
ROLLBACK;
