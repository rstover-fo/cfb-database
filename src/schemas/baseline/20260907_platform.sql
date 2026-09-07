-- Disposable PostgreSQL 17 prerequisites. Platform roles are NOLOGIN stand-ins;
-- this does not provision Supabase authentication, storage, cron or secrets.
DO $roles$
DECLARE role_name text;
BEGIN
    IF current_user <> 'postgres' THEN
        RAISE EXCEPTION 'This reviewed disposable baseline requires the postgres owner';
    END IF;
    FOREACH role_name IN ARRAY ARRAY['anon','authenticated','service_role','analyst_ro','supabase_admin']
    LOOP
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname=role_name) THEN
            EXECUTE format('CREATE ROLE %I NOLOGIN',role_name);
        END IF;
    END LOOP;
END
$roles$;
GRANT analyst_ro TO anon, authenticated, service_role;
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;
