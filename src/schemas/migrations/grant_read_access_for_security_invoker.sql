-- Migration: grant_read_access_for_security_invoker
-- Applied: 2026-02-07
--
-- Fix: After converting public views from SECURITY DEFINER to SECURITY INVOKER,
-- the anon role could no longer read underlying tables in schemas it didn't have
-- USAGE on. This grants USAGE + SELECT on all data schemas to anon/authenticated
-- while revoking DML to maintain read-only access.

-- Grant USAGE on all data schemas
GRANT USAGE ON SCHEMA ref TO anon, authenticated;
GRANT USAGE ON SCHEMA ratings TO anon, authenticated;
GRANT USAGE ON SCHEMA recruiting TO anon, authenticated;
GRANT USAGE ON SCHEMA stats TO anon, authenticated;
GRANT USAGE ON SCHEMA marts TO anon, authenticated;
GRANT USAGE ON SCHEMA core TO anon, authenticated;
GRANT USAGE ON SCHEMA api TO anon, authenticated;
GRANT USAGE ON SCHEMA analytics TO anon, authenticated;
GRANT USAGE ON SCHEMA betting TO anon, authenticated;
GRANT USAGE ON SCHEMA draft TO anon, authenticated;

-- Grant SELECT on all tables in each schema
GRANT SELECT ON ALL TABLES IN SCHEMA ref TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA ratings TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA recruiting TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA stats TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA api TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA betting TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA draft TO anon, authenticated;

-- Revoke DML on all newly exposed schemas (read-only database)
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA ref FROM anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA ratings FROM anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA recruiting FROM anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA stats FROM anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA marts FROM anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA core FROM anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA api FROM anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA analytics FROM anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA betting FROM anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA draft FROM anon, authenticated;
