-- Data freshness: track when each key table was last loaded and whether it's stale
-- Grain: schema_name + table_name (one row per tracked table)
-- Sources: pg_stat_user_tables activity timestamps + reltuples estimates
--
-- Two failure modes this guards against (2026-08-31/09-01 incident):
--   1. Stats-reset blindness. A Postgres crash-restart (2026-08-31 00:26 UTC)
--      zeroes pg_stat_user_tables: last_vacuum/autovacuum/analyze/autoanalyze
--      all go NULL even though the table is being written to right now (e.g.
--      betting.lines took 616 inserts on 2026-09-01 with no recorded activity
--      timestamp). Naive GREATEST-of-four-timestamps reports these as stale
--      indefinitely, even fresh, until the next natural VACUUM/ANALYZE lands.
--      Fix: when a table has recorded writes (n_tup_ins/upd/del > 0) but no
--      activity timestamp, fall back to pg_postmaster_start_time() -- the
--      table cannot have been idle any longer than the server has been up.
--   2. Partitioned-parent blindness. core.plays is a partitioned parent
--      (relkind 'p'). All real writes and autovacuum/autoanalyze activity
--      land on its partitions (core.plays_y2026, etc); the parent's OWN
--      pg_stat_user_tables row never gets a timestamp from autovacuum in
--      practice (autovacuum analyzes the parent only after accumulated
--      cross-partition changes clear a threshold, which a stats reset also
--      zeroes out). Fix: aggregate by pg_partition_root() so a partition's
--      autoanalyze rolls up to its parent. Note the parent's OWN
--      pg_stat_user_tables row (PG14+) already carries a rolled-up
--      reltuples estimate independent of its children's -- it is excluded
--      from the source set below so its reltuples/writes aren't double
--      counted on top of the children's.

DROP MATERIALIZED VIEW IF EXISTS marts.data_freshness CASCADE;

CREATE MATERIALIZED VIEW marts.data_freshness AS
WITH tracked_tables AS (
    SELECT * FROM (VALUES
        ('ref', 'teams', 'static'),
        ('ref', 'conferences', 'static'),
        ('ref', 'venues', 'static'),
        ('ref', 'coaches', 'seasonal'),
        ('core', 'games', 'weekly'),
        ('core', 'drives', 'weekly'),
        ('core', 'plays', 'weekly'),
        ('core', 'game_team_stats', 'weekly'),
        ('core', 'game_player_stats', 'weekly'),
        ('core', 'roster', 'seasonal'),
        ('stats', 'player_season_stats', 'seasonal'),
        ('stats', 'team_season_stats', 'seasonal'),
        ('ratings', 'sp_ratings', 'weekly'),
        ('ratings', 'elo_ratings', 'weekly'),
        ('ratings', 'fpi_ratings', 'seasonal'),
        ('ratings', 'srs_ratings', 'seasonal'),
        ('ratings', 'core_ratings', 'weekly'),
        ('recruiting', 'recruits', 'seasonal'),
        ('recruiting', 'team_recruiting', 'seasonal'),
        ('recruiting', 'transfer_portal', 'seasonal'),
        ('betting', 'lines', 'weekly'),
        ('draft', 'draft_picks', 'seasonal'),
        -- Was ('metrics', 'predicted_points', 'weekly') -- that table never
        -- existed; aspirational name for what shipped as metrics.ppa_predicted,
        -- the /ppa/predicted static model grid loaded once by the run.py-only
        -- metrics_ppa_predicted source, not refreshed weekly. Every apply of
        -- this file with the old entry made daily verify_load.py fail
        -- unconditionally (LEFT JOIN miss -> last_activity NULL -> stale).
        ('metrics', 'ppa_predicted', 'static'),
        ('metrics', 'win_probability', 'weekly')
    ) AS t(schema_name, table_name, expected_refresh_frequency)
),
table_stats AS (
    -- relkind <> 'p': exclude partitioned-parent shell rows. A partitioned
    -- parent (e.g. core.plays) gets its own pg_stat_user_tables row on
    -- PG14+, but it's a rollup pg_class.reltuples estimate maintained
    -- independently of the children -- summing it alongside the children
    -- would roughly double-count row_count. The parent carries no write
    -- counters of its own (n_tup_ins/upd/del stay 0; all DML is attributed
    -- to the partition actually written) and no activity timestamp of its
    -- own in practice, so excluding it loses nothing.
    SELECT
        root.relnamespace::regnamespace::text AS schema_name,
        root.relname AS table_name,
        SUM(GREATEST(c.reltuples, 0))::bigint AS row_count,
        MAX(GREATEST(
            s.last_vacuum, s.last_autovacuum,
            s.last_analyze, s.last_autoanalyze
        )) AS last_activity_recorded,
        SUM(s.n_tup_ins + s.n_tup_upd + s.n_tup_del) AS writes_since_reset
    FROM pg_stat_user_tables s
    JOIN pg_class c ON s.relid = c.oid
    JOIN pg_class root
        ON root.oid = COALESCE(pg_partition_root(s.relid::regclass), s.relid::regclass)::oid
    WHERE c.relkind <> 'p'
    GROUP BY root.relnamespace, root.relname
),
freshness AS (
    SELECT
        tt.schema_name,
        tt.table_name,
        COALESCE(ts.row_count, 0) AS row_count,
        -- A table with recorded writes since the last stats reset was active
        -- no earlier than the restart, so pg_postmaster_start_time() is an
        -- honest floor when the reset wiped the real timestamp.
        COALESCE(
            ts.last_activity_recorded,
            CASE WHEN ts.writes_since_reset > 0 THEN pg_postmaster_start_time() END
        ) AS last_activity,
        tt.expected_refresh_frequency
    FROM tracked_tables tt
    LEFT JOIN table_stats ts
        ON ts.schema_name = tt.schema_name
        AND ts.table_name = tt.table_name
)
SELECT
    schema_name,
    table_name,
    row_count,
    last_activity,
    expected_refresh_frequency,
    CASE
        WHEN last_activity IS NOT NULL
        THEN ROUND(EXTRACT(EPOCH FROM (now() - last_activity)) / 86400.0, 1)
    END AS days_since_activity,
    CASE
        WHEN expected_refresh_frequency = 'static' THEN false
        WHEN expected_refresh_frequency = 'weekly'
            AND (last_activity IS NULL
                 OR last_activity < now() - interval '14 days') THEN true
        WHEN expected_refresh_frequency = 'seasonal'
            AND (last_activity IS NULL
                 OR last_activity < now() - interval '90 days') THEN true
        ELSE false
    END AS is_stale
FROM freshness
ORDER BY schema_name, table_name;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.data_freshness (schema_name, table_name);

-- Re-grant on every apply: this file DROPs the matview, which discards its
-- grants (no ALTER DEFAULT PRIVILEGES for the PostgREST roles in marts), and
-- public.get_data_freshness() is a plain SQL function executing as the CALLER,
-- so anon/authenticated need direct SELECT here for the RPC to work. Do NOT
-- fix a lost grant by re-running migrations/grant_read_access_for_security_invoker.sql
-- -- its blanket GRANT ... ALL TABLES IN SCHEMA analytics would re-grant
-- analytics.ep_states, undoing the deliberate revoke in api/043_expected_points.sql.
GRANT SELECT ON marts.data_freshness TO anon, authenticated;
