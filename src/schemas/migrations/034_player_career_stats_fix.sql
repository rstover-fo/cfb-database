-- Migration: 034_player_career_stats_fix
--
-- Fixes REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.player_career_stats
-- failing with:
--   duplicate key value violates unique constraint "ux_player_career_stats"
--   DETAIL: Key (player_id, category, stat_type)=(...) already exists.
--
-- Root cause: 013_analytics_views.sql's original definition grouped by
-- r.player_id, r.player, r.position, r.category, r.stat_type, but the
-- unique index backing CONCURRENTLY refresh is only on
-- (player_id, category, stat_type). A player whose `player` name spelling
-- or `position` differs across seasons in stats.player_season_stats (name
-- formatting changes, position switches) produces more than one GROUP BY
-- group for the same (player_id, category, stat_type) -- i.e. more than one
-- row per unique-index key -- which is exactly what the index rejects.
--
-- Fix: aggregate player/position the same way team/conference already are
-- in this view (MAX(CASE WHEN rn = 1 THEN ... END), picking the most recent
-- season's value) instead of grouping on them directly, and drop them from
-- GROUP BY. This is the identical fix applied to 013_analytics_views.sql --
-- copied verbatim here so prod can be repaired without waiting on a full
-- migration run.
--
-- Apply via:
--   python scripts/run_migrations.py --file src/schemas/migrations/034_player_career_stats_fix.sql
--
-- Idempotent: DROP MATERIALIZED VIEW IF EXISTS + recreate.

DROP MATERIALIZED VIEW IF EXISTS analytics.player_career_stats;

CREATE MATERIALIZED VIEW analytics.player_career_stats AS
WITH ranked AS (
    SELECT
        player_id,
        player,
        position,
        team,
        conference,
        season,
        category,
        stat_type,
        stat,
        ROW_NUMBER() OVER (
            PARTITION BY player_id, category, stat_type
            ORDER BY season DESC
        ) AS rn
    FROM stats.player_season_stats
)
SELECT
    r.player_id,
    -- last player name / position (most recent season) -- player/position
    -- can vary across seasons for the same player_id (name formatting,
    -- position changes), and the unique index below is keyed on just
    -- (player_id, category, stat_type), so these must be aggregated
    -- the same way team/conference are, not grouped on directly.
    MAX(CASE WHEN r.rn = 1 THEN r.player END) AS player,
    MAX(CASE WHEN r.rn = 1 THEN r.position END) AS position,
    -- last team / conference (most recent season)
    MAX(CASE WHEN r.rn = 1 THEN r.team END) AS team,
    MAX(CASE WHEN r.rn = 1 THEN r.conference END) AS conference,
    COUNT(DISTINCT r.season)::int AS seasons_played,
    MIN(r.season)::int AS first_season,
    MAX(r.season)::int AS last_season,
    r.category,
    r.stat_type,
    SUM(r.stat::numeric) AS total_stat,
    ROUND(SUM(r.stat::numeric) / NULLIF(COUNT(DISTINCT r.season), 0), 2) AS avg_stat_per_season
FROM ranked r
GROUP BY r.player_id, r.category, r.stat_type;

-- Unique index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX ux_player_career_stats
    ON analytics.player_career_stats(player_id, category, stat_type);

-- Query indexes
CREATE INDEX ix_player_career_stats_player
    ON analytics.player_career_stats(player);
CREATE INDEX ix_player_career_stats_team
    ON analytics.player_career_stats(team);
CREATE INDEX ix_player_career_stats_position
    ON analytics.player_career_stats(position);
CREATE INDEX ix_player_career_stats_category
    ON analytics.player_career_stats(category, stat_type);
