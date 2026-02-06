-- Player season leaders API view
-- Leaderboard of top players by season/category (passing, rushing, receiving, defense)
-- Exposed via PostgREST as /api/player_season_leaders
-- Extracted from deployed Supabase database on 2026-02-06

CREATE OR REPLACE VIEW api.player_season_leaders AS
WITH passing AS (
    SELECT
        player_season_stats.season,
        'passing'::text AS category,
        player_season_stats.player_id,
        player_season_stats.player AS player_name,
        player_season_stats.team,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'YDS'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS yards,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'TD'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS touchdowns,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'INT'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS interceptions,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'PCT'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS pct,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'ATT'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS attempts,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'COMPLETIONS'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS completions
    FROM stats.player_season_stats
    WHERE player_season_stats.category::text = 'passing'::text
    GROUP BY player_season_stats.season, player_season_stats.player_id, player_season_stats.player, player_season_stats.team
), rushing AS (
    SELECT
        player_season_stats.season,
        'rushing'::text AS category,
        player_season_stats.player_id,
        player_season_stats.player AS player_name,
        player_season_stats.team,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'YDS'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS yards,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'TD'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS touchdowns,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'CAR'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS carries,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'YPC'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS yards_per_carry,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'LONG'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS longest
    FROM stats.player_season_stats
    WHERE player_season_stats.category::text = 'rushing'::text
    GROUP BY player_season_stats.season, player_season_stats.player_id, player_season_stats.player, player_season_stats.team
), receiving AS (
    SELECT
        player_season_stats.season,
        'receiving'::text AS category,
        player_season_stats.player_id,
        player_season_stats.player AS player_name,
        player_season_stats.team,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'YDS'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS yards,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'TD'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS touchdowns,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'REC'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS receptions,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'YPR'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS yards_per_reception,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'LONG'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS longest
    FROM stats.player_season_stats
    WHERE player_season_stats.category::text = 'receiving'::text
    GROUP BY player_season_stats.season, player_season_stats.player_id, player_season_stats.player, player_season_stats.team
), defensive AS (
    SELECT
        player_season_stats.season,
        'defense'::text AS category,
        player_season_stats.player_id,
        player_season_stats.player AS player_name,
        player_season_stats.team,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'TOT'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS total_tackles,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'SOLO'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS solo_tackles,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'SACKS'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS sacks,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'TFL'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS tackles_for_loss,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'PD'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS passes_defended
    FROM stats.player_season_stats
    WHERE player_season_stats.category::text = 'defensive'::text
    GROUP BY player_season_stats.season, player_season_stats.player_id, player_season_stats.player, player_season_stats.team
)
SELECT
    passing.season,
    passing.category,
    passing.player_id,
    passing.player_name,
    passing.team,
    passing.yards,
    passing.touchdowns,
    passing.interceptions,
    passing.pct,
    passing.attempts,
    passing.completions,
    NULL::numeric AS carries,
    NULL::numeric AS yards_per_carry,
    NULL::numeric AS receptions,
    NULL::numeric AS yards_per_reception,
    NULL::numeric AS longest,
    NULL::numeric AS total_tackles,
    NULL::numeric AS solo_tackles,
    NULL::numeric AS sacks,
    NULL::numeric AS tackles_for_loss,
    NULL::numeric AS passes_defended,
    rank() OVER (PARTITION BY passing.season ORDER BY passing.yards DESC NULLS LAST) AS yards_rank
FROM passing
UNION ALL
SELECT
    rushing.season,
    rushing.category,
    rushing.player_id,
    rushing.player_name,
    rushing.team,
    rushing.yards,
    rushing.touchdowns,
    NULL::numeric AS interceptions,
    NULL::numeric AS pct,
    NULL::numeric AS attempts,
    NULL::numeric AS completions,
    rushing.carries,
    rushing.yards_per_carry,
    NULL::numeric AS receptions,
    NULL::numeric AS yards_per_reception,
    rushing.longest,
    NULL::numeric AS total_tackles,
    NULL::numeric AS solo_tackles,
    NULL::numeric AS sacks,
    NULL::numeric AS tackles_for_loss,
    NULL::numeric AS passes_defended,
    rank() OVER (PARTITION BY rushing.season ORDER BY rushing.yards DESC NULLS LAST) AS yards_rank
FROM rushing
UNION ALL
SELECT
    receiving.season,
    receiving.category,
    receiving.player_id,
    receiving.player_name,
    receiving.team,
    receiving.yards,
    receiving.touchdowns,
    NULL::numeric AS interceptions,
    NULL::numeric AS pct,
    NULL::numeric AS attempts,
    NULL::numeric AS completions,
    NULL::numeric AS carries,
    NULL::numeric AS yards_per_carry,
    receiving.receptions,
    receiving.yards_per_reception,
    receiving.longest,
    NULL::numeric AS total_tackles,
    NULL::numeric AS solo_tackles,
    NULL::numeric AS sacks,
    NULL::numeric AS tackles_for_loss,
    NULL::numeric AS passes_defended,
    rank() OVER (PARTITION BY receiving.season ORDER BY receiving.yards DESC NULLS LAST) AS yards_rank
FROM receiving
UNION ALL
SELECT
    defensive.season,
    defensive.category,
    defensive.player_id,
    defensive.player_name,
    defensive.team,
    NULL::numeric AS yards,
    NULL::numeric AS touchdowns,
    NULL::numeric AS interceptions,
    NULL::numeric AS pct,
    NULL::numeric AS attempts,
    NULL::numeric AS completions,
    NULL::numeric AS carries,
    NULL::numeric AS yards_per_carry,
    NULL::numeric AS receptions,
    NULL::numeric AS yards_per_reception,
    NULL::numeric AS longest,
    defensive.total_tackles,
    defensive.solo_tackles,
    defensive.sacks,
    defensive.tackles_for_loss,
    defensive.passes_defended,
    rank() OVER (PARTITION BY defensive.season ORDER BY defensive.total_tackles DESC NULLS LAST) AS yards_rank
FROM defensive;

COMMENT ON VIEW api.player_season_leaders IS 'Player season leaderboards by category (passing, rushing, receiving, defense) with yards_rank';
