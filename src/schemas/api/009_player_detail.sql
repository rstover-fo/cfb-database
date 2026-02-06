-- Player detail API view
-- Comprehensive player profile: roster info, recruiting, season stats (pass/rush/rec/def), and PPA
-- Exposed via PostgREST as /api/player_detail
-- Extracted from deployed Supabase database on 2026-02-06

CREATE OR REPLACE VIEW api.player_detail AS
WITH player_passing AS (
    SELECT
        player_season_stats.player_id,
        player_season_stats.season,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'ATT'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS pass_att,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'COMPLETIONS'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS pass_cmp,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'YDS'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS pass_yds,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'TD'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS pass_td,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'INT'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS pass_int,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'PCT'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS pass_pct
    FROM stats.player_season_stats
    WHERE player_season_stats.category::text = 'passing'::text
    GROUP BY player_season_stats.player_id, player_season_stats.season
), player_rushing AS (
    SELECT
        player_season_stats.player_id,
        player_season_stats.season,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'CAR'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rush_car,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'YDS'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rush_yds,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'TD'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rush_td,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'YPC'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rush_ypc
    FROM stats.player_season_stats
    WHERE player_season_stats.category::text = 'rushing'::text
    GROUP BY player_season_stats.player_id, player_season_stats.season
), player_receiving AS (
    SELECT
        player_season_stats.player_id,
        player_season_stats.season,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'REC'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rec,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'YDS'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rec_yds,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'TD'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rec_td,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'YPR'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rec_ypr
    FROM stats.player_season_stats
    WHERE player_season_stats.category::text = 'receiving'::text
    GROUP BY player_season_stats.player_id, player_season_stats.season
), player_defense AS (
    SELECT
        player_season_stats.player_id,
        player_season_stats.season,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'TOT'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS tackles,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'SACKS'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS sacks,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'TFL'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS tfl,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'PD'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS pass_def
    FROM stats.player_season_stats
    WHERE player_season_stats.category::text = 'defensive'::text
    GROUP BY player_season_stats.player_id, player_season_stats.season
)
SELECT
    r.id AS player_id,
    (r.first_name::text || ' '::text) || r.last_name::text AS name,
    r.team,
    r."position",
    r.year AS season,
    r.height,
    r.weight,
    r.jersey,
    r.home_city,
    r.home_state,
    rec.stars,
    rec.rating AS recruit_rating,
    rec.ranking AS national_ranking,
    rec.year AS recruit_class,
    pp.pass_att,
    pp.pass_cmp,
    pp.pass_yds,
    pp.pass_td,
    pp.pass_int,
    pp.pass_pct,
    pr.rush_car,
    pr.rush_yds,
    pr.rush_td,
    pr.rush_ypc,
    prv.rec,
    prv.rec_yds,
    prv.rec_td,
    prv.rec_ypr,
    pd.tackles,
    pd.sacks,
    pd.tfl,
    pd.pass_def,
    ppa.average_ppa__all AS ppa_avg,
    ppa.total_ppa__all AS ppa_total
FROM core.roster r
LEFT JOIN recruiting.recruits rec ON rec.athlete_id::text = r.id::text
LEFT JOIN player_passing pp ON pp.player_id::text = r.id::text AND pp.season = r.year
LEFT JOIN player_rushing pr ON pr.player_id::text = r.id::text AND pr.season = r.year
LEFT JOIN player_receiving prv ON prv.player_id::text = r.id::text AND prv.season = r.year
LEFT JOIN player_defense pd ON pd.player_id::text = r.id::text AND pd.season = r.year
LEFT JOIN metrics.ppa_players_season ppa ON ppa.id::text = r.id::text AND ppa.season = r.year;

COMMENT ON VIEW api.player_detail IS 'Comprehensive player profile with roster info, recruiting data, season stats, and PPA metrics';
