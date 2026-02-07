-- Pivoted player season stats function
-- Returns a wide-format row per player with all stat categories as columns.
-- Created ad-hoc in Supabase; now tracked in version control.

CREATE OR REPLACE FUNCTION public.get_player_season_stats_pivoted(p_team TEXT, p_season INT)
RETURNS TABLE(
    player_id VARCHAR,
    player VARCHAR,
    "position" VARCHAR,
    pass_att INT,
    pass_comp INT,
    pass_yds INT,
    pass_td INT,
    pass_int INT,
    rush_car INT,
    rush_yds INT,
    rush_td INT,
    rec INT,
    rec_yds INT,
    rec_td INT,
    tackles INT,
    solo INT,
    tfl NUMERIC,
    sacks NUMERIC,
    interceptions INT,
    pd INT,
    fg_made INT,
    fg_att INT,
    xp_made INT,
    xp_att INT,
    points INT
)
LANGUAGE plpgsql
SET search_path = ''
AS $function$
BEGIN
    RETURN QUERY
    WITH pivoted AS (
        SELECT
            s.player_id,
            s.player,
            s.position,
            MAX(CASE WHEN s.category = 'passing' AND s.stat_type = 'ATT' THEN s.stat::INT END) AS pass_att,
            MAX(CASE WHEN s.category = 'passing' AND s.stat_type = 'COMPLETIONS' THEN s.stat::INT END) AS pass_comp,
            MAX(CASE WHEN s.category = 'passing' AND s.stat_type = 'YDS' THEN s.stat::INT END) AS pass_yds,
            MAX(CASE WHEN s.category = 'passing' AND s.stat_type = 'TD' THEN s.stat::INT END) AS pass_td,
            MAX(CASE WHEN s.category = 'passing' AND s.stat_type = 'INT' THEN s.stat::INT END) AS pass_int,
            MAX(CASE WHEN s.category = 'rushing' AND s.stat_type = 'CAR' THEN s.stat::INT END) AS rush_car,
            MAX(CASE WHEN s.category = 'rushing' AND s.stat_type = 'YDS' THEN s.stat::INT END) AS rush_yds,
            MAX(CASE WHEN s.category = 'rushing' AND s.stat_type = 'TD' THEN s.stat::INT END) AS rush_td,
            MAX(CASE WHEN s.category = 'receiving' AND s.stat_type = 'REC' THEN s.stat::INT END) AS rec,
            MAX(CASE WHEN s.category = 'receiving' AND s.stat_type = 'YDS' THEN s.stat::INT END) AS rec_yds,
            MAX(CASE WHEN s.category = 'receiving' AND s.stat_type = 'TD' THEN s.stat::INT END) AS rec_td,
            MAX(CASE WHEN s.category = 'defensive' AND s.stat_type = 'TOT' THEN s.stat::INT END) AS tackles,
            MAX(CASE WHEN s.category = 'defensive' AND s.stat_type = 'SOLO' THEN s.stat::INT END) AS solo,
            MAX(CASE WHEN s.category = 'defensive' AND s.stat_type = 'TFL' THEN s.stat::NUMERIC END) AS tfl,
            MAX(CASE WHEN s.category = 'defensive' AND s.stat_type = 'SACKS' THEN s.stat::NUMERIC END) AS sacks,
            MAX(CASE WHEN s.category = 'interceptions' AND s.stat_type = 'INT' THEN s.stat::INT END) AS interceptions,
            MAX(CASE WHEN s.category = 'defensive' AND s.stat_type = 'PD' THEN s.stat::INT END) AS pd,
            MAX(CASE WHEN s.category = 'kicking' AND s.stat_type = 'FGM' THEN s.stat::INT END) AS fg_made,
            MAX(CASE WHEN s.category = 'kicking' AND s.stat_type = 'FGA' THEN s.stat::INT END) AS fg_att,
            MAX(CASE WHEN s.category = 'kicking' AND s.stat_type = 'XPM' THEN s.stat::INT END) AS xp_made,
            MAX(CASE WHEN s.category = 'kicking' AND s.stat_type = 'XPA' THEN s.stat::INT END) AS xp_att,
            MAX(CASE WHEN s.category = 'kicking' AND s.stat_type = 'PTS' THEN s.stat::INT END) AS points
        FROM stats.player_season_stats s
        WHERE s.team = p_team AND s.season = p_season
        GROUP BY s.player_id, s.player, s.position
    )
    SELECT * FROM pivoted;
END;
$function$;
