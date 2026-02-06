-- Play-level analysis functions
-- RPC functions for down/distance, field position, and red zone splits.
-- Created ad-hoc in Supabase; now tracked in version control.

CREATE OR REPLACE FUNCTION public.get_down_distance_splits(p_team TEXT, p_season INT)
RETURNS TABLE(
    down INT,
    distance_bucket TEXT,
    side TEXT,
    play_count BIGINT,
    success_rate NUMERIC,
    epa_per_play NUMERIC,
    conversion_rate NUMERIC
)
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    WITH bucketed_plays AS (
        SELECT
            p.down,
            CASE
                WHEN p.distance BETWEEN 1 AND 3 THEN '1-3'
                WHEN p.distance BETWEEN 4 AND 6 THEN '4-6'
                WHEN p.distance BETWEEN 7 AND 10 THEN '7-10'
                ELSE '11+'
            END AS distance_bucket,
            CASE
                WHEN p.offense = p_team THEN 'offense'
                ELSE 'defense'
            END AS side,
            p.ppa AS epa,
            CASE WHEN p.ppa > 0 THEN 1 ELSE 0 END AS success,
            CASE
                WHEN p.down IN (3, 4) AND p.yards_gained >= p.distance THEN 1
                ELSE 0
            END AS converted
        FROM core.plays p
        JOIN core.games g ON p.game_id = g.id
        WHERE g.season = p_season
          AND (p.offense = p_team OR p.defense = p_team)
          AND p.down IS NOT NULL
          AND p.down BETWEEN 1 AND 4
          AND p.distance IS NOT NULL
          AND p.distance > 0
    )
    SELECT
        bp.down::INT,
        bp.distance_bucket::TEXT,
        bp.side::TEXT,
        COUNT(*)::BIGINT AS play_count,
        ROUND(AVG(bp.success)::NUMERIC, 3) AS success_rate,
        ROUND(AVG(bp.epa)::NUMERIC, 3) AS epa_per_play,
        CASE
            WHEN bp.down IN (3, 4) THEN ROUND(AVG(bp.converted)::NUMERIC, 3)
            ELSE NULL
        END AS conversion_rate
    FROM bucketed_plays bp
    GROUP BY bp.down, bp.distance_bucket, bp.side
    ORDER BY bp.side, bp.down, bp.distance_bucket;
END;
$function$;

CREATE OR REPLACE FUNCTION public.get_field_position_splits(p_team TEXT, p_season INT)
RETURNS TABLE(
    zone TEXT,
    zone_label TEXT,
    side TEXT,
    play_count BIGINT,
    success_rate NUMERIC,
    epa_per_play NUMERIC,
    yards_per_play NUMERIC,
    scoring_rate NUMERIC
)
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    WITH zoned_plays AS (
        SELECT
            CASE
                WHEN p.offense = p_team THEN 'offense'
                ELSE 'defense'
            END AS side,
            CASE
                WHEN p.offense = p_team THEN
                    CASE
                        WHEN p.yardline >= 80 THEN 'own_1_20'
                        WHEN p.yardline >= 50 THEN 'own_21_50'
                        WHEN p.yardline >= 20 THEN 'opp_49_21'
                        ELSE 'opp_20_1'
                    END
                ELSE
                    CASE
                        WHEN p.yardline <= 20 THEN 'own_1_20'
                        WHEN p.yardline <= 50 THEN 'own_21_50'
                        WHEN p.yardline <= 80 THEN 'opp_49_21'
                        ELSE 'opp_20_1'
                    END
            END AS zone,
            p.ppa,
            p.yards_gained,
            CASE WHEN p.scoring = true THEN 1 ELSE 0 END AS scored,
            CASE WHEN p.ppa > 0 THEN 1 ELSE 0 END AS successful
        FROM core.plays p
        JOIN core.games g ON p.game_id = g.id
        WHERE g.season = p_season
          AND (p.offense = p_team OR p.defense = p_team)
          AND p.yardline IS NOT NULL
    )
    SELECT
        zp.zone::TEXT,
        CASE zp.zone
            WHEN 'own_1_20' THEN 'Own 1-20'
            WHEN 'own_21_50' THEN 'Own 21-50'
            WHEN 'opp_49_21' THEN 'Opp 49-21'
            WHEN 'opp_20_1' THEN 'Red Zone'
        END::TEXT AS zone_label,
        zp.side::TEXT,
        COUNT(*)::BIGINT AS play_count,
        ROUND(AVG(zp.successful)::NUMERIC, 3) AS success_rate,
        ROUND(AVG(zp.ppa)::NUMERIC, 3) AS epa_per_play,
        ROUND(AVG(zp.yards_gained)::NUMERIC, 1) AS yards_per_play,
        ROUND(AVG(zp.scored)::NUMERIC, 3) AS scoring_rate
    FROM zoned_plays zp
    GROUP BY zp.zone, zp.side
    ORDER BY zp.side,
        CASE zp.zone
            WHEN 'own_1_20' THEN 1
            WHEN 'own_21_50' THEN 2
            WHEN 'opp_49_21' THEN 3
            WHEN 'opp_20_1' THEN 4
        END;
END;
$function$;

CREATE OR REPLACE FUNCTION public.get_red_zone_splits(p_team TEXT, p_season INT)
RETURNS TABLE(
    side TEXT,
    trips BIGINT,
    touchdowns BIGINT,
    field_goals BIGINT,
    turnovers BIGINT,
    td_rate NUMERIC,
    fg_rate NUMERIC,
    scoring_rate NUMERIC,
    points_per_trip NUMERIC,
    epa_per_play NUMERIC
)
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    WITH red_zone_drives AS (
        SELECT
            CASE WHEN d.offense = p_team THEN 'offense' ELSE 'defense' END AS side,
            d.id AS drive_id,
            d.scoring,
            d.drive_result,
            d.start_yardline
        FROM core.drives d
        JOIN core.games g ON d.game_id = g.id
        WHERE g.season = p_season
          AND (d.offense = p_team OR d.defense = p_team)
          AND d.start_yardline >= 80
    ),
    red_zone_plays AS (
        SELECT
            CASE WHEN p.offense = p_team THEN 'offense' ELSE 'defense' END AS side,
            p.ppa
        FROM core.plays p
        JOIN core.games g ON p.game_id = g.id
        WHERE g.season = p_season
          AND (p.offense = p_team OR p.defense = p_team)
          AND p.yardline >= 80
    )
    SELECT
        rzd.side::TEXT,
        COUNT(DISTINCT rzd.drive_id)::BIGINT AS trips,
        COUNT(DISTINCT CASE WHEN rzd.drive_result = 'TD' THEN rzd.drive_id END)::BIGINT AS touchdowns,
        COUNT(DISTINCT CASE WHEN rzd.drive_result = 'FG' THEN rzd.drive_id END)::BIGINT AS field_goals,
        COUNT(DISTINCT CASE WHEN rzd.drive_result IN ('INT', 'FUMBLE', 'INT TD', 'FUMBLE RETURN TD') THEN rzd.drive_id END)::BIGINT AS turnovers,
        ROUND(COUNT(DISTINCT CASE WHEN rzd.drive_result = 'TD' THEN rzd.drive_id END)::NUMERIC / NULLIF(COUNT(DISTINCT rzd.drive_id), 0), 3) AS td_rate,
        ROUND(COUNT(DISTINCT CASE WHEN rzd.drive_result = 'FG' THEN rzd.drive_id END)::NUMERIC / NULLIF(COUNT(DISTINCT rzd.drive_id), 0), 3) AS fg_rate,
        ROUND(COUNT(DISTINCT CASE WHEN rzd.drive_result IN ('TD', 'FG') THEN rzd.drive_id END)::NUMERIC / NULLIF(COUNT(DISTINCT rzd.drive_id), 0), 3) AS scoring_rate,
        ROUND((COUNT(DISTINCT CASE WHEN rzd.drive_result = 'TD' THEN rzd.drive_id END) * 7 + COUNT(DISTINCT CASE WHEN rzd.drive_result = 'FG' THEN rzd.drive_id END) * 3)::NUMERIC / NULLIF(COUNT(DISTINCT rzd.drive_id), 0), 2) AS points_per_trip,
        ROUND((SELECT AVG(rzp.ppa) FROM red_zone_plays rzp WHERE rzp.side = rzd.side)::NUMERIC, 3) AS epa_per_play
    FROM red_zone_drives rzd
    GROUP BY rzd.side;
END;
$function$;
