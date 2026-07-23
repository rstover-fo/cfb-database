-- Play-level analysis functions
-- RPC functions for down/distance, field position, and red zone splits.
-- Created ad-hoc in Supabase; now tracked in version control.
-- Refactored: get_down_distance_splits and get_field_position_splits now use
-- marts.play_epa instead of core.plays JOIN core.games for ~10x performance.

CREATE OR REPLACE FUNCTION public.get_down_distance_splits(p_team text, p_season integer)
RETURNS TABLE(
    down integer,
    distance_bucket text,
    side text,
    play_count bigint,
    success_rate numeric,
    epa_per_play numeric,
    conversion_rate numeric
)
LANGUAGE plpgsql
STABLE
SET search_path = ''
AS $function$
BEGIN
    RETURN QUERY
    WITH bucketed_plays AS (
        SELECT
            pe.down,
            -- Recompute 4-bucket distance scheme for frontend compatibility
            -- (matview uses short/medium/long; frontend expects 1-3/4-6/7-10/11+)
            CASE
                WHEN pe.distance BETWEEN 1 AND 3 THEN '1-3'
                WHEN pe.distance BETWEEN 4 AND 6 THEN '4-6'
                WHEN pe.distance BETWEEN 7 AND 10 THEN '7-10'
                ELSE '11+'
            END AS distance_bucket,
            CASE
                WHEN pe.offense = p_team THEN 'offense'
                ELSE 'defense'
            END AS side,
            pe.epa,
            pe.success,
            CASE
                WHEN pe.down IN (3, 4) AND pe.yards_gained >= pe.distance THEN 1
                ELSE 0
            END AS converted
        FROM marts.play_epa pe
        WHERE pe.season = p_season
          AND (pe.offense = p_team OR pe.defense = p_team)
          AND pe.down IS NOT NULL
          AND pe.down BETWEEN 1 AND 4
          AND pe.distance IS NOT NULL
          AND pe.distance > 0
          AND NOT pe.is_garbage_time
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

CREATE OR REPLACE FUNCTION public.get_field_position_splits(p_team text, p_season integer)
RETURNS TABLE(
    zone text,
    zone_label text,
    side text,
    play_count bigint,
    success_rate numeric,
    epa_per_play numeric,
    yards_per_play numeric,
    scoring_rate numeric
)
LANGUAGE plpgsql
STABLE
SET search_path = ''
AS $function$
BEGIN
    RETURN QUERY
    WITH zoned_plays AS (
        SELECT
            CASE
                WHEN pe.offense = p_team THEN 'offense'
                ELSE 'defense'
            END AS side,
            -- Recompute 4-zone field position from yards_to_goal
            -- yards_to_goal is from offense perspective; zones flip for defense
            -- yards_to_goal == yardline in CFBD data; use same boundaries as original
            CASE
                WHEN pe.offense = p_team THEN
                    CASE
                        WHEN pe.yards_to_goal >= 80 THEN 'own_1_20'
                        WHEN pe.yards_to_goal >= 50 THEN 'own_21_50'
                        WHEN pe.yards_to_goal >= 20 THEN 'opp_49_21'
                        ELSE 'opp_20_1'
                    END
                ELSE
                    CASE
                        WHEN pe.yards_to_goal <= 20 THEN 'own_1_20'
                        WHEN pe.yards_to_goal <= 50 THEN 'own_21_50'
                        WHEN pe.yards_to_goal <= 80 THEN 'opp_49_21'
                        ELSE 'opp_20_1'
                    END
            END AS zone,
            pe.epa,
            pe.yards_gained,
            CASE WHEN pe.scoring = true THEN 1 ELSE 0 END AS scored,
            pe.success AS successful
        FROM marts.play_epa pe
        WHERE pe.season = p_season
          AND (pe.offense = p_team OR pe.defense = p_team)
          AND pe.yards_to_goal IS NOT NULL
          AND NOT pe.is_garbage_time
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
        ROUND(AVG(zp.epa)::NUMERIC, 3) AS epa_per_play,
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

CREATE OR REPLACE FUNCTION public.get_red_zone_splits(p_team text, p_season integer)
RETURNS TABLE(
    side text,
    trips bigint,
    touchdowns bigint,
    field_goals bigint,
    turnovers bigint,
    td_rate numeric,
    fg_rate numeric,
    scoring_rate numeric,
    points_per_trip numeric,
    epa_per_play numeric
)
LANGUAGE plpgsql
STABLE
SET search_path = ''
AS $function$
BEGIN
    RETURN QUERY
    -- Fixed 2026-07-23: the previous version filtered core.drives on
    -- start_yardline >= 80 -- the ABSOLUTE yardline column (direction-
    -- dependent, the only start_yardline reference in the repo) and a
    -- drive-START condition, when a red-zone trip is a drive that REACHES
    -- yards_to_goal <= 20. It undercounted both sides and reported ~0
    -- defensive TDs allowed. Trips are now play-derived (any snap at
    -- yards_to_goal <= 20), with outcomes taken from the drive row joined
    -- on (game_id, drive_number) -- verified against api.game_plays ground
    -- truth (Oklahoma 2025: offense 38 trips/26 TD, defense 32/13). The
    -- EPA sub-CTE had the same absolute-yardline bug (yardline >= 80) and
    -- now uses yards_to_goal <= 20. drive_result matching mirrors
    -- marts/006_scoring_opportunities.sql's casing-tolerant sets.
    WITH red_zone_trips AS (
        -- Garbage-time exclusion is intentionally NOT applied here:
        -- trips/scores are drive-level facts and stay unfiltered per the
        -- Phase 1 garbage-time centralization plan.
        SELECT DISTINCT
            CASE WHEN p.offense = p_team THEN 'offense' ELSE 'defense' END AS side,
            p.game_id,
            p.drive_number
        FROM core.plays p
        JOIN core.games g ON p.game_id = g.id
        WHERE g.season = p_season
          AND (p.offense = p_team OR p.defense = p_team)
          AND p.yards_to_goal <= 20
          AND p.drive_number IS NOT NULL
    ),
    red_zone_drives AS (
        SELECT
            t.side,
            t.game_id,
            t.drive_number,
            d.drive_result
        FROM red_zone_trips t
        -- LEFT JOIN: a trip observed in plays still counts even if its
        -- drive row is absent (it then contributes no outcome).
        LEFT JOIN core.drives d
          ON d.game_id = t.game_id AND d.drive_number = t.drive_number
    ),
    red_zone_plays AS (
        SELECT
            CASE WHEN p.offense = p_team THEN 'offense' ELSE 'defense' END AS side,
            p.ppa
        FROM core.plays p
        JOIN core.games g ON p.game_id = g.id
        WHERE g.season = p_season
          AND (p.offense = p_team OR p.defense = p_team)
          AND p.yards_to_goal <= 20
          AND NOT public.is_garbage_time(p.period::integer, p.score_diff::integer)
    )
    SELECT
        rzd.side::TEXT,
        COUNT(*)::BIGINT AS trips,
        COUNT(*) FILTER (WHERE rzd.drive_result IN ('TD', 'Touchdown'))::BIGINT AS touchdowns,
        COUNT(*) FILTER (WHERE rzd.drive_result IN ('FG', 'Field Goal'))::BIGINT AS field_goals,
        COUNT(*) FILTER (WHERE rzd.drive_result IN ('INT', 'FUMBLE', 'INT TD', 'FUMBLE RETURN TD', 'Interception', 'Fumble', 'Fumble Lost', 'Interception Return'))::BIGINT AS turnovers,
        ROUND(COUNT(*) FILTER (WHERE rzd.drive_result IN ('TD', 'Touchdown'))::NUMERIC / NULLIF(COUNT(*), 0), 3) AS td_rate,
        ROUND(COUNT(*) FILTER (WHERE rzd.drive_result IN ('FG', 'Field Goal'))::NUMERIC / NULLIF(COUNT(*), 0), 3) AS fg_rate,
        ROUND(COUNT(*) FILTER (WHERE rzd.drive_result IN ('TD', 'FG', 'Touchdown', 'Field Goal'))::NUMERIC / NULLIF(COUNT(*), 0), 3) AS scoring_rate,
        ROUND((COUNT(*) FILTER (WHERE rzd.drive_result IN ('TD', 'Touchdown')) * 7 + COUNT(*) FILTER (WHERE rzd.drive_result IN ('FG', 'Field Goal')) * 3)::NUMERIC / NULLIF(COUNT(*), 0), 2) AS points_per_trip,
        ROUND((SELECT AVG(rzp.ppa) FROM red_zone_plays rzp WHERE rzp.side = rzd.side)::NUMERIC, 3) AS epa_per_play
    FROM red_zone_drives rzd
    GROUP BY rzd.side;
END;
$function$;
