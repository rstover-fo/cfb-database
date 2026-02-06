-- get_conference_head_to_head: conference vs conference head-to-head records
-- Returns season-by-season breakdown between two conferences
--
-- Usage:
--   SELECT * FROM get_conference_head_to_head('SEC', 'Big Ten');
--   SELECT * FROM get_conference_head_to_head('SEC', 'Big 12', 2020, 2024);

CREATE OR REPLACE FUNCTION get_conference_head_to_head(
    p_conf1 text,
    p_conf2 text,
    p_season_start int DEFAULT NULL,
    p_season_end int DEFAULT NULL
)
RETURNS TABLE(
    conference_1 text,
    conference_2 text,
    season bigint,
    total_games int,
    conf1_wins int,
    conf2_wins int,
    ties int,
    conf1_win_pct numeric,
    avg_point_diff numeric
)
LANGUAGE sql
STABLE
AS $$
    SELECT
        h.conference_1,
        h.conference_2,
        h.season,
        h.total_games,
        -- Flip wins if the user's conferences don't match alphabetical order
        CASE WHEN LEAST(p_conf1, p_conf2) = p_conf1
            THEN h.conf1_wins ELSE h.conf2_wins END AS conf1_wins,
        CASE WHEN LEAST(p_conf1, p_conf2) = p_conf1
            THEN h.conf2_wins ELSE h.conf1_wins END AS conf2_wins,
        h.ties,
        CASE WHEN LEAST(p_conf1, p_conf2) = p_conf1
            THEN h.conf1_win_pct
            ELSE ROUND(1.0 - COALESCE(h.conf1_win_pct, 0), 4)
        END AS conf1_win_pct,
        CASE WHEN LEAST(p_conf1, p_conf2) = p_conf1
            THEN h.avg_point_diff
            ELSE -h.avg_point_diff
        END AS avg_point_diff
    FROM marts.conference_head_to_head h
    WHERE h.conference_1 = LEAST(p_conf1, p_conf2)
      AND h.conference_2 = GREATEST(p_conf1, p_conf2)
      AND (p_season_start IS NULL OR h.season >= p_season_start)
      AND (p_season_end IS NULL OR h.season <= p_season_end)
    ORDER BY h.season DESC;
$$;

COMMENT ON FUNCTION get_conference_head_to_head IS
'Conference vs conference head-to-head records by season. '
'Results are always oriented so p_conf1 wins/pct are shown first, regardless of alphabetical order. '
'Optional season range filtering.';
