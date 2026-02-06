-- api.game_line_scores
-- Game line scores pivoted into Q1-Q4 columns with OT periods summed.
--
-- PostgREST usage:
--   GET /api/game_line_scores?game_id=eq.401628455

CREATE OR REPLACE VIEW api.game_line_scores AS
SELECT
    g.id AS game_id,
    g.season,
    home_q1.value AS home_q1,
    home_q2.value AS home_q2,
    home_q3.value AS home_q3,
    home_q4.value AS home_q4,
    home_ot.ot_total AS home_ot,
    away_q1.value AS away_q1,
    away_q2.value AS away_q2,
    away_q3.value AS away_q3,
    away_q4.value AS away_q4,
    away_ot.ot_total AS away_ot
FROM core.games g
LEFT JOIN LATERAL (
    SELECT value FROM core.games__home_line_scores WHERE _dlt_parent_id = g._dlt_id AND _dlt_list_idx = 0
) home_q1 ON true
LEFT JOIN LATERAL (
    SELECT value FROM core.games__home_line_scores WHERE _dlt_parent_id = g._dlt_id AND _dlt_list_idx = 1
) home_q2 ON true
LEFT JOIN LATERAL (
    SELECT value FROM core.games__home_line_scores WHERE _dlt_parent_id = g._dlt_id AND _dlt_list_idx = 2
) home_q3 ON true
LEFT JOIN LATERAL (
    SELECT value FROM core.games__home_line_scores WHERE _dlt_parent_id = g._dlt_id AND _dlt_list_idx = 3
) home_q4 ON true
LEFT JOIN LATERAL (
    SELECT SUM(value) AS ot_total FROM core.games__home_line_scores WHERE _dlt_parent_id = g._dlt_id AND _dlt_list_idx >= 4
) home_ot ON true
LEFT JOIN LATERAL (
    SELECT value FROM core.games__away_line_scores WHERE _dlt_parent_id = g._dlt_id AND _dlt_list_idx = 0
) away_q1 ON true
LEFT JOIN LATERAL (
    SELECT value FROM core.games__away_line_scores WHERE _dlt_parent_id = g._dlt_id AND _dlt_list_idx = 1
) away_q2 ON true
LEFT JOIN LATERAL (
    SELECT value FROM core.games__away_line_scores WHERE _dlt_parent_id = g._dlt_id AND _dlt_list_idx = 2
) away_q3 ON true
LEFT JOIN LATERAL (
    SELECT value FROM core.games__away_line_scores WHERE _dlt_parent_id = g._dlt_id AND _dlt_list_idx = 3
) away_q4 ON true
LEFT JOIN LATERAL (
    SELECT SUM(value) AS ot_total FROM core.games__away_line_scores WHERE _dlt_parent_id = g._dlt_id AND _dlt_list_idx >= 4
) away_ot ON true;

COMMENT ON VIEW api.game_line_scores IS 'Game line scores pivoted into Q1-Q4 columns with OT periods summed.';
