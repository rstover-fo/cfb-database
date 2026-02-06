-- api.conference_comparison
-- Conference-level aggregate metrics per season with percentile rankings.
-- Filter by: conference, season
-- Example: /api/conference_comparison?season=eq.2024&order=avg_sp_pctl.desc

CREATE OR REPLACE VIEW api.conference_comparison AS
SELECT
    cc.conference,
    cc.season,
    cc.member_count,
    cc.avg_wins,
    cc.avg_sp_rating,
    cc.median_sp_rating,
    cc.best_team,
    cc.best_team_sp,
    cc.worst_team,
    cc.worst_team_sp,
    cc.std_dev_sp,
    cc.avg_epa_per_play,
    cc.avg_success_rate,
    cc.avg_recruiting_rank,
    cc.total_blue_chips,
    cc.avg_blue_chip_ratio,
    cc.non_conf_win_pct,
    cc.ranked_team_count,
    cc.avg_sp_pctl,
    cc.avg_epa_pctl,
    cc.avg_recruiting_pctl,
    cc.non_conf_win_pct_pctl
FROM marts.conference_comparison cc;

COMMENT ON VIEW api.conference_comparison IS
'Conference-level aggregate metrics with percentile rankings. '
'One row per conference-season. Enables SEC vs Big Ten comparisons. '
'non_conf_win_pct = record against other conferences. '
'std_dev_sp measures parity (lower = more competitive balance).';
