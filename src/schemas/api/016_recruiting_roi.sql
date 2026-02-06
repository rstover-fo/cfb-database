-- api.recruiting_roi
-- Recruiting ROI: connects 4-year rolling recruiting to on-field outcomes.
-- Filter by: team, season, conference
-- Example: /api/recruiting_roi?season=eq.2024&order=recruiting_efficiency_pctl.desc

CREATE OR REPLACE VIEW api.recruiting_roi AS
SELECT
    team,
    season,
    conference,
    avg_class_rank_4yr,
    avg_class_points_4yr,
    total_blue_chips_4yr,
    blue_chip_ratio,
    wins,
    losses,
    win_pct,
    sp_rating,
    sp_rank,
    epa_per_play,
    success_rate,
    players_drafted,
    draft_picks_value,
    wins_over_expected,
    epa_over_expected,
    recruiting_efficiency,
    win_pct_pctl,
    epa_pctl,
    recruiting_efficiency_pctl
FROM marts.recruiting_roi;

COMMENT ON VIEW api.recruiting_roi IS
'Recruiting ROI: 4-year rolling recruiting investment vs on-field outcomes. '
'One row per team-season. blue_chip_ratio = 4+5 star ratio. '
'recruiting_efficiency = wins / avg_class_rank. '
'wins_over_expected = actual wins - median wins for teams with similar recruiting rank.';
