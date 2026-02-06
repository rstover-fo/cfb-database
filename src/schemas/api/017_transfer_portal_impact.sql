-- api.transfer_portal_impact
-- Transfer portal activity and its correlation with team performance changes.
-- Filter by: team, season, conference
-- Example: /api/transfer_portal_impact?season=eq.2024&order=win_delta.desc

CREATE OR REPLACE VIEW api.transfer_portal_impact AS
SELECT
    team,
    season,
    conference,
    transfers_in,
    transfers_out,
    net_transfers,
    avg_incoming_stars,
    avg_incoming_rating,
    incoming_high_stars,
    prior_season_wins,
    prior_season_sp_rating,
    current_wins,
    current_sp_rating,
    win_delta,
    sp_delta,
    portal_dependency,
    win_delta_per_transfer_in,
    net_transfers_pctl,
    win_delta_pctl,
    portal_dependency_pctl
FROM marts.transfer_portal_impact;

COMMENT ON VIEW api.transfer_portal_impact IS
'Transfer portal impact: portal activity correlated with team performance changes. '
'One row per team-season (portal era ~2021+). '
'portal_dependency = transfers_in / roster_size. '
'win_delta = current_wins - prior_season_wins.';
