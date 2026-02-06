-- api.player_comparison
-- Thin view over marts.player_comparison materialized view.
-- PostgREST pushes WHERE filters down to indexed matview lookups.
--
-- Filter by: player_id, name, team, season, position_group
-- Example: /api/player_comparison?name=like.*Bryce Young*&season=eq.2022

CREATE OR REPLACE VIEW api.player_comparison AS
SELECT
    player_id, name, team, position, position_group, season,
    height, weight, jersey, home_city, home_state,
    stars, recruit_rating, national_ranking, recruit_class,
    pass_att, pass_cmp, pass_yds, pass_td, pass_int, pass_pct,
    rush_car, rush_yds, rush_td, rush_ypc,
    rec, rec_yds, rec_td, rec_ypr,
    tackles, sacks, tfl, pass_def,
    ppa_avg, ppa_total,
    pass_yds_pctl, pass_td_pctl, pass_pct_pctl,
    rush_yds_pctl, rush_td_pctl, rush_ypc_pctl,
    rec_yds_pctl, rec_td_pctl,
    tackles_pctl, sacks_pctl, tfl_pctl,
    ppa_avg_pctl
FROM marts.player_comparison;

COMMENT ON VIEW api.player_comparison IS 'Player stats with positional percentiles for comparison. Backed by materialized view for fast lookups. Filter by player_id, name, team, season, or position_group.';
