-- Matchup forecast API view
-- Game-level forecast surface with transparent probability components.
-- Exposed via PostgREST as /api/matchup_forecast

DROP VIEW IF EXISTS api.matchup_forecast;

CREATE VIEW api.matchup_forecast AS
SELECT
    p.game_id,
    p.season,
    p.week,
    p.season_type,
    p.start_date,
    p.completed,
    p.neutral_site,
    p.conference_game,
    p.home_team,
    p.away_team,
    p.market_spread,
    p.market_over_under,
    p.home_win_probability,
    p.away_win_probability,
    p.projected_winner,
    p.projected_margin,
    p.confidence_tier,
    p.cfbd_home_win_prob,
    p.market_home_win_prob,
    p.elo_home_win_prob,
    p.sp_home_win_prob,
    p.model_version,
    p.home_points,
    p.away_points,
    CASE
        WHEN p.completed
          AND p.home_points IS NOT NULL
          AND p.away_points IS NOT NULL THEN
            CASE
                WHEN p.home_points > p.away_points THEN p.home_team
                WHEN p.away_points > p.home_points THEN p.away_team
                ELSE NULL
            END
        ELSE NULL
    END AS actual_winner,
    p.brier_loss,
    home_out.expected_wins AS home_expected_wins,
    home_out.bowl_eligibility_prob AS home_bowl_eligibility_prob,
    home_out.ten_plus_win_prob AS home_ten_plus_win_prob,
    away_out.expected_wins AS away_expected_wins,
    away_out.bowl_eligibility_prob AS away_bowl_eligibility_prob,
    away_out.ten_plus_win_prob AS away_ten_plus_win_prob
FROM marts.pre_game_win_probability p
LEFT JOIN marts.season_simulation_outcomes home_out
    ON home_out.season = p.season
   AND home_out.team = p.home_team
LEFT JOIN marts.season_simulation_outcomes away_out
    ON away_out.season = p.season
   AND away_out.team = p.away_team;

COMMENT ON VIEW api.matchup_forecast IS
    'Phase 1 matchup forecast with blended pregame win probabilities and team season outlook context.';
