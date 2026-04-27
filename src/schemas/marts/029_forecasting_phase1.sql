-- Phase 1 forecasting foundation
-- 1) Game-level blended pregame win probabilities
-- 2) Team-level season simulation outlook for the latest season

CREATE SCHEMA IF NOT EXISTS marts;

DROP MATERIALIZED VIEW IF EXISTS marts.season_simulation_outcomes CASCADE;
DROP MATERIALIZED VIEW IF EXISTS marts.pre_game_win_probability CASCADE;

-- -----------------------------------------------------------------------------
-- 1) Game-level blended pregame probabilities
-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW marts.pre_game_win_probability AS
WITH consensus_lines AS (
    -- Prefer consensus line when available; otherwise use first provider
    SELECT DISTINCT ON (game_id)
        game_id,
        spread,
        over_under,
        provider
    FROM betting.lines
    ORDER BY game_id, CASE WHEN provider = 'consensus' THEN 0 ELSE 1 END, provider
),
games AS (
    SELECT
        g.id AS game_id,
        g.season::int AS season,
        g.week::int AS week,
        g.season_type,
        g.start_date,
        COALESCE(g.completed, false) AS completed,
        g.neutral_site,
        g.conference_game,
        g.home_team,
        g.away_team,
        g.home_points,
        g.away_points,
        wp.home_win_probability AS cfbd_home_win_prob,
        cl.spread AS market_spread,
        cl.over_under AS market_over_under
    FROM core.games g
    LEFT JOIN metrics.pregame_win_probability wp
        ON wp.game_id = g.id
    LEFT JOIN consensus_lines cl
        ON cl.game_id = g.id
    WHERE g.home_team IS NOT NULL
      AND g.away_team IS NOT NULL
),
ratings AS (
    SELECT
        gm.*,
        home_elo.elo::numeric AS home_elo_rating,
        away_elo.elo::numeric AS away_elo_rating,
        home_sp.rating::numeric AS home_sp_rating,
        away_sp.rating::numeric AS away_sp_rating
    FROM games gm
    LEFT JOIN LATERAL (
        SELECT e.elo, e.year
        FROM ratings.elo_ratings e
        WHERE e.team = gm.home_team
          AND e.year <= gm.season
        ORDER BY CASE WHEN e.year = gm.season THEN 0 ELSE 1 END, e.year DESC
        LIMIT 1
    ) home_elo ON true
    LEFT JOIN LATERAL (
        SELECT e.elo, e.year
        FROM ratings.elo_ratings e
        WHERE e.team = gm.away_team
          AND e.year <= gm.season
        ORDER BY CASE WHEN e.year = gm.season THEN 0 ELSE 1 END, e.year DESC
        LIMIT 1
    ) away_elo ON true
    LEFT JOIN LATERAL (
        SELECT s.rating, s.year
        FROM ratings.sp_ratings s
        WHERE s.team = gm.home_team
          AND s.year <= gm.season
        ORDER BY CASE WHEN s.year = gm.season THEN 0 ELSE 1 END, s.year DESC
        LIMIT 1
    ) home_sp ON true
    LEFT JOIN LATERAL (
        SELECT s.rating, s.year
        FROM ratings.sp_ratings s
        WHERE s.team = gm.away_team
          AND s.year <= gm.season
        ORDER BY CASE WHEN s.year = gm.season THEN 0 ELSE 1 END, s.year DESC
        LIMIT 1
    ) away_sp ON true
),
signals AS (
    SELECT
        r.*,
        (r.home_elo_rating - r.away_elo_rating) AS elo_diff,
        (r.home_sp_rating - r.away_sp_rating) AS sp_diff,
        CASE
            WHEN r.home_elo_rating IS NOT NULL AND r.away_elo_rating IS NOT NULL THEN
                1.0 / (1.0 + POWER(10.0, -((r.home_elo_rating - r.away_elo_rating) / 400.0)))
            ELSE NULL
        END AS elo_home_win_prob,
        CASE
            WHEN r.home_sp_rating IS NOT NULL AND r.away_sp_rating IS NOT NULL THEN
                1.0 / (1.0 + EXP(-((r.home_sp_rating - r.away_sp_rating) / 7.5)))
            ELSE NULL
        END AS sp_home_win_prob,
        CASE
            WHEN r.market_spread IS NOT NULL THEN
                1.0 / (1.0 + EXP((r.market_spread::numeric / 6.5)::double precision))
            ELSE NULL
        END AS market_home_win_prob
    FROM ratings r
),
blended AS (
    SELECT
        s.*,
        (
            CASE WHEN s.cfbd_home_win_prob IS NOT NULL THEN 0.50 ELSE 0.00 END +
            CASE WHEN s.market_home_win_prob IS NOT NULL THEN 0.25 ELSE 0.00 END +
            CASE WHEN s.elo_home_win_prob IS NOT NULL THEN 0.15 ELSE 0.00 END +
            CASE WHEN s.sp_home_win_prob IS NOT NULL THEN 0.10 ELSE 0.00 END
        ) AS total_weight,
        (
            COALESCE(s.cfbd_home_win_prob, 0.0) * 0.50 +
            COALESCE(s.market_home_win_prob, 0.0) * 0.25 +
            COALESCE(s.elo_home_win_prob, 0.0) * 0.15 +
            COALESCE(s.sp_home_win_prob, 0.0) * 0.10
        ) AS weighted_prob_sum
    FROM signals s
),
scored AS (
    SELECT
        b.*,
        GREATEST(
            0.01::numeric,
            LEAST(
                0.99::numeric,
                CASE
                    WHEN b.total_weight > 0 THEN (b.weighted_prob_sum / b.total_weight)::numeric
                    ELSE 0.50::numeric
                END
            )
        ) AS home_prob
    FROM blended b
)
SELECT
    s.game_id,
    s.season,
    s.week,
    s.season_type,
    s.start_date,
    s.completed,
    s.neutral_site,
    s.conference_game,
    s.home_team,
    s.away_team,
    s.home_points,
    s.away_points,
    s.market_spread,
    s.market_over_under,
    s.home_elo_rating,
    s.away_elo_rating,
    s.elo_diff,
    s.home_sp_rating,
    s.away_sp_rating,
    s.sp_diff,
    ROUND(s.cfbd_home_win_prob::numeric, 4) AS cfbd_home_win_prob,
    ROUND(s.market_home_win_prob::numeric, 4) AS market_home_win_prob,
    ROUND(s.elo_home_win_prob::numeric, 4) AS elo_home_win_prob,
    ROUND(s.sp_home_win_prob::numeric, 4) AS sp_home_win_prob,
    ROUND(s.home_prob, 4) AS home_win_probability,
    ROUND((1.0::numeric - s.home_prob), 4) AS away_win_probability,
    CASE
        WHEN s.home_prob >= 0.50 THEN s.home_team
        ELSE s.away_team
    END AS projected_winner,
    ROUND(((s.home_prob - 0.50) * 24.0), 1) AS projected_margin,
    CASE
        WHEN ABS(s.home_prob - 0.50) >= 0.30 THEN 'heavy_favorite'
        WHEN ABS(s.home_prob - 0.50) >= 0.20 THEN 'strong_lean'
        WHEN ABS(s.home_prob - 0.50) >= 0.10 THEN 'lean'
        ELSE 'toss_up'
    END AS confidence_tier,
    CASE
        WHEN s.completed
          AND s.home_points IS NOT NULL
          AND s.away_points IS NOT NULL THEN
            CASE
                WHEN s.home_points > s.away_points THEN 1.0::numeric
                WHEN s.home_points < s.away_points THEN 0.0::numeric
                ELSE 0.5::numeric
            END
        ELSE NULL
    END AS actual_home_result,
    CASE
        WHEN s.completed
          AND s.home_points IS NOT NULL
          AND s.away_points IS NOT NULL THEN
            ROUND(
                POWER(
                    (
                        s.home_prob -
                        CASE
                            WHEN s.home_points > s.away_points THEN 1.0::numeric
                            WHEN s.home_points < s.away_points THEN 0.0::numeric
                            ELSE 0.5::numeric
                        END
                    ),
                    2
                ),
                4
            )
        ELSE NULL
    END AS brier_loss,
    'v1_blended_cfbd_market_elo_sp'::text AS model_version
FROM scored s;

CREATE UNIQUE INDEX ON marts.pre_game_win_probability (game_id);
CREATE INDEX ON marts.pre_game_win_probability (season, week);
CREATE INDEX ON marts.pre_game_win_probability (home_team);
CREATE INDEX ON marts.pre_game_win_probability (away_team);
CREATE INDEX ON marts.pre_game_win_probability (completed, start_date);
CREATE INDEX ON marts.pre_game_win_probability (home_win_probability DESC);

-- -----------------------------------------------------------------------------
-- 2) Team-level season simulation outcomes (latest season only)
-- -----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW marts.season_simulation_outcomes AS
WITH latest_season AS (
    SELECT MAX(season)::int AS season
    FROM marts.pre_game_win_probability
    WHERE COALESCE(season_type, 'regular') = 'regular'
),
team_games AS (
    SELECT
        p.season,
        p.game_id,
        p.week,
        p.completed,
        p.home_team AS team,
        p.away_team AS opponent,
        p.home_win_probability AS win_probability,
        CASE
            WHEN p.completed
              AND p.home_points IS NOT NULL
              AND p.away_points IS NOT NULL THEN
                CASE
                    WHEN p.home_points > p.away_points THEN 1.0::numeric
                    WHEN p.home_points < p.away_points THEN 0.0::numeric
                    ELSE 0.5::numeric
                END
            ELSE NULL
        END AS actual_win
    FROM marts.pre_game_win_probability p
    JOIN latest_season ls ON ls.season = p.season
    WHERE COALESCE(p.season_type, 'regular') = 'regular'

    UNION ALL

    SELECT
        p.season,
        p.game_id,
        p.week,
        p.completed,
        p.away_team AS team,
        p.home_team AS opponent,
        1.0::numeric - p.home_win_probability AS win_probability,
        CASE
            WHEN p.completed
              AND p.home_points IS NOT NULL
              AND p.away_points IS NOT NULL THEN
                CASE
                    WHEN p.away_points > p.home_points THEN 1.0::numeric
                    WHEN p.away_points < p.home_points THEN 0.0::numeric
                    ELSE 0.5::numeric
                END
            ELSE NULL
        END AS actual_win
    FROM marts.pre_game_win_probability p
    JOIN latest_season ls ON ls.season = p.season
    WHERE COALESCE(p.season_type, 'regular') = 'regular'
),
team_set AS (
    SELECT DISTINCT season, team
    FROM team_games
),
simulations AS (
    SELECT generate_series(1, 2000) AS sim_id
),
team_sim_wins AS (
    SELECT
        ts.season,
        ts.team,
        s.sim_id,
        SUM(
            CASE
                WHEN tg.completed AND tg.actual_win IS NOT NULL THEN tg.actual_win
                WHEN tg.win_probability IS NULL THEN 0.5::numeric
                WHEN RANDOM() < tg.win_probability THEN 1.0::numeric
                ELSE 0.0::numeric
            END
        ) AS simulated_wins,
        COUNT(*)::int AS total_games
    FROM team_set ts
    CROSS JOIN simulations s
    JOIN team_games tg
        ON tg.season = ts.season
       AND tg.team = ts.team
    GROUP BY ts.season, ts.team, s.sim_id
),
summary AS (
    SELECT
        season,
        team,
        MAX(total_games)::int AS scheduled_games,
        ROUND(AVG(simulated_wins), 2) AS expected_wins,
        ROUND(AVG(total_games - simulated_wins), 2) AS expected_losses,
        ROUND(STDDEV_POP(simulated_wins)::numeric, 2) AS wins_stddev,
        ROUND(PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY simulated_wins)::numeric, 2) AS wins_p10,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY simulated_wins)::numeric, 2) AS wins_p50,
        ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY simulated_wins)::numeric, 2) AS wins_p90,
        ROUND(AVG((simulated_wins >= 6.0)::int)::numeric, 4) AS bowl_eligibility_prob,
        ROUND(AVG((simulated_wins >= 10.0)::int)::numeric, 4) AS ten_plus_win_prob,
        ROUND(AVG((simulated_wins >= 12.0)::int)::numeric, 4) AS perfect_regular_season_prob,
        COUNT(*)::int AS simulation_count
    FROM team_sim_wins
    GROUP BY season, team
),
distribution_counts AS (
    SELECT
        season,
        team,
        simulated_wins,
        COUNT(*)::int AS scenario_count
    FROM team_sim_wins
    GROUP BY season, team, simulated_wins
),
distribution AS (
    SELECT
        dc.season,
        dc.team,
        jsonb_object_agg(
            dc.simulated_wins::text,
            ROUND((dc.scenario_count::numeric / NULLIF(s.simulation_count, 0)::numeric), 4)
            ORDER BY dc.simulated_wins
        ) AS win_distribution
    FROM distribution_counts dc
    JOIN summary s
        ON s.season = dc.season
       AND s.team = dc.team
    GROUP BY dc.season, dc.team
)
SELECT
    s.season,
    s.team,
    s.scheduled_games,
    s.expected_wins,
    s.expected_losses,
    s.wins_stddev,
    s.wins_p10,
    s.wins_p50,
    s.wins_p90,
    s.bowl_eligibility_prob,
    s.ten_plus_win_prob,
    s.perfect_regular_season_prob,
    d.win_distribution,
    s.simulation_count,
    'v1_monte_carlo_2000'::text AS simulation_method
FROM summary s
LEFT JOIN distribution d
    ON d.season = s.season
   AND d.team = s.team;

CREATE UNIQUE INDEX ON marts.season_simulation_outcomes (season, team);
CREATE INDEX ON marts.season_simulation_outcomes (season);
CREATE INDEX ON marts.season_simulation_outcomes (expected_wins DESC);
CREATE INDEX ON marts.season_simulation_outcomes (bowl_eligibility_prob DESC);
CREATE INDEX ON marts.season_simulation_outcomes (ten_plus_win_prob DESC);
