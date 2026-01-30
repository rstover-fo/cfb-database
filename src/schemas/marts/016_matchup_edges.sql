-- Matchup edge indicators for game predictions
-- Compares offensive/defensive style matchups between teams
-- Depends on: marts.team_style_profile

DROP MATERIALIZED VIEW IF EXISTS marts.matchup_edges CASCADE;

CREATE MATERIALIZED VIEW marts.matchup_edges AS
WITH game_matchups AS (
    SELECT DISTINCT
        g.season,
        g.id AS game_id,
        g.home_team AS team_a,
        g.away_team AS team_b,
        g.home_points AS team_a_points,
        g.away_points AS team_b_points
    FROM core.games g
    WHERE g.season >= 2014  -- Playoff era onwards for predictions
      AND g.completed = true
)
SELECT
    gm.season,
    gm.game_id,
    gm.team_a,
    gm.team_b,
    gm.team_a_points,
    gm.team_b_points,
    -- Team A style
    sa.run_rate AS a_run_rate,
    sa.offensive_identity AS a_offensive_identity,
    sa.epa_rushing AS a_epa_rushing,
    sa.epa_passing AS a_epa_passing,
    sa.tempo_category AS a_tempo,
    -- Team B style
    sb.run_rate AS b_run_rate,
    sb.offensive_identity AS b_offensive_identity,
    sb.epa_rushing AS b_epa_rushing,
    sb.epa_passing AS b_epa_passing,
    sb.tempo_category AS b_tempo,
    -- Matchup edges (positive = advantage to team_a)
    (sa.epa_rushing - sb.def_epa_vs_run)::NUMERIC(6,4) AS a_rush_edge,
    (sa.epa_passing - sb.def_epa_vs_pass)::NUMERIC(6,4) AS a_pass_edge,
    (sb.epa_rushing - sa.def_epa_vs_run)::NUMERIC(6,4) AS b_rush_edge,
    (sb.epa_passing - sa.def_epa_vs_pass)::NUMERIC(6,4) AS b_pass_edge,
    -- Tempo mismatch (high value = big tempo difference)
    ABS(sa.plays_per_game - sb.plays_per_game)::NUMERIC(5,1) AS tempo_mismatch,
    -- Overall edge estimate (weighted by play calling tendency)
    ((sa.epa_rushing - sb.def_epa_vs_run) * sa.run_rate +
     (sa.epa_passing - sb.def_epa_vs_pass) * sa.pass_rate -
     (sb.epa_rushing - sa.def_epa_vs_run) * sb.run_rate -
     (sb.epa_passing - sa.def_epa_vs_pass) * sb.pass_rate
    )::NUMERIC(6,4) AS net_edge_a,
    -- Actual outcome for model validation
    CASE
        WHEN gm.team_a_points > gm.team_b_points THEN 'team_a'
        WHEN gm.team_b_points > gm.team_a_points THEN 'team_b'
        ELSE 'tie'
    END AS actual_winner,
    (gm.team_a_points - gm.team_b_points) AS actual_margin
FROM game_matchups gm
JOIN marts.team_style_profile sa ON gm.season = sa.season AND gm.team_a = sa.team
JOIN marts.team_style_profile sb ON gm.season = sb.season AND gm.team_b = sb.team;

CREATE UNIQUE INDEX ON marts.matchup_edges (game_id);
CREATE INDEX ON marts.matchup_edges (season);
CREATE INDEX ON marts.matchup_edges (team_a);
CREATE INDEX ON marts.matchup_edges (team_b);
CREATE INDEX ON marts.matchup_edges (net_edge_a DESC);
