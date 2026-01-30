-- Team offensive/defensive style profile
-- Classifies teams by run/pass tendency, tempo, and EPA efficiency

DROP MATERIALIZED VIEW IF EXISTS marts.team_style_profile CASCADE;

CREATE MATERIALIZED VIEW marts.team_style_profile AS
WITH play_types AS (
    SELECT
        season,
        offense AS team,
        COUNT(*) AS total_plays,
        SUM(CASE WHEN play_category = 'rush' THEN 1 ELSE 0 END) AS rush_plays,
        SUM(CASE WHEN play_category = 'pass' THEN 1 ELSE 0 END) AS pass_plays,
        AVG(CASE WHEN play_category = 'rush' THEN epa END)::NUMERIC(6,4) AS rush_epa,
        AVG(CASE WHEN play_category = 'pass' THEN epa END)::NUMERIC(6,4) AS pass_epa,
        AVG(CASE WHEN play_category = 'rush' THEN success END)::NUMERIC(5,3) AS rush_success,
        AVG(CASE WHEN play_category = 'pass' THEN success END)::NUMERIC(5,3) AS pass_success
    FROM marts.play_epa
    WHERE NOT is_garbage_time
    GROUP BY season, offense
),
tempo AS (
    SELECT
        season,
        offense AS team,
        COUNT(*)::NUMERIC / COUNT(DISTINCT game_id) AS plays_per_game
    FROM marts.play_epa
    WHERE NOT is_garbage_time
    GROUP BY season, offense
),
defense AS (
    SELECT
        season,
        defense AS team,
        AVG(CASE WHEN play_category = 'rush' THEN epa END)::NUMERIC(6,4) AS def_rush_epa,
        AVG(CASE WHEN play_category = 'pass' THEN epa END)::NUMERIC(6,4) AS def_pass_epa,
        AVG(epa)::NUMERIC(6,4) AS def_epa_allowed
    FROM marts.play_epa
    WHERE NOT is_garbage_time
    GROUP BY season, defense
)
SELECT
    pt.season,
    pt.team,
    -- Offensive style
    (pt.rush_plays::NUMERIC / NULLIF(pt.total_plays, 0))::NUMERIC(5,3) AS run_rate,
    (pt.pass_plays::NUMERIC / NULLIF(pt.total_plays, 0))::NUMERIC(5,3) AS pass_rate,
    pt.rush_epa AS epa_rushing,
    pt.pass_epa AS epa_passing,
    pt.rush_success AS rush_success_rate,
    pt.pass_success AS pass_success_rate,
    -- Tempo
    t.plays_per_game::NUMERIC(5,1),
    CASE
        WHEN t.plays_per_game >= 75 THEN 'up_tempo'
        WHEN t.plays_per_game >= 65 THEN 'balanced'
        ELSE 'slow'
    END AS tempo_category,
    -- Defense
    d.def_rush_epa AS def_epa_vs_run,
    d.def_pass_epa AS def_epa_vs_pass,
    d.def_epa_allowed,
    -- Style tags
    CASE
        WHEN pt.rush_plays::NUMERIC / NULLIF(pt.total_plays, 0) >= 0.55 THEN 'run_heavy'
        WHEN pt.pass_plays::NUMERIC / NULLIF(pt.total_plays, 0) >= 0.55 THEN 'pass_heavy'
        ELSE 'balanced'
    END AS offensive_identity,
    -- Defensive identity based on what they stop best
    CASE
        WHEN d.def_rush_epa < d.def_pass_epa THEN 'run_stopper'
        WHEN d.def_pass_epa < d.def_rush_epa THEN 'pass_stopper'
        ELSE 'balanced_defense'
    END AS defensive_identity
FROM play_types pt
JOIN tempo t ON pt.season = t.season AND pt.team = t.team
LEFT JOIN defense d ON pt.season = d.season AND pt.team = d.team;

CREATE UNIQUE INDEX ON marts.team_style_profile (season, team);
CREATE INDEX ON marts.team_style_profile (season, offensive_identity);
CREATE INDEX ON marts.team_style_profile (season, tempo_category);
CREATE INDEX ON marts.team_style_profile (season, defensive_identity);
