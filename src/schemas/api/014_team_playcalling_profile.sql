-- api.team_playcalling_profile
-- Team playcalling identity with situational tendencies and percentile rankings.
-- Regular view (not materialized) — aggregates ~60K matview rows into ~1,400 team-season rows.
-- PERCENT_RANK over 1,400 rows completes in single-digit ms.
--
-- Sources: marts.team_playcalling_tendencies, marts.team_situational_success,
--          marts.team_epa_season (for games_played), ref.teams (for conference)
-- Filter by: team, season, conference
-- Example: /api/team_playcalling_profile?team=eq.Ohio State&season=eq.2024

CREATE OR REPLACE VIEW api.team_playcalling_profile AS
WITH tendency_agg AS (
    SELECT
        team,
        season,
        SUM(total_plays) AS total_plays,
        ROUND(SUM(rush_plays)::numeric / NULLIF(SUM(total_plays), 0), 4)
            AS overall_run_rate,
        ROUND(SUM(rush_plays) FILTER (WHERE down IN (1, 2))::numeric
            / NULLIF(SUM(total_plays) FILTER (WHERE down IN (1, 2)), 0), 4)
            AS early_down_run_rate,
        ROUND(SUM(pass_plays) FILTER (WHERE down = 3)::numeric
            / NULLIF(SUM(total_plays) FILTER (WHERE down = 3), 0), 4)
            AS third_down_pass_rate,
        ROUND(SUM(rush_plays) FILTER (WHERE field_position = 'red_zone')::numeric
            / NULLIF(SUM(total_plays) FILTER (WHERE field_position = 'red_zone'), 0), 4)
            AS red_zone_run_rate,
        ROUND(SUM(rush_plays) FILTER (WHERE score_diff_bucket IN ('big_lead', 'small_lead'))::numeric
            / NULLIF(SUM(total_plays) FILTER (WHERE score_diff_bucket IN ('big_lead', 'small_lead')), 0), 4)
            AS leading_run_rate,
        ROUND(SUM(rush_plays) FILTER (WHERE score_diff_bucket IN ('big_deficit', 'small_deficit'))::numeric
            / NULLIF(SUM(total_plays) FILTER (WHERE score_diff_bucket IN ('big_deficit', 'small_deficit')), 0), 4)
            AS trailing_run_rate
    FROM marts.team_playcalling_tendencies
    GROUP BY team, season
),
success_agg AS (
    SELECT
        team,
        season,
        ROUND((SUM(success_rate * total_plays) / NULLIF(SUM(total_plays), 0))::numeric, 4)
            AS overall_success_rate,
        ROUND((SUM(avg_epa * total_plays) / NULLIF(SUM(total_plays), 0))::numeric, 4)
            AS overall_avg_epa,
        ROUND((SUM(success_rate * total_plays) FILTER (WHERE down = 3)
            / NULLIF(SUM(total_plays) FILTER (WHERE down = 3), 0))::numeric, 4)
            AS third_down_success_rate,
        ROUND((SUM(success_rate * total_plays) FILTER (WHERE field_position = 'red_zone')
            / NULLIF(SUM(total_plays) FILTER (WHERE field_position = 'red_zone'), 0))::numeric, 4)
            AS red_zone_success_rate
    FROM marts.team_situational_success
    WHERE total_plays >= 10
    GROUP BY team, season
),
teams_deduped AS (
    -- ref.teams has 35 duplicate school names; pick FBS classification first, else first row
    SELECT DISTINCT ON (school)
        school, conference
    FROM ref.teams
    ORDER BY school, classification NULLS LAST
),
combined AS (
    SELECT
        ta.team,
        ta.season,
        t.conference,
        tes.games_played,
        ta.overall_run_rate,
        ta.early_down_run_rate,
        ta.third_down_pass_rate,
        ta.red_zone_run_rate,
        sa.overall_success_rate,
        sa.overall_avg_epa,
        sa.third_down_success_rate,
        sa.red_zone_success_rate,
        ta.leading_run_rate,
        ta.trailing_run_rate,
        ROUND((ta.leading_run_rate - ta.trailing_run_rate)::numeric, 4) AS run_rate_delta,
        ROUND(ta.total_plays::numeric / NULLIF(tes.games_played, 0), 1) AS pace_plays_per_game
    FROM tendency_agg ta
    LEFT JOIN success_agg sa ON sa.team = ta.team AND sa.season = ta.season
    LEFT JOIN marts.team_epa_season tes ON tes.team = ta.team AND tes.season = ta.season
    LEFT JOIN teams_deduped t ON t.school = ta.team
)
SELECT
    c.team,
    c.season,
    c.conference,
    c.games_played,
    c.overall_run_rate,
    c.early_down_run_rate,
    c.third_down_pass_rate,
    c.red_zone_run_rate,
    c.overall_success_rate,
    c.overall_avg_epa,
    c.third_down_success_rate,
    c.red_zone_success_rate,
    c.leading_run_rate,
    c.trailing_run_rate,
    c.run_rate_delta,
    c.pace_plays_per_game,
    -- Percentiles (NULL-safe, PARTITION BY season)
    CASE WHEN c.overall_run_rate IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.overall_run_rate)
    END AS overall_run_rate_pctl,
    CASE WHEN c.early_down_run_rate IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.early_down_run_rate)
    END AS early_down_run_rate_pctl,
    CASE WHEN c.third_down_pass_rate IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.third_down_pass_rate)
    END AS third_down_pass_rate_pctl,
    CASE WHEN c.overall_avg_epa IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.overall_avg_epa)
    END AS overall_epa_pctl,
    CASE WHEN c.third_down_success_rate IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.third_down_success_rate)
    END AS third_down_success_pctl,
    CASE WHEN c.red_zone_success_rate IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.red_zone_success_rate)
    END AS red_zone_success_pctl,
    CASE WHEN c.run_rate_delta IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.run_rate_delta)
    END AS run_rate_delta_pctl,
    CASE WHEN c.pace_plays_per_game IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.pace_plays_per_game)
    END AS pace_pctl
FROM combined c;

COMMENT ON VIEW api.team_playcalling_profile IS
'Team playcalling identity with situational tendencies and percentile rankings. '
'One row per team-season. Filter by team, season, conference. '
'Percentiles are per-season. NULL rates indicate insufficient plays in that situation. '
'Backed by materialized views (tendencies + success).';
