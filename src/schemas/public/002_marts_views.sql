-- Public-facing views that expose marts materialized views
-- These provide stable read interfaces for the frontend app.
-- Created ad-hoc in Supabase; now tracked in version control.

CREATE OR REPLACE VIEW public.defensive_havoc
WITH (security_invoker = true)
AS
SELECT
    team,
    season,
    defensive_plays,
    opp_epa_per_play,
    opp_success_rate,
    havoc_plays,
    havoc_rate,
    sacks,
    interceptions,
    fumbles,
    turnovers_forced,
    stuffs,
    stuff_rate,
    tfls
FROM marts.defensive_havoc;

CREATE OR REPLACE VIEW public.team_epa_season
WITH (security_invoker = true)
AS
SELECT
    e.team,
    e.season,
    e.games_played AS games,
    e.total_plays,
    ROUND((e.epa_per_play * e.total_plays::NUMERIC), 2) AS total_epa,
    e.epa_per_play,
    e.success_rate,
    COALESCE(e.explosiveness, 0::NUMERIC) AS explosiveness,
    (RANK() OVER (PARTITION BY e.season ORDER BY e.epa_per_play DESC))::INT AS off_epa_rank,
    (RANK() OVER (PARTITION BY e.season ORDER BY COALESCE(d.opp_epa_per_play, 999::NUMERIC)))::INT AS def_epa_rank
FROM marts.team_epa_season e
LEFT JOIN marts.defensive_havoc d
    ON e.team = d.team AND e.season = d.season;

CREATE OR REPLACE VIEW public.team_season_epa
WITH (security_invoker = true)
AS
SELECT
    team,
    season,
    epa_per_play,
    success_rate,
    explosiveness,
    epa_tier,
    total_plays,
    games_played
FROM marts.team_epa_season;

CREATE OR REPLACE VIEW public.team_season_trajectory
WITH (security_invoker = true)
AS
SELECT
    team,
    season,
    epa_per_play,
    success_rate,
    off_epa_rank,
    def_epa_rank,
    win_pct,
    wins,
    games,
    recruiting_rank,
    era_code,
    era_name,
    prev_epa,
    epa_delta
FROM marts.team_season_trajectory;

CREATE OR REPLACE VIEW public.team_style_profile
WITH (security_invoker = true)
AS
SELECT
    team,
    season,
    run_rate,
    pass_rate,
    epa_rushing,
    epa_passing,
    plays_per_game,
    tempo_category,
    offensive_identity,
    def_epa_vs_run,
    def_epa_vs_pass
FROM marts.team_style_profile;

CREATE OR REPLACE VIEW public.team_tempo_metrics
WITH (security_invoker = true)
AS
SELECT
    season,
    team,
    games,
    total_plays,
    plays_per_game,
    tempo_tier,
    epa_per_play,
    success_rate,
    explosiveness
FROM marts.team_tempo_metrics;
