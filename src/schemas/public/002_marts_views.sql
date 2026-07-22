-- Public-facing views that expose marts materialized views
-- These provide stable read interfaces for the frontend app.
-- Created ad-hoc in Supabase; now tracked in version control.
--
-- public.team_epa_season's `classification` column is SEASON-ACCURATE (derived
-- from core.games per (team, season), ref.teams-current-membership fallback) --
-- see the team_season_class CTE below and the 2026-07-22 changelog entry in
-- docs/SCHEMA_CONTRACT.md.

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
    tfls,
    front_seven_havoc_rate,
    db_havoc_rate
FROM marts.defensive_havoc;

CREATE OR REPLACE VIEW public.team_epa_season
WITH (security_invoker = true)
AS
-- classification here is SEASON-ACCURATE, not "current membership":
-- ref.teams mirrors CFBD /teams (current classification only), so a
-- realignment team (e.g. North Dakota State moving FCS -> FBS for 2026)
-- would otherwise have ALL of its historical seasons reclassified to its
-- new division, leaking a dominant FCS season onto FBS rank partitions.
-- team_season_class derives classification per (team, season) from
-- core.games.home_classification/away_classification (dlt snake_case of
-- CFBD homeClassification/awayClassification -- the classification each
-- team actually carried in its own games that season; same source used by
-- scripts/verify_load.py and scripts/generate_recaps.py). Scans
-- core.games (~90K rows) grouped by (team, season) -- fine for view-time
-- cost here. Falls back to the ref.teams dedup (current-membership) only
-- for team-seasons with no loaded games.
WITH team_season_class AS (
    SELECT
        team,
        season,
        mode() WITHIN GROUP (ORDER BY classification) AS classification
    FROM (
        SELECT g.home_team AS team, g.season, g.home_classification AS classification
        FROM core.games g
        WHERE g.home_classification IS NOT NULL
        UNION ALL
        SELECT g.away_team, g.season, g.away_classification
        FROM core.games g
        WHERE g.away_classification IS NOT NULL
    ) x
    GROUP BY team, season
),
teams_deduped AS (
    -- ref.teams has ~35 duplicate school names; pick FBS classification first, else first row
    -- fallback only: current membership, used when core.games has no rows for a team-season
    SELECT DISTINCT ON (school)
        school, classification
    FROM ref.teams
    ORDER BY school, classification NULLS LAST
)
SELECT
    e.team,
    e.season,
    e.games_played AS games,
    e.total_plays,
    ROUND((e.epa_per_play * e.total_plays::NUMERIC), 2) AS total_epa,
    e.epa_per_play,
    e.success_rate,
    COALESCE(e.explosiveness, 0::NUMERIC) AS explosiveness,
    (RANK() OVER (PARTITION BY e.season, COALESCE(tsc.classification, t.classification) ORDER BY e.epa_per_play DESC))::INT AS off_epa_rank,
    (RANK() OVER (PARTITION BY e.season, COALESCE(tsc.classification, t.classification) ORDER BY COALESCE(d.opp_epa_per_play, 999::NUMERIC)))::INT AS def_epa_rank,
    -- appended LAST: CREATE OR REPLACE VIEW can only add columns at the end
    -- season-accurate classification (core.games), falling back to ref.teams current membership
    COALESCE(tsc.classification, t.classification) AS classification
FROM marts.team_epa_season e
LEFT JOIN marts.defensive_havoc d
    ON e.team = d.team AND e.season = d.season
LEFT JOIN team_season_class tsc
    ON tsc.team = e.team AND tsc.season = e.season
LEFT JOIN teams_deduped t
    ON t.school = e.team;

COMMENT ON VIEW public.team_epa_season IS
    'Season-level EPA metrics per team with off_epa_rank/def_epa_rank scoped PARTITION BY season, classification. classification is season-accurate (core.games home_classification/away_classification per (team, season), ref.teams fallback) -- realignment teams (e.g. North Dakota State moving FCS -> FBS for 2026) keep their historical seasons in the correct classification partition instead of leaking into their current one.';

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
