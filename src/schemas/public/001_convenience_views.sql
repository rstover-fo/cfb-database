-- Convenience views in public schema
-- These expose core/ref tables with simplified column sets for app consumption.
-- Created ad-hoc in Supabase; now tracked in version control.

CREATE OR REPLACE VIEW public.teams AS
SELECT
    id,
    school,
    mascot,
    abbreviation,
    conference,
    classification,
    color,
    alternate_color,
    twitter,
    location__id,
    location__name,
    location__city,
    location__state,
    location__zip,
    location__country_code,
    location__timezone,
    location__latitude,
    location__longitude,
    location__elevation,
    location__capacity,
    location__construction_year,
    location__grass,
    location__dome,
    _dlt_load_id,
    _dlt_id,
    division
FROM ref.teams;

CREATE OR REPLACE VIEW public.teams_with_logos AS
SELECT
    t.id,
    t.school,
    t.mascot,
    t.abbreviation,
    t.conference,
    t.classification,
    t.color,
    t.alternate_color AS alt_color,
    l.value AS logo,
    l_dark.value AS alt_logo,
    t.division
FROM public.teams t
LEFT JOIN ref.teams__logos l
    ON l._dlt_parent_id = t._dlt_id
    AND l.value NOT LIKE '%dark%'
LEFT JOIN ref.teams__logos l_dark
    ON l_dark._dlt_parent_id = t._dlt_id
    AND l_dark.value LIKE '%dark%';

CREATE OR REPLACE VIEW public.games AS
SELECT
    id,
    season,
    week,
    start_date,
    home_team,
    home_points,
    away_team,
    away_points,
    completed,
    conference_game,
    neutral_site
FROM core.games;

CREATE OR REPLACE VIEW public.roster AS
SELECT
    id,
    first_name,
    last_name,
    jersey,
    "position",
    height,
    weight,
    home_city,
    home_state,
    year,
    team
FROM core.roster;
