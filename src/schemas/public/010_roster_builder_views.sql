-- Roster Builder search views
-- Expose recruiting.recruits and recruiting.transfer_portal to PostgREST
-- for the cfb-app "Armchair GM" roster builder feature.
--
-- These are lightweight wrappers with search-friendly column selections.
-- See: cfb-app/docs/plans/2026-02-06-feat-roster-builder-armchair-gm-plan.md

-- =============================================================================
-- TRANSFER PORTAL SEARCH
-- =============================================================================

CREATE OR REPLACE VIEW public.transfer_portal_search
WITH (security_invoker = true)
AS
SELECT
    season,
    first_name,
    last_name,
    "position",
    origin,
    destination,
    stars,
    rating,
    transfer_date,
    eligibility
FROM recruiting.transfer_portal;

COMMENT ON VIEW public.transfer_portal_search IS
    'Transfer portal entries for roster builder search. Consumed by cfb-app.';

-- =============================================================================
-- RECRUITS SEARCH
-- =============================================================================

CREATE OR REPLACE VIEW public.recruits_search
WITH (security_invoker = true)
AS
SELECT
    id,
    athlete_id,
    year,
    name,
    "position",
    height,
    weight,
    stars,
    rating,
    ranking,
    committed_to,
    school,
    city,
    state_province,
    country
FROM recruiting.recruits;

COMMENT ON VIEW public.recruits_search IS
    'Recruiting class entries for roster builder search. Consumed by cfb-app.';
