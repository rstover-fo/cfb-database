-- Recruit lookup API view
-- Stable view of recruiting.recruits for cfb-scout recruiting data
-- Exposed via PostgREST as /api/recruit_lookup

DROP VIEW IF EXISTS api.recruit_lookup;

CREATE VIEW api.recruit_lookup AS
SELECT
    id,
    athlete_id,
    recruit_type,
    year,
    ranking,
    name,
    school,
    committed_to,
    position,
    height,
    weight,
    stars,
    rating,
    city,
    state_province,
    country
FROM recruiting.recruits;

COMMENT ON VIEW api.recruit_lookup IS 'Stable recruiting view for cfb-scout recruiting data';
