-- Roster lookup API view
-- Stable view of core.roster for cfb-scout player matching
-- Exposed via PostgREST as /api/roster_lookup

DROP VIEW IF EXISTS api.roster_lookup;

CREATE VIEW api.roster_lookup AS
SELECT
    id,
    first_name,
    last_name,
    team,
    position,
    height,
    weight,
    year,
    jersey,
    home_city,
    home_state,
    home_country
FROM core.roster
WHERE team IS NOT NULL;

COMMENT ON VIEW api.roster_lookup IS 'Stable roster view for cfb-scout player matching';

-- Grants are part of the definition: an apply that DROPs/recreates the
-- view would otherwise leave the PostgREST roles without read access
-- (no ALTER DEFAULT PRIVILEGES for them in this database).
GRANT SELECT ON api.roster_lookup TO anon, authenticated;
