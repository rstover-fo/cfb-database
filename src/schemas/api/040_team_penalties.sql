-- Team penalty box-score API view
-- Official per-game penalty counts/yards (both teams on each row).
-- Exposed via PostgREST as /api/team_penalties.

DROP VIEW IF EXISTS api.team_penalties;

CREATE VIEW api.team_penalties AS
SELECT
    game_id,
    season,
    week,
    season_type,
    team,
    opponent,
    home_away,
    penalties,
    penalty_yards,
    opponent_penalties,
    opponent_penalty_yards
FROM marts.team_penalty_box;

COMMENT ON VIEW api.team_penalties IS 'Official box-score penalty counts per (game, team), 2004+: penalties/penalty_yards committed by team, opponent_* committed against them (from CFBD totalPenaltiesYards). Aggregate over season for per-game averages; use api.penalty_log for infraction-level detail (holding, PI, ...).';

-- Grants are part of the definition: an apply that DROPs/recreates the
-- view would otherwise leave the PostgREST roles without read access
-- (no ALTER DEFAULT PRIVILEGES for them in this database).
GRANT SELECT ON api.team_penalties TO anon, authenticated;
