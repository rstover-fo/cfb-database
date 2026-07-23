-- Penalty event log API view
-- Play-derived penalty events with best-effort parsed infraction/attribution.
-- Exposed via PostgREST as /api/penalty_log; also the surface that lets
-- run_analyst_query answer infraction-level questions ("holding calls
-- against X's opponents vs their season average").

DROP VIEW IF EXISTS api.penalty_log;

CREATE VIEW api.penalty_log AS
SELECT
    play_id,
    game_id,
    season,
    week,
    season_type,
    period,
    down,
    distance,
    offense,
    defense,
    play_type,
    is_penalty_play_type,
    penalized_team,
    benefiting_team,
    infraction,
    penalty_yards,
    declined,
    offsetting,
    no_play,
    multi_penalty,
    yards_gained,
    ppa,
    play_text,
    parse_ok
FROM marts.penalty_log;

COMMENT ON VIEW api.penalty_log IS 'Play-derived penalty events, one row per play carrying penalty text (2004+). infraction/penalized_team are BEST-EFFORT parses of CFBD free-text play_text (four provider formats; see marts/041): infraction = ''Unknown'' or penalized_team IS NULL means unclassified, not absent -- filter on parse_ok for fully-attributed rows and treat counts as floors. is_penalty_play_type separates flag-was-the-play rows from penalties embedded in other plays (declined/offsetting/tacked-on). For official per-game counts use api.team_penalties.';

-- Grants are part of the definition: an apply that DROPs/recreates the
-- view would otherwise leave the PostgREST roles without read access
-- (no ALTER DEFAULT PRIVILEGES for them in this database).
GRANT SELECT ON api.penalty_log TO anon, authenticated;
