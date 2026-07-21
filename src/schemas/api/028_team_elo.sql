-- api.team_elo
-- Season-end house Elo rating per team per season.
-- Thin passthrough of marts.house_elo (Tier 2 analytics,
-- docs/plans/2026-07-21-tier2-analytics-plan.md).
--
-- PostgREST usage:
--   GET /api/team_elo?season=eq.2024&order=elo_rank.asc

CREATE OR REPLACE VIEW api.team_elo AS
SELECT
    team,
    season,
    season_end_elo,
    elo_rank,
    games_played,
    low_confidence,
    cfbd_elo
FROM marts.house_elo;

GRANT SELECT ON api.team_elo TO anon, authenticated;

COMMENT ON VIEW api.team_elo IS 'Season-end house Elo rating per team per season. Columns: team, season, season_end_elo, elo_rank, games_played, low_confidence, cfbd_elo. low_confidence flags teams with <4 games in the season or seasons before 1900; cfbd_elo is CFBD''s own Elo (coverage ~2015+) for side-by-side comparison only. Backed by marts.house_elo.';
