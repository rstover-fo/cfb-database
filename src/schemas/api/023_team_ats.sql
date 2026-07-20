-- Team ATS (against-the-spread) records API view
-- Thin passthrough of marts.team_ats_records (ATS win/loss/push record, cover margin)
-- Query with filters: ?season=eq.2024&order=ats_win_pct.desc
-- Exposed via PostgREST as /api/team_ats

DROP VIEW IF EXISTS api.team_ats;

CREATE VIEW api.team_ats AS
SELECT *
FROM marts.team_ats_records;

COMMENT ON VIEW api.team_ats IS 'Team against-the-spread records by season: games, ats_wins/ats_losses/ats_pushes, avg_cover_margin, and computed ats_win_pct. Backed by marts.team_ats_records.';
