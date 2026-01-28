-- Team history API view
-- Multi-season trends for a team
-- Exposed via PostgREST as /api/team_history

DROP VIEW IF EXISTS api.team_history;

CREATE VIEW api.team_history AS
SELECT
    tss.team,
    tss.season,
    tss.conference,

    -- Record
    tss.games,
    tss.wins,
    tss.losses,
    tss.conf_wins,
    tss.conf_losses,

    -- Scoring
    tss.ppg,
    tss.opp_ppg,
    tss.avg_margin,

    -- Ratings
    tss.sp_rating,
    tss.sp_rank,
    tss.elo,
    tss.fpi,

    -- EPA (if available for that season)
    epa.epa_per_play,
    epa.epa_tier,
    epa.success_rate,
    epa.explosiveness,
    epa.total_plays,

    -- Recruiting
    tss.recruiting_rank,
    tss.recruiting_points

FROM marts.team_season_summary tss
LEFT JOIN marts.team_epa_season epa
    ON epa.team = tss.team AND epa.season = tss.season
ORDER BY tss.team, tss.season DESC;

COMMENT ON VIEW api.team_history IS 'Multi-season team history with records, ratings, and EPA trends';
