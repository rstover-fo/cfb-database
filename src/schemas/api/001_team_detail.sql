-- Team detail API view
-- Single team page data: current season + ratings + recruiting + EPA
-- Exposed via PostgREST as /api/team_detail

CREATE SCHEMA IF NOT EXISTS api;

DROP VIEW IF EXISTS api.team_detail;

CREATE VIEW api.team_detail AS
SELECT
    t.school,
    t.mascot,
    t.abbreviation,
    t.color,
    t.alternate_color,
    logo.value AS logo_url,  -- Primary logo from child table
    t.conference,
    t.classification,

    -- Current season summary (most recent)
    tss.season AS current_season,
    tss.games,
    tss.wins,
    tss.losses,
    tss.conf_wins,
    tss.conf_losses,
    tss.ppg,
    tss.opp_ppg,
    tss.avg_margin,

    -- Ratings
    tss.sp_rating,
    tss.sp_rank,
    tss.sp_offense,
    tss.sp_defense,
    tss.elo,
    tss.fpi,

    -- EPA metrics
    epa.epa_per_play,
    epa.epa_tier,
    epa.success_rate,
    epa.explosiveness,

    -- Recruiting
    tss.recruiting_rank,
    tss.recruiting_points

FROM ref.teams t
LEFT JOIN LATERAL (
    SELECT value FROM ref.teams__logos
    WHERE _dlt_parent_id = t._dlt_id AND _dlt_list_idx = 0
    LIMIT 1
) logo ON true
LEFT JOIN LATERAL (
    SELECT * FROM marts.team_season_summary
    WHERE team = t.school
    ORDER BY season DESC LIMIT 1
) tss ON true
LEFT JOIN LATERAL (
    SELECT * FROM marts.team_epa_season
    WHERE team = t.school
    ORDER BY season DESC LIMIT 1
) epa ON true
WHERE t.classification = 'fbs';  -- Only FBS teams by default

COMMENT ON VIEW api.team_detail IS 'Team page data with current season stats, ratings, and EPA metrics';
