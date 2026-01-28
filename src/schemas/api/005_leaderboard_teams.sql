-- Team leaderboard API view
-- Flexible team rankings/leaderboards by season
-- Query with filters: ?season=eq.2024&order=epa_per_play.desc
-- Exposed via PostgREST as /api/leaderboard_teams

DROP VIEW IF EXISTS api.leaderboard_teams;

CREATE VIEW api.leaderboard_teams AS
SELECT
    tss.team,
    tss.conference,
    tss.season,

    -- Record
    tss.games,
    tss.wins,
    tss.losses,
    ROUND(tss.wins::numeric / NULLIF(tss.games, 0), 3) AS win_pct,
    tss.conf_wins,
    tss.conf_losses,

    -- Scoring
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
    epa.total_plays,

    -- Recruiting
    tss.recruiting_rank,
    tss.recruiting_points,

    -- Computed rankings within season
    RANK() OVER (PARTITION BY tss.season ORDER BY tss.wins DESC, tss.avg_margin DESC) AS wins_rank,
    RANK() OVER (PARTITION BY tss.season ORDER BY tss.ppg DESC) AS ppg_rank,
    RANK() OVER (PARTITION BY tss.season ORDER BY tss.opp_ppg ASC) AS defense_ppg_rank,
    RANK() OVER (PARTITION BY tss.season ORDER BY epa.epa_per_play DESC NULLS LAST) AS epa_rank

FROM marts.team_season_summary tss
LEFT JOIN marts.team_epa_season epa
    ON epa.team = tss.team AND epa.season = tss.season;

COMMENT ON VIEW api.leaderboard_teams IS 'Team leaderboard with records, ratings, EPA, and computed rankings';
