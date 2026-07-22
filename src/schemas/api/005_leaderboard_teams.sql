-- Team leaderboard API view
-- Flexible team rankings/leaderboards by season
-- Query with filters: ?season=eq.2024&order=epa_per_play.desc
-- Exposed via PostgREST as /api/leaderboard_teams
--
-- Rank columns (wins_rank, ppg_rank, defense_ppg_rank, epa_rank) are computed
-- WITHIN classification (PARTITION BY season, classification), not across all
-- of FBS+FCS+lower. All rows are still returned (no WHERE fbs filter) --
-- consumers filter by classification themselves.

DROP VIEW IF EXISTS api.leaderboard_teams;

CREATE VIEW api.leaderboard_teams AS
WITH teams_deduped AS (
    -- ref.teams has ~35 duplicate school names; pick FBS classification first, else first row
    SELECT DISTINCT ON (school)
        school, classification
    FROM ref.teams
    ORDER BY school, classification NULLS LAST
)
SELECT
    tss.team,
    tss.conference,
    tss.season,
    t.classification,

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

    -- Computed rankings within season AND classification (fix: previously
    -- ranked across FBS+FCS+lower, so FBS teams could show ranks like #176
    -- when FBS has ~136 teams)
    RANK() OVER (PARTITION BY tss.season, t.classification ORDER BY tss.wins DESC, tss.avg_margin DESC) AS wins_rank,
    RANK() OVER (PARTITION BY tss.season, t.classification ORDER BY tss.ppg DESC) AS ppg_rank,
    RANK() OVER (PARTITION BY tss.season, t.classification ORDER BY tss.opp_ppg ASC) AS defense_ppg_rank,
    RANK() OVER (PARTITION BY tss.season, t.classification ORDER BY epa.epa_per_play DESC NULLS LAST) AS epa_rank

FROM marts.team_season_summary tss
LEFT JOIN marts.team_epa_season epa
    ON epa.team = tss.team AND epa.season = tss.season
LEFT JOIN teams_deduped t
    ON t.school = tss.team;

COMMENT ON VIEW api.leaderboard_teams IS 'Team leaderboard with records, ratings, EPA, and computed rankings. Rank columns (wins_rank, ppg_rank, defense_ppg_rank, epa_rank) are computed within classification (PARTITION BY season, classification) -- FBS teams rank among ~136 FBS peers, not all classifications combined. All rows returned regardless of classification; filter client-side.';
