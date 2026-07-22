-- Team leaderboard API view
-- Flexible team rankings/leaderboards by season
-- Query with filters: ?season=eq.2024&order=epa_per_play.desc
-- Exposed via PostgREST as /api/leaderboard_teams
--
-- Rank columns (wins_rank, ppg_rank, defense_ppg_rank, epa_rank) are computed
-- WITHIN classification (PARTITION BY season, classification), not across all
-- of FBS+FCS+lower. All rows are still returned (no WHERE fbs filter) --
-- consumers filter by classification themselves.
--
-- `classification` is SEASON-ACCURATE, not "current membership": ref.teams
-- mirrors CFBD /teams (current classification only), so a realignment team
-- (e.g. North Dakota State moving FCS -> FBS for 2026) would otherwise have
-- ALL of its historical seasons reclassified to its new division, leaking a
-- dominant FCS season onto FBS rank partitions/leaderboards. team_season_class
-- derives classification per (team, season) from
-- core.games.home_classification/away_classification (dlt snake_case of CFBD
-- homeClassification/awayClassification -- the classification each team
-- actually carried in its own games that season; same source used by
-- scripts/verify_load.py and scripts/generate_recaps.py). Scans core.games
-- (~90K rows) grouped by (team, season) -- fine for view-time cost here.
-- Falls back to the ref.teams dedup (current-membership) only for
-- team-seasons with no loaded games.

DROP VIEW IF EXISTS api.leaderboard_teams;

CREATE VIEW api.leaderboard_teams AS
WITH team_season_class AS (
    SELECT
        team,
        season,
        mode() WITHIN GROUP (ORDER BY classification) AS classification
    FROM (
        SELECT g.home_team AS team, g.season, g.home_classification AS classification
        FROM core.games g
        WHERE g.home_classification IS NOT NULL
        UNION ALL
        SELECT g.away_team, g.season, g.away_classification
        FROM core.games g
        WHERE g.away_classification IS NOT NULL
    ) x
    GROUP BY team, season
),
teams_deduped AS (
    -- ref.teams has ~35 duplicate school names; pick FBS classification first, else first row
    -- fallback only: current membership, used when core.games has no rows for a team-season
    SELECT DISTINCT ON (school)
        school, classification
    FROM ref.teams
    ORDER BY school, classification NULLS LAST
)
SELECT
    tss.team,
    tss.conference,
    tss.season,
    COALESCE(tsc.classification, t.classification) AS classification,

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
    RANK() OVER (PARTITION BY tss.season, COALESCE(tsc.classification, t.classification) ORDER BY tss.wins DESC, tss.avg_margin DESC) AS wins_rank,
    RANK() OVER (PARTITION BY tss.season, COALESCE(tsc.classification, t.classification) ORDER BY tss.ppg DESC) AS ppg_rank,
    RANK() OVER (PARTITION BY tss.season, COALESCE(tsc.classification, t.classification) ORDER BY tss.opp_ppg ASC) AS defense_ppg_rank,
    RANK() OVER (PARTITION BY tss.season, COALESCE(tsc.classification, t.classification) ORDER BY epa.epa_per_play DESC NULLS LAST) AS epa_rank

FROM marts.team_season_summary tss
LEFT JOIN marts.team_epa_season epa
    ON epa.team = tss.team AND epa.season = tss.season
LEFT JOIN team_season_class tsc
    ON tsc.team = tss.team AND tsc.season = tss.season
LEFT JOIN teams_deduped t
    ON t.school = tss.team;

COMMENT ON VIEW api.leaderboard_teams IS 'Team leaderboard with records, ratings, EPA, and computed rankings. Rank columns (wins_rank, ppg_rank, defense_ppg_rank, epa_rank) are computed within classification (PARTITION BY season, classification) -- FBS teams rank among ~136 FBS peers, not all classifications combined. All rows returned regardless of classification; filter client-side. classification is season-accurate (core.games home_classification/away_classification per (team, season), ref.teams fallback) -- realignment teams (e.g. North Dakota State moving FCS -> FBS for 2026) keep their historical seasons in the correct classification partition instead of leaking into their current one.';

-- Re-grant on every apply: this file DROPs the view first, which discards
-- existing grants (no ALTER DEFAULT PRIVILEGES in this database).
GRANT SELECT ON api.leaderboard_teams TO anon, authenticated;
