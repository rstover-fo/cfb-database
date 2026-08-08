-- Team performance trajectory year-over-year with era awareness
-- Depends on: marts.team_epa_season, marts.team_season_summary,
--             marts.defensive_havoc, ref.eras, core.games, ref.teams
--
-- 2026-08-08: reconstructed to carry off_epa_rank/def_epa_rank and the
-- wins/games column names that public.team_season_trajectory (public/002) and
-- get_trajectory_averages (public/008) select. The live matview had these
-- columns from an uncommitted apply; this file had drifted behind it, which
-- surfaced when the CORE-ratings stage-2 deploy rebuilt the matview from
-- source (the team_season_summary CASCADE). Rank semantics copied from
-- public.team_epa_season in public/002: RANK within (season, classification)
-- -- classification season-accurate from core.games with ref.teams fallback --
-- by epa_per_play DESC for offense and defensive_havoc.opp_epa_per_play ASC
-- (999 when NULL) for defense. The pre-drift names (total_wins, games_played,
-- ...) are kept alongside the aliases so both column vocabularies resolve.

DROP MATERIALIZED VIEW IF EXISTS marts.team_season_trajectory CASCADE;

CREATE MATERIALIZED VIEW marts.team_season_trajectory AS
WITH team_season_class AS (
    -- Season-accurate classification from the games actually played
    SELECT team, season, MAX(classification) AS classification
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
),
team_metrics AS (
    SELECT
        t.team,
        t.season,
        t.epa_per_play,
        t.success_rate,
        t.epa_tier,
        t.total_plays,
        t.games_played,
        -- Win record from team_season_summary if available
        s.wins AS total_wins,
        s.losses AS total_losses,
        s.conf_wins,
        s.conf_losses,
        -- Recruiting rank
        r.rank AS recruiting_rank,
        r.points AS recruiting_points,
        -- EPA ranks within (season, classification), matching public.team_epa_season
        (RANK() OVER (
            PARTITION BY t.season, COALESCE(tsc.classification, td.classification)
            ORDER BY t.epa_per_play DESC
        ))::INT AS off_epa_rank,
        (RANK() OVER (
            PARTITION BY t.season, COALESCE(tsc.classification, td.classification)
            ORDER BY COALESCE(d.opp_epa_per_play, 999::NUMERIC)
        ))::INT AS def_epa_rank
    FROM marts.team_epa_season t
    LEFT JOIN marts.team_season_summary s
        ON t.team = s.team AND t.season = s.season
    LEFT JOIN marts.defensive_havoc d
        ON t.team = d.team AND t.season = d.season
    LEFT JOIN team_season_class tsc
        ON tsc.team = t.team AND tsc.season = t.season
    LEFT JOIN teams_deduped td
        ON td.school = t.team
    LEFT JOIN recruiting.team_recruiting r
        ON t.team = r.team AND t.season = r.year
)
SELECT
    m.team,
    m.season,
    m.epa_per_play,
    m.success_rate,
    m.epa_tier,
    m.total_plays,
    m.games_played,
    m.games_played AS games,
    m.total_wins,
    m.total_wins AS wins,
    m.total_losses,
    -- Win percentage
    CASE
        WHEN COALESCE(m.total_wins, 0) + COALESCE(m.total_losses, 0) > 0
        THEN ROUND(m.total_wins::NUMERIC / (m.total_wins + m.total_losses), 3)
        ELSE NULL
    END AS win_pct,
    m.conf_wins,
    m.conf_losses,
    m.recruiting_rank,
    m.recruiting_points,
    m.off_epa_rank,
    m.def_epa_rank,
    -- Era assignment (primary era for the season)
    (SELECT e.era_code FROM ref.get_era(m.season::INT) e ORDER BY e.era_code LIMIT 1) AS era_code,
    (SELECT e.era_name FROM ref.get_era(m.season::INT) e ORDER BY e.era_code LIMIT 1) AS era_name,
    -- Year-over-year deltas
    LAG(m.epa_per_play) OVER (PARTITION BY m.team ORDER BY m.season) AS prev_epa,
    m.epa_per_play - LAG(m.epa_per_play) OVER (PARTITION BY m.team ORDER BY m.season) AS epa_delta,
    LAG(m.total_wins::NUMERIC / NULLIF(m.total_wins + m.total_losses, 0)) OVER (PARTITION BY m.team ORDER BY m.season) AS prev_win_pct,
    CASE
        WHEN COALESCE(m.total_wins, 0) + COALESCE(m.total_losses, 0) > 0
        THEN ROUND(m.total_wins::NUMERIC / (m.total_wins + m.total_losses), 3) -
             LAG(m.total_wins::NUMERIC / NULLIF(m.total_wins + m.total_losses, 0)) OVER (PARTITION BY m.team ORDER BY m.season)
        ELSE NULL
    END AS win_pct_delta,
    -- Recruiting trend
    LAG(m.recruiting_rank) OVER (PARTITION BY m.team ORDER BY m.season) AS prev_recruiting_rank,
    LAG(m.recruiting_rank) OVER (PARTITION BY m.team ORDER BY m.season) - m.recruiting_rank AS recruiting_rank_improvement
FROM team_metrics m;

CREATE UNIQUE INDEX ON marts.team_season_trajectory (season, team);
CREATE INDEX ON marts.team_season_trajectory (team);
CREATE INDEX ON marts.team_season_trajectory (era_code, season);
CREATE INDEX ON marts.team_season_trajectory (epa_delta DESC NULLS LAST);
