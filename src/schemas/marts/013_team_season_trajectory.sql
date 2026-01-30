-- Team performance trajectory year-over-year with era awareness
-- Depends on: marts.team_epa_season, ref.eras

DROP MATERIALIZED VIEW IF EXISTS marts.team_season_trajectory CASCADE;

CREATE MATERIALIZED VIEW marts.team_season_trajectory AS
WITH team_metrics AS (
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
        r.points AS recruiting_points
    FROM marts.team_epa_season t
    LEFT JOIN marts.team_season_summary s
        ON t.team = s.team AND t.season = s.season
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
    m.total_wins,
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
