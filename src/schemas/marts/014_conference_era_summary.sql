-- Conference strength by era
-- Aggregates team performance within conferences across CFB eras

DROP MATERIALIZED VIEW IF EXISTS marts.conference_era_summary CASCADE;

CREATE MATERIALIZED VIEW marts.conference_era_summary AS
WITH conference_seasons AS (
    SELECT
        e.era_code,
        e.era_name,
        t.season,
        g.home_conference AS conference,
        AVG(t.epa_per_play)::NUMERIC(6,4) AS avg_epa,
        AVG(t.success_rate)::NUMERIC(5,3) AS avg_success_rate,
        COUNT(DISTINCT t.team) AS teams
    FROM marts.team_epa_season t
    JOIN core.games g ON t.team = g.home_team AND t.season = g.season
    CROSS JOIN LATERAL ref.get_era(t.season::INT) e
    WHERE g.home_conference IS NOT NULL
      AND g.home_conference != ''
    GROUP BY e.era_code, e.era_name, t.season, g.home_conference
)
SELECT
    era_code,
    era_name,
    conference,
    COUNT(DISTINCT season) AS seasons,
    AVG(avg_epa)::NUMERIC(6,4) AS avg_epa,
    AVG(avg_success_rate)::NUMERIC(5,3) AS avg_success_rate,
    AVG(teams)::NUMERIC(4,1) AS avg_teams,
    -- Best and worst seasons for the conference
    MAX(avg_epa)::NUMERIC(6,4) AS best_season_epa,
    MIN(avg_epa)::NUMERIC(6,4) AS worst_season_epa,
    -- Era-level ranking
    RANK() OVER (PARTITION BY era_code ORDER BY AVG(avg_epa) DESC) AS era_rank
FROM conference_seasons
GROUP BY era_code, era_name, conference;

CREATE UNIQUE INDEX ON marts.conference_era_summary (era_code, conference);
CREATE INDEX ON marts.conference_era_summary (conference);
CREATE INDEX ON marts.conference_era_summary (era_rank);
