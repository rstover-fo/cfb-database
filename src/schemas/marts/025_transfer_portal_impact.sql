-- Transfer portal impact: tracks portal activity and correlates with team performance changes
-- Grain: Team × Season
-- NOTE: transfer_portal has NO player_id — uses star ratings as quality proxy
-- Portal data available from ~2021+

DROP MATERIALIZED VIEW IF EXISTS marts.transfer_portal_impact CASCADE;

CREATE MATERIALIZED VIEW marts.transfer_portal_impact AS
WITH portal_in AS (
    -- Players transferring INTO each team
    SELECT
        tp.destination AS team,
        tp.season,
        COUNT(*) AS transfers_in,
        ROUND(AVG(tp.stars)::numeric, 2) AS avg_incoming_stars,
        ROUND(AVG(tp.rating)::numeric, 4) AS avg_incoming_rating,
        COUNT(*) FILTER (WHERE tp.stars >= 4) AS incoming_high_stars
    FROM recruiting.transfer_portal tp
    WHERE tp.destination IS NOT NULL
    GROUP BY tp.destination, tp.season
),
portal_out AS (
    -- Players transferring OUT of each team
    SELECT
        tp.origin AS team,
        tp.season,
        COUNT(*) AS transfers_out
    FROM recruiting.transfer_portal tp
    WHERE tp.origin IS NOT NULL
    GROUP BY tp.origin, tp.season
),
roster_size AS (
    -- Total roster size per team-season for portal dependency calculation
    SELECT
        team,
        year AS season,
        COUNT(*) AS total_roster
    FROM core.roster
    GROUP BY team, year
),
-- Team performance for current and prior season
team_perf AS (
    SELECT
        tss.team,
        tss.season,
        tss.conference,
        tss.wins,
        tss.sp_rating,
        LAG(tss.wins) OVER (PARTITION BY tss.team ORDER BY tss.season) AS prior_season_wins,
        LAG(tss.sp_rating) OVER (PARTITION BY tss.team ORDER BY tss.season) AS prior_season_sp_rating
    FROM marts.team_season_summary tss
),
combined AS (
    SELECT
        tp.team,
        tp.season,
        tp.conference,

        -- Portal activity
        COALESCE(pi.transfers_in, 0)::int AS transfers_in,
        COALESCE(po.transfers_out, 0)::int AS transfers_out,
        (COALESCE(pi.transfers_in, 0) - COALESCE(po.transfers_out, 0))::int AS net_transfers,
        pi.avg_incoming_stars,
        pi.avg_incoming_rating,
        COALESCE(pi.incoming_high_stars, 0)::int AS incoming_high_stars,

        -- Prior-year context
        tp.prior_season_wins,
        tp.prior_season_sp_rating,

        -- Current season results
        tp.wins AS current_wins,
        tp.sp_rating AS current_sp_rating,

        -- Impact metrics
        (tp.wins - tp.prior_season_wins)::int AS win_delta,
        ROUND((tp.sp_rating - tp.prior_season_sp_rating)::numeric, 2) AS sp_delta,
        ROUND(
            COALESCE(pi.transfers_in, 0)::numeric / NULLIF(rs.total_roster, 0),
            4
        ) AS portal_dependency,
        ROUND(
            (tp.wins - tp.prior_season_wins)::numeric / NULLIF(pi.transfers_in, 0),
            4
        ) AS win_delta_per_transfer_in

    FROM team_perf tp
    LEFT JOIN portal_in pi ON pi.team = tp.team AND pi.season = tp.season
    LEFT JOIN portal_out po ON po.team = tp.team AND po.season = tp.season
    LEFT JOIN roster_size rs ON rs.team = tp.team AND rs.season = tp.season
    WHERE tp.prior_season_wins IS NOT NULL  -- need prior season for delta
      AND (pi.transfers_in IS NOT NULL OR po.transfers_out IS NOT NULL)  -- has portal activity
)
SELECT
    c.*,

    -- Percentiles (per season)
    CASE WHEN c.net_transfers IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.net_transfers)
    END AS net_transfers_pctl,
    CASE WHEN c.win_delta IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.win_delta)
    END AS win_delta_pctl,
    CASE WHEN c.portal_dependency IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY c.season ORDER BY c.portal_dependency)
    END AS portal_dependency_pctl

FROM combined c;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.transfer_portal_impact (team, season);

-- Query indexes
CREATE INDEX ON marts.transfer_portal_impact (season, win_delta DESC);
CREATE INDEX ON marts.transfer_portal_impact (season, net_transfers DESC);
CREATE INDEX ON marts.transfer_portal_impact (conference, season);
