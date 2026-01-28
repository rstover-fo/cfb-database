-- Defensive havoc metrics: disruptive plays and opponent EPA
-- Grain: Team × Season (defensive perspective)
-- Includes: stuffs, sacks, turnovers, havoc rate, opponent EPA

DROP MATERIALIZED VIEW IF EXISTS marts.defensive_havoc CASCADE;

CREATE MATERIALIZED VIEW marts.defensive_havoc AS
WITH defensive_plays AS (
    SELECT
        p.defense AS team,
        g.season,
        p.ppa,
        p.play_type,
        p.yards_gained,

        NOT is_garbage_time(p.period::integer, p.score_diff::integer) AS is_competitive,

        -- Havoc plays (disruptive plays)
        CASE
            WHEN p.play_type ILIKE '%sack%' THEN true
            WHEN p.play_type ILIKE '%interception%' THEN true
            WHEN p.play_type ILIKE '%fumble%' AND p.play_type ILIKE '%lost%' THEN true
            WHEN p.play_type ILIKE '%fumble recovery%' THEN true
            ELSE false
        END AS is_havoc,

        -- Specific havoc types
        CASE WHEN p.play_type ILIKE '%sack%' THEN 1 ELSE 0 END AS is_sack,
        CASE WHEN p.play_type ILIKE '%interception%' THEN 1 ELSE 0 END AS is_interception,
        CASE WHEN p.play_type ILIKE '%fumble%' THEN 1 ELSE 0 END AS is_fumble,

        -- Stuff: rush for <= 0 yards
        CASE
            WHEN (p.play_type ILIKE '%rush%' OR p.play_type ILIKE '%run%')
                AND COALESCE(p.yards_gained, 0) <= 0 THEN true
            ELSE false
        END AS is_stuff,

        -- TFL: tackle for loss (any play for negative yards)
        CASE WHEN COALESCE(p.yards_gained, 0) < 0 THEN true ELSE false END AS is_tfl

    FROM core.plays p
    JOIN core.games g ON p.game_id = g.id
    WHERE p.defense IS NOT NULL
)
SELECT
    team,
    season,

    -- Play counts
    COUNT(*) FILTER (WHERE is_competitive) AS defensive_plays,

    -- Opponent EPA (lower is better for defense)
    ROUND(AVG(ppa) FILTER (WHERE is_competitive)::numeric, 4) AS opp_epa_per_play,
    ROUND(AVG(CASE WHEN ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE is_competitive)::numeric, 4) AS opp_success_rate,

    -- Havoc plays
    SUM(CASE WHEN is_competitive AND is_havoc THEN 1 ELSE 0 END)::int AS havoc_plays,
    ROUND(
        SUM(CASE WHEN is_competitive AND is_havoc THEN 1 ELSE 0 END)::numeric /
        NULLIF(COUNT(*) FILTER (WHERE is_competitive), 0),
        4
    ) AS havoc_rate,

    -- Sacks
    SUM(CASE WHEN is_competitive THEN is_sack ELSE 0 END)::int AS sacks,

    -- Interceptions
    SUM(CASE WHEN is_competitive THEN is_interception ELSE 0 END)::int AS interceptions,

    -- Fumbles forced/recovered
    SUM(CASE WHEN is_competitive THEN is_fumble ELSE 0 END)::int AS fumbles,

    -- Total turnovers
    SUM(CASE WHEN is_competitive THEN is_interception + is_fumble ELSE 0 END)::int AS turnovers_forced,

    -- Stuffs (rushes for <= 0 yards)
    SUM(CASE WHEN is_competitive AND is_stuff THEN 1 ELSE 0 END)::int AS stuffs,
    ROUND(
        SUM(CASE WHEN is_competitive AND is_stuff THEN 1 ELSE 0 END)::numeric /
        NULLIF(COUNT(*) FILTER (WHERE is_competitive AND (play_type ILIKE '%rush%' OR play_type ILIKE '%run%')), 0),
        4
    ) AS stuff_rate,

    -- TFLs
    SUM(CASE WHEN is_competitive AND is_tfl THEN 1 ELSE 0 END)::int AS tfls

FROM defensive_plays
GROUP BY team, season;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.defensive_havoc (team, season);

-- Query indexes
CREATE INDEX ON marts.defensive_havoc (season);
CREATE INDEX ON marts.defensive_havoc (havoc_rate DESC);
CREATE INDEX ON marts.defensive_havoc (opp_epa_per_play ASC);
