-- Defensive havoc metrics: disruptive plays and opponent EPA
-- Grain: Team × Season (defensive perspective)
-- Includes: stuffs, sacks, turnovers, havoc rate, opponent EPA
--
-- ============================================================================
-- HAVOC-RATE SOURCE OF TRUTH: stats.game_havoc (CFBD /stats/game/havoc)
-- ============================================================================
-- Phase 4 (2026-07-19 tier1 unlock): the havoc-rate family is re-sourced from
-- CFBD's authoritative per-game havoc table instead of the old
-- play_type ILIKE '%sack%'/'%interception%'/'%fumble%' string heuristic.
--
-- ASSUMED stats.game_havoc columns (dlt-flattened from the /stats/game/havoc
-- response; the LIVE table could NOT be inspected when this was written, so we
-- code to the API-documented shape). Each game row carries offense/defense
-- havoc splits, each with total / frontSeven / db sub-fields. dlt snake-cases
-- and double-underscores nested objects, so the DEFENSE split (havoc CREATED by
-- this team's defense -- what we want here) is ASSUMED to be:
--     defensive_havoc__total        -- overall defensive havoc rate (fraction 0..1)
--     defensive_havoc__front_seven  -- front-seven havoc rate (fraction 0..1)
--     defensive_havoc__db           -- defensive-back havoc rate (fraction 0..1)
-- (offensive_havoc__* is assumed present but is unused here.)
-- game_id -> season is derived by joining core.games, so NO `season` column is
-- assumed to exist on stats.game_havoc itself.
--
-- >>> DEPLOY-FAILURE DIAGNOSIS <<<
-- 1. If CREATE fails with 'column "defensive_havoc__..." does not exist', the
--    live dlt column names differ from the assumption above. Most likely
--    alternative: the API nests under `defense` (not `defensiveHavoc`), giving
--        defense__total / defense__front_seven / defense__db
--    Fix: swap the three column refs in the game_havoc_season CTE below (single
--    edit point) to the live names, then re-deploy.
-- 2. If the empty-guard at the bottom RAISEs (havoc_rate NULL for every row),
--    stats.game_havoc contributed nothing: gate table empty, wrong dlt column
--    names, or game_havoc.team not matching core.plays.defense naming.
-- 3. If the test_defensive_havoc [0,1] range check fails post-deploy, the live
--    havoc values are percentages (0..100) not fractions; divide by 100 in the
--    game_havoc_season CTE.
--
-- AGGREGATION CHOICE (game -> season): SIMPLE AVG of per-game defensive havoc
-- rates. The endpoint provides RATES ONLY (no per-game defensive-play counts),
-- so snap-weighting is not possible from game_havoc alone. havoc_plays is then
-- reconstructed as ROUND(havoc_rate * defensive_plays); it is NULL for
-- team-seasons with no game_havoc coverage.
--
-- DISRUPTIVE COUNTS (sacks/interceptions/fumbles/turnovers_forced/stuffs/
-- stuff_rate/tfls): RETAINED as play_type-derived APPROXIMATIONS (documented).
-- stats.team_season_stats (CFBD /stats/season, EAV) was evaluated as an
-- authoritative alternative but rejected for this pass: it has no 'stuffs'
-- analogue, its 'tacklesForLoss' differs from this mart's TFL definition (any
-- play for negative yards), and the defensive-vs-offensive perspective of its
-- 'sacks'/'interceptions' stat_names could not be confirmed against the live
-- table this session. Re-source once the stat_name catalog + perspective are
-- verified live (a VALUES-block mapping CTE keyed on stat_name would slot in).
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS marts.defensive_havoc CASCADE;

CREATE MATERIALIZED VIEW marts.defensive_havoc AS
WITH defensive_plays AS (
    SELECT
        p.defense AS team,
        g.season,
        p.ppa,
        p.play_type,

        -- Inlined garbage time check for performance
        NOT (
            (p.period = 4 AND ABS(COALESCE(p.score_diff, 0)) > 28) OR
            (p.period >= 3 AND ABS(COALESCE(p.score_diff, 0)) > 35)
        ) AS is_competitive,

        -- Specific havoc types (play_type-derived APPROXIMATIONS; see header)
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
),
plays_agg AS (
    SELECT
        team,
        season,

        -- Play counts
        COUNT(*) FILTER (WHERE is_competitive) AS defensive_plays,

        -- Opponent EPA (lower is better for defense) -- kept plays-derived: real EPA
        ROUND(AVG(ppa) FILTER (WHERE is_competitive)::numeric, 4) AS opp_epa_per_play,
        ROUND(AVG(CASE WHEN ppa > 0 THEN 1.0 ELSE 0.0 END) FILTER (WHERE is_competitive)::numeric, 4) AS opp_success_rate,

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
    GROUP BY team, season
),
-- Authoritative havoc rates from CFBD, aggregated game -> season (simple AVG).
-- game_id -> season via core.games (no season column assumed on game_havoc).
game_havoc_season AS (
    SELECT
        gh.team,
        g.season,
        ROUND(AVG(gh.defensive_havoc__total)::numeric, 4)       AS havoc_rate,
        ROUND(AVG(gh.defensive_havoc__front_seven)::numeric, 4) AS front_seven_havoc_rate,
        ROUND(AVG(gh.defensive_havoc__db)::numeric, 4)          AS db_havoc_rate
    FROM stats.game_havoc gh
    JOIN core.games g ON gh.game_id = g.id
    GROUP BY gh.team, g.season
)
SELECT
    pa.team,
    pa.season,

    -- Opponent-EPA family (plays-derived, real EPA)
    pa.defensive_plays,
    pa.opp_epa_per_play,
    pa.opp_success_rate,

    -- Havoc plays: reconstructed count = authoritative season rate x competitive
    -- defensive plays (game_havoc supplies rates, not counts). NULL when the
    -- team-season has no game_havoc coverage.
    ROUND(gh.havoc_rate * pa.defensive_plays)::int AS havoc_plays,

    -- Authoritative havoc rate (CFBD stats.game_havoc, season AVG)
    gh.havoc_rate,

    -- Disruptive counts (play_type-derived approximations; see header)
    pa.sacks,
    pa.interceptions,
    pa.fumbles,
    pa.turnovers_forced,
    pa.stuffs,
    pa.stuff_rate,
    pa.tfls,

    -- Additive havoc splits (CFBD stats.game_havoc, season AVG)
    gh.front_seven_havoc_rate,
    gh.db_havoc_rate

FROM plays_agg pa
LEFT JOIN game_havoc_season gh
    ON pa.team = gh.team AND pa.season = gh.season;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.defensive_havoc (team, season);

-- Query indexes
CREATE INDEX ON marts.defensive_havoc (season);
CREATE INDEX ON marts.defensive_havoc (havoc_rate DESC);
CREATE INDEX ON marts.defensive_havoc (opp_epa_per_play ASC);

-- Empty-guard: stats.game_havoc is a gate table. Because the mart's grain is
-- driven by the plays-derived side, it keeps rows even when game_havoc is empty
-- (havoc_rate simply comes out NULL). Guarding on mart emptiness would there-
-- fore MISS a silently-broken havoc join, so instead we require the game_havoc
-- family to have populated at least one row. Fail loudly otherwise.
DO $$
DECLARE
    havoc_rows bigint;
BEGIN
    SELECT COUNT(*) INTO havoc_rows
    FROM marts.defensive_havoc
    WHERE havoc_rate IS NOT NULL;

    IF havoc_rows = 0 THEN
        RAISE EXCEPTION
            'marts.defensive_havoc: havoc_rate is NULL for every row -- '
            'stats.game_havoc contributed nothing. Verify the gate table is '
            'loaded, that the assumed dlt columns (defensive_havoc__total / '
            '__front_seven / __db) match the live schema, and that '
            'game_havoc.team matches core.plays.defense naming.';
    END IF;
END $$;
