-- marts.team_penalty_box
-- =============================================================================
-- Structured per-game penalty counts (2026-07-23 penalty analytics layer).
-- Grain: (game_id, team) -- one row per team per game, with the opponent's
-- numbers denormalized onto the same row.
--
-- Source: CFBD's official box score via core.game_team_stats child EAV rows
-- (category = 'totalPenaltiesYards', value "7-55" = count-yards; format
-- verified by probe, deploy run 30043634618). Unlike marts.penalty_log this
-- is NOT parsed from free text -- it is the scorer's official tally, so use
-- it for counts/yards and the log for infraction-level detail. Season-level
-- cross-check against stats.team_season_stats (penalties / penaltyYards /
-- *Opponent) lives in src/schemas/api/validation_penalties.sql.

DROP MATERIALIZED VIEW IF EXISTS marts.team_penalty_box CASCADE;

CREATE MATERIALIZED VIEW marts.team_penalty_box AS
WITH box AS (
    -- Pivot idiom per public/007_player_stats_function.sql; join chain per
    -- api/011_game_box_score.sql.
    SELECT
        gts.id AS game_id,
        t.team,
        t.home_away,
        MAX(CASE WHEN s.category = 'totalPenaltiesYards' THEN s.stat END) AS pen_raw
    FROM core.game_team_stats gts
    JOIN core.game_team_stats__teams t ON t._dlt_parent_id = gts._dlt_id
    JOIN core.game_team_stats__teams__stats s ON s._dlt_parent_id = t._dlt_id
    GROUP BY gts.id, t.team, t.home_away
),
parsed AS (
    SELECT
        game_id,
        team,
        home_away,
        NULLIF(split_part(pen_raw, '-', 1), '')::int AS penalties,
        NULLIF(split_part(pen_raw, '-', 2), '')::int AS penalty_yards
    FROM box
    WHERE pen_raw IS NOT NULL
)
SELECT
    p.game_id,
    g.season,
    g.week,
    g.season_type,
    p.team,
    opp.team AS opponent,
    p.home_away,
    p.penalties,
    p.penalty_yards,
    opp.penalties AS opponent_penalties,
    opp.penalty_yards AS opponent_penalty_yards
FROM parsed p
JOIN core.games g ON g.id = p.game_id
LEFT JOIN parsed opp ON opp.game_id = p.game_id AND opp.team <> p.team;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.team_penalty_box (game_id, team);

-- Query indexes
CREATE INDEX ON marts.team_penalty_box (season, team);

-- Empty-guard (house convention): core.game_team_stats is backfilled 2004+.
DO $$
BEGIN
    IF (SELECT count(*) FROM marts.team_penalty_box) = 0 THEN
        RAISE EXCEPTION 'marts.team_penalty_box is empty: no totalPenaltiesYards rows found in core.game_team_stats__teams__stats. Investigate before serving downstream.';
    END IF;
END $$;
