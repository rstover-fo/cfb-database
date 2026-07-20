-- api.game_plays
-- Play-by-play for a game, one row per snap. Backed by core.plays, which is
-- partitioned by season (src/schemas/011_partition_plays.sql) -- core.plays.season
-- is the partition key and is populated directly by the ingestion pipeline
-- (src/pipelines/sources/plays.py sets play["season"] = year), so no join to
-- core.games is needed here either.
--
-- Play types are NOT filtered in this view -- cfb-app filters play types
-- client-side (kickoffs, timeouts, administrative plays, etc. are all included).
--
-- Callers should always filter by game_id: core.plays has no season predicate
-- by default, so an unfiltered query scans every season partition.
--
-- PostgREST usage:
--   GET /api/game_plays?game_id=eq.401628455&order=drive_number,play_number

CREATE OR REPLACE VIEW api.game_plays AS
SELECT
    p.game_id,
    p.season,
    p.drive_number,
    p.play_number,
    p.offense,
    p.defense,
    p.period,
    p.clock__minutes AS clock_minutes,
    p.clock__seconds AS clock_seconds,
    p.down,
    p.distance,
    p.yards_to_goal,
    p.yards_gained,
    p.play_type,
    p.play_text,
    p.ppa,
    p.scoring,
    p.offense_score,
    p.defense_score
FROM core.plays p;

GRANT SELECT ON api.game_plays TO anon, authenticated;

COMMENT ON VIEW api.game_plays IS 'Play-by-play for a game. One row per snap, unfiltered by play type. Columns: game_id, season, drive_number, play_number, offense, defense, period, clock_minutes, clock_seconds, down, distance, yards_to_goal, yards_gained, play_type, play_text, ppa, scoring, offense_score, defense_score.';
