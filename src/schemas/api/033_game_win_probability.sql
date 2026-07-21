-- api.game_win_probability
-- In-game (per-play) win probability for a single game. Backed by
-- metrics.win_probability (P3.2 Lane B, docs/pipeline-manifest.md row 47),
-- loaded per-game via src/pipelines/sources/metrics.py::win_probability_resource
-- / src/pipelines/run.py::run_metrics_wp_pipeline -- NOT the year-driven
-- metrics_source every other metrics.* table comes from.
--
-- Distinct from api.game_elo_history / api.game_predictions (Tier 2, house
-- Elo-based win probability computed from core.games): this view is CFBD's
-- own in-play model, one row per snap, driven by real-time game state
-- (score, down/distance/field position, clock). Tier 2's is a single
-- pregame number per model per game. Use this view for a live/historical
-- win-probability chart across a game; use api.game_elo_history or
-- api.game_predictions for the house pregame number.
--
-- Column provenance:
--   game_id, season, play_id, home_win_probability, down, distance,
--   yard_line, play_text -- straight from metrics.win_probability, whose
--   shape mirrors CFBD's /metrics/wp response. down/distance/yard_line may
--   be NULL on some plays (e.g. kickoffs) per CFBD's own data, not a load bug.
--   home_team, away_team -- LEFT JOIN core.games on game_id (a table CFBD's
--   response may or may not also carry team names on; core.games is the
--   verified source, so it's used here instead of trusting an unconfirmed
--   WP payload field).
--   period, clock_minutes, clock_seconds -- LEFT JOIN core.plays on play_id,
--   DEFENSIVE: whether CFBD's /metrics/wp playId corresponds 1:1 with
--   core.plays.id (varchar) is UNCONFIRMED as of this view's authoring --
--   see src/schemas/migrations/026_win_probability_indexes.sql's header and
--   scripts/probe_metrics_wp.py (P3.2 W1). If the ids don't line up, this
--   LEFT JOIN simply yields NULL period/clock_minutes/clock_seconds for
--   every row rather than breaking the view or dropping win-probability
--   rows (it's a LEFT JOIN, not an inner join).
--
-- Ordering: no confirmed per-game play-sequence field exists in the
-- /metrics/wp payload (see 026's header) -- play_id ascending is the working
-- ordering assumption pending probe confirmation.
--
-- PostgREST usage:
--   GET /api/game_win_probability?game_id=eq.401628455&order=play_id

CREATE OR REPLACE VIEW api.game_win_probability AS
SELECT
    wp.game_id,
    wp.season,
    wp.play_id,
    g.home_team,
    g.away_team,
    wp.home_win_probability,
    wp.down,
    wp.distance,
    wp.yard_line,
    wp.play_text,
    p.period,
    p.clock__minutes AS clock_minutes,
    p.clock__seconds AS clock_seconds
FROM metrics.win_probability wp
LEFT JOIN core.games g ON g.id = wp.game_id
LEFT JOIN core.plays p ON p.id = wp.play_id::varchar
ORDER BY wp.game_id, wp.play_id;

GRANT SELECT ON api.game_win_probability TO anon, authenticated;

COMMENT ON VIEW api.game_win_probability IS 'In-game (per-play) win probability for a game, CFBD''s own in-play model (not the Tier 2 house pregame win probability in api.game_elo_history/api.game_predictions). Columns: game_id, season, play_id, home_team, away_team, home_win_probability, down, distance, yard_line, play_text, period, clock_minutes, clock_seconds. period/clock_minutes/clock_seconds come from a defensive LEFT JOIN to core.plays on play_id -- NULL if that id correspondence does not hold (unconfirmed as of deploy; see scripts/probe_metrics_wp.py). Coverage starts 2014 (metrics year range) but is only as complete as the per-game backfill in deploys/p32-backfill-manifests.md. Backed by metrics.win_probability.';
