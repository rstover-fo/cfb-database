-- api.live_scoreboard
-- Latest /scoreboard poll snapshot per live/recent game -- score, clock,
-- possession, market line, and both CFBD's and the house closed-form live
-- win probability. Backs the Saturday in-game dashboard (Tier 3 analytics,
-- docs/plans/2026-07-21-tier3-analytics-plan.md, Pillar D).
--
-- PLAIN VIEW, NOT MATERIALIZED -- deliberately. live.scoreboard_snapshots is
-- polled every 5 minutes by .github/workflows/live-scoreboard.yml
-- (scripts/poll_scoreboard.py); a materialized view only reflects data as of
-- its last REFRESH, so on that poll cadence a matview would always be lagging
-- the current game state by up to a refresh cycle -- exactly the opposite of
-- what a live scoreboard needs. A plain view re-executes its query (the
-- DISTINCT ON below) on every request instead, so it is always current as of
-- the latest poll tick with zero refresh-lag risk.
--
-- LATEST-PER-GAME: live.scoreboard_snapshots is append-only, one row per
-- game per poll tick with no unique constraint on (game_id, captured_at)
-- (see migration 028's header), so DISTINCT ON (game_id) ORDER BY game_id,
-- captured_at DESC picks the single most recent snapshot per game.
--
-- 24-HOUR WINDOW: restricted to snapshots captured within the last 24 hours
-- so finished games age themselves out of this view -- a game that ended
-- Saturday afternoon should not still surface here on a Sunday query. Full
-- poll-tick history remains queryable directly from
-- live.scoreboard_snapshots.
--
-- house_live_home_wp is the closed-form house live win probability (formula
-- documented in migration 028's header: f = clamp(seconds_remaining/3600,
-- eps, 1); projected = current_margin + pregame_expected_margin * f;
-- home_wp = Phi(projected / (sigma * sqrt(f)))); cfbd_home_wp is CFBD's own
-- live WP where available, carried alongside purely for comparison.
--
-- PostgREST usage:
--   GET /api/live_scoreboard?status=eq.in_progress
--   GET /api/live_scoreboard?game_id=eq.401628455

CREATE OR REPLACE VIEW api.live_scoreboard AS
SELECT DISTINCT ON (game_id)
    game_id,
    season,
    week,
    season_type,
    status,
    period,
    clock,
    seconds_remaining,
    home_team,
    away_team,
    home_points,
    away_points,
    possession,
    spread,
    over_under,
    cfbd_home_wp,
    house_live_home_wp,
    pregame_expected_margin,
    captured_at
FROM live.scoreboard_snapshots
WHERE captured_at > now() - INTERVAL '24 hours'
ORDER BY game_id, captured_at DESC;

GRANT SELECT ON api.live_scoreboard TO anon, authenticated;

COMMENT ON VIEW api.live_scoreboard IS 'Latest /scoreboard poll snapshot per game captured within the last 24 hours (stale finished games age out on their own). Columns: game_id, season, week, season_type, status, period, clock, seconds_remaining, home_team, away_team, home_points, away_points, possession, spread, over_under, cfbd_home_wp, house_live_home_wp, pregame_expected_margin, captured_at. Plain view, not materialized, so it is always current as of the latest 5-minute poll tick -- see file header. DISTINCT ON (game_id) ORDER BY captured_at DESC selects the latest snapshot; query live.scoreboard_snapshots directly for full poll-tick history. Backed by live.scoreboard_snapshots.';
