-- marts.house_elo_game
-- Game-grain house Elo history (Tier 2 analytics,
-- docs/plans/2026-07-21-tier2-analytics-plan.md).
-- Grain: (game_id) -- one row per game
-- Source: analytics.house_elo_game, written by scripts/compute_house_elo.py
--
-- Thin passthrough of every column in analytics.house_elo_game (pregame /
-- postgame Elo both sides, win prob, expected vs actual margin, CFBD Elo
-- copies retained for validation), plus two derived columns comparing the
-- model's pregame expectation to what actually happened:
--   margin_error     = expected_home_margin - actual_home_margin
--   abs_margin_error = ABS(margin_error)
-- Positive margin_error means the model expected the home team to win by
-- more than it actually did (i.e. it overrated the home side for this game).

-- Prerequisite: the staging table this matview reads. DDL is intentionally
-- duplicated from migrations/025_tier2_analytics_staging.sql (which is
-- POPULATED by scripts/compute_house_elo.py, not this file) so this mart
-- stands alone in any provisioning order -- mirrors the 022<->marts/011
-- precedent. Keep the two definitions in sync.
CREATE TABLE IF NOT EXISTS analytics.house_elo_game (
    game_id BIGINT NOT NULL,
    season BIGINT NOT NULL,
    week BIGINT,
    season_type VARCHAR,
    start_date TIMESTAMPTZ,
    neutral_site BOOLEAN,
    home_team VARCHAR NOT NULL,
    away_team VARCHAR NOT NULL,

    home_pregame_elo NUMERIC(8, 2),
    away_pregame_elo NUMERIC(8, 2),
    home_postgame_elo NUMERIC(8, 2),
    away_postgame_elo NUMERIC(8, 2),
    home_win_prob NUMERIC(5, 4),
    expected_home_margin NUMERIC(6, 2),
    actual_home_margin BIGINT,
    mov_multiplier NUMERIC(6, 3),

    cfbd_home_pregame_elo NUMERIC(8, 2),
    cfbd_away_pregame_elo NUMERIC(8, 2)
);

CREATE UNIQUE INDEX IF NOT EXISTS house_elo_game_key
    ON analytics.house_elo_game (game_id);

DROP MATERIALIZED VIEW IF EXISTS marts.house_elo_game CASCADE;

CREATE MATERIALIZED VIEW marts.house_elo_game AS
SELECT
    game_id,
    season,
    week,
    season_type,
    start_date,
    neutral_site,
    home_team,
    away_team,

    home_pregame_elo,
    away_pregame_elo,
    home_postgame_elo,
    away_postgame_elo,
    home_win_prob,
    expected_home_margin,
    actual_home_margin,
    mov_multiplier,

    cfbd_home_pregame_elo,
    cfbd_away_pregame_elo,

    -- Derived: how far off the pregame expectation was from what happened.
    (expected_home_margin - actual_home_margin) AS margin_error,
    ABS(expected_home_margin - actual_home_margin) AS abs_margin_error
FROM analytics.house_elo_game;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.house_elo_game (game_id);

-- Query indexes
CREATE INDEX ON marts.house_elo_game (season);
CREATE INDEX ON marts.house_elo_game (home_team, season);
CREATE INDEX ON marts.house_elo_game (away_team, season);

-- Empty-guard: analytics.house_elo_game backs this mart via
-- scripts/compute_house_elo.py. If the historical build has not run yet (or
-- the staging table ever refreshes to zero rows), fail loudly at deploy time
-- instead of silently serving an empty mart downstream.
DO $$
BEGIN
    IF (SELECT count(*) FROM marts.house_elo_game) = 0 THEN
        RAISE EXCEPTION 'marts.house_elo_game is empty: analytics.house_elo_game has no rows. Run scripts/compute_house_elo.py --full to build the historical house Elo ratings, then refresh this mart before use.';
    END IF;
END $$;
