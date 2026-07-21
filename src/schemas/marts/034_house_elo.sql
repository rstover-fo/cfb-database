-- marts.house_elo
-- Season-end house Elo rating per team per season (Tier 2 analytics,
-- docs/plans/2026-07-21-tier2-analytics-plan.md).
-- Grain: (team, season) -- one row per team per season
-- Source: analytics.house_elo_game, written by scripts/compute_house_elo.py
--
-- For each team, takes that team's LAST game of the season (home or away,
-- ordered by start_date then game_id) and uses that side's POSTGAME Elo as
-- the season-end rating. games_played counts every game (home + away) the
-- team appears in that season within analytics.house_elo_game. cfbd_elo is
-- CFBD's own Elo (ratings.elo_ratings, coverage ~2015+) joined in purely for
-- side-by-side validation -- it is not used in any computation here.

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

DROP MATERIALIZED VIEW IF EXISTS marts.house_elo CASCADE;

CREATE MATERIALIZED VIEW marts.house_elo AS
WITH team_games AS (
    -- Home-side appearances
    SELECT
        home_team AS team,
        season,
        home_postgame_elo AS postgame_elo,
        start_date,
        game_id
    FROM analytics.house_elo_game

    UNION ALL

    -- Away-side appearances
    SELECT
        away_team AS team,
        season,
        away_postgame_elo AS postgame_elo,
        start_date,
        game_id
    FROM analytics.house_elo_game
),
season_end AS (
    -- Each team's most recent game of the season -> that side's postgame Elo
    -- becomes the season-end rating.
    SELECT DISTINCT ON (team, season)
        team,
        season,
        postgame_elo AS season_end_elo
    FROM team_games
    ORDER BY team, season, start_date DESC NULLS LAST, game_id DESC
),
games_count AS (
    SELECT
        team,
        season,
        COUNT(*) AS games_played
    FROM team_games
    GROUP BY team, season
)
SELECT
    se.team,
    se.season,
    se.season_end_elo,
    RANK() OVER (PARTITION BY se.season ORDER BY se.season_end_elo DESC) AS elo_rank,
    gc.games_played,
    (gc.games_played < 4 OR se.season < 1900) AS low_confidence,
    er.elo AS cfbd_elo
FROM season_end se
JOIN games_count gc
    ON gc.team = se.team AND gc.season = se.season
LEFT JOIN ratings.elo_ratings er
    ON er.year = se.season AND er.team = se.team;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.house_elo (team, season);

-- Query indexes
CREATE INDEX ON marts.house_elo (season, elo_rank);
CREATE INDEX ON marts.house_elo (season);

-- Empty-guard: analytics.house_elo_game backs this mart via
-- scripts/compute_house_elo.py. If the historical build has not run yet (or
-- the staging table ever refreshes to zero rows), fail loudly at deploy time
-- instead of silently serving an empty mart downstream.
DO $$
BEGIN
    IF (SELECT count(*) FROM marts.house_elo) = 0 THEN
        RAISE EXCEPTION 'marts.house_elo is empty: analytics.house_elo_game has no rows. Run scripts/compute_house_elo.py --full to build the historical house Elo ratings, then refresh this mart before use.';
    END IF;
END $$;
