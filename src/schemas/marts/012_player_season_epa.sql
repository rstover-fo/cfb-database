-- Player EPA aggregated by season
-- Depends on: marts.player_game_epa
--
-- Rekeyed on athlete_id (Phase 3 of the Tier 1 analytics-unlock plan): the
-- grain is now (season, team, athlete_id, play_category) so two different
-- players who share a name no longer collapse into one season row.
-- play_category values are 'passing' / 'rushing' / 'receiving' (inherited from
-- marts.player_game_epa). player_name is carried through as MAX() -- it is
-- functionally determined by athlete_id, so aggregating it keeps the unique
-- (season, team, athlete_id, play_category) key intact even if a single
-- athlete's name spelling varies across games.

DROP MATERIALIZED VIEW IF EXISTS marts.player_season_epa CASCADE;

CREATE MATERIALIZED VIEW marts.player_season_epa AS
SELECT
    season,
    team,
    MAX(player_name) AS player_name,
    athlete_id,
    play_category,
    COUNT(DISTINCT game_id) AS games,
    SUM(plays) AS total_plays,
    SUM(total_epa)::NUMERIC(8, 2) AS total_epa,
    (SUM(total_epa) / NULLIF(SUM(plays), 0))::NUMERIC(6, 4) AS epa_per_play,
    (SUM(plays * success_rate) / NULLIF(SUM(plays), 0))::NUMERIC(5, 3) AS success_rate,
    SUM(explosive_plays) AS explosive_plays,
    SUM(total_yards) AS total_yards,
    -- Usage: plays per game
    (SUM(plays)::NUMERIC / COUNT(DISTINCT game_id))::NUMERIC(5, 1) AS plays_per_game,
    -- Rankings within season/category
    RANK() OVER (
        PARTITION BY season, play_category
        ORDER BY SUM(total_epa) DESC
    ) AS epa_rank
FROM marts.player_game_epa
GROUP BY season, team, athlete_id, play_category
HAVING SUM(plays) >= 20;  -- Minimum 20 plays on season

CREATE UNIQUE INDEX ON marts.player_season_epa (season, team, athlete_id, play_category);
CREATE INDEX ON marts.player_season_epa (season, play_category, epa_rank);
CREATE INDEX ON marts.player_season_epa (player_name);
CREATE INDEX ON marts.player_season_epa (total_epa DESC);
