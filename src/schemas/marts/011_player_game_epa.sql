-- Player EPA attribution per game
-- Extracts player names from play_text for rushing and passing plays
-- Minimum 3 plays per player/game/category to be included

DROP MATERIALIZED VIEW IF EXISTS marts.player_game_epa CASCADE;

CREATE MATERIALIZED VIEW marts.player_game_epa AS
WITH rushing_plays AS (
    SELECT
        game_id, season, offense AS team,
        -- Extract rusher from play_text (pattern: "Name rush for X yards")
        TRIM(SPLIT_PART(play_text, ' rush ', 1)) AS player_name,
        'rushing' AS play_category,
        epa, success, explosive, yards_gained
    FROM marts.play_epa
    WHERE play_category = 'rush'
      AND play_text LIKE '% rush %'
      AND NOT is_garbage_time
),
passing_plays AS (
    SELECT
        game_id, season, offense AS team,
        -- Extract passer from play_text (pattern: "Name pass ...")
        TRIM(SPLIT_PART(play_text, ' pass ', 1)) AS player_name,
        'passing' AS play_category,
        epa, success, explosive, yards_gained
    FROM marts.play_epa
    WHERE play_category = 'pass'
      AND play_text LIKE '% pass %'
      AND NOT is_garbage_time
),
all_attributed AS (
    SELECT * FROM rushing_plays
    UNION ALL
    SELECT * FROM passing_plays
)
SELECT
    game_id,
    season,
    team,
    player_name,
    play_category,
    COUNT(*) AS plays,
    SUM(epa)::NUMERIC(8,2) AS total_epa,
    AVG(epa)::NUMERIC(6,4) AS epa_per_play,
    AVG(success)::NUMERIC(5,3) AS success_rate,
    SUM(explosive) AS explosive_plays,
    SUM(yards_gained) AS total_yards
FROM all_attributed
WHERE player_name IS NOT NULL
  AND player_name != ''
  AND LENGTH(player_name) > 2  -- Filter out artifacts
GROUP BY game_id, season, team, player_name, play_category
HAVING COUNT(*) >= 3;  -- Minimum 3 plays to be included

CREATE UNIQUE INDEX ON marts.player_game_epa (game_id, team, player_name, play_category);
CREATE INDEX ON marts.player_game_epa (player_name, season);
CREATE INDEX ON marts.player_game_epa (team, season);
CREATE INDEX ON marts.player_game_epa (total_epa DESC);
