-- Minimal warehouse fixture for dlt nested-row linkage and api.game_line_scores.
--
-- IDs and the 2099 season are synthetic and reserved for disposable test databases.
-- The complete game has two overtime periods. The partial game distinguishes an
-- explicit NULL child value, a real zero, and absent child rows.

INSERT INTO core.games (
    id,
    season,
    week,
    season_type,
    start_date,
    completed,
    home_team,
    home_points,
    away_team,
    away_points,
    _dlt_load_id,
    _dlt_id
)
VALUES
    (
        999900000001,
        2099,
        1,
        'regular',
        '2099-09-05 18:00:00+00',
        true,
        'Fixture Home',
        32,
        'Fixture Away',
        27,
        'fixture-load-001',
        'fixture-game-complete'
    ),
    (
        999900000002,
        2099,
        2,
        'regular',
        '2099-09-12 18:00:00+00',
        false,
        'Fixture Partial Home',
        NULL,
        'Fixture Partial Away',
        NULL,
        'fixture-load-001',
        'fixture-game-partial'
    );

-- For a direct dlt list child, _dlt_root_id and _dlt_parent_id both point to
-- core.games._dlt_id. _dlt_list_idx is the zero-based source-array position.
INSERT INTO core.games__home_line_scores (
    value,
    _dlt_root_id,
    _dlt_parent_id,
    _dlt_list_idx,
    _dlt_id
)
VALUES
    (7,  'fixture-game-complete', 'fixture-game-complete', 0, 'fixture-home-complete-0'),
    (0,  'fixture-game-complete', 'fixture-game-complete', 1, 'fixture-home-complete-1'),
    (14, 'fixture-game-complete', 'fixture-game-complete', 2, 'fixture-home-complete-2'),
    (3,  'fixture-game-complete', 'fixture-game-complete', 3, 'fixture-home-complete-3'),
    (6,  'fixture-game-complete', 'fixture-game-complete', 4, 'fixture-home-complete-4'),
    (2,  'fixture-game-complete', 'fixture-game-complete', 5, 'fixture-home-complete-5'),
    (NULL, 'fixture-game-partial', 'fixture-game-partial', 0, 'fixture-home-partial-0'),
    (0,    'fixture-game-partial', 'fixture-game-partial', 1, 'fixture-home-partial-1');

INSERT INTO core.games__away_line_scores (
    value,
    _dlt_root_id,
    _dlt_parent_id,
    _dlt_list_idx,
    _dlt_id
)
VALUES
    (0,  'fixture-game-complete', 'fixture-game-complete', 0, 'fixture-away-complete-0'),
    (10, 'fixture-game-complete', 'fixture-game-complete', 1, 'fixture-away-complete-1'),
    (7,  'fixture-game-complete', 'fixture-game-complete', 2, 'fixture-away-complete-2'),
    (7,  'fixture-game-complete', 'fixture-game-complete', 3, 'fixture-away-complete-3'),
    (3,  'fixture-game-complete', 'fixture-game-complete', 4, 'fixture-away-complete-4'),
    (0,  'fixture-game-complete', 'fixture-game-complete', 5, 'fixture-away-complete-5');

-- Expected api.game_line_scores rows ordered by game_id:
-- 999900000001 | 2099 | 7 | 0 | 14 | 3 | 8 | 0 | 10 | 7 | 7 | 3
-- 999900000002 | 2099 | NULL | 0 | NULL | NULL | NULL |
--                                  NULL | NULL | NULL | NULL | NULL
