-- Migration: 059_rushing_grants_indexes
--
-- Grants + indexes for the /rushing charting unit (spec v5.26.0,
-- 2026-09-02/03): rushing.py's five resources -- rushing_plays_resource,
-- rushing_player_games_resource, rushing_team_games_resource,
-- rushing_player_season_resource, rushing_team_season_resource -- writing
-- stats.rushing_plays, stats.rushing_player_games, stats.rushing_team_games,
-- stats.rushing_player_season, stats.rushing_team_season.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-058. Idempotent (GRANT is naturally re-appliable;
-- indexes use IF NOT EXISTS; comments overwrite on re-apply).
--
-- APPLY AFTER THE FIRST SUCCESSFUL LOAD of the rushing source (same
-- precondition as 026_win_probability_indexes.sql, 050_expansion_grants_indexes.sql,
-- 054_fanout_grants_indexes.sql, and 057_passing_grants_indexes.sql) -- dlt
-- creates each table on first write, so the plain GRANT/CREATE INDEX
-- statements below will fail with "relation does not exist" against a
-- database where these tables haven't been created yet. `rushing` is wired
-- into scripts/load_season.py's SOURCE_ORDER (run.py::run_rushing_pipeline),
-- so the Stage A backfill (2025, 2026) already created and populated all
-- five tables before this file was written.
--
-- `stats` already has schema USAGE granted to anon/authenticated
-- (grant_read_access_for_security_invoker.sql), so this migration only
-- needs table-level GRANTs, same as 054/057.
--
-- Column names below were read live from information_schema.columns on
-- 2026-09-03, after the Stage A backfill --
-- NOT a spec guess like 057's header warned about for passing. Row counts
-- at read time: 63,234 plays / 8,205 player-games / 1,758 team-games /
-- 1,698 player-seasons / 152 team-seasons. dlt snake_cases camelCase fields,
-- flattens nested dicts with "__" (e.g. `offense.attempts` ->
-- `offense__attempts`, `directions.left.carries` ->
-- `directions__left__carries` on player-grain rows and
-- `offense__directions__left__carries` on team-grain rows), and creates a
-- `<column>__v_double` twin for a bigint metric column when a later load
-- observed a fractional value for it -- present on many of the direction
-- and headline metric columns here (mart-authoring concern, tracked for
-- U6; this migration comments the primary column name only, per KTD7).
--
-- Primary keys (KTD3, matching the dlt merge disposition's keys in
-- rushing.py -- not enforced here as a Postgres constraint, dlt-managed):
--   stats.rushing_plays          (game_id, play_id)
--   stats.rushing_player_games   (game_id, player_id)
--   stats.rushing_team_games     (game_id, team)
--   stats.rushing_player_season  (season, player_id, team)
--   stats.rushing_team_season    (season, team)
--
-- The 446 charting-metric COMMENT ON COLUMN statements (14 headline metrics
-- x 2 player tables, 14 x 2 sides x 2 team tables, 15 direction metrics x 4
-- directions x 2 player tables, 15 x 4 x 2 sides x 2 team tables, plus 2 on
-- rushing_plays) run through a guarded DO block, same reasoning as 057's
-- guard: dlt omits a column from a table's schema entirely when every value
-- loaded for it so far was NULL, so a database whose first rushing load for
-- some future season happens to land on an all-uncharted week could
-- genuinely be missing one of these columns. Every column checked here DID
-- exist against the live database at authoring time, but
-- the guard removes the dependency on that staying true for a re-apply
-- against a differently-seeded database. run_migrations.py --file executes
-- a whole file as one implicit transaction, so an unguarded COMMENT naming
-- a missing column would error the whole file and roll back the grants/
-- indexes above too -- the opposite of "deliberately re-appliable".
--
-- The metric column lists and their comment text are built inside the DO
-- block from small arrays (direction names x metric-name/description
-- pairs) rather than hand-typed as 446 individual COMMENT statements --
-- see the block below for the loop shape.
--
-- Apply via:
--   python scripts/run_migrations.py --file src/schemas/migrations/059_rushing_grants_indexes.sql

-- ---------------------------------------------------------------------------
-- Grants -- root (always-created) tables
-- ---------------------------------------------------------------------------

GRANT SELECT ON
    stats.rushing_plays,
    stats.rushing_player_games,
    stats.rushing_team_games,
    stats.rushing_player_season,
    stats.rushing_team_season
    TO anon, authenticated;

REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON
    stats.rushing_plays,
    stats.rushing_player_games,
    stats.rushing_team_games,
    stats.rushing_player_season,
    stats.rushing_team_season
    FROM anon, authenticated;

-- No dlt child-table grant block here (unlike 057's defensive one): KTD4
-- confirms none of the five rushing tables nests an array -- `directions`
-- and `offense`/`defense` are flat dicts that flatten into "__" columns on
-- the parent, and `clock` on rushing_plays is likewise flat -- so dlt never
-- creates a rushing_* child table. The live column dump backing this
-- migration's header shows only the five parent tables (plus their
-- `_dlt_load_id`/`_dlt_id` bookkeeping columns), confirming this at the
-- table level too.

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

-- rushing_plays' PK is (game_id, play_id) -- game_id is already the leading
-- PK column, but an explicit index documents the lookup path the same way
-- 057's idx_passing_plays_game_id does for a PK-leading column.
CREATE INDEX IF NOT EXISTS idx_rushing_plays_game_id
    ON stats.rushing_plays (game_id);

-- rusher_id is the novel join surface this unit adds at play grain --
-- nullable (unattributed/team rushes carry no rusher_id) and never enters
-- the PK, but it is the per-player play lookup path.
CREATE INDEX IF NOT EXISTS idx_rushing_plays_rusher_id
    ON stats.rushing_plays (rusher_id);

-- (offense_id, season) is the team-scoped play lookup path (e.g. "all of
-- Michigan's 2025 rushes") -- offense_id is numeric (ref.teams(id)), unlike
-- the offense name string, so this composite avoids the 35-duplicate-
-- school-name join trap noted below.
CREATE INDEX IF NOT EXISTS idx_rushing_plays_offense_id_season
    ON stats.rushing_plays (offense_id, season);

CREATE INDEX IF NOT EXISTS idx_rushing_player_games_player_id
    ON stats.rushing_player_games (player_id);

CREATE INDEX IF NOT EXISTS idx_rushing_player_season_player_id_season
    ON stats.rushing_player_season (player_id, season);

CREATE INDEX IF NOT EXISTS idx_rushing_team_games_game_id
    ON stats.rushing_team_games (game_id);

-- No index on `team` on either team-grain table, same reasoning as 057:
-- ~136-value low-cardinality (roughly FBS+), a btree on it does little for
-- selectivity and isn't worth the write overhead.

-- ---------------------------------------------------------------------------
-- Table comments -- grain, PK, and the R10 non-reconciliation statement.
-- ---------------------------------------------------------------------------

COMMENT ON TABLE stats.rushing_plays IS
    'Play-grain rushing charting from CFBD /rushing/plays (spec v5.26.0). One row per rush attempt, with direction (left/middle/right/unknown), attribution status, and per-play PPA/success where charted. PK (game_id, play_id). parse_status marks charting completeness (complete/partial/invalid -- invalid is its own bucket, never counted as charted and never folded into partial or any coverage denominator). Data loaded from 2025 forward only (RUSHING_DATA_START).';

COMMENT ON TABLE stats.rushing_player_games IS
    'Player-game rushing charting from CFBD /rushing/players/games (spec v5.26.0). One row per (game, player). PK (game_id, player_id). Player totals here include ONLY individually attributed rushes (attribution_status = individual on stats.rushing_plays); sacks, kneels, team_rushes, and unattributed/multi_carrier attempts are counted separately as their own columns on this row and are NOT folded into attempts/total_rushing_yards. Player-grain totals will not sum to the corresponding stats.rushing_team_games row for the same game -- team and player totals are different populations by upstream design, not a reconciliation bug.';

COMMENT ON TABLE stats.rushing_player_season IS
    'Player-season rushing charting from CFBD /rushing/players/season (spec v5.26.0). One row per (season, player, team) -- team is part of the key so a mid-season transfer produces one row per team stint (same reasoning as stats.player_success_season). Player totals here include ONLY individually attributed rushes; sacks, kneels, team_rushes, and unattributed/multi_carrier attempts are counted separately as their own columns on this row and are NOT folded into attempts/total_rushing_yards. Player-grain totals will not sum to the corresponding stats.rushing_team_season row -- team and player totals are different populations by upstream design, not a reconciliation bug.';

COMMENT ON TABLE stats.rushing_team_games IS
    'Team-game rushing charting from CFBD /rushing/teams/games (spec v5.26.0). One row per (game, team), with offense__* (this team''s rushing offense in this game) and defense__* (this team''s rushing defense in this game -- i.e. what the opponent''s rushing offense did against them, NOT the opponent''s own offensive row) columns side by side. PK (game_id, team). offense__sacks and offense__kneels are tracked on this row the same as on the player-game tables but are not rushing attempts on either grain; offense__attempts will exceed the sum of this game''s individually-attributed player rushes by offense__team_rushes + offense__unattributed_attempts + offense__multi_carrier_attempts -- team and player totals are different populations by upstream design, not a reconciliation bug.';

COMMENT ON TABLE stats.rushing_team_season IS
    'Team-season rushing charting from CFBD /rushing/teams/season (spec v5.26.0). One row per (season, team), with offense__* (this team''s rushing offense this season) and defense__* (this team''s rushing defense this season -- i.e. what opposing rushing offenses did against them, NOT the opponent''s own offensive row) columns side by side. PK (season, team). offense__sacks and offense__kneels are tracked on this row the same as on the player-season tables but are not rushing attempts on either grain; offense__attempts will exceed the sum of this season''s individually-attributed player rushes by offense__team_rushes + offense__unattributed_attempts + offense__multi_carrier_attempts -- team and player totals are different populations by upstream design, not a reconciliation bug.';

-- ---------------------------------------------------------------------------
-- Column comments -- always-present columns (join spine, status/coverage
-- markers). Left as plain, unguarded statements: every column here carries
-- a value (or a well-defined 0) on every row CFBD returns, so each
-- materializes on a table's first load regardless of what has or hasn't
-- been charted yet -- unlike the metric family below.
-- ---------------------------------------------------------------------------

COMMENT ON COLUMN stats.rushing_plays.parse_status IS
    'CFBD charting-completeness marker for this play: complete, partial, or invalid. invalid is its own bucket -- CFBD could not reliably chart this rush at all, so it is never counted as charted and never folded into partial or any coverage denominator (rushing_yards_available, direction_eligible_attempts, direction_available_attempts). partial means charting is incomplete (some charted-metric columns may read NULL); complete means charting has finished for this play. 2025 is expected to carry a mix of all three as an active re-charting queue; 2026+ is expected mostly complete.';

COMMENT ON COLUMN stats.rushing_plays.attribution_status IS
    'CFBD''s rusher-attribution outcome for this play: individual (cleanly attributed to one rusher -- the only status counted in player-grain aggregate totals), team (a team-credited rush with no individual rusher), multi_carrier (more than one player involved, not split to one rusher), or unmatched/ambiguous/conflict/unlinked (attribution could not be resolved to a specific player). Player-grain aggregate tables (stats.rushing_player_games/season) count team, multi_carrier, and the unresolved statuses separately on the row and exclude them from attempts/total_rushing_yards -- see those tables'' comments for the R10 non-reconciliation statement.';

COMMENT ON COLUMN stats.rushing_plays.direction_analysis_eligible IS
    'Whether this play is eligible for direction charting at all (some plays -- e.g. kneels, certain broken plays -- are never direction-eligible regardless of charting progress). FALSE means rush_direction will never be populated for this play; TRUE means it can be, pending charting. Aggregate-row direction_eligible_attempts sums this flag.';

COMMENT ON COLUMN stats.rushing_plays.rush_direction IS
    'Charted rush direction: left, middle, right, or unknown (charted as direction-eligible but a specific side has not yet been assigned). NULL means this play has not been charted for direction yet -- see direction_analysis_eligible for whether it ever will be. Aggregate-row direction_available_attempts sums the non-NULL count.';

COMMENT ON COLUMN stats.rushing_player_games.rushing_yards_available IS
    'Count of this player''s attempts in this game whose rushing yardage has been charted -- the coverage denominator for total_rushing_yards, yards_per_carry, and the other yardage-based aggregates on this row. Never NULL; 0 means none of this player''s attempts in this game are charted for yardage yet, in which case the yardage aggregates read NULL.';
COMMENT ON COLUMN stats.rushing_player_games.direction_eligible_attempts IS
    'Count of this player''s attempts in this game that are eligible for direction charting (see stats.rushing_plays.direction_analysis_eligible) -- the eligibility denominator for the direction split on this row. Never NULL.';
COMMENT ON COLUMN stats.rushing_player_games.direction_available_attempts IS
    'Count of this player''s attempts in this game that have actually been charted with a direction -- the charting-coverage denominator for the directions__* aggregates on this row (direction_eligible_attempts is the eligibility denominator and is always >= this column). Never NULL; 0 means none of this player''s eligible attempts in this game are charted for direction yet.';

COMMENT ON COLUMN stats.rushing_player_season.rushing_yards_available IS
    'Count of this player''s attempts this season whose rushing yardage has been charted -- the coverage denominator for total_rushing_yards, yards_per_carry, and the other yardage-based aggregates on this row. Never NULL; 0 means none of this player''s attempts this season are charted for yardage yet, in which case the yardage aggregates read NULL.';
COMMENT ON COLUMN stats.rushing_player_season.direction_eligible_attempts IS
    'Count of this player''s attempts this season that are eligible for direction charting (see stats.rushing_plays.direction_analysis_eligible) -- the eligibility denominator for the direction split on this row. Never NULL.';
COMMENT ON COLUMN stats.rushing_player_season.direction_available_attempts IS
    'Count of this player''s attempts this season that have actually been charted with a direction -- the charting-coverage denominator for the directions__* aggregates on this row (direction_eligible_attempts is the eligibility denominator and is always >= this column). Never NULL; 0 means none of this player''s eligible attempts this season are charted for direction yet.';

COMMENT ON COLUMN stats.rushing_team_games.offense__rushing_yards_available IS
    'Count of this team''s rushing-offense attempts in this game whose yardage has been charted -- the coverage denominator for offense__total_rushing_yards, offense__yards_per_carry, and the other offense yardage-based aggregates on this row. Never NULL; 0 means none of the offense''s attempts in this game are charted for yardage yet.';
COMMENT ON COLUMN stats.rushing_team_games.offense__direction_eligible_attempts IS
    'Count of this team''s rushing-offense attempts in this game that are eligible for direction charting -- the eligibility denominator for the offense__directions__* split on this row. Never NULL.';
COMMENT ON COLUMN stats.rushing_team_games.offense__direction_available_attempts IS
    'Count of this team''s rushing-offense attempts in this game that have actually been charted with a direction -- the charting-coverage denominator for the offense__directions__* aggregates (offense__direction_eligible_attempts is the eligibility denominator and is always >= this column). Never NULL.';
COMMENT ON COLUMN stats.rushing_team_games.offense__touchdown_status_available IS
    'Count of this team''s rushing-offense attempts in this game whose touchdown status has been charted/confirmed -- the coverage denominator for offense__rushing_touchdowns. Never NULL.';
COMMENT ON COLUMN stats.rushing_team_games.defense__rushing_yards_available IS
    'Count of attempts against this team''s rushing defense in this game whose yardage has been charted -- the coverage denominator for defense__total_rushing_yards, defense__yards_per_carry, and the other defense yardage-based aggregates on this row. Never NULL; 0 means none of the attempts against this defense in this game are charted for yardage yet.';
COMMENT ON COLUMN stats.rushing_team_games.defense__direction_eligible_attempts IS
    'Count of attempts against this team''s rushing defense in this game that are eligible for direction charting -- the eligibility denominator for the defense__directions__* split on this row. Never NULL.';
COMMENT ON COLUMN stats.rushing_team_games.defense__direction_available_attempts IS
    'Count of attempts against this team''s rushing defense in this game that have actually been charted with a direction -- the charting-coverage denominator for the defense__directions__* aggregates (defense__direction_eligible_attempts is the eligibility denominator and is always >= this column). Never NULL.';
COMMENT ON COLUMN stats.rushing_team_games.defense__touchdown_status_available IS
    'Count of attempts against this team''s rushing defense in this game whose touchdown status has been charted/confirmed -- the coverage denominator for defense__rushing_touchdowns. Never NULL.';

COMMENT ON COLUMN stats.rushing_team_season.offense__rushing_yards_available IS
    'Count of this team''s rushing-offense attempts this season whose yardage has been charted -- the coverage denominator for offense__total_rushing_yards, offense__yards_per_carry, and the other offense yardage-based aggregates on this row. Never NULL; 0 means none of the offense''s attempts this season are charted for yardage yet.';
COMMENT ON COLUMN stats.rushing_team_season.offense__direction_eligible_attempts IS
    'Count of this team''s rushing-offense attempts this season that are eligible for direction charting -- the eligibility denominator for the offense__directions__* split on this row. Never NULL.';
COMMENT ON COLUMN stats.rushing_team_season.offense__direction_available_attempts IS
    'Count of this team''s rushing-offense attempts this season that have actually been charted with a direction -- the charting-coverage denominator for the offense__directions__* aggregates (offense__direction_eligible_attempts is the eligibility denominator and is always >= this column). Never NULL.';
COMMENT ON COLUMN stats.rushing_team_season.offense__touchdown_status_available IS
    'Count of this team''s rushing-offense attempts this season whose touchdown status has been charted/confirmed -- the coverage denominator for offense__rushing_touchdowns. Never NULL.';
COMMENT ON COLUMN stats.rushing_team_season.defense__rushing_yards_available IS
    'Count of attempts against this team''s rushing defense this season whose yardage has been charted -- the coverage denominator for defense__total_rushing_yards, defense__yards_per_carry, and the other defense yardage-based aggregates on this row. Never NULL; 0 means none of the attempts against this defense this season are charted for yardage yet.';
COMMENT ON COLUMN stats.rushing_team_season.defense__direction_eligible_attempts IS
    'Count of attempts against this team''s rushing defense this season that are eligible for direction charting -- the eligibility denominator for the defense__directions__* split on this row. Never NULL.';
COMMENT ON COLUMN stats.rushing_team_season.defense__direction_available_attempts IS
    'Count of attempts against this team''s rushing defense this season that have actually been charted with a direction -- the charting-coverage denominator for the defense__directions__* aggregates (defense__direction_eligible_attempts is the eligibility denominator and is always >= this column). Never NULL.';
COMMENT ON COLUMN stats.rushing_team_season.defense__touchdown_status_available IS
    'Count of attempts against this team''s rushing defense this season whose touchdown status has been charted/confirmed -- the coverage denominator for defense__rushing_touchdowns. Never NULL.';

-- ---------------------------------------------------------------------------
-- Column comments -- charting-metric family (GUARDED). Covers stats.
-- rushing_plays.{ppa,success} plus the two headline-metric blocks on each
-- aggregate table:
--   * base (non-direction) metrics: yards_per_carry, success_rate, ppa,
--     total_ppa, line_yards, line_yards_total, second_level_yards,
--     second_level_yards_total, open_field_yards, open_field_yards_total,
--     stuff_rate, power_success, explosiveness, total_rushing_yards --
--     unprefixed on the two player tables, offense__/defense__ prefixed on
--     the two team tables.
--   * per-direction metrics: the same 13 rate/average/sum metrics above
--     (minus total_rushing_yards) plus carries and yards, each repeated for
--     direction in (unknown, right, middle, left) as
--     directions__<direction>__<metric> (player tables) or
--     <side>__directions__<direction>__<metric> (team tables).
--
-- Convention, mirroring 057: NULL means the attempt(s) behind this value
-- were not charted for this metric; 0 (where the metric is a count/sum) is
-- a real observed value. The denominator named in each comment is never
-- NULL, so a NULL metric alongside a 0 denominator means "nothing charted
-- yet" and a NULL metric alongside a positive denominator would be a data
-- anomaly worth flagging, not an expected state.
--
-- The 'unknown' direction bucket holds attempts CFBD has charted as
-- direction-eligible but has not yet assigned a specific left/middle/right
-- side to -- distinct from attempts that have not been charted for
-- direction at all (those simply don't count toward direction_available_
-- attempts and contribute nothing to any of the four direction buckets).
-- ---------------------------------------------------------------------------

DO $comment$
DECLARE
    -- Non-direction headline metrics: (column suffix, description). Reused
    -- for both player tables (unprefixed) and team tables (offense__/
    -- defense__ prefixed), with the denominator column name and subject
    -- wording swapped per table/side below.
    base_metrics text[][] := ARRAY[
        ARRAY['yards_per_carry', 'Average rushing yards per charted carry'],
        ARRAY['success_rate', 'Fraction of charted attempts marked a rushing success'],
        ARRAY['ppa', 'Average predicted points added (PPA) per charted attempt'],
        ARRAY['total_ppa', 'Sum of predicted points added (PPA) across charted attempts'],
        ARRAY['line_yards', 'Average line yards (yards created at or behind the line of scrimmage, CFBD methodology) per charted attempt'],
        ARRAY['line_yards_total', 'Sum of line yards across charted attempts'],
        ARRAY['second_level_yards', 'Average second-level yards (5-10 yards past the line of scrimmage) per charted attempt'],
        ARRAY['second_level_yards_total', 'Sum of second-level yards across charted attempts'],
        ARRAY['open_field_yards', 'Average open-field yards (10+ yards past the line of scrimmage) per charted attempt'],
        ARRAY['open_field_yards_total', 'Sum of open-field yards across charted attempts'],
        ARRAY['stuff_rate', 'Fraction of charted attempts stopped at or behind the line of scrimmage'],
        ARRAY['power_success', 'Power-situation (short-yardage / goal-line) success count across charted attempts'],
        ARRAY['explosiveness', 'Average explosiveness (EPA-based) on successful charted attempts'],
        ARRAY['total_rushing_yards', 'Sum of charted rushing yards']
    ];

    -- Per-direction metrics: same 13 rate/average/sum metrics as above
    -- (minus total_rushing_yards, which has no per-direction counterpart)
    -- plus carries and yards, the two raw per-direction tallies.
    dir_metrics text[][] := ARRAY[
        ARRAY['carries', 'Count of charted carries'],
        ARRAY['yards', 'Sum of charted rushing yards'],
        ARRAY['yards_per_carry', 'Average rushing yards per charted carry'],
        ARRAY['success_rate', 'Fraction of charted attempts marked a rushing success'],
        ARRAY['ppa', 'Average predicted points added (PPA) per charted attempt'],
        ARRAY['total_ppa', 'Sum of predicted points added (PPA) across charted attempts'],
        ARRAY['line_yards', 'Average line yards (yards created at or behind the line of scrimmage, CFBD methodology) per charted attempt'],
        ARRAY['line_yards_total', 'Sum of line yards across charted attempts'],
        ARRAY['second_level_yards', 'Average second-level yards (5-10 yards past the line of scrimmage) per charted attempt'],
        ARRAY['second_level_yards_total', 'Sum of second-level yards across charted attempts'],
        ARRAY['open_field_yards', 'Average open-field yards (10+ yards past the line of scrimmage) per charted attempt'],
        ARRAY['open_field_yards_total', 'Sum of open-field yards across charted attempts'],
        ARRAY['stuff_rate', 'Fraction of charted attempts stopped at or behind the line of scrimmage'],
        ARRAY['power_success', 'Power-situation (short-yardage / goal-line) success count across charted attempts'],
        ARRAY['explosiveness', 'Average explosiveness (EPA-based) on successful charted attempts']
    ];

    directions text[] := ARRAY['unknown', 'right', 'middle', 'left'];
    player_tables text[] := ARRAY['rushing_player_games', 'rushing_player_season'];
    team_tables text[] := ARRAY['rushing_team_games', 'rushing_team_season'];
    sides text[] := ARRAY['offense', 'defense'];

    -- Accumulated (table, column, comment) queue, applied in one final loop
    -- (same exists-check/execute/notice pattern as 057's guarded block).
    q_table text[] := ARRAY[]::text[];
    q_col text[] := ARRAY[]::text[];
    q_comment text[] := ARRAY[]::text[];

    tbl text;
    side text;
    d text;
    i int;
    subject text;
    denom text;
    elig_col text;
    avail_col text;
    unknown_note text;
    n_commented integer := 0;
    n_skipped integer := 0;
BEGIN
    -- stats.rushing_plays: the two play-grain charted metrics.
    q_table := q_table || 'rushing_plays'; q_col := q_col || 'ppa';
    q_comment := q_comment || 'Predicted points added (PPA) for this charted rush. NULL means this play has not been charted for PPA yet.';
    q_table := q_table || 'rushing_plays'; q_col := q_col || 'success';
    q_comment := q_comment || 'Whether this charted rush was a success by CFBD''s success-rate definition. NULL means this play has not been charted for success yet.';

    -- Player tables (rushing_player_games, rushing_player_season): base
    -- metrics unprefixed, direction metrics as directions__<direction>__<metric>.
    FOREACH tbl IN ARRAY player_tables LOOP
        subject := CASE WHEN tbl = 'rushing_player_games'
            THEN 'this player in this game' ELSE 'this player this season' END;
        denom := 'rushing_yards_available';
        elig_col := 'direction_eligible_attempts';
        avail_col := 'direction_available_attempts';

        FOR i IN 1..array_length(base_metrics, 1) LOOP
            q_table := q_table || tbl;
            q_col := q_col || base_metrics[i][1];
            q_comment := q_comment || format(
                '%s for %s. NULL means none of the attempts have been charted for this metric yet; %s is the charting-coverage denominator.',
                base_metrics[i][2], subject, denom
            );
        END LOOP;

        FOREACH d IN ARRAY directions LOOP
            unknown_note := CASE WHEN d = 'unknown'
                THEN ' Rushes charted as direction-eligible but not yet assigned a left/middle/right side land in this ''unknown'' bucket -- distinct from rushes that have not been charted for direction at all (which contribute to neither this column nor ' || avail_col || ').'
                ELSE '' END;
            FOR i IN 1..array_length(dir_metrics, 1) LOOP
                q_table := q_table || tbl;
                q_col := q_col || format('directions__%s__%s', d, dir_metrics[i][1]);
                q_comment := q_comment || format(
                    '%s for %s (%s direction). NULL means none of the attempts in this direction have been charted for this metric yet; %s is the direction-charting coverage denominator (%s is the direction-eligibility denominator).%s',
                    dir_metrics[i][2], subject, d, avail_col, elig_col, unknown_note
                );
            END LOOP;
        END LOOP;
    END LOOP;

    -- Team tables (rushing_team_games, rushing_team_season): base metrics
    -- as <side>__<metric>, direction metrics as
    -- <side>__directions__<direction>__<metric>.
    FOREACH tbl IN ARRAY team_tables LOOP
        FOREACH side IN ARRAY sides LOOP
            subject := CASE
                WHEN tbl = 'rushing_team_games' AND side = 'offense' THEN 'this team''s rushing offense in this game'
                WHEN tbl = 'rushing_team_games' AND side = 'defense' THEN 'this team''s rushing defense in this game (what the opponent''s rushing offense did against them)'
                WHEN tbl = 'rushing_team_season' AND side = 'offense' THEN 'this team''s rushing offense this season'
                ELSE 'this team''s rushing defense this season (what opposing rushing offenses did against them)'
            END;
            denom := side || '__rushing_yards_available';
            elig_col := side || '__direction_eligible_attempts';
            avail_col := side || '__direction_available_attempts';

            FOR i IN 1..array_length(base_metrics, 1) LOOP
                q_table := q_table || tbl;
                q_col := q_col || format('%s__%s', side, base_metrics[i][1]);
                q_comment := q_comment || format(
                    '%s for %s. NULL means none of the attempts have been charted for this metric yet; %s is the charting-coverage denominator.',
                    base_metrics[i][2], subject, denom
                );
            END LOOP;

            FOREACH d IN ARRAY directions LOOP
                unknown_note := CASE WHEN d = 'unknown'
                    THEN ' Rushes charted as direction-eligible but not yet assigned a left/middle/right side land in this ''unknown'' bucket -- distinct from rushes that have not been charted for direction at all (which contribute to neither this column nor ' || avail_col || ').'
                    ELSE '' END;
                FOR i IN 1..array_length(dir_metrics, 1) LOOP
                    q_table := q_table || tbl;
                    q_col := q_col || format('%s__directions__%s__%s', side, d, dir_metrics[i][1]);
                    q_comment := q_comment || format(
                        '%s for %s (%s direction). NULL means none of the attempts in this direction have been charted for this metric yet; %s is the direction-charting coverage denominator (%s is the direction-eligibility denominator).%s',
                        dir_metrics[i][2], subject, d, avail_col, elig_col, unknown_note
                    );
                END LOOP;
            END LOOP;
        END LOOP;
    END LOOP;

    -- Apply the queue: guarded, exists-check-then-execute, same shape as
    -- 057's charting-aggregate block.
    FOR i IN 1..array_length(q_table, 1) LOOP
        IF EXISTS (
            SELECT 1 FROM information_schema.columns c
            WHERE c.table_schema = 'stats'
              AND c.table_name = q_table[i]
              AND c.column_name = q_col[i]
        ) THEN
            EXECUTE format(
                'COMMENT ON COLUMN stats.%I.%I IS %L',
                q_table[i], q_col[i], q_comment[i]
            );
            n_commented := n_commented + 1;
        ELSE
            n_skipped := n_skipped + 1;
        END IF;
    END LOOP;

    RAISE NOTICE '059_rushing_grants_indexes: % charting-metric column comment(s) applied, % skipped (column not present on this load)',
        n_commented, n_skipped;
END $comment$;
