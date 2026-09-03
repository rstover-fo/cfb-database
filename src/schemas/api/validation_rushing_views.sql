-- Behavioral validation for the 2026-09-03 rushing-charting unit (U8):
-- api.rushing_charting_player_season, api.rushing_charting_team_season,
-- api.rushing_charting_direction_season (marts 050-052 / api 050-052), and
-- the get_player_detail RPC's additive rushing_charting column (U7). Applied
-- as its OWN file/transaction -- LAST in deploys/rushing_views-manifest.json,
-- after the marts, api views, and get_player_detail.sql -- so a failed
-- assertion reports without rolling back the DDL. Read-only; safe to re-run
-- any time as a health check (after the manifest apply, after a daily
-- refresh once this unit is wired into one, or ad hoc). Style follows
-- src/schemas/api/validation_expansion_views.sql.
--
-- Pre-backfill safe (KTD8): row counts and the direction-shape/grants/
-- denominator checks below all tolerate the tables existing with zero rows
-- (RAISE NOTICE, not EXCEPTION) -- this file can run immediately after the
-- manifest apply, before the 2025 backfill's data has landed, without
-- failing the deploy.
--
-- Six assertion groups:
--   (a) existence: the three api views (relkind 'v'), their three marts
--       counterparts (relkind 'm'), and public.get_player_detail(text,
--       integer) -- EXCEPTION if any is missing (a genuine deploy-order
--       failure, unlike (b)-(d) which tolerate empty data).
--   (b) row counts -- NOTICE only, even at zero (pre-backfill safe). Live
--       expectation printed for context from the Stage A 2025 backfill:
--       1,698 player-seasons, 152 team-seasons -> direction view
--       1,698*4 + 152*8 = 8,008 rows (4 offense-only rows per player-season,
--       4 offense + 4 defense per team-season).
--   (c) direction-view shape, SKIPPED when the view is empty: exactly four
--       distinct `direction` values {left, middle, right, unknown}; every
--       (season, entity_type, entity_id, team, side) group has exactly 4
--       rows, never fewer; every player row has side = 'offense' (rushing
--       charting has no way to attribute a carry to a defensive player).
--   (d) denominator presence: rushing_yards_available, direction_eligible_
--       attempts, direction_available_attempts on the player-season view;
--       the same three (offense_/defense_ prefixed) plus offense_/defense_
--       touchdown_status_available on the team-season view -- checked via
--       information_schema.columns so a column dropped by a mart rewrite
--       fails loudly rather than silently degrading a rate metric to
--       "no coverage denominator".
--   (e) variant-twin tripwire (KTD7): dlt sometimes loads a numeric metric
--       as a VARIANT, splitting it into a bigint base column plus a sibling
--       `<col>__v_double` column carrying the values dlt couldn't fit in
--       the base type -- and which twins appear is inconsistent per
--       direction block (see marts/052's header). Marts 050-052 COALESCE
--       exactly the twins that existed live on 2026-09-03; this allow-list
--       is that same set. A NEW `__v_double` column appearing on
--       stats.rushing_player_season or stats.rushing_team_season that is
--       NOT in the allow-list means a later load pushed a previously-clean
--       column into VARIANT territory -- the affected mart is now silently
--       reading NULLs from the (uncoalesced) base column while the real
--       values sit in the twin. EXCEPTION naming the column; the fix is to
--       add the COALESCE to the relevant mart and extend the allow-list
--       here (and in that mart's header) to match.
--   (f) grants tripwire: anon/authenticated kept SELECT on all three views.

DO $$
DECLARE
    n_player_season BIGINT;
    n_team_season BIGINT;
    n_direction_season BIGINT;
    direction_values TEXT[];
    n_bad_groups BIGINT;
    n_bad_player_side BIGINT;
    col_check TEXT;
    view_name TEXT;
    col_name TEXT;
    schema_part TEXT;
    table_part TEXT;
    allowed_player_twins TEXT[];
    allowed_team_twins TEXT[];
    twin_col TEXT;
    role_name TEXT;
BEGIN
    -- (a) existence: relkind-checked so a view accidentally recreated as a
    -- matview (or vice versa) fails loudly instead of just "exists".
    FOREACH view_name IN ARRAY ARRAY[
        'api.rushing_charting_player_season',
        'api.rushing_charting_team_season',
        'api.rushing_charting_direction_season'
    ] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname || '.' || c.relname = view_name
              AND c.relkind = 'v'
        ) THEN
            RAISE EXCEPTION '%: missing, or not a plain view (relkind v)', view_name;
        END IF;
    END LOOP;

    FOREACH view_name IN ARRAY ARRAY[
        'marts.rushing_charting_player_season',
        'marts.rushing_charting_team_season',
        'marts.rushing_charting_direction_season'
    ] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname || '.' || c.relname = view_name
              AND c.relkind = 'm'
        ) THEN
            RAISE EXCEPTION '%: missing, or not a materialized view (relkind m)', view_name;
        END IF;
    END LOOP;

    IF to_regprocedure('public.get_player_detail(text, integer)') IS NULL THEN
        RAISE EXCEPTION 'public.get_player_detail(text, integer): function missing';
    END IF;

    RAISE NOTICE 'existence check passed: 3 api views, 3 marts, get_player_detail(text, integer)';

    -- (b) row counts -- NOTICE only, pre-backfill safe
    SELECT COUNT(*) INTO n_player_season FROM api.rushing_charting_player_season;
    SELECT COUNT(*) INTO n_team_season FROM api.rushing_charting_team_season;
    SELECT COUNT(*) INTO n_direction_season FROM api.rushing_charting_direction_season;

    RAISE NOTICE 'rushing_charting_player_season: % rows (live expectation ~1,698 player-seasons from the 2025 Stage A backfill)', n_player_season;
    RAISE NOTICE 'rushing_charting_team_season: % rows (live expectation ~152 team-seasons from the 2025 Stage A backfill)', n_team_season;
    RAISE NOTICE 'rushing_charting_direction_season: % rows (live expectation ~8,008 = 1,698*4 + 152*8 from the 2025 Stage A backfill)', n_direction_season;

    IF n_player_season = 0 THEN
        RAISE NOTICE 'rushing_charting_player_season: 0 rows -- OK if the 2025 backfill has not run yet, not a failure';
    END IF;
    IF n_team_season = 0 THEN
        RAISE NOTICE 'rushing_charting_team_season: 0 rows -- OK if the 2025 backfill has not run yet, not a failure';
    END IF;
    IF n_direction_season = 0 THEN
        RAISE NOTICE 'rushing_charting_direction_season: 0 rows -- OK if the 2025 backfill has not run yet, not a failure';
    END IF;

    -- (c) direction-view shape, skipped entirely when empty (KTD8)
    IF n_direction_season = 0 THEN
        RAISE NOTICE 'rushing_charting_direction_season: skipping shape checks (0 rows, pre-backfill)';
    ELSE
        SELECT array_agg(DISTINCT direction ORDER BY direction) INTO direction_values
        FROM api.rushing_charting_direction_season;

        RAISE NOTICE 'rushing_charting_direction_season distinct directions: %', direction_values;

        IF direction_values IS DISTINCT FROM ARRAY['left', 'middle', 'right', 'unknown'] THEN
            RAISE EXCEPTION 'rushing_charting_direction_season: expected exactly {left, middle, right, unknown}, got %', direction_values;
        END IF;

        SELECT COUNT(*) INTO n_bad_groups
        FROM (
            SELECT season, entity_type, entity_id, team, side
            FROM api.rushing_charting_direction_season
            GROUP BY season, entity_type, entity_id, team, side
            HAVING COUNT(*) <> 4
        ) grp;

        IF n_bad_groups > 0 THEN
            RAISE EXCEPTION 'rushing_charting_direction_season: % (season, entity_type, entity_id, team, side) group(s) do not have exactly 4 direction rows', n_bad_groups;
        END IF;

        SELECT COUNT(*) INTO n_bad_player_side
        FROM api.rushing_charting_direction_season
        WHERE entity_type = 'player' AND side <> 'offense';

        IF n_bad_player_side > 0 THEN
            RAISE EXCEPTION 'rushing_charting_direction_season: % player row(s) with side <> ''offense'' -- rushing charting cannot attribute a carry to a defensive player', n_bad_player_side;
        END IF;

        RAISE NOTICE 'rushing_charting_direction_season shape checks passed: 4 directions, every (entity, side) group has exactly 4 rows, players are offense-only';
    END IF;

    -- (d) denominator presence via information_schema.columns
    FOREACH col_check IN ARRAY ARRAY[
        'api.rushing_charting_player_season|rushing_yards_available',
        'api.rushing_charting_player_season|direction_eligible_attempts',
        'api.rushing_charting_player_season|direction_available_attempts',
        'api.rushing_charting_team_season|offense_rushing_yards_available',
        'api.rushing_charting_team_season|offense_direction_eligible_attempts',
        'api.rushing_charting_team_season|offense_direction_available_attempts',
        'api.rushing_charting_team_season|offense_touchdown_status_available',
        'api.rushing_charting_team_season|defense_rushing_yards_available',
        'api.rushing_charting_team_season|defense_direction_eligible_attempts',
        'api.rushing_charting_team_season|defense_direction_available_attempts',
        'api.rushing_charting_team_season|defense_touchdown_status_available'
    ] LOOP
        view_name := split_part(col_check, '|', 1);
        col_name := split_part(col_check, '|', 2);
        schema_part := split_part(view_name, '.', 1);
        table_part := split_part(view_name, '.', 2);

        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = schema_part
              AND table_name = table_part
              AND column_name = col_name
        ) THEN
            RAISE EXCEPTION '%.%: denominator column % missing', schema_part, table_part, col_name;
        END IF;
    END LOOP;

    RAISE NOTICE 'denominator column presence check passed (rushing_yards_available, direction_eligible_attempts, direction_available_attempts, offense_/defense_ touchdown_status_available)';

    -- (e) variant-twin tripwire (KTD7) -- allow-list is the exact set of
    -- __v_double columns marts 050-052 COALESCE, live-verified 2026-09-03
    allowed_player_twins := ARRAY[
        'open_field_yards__v_double',
        'power_success__v_double',
        'directions__unknown__power_success__v_double',
        'directions__middle__yards_per_carry__v_double',
        'directions__middle__success_rate__v_double',
        'directions__middle__ppa__v_double',
        'directions__middle__total_ppa__v_double',
        'directions__middle__line_yards__v_double',
        'directions__middle__line_yards_total__v_double',
        'directions__middle__second_level_yards__v_double',
        'directions__middle__open_field_yards__v_double',
        'directions__middle__stuff_rate__v_double',
        'directions__middle__explosiveness__v_double',
        'directions__left__line_yards_total__v_double',
        'directions__right__power_success__v_double',
        'directions__middle__power_success__v_double',
        'directions__left__power_success__v_double'
    ];

    FOR twin_col IN
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'stats' AND table_name = 'rushing_player_season'
          AND column_name ~ '__v_double$'
    LOOP
        IF NOT (twin_col = ANY(allowed_player_twins)) THEN
            RAISE EXCEPTION 'stats.rushing_player_season: unexpected __v_double twin column % not in the KTD7 allow-list -- a mart may be silently reading NULLs where dlt routed doubles; add the COALESCE to the affected mart and extend this allow-list (and the mart header) to match', twin_col;
        END IF;
    END LOOP;

    allowed_team_twins := ARRAY[
        'defense__directions__right__line_yards_total__v_double',
        'defense__directions__middle__second_level_yards__v_double',
        'defense__directions__middle__power_success__v_double',
        'defense__directions__left__line_yards__v_double',
        'offense__line_yards_total__v_double',
        'offense__second_level_yards__v_double',
        'offense__open_field_yards__v_double',
        'defense__directions__right__power_success__v_double'
    ];

    FOR twin_col IN
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'stats' AND table_name = 'rushing_team_season'
          AND column_name ~ '__v_double$'
    LOOP
        IF NOT (twin_col = ANY(allowed_team_twins)) THEN
            RAISE EXCEPTION 'stats.rushing_team_season: unexpected __v_double twin column % not in the KTD7 allow-list -- a mart may be silently reading NULLs where dlt routed doubles; add the COALESCE to the affected mart and extend this allow-list (and the mart header) to match', twin_col;
        END IF;
    END LOOP;

    RAISE NOTICE 'variant-twin tripwire (KTD7) passed: % player-season twin(s), % team-season twin(s), all within the allow-list',
        array_length(allowed_player_twins, 1), array_length(allowed_team_twins, 1);

    -- (f) grants tripwire: anon/authenticated kept SELECT on all three views
    FOREACH view_name IN ARRAY ARRAY[
        'api.rushing_charting_player_season',
        'api.rushing_charting_team_season',
        'api.rushing_charting_direction_season'
    ] LOOP
        FOREACH role_name IN ARRAY ARRAY['anon', 'authenticated'] LOOP
            IF NOT has_table_privilege(role_name, view_name, 'SELECT') THEN
                RAISE EXCEPTION '%: role % lost SELECT', view_name, role_name;
            END IF;
        END LOOP;
    END LOOP;

    RAISE NOTICE 'grants tripwire passed: anon/authenticated hold SELECT on all 3 views';

    RAISE NOTICE 'rushing_views validation passed';
END $$;
