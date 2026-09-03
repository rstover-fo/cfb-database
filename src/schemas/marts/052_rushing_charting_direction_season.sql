-- marts.rushing_charting_direction_season
-- =============================================================================
-- Rushing charting (spec v5.26.0, 2026-09-03 rushing-charting unit, U6).
-- THE TALL DIRECTION-SPLIT MART: the product decision is "direction splits
-- as tall rows in one view, not wide columns on the headline views" -- one
-- grain serves team, RB, and defense
-- readers without pushing marts 050/051 past 60 columns.
--
-- Grain: (season, entity_type, entity_id, team, side, direction).
-- entity_type is 'player' or 'team'; side is 'offense' or 'defense'
-- (players are offense only -- rushing charting has no way to attribute a
-- carry against a specific defensive player). Exactly FOUR rows exist per
-- (entity, side) -- left, middle, right, unknown -- even when every metric
-- in a row is NULL, because the melt below is a fixed four-row
-- `VALUES (...)` list, not a conditional UNION. A consumer counting rows
-- for one (season, entity_id, team, side) always gets 4, never fewer:
--   - player-season row -> 4 rows, all side = 'offense'
--   - team-season row   -> 8 rows, 4 offense + 4 defense
--
-- `unknown` is READ from directions__unknown__* on the source tables, never
-- derived by subtraction -- CFBD charts `unknown` as its own bucket when a
-- carry's direction could not be determined, same status as left/middle/
-- right, not a residual.
--
-- Shares are the CONSUMER'S to compute, not this mart's: a direction's
-- share of a side's total carries is this row's `carries` /
-- `direction_available_attempts` (or `direction_eligible_attempts` for the
-- eligible-population version) on that same row -- both denominators ride
-- along on every row so no join back to marts 050/051 is required for that
-- one computation. This mart deliberately does not pre-compute a share
-- column: eligible vs available denominators serve different questions
-- (see below) and baking in one choice would silently answer the other
-- wrong.
--
-- Denominators (R8): direction_eligible_attempts is the count of carries
-- CFBD considered eligible for direction charting (e.g. excludes kneels/
-- sacks by construction upstream); direction_available_attempts is the
-- count actually charted with a direction (left/middle/right/unknown all
-- count as charted -- only a carry CFBD has not gotten to yet is excluded).
-- available <= eligible. Both ride on every row of this mart (constant
-- across a row's 4-direction block) so a consumer never has to join back to
-- marts 050/051 just to compute direction coverage.
--
-- R10 CONTRACT (non-reconciliation): a player entity's carries summed
-- across its 4 direction rows will NOT equal the corresponding team
-- entity's carries summed across its 4 offense direction rows for the same
-- (season, team) -- same upstream attribution gap as marts 050/051 (team-
-- only/multi-carrier/unattributed attempts never attach to a player row).
--
-- team_id: numeric CFBD/ESPN team id from the same never-guess
-- (season, offense -> offense_id) mapping mart 051 uses, built fresh here
-- via the same team_ids CTE (kept local rather than joined against mart 051
-- so this mart has no mart-to-mart dependency and can refresh independently
-- -- both marts read the same base table, stats.rushing_plays, so they can
-- never disagree). Applied to BOTH player and team rows: a player row's
-- team_id is that of the team the player carried for that season, from the
-- same mapping -- not a separate/looser join. entity_id is `text` on every
-- row: player rows carry the CFBD athlete id string as-is (never NULL --
-- player_id is NOT NULL on stats.rushing_player_season); team rows carry
-- COALESCE(team_id::text, team) so the unique index below never sees a NULL
-- entity_id even on the rare ambiguous-team_id case.
--
-- Column names LIVE-VERIFIED 2026-09-03 via information_schema.columns
-- against stats.rushing_player_season (108 cols) and stats.rushing_team_season
-- (185 cols), including a per-column dlt VARIANT (__v_double) twin check --
-- twins are inconsistent per direction block (e.g. player-season directions__
-- middle__* carries ten twins, directions__right__* carries only one), which
-- is exactly why every metric expression below is
-- COALESCE(<col>::double precision, <col>__v_double) when a twin exists,
-- else <col>::double precision (KTD7) -- generated mechanically from the
-- live column list rather than hand-typed. `carries` (the direction-block
-- analog of `attempts`) stays bigint, untouched, in every tuple.
--
-- Data starts 2025 (RUSHING_DATA_START in src/pipelines/sources/rushing.py).
--
-- DEPLOY-ORDER CAVEAT (same shape as marts 045/047/050/051): dlt omits a
-- column entirely from a table's schema when every value loaded for it so
-- far was NULL. This file must be applied AFTER the rushing source's first
-- successful load, not before -- the 2026-09-03 live check above confirms
-- every column this file reads already exists today.
DROP MATERIALIZED VIEW IF EXISTS marts.rushing_charting_direction_season CASCADE;

CREATE MATERIALIZED VIEW marts.rushing_charting_direction_season AS
WITH team_ids AS (
    SELECT
        rp.season,
        rp.offense AS team,
        CASE
            WHEN COUNT(DISTINCT rp.offense_id) = 1 THEN MIN(rp.offense_id)
        END AS team_id
    FROM stats.rushing_plays rp
    WHERE rp.offense_id IS NOT NULL
    GROUP BY rp.season, rp.offense
),
player_melt AS (
    SELECT
        rps.season,
        'player'::text AS entity_type,
        rps.player_id AS entity_id,
        rps.team,
        ti.team_id,
        'offense'::text AS side,
        d.direction,
        d.carries,
        d.yards,
        d.yards_per_carry,
        d.success_rate,
        d.ppa,
        d.total_ppa,
        d.line_yards,
        d.line_yards_total,
        d.second_level_yards,
        d.second_level_yards_total,
        d.open_field_yards,
        d.open_field_yards_total,
        d.stuff_rate,
        d.power_success,
        d.explosiveness,
        rps.direction_eligible_attempts,
        rps.direction_available_attempts
    FROM stats.rushing_player_season rps
    LEFT JOIN team_ids ti
        ON ti.season = rps.season
       AND ti.team = rps.team
    CROSS JOIN LATERAL (VALUES
        ('left',
            rps.directions__left__carries,
            rps.directions__left__yards::double precision,
            rps.directions__left__yards_per_carry::double precision,
            rps.directions__left__success_rate::double precision,
            rps.directions__left__ppa::double precision,
            rps.directions__left__total_ppa::double precision,
            rps.directions__left__line_yards::double precision,
            COALESCE(rps.directions__left__line_yards_total::double precision, rps.directions__left__line_yards_total__v_double),
            rps.directions__left__second_level_yards::double precision,
            rps.directions__left__second_level_yards_total::double precision,
            rps.directions__left__open_field_yards::double precision,
            rps.directions__left__open_field_yards_total::double precision,
            rps.directions__left__stuff_rate::double precision,
            COALESCE(rps.directions__left__power_success::double precision, rps.directions__left__power_success__v_double),
            rps.directions__left__explosiveness::double precision
        ),
        ('middle',
            rps.directions__middle__carries,
            rps.directions__middle__yards::double precision,
            COALESCE(rps.directions__middle__yards_per_carry::double precision, rps.directions__middle__yards_per_carry__v_double),
            COALESCE(rps.directions__middle__success_rate::double precision, rps.directions__middle__success_rate__v_double),
            COALESCE(rps.directions__middle__ppa::double precision, rps.directions__middle__ppa__v_double),
            COALESCE(rps.directions__middle__total_ppa::double precision, rps.directions__middle__total_ppa__v_double),
            COALESCE(rps.directions__middle__line_yards::double precision, rps.directions__middle__line_yards__v_double),
            COALESCE(rps.directions__middle__line_yards_total::double precision, rps.directions__middle__line_yards_total__v_double),
            COALESCE(rps.directions__middle__second_level_yards::double precision, rps.directions__middle__second_level_yards__v_double),
            rps.directions__middle__second_level_yards_total::double precision,
            COALESCE(rps.directions__middle__open_field_yards::double precision, rps.directions__middle__open_field_yards__v_double),
            rps.directions__middle__open_field_yards_total::double precision,
            COALESCE(rps.directions__middle__stuff_rate::double precision, rps.directions__middle__stuff_rate__v_double),
            COALESCE(rps.directions__middle__power_success::double precision, rps.directions__middle__power_success__v_double),
            COALESCE(rps.directions__middle__explosiveness::double precision, rps.directions__middle__explosiveness__v_double)
        ),
        ('right',
            rps.directions__right__carries,
            rps.directions__right__yards::double precision,
            rps.directions__right__yards_per_carry::double precision,
            rps.directions__right__success_rate::double precision,
            rps.directions__right__ppa::double precision,
            rps.directions__right__total_ppa::double precision,
            rps.directions__right__line_yards::double precision,
            rps.directions__right__line_yards_total::double precision,
            rps.directions__right__second_level_yards::double precision,
            rps.directions__right__second_level_yards_total::double precision,
            rps.directions__right__open_field_yards::double precision,
            rps.directions__right__open_field_yards_total::double precision,
            rps.directions__right__stuff_rate::double precision,
            COALESCE(rps.directions__right__power_success::double precision, rps.directions__right__power_success__v_double),
            rps.directions__right__explosiveness::double precision
        ),
        ('unknown',
            rps.directions__unknown__carries,
            rps.directions__unknown__yards::double precision,
            rps.directions__unknown__yards_per_carry::double precision,
            rps.directions__unknown__success_rate::double precision,
            rps.directions__unknown__ppa::double precision,
            rps.directions__unknown__total_ppa::double precision,
            rps.directions__unknown__line_yards::double precision,
            rps.directions__unknown__line_yards_total::double precision,
            rps.directions__unknown__second_level_yards::double precision,
            rps.directions__unknown__second_level_yards_total::double precision,
            rps.directions__unknown__open_field_yards::double precision,
            rps.directions__unknown__open_field_yards_total::double precision,
            rps.directions__unknown__stuff_rate::double precision,
            COALESCE(rps.directions__unknown__power_success::double precision, rps.directions__unknown__power_success__v_double),
            rps.directions__unknown__explosiveness::double precision
        )
    ) AS d(direction, carries, yards, yards_per_carry, success_rate, ppa, total_ppa,
           line_yards, line_yards_total, second_level_yards, second_level_yards_total,
           open_field_yards, open_field_yards_total, stuff_rate, power_success, explosiveness)
),
team_offense_melt AS (
    SELECT
        rts.season,
        'team'::text AS entity_type,
        COALESCE(ti.team_id::text, rts.team) AS entity_id,
        rts.team,
        ti.team_id,
        'offense'::text AS side,
        d.direction,
        d.carries,
        d.yards,
        d.yards_per_carry,
        d.success_rate,
        d.ppa,
        d.total_ppa,
        d.line_yards,
        d.line_yards_total,
        d.second_level_yards,
        d.second_level_yards_total,
        d.open_field_yards,
        d.open_field_yards_total,
        d.stuff_rate,
        d.power_success,
        d.explosiveness,
        rts.offense__direction_eligible_attempts AS direction_eligible_attempts,
        rts.offense__direction_available_attempts AS direction_available_attempts
    FROM stats.rushing_team_season rts
    LEFT JOIN team_ids ti
        ON ti.season = rts.season
       AND ti.team = rts.team
    CROSS JOIN LATERAL (VALUES
        ('left',
            rts.offense__directions__left__carries,
            rts.offense__directions__left__yards::double precision,
            rts.offense__directions__left__yards_per_carry::double precision,
            rts.offense__directions__left__success_rate::double precision,
            rts.offense__directions__left__ppa::double precision,
            rts.offense__directions__left__total_ppa::double precision,
            rts.offense__directions__left__line_yards::double precision,
            rts.offense__directions__left__line_yards_total::double precision,
            rts.offense__directions__left__second_level_yards::double precision,
            rts.offense__directions__left__second_level_yards_total::double precision,
            rts.offense__directions__left__open_field_yards::double precision,
            rts.offense__directions__left__open_field_yards_total::double precision,
            rts.offense__directions__left__stuff_rate::double precision,
            rts.offense__directions__left__power_success::double precision,
            rts.offense__directions__left__explosiveness::double precision
        ),
        ('middle',
            rts.offense__directions__middle__carries,
            rts.offense__directions__middle__yards::double precision,
            rts.offense__directions__middle__yards_per_carry::double precision,
            rts.offense__directions__middle__success_rate::double precision,
            rts.offense__directions__middle__ppa::double precision,
            rts.offense__directions__middle__total_ppa::double precision,
            rts.offense__directions__middle__line_yards::double precision,
            rts.offense__directions__middle__line_yards_total::double precision,
            rts.offense__directions__middle__second_level_yards::double precision,
            rts.offense__directions__middle__second_level_yards_total::double precision,
            rts.offense__directions__middle__open_field_yards::double precision,
            rts.offense__directions__middle__open_field_yards_total::double precision,
            rts.offense__directions__middle__stuff_rate::double precision,
            rts.offense__directions__middle__power_success::double precision,
            rts.offense__directions__middle__explosiveness::double precision
        ),
        ('right',
            rts.offense__directions__right__carries,
            rts.offense__directions__right__yards::double precision,
            rts.offense__directions__right__yards_per_carry::double precision,
            rts.offense__directions__right__success_rate::double precision,
            rts.offense__directions__right__ppa::double precision,
            rts.offense__directions__right__total_ppa::double precision,
            rts.offense__directions__right__line_yards::double precision,
            rts.offense__directions__right__line_yards_total::double precision,
            rts.offense__directions__right__second_level_yards::double precision,
            rts.offense__directions__right__second_level_yards_total::double precision,
            rts.offense__directions__right__open_field_yards::double precision,
            rts.offense__directions__right__open_field_yards_total::double precision,
            rts.offense__directions__right__stuff_rate::double precision,
            rts.offense__directions__right__power_success::double precision,
            rts.offense__directions__right__explosiveness::double precision
        ),
        ('unknown',
            rts.offense__directions__unknown__carries,
            rts.offense__directions__unknown__yards::double precision,
            rts.offense__directions__unknown__yards_per_carry::double precision,
            rts.offense__directions__unknown__success_rate::double precision,
            rts.offense__directions__unknown__ppa::double precision,
            rts.offense__directions__unknown__total_ppa::double precision,
            rts.offense__directions__unknown__line_yards::double precision,
            rts.offense__directions__unknown__line_yards_total::double precision,
            rts.offense__directions__unknown__second_level_yards::double precision,
            rts.offense__directions__unknown__second_level_yards_total::double precision,
            rts.offense__directions__unknown__open_field_yards::double precision,
            rts.offense__directions__unknown__open_field_yards_total::double precision,
            rts.offense__directions__unknown__stuff_rate::double precision,
            rts.offense__directions__unknown__power_success::double precision,
            rts.offense__directions__unknown__explosiveness::double precision
        )
    ) AS d(direction, carries, yards, yards_per_carry, success_rate, ppa, total_ppa,
           line_yards, line_yards_total, second_level_yards, second_level_yards_total,
           open_field_yards, open_field_yards_total, stuff_rate, power_success, explosiveness)
),
team_defense_melt AS (
    SELECT
        rts.season,
        'team'::text AS entity_type,
        COALESCE(ti.team_id::text, rts.team) AS entity_id,
        rts.team,
        ti.team_id,
        'defense'::text AS side,
        d.direction,
        d.carries,
        d.yards,
        d.yards_per_carry,
        d.success_rate,
        d.ppa,
        d.total_ppa,
        d.line_yards,
        d.line_yards_total,
        d.second_level_yards,
        d.second_level_yards_total,
        d.open_field_yards,
        d.open_field_yards_total,
        d.stuff_rate,
        d.power_success,
        d.explosiveness,
        rts.defense__direction_eligible_attempts AS direction_eligible_attempts,
        rts.defense__direction_available_attempts AS direction_available_attempts
    FROM stats.rushing_team_season rts
    LEFT JOIN team_ids ti
        ON ti.season = rts.season
       AND ti.team = rts.team
    CROSS JOIN LATERAL (VALUES
        ('left',
            rts.defense__directions__left__carries,
            rts.defense__directions__left__yards::double precision,
            rts.defense__directions__left__yards_per_carry::double precision,
            rts.defense__directions__left__success_rate::double precision,
            rts.defense__directions__left__ppa::double precision,
            rts.defense__directions__left__total_ppa::double precision,
            COALESCE(rts.defense__directions__left__line_yards::double precision, rts.defense__directions__left__line_yards__v_double),
            rts.defense__directions__left__line_yards_total::double precision,
            rts.defense__directions__left__second_level_yards::double precision,
            rts.defense__directions__left__second_level_yards_total::double precision,
            rts.defense__directions__left__open_field_yards::double precision,
            rts.defense__directions__left__open_field_yards_total::double precision,
            rts.defense__directions__left__stuff_rate::double precision,
            rts.defense__directions__left__power_success::double precision,
            rts.defense__directions__left__explosiveness::double precision
        ),
        ('middle',
            rts.defense__directions__middle__carries,
            rts.defense__directions__middle__yards::double precision,
            rts.defense__directions__middle__yards_per_carry::double precision,
            rts.defense__directions__middle__success_rate::double precision,
            rts.defense__directions__middle__ppa::double precision,
            rts.defense__directions__middle__total_ppa::double precision,
            rts.defense__directions__middle__line_yards::double precision,
            rts.defense__directions__middle__line_yards_total::double precision,
            COALESCE(rts.defense__directions__middle__second_level_yards::double precision, rts.defense__directions__middle__second_level_yards__v_double),
            rts.defense__directions__middle__second_level_yards_total::double precision,
            rts.defense__directions__middle__open_field_yards::double precision,
            rts.defense__directions__middle__open_field_yards_total::double precision,
            rts.defense__directions__middle__stuff_rate::double precision,
            COALESCE(rts.defense__directions__middle__power_success::double precision, rts.defense__directions__middle__power_success__v_double),
            rts.defense__directions__middle__explosiveness::double precision
        ),
        ('right',
            rts.defense__directions__right__carries,
            rts.defense__directions__right__yards::double precision,
            rts.defense__directions__right__yards_per_carry::double precision,
            rts.defense__directions__right__success_rate::double precision,
            rts.defense__directions__right__ppa::double precision,
            rts.defense__directions__right__total_ppa::double precision,
            rts.defense__directions__right__line_yards::double precision,
            COALESCE(rts.defense__directions__right__line_yards_total::double precision, rts.defense__directions__right__line_yards_total__v_double),
            rts.defense__directions__right__second_level_yards::double precision,
            rts.defense__directions__right__second_level_yards_total::double precision,
            rts.defense__directions__right__open_field_yards::double precision,
            rts.defense__directions__right__open_field_yards_total::double precision,
            rts.defense__directions__right__stuff_rate::double precision,
            COALESCE(rts.defense__directions__right__power_success::double precision, rts.defense__directions__right__power_success__v_double),
            rts.defense__directions__right__explosiveness::double precision
        ),
        ('unknown',
            rts.defense__directions__unknown__carries,
            rts.defense__directions__unknown__yards::double precision,
            rts.defense__directions__unknown__yards_per_carry::double precision,
            rts.defense__directions__unknown__success_rate::double precision,
            rts.defense__directions__unknown__ppa::double precision,
            rts.defense__directions__unknown__total_ppa::double precision,
            rts.defense__directions__unknown__line_yards::double precision,
            rts.defense__directions__unknown__line_yards_total::double precision,
            rts.defense__directions__unknown__second_level_yards::double precision,
            rts.defense__directions__unknown__second_level_yards_total::double precision,
            rts.defense__directions__unknown__open_field_yards::double precision,
            rts.defense__directions__unknown__open_field_yards_total::double precision,
            rts.defense__directions__unknown__stuff_rate::double precision,
            rts.defense__directions__unknown__power_success::double precision,
            rts.defense__directions__unknown__explosiveness::double precision
        )
    ) AS d(direction, carries, yards, yards_per_carry, success_rate, ppa, total_ppa,
           line_yards, line_yards_total, second_level_yards, second_level_yards_total,
           open_field_yards, open_field_yards_total, stuff_rate, power_success, explosiveness)
)
SELECT * FROM player_melt
UNION ALL
SELECT * FROM team_offense_melt
UNION ALL
SELECT * FROM team_defense_melt;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.rushing_charting_direction_season
    (season, entity_type, entity_id, team, side, direction);

-- Query indexes
CREATE INDEX ON marts.rushing_charting_direction_season (season, team, side);
CREATE INDEX ON marts.rushing_charting_direction_season (entity_id, entity_type);
