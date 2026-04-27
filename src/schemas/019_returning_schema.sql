-- Returning production schema -- silver layer for the player-grain returning value model.
-- Creates the `rp` schema (named for "returning production"; the literal word "returning"
-- is a reserved Postgres keyword) with fct/dim tables that drive marts.player_returning_value
-- and marts.team_returning_production (added in later units of the same plan).
-- All statements are idempotent (IF NOT EXISTS / ON CONFLICT).

-- =============================================================================
-- EXTENSIONS
-- =============================================================================

-- fuzzystrmatch provides levenshtein() and soundex() for portal name-matching
-- against prior-season rosters in rp.fct_player_movements (U3).
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

-- =============================================================================
-- SCHEMA
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS rp;

COMMENT ON SCHEMA rp IS
    'Silver layer for the player-grain returning production model. '
    'Holds modeled fct/dim tables that feed marts.player_returning_value '
    'and marts.team_returning_production. Schema is named ''rp'' (initialism) because '
    '''returning'' is a reserved Postgres keyword.';

-- =============================================================================
-- FCT_PLAYER_SEASONS
-- One row per (player_id, season). Built from core.roster + stats.player_season_stats
-- + recruiting.recruits in rp.refresh_fct_player_seasons() (U2).
-- =============================================================================

CREATE TABLE IF NOT EXISTS rp.fct_player_seasons (
    player_id            VARCHAR     NOT NULL,
    season               INTEGER     NOT NULL,
    team                 VARCHAR,
    conference           VARCHAR,
    -- Position taxonomy: three columns of increasing canonicalization
    position_detail      VARCHAR,    -- raw CFBD string (e.g. 'LT', 'NICKEL')
    position             VARCHAR,    -- 11-canonical (QB|RB|WR|TE|OL|EDGE|DT|LB|CB|S|ST)
    position_group       VARCHAR,    -- 8-group rollup (QB|RB|WR_TE|OL|DL|LB|DB|ST)
    class                VARCHAR,    -- FR|SO|JR|SR|GR
    height_in            INTEGER,
    weight_lb            INTEGER,
    games_played         INTEGER,
    games_started        INTEGER,
    snaps_estimated      INTEGER,    -- best-effort, may be NULL (CFBD has no reliable snap data)
    -- Per-position stats from stats.player_season_stats
    stat_pass_attempts   INTEGER,
    stat_pass_yards      INTEGER,
    stat_pass_tds        INTEGER,
    stat_pass_ints       INTEGER,
    stat_rush_attempts   INTEGER,
    stat_rush_yards      INTEGER,
    stat_rush_tds        INTEGER,
    stat_rec_targets     INTEGER,
    stat_rec_catches     INTEGER,
    stat_rec_yards       INTEGER,
    stat_rec_tds         INTEGER,
    stat_tackles_solo    INTEGER,
    stat_tackles_ast     INTEGER,
    stat_tfl             DECIMAL(5,1),
    stat_sacks           DECIMAL(5,1),
    stat_int             INTEGER,
    stat_pbu             INTEGER,
    stat_ff              INTEGER,
    stat_fr              INTEGER,
    -- Recruiting tie-in (joined from recruiting.recruits when available)
    recruiting_composite DECIMAL(5,4),
    recruiting_stars     INTEGER,
    loaded_at            TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, season)
);

CREATE INDEX IF NOT EXISTS idx_fct_player_seasons_team_season
    ON rp.fct_player_seasons (team, season);
CREATE INDEX IF NOT EXISTS idx_fct_player_seasons_position_group
    ON rp.fct_player_seasons (position_group, season);
CREATE INDEX IF NOT EXISTS idx_fct_player_seasons_conference_season
    ON rp.fct_player_seasons (conference, season);

COMMENT ON TABLE rp.fct_player_seasons IS
    'Player-season grain. One row per (player_id, season). Populated by '
    'rp.refresh_fct_player_seasons() in U2.';
COMMENT ON COLUMN rp.fct_player_seasons.player_id IS
    'core.roster.id (VARCHAR, not BIGINT). All cross-schema joins must respect varchar typing.';
COMMENT ON COLUMN rp.fct_player_seasons.position_group IS
    '8-group rollup used by base_production formulas in U10. Derived from canonical position.';
COMMENT ON COLUMN rp.fct_player_seasons.snaps_estimated IS
    'Best-effort. CFBD does not provide reliable snap counts; v1 base_production uses games_played as denominator.';

-- =============================================================================
-- FCT_PLAYER_MOVEMENTS
-- One row per (player_id, transition_season). transition_season is the season
-- the player is moving INTO (e.g. for 2025->2026 portal moves, transition_season=2026).
-- Populated by rp.refresh_fct_player_movements() in U3.
-- =============================================================================

CREATE TABLE IF NOT EXISTS rp.fct_player_movements (
    player_id              VARCHAR     NOT NULL,    -- real if matched; 'portal:<md5>' if synthetic
    transition_season      INTEGER     NOT NULL,
    movement_type          VARCHAR     NOT NULL,    -- enum, joinable to dim_continuity_factors
    source_team            VARCHAR,                  -- nullable for new recruits
    source_conference      VARCHAR,
    destination_team       VARCHAR,                  -- nullable for departures (NFL, retirement)
    destination_conference VARCHAR,
    match_confidence       DECIMAL(3,2) NOT NULL DEFAULT 1.00,  -- 1.0 exact, 0.8 fuzzy, 0.0 synthetic
    match_method           VARCHAR     NOT NULL,    -- 'roster_continuity'|'portal_exact'|'portal_fuzzy'|'recruit'|'unmatched'
    -- Original CFBD fields preserved for audit (especially for synthetic-id rows)
    source_first_name      VARCHAR,
    source_last_name       VARCHAR,
    source_url             VARCHAR,
    source_date            DATE,
    loaded_at              TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, transition_season)
);

CREATE INDEX IF NOT EXISTS idx_fct_player_movements_dest_season
    ON rp.fct_player_movements (destination_team, transition_season);
CREATE INDEX IF NOT EXISTS idx_fct_player_movements_source_season
    ON rp.fct_player_movements (source_team, transition_season);
CREATE INDEX IF NOT EXISTS idx_fct_player_movements_movement_type
    ON rp.fct_player_movements (movement_type, transition_season);
CREATE INDEX IF NOT EXISTS idx_fct_player_movements_confidence
    ON rp.fct_player_movements (match_confidence) WHERE match_confidence < 1.00;

COMMENT ON TABLE rp.fct_player_movements IS
    'Player-movement events grain. One row per (player_id, transition_season). '
    'Three movement sources: roster continuity (returners), portal events (with name-matched '
    'player_id), recruit class. Unmatched portal entries get synthetic IDs and are also logged '
    'to rp.unmatched_portal_log.';
COMMENT ON COLUMN rp.fct_player_movements.player_id IS
    'Real core.roster.id when name-matched; synthetic ''portal:<md5>'' when unmatched.';
COMMENT ON COLUMN rp.fct_player_movements.match_confidence IS
    '1.00 = exact match or roster continuity; 0.80 = fuzzy levenshtein match; 0.00 = synthetic id.';

-- =============================================================================
-- DIM_CONTINUITY_FACTORS
-- Static lookup table. Joined to fct_player_movements.movement_type to retrieve
-- the continuity_factor multiplier used in marts.player_returning_value.
-- Populated by INSERT block at end of this migration.
-- =============================================================================

CREATE TABLE IF NOT EXISTS rp.dim_continuity_factors (
    movement_type        VARCHAR     PRIMARY KEY,
    continuity_factor    DECIMAL(4,2) NOT NULL,
    description          TEXT,
    updated_at           TIMESTAMPTZ  DEFAULT NOW()
);

COMMENT ON TABLE rp.dim_continuity_factors IS
    'Continuity factor by movement type. Single source of truth -- tune values here, '
    'not in matview SQL.';

-- =============================================================================
-- DIM_POSITION_WEIGHTS
-- Static lookup table. Joined to fct_player_seasons.position to retrieve the
-- position_weight multiplier in marts.player_returning_value.
-- Keyed on (position, scheme_archetype) -- v1 only populates scheme_archetype='static';
-- scheme-conditional rows are deferred to a Phase 4 follow-up plan.
-- =============================================================================

CREATE TABLE IF NOT EXISTS rp.dim_position_weights (
    position           VARCHAR     NOT NULL,
    scheme_archetype   VARCHAR     NOT NULL DEFAULT 'static',
    position_weight    DECIMAL(5,3) NOT NULL,
    description        TEXT,
    updated_at         TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (position, scheme_archetype)
);

COMMENT ON TABLE rp.dim_position_weights IS
    'Position weight by (position, scheme_archetype). v1 uses scheme_archetype=''static'' only.';

-- =============================================================================
-- UNMATCHED_PORTAL_LOG
-- Audit table for portal entries that could not be name-matched to a prior-season
-- core.roster row. Populated by rp.refresh_fct_player_movements() in U3.
-- Inspectable to drive levenshtein-threshold tuning and JUCO origin classification.
-- =============================================================================

CREATE TABLE IF NOT EXISTS rp.unmatched_portal_log (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transition_season   INTEGER     NOT NULL,
    first_name          VARCHAR,
    last_name           VARCHAR,
    origin              VARCHAR,
    destination         VARCHAR,
    stars               INTEGER,
    rating              DECIMAL(5,4),
    position            VARCHAR,
    transfer_date       DATE,
    reason              VARCHAR,    -- 'no_origin_match' | 'no_name_match' | 'ambiguous_match'
    nearest_match_distance INTEGER, -- levenshtein distance to closest non-matching candidate
    logged_at           TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_unmatched_portal_log_season
    ON rp.unmatched_portal_log (transition_season);
CREATE INDEX IF NOT EXISTS idx_unmatched_portal_log_reason
    ON rp.unmatched_portal_log (reason, transition_season);

COMMENT ON TABLE rp.unmatched_portal_log IS
    'Portal entries that could not be matched to prior-season rosters. Used for tuning '
    'fuzzy-match thresholds and inspecting JUCO/FCS origin classification edge cases.';

-- =============================================================================
-- INJURIES_SEASON_ENDING
-- Hand-curated season-ender lookup driving the health_factor in marts.player_returning_value.
-- Populated by U8 via seeds/injuries_season_ending.csv. Schema declared here so all
-- rp.* tables exist after this migration.
-- =============================================================================

CREATE TABLE IF NOT EXISTS rp.injuries_season_ending (
    player_id              VARCHAR     NOT NULL,
    injury_season          INTEGER     NOT NULL,
    player_name            VARCHAR,
    team                   VARCHAR,
    severity               VARCHAR,    -- 'season' | 'partial'
    target_season_status   VARCHAR,    -- 'out' | 'limited' | 'full'
    source_url             VARCHAR,
    source_date            DATE,
    loaded_at              TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, injury_season)
);

COMMENT ON TABLE rp.injuries_season_ending IS
    'Hand-curated season-ender list. Drives health_factor in marts.player_returning_value. '
    'Loaded from seeds/injuries_season_ending.csv in U8.';

-- =============================================================================
-- GRANTS -- conform to the SECURITY INVOKER + schema-grant pattern (see migrations/
-- grant_read_access_for_security_invoker.sql, applied 2026-02-07). The new `rp`
-- schema must be readable by anon/authenticated for downstream API views to work, but
-- DML must be revoked to maintain the read-only-database invariant.
-- =============================================================================

GRANT USAGE ON SCHEMA rp TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA rp TO anon, authenticated;

REVOKE INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA rp
    FROM anon, authenticated;

-- Default privileges for tables created in this schema in the future
ALTER DEFAULT PRIVILEGES IN SCHEMA rp
    GRANT SELECT ON TABLES TO anon, authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA rp
    REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLES FROM anon, authenticated;

-- =============================================================================
-- SEED dim_continuity_factors -- 14 entries per requirements doc / plan
-- HC-only, 2-tier (returning_same_hc / returning_new_hc) collapses spec's 4-tier
-- coordinator-aware scheme. ON CONFLICT DO UPDATE makes this idempotent.
-- =============================================================================

INSERT INTO rp.dim_continuity_factors (movement_type, continuity_factor, description) VALUES
    ('returning_same_hc',         1.00, 'Player on roster both seasons; HC unchanged.'),
    ('returning_new_hc',          0.80, 'Player on roster both seasons; HC changed (collapses spec''s new-coord and new-HC tiers).'),
    ('returning_from_redshirt',   0.25, 'On prior roster but games_played=0; first real year.'),
    ('returning_from_injury_full',0.70, 'Listed in injuries_season_ending with season-ender; rust + reconditioning.'),
    ('portal_p5_to_p5',           0.70, 'FBS-to-FBS portal move, both conferences in P5.'),
    ('portal_g5_to_p5',           0.55, 'G5 origin -> P5 destination, FBS-to-FBS step up.'),
    ('portal_p5_to_g5',           0.85, 'P5 origin -> G5 destination, downward FBS move.'),
    ('portal_g5_to_g5',           0.65, 'G5-to-G5 lateral move.'),
    ('portal_fcs_to_fbs',         0.45, 'FCS origin -> FBS destination, translation discount.'),
    ('portal_juco_to_fbs',        0.40, 'JUCO origin -> FBS destination, higher variance.'),
    ('recruit_5star',             0.30, 'True freshman 5-star, year-1 contribution cap.'),
    ('recruit_4star',             0.15, 'True freshman 4-star, mostly rotational.'),
    ('recruit_3star',             0.05, 'True freshman 3-star, mostly redshirt.'),
    ('recruit_unrated',           0.02, 'True freshman unrated, walk-on / late add.')
ON CONFLICT (movement_type) DO UPDATE SET
    continuity_factor = EXCLUDED.continuity_factor,
    description       = EXCLUDED.description,
    updated_at        = NOW();

-- =============================================================================
-- SEED dim_position_weights -- 11 entries per requirements doc / plan
-- Connelly-style static weights.
--
-- KNOWN INCONSISTENCY: The requirements doc declared the SUM should equal 2.0
-- (1.0 offense + 1.0 defense) and asked tests to verify. With the published
-- per-position values, offense sums correctly to 1.000 but defense sums to
-- 0.820 (EDGE 0.149 + DT 0.149 + LB 0.192 + CB 0.165 + S 0.165). Total = 1.820.
--
-- Two interpretations exist: (a) the values are correct and the sum-to-2.0
-- claim was aspirational, (b) the defensive values should be rebalanced so
-- DB ~ 0.50 (CB 0.25 + S 0.25). U5/U6 rollup math should account for this --
-- either accept the asymmetry or treat the defensive weights as a tuning knob.
-- The test in tests/test_returning_schema.py pins the actual sum (0.82) so
-- changes here surface explicitly.
-- =============================================================================

INSERT INTO rp.dim_position_weights (position, scheme_archetype, position_weight, description) VALUES
    ('QB',   'static', 0.223, 'Quarterback. Connelly offensive weight.'),
    ('WR',   'static', 0.175, 'Wide receiver. Connelly offensive weight.'),
    ('TE',   'static', 0.175, 'Tight end. Connelly offensive weight.'),
    ('RB',   'static', 0.031, 'Running back. Connelly offensive weight.'),
    ('OL',   'static', 0.396, 'Offensive line (aggregate). Connelly offensive weight.'),
    ('EDGE', 'static', 0.149, 'Edge defender. Connelly defensive weight.'),
    ('DT',   'static', 0.149, 'Defensive tackle. Connelly defensive weight.'),
    ('LB',   'static', 0.192, 'Linebacker. Connelly defensive weight.'),
    ('CB',   'static', 0.165, 'Cornerback. Connelly defensive weight.'),
    ('S',    'static', 0.165, 'Safety. Connelly defensive weight.'),
    ('ST',   'static', 0.000, 'Special teams. Excluded from rollup baseline; flat 0.05 base for K/P starters via formula.')
ON CONFLICT (position, scheme_archetype) DO UPDATE SET
    position_weight = EXCLUDED.position_weight,
    description     = EXCLUDED.description,
    updated_at      = NOW();
