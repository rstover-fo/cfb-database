-- Migration: 052_sportsdataverse_xwalk_ratings
--
-- Target tables for four sportsdataverse-data flat-file sources (B2 items 1-2):
-- ESPN/Fox/Yahoo team + game id crosswalks, and two weekly external-ratings
-- snapshots (ESPN FPI; an adjusted-EPA/FEI system) sourced from
-- github.com/sportsdataverse/sportsdataverse-data. Parsers:
-- src/pipelines/sources/flatfile_parsers/sportsdataverse.py. Registry entries:
-- src/pipelines/sources/flat_files.py (sdv_team_xwalk, sdv_game_xwalk,
-- sdv_fpi_weekly, sdv_ratings_weekly).
--
-- Deliberately absent: a player/roster crosswalk table. The task anticipated
-- one (a `cfb_rosters_crosswalk`-shaped asset); it does not exist in the real
-- `cfb_crosswalk` GitHub release, which contains only the two team/schedule
-- datasets below. See the parser module's docstring for the full finding.
--
-- ID-namespace note (carried on the two xwalk tables' comments below): neither
-- crosswalk carries a CFBD id column. Both are pure ESPN/Fox/Yahoo crosswalks;
-- the join to CFBD-sourced ref.teams/core.games relies on CFBD's numeric ids
-- being ESPN's ids -- verified against the live warehouse 2026-08-29: ref.teams.id 333=Alabama/2483=Oregon match ESPN ids, core.games 2024 ids are ESPN event ids incl. 401632103, and ESPN-style athlete id 5083552 exists in core.roster.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-041+. Idempotent (IF NOT EXISTS throughout).
--
-- Note on dlt coexistence (mirrors 041): these are merge targets for dlt
-- pipelines; dlt adds its _dlt_id/_dlt_load_id bookkeeping columns on first
-- load. Types below match dlt's inference for the parsed values
-- (bigint/double precision/text/date/timestamptz), verified against the real
-- 2025-season parquet files' pyarrow schemas (not API docs).
--
-- Corrections applied by the B6a schema-architect review, before this
-- migration was ever applied to a live database:
-- 1. ref.team_id_xwalk's PK is (season, xwalk_key), not (season, norm_key) --
--    norm_key is NOT unique (the "roosevelt lakers" collision, two distinct
--    schools sharing a norm_key with different espn_team_id values) and
--    would silently drop one school under dlt merge. xwalk_key is a
--    parser-derived tiebreak (norm_key + first non-null of
--    espn/fox/yahoo_team_id); norm_key is kept as a plain non-unique indexed
--    column. See parse_team_xwalk's docstring for the full reasoning.
-- 2. ratings.fpi_weekly -> ratings.espn_fpi_weekly (distinguishes this
--    weekly-snapshot ESPN FPI table from CFBD's own season-grain
--    ratings.fpi_ratings).
-- 3. ratings.external_weekly -> ratings.sdv_ratings_weekly (matches the
--    repo's source-prefix table-naming convention: massey_composite,
--    nflverse_draft_picks).
-- 4. idx_team_id_xwalk_espn / idx_game_id_xwalk_espn made partial
--    (WHERE ... IS NOT NULL) -- espn_team_id/espn_game_id are 9%/45% null.

-- ---------------------------------------------------------------------------
-- ref.team_id_xwalk -- ESPN/Fox/Yahoo team identity crosswalk (weekly refresh;
-- sportsdataverse's cfb_teams_crosswalk_{season}.parquet)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ref.team_id_xwalk (
    season bigint NOT NULL,
    norm_key text NOT NULL,
    xwalk_key text NOT NULL,
    espn_team_id bigint,
    espn_team text,
    espn_abbreviation text,
    fox_team_id text,
    fox_team text,
    fox_abbreviation text,
    yahoo_team_id text,
    yahoo_team text,
    yahoo_abbreviation text,
    matched_sources text,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, xwalk_key)
);

COMMENT ON TABLE ref.team_id_xwalk IS
    'ESPN/Fox/Yahoo team identity crosswalk from sportsdataverse-data -- '
    'distinct from ref.team_name_xwalk (migration 041: source-spelling -> '
    'CFBD-full-name crosswalk used by the massey/sbr ingestion path). This '
    'table is NOT consulted by build_flat_file_source''s uses_xwalk resolver '
    '-- the sdv_* sources join to ref.teams/core.games directly on the '
    'verified-equal numeric ESPN/CFBD id (see the sportsdataverse.py module '
    'docstring), bypassing name resolution entirely.';
COMMENT ON COLUMN ref.team_id_xwalk.espn_team_id IS
    'ESPN numeric team id. ~9% null in the 2025 file (fox/yahoo-only matches '
    'with no ESPN counterpart) -- never assume non-null. By CFBD/ESPN''s '
    'documented shared id namespace this should equal ref.teams.id, but that '
    'was verified against the live warehouse on 2026-08-29 (333=Alabama, '
    '2483=Oregon).';
COMMENT ON COLUMN ref.team_id_xwalk.norm_key IS
    'Normalized "school mascot" key. NOT unique alone (kept as a plain '
    'indexed lookup column, not the PK): the 2025 file has one known '
    'collision ("roosevelt lakers", two distinct small colleges with '
    'different espn_team_id values) -- an accepted sportsdataverse '
    'data-quality edge, same class of issue as ref.teams'' 35 duplicate '
    'school names. xwalk_key (the actual PK component) resolves the '
    'collision -- see its comment.';
COMMENT ON COLUMN ref.team_id_xwalk.xwalk_key IS
    'Primary key component alongside season: norm_key + ''#'' + the first '
    'non-null of espn_team_id/fox_team_id/yahoo_team_id, computed by '
    'parse_team_xwalk. Exists because norm_key alone collides (see its '
    'comment) -- distinct source ids on the colliding rows (599 vs 127991 '
    'for "roosevelt lakers") make xwalk_key distinct too. A residual '
    'collision is possible only if two colliding norm_key rows also share '
    'the same tiebreak id (all three source ids null on both) -- not '
    'observed in the 2025 file.';

CREATE INDEX IF NOT EXISTS idx_team_id_xwalk_norm_key ON ref.team_id_xwalk (season, norm_key);
CREATE INDEX IF NOT EXISTS idx_team_id_xwalk_espn ON ref.team_id_xwalk (espn_team_id)
    WHERE espn_team_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- ref.game_id_xwalk -- ESPN/Fox/Yahoo game identity crosswalk (weekly refresh;
-- sportsdataverse's cfb_schedule_crosswalk_{season}.parquet)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ref.game_id_xwalk (
    season bigint NOT NULL,
    matchup_key text NOT NULL,
    yahoo_date date NOT NULL,
    espn_game_id bigint,
    fox_game_id text,
    yahoo_game_id text,
    yahoo_global_game_id text,
    home_team text,
    away_team text,
    espn_date date,
    fox_date date,
    matched_sources text,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, matchup_key, yahoo_date)
);

COMMENT ON COLUMN ref.game_id_xwalk.espn_game_id IS
    'ESPN numeric game id. ~45% null in the 2025 file (yahoo-only matches) -- '
    'never assume non-null. By CFBD/ESPN''s shared id namespace this equals '
    'core.games.id -- verified against the live warehouse on 2026-08-29 '
    '(2024 core.games ids are ESPN event ids, incl. 401632103).';
COMMENT ON COLUMN ref.game_id_xwalk.matchup_key IS
    'Not unique alone -- rematches (e.g. a regular-season game repeated as a '
    'conference title game) share the same "away norm|home norm" key. '
    'yahoo_date (always present, unlike espn_date/fox_date) disambiguates.';
COMMENT ON COLUMN ref.game_id_xwalk.home_team IS
    'ESPN-style full mascot name (e.g. "Ohio State Buckeyes"), not CFBD''s '
    'bare-school spelling ("Ohio State") -- do not join this column directly '
    'against ref.teams.school or core.games team names.';

CREATE INDEX IF NOT EXISTS idx_game_id_xwalk_espn ON ref.game_id_xwalk (espn_game_id)
    WHERE espn_game_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_game_id_xwalk_yahoo ON ref.game_id_xwalk (yahoo_game_id);

-- ---------------------------------------------------------------------------
-- ratings.espn_fpi_weekly -- ESPN FPI, weekly-snapshot granularity (distinct from
-- the existing season-level ratings.fpi_ratings sourced directly from CFBD's
-- /ratings/fpi; this table is the in-season week-by-week history CFBD's
-- endpoint does not expose). sportsdataverse's cfb_fpi_weekly_{season}.parquet.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ratings.espn_fpi_weekly (
    season bigint NOT NULL,
    season_type bigint NOT NULL,
    week bigint NOT NULL,
    team_id bigint NOT NULL,
    last_updated timestamptz,
    run_date_time_key bigint,
    snapshot_out_of_sequence boolean,
    fpi double precision,
    fpirank double precision,
    projectedw double precision,
    projectedl double precision,
    projectedt double precision,
    projectedwpctrank double precision,
    probwinout double precision,
    probwinconf double precision,
    sosremainingrank double precision,
    accomplishment double precision,
    accomplishmentrank double precision,
    adjwins double precision,
    adjlosses double precision,
    adjwinpctrank double precision,
    gamecontrol double precision,
    gamecontrolrank double precision,
    adjavgingamewp double precision,
    adjavgingamewprank double precision,
    avgingamewp double precision,
    avgingamewprank double precision,
    avgsosrank double precision,
    topsosrank double precision,
    epaoffense double precision,
    epadefense double precision,
    epaspecialteams double precision,
    probwindiv double precision,
    probmakeplayoffs double precision,
    probmaketitlegame double precision,
    numwins double precision,
    numlosses double precision,
    numties double precision,
    probwintitle double precision,
    rankchange7days double precision,
    prob6wins double precision,
    rank double precision,
    offefficiency double precision,
    offefficiencyrank double precision,
    defefficiency double precision,
    defefficiencyrank double precision,
    stefficiency double precision,
    stefficiencyrank double precision,
    totefficiency double precision,
    totefficiencyrank double precision,
    snapshot_is_contemporaneous boolean,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, season_type, week, team_id)
);

COMMENT ON COLUMN ratings.espn_fpi_weekly.season_type IS
    'CFBD-style postseason-week-restart convention applies: 2 = regular '
    'season (week 1-16 observed in 2025), 3 = postseason (week resets to 1) '
    '-- included in the PK to avoid regular/postseason week collisions.';
COMMENT ON COLUMN ratings.espn_fpi_weekly.team_id IS
    'ESPN numeric team id (0 nulls observed). By CFBD/ESPN''s shared id '
    'namespace this equals ref.teams.id -- verified against the live '
    'warehouse on 2026-08-29 (333=Alabama).';
COMMENT ON COLUMN ratings.espn_fpi_weekly.projectedt IS
    'Always NULL in every 2025 row observed (an all-null Arrow column '
    'upstream) -- typed double precision on the assumption it would hold a '
    'number like projectedw/projectedl if ESPN ever populates it.';

CREATE INDEX IF NOT EXISTS idx_espn_fpi_weekly_season_week ON ratings.espn_fpi_weekly (season, week);

-- ---------------------------------------------------------------------------
-- ratings.sdv_ratings_weekly -- external adjusted-EPA/FEI weekly ratings (a
-- second, independent benchmark for the house adjusted-EPA model in
-- scripts/compute_adjusted_epa_week.py). sportsdataverse's
-- cfb_ratings_weekly_{season}.parquet.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ratings.sdv_ratings_weekly (
    season bigint NOT NULL,
    through_week bigint NOT NULL,
    team_id bigint NOT NULL,
    adj_off_epa double precision,
    adj_def_epa double precision,
    adj_st_epa double precision,
    adj_net double precision,
    fei_off double precision,
    fei_def double precision,
    fei_net double precision,
    games bigint,
    off_pace double precision,
    off_rank bigint,
    def_rank bigint,
    net_rank bigint,
    net_z double precision,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, through_week, team_id)
);

COMMENT ON COLUMN ratings.sdv_ratings_weekly.team_id IS
    'Source ships this as a string ("333"); the parser casts it to bigint so '
    'it lines up with ratings.espn_fpi_weekly.team_id and (by the same '
    'unverified-but-documented equivalence) ref.teams.id.';
COMMENT ON TABLE ratings.sdv_ratings_weekly IS
    'Single external system (adjusted EPA + FEI), wide format -- no '
    'system/label column. A second external ratings system would need either '
    'a new table or a reshape to long format, not a bolt-on column here.';

CREATE INDEX IF NOT EXISTS idx_sdv_ratings_weekly_team ON ratings.sdv_ratings_weekly (team_id, season);

-- ---------------------------------------------------------------------------
-- Grants (mirror 041: read-only exposure via existing ref/ratings schema USAGE)
-- ---------------------------------------------------------------------------

GRANT SELECT ON ref.team_id_xwalk, ref.game_id_xwalk TO anon, authenticated;
GRANT SELECT ON ratings.espn_fpi_weekly, ratings.sdv_ratings_weekly TO anon, authenticated;
