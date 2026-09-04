-- Migration: 059_pff_tables
--
-- PFF Premium Stats manual-CSV ingest lane (design:
-- docs/brainstorms/2026-09-01-pff-plus-api.md, section 4 option A1). Five
-- season-grain report-family tables plus the committed team-name map and an
-- (initially empty) player crosswalk shell:
--
--   pff.passing_summary    (44 CSV cols, ~550 rows/season)
--   pff.receiving_summary  (47 CSV cols, ~2,300 rows/season)
--   pff.rushing_summary    (47 CSV cols, ~1,700 rows/season)
--   pff.offense_blocking   (31 CSV cols, ~5,800 rows/season)
--   pff.defense_summary    (55 CSV cols, ~5,400 rows/season)
--   pff.team_map           (136 rows, seeded below -- canonical copy of
--                           docs/brainstorms/2026-09-01-pff-team-name-map.json)
--   pff.player_xwalk       (empty shell; filled by
--                           scripts/build_pff_player_xwalk.py)
--
-- Sources are hand-exported CSVs from PFF's auth-gated Premium Stats site
-- (no fetch URL exists) loaded via scripts/load_flat_files.py --source
-- pff_<family> --season YYYY --file <csv>. Parsers:
-- src/pipelines/sources/flatfile_parsers/pff.py. Registry entries:
-- src/pipelines/sources/flat_files.py (pff_* specs).
--
-- Column contract: CSV column names verbatim (already snake_case), verified
-- identical across real 2023/2024/2025 exports of all five families. Types
-- are derived from the observed values across those 15 files, not from any
-- vendor doc: player_id/franchise_id bigint; columns whose observed values
-- are all integral -> integer; any observed decimal (incl. scientific
-- notation -- elusive_rating prints "1.0e3" for values >= 1000) -> numeric;
-- names -> text. Two columns are injected at load time, not present in the
-- CSVs: season (from the operator's --season, verified against the file's
-- FBS-membership fingerprint by the parser) and school (team_name resolved
-- through pff.team_map; unmapped names fail the load).
--
-- SCHEMA POSTURE: `pff` is created WITHOUT any anon/authenticated grant --
-- no GRANT USAGE on the schema, no GRANT SELECT on any table (mirrors
-- migration 053's ncaa posture). Reason: LICENSED DATA. PFF Premium Stats
-- rows are a paid subscription's exports; until the redistribution/ToS
-- question (brainstorm section 3.3) is settled, nothing PFF-derived may be
-- reachable through PostgREST or enter the api/public contract surface
-- (docs/SCHEMA_CONTRACT.md deliberately does not mention this schema).
-- Service-role/pipeline access only.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-041+/052/053/055. Idempotent (IF NOT EXISTS
-- throughout; the team_map seed upserts).
--
-- Note on dlt coexistence (mirrors 041/052/053): the five family tables are
-- merge targets for the flat-file dlt pipeline; dlt adds its
-- _dlt_id/_dlt_load_id bookkeeping columns on first load. The parser casts
-- every value (int/float/text, "" -> NULL) before load, so column types
-- below are the POST-parse types.

CREATE SCHEMA IF NOT EXISTS pff;

COMMENT ON SCHEMA pff IS
    'PFF Premium Stats (hand-exported CSVs; licensed data). DELIBERATELY '
    'UNGRANTED: no USAGE for anon/authenticated, no per-table SELECT -- '
    'service-role/pipeline access only, like the ncaa and scouting schemas, '
    'until the PFF redistribution/ToS question is settled '
    '(docs/brainstorms/2026-09-01-pff-plus-api.md section 3.3). Grain: one '
    'row per player per season per report family, keyed (player_id, season) '
    'on PFF''s own stable player id. player_id/franchise_id are PFF''s id '
    'namespace -- they share NOTHING with CFBD/ESPN ids; join to CFBD '
    'athletes only through pff.player_xwalk, and to CFBD schools only '
    'through the school column (pre-resolved via pff.team_map).';

-- ---------------------------------------------------------------------------
-- pff.passing_summary (manual load per season; season fingerprint enforced)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pff.passing_summary (
    season integer NOT NULL,
    school text NOT NULL,
    player text NOT NULL,
    player_id bigint NOT NULL,
    position text,
    team_name text NOT NULL,
    player_game_count integer,
    accuracy_percent numeric,
    aimed_passes integer,
    attempts integer,
    avg_depth_of_target numeric,
    avg_time_to_throw numeric,
    bats integer,
    big_time_throws integer,
    btt_rate numeric,
    completion_percent numeric,
    completions integer,
    declined_penalties integer,
    def_gen_pressures integer,
    drop_rate numeric,
    dropbacks integer,
    drops integer,
    epa numeric,
    first_downs integer,
    franchise_id bigint,
    grades_hands_fumble numeric,
    grades_offense numeric,
    grades_pass numeric,
    grades_run numeric,
    hit_as_threw integer,
    interceptions integer,
    passing_snaps integer,
    penalties integer,
    positive_epa_percent numeric,
    pressure_to_sack_rate numeric,
    qb_rating numeric,
    sack_percent numeric,
    sacks integer,
    scrambles integer,
    spikes integer,
    thrown_aways integer,
    touchdowns integer,
    turnover_worthy_plays integer,
    twp_rate numeric,
    yards integer,
    ypa numeric,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, season)
);

CREATE INDEX IF NOT EXISTS idx_pff_passing_summary_season ON pff.passing_summary (season);
CREATE INDEX IF NOT EXISTS idx_pff_passing_summary_school ON pff.passing_summary (season, school);

COMMENT ON TABLE pff.passing_summary IS
    'One row per player-season. Population mixes positions: anyone with a dropback (~550 rows/season: 400+ QBs plus WR/HB/P/K trick-play rows).';
COMMENT ON COLUMN pff.passing_summary.season IS
    'Not in the CSV -- injected at load time from the operator''s --season, verified by the parser''s FBS-membership fingerprint guard.';
COMMENT ON COLUMN pff.passing_summary.school IS
    'CFBD school full name, resolved from team_name via pff.team_map at load time; the join key toward core/ref tables.';
COMMENT ON COLUMN pff.passing_summary.team_name IS
    'PFF''s own ALL-CAPS team abbreviation, verbatim from the export (e.g. BOWL GREEN, NWESTERN).';

-- ---------------------------------------------------------------------------
-- pff.receiving_summary (manual load per season; season fingerprint enforced)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pff.receiving_summary (
    season integer NOT NULL,
    school text NOT NULL,
    player text NOT NULL,
    player_id bigint NOT NULL,
    position text,
    team_name text NOT NULL,
    player_game_count integer,
    avg_depth_of_target numeric,
    avoided_tackles integer,
    caught_percent numeric,
    contested_catch_rate numeric,
    contested_receptions integer,
    contested_targets integer,
    declined_penalties integer,
    drop_rate numeric,
    drops integer,
    epa numeric,
    first_downs integer,
    franchise_id bigint,
    fumbles integer,
    grades_hands_drop numeric,
    grades_hands_fumble numeric,
    grades_offense numeric,
    grades_pass_block numeric,
    grades_pass_route numeric,
    inline_rate numeric,
    inline_snaps integer,
    interceptions integer,
    longest integer,
    pass_block_rate numeric,
    pass_blocks integer,
    pass_plays integer,
    penalties integer,
    positive_epa_percent numeric,
    receptions integer,
    route_rate numeric,
    routes integer,
    slot_rate numeric,
    slot_snaps integer,
    targeted_qb_rating numeric,
    targets integer,
    touchdowns integer,
    wide_rate numeric,
    wide_snaps integer,
    yards integer,
    yards_after_catch integer,
    yards_after_catch_per_reception numeric,
    yards_per_reception numeric,
    yprr numeric,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, season)
);

CREATE INDEX IF NOT EXISTS idx_pff_receiving_summary_season ON pff.receiving_summary (season);
CREATE INDEX IF NOT EXISTS idx_pff_receiving_summary_school ON pff.receiving_summary (season, school);

COMMENT ON TABLE pff.receiving_summary IS
    'One row per player-season. Population mixes positions: anyone who ran a route (~2,300 rows/season; WR/HB/TE dominate, plus QBs and linemen).';
COMMENT ON COLUMN pff.receiving_summary.season IS
    'Not in the CSV -- injected at load time from the operator''s --season, verified by the parser''s FBS-membership fingerprint guard.';
COMMENT ON COLUMN pff.receiving_summary.school IS
    'CFBD school full name, resolved from team_name via pff.team_map at load time; the join key toward core/ref tables.';
COMMENT ON COLUMN pff.receiving_summary.team_name IS
    'PFF''s own ALL-CAPS team abbreviation, verbatim from the export (e.g. BOWL GREEN, NWESTERN).';

-- ---------------------------------------------------------------------------
-- pff.rushing_summary (manual load per season; season fingerprint enforced)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pff.rushing_summary (
    season integer NOT NULL,
    school text NOT NULL,
    player text NOT NULL,
    player_id bigint NOT NULL,
    position text,
    team_name text NOT NULL,
    player_game_count integer,
    attempts integer,
    avoided_tackles integer,
    breakaway_attempts integer,
    breakaway_percent numeric,
    breakaway_yards integer,
    declined_penalties integer,
    designed_yards integer,
    drops integer,
    elu_recv_mtf integer,
    elu_rush_mtf integer,
    elu_yco integer,
    elusive_rating numeric,
    explosive integer,
    first_downs integer,
    franchise_id bigint,
    fumbles integer,
    gap_attempts integer,
    grades_hands_fumble numeric,
    grades_offense numeric,
    grades_offense_penalty numeric,
    grades_pass numeric,
    grades_pass_block numeric,
    grades_pass_route numeric,
    grades_run numeric,
    grades_run_block numeric,
    longest integer,
    penalties integer,
    rec_yards integer,
    receptions integer,
    routes integer,
    run_plays integer,
    scramble_yards integer,
    scrambles integer,
    targets integer,
    total_touches integer,
    touchdowns integer,
    yards integer,
    yards_after_contact integer,
    yco_attempt numeric,
    ypa numeric,
    yprr numeric,
    zone_attempts integer,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, season)
);

CREATE INDEX IF NOT EXISTS idx_pff_rushing_summary_season ON pff.rushing_summary (season);
CREATE INDEX IF NOT EXISTS idx_pff_rushing_summary_school ON pff.rushing_summary (season, school);

COMMENT ON TABLE pff.rushing_summary IS
    'One row per player-season. Population mixes positions: anyone with a carry (~1,700 rows/season; HB/QB/WR dominate).';
COMMENT ON COLUMN pff.rushing_summary.season IS
    'Not in the CSV -- injected at load time from the operator''s --season, verified by the parser''s FBS-membership fingerprint guard.';
COMMENT ON COLUMN pff.rushing_summary.school IS
    'CFBD school full name, resolved from team_name via pff.team_map at load time; the join key toward core/ref tables.';
COMMENT ON COLUMN pff.rushing_summary.team_name IS
    'PFF''s own ALL-CAPS team abbreviation, verbatim from the export (e.g. BOWL GREEN, NWESTERN).';

-- ---------------------------------------------------------------------------
-- pff.offense_blocking (manual load per season; season fingerprint enforced)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pff.offense_blocking (
    season integer NOT NULL,
    school text NOT NULL,
    player text NOT NULL,
    player_id bigint NOT NULL,
    position text,
    team_name text NOT NULL,
    player_game_count integer,
    block_percent numeric,
    declined_penalties integer,
    franchise_id bigint,
    grades_offense numeric,
    grades_pass_block numeric,
    grades_run_block numeric,
    hits_allowed integer,
    hurries_allowed integer,
    non_spike_pass_block integer,
    non_spike_pass_block_percentage numeric,
    pass_block_percent numeric,
    pbe numeric,
    penalties integer,
    pressures_allowed integer,
    sacks_allowed integer,
    snap_counts_block integer,
    snap_counts_ce integer,
    snap_counts_lg integer,
    snap_counts_lt integer,
    snap_counts_offense integer,
    snap_counts_pass_block integer,
    snap_counts_pass_play integer,
    snap_counts_rg integer,
    snap_counts_rt integer,
    snap_counts_run_block integer,
    snap_counts_te integer,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, season)
);

CREATE INDEX IF NOT EXISTS idx_pff_offense_blocking_season ON pff.offense_blocking (season);
CREATE INDEX IF NOT EXISTS idx_pff_offense_blocking_school ON pff.offense_blocking (season, school);

COMMENT ON TABLE pff.offense_blocking IS
    'One row per player-season. Population mixes positions: every position that ever blocks (~5,800 rows/season, 1,400+ WRs -- filter OL on slot snap counts, not the position label).';
COMMENT ON COLUMN pff.offense_blocking.season IS
    'Not in the CSV -- injected at load time from the operator''s --season, verified by the parser''s FBS-membership fingerprint guard.';
COMMENT ON COLUMN pff.offense_blocking.school IS
    'CFBD school full name, resolved from team_name via pff.team_map at load time; the join key toward core/ref tables.';
COMMENT ON COLUMN pff.offense_blocking.team_name IS
    'PFF''s own ALL-CAPS team abbreviation, verbatim from the export (e.g. BOWL GREEN, NWESTERN).';

-- ---------------------------------------------------------------------------
-- pff.defense_summary (manual load per season; season fingerprint enforced)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pff.defense_summary (
    season integer NOT NULL,
    school text NOT NULL,
    player text NOT NULL,
    player_id bigint NOT NULL,
    position text,
    team_name text NOT NULL,
    player_game_count integer,
    assists integer,
    batted_passes integer,
    catch_rate numeric,
    declined_penalties integer,
    forced_fumbles integer,
    franchise_id bigint,
    fumble_recoveries integer,
    fumble_recovery_touchdowns integer,
    grades_coverage_defense numeric,
    grades_defense numeric,
    grades_defense_penalty numeric,
    grades_pass_rush_defense numeric,
    grades_run_defense numeric,
    grades_tackle numeric,
    hits integer,
    hurries integer,
    interception_touchdowns integer,
    interceptions integer,
    longest integer,
    missed_tackle_rate numeric,
    missed_tackles integer,
    pass_break_ups integer,
    penalties integer,
    qb_rating_against numeric,
    receptions integer,
    sacks integer,
    safeties integer,
    snap_counts_box integer,
    snap_counts_corner integer,
    snap_counts_coverage integer,
    snap_counts_defense integer,
    snap_counts_dl integer,
    snap_counts_dl_a_gap integer,
    snap_counts_dl_b_gap integer,
    snap_counts_dl_outside_t integer,
    snap_counts_dl_over_t integer,
    snap_counts_fs integer,
    snap_counts_offball integer,
    snap_counts_pass_rush integer,
    snap_counts_run_defense integer,
    snap_counts_slot integer,
    stops integer,
    tackles integer,
    tackles_for_loss integer,
    targets integer,
    total_pressures integer,
    touchdowns integer,
    yards integer,
    yards_after_catch integer,
    yards_per_reception numeric,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, season)
);

CREATE INDEX IF NOT EXISTS idx_pff_defense_summary_season ON pff.defense_summary (season);
CREATE INDEX IF NOT EXISTS idx_pff_defense_summary_school ON pff.defense_summary (season, school);

COMMENT ON TABLE pff.defense_summary IS
    'One row per player-season. Population mixes positions: anyone with a defensive snap (~5,400 rows/season; core CB/S/LB/DI/ED plus offensive players with defensive snaps).';
COMMENT ON COLUMN pff.defense_summary.season IS
    'Not in the CSV -- injected at load time from the operator''s --season, verified by the parser''s FBS-membership fingerprint guard.';
COMMENT ON COLUMN pff.defense_summary.school IS
    'CFBD school full name, resolved from team_name via pff.team_map at load time; the join key toward core/ref tables.';
COMMENT ON COLUMN pff.defense_summary.team_name IS
    'PFF''s own ALL-CAPS team abbreviation, verbatim from the export (e.g. BOWL GREEN, NWESTERN).';

-- ---------------------------------------------------------------------------
-- pff.team_map -- committed PFF-abbreviation -> CFBD-school map
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pff.team_map (
    pff_team_name text PRIMARY KEY,
    cfbd_school text NOT NULL
);

COMMENT ON TABLE pff.team_map IS
    'PFF ALL-CAPS team abbreviation -> CFBD school full name. Seeded below '
    'from docs/brainstorms/2026-09-01-pff-team-name-map.json (validated over '
    'the 2023-2025 exports: all 136 FBS names map); this migration is the '
    'canonical copy. A load meeting an unmapped name fails loud '
    '(UnmappedNamesError) -- a new name means realignment: add the mapping '
    'here deliberately, never guess in code.';

INSERT INTO pff.team_map (pff_team_name, cfbd_school) VALUES
    ('AIR FORCE', 'Air Force'),
    ('AKRON', 'Akron'),
    ('ALABAMA', 'Alabama'),
    ('APP STATE', 'App State'),
    ('ARIZONA', 'Arizona'),
    ('ARIZONA ST', 'Arizona State'),
    ('ARK STATE', 'Arkansas State'),
    ('ARKANSAS', 'Arkansas'),
    ('ARMY', 'Army'),
    ('AUBURN', 'Auburn'),
    ('BALL ST', 'Ball State'),
    ('BAYLOR', 'Baylor'),
    ('BOISE ST', 'Boise State'),
    ('BOSTON COL', 'Boston College'),
    ('BOWL GREEN', 'Bowling Green'),
    ('BUFFALO', 'Buffalo'),
    ('BYU', 'BYU'),
    ('C MICHIGAN', 'Central Michigan'),
    ('CAL', 'California'),
    ('CHARLOTTE', 'Charlotte'),
    ('CINCINNATI', 'Cincinnati'),
    ('CLEMSON', 'Clemson'),
    ('COAST CAR', 'Coastal Carolina'),
    ('COLO STATE', 'Colorado State'),
    ('COLORADO', 'Colorado'),
    ('DELAWARE', 'Delaware'),
    ('DOMINION', 'Old Dominion'),
    ('DUKE', 'Duke'),
    ('E CAROLINA', 'East Carolina'),
    ('E MICHIGAN', 'Eastern Michigan'),
    ('FAU', 'Florida Atlantic'),
    ('FIU', 'Florida International'),
    ('FLORIDA', 'Florida'),
    ('FLORIDA ST', 'Florida State'),
    ('FRESNO ST', 'Fresno State'),
    ('GA SOUTHRN', 'Georgia Southern'),
    ('GA STATE', 'Georgia State'),
    ('GA TECH', 'Georgia Tech'),
    ('GEORGIA', 'Georgia'),
    ('HAWAII', 'Hawai''i'),
    ('HOUSTON', 'Houston'),
    ('ILLINOIS', 'Illinois'),
    ('INDIANA', 'Indiana'),
    ('IOWA', 'Iowa'),
    ('IOWA STATE', 'Iowa State'),
    ('JAMES MAD', 'James Madison'),
    ('JVILLE ST', 'Jacksonville State'),
    ('KANSAS', 'Kansas'),
    ('KANSAS ST', 'Kansas State'),
    ('KENNESAW', 'Kennesaw State'),
    ('KENT STATE', 'Kent State'),
    ('KENTUCKY', 'Kentucky'),
    ('LA LAFAYET', 'Louisiana'),
    ('LA MONROE', 'UL Monroe'),
    ('LA TECH', 'Louisiana Tech'),
    ('LIBERTY', 'Liberty'),
    ('LOUISVILLE', 'Louisville'),
    ('LSU', 'LSU'),
    ('MARSHALL', 'Marshall'),
    ('MARYLAND', 'Maryland'),
    ('MEMPHIS', 'Memphis'),
    ('MIAMI FL', 'Miami'),
    ('MIAMI OH', 'Miami (OH)'),
    ('MICH STATE', 'Michigan State'),
    ('MICHIGAN', 'Michigan'),
    ('MIDDLE TN', 'Middle Tennessee'),
    ('MINNESOTA', 'Minnesota'),
    ('MISS STATE', 'Mississippi State'),
    ('MISSOURI', 'Missouri'),
    ('MO STATE', 'Missouri State'),
    ('N CAROLINA', 'North Carolina'),
    ('N ILLINOIS', 'Northern Illinois'),
    ('N TEXAS', 'North Texas'),
    ('NAVY', 'Navy'),
    ('NC STATE', 'NC State'),
    ('NEBRASKA', 'Nebraska'),
    ('NEVADA', 'Nevada'),
    ('NEW MEX ST', 'New Mexico State'),
    ('NEW MEXICO', 'New Mexico'),
    ('NOTRE DAME', 'Notre Dame'),
    ('NWESTERN', 'Northwestern'),
    ('OHIO', 'Ohio'),
    ('OHIO STATE', 'Ohio State'),
    ('OKLA STATE', 'Oklahoma State'),
    ('OKLAHOMA', 'Oklahoma'),
    ('OLE MISS', 'Ole Miss'),
    ('OREGON', 'Oregon'),
    ('OREGON ST', 'Oregon State'),
    ('PENN STATE', 'Penn State'),
    ('PITTSBURGH', 'Pittsburgh'),
    ('PURDUE', 'Purdue'),
    ('RICE', 'Rice'),
    ('RUTGERS', 'Rutgers'),
    ('S ALABAMA', 'South Alabama'),
    ('S CAROLINA', 'South Carolina'),
    ('S DIEGO ST', 'San Diego State'),
    ('S JOSE ST', 'San José State'),
    ('SM HOUSTON', 'Sam Houston'),
    ('SMU', 'SMU'),
    ('SO MISS', 'Southern Miss'),
    ('STANFORD', 'Stanford'),
    ('SYRACUSE', 'Syracuse'),
    ('TCU', 'TCU'),
    ('TEMPLE', 'Temple'),
    ('TENNESSEE', 'Tennessee'),
    ('TEXAS', 'Texas'),
    ('TEXAS A&M', 'Texas A&M'),
    ('TEXAS ST', 'Texas State'),
    ('TEXAS TECH', 'Texas Tech'),
    ('TOLEDO', 'Toledo'),
    ('TROY', 'Troy'),
    ('TULANE', 'Tulane'),
    ('TULSA', 'Tulsa'),
    ('UAB', 'UAB'),
    ('UCF', 'UCF'),
    ('UCLA', 'UCLA'),
    ('UCONN', 'UConn'),
    ('UMASS', 'Massachusetts'),
    ('UNLV', 'UNLV'),
    ('USC', 'USC'),
    ('USF', 'South Florida'),
    ('UTAH', 'Utah'),
    ('UTAH ST', 'Utah State'),
    ('UTEP', 'UTEP'),
    ('UTSA', 'UTSA'),
    ('VA TECH', 'Virginia Tech'),
    ('VANDERBILT', 'Vanderbilt'),
    ('VIRGINIA', 'Virginia'),
    ('W KENTUCKY', 'Western Kentucky'),
    ('W MICHIGAN', 'Western Michigan'),
    ('W VIRGINIA', 'West Virginia'),
    ('WAKE', 'Wake Forest'),
    ('WASH STATE', 'Washington State'),
    ('WASHINGTON', 'Washington'),
    ('WISCONSIN', 'Wisconsin'),
    ('WYOMING', 'Wyoming')
ON CONFLICT (pff_team_name) DO UPDATE SET cfbd_school = EXCLUDED.cfbd_school;

-- ---------------------------------------------------------------------------
-- pff.player_xwalk -- PFF player id -> CFBD athlete id (filled by script)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pff.player_xwalk (
    pff_player_id bigint PRIMARY KEY,
    athlete_id text,
    match_method text,
    matched_at timestamptz
);

COMMENT ON TABLE pff.player_xwalk IS
    'PFF player_id -> core.roster.id, built by '
    'scripts/build_pff_player_xwalk.py (normalized last name + first initial '
    '+ school, suffixes stripped both sides; ambiguous candidates are left '
    'unmatched and reported, never guessed). Created empty by this '
    'migration.';
COMMENT ON COLUMN pff.player_xwalk.athlete_id IS
    'core.roster.id as text; NULL only if a row is ever staged unmatched '
    '(the build script inserts matches only).';

-- ---------------------------------------------------------------------------
-- Grants: NONE. See the schema comment above -- licensed data, deliberate.
-- ---------------------------------------------------------------------------
