-- Migration: 053_ncaa_tables
--
-- The NCAA (stats.ncaa.org) bundle (B6a): schedules, teams, rosters,
-- linescores, player_stats, team_stats, and pbp, sourced from
-- sportsdataverse-data's ncaa_mfb_* GitHub release tags (one parquet per
-- season per dataset -- see flat_files.py's "Per-season multi-file sources"
-- docstring for the fetch mechanism). Parsers:
-- src/pipelines/sources/flatfile_parsers/ncaa.py. Registry entries:
-- src/pipelines/sources/flat_files.py (ncaa_schedule, ncaa_teams,
-- ncaa_rosters, ncaa_linescores, ncaa_player_stats, ncaa_team_stats,
-- ncaa_pbp).
--
-- SCHEMA POSTURE: `ncaa` is created WITHOUT any anon/authenticated grant --
-- no GRANT USAGE on the schema, no GRANT SELECT on any table (mirrors
-- src/schemas/scouting/001_tables.sql's posture, not migrations 041/052's
-- read-only-exposure posture). Reason: stats.ncaa.org's id space (contest,
-- team, player ids below) is DISJOINT from CFBD/ESPN's, and unlike the sdv_*
-- crosswalk/ratings tables in migration 052 (whose numeric ids were verified
-- equal to ESPN's) there is no verified equivalence to lean on here at all.
-- Exposing this schema via PostgREST before a deliberate crosswalk/exposure
-- decision would invite a silent wrong-team/wrong-player join downstream
-- (cfb-app, cfb-scout) the moment someone joins on a bare id column that
-- merely happens to share a name with a CFBD one. Until that decision is
-- made, this schema is reachable only by the pipeline's service-role
-- connection. See ncaa.py's module docstring for the full id-space finding,
-- including that ncaa_team_id/ncaa_player_id are RE-ISSUED EVERY SEASON
-- (not even stable within this schema across years) -- verified: Alabama's
-- ncaa_team_id is 62682 in the 2013 file and 606070 in 2025; a specific
-- Alabama player confirmed on both the 2024 and 2025 rosters carries two
-- different ncaa_player_id values.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-041+/052. Idempotent (IF NOT EXISTS throughout).
--
-- Note on dlt coexistence (mirrors 041/052): these are merge targets for dlt
-- pipelines; dlt adds its _dlt_id/_dlt_load_id bookkeeping columns on first
-- load. Types below match dlt's inference for the parser's output values,
-- verified against the real 2025-season (and, for schema-drift columns,
-- 2013-season) parquet files' pyarrow schemas -- not API docs. Several
-- source columns ship as numeric-looking strings (stats.ncaa.org's own
-- ids, and every stat value in player_stats/team_stats) -- the parser casts
-- these to bigint/double precision before load, so column types below are
-- the POST-parse types, not the raw parquet types.

CREATE SCHEMA IF NOT EXISTS ncaa;

COMMENT ON SCHEMA ncaa IS
    'stats.ncaa.org data via sportsdataverse-data (ncaa_mfb_* release tags). '
    'DELIBERATELY UNGRANTED: no USAGE for anon/authenticated, no per-table '
    'SELECT -- service-role/pipeline access only, like the scouting schema. '
    'stats.ncaa.org contest/team/player ids are a disjoint id space from '
    'CFBD/ESPN with no verified crosswalk (unlike ref.team_id_xwalk/'
    'ref.game_id_xwalk in migration 052, whose ids ARE verified-equal to '
    'ESPN''s) -- keeping this schema unreachable via PostgREST prevents a '
    'silent wrong-team/wrong-player join downstream until a deliberate '
    'crosswalk/exposure decision is made. Bare, file-native id columns are '
    'prefixed ncaa_ throughout for the same reason (ncaa_contest_id, '
    'ncaa_team_id, ncaa_player_id, ncaa_opponent_id); ESPN bridge columns '
    '(espn_game_id, where the source file carries one) are kept unprefixed '
    'and are the intended future FCS-join path. ncaa_team_id and '
    'ncaa_player_id are RE-ISSUED EVERY SEASON -- never a stable identifier '
    'across years even within this schema (see ncaa.py''s module docstring '
    'for the verification).';

-- ---------------------------------------------------------------------------
-- ncaa.schedule (weekly refresh in-season; ncaa_mfb_schedule_{season}.parquet)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ncaa.schedule (
    season bigint NOT NULL,
    ncaa_contest_id bigint NOT NULL,
    ncaa_team_id bigint NOT NULL,
    ncaa_opponent_id bigint,
    team_name text,
    opponent text,
    game_date date,
    result text,
    outcome text,
    team_score bigint,
    opponent_score bigint,
    attendance bigint,
    academic_year integer,
    espn_game_id bigint,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, ncaa_contest_id, ncaa_team_id)
);

COMMENT ON TABLE ncaa.schedule IS
    'One row per team per contest (a played game has 2 rows sharing one '
    'ncaa_contest_id). 2 rows in the 2025 file had a null upstream '
    'contest_id (canceled Week 0 games with no box score) -- dropped by the '
    'parser, not represented here.';
COMMENT ON COLUMN ncaa.schedule.ncaa_team_id IS
    'stats.ncaa.org''s own team id -- RE-ISSUED EVERY SEASON, not a stable '
    'cross-season identifier (see schema comment). NOT an ESPN/CFBD id.';
COMMENT ON COLUMN ncaa.schedule.espn_game_id IS
    'ESPN/CFBD bridge id (nullable -- absent as a column entirely in the '
    '2013 file, ~2% null in 2025). The intended future FCS-join path to '
    'core.games; not verified equal to core.games.id in this migration '
    '(unlike ref.game_id_xwalk''s verified equivalence in migration 052).';
COMMENT ON COLUMN ncaa.schedule.team_name IS
    'Raw source text, not cleaned -- can carry embedded season-end '
    'annotations, e.g. "Kennesaw St. Owls (10-4) *Myrtle Beach Bowl".';

CREATE INDEX IF NOT EXISTS idx_ncaa_schedule_season ON ncaa.schedule (season);
CREATE INDEX IF NOT EXISTS idx_ncaa_schedule_team ON ncaa.schedule (season, ncaa_team_id);
CREATE INDEX IF NOT EXISTS idx_ncaa_schedule_espn_game
    ON ncaa.schedule (espn_game_id) WHERE espn_game_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- ncaa.teams (annual refresh; ncaa_mfb_teams_{season}.parquet)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ncaa.teams (
    season bigint NOT NULL,
    ncaa_team_id bigint NOT NULL,
    team_name text,
    academic_year integer,
    division integer,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, ncaa_team_id)
);

COMMENT ON COLUMN ncaa.teams.ncaa_team_id IS
    'stats.ncaa.org''s own team id -- RE-ISSUED EVERY SEASON (see schema '
    'comment). NOT an ESPN/CFBD id.';
COMMENT ON COLUMN ncaa.teams.division IS
    'Raw stats.ncaa.org numeric division code, not decoded to FBS/FCS text. '
    'Observed values: 11 (136 teams in 2025, 128 in 2013) and 12 (129 teams '
    'in 2025, 124 in 2013) -- counts consistent with FBS/FCS membership in '
    'those years, but this is an inference from team counts, not a '
    'documented NCAA code table.';

CREATE INDEX IF NOT EXISTS idx_ncaa_teams_season ON ncaa.teams (season);

-- ---------------------------------------------------------------------------
-- ncaa.rosters (annual refresh; ncaa_mfb_rosters_{season}.parquet)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ncaa.rosters (
    season bigint NOT NULL,
    ncaa_team_id bigint NOT NULL,
    ncaa_player_id bigint NOT NULL,
    team_name text,
    player_name text,
    jersey text,
    statcrew_jersey text,
    player_class text,
    position text,
    height_inches double precision,
    weight bigint,
    hometown text,
    high_school text,
    games_played bigint,
    games_started bigint,
    academic_year integer,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, ncaa_team_id, ncaa_player_id)
);

COMMENT ON COLUMN ncaa.rosters.ncaa_player_id IS
    'stats.ncaa.org''s own player id -- RE-ISSUED EVERY SEASON, verified: '
    'cross-referencing 82 players common to Alabama''s 2024 and 2025 '
    'rosters by name, every one carries a different ncaa_player_id between '
    'the two seasons (e.g. Vito Perri: 8626995 in 2024, 9240713 in 2025). '
    'NOT an ESPN/CFBD id, and not stable across seasons even within this '
    'schema -- never join across seasons on this column alone.';
COMMENT ON COLUMN ncaa.rosters.height_inches IS
    'Parsed from the source''s "feet-inches" text format (e.g. "6-0" -> 72.0).';

CREATE INDEX IF NOT EXISTS idx_ncaa_rosters_season ON ncaa.rosters (season);
CREATE INDEX IF NOT EXISTS idx_ncaa_rosters_team ON ncaa.rosters (season, ncaa_team_id);

-- ---------------------------------------------------------------------------
-- ncaa.linescores (weekly refresh in-season; ncaa_mfb_linescore_{season}.parquet)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ncaa.linescores (
    season bigint NOT NULL,
    ncaa_contest_id bigint NOT NULL,
    team_name text NOT NULL,
    period text NOT NULL,
    home_away text,
    points bigint,
    final_score bigint,
    game_date date,
    venue text,
    attendance bigint,
    espn_game_id bigint,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, ncaa_contest_id, team_name, period)
);

COMMENT ON TABLE ncaa.linescores IS
    'No team-id column in the source file at all -- team_name (a name, not '
    'an id) is part of the PK alongside ncaa_contest_id, which is safe '
    'because a single contest''s two team names never collide with each '
    'other within that same contest_id.';
COMMENT ON COLUMN ncaa.linescores.period IS
    'Observed values: "1"-"4" (quarters) plus overtime labels "1OT".."5OT".';
COMMENT ON COLUMN ncaa.linescores.final_score IS
    'The GAME''S total final score (renamed from the source''s "final" '
    'column) -- repeated identically on every period row for a team, not '
    'the score for that specific period (see "points" for that).';
COMMENT ON COLUMN ncaa.linescores.game_date IS
    'Date portion only of the source''s "MM/DD/YYYY HH:MM AM/PM" -- the '
    'time-of-day is local-venue time with no timezone information upstream, '
    'so it was dropped rather than fabricating a timezone-aware timestamp.';

CREATE INDEX IF NOT EXISTS idx_ncaa_linescores_season ON ncaa.linescores (season);
CREATE INDEX IF NOT EXISTS idx_ncaa_linescores_contest ON ncaa.linescores (season, ncaa_contest_id);
CREATE INDEX IF NOT EXISTS idx_ncaa_linescores_espn_game
    ON ncaa.linescores (espn_game_id) WHERE espn_game_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- ncaa.player_stats (weekly refresh in-season; ncaa_mfb_player_stats_{season}.parquet)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ncaa.player_stats (
    season bigint NOT NULL,
    ncaa_contest_id bigint NOT NULL,
    ncaa_team_id bigint NOT NULL,
    player_number text NOT NULL,
    category text NOT NULL,
    row_seq bigint NOT NULL,
    name text,
    position text,
    espn_game_id bigint,
    rush_attempts double precision,
    rush_yds_gained double precision,
    rush_yds_lost double precision,
    yds_rush double precision,
    rush_tds double precision,
    rush_long double precision,
    pass_attempts double precision,
    completions double precision,
    pass_yards double precision,
    interceptions double precision,
    pass_tds double precision,
    pass_eff double precision,
    yds_per_completion double precision,
    pct double precision,
    long_pass double precision,
    rec double precision,
    receiving_yards double precision,
    yards_per_reception double precision,
    rec_td double precision,
    long_rec double precision,
    yds double precision,
    plays double precision,
    pbu double precision,
    int double precision,
    intyds double precision,
    int_ret_tds double precision,
    pdef double precision,
    ko_ret double precision,
    ko_ret_yds double precision,
    kick_ret_tds double precision,
    long_kor double precision,
    sacks double precision,
    solo_tack double precision,
    asst_tack double precision,
    tackles double precision,
    fgm double precision,
    fga double precision,
    fg_blocks_allowed double precision,
    punt_ret double precision,
    punt_ret_yds double precision,
    punt_ret_tds double precision,
    long_pr double precision,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, ncaa_contest_id, ncaa_team_id, player_number, category, row_seq)
);

COMMENT ON TABLE ncaa.player_stats IS
    'Long/wide hybrid: one row per player per stat-category per game, but '
    'stats.ncaa.org''s per-category box score is itself a concatenation of '
    'several physical sub-tables, so the same (contest, team, '
    'player_number, category) key can legitimately repeat with disjoint '
    'populated columns (verified: Ethan Loss, jersey 19, contest 6386278, '
    'has two category="other" rows -- one carrying yds/plays, the other '
    'ko_ret/ko_ret_yds/...). row_seq (0-based, per-key encounter order) '
    'exists ONLY to make the primary key deterministic in that case -- it '
    'is not a meaningful sort order. Historical seasons are sparser: the '
    '2013 file has none of the stat columns populated (every row''s '
    'category is "other" with nothing else) -- dlt schema evolution fills '
    'the gap; do not assume a stat column is populated for every season.';
COMMENT ON COLUMN ncaa.player_stats.ncaa_team_id IS
    'stats.ncaa.org''s own team id -- RE-ISSUED EVERY SEASON (see schema '
    'comment). NOT an ESPN/CFBD id.';
COMMENT ON COLUMN ncaa.player_stats.player_number IS
    'Jersey number as text. The source''s "number" column is null for '
    '~20% of rows (team-total rows, where "name" is literally "TEAM" or '
    'the school name) -- coalesced to the sentinel ''TEAM'' here rather '
    'than dropped, since those rows carry real team-aggregate data. There '
    'is NO player-id column in this source at all -- this table cannot be '
    'joined to ncaa.rosters by a stable id, only by a fragile (team, '
    'jersey-number-that-game, name) heuristic (not implemented anywhere in '
    'this pipeline).';
COMMENT ON COLUMN ncaa.player_stats.row_seq IS
    'Disambiguates a confirmed upstream duplicate-row quirk -- see table '
    'comment. Not meaningful on its own; exists only so the primary key is '
    'deterministic.';

CREATE INDEX IF NOT EXISTS idx_ncaa_player_stats_season ON ncaa.player_stats (season);
CREATE INDEX IF NOT EXISTS idx_ncaa_player_stats_contest
    ON ncaa.player_stats (season, ncaa_contest_id);
CREATE INDEX IF NOT EXISTS idx_ncaa_player_stats_team
    ON ncaa.player_stats (season, ncaa_team_id);

-- ---------------------------------------------------------------------------
-- ncaa.team_stats (weekly refresh in-season; ncaa_mfb_team_stats_{season}.parquet)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ncaa.team_stats (
    season bigint NOT NULL,
    ncaa_contest_id bigint NOT NULL,
    category text NOT NULL,
    stat text NOT NULL,
    period text NOT NULL,
    row_seq bigint NOT NULL,
    away_team text,
    away_value double precision,
    home_team text,
    home_value double precision,
    espn_game_id bigint,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, ncaa_contest_id, category, stat, period, row_seq)
);

COMMENT ON TABLE ncaa.team_stats IS
    'Long format: one row per named stat per period of one game, with both '
    'teams'' values on the same row. Confirmed upstream data-entry quirk in '
    'overtime games: an "1stOT"-style value leaks into the stat column '
    'instead of (or alongside) period, producing genuine duplicate '
    '(ncaa_contest_id, category, stat, period) keys (verified: contest '
    '6386336, a Temple @ Tulsa OT game, has 6 rows for '
    'category=''Rushing''/stat=''1stOT''/period=''total'' with different '
    'values). row_seq (0-based, per-key encounter order) makes the primary '
    'key deterministic without dropping any row -- not a meaningful sort '
    'order on its own.';
COMMENT ON COLUMN ncaa.team_stats.away_value IS
    'Source ships this as a numeric string, occasionally with a '
    'thousand-separator comma for high-magnitude values (e.g. a '
    'small-sample-size game''s Pass Eff of "1,051.60") -- the parser strips '
    'commas and casts to double precision.';

CREATE INDEX IF NOT EXISTS idx_ncaa_team_stats_season ON ncaa.team_stats (season);
CREATE INDEX IF NOT EXISTS idx_ncaa_team_stats_contest
    ON ncaa.team_stats (season, ncaa_contest_id);

-- ---------------------------------------------------------------------------
-- ncaa.pbp (weekly refresh in-season; ncaa_mfb_pbp_{season}.parquet)
--
-- Scope note: the 2025-season file is ~11.8MB (measured via direct
-- download), far under the ~200MB single-file threshold that would have
-- called for deferring this dataset -- included in full.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ncaa.pbp (
    season bigint NOT NULL,
    ncaa_contest_id bigint NOT NULL,
    drive_number bigint NOT NULL,
    play_number bigint NOT NULL,
    offense text,
    drive_result text,
    drive_scored boolean,
    down bigint,
    distance bigint,
    yard_line text,
    yard_line_side text,
    yard_line_number bigint,
    play_type text,
    clock text,
    yards_gained bigint,
    formation text,
    passer text,
    rusher text,
    receiver text,
    kicker text,
    punter text,
    returner text,
    run_direction text,
    qb_scramble boolean,
    pass_complete boolean,
    pass_depth text,
    pass_direction text,
    tackler_1 text,
    tackler_2 text,
    kick_yards bigint,
    return_yards bigint,
    punt_yards bigint,
    fg_distance bigint,
    fg_made boolean,
    is_first_down boolean,
    is_touchdown boolean,
    is_safety boolean,
    is_fumble boolean,
    is_turnover boolean,
    turnover_type text,
    out_of_bounds boolean,
    no_play boolean,
    fair_catch boolean,
    penalty_flag boolean,
    penalty_team text,
    penalty_type text,
    penalty_player text,
    penalty_yards bigint,
    end_yard_line text,
    play_text text,
    espn_game_id bigint,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, ncaa_contest_id, drive_number, play_number)
);

COMMENT ON COLUMN ncaa.pbp.ncaa_contest_id IS
    'stats.ncaa.org''s own contest id -- see ncaa.schedule for the same id '
    'namespace. Globally unique in practice (unlike ncaa_team_id/'
    'ncaa_player_id, contest ids are not observed to repeat across seasons '
    'given their magnitude growth: ~688871 in 2013 vs ~6386278 in 2025), '
    'but season is kept in the PK for consistency with every other table '
    'in this schema.';

CREATE INDEX IF NOT EXISTS idx_ncaa_pbp_season ON ncaa.pbp (season);
CREATE INDEX IF NOT EXISTS idx_ncaa_pbp_contest ON ncaa.pbp (season, ncaa_contest_id);

-- ---------------------------------------------------------------------------
-- Grants: NONE. See the CREATE SCHEMA comment above -- this is deliberate.
-- ---------------------------------------------------------------------------
