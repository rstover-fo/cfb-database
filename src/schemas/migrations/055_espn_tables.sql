-- Migration: 055_espn_tables
--
-- The ESPN player-grain bundle (B6b): passing/rushing/receiving/defensive
-- advanced player stats (EPA splits) and play-participant attribution,
-- sourced from sportsdataverse-data's espn_cfb_adv_*/
-- espn_cfb_play_participants GitHub release tags (one parquet per season
-- per dataset -- see flat_files.py's "Per-season multi-file sources"
-- docstring for the fetch mechanism). Parsers:
-- src/pipelines/sources/flatfile_parsers/espn.py. Registry entries:
-- src/pipelines/sources/flat_files.py (espn_player_passing,
-- espn_player_rushing, espn_player_receiving, espn_player_defense,
-- espn_play_participants).
--
-- SCHEMA POSTURE: no new `espn` schema -- these five tables live in the
-- EXISTING `stats` schema (already GRANT USAGE'd to anon/authenticated via
-- grant_read_access_for_security_invoker.sql), unlike migration 053's
-- deliberately ungranted `ncaa` schema. Schema-architect verdict: ESPN's
-- numeric ids ARE CFBD's ids (verified against the live warehouse
-- 2026-08-29: ref.teams.id 333=Alabama/2483=Oregon match ESPN ids,
-- core.games ids are ESPN event ids, athlete ids are shared) -- provenance
-- is expressed by the `espn_` table-name prefix, per this repo's existing
-- convention (ratings.massey_composite, draft.nflverse_draft_picks,
-- ratings.espn_fpi_weekly), not by schema isolation. Each table below is
-- GRANTed SELECT individually.
--
-- DROPPED FROM THIS UNIT, reported loudly rather than silently omitted:
-- `espn_pbp_2002_2003` / `core.espn_plays_2002_2003`. The task's premise
-- (a pre-CFBD 2002+2003 play-by-play gap-fill) does not hold under real
-- verification: the sportsdataverse package's own `load_cfb_pbp` docstring
-- states "seasons: an int or iterable of seasons (>= 2004)", confirmed
-- live -- `play_by_play_2004.parquet` downloads (25.4MB) while
-- `play_by_play_2002.parquet` and `play_by_play_2003.parquet` both 404
-- under the real `espn_cfb_pbp` release tag. cfbfastR's documented 2004+
-- floor was correct; the 2002-03 claim (traced to a brainstorm doc) was
-- not. No substitute season or dataset was invented in its place.
--
-- NO ATHLETE-ID COLUMN, reported loudly per the task's "report if any file
-- lacks an athlete id" instruction: none of espn_player_passing/_rushing/
-- _receiving/_defense carries an athlete id at all -- each identifies a
-- player ONLY by a free-text name column (ESPN's advanced-stat text
-- scrape, a different upstream code path than the id-carrying
-- espn_cfb_play_participants live-API wrapper). The "shared CFBD/ESPN id
-- namespace, joins to core.roster.id" wording (migration 050's PFF/SIS-era
-- join-spine convention) therefore applies ONLY to
-- stats.espn_play_participants' {type}_player_id columns below -- see that
-- table's column comments. None of the five tables carries a `position`
-- column either (adv_passing/adv_rushing/adv_receiving are already
-- position-scoped by name; adv_defensive_players and play_participants
-- simply don't ship one). espn_play_participants also carries no team
-- id/name column at all -- see its table comment.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-041+/052/053/054. Idempotent (IF NOT EXISTS
-- throughout).
--
-- Note on dlt coexistence (mirrors 052/053): these are merge targets for
-- dlt pipelines; dlt adds its _dlt_id/_dlt_load_id bookkeeping columns on
-- first load. Types below match dlt's inference for the parser's output
-- values, verified against the real 2025-season (and, for schema-drift
-- columns, 2005/2014-season) parquet files' pyarrow schemas -- not API
-- docs. The parser pre-renames every Title_Case/camelCase source column
-- (e.g. "CompPct", "EPA_per_Play") to the exact snake_case name used below
-- BEFORE the row reaches dlt, so the loaded column name matches this
-- migration by construction rather than depending on dlt's naming-
-- convention normalizer to independently agree (see espn.py's module
-- docstring for the full rename map).

-- ---------------------------------------------------------------------------
-- stats.espn_player_passing (weekly refresh in-season; ESPN advanced-stat
-- text scrape, espn_cfb_adv_passing/adv_passing_{season}.parquet)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stats.espn_player_passing (
    season bigint NOT NULL,
    game_id bigint NOT NULL,
    pos_team_id bigint NOT NULL,
    passer_player_name text NOT NULL,
    week bigint,
    pos_team text,
    comp bigint,
    att bigint,
    x_comp double precision,
    yds double precision,
    pass_td bigint,
    int bigint,
    ypa double precision,
    epa double precision,
    epa_per_play double precision,
    wpa double precision,
    sr double precision,
    sck bigint,
    comp_pct double precision,
    x_comp_pct double precision,
    cpoe double precision,
    qbr_epa double precision,
    sack_epa double precision,
    pass_epa double precision,
    rush_epa double precision,
    pen_epa double precision,
    spread double precision,
    era0 bigint,
    era1 bigint,
    era2 bigint,
    era3 bigint,
    exp_qbr double precision,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, game_id, pos_team_id, passer_player_name)
);

COMMENT ON TABLE stats.espn_player_passing IS
    'ESPN advanced-stat (EPA-split) passing box score, one row per '
    'passer per game. Source: sportsdataverse-data espn_cfb_adv_passing '
    'release (ESPN''s text-scraped advanced box score, distinct upstream '
    'path from espn_cfb_play_participants -- see espn.py module docstring). '
    'NO athlete-id column exists in this dataset -- passer_player_name is '
    'free text, not joinable to core.roster.id by id. NO position column '
    '(the table is already passer-scoped).';
COMMENT ON COLUMN stats.espn_player_passing.pos_team_id IS
    'ESPN numeric team id, verified equal to ref.teams.id (2026-08-29 '
    'warehouse check, same equivalence as the sdv_*/ncaa_mfb_* bundles).';
COMMENT ON COLUMN stats.espn_player_passing.game_id IS
    'ESPN event id, equal to core.games.id.';
COMMENT ON COLUMN stats.espn_player_passing.passer_player_name IS
    'Free-text player name -- the ONLY player identifier this source '
    'carries (no athlete id). Never null (0 nulls observed across the '
    '2005 and 2025 files).';

CREATE INDEX IF NOT EXISTS idx_espn_player_passing_season
    ON stats.espn_player_passing (season, passer_player_name);
CREATE INDEX IF NOT EXISTS idx_espn_player_passing_game
    ON stats.espn_player_passing (game_id);

GRANT SELECT ON stats.espn_player_passing TO anon, authenticated;

-- ---------------------------------------------------------------------------
-- stats.espn_player_rushing (weekly refresh in-season;
-- espn_cfb_adv_rushing/adv_rushing_{season}.parquet)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stats.espn_player_rushing (
    season bigint NOT NULL,
    game_id bigint NOT NULL,
    pos_team_id bigint NOT NULL,
    rusher_player_name text NOT NULL,
    week bigint,
    pos_team text,
    car bigint,
    yds double precision,
    rush_td bigint,
    ypc double precision,
    epa double precision,
    epa_per_play double precision,
    wpa double precision,
    sr double precision,
    fum bigint,
    fum_lost bigint,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, game_id, pos_team_id, rusher_player_name)
);

COMMENT ON TABLE stats.espn_player_rushing IS
    'ESPN advanced-stat (EPA-split) rushing box score, one row per rusher '
    'per game. Source: sportsdataverse-data espn_cfb_adv_rushing release. '
    'NO athlete-id column; NO position column. See '
    'stats.espn_player_passing''s table comment for the shared caveats.';
COMMENT ON COLUMN stats.espn_player_rushing.rusher_player_name IS
    'Free-text player name -- the ONLY player identifier this source '
    'carries. NULL for a real subset of rows (78/10206 in the 2025 file) '
    '-- genuine unattributed-rusher plays (real yardage/EPA; ESPN''s text '
    'scrape could not resolve a name), coalesced by the parser to the '
    'sentinel ''UNATTRIBUTED'' rather than dropped, verified unique per '
    '(game_id, pos_team_id) with 0 collisions.';
COMMENT ON COLUMN stats.espn_player_rushing.pos_team_id IS
    'ESPN numeric team id, verified equal to ref.teams.id.';

CREATE INDEX IF NOT EXISTS idx_espn_player_rushing_season
    ON stats.espn_player_rushing (season, rusher_player_name);
CREATE INDEX IF NOT EXISTS idx_espn_player_rushing_game
    ON stats.espn_player_rushing (game_id);

GRANT SELECT ON stats.espn_player_rushing TO anon, authenticated;

-- ---------------------------------------------------------------------------
-- stats.espn_player_receiving (weekly refresh in-season;
-- espn_cfb_adv_receiving/adv_receiving_{season}.parquet)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stats.espn_player_receiving (
    season bigint NOT NULL,
    game_id bigint NOT NULL,
    pos_team_id bigint NOT NULL,
    receiver_player_name text NOT NULL,
    week bigint,
    pos_team text,
    rec bigint,
    tar bigint,
    yds double precision,
    rec_td bigint,
    ypt double precision,
    epa double precision,
    epa_per_play double precision,
    wpa double precision,
    sr double precision,
    fum bigint,
    fum_lost bigint,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, game_id, pos_team_id, receiver_player_name)
);

COMMENT ON TABLE stats.espn_player_receiving IS
    'ESPN advanced-stat (EPA-split) receiving box score, one row per '
    'receiver per game (a "receiver" here includes a 0-target/0-reception '
    'unattributed-target row -- see receiver_player_name comment). Source: '
    'sportsdataverse-data espn_cfb_adv_receiving release. NO athlete-id '
    'column; NO position column.';
COMMENT ON COLUMN stats.espn_player_receiving.receiver_player_name IS
    'Free-text player name -- the ONLY player identifier this source '
    'carries. NULL for a real subset of rows (1749/18529 in the 2025 file) '
    '-- genuine unattributed-target plays, coalesced by the parser to the '
    'sentinel ''UNATTRIBUTED'' rather than dropped, verified unique per '
    '(game_id, pos_team_id) with 0 collisions.';
COMMENT ON COLUMN stats.espn_player_receiving.pos_team_id IS
    'ESPN numeric team id, verified equal to ref.teams.id.';

CREATE INDEX IF NOT EXISTS idx_espn_player_receiving_season
    ON stats.espn_player_receiving (season, receiver_player_name);
CREATE INDEX IF NOT EXISTS idx_espn_player_receiving_game
    ON stats.espn_player_receiving (game_id);

GRANT SELECT ON stats.espn_player_receiving TO anon, authenticated;

-- ---------------------------------------------------------------------------
-- stats.espn_player_defense (weekly refresh in-season; advanced defensive
-- players, espn_cfb_adv_defensive_players/adv_defensive_players_{season}.parquet)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stats.espn_player_defense (
    season bigint NOT NULL,
    game_id bigint NOT NULL,
    def_pos_team_id bigint NOT NULL,
    player_name text NOT NULL,
    week bigint,
    def_pos_team text,
    sacks bigint,
    sacks_yards bigint,
    pass_breakups bigint,
    interceptions bigint,
    interceptions_yards bigint,
    forced_fumbles bigint,
    fumble_recoveries bigint,
    fumble_recoveries_yards bigint,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, game_id, def_pos_team_id, player_name)
);

COMMENT ON TABLE stats.espn_player_defense IS
    'ESPN advanced defensive-player box score, one row per defender per '
    'game. Source: sportsdataverse-data espn_cfb_adv_defensive_players '
    'release. interceptions/interceptions_yards are absent (NULL) in '
    'seasons before ESPN added them upstream -- confirmed schema drift, '
    '2005''s file has 12 columns vs 2025''s 14; dlt evolves the pre-created '
    'columns here for whichever season''s richer file loads. Every count '
    'column is NULL (not 0) when that category was not reported for the '
    'player in that game -- NULL-never-0 per this repo''s convention. NO '
    'athlete-id column; NO position column.';
COMMENT ON COLUMN stats.espn_player_defense.player_name IS
    'Free-text player name -- the ONLY player identifier this source '
    'carries. Never null (0 nulls observed). Occasionally carries a raw '
    'ESPN play-text fragment instead of a clean name (a confirmed scrape '
    'artifact, e.g. "by #23 L.Johnson-Burrell at SAC12, End Of Play" on an '
    'interception-return row in the 2025 file) -- kept verbatim, not '
    'cleaned, same posture as ncaa.schedule.team_name''s embedded '
    'annotations.';
COMMENT ON COLUMN stats.espn_player_defense.def_pos_team_id IS
    'ESPN numeric team id, verified equal to ref.teams.id.';

CREATE INDEX IF NOT EXISTS idx_espn_player_defense_season
    ON stats.espn_player_defense (season, player_name);
CREATE INDEX IF NOT EXISTS idx_espn_player_defense_game
    ON stats.espn_player_defense (game_id);

GRANT SELECT ON stats.espn_player_defense TO anon, authenticated;

-- ---------------------------------------------------------------------------
-- stats.espn_play_participants (weekly refresh in-season; per-play
-- participant attribution, espn_cfb_play_participants/
-- play_participants_{season}.parquet)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stats.espn_play_participants (
    season bigint NOT NULL,
    game_id bigint NOT NULL,
    play_id bigint NOT NULL,
    week bigint,
    kicker_player_name text,
    kicker_player_id bigint,
    kicker_player_names text,
    kicker_player_ids text,
    tackler_player_name text,
    tackler_player_id bigint,
    tackler_player_names text,
    tackler_player_ids text,
    returner_player_name text,
    returner_player_id bigint,
    returner_player_names text,
    returner_player_ids text,
    rusher_player_name text,
    rusher_player_id bigint,
    rusher_player_names text,
    rusher_player_ids text,
    passer_player_name text,
    passer_player_id bigint,
    passer_player_names text,
    passer_player_ids text,
    receiver_player_name text,
    receiver_player_id bigint,
    receiver_player_names text,
    receiver_player_ids text,
    punter_player_name text,
    punter_player_id bigint,
    punter_player_names text,
    punter_player_ids text,
    assisted_by_player_name text,
    assisted_by_player_id bigint,
    assisted_by_player_names text,
    assisted_by_player_ids text,
    penalized_player_name text,
    penalized_player_id bigint,
    penalized_player_names text,
    penalized_player_ids text,
    scorer_player_name text,
    scorer_player_id bigint,
    scorer_player_names text,
    scorer_player_ids text,
    pat_scorer_player_name text,
    pat_scorer_player_id bigint,
    pat_scorer_player_names text,
    pat_scorer_player_ids text,
    sacked_by_player_name text,
    sacked_by_player_id bigint,
    sacked_by_player_names text,
    sacked_by_player_ids text,
    pass_defender_player_name text,
    pass_defender_player_id bigint,
    pass_defender_player_names text,
    pass_defender_player_ids text,
    recoverer_player_name text,
    recoverer_player_id bigint,
    recoverer_player_names text,
    recoverer_player_ids text,
    fumbler_player_name text,
    fumbler_player_id bigint,
    fumbler_player_names text,
    fumbler_player_ids text,
    forced_by_player_name text,
    forced_by_player_id bigint,
    forced_by_player_names text,
    forced_by_player_ids text,
    pat_passer_player_name text,
    pat_passer_player_id bigint,
    pat_passer_player_names text,
    pat_passer_player_ids text,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, game_id, play_id)
);

COMMENT ON TABLE stats.espn_play_participants IS
    'ESPN per-play participant attribution (kicker/tackler/rusher/passer/'
    'receiver/... for each play), one row per play. Source: '
    'sportsdataverse-data espn_cfb_play_participants release. NO team '
    'id/name column at all -- attributing a participant to a team requires '
    'a separate join back to a pbp table by (game_id, play_id), not '
    'provided here. "fumbler" is a participant type added upstream between '
    '2014 (16 types, 68 columns) and 2025 (17 types, 72 columns) -- '
    'confirmed schema drift, dlt evolves the pre-created columns here.';
COMMENT ON COLUMN stats.espn_play_participants.game_id IS
    'ESPN event id, equal to core.games.id.';
COMMENT ON COLUMN stats.espn_play_participants.kicker_player_id IS
    'Shared CFBD/ESPN athlete-id namespace (verified: clean numeric '
    'strings in source, 0 parse failures across 154653 rows in the 2025 '
    'file) -- joins to core.roster.id, per the PFF/SIS-era join-spine '
    'convention (migration 050). The same note applies to every other '
    '*_player_id column in this table (tackler_player_id, '
    'returner_player_id, ... pat_passer_player_id) -- NOT repeated on '
    'each one individually.';
COMMENT ON COLUMN stats.espn_play_participants.kicker_player_names IS
    'Python repr() string literal (e.g. "[''Ben Barnes'']" or "[]"), NOT '
    'JSON or a Postgres array -- every occurrence of this participant type '
    'on the play, in ESPN''s order (the scalar kicker_player_name/'
    'kicker_player_id columns carry only the first occurrence). A '
    'consumer needing the full list must ast.literal_eval this column '
    'client-side. The same note applies to every other *_player_names/'
    '*_player_ids column in this table.';

CREATE INDEX IF NOT EXISTS idx_espn_play_participants_game
    ON stats.espn_play_participants (game_id);
CREATE INDEX IF NOT EXISTS idx_espn_play_participants_season
    ON stats.espn_play_participants (season);

GRANT SELECT ON stats.espn_play_participants TO anon, authenticated;
