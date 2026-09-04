"""Flat-file ingestion framework: registry, contracts, and dlt source builder.

This module is the contract surface for the flat-file subsystem
(docs/brainstorms/2026-07-23-warehouse-extension-data-sources.md). The pieces:

- ``FlatFileSpec`` -- declarative registry entry per source (what to fetch, how
  to parse, where to load, on what cadence).
- ``ParseContext`` -- everything a pure parser is allowed to know about a fetch.
- ``REGISTRY`` -- the five launch sources (massey, nflverse_combine,
  nflverse_draft, sbr, availability) plus the sportsdataverse-data additions
  (sdv_team_xwalk, sdv_game_xwalk, sdv_fpi_weekly, sdv_ratings_weekly), the
  NCAA bundle (ncaa_schedule, ncaa_teams, ncaa_rosters, ncaa_linescores,
  ncaa_player_stats, ncaa_team_stats, ncaa_pbp), the ESPN player-grain
  bundle (espn_player_passing, espn_player_rushing, espn_player_receiving,
  espn_player_defense, espn_play_participants), and the PFF Premium Stats
  manual-CSV bundle (pff_passing_summary, pff_receiving_summary,
  pff_rushing_summary, pff_offense_blocking, pff_defense_summary -- see the
  registry comment above their entries for the manual-drop/--season
  mechanics).
- ``build_flat_file_source()`` -- wraps parsed rows into a ``@dlt.source`` whose
  resources merge into pre-created tables (migration 041).

Parser contract (modules under ``flatfile_parsers/``): a pure function
``parse(raw: bytes, ctx: ParseContext) -> Iterator[dict]`` -- no I/O, no DB.
Each yielded dict targets ``spec.table`` unless it carries the reserved key
``"_table"`` naming an alternate table (used for Massey's per-system child
rows). Parsers must fail loud on structural surprises (raise
``ParserStructureError``) rather than guess, and raise ``StaleSnapshotError``
when the file's self-declared date shows it is not current-season data (Massey
serves last season's final snapshot during the offseason).

Team-name crosswalk: when ``spec.uses_xwalk``, the framework resolves each
field in ``spec.xwalk_fields`` from the source's spelling to the exact CFBD
full-name string used across core.games/ref.teams. Rows with unmapped names
are dropped from the load but counted; if the unmapped fraction exceeds
``spec.unmapped_fail_rate`` the whole source fails (``UnmappedNamesError``) so
new name variants surface in CI instead of silently dropping rows.

Per-season multi-file sources (B6a): sportsdataverse-data publishes one
parquet per season per dataset under a single release tag (e.g.
``cfb_teams_crosswalk_2025.parquet``, ``..._2026.parquet``, ... all under the
``cfb_crosswalk`` tag; same shape for the ``ncaa_mfb_*`` tags). This differs
from a single stable URL (massey/nflverse) in three ways this module and
``scripts/load_flat_files.py`` handle together:

- **``url_template`` replaces ``fetch_url``.** A season-parameterized spec
  sets ``url_template`` (a URL string with a literal ``{season}`` slot) and
  leaves ``fetch_url=None``; ``resolve_fetch_url(spec, season)`` picks
  whichever is set. Single-file specs are untouched -- they keep using
  ``fetch_url`` and never set ``url_template``.
- **Ledger keying is per season, not per source** (``ledger_key()``). A
  season-parameterized spec's ledger ``source`` value is
  ``f"{name}:{season}"``, not the bare registry name. Reason: `is_due()`'s
  weekly-cadence check ("was *this* file loaded in the last 6 days?") must
  answer for the *current* season's file specifically -- if every season
  shared one ledger name, a same-week historical backfill
  (``--season 2019``) would stamp a fresh ``loaded_at`` under that shared
  name and make the current season's file look "recently loaded," silently
  suppressing its weekly due-check for up to 6 days. Single-file specs are
  unaffected (``ledger_key()`` returns the bare ``spec.name`` when
  ``url_template`` is unset, exactly the pre-B6a behavior).
- **404 fallback for an unpublished current season**
  (``spec.fallback_latest=True``; implemented in ``load_flat_files.py``'s
  ``_fetch_seasoned``). The season a ``--due`` run resolves via
  ``season_for_date(today)`` often has no file yet -- college football
  seasons start in late August, and CFBD/sportsdataverse's own ingest runs
  lag the season rollover by anywhere from days to a few weeks (this is
  literally true at the time this was written: today resolves to season
  2026, but sportsdataverse's most recent published asset for every
  ``ncaa_mfb_*`` and ``cfb_*`` tag is still 2025). A plain 404 there must not
  fail the run: when the spec opts in and the season came from "today" (not
  from an explicit ``--season``), the driver retries up to
  ``FALLBACK_MAX_STEPS`` seasons back and loads whichever most-recent one
  succeeds, recording it under *that* season's own ledger key -- so the
  current season's key stays "never loaded" and next week's ``--due`` run
  tries again, self-healing the moment the real file appears (same idea as
  ``PRESEASON_INPUT_SOURCES``' empty-response self-heal in
  ``load_season.py``, one layer down at the HTTP-404 level instead of an
  empty-but-200 API response). An explicit ``--season YYYY`` request never
  falls back -- a 404 there is a loud ``SeasonNotPublishedError``, not a
  silent substitution of a different season's data for the one asked for.
- **``--due`` semantics stay season-scalar.** The driver resolves exactly
  one season (today's, or an explicit override) and checks/fetches only
  that season's file; a full historical backfill is one ``--season YYYY``
  invocation per season, not a new multi-season CLI flag -- there is
  already a way to ask for one season at a time, and looping over years is
  the caller's job (a shell loop or workflow step), not this module's.
"""

import importlib
from dataclasses import dataclass
from datetime import date

import dlt
from dlt.sources import DltSource

# Reserved row key a parser may set to direct a row to an alternate table.
TABLE_KEY = "_table"


class ParserStructureError(Exception):
    """The file's structure does not match what the parser expects.

    Raised instead of guessing: a Massey section marker missing, an SBR row
    pair that doesn't pair, an unexpected parquet column set.
    """


class StaleSnapshotError(Exception):
    """The file self-identifies as data from a previous season.

    The driver maps this to a ``no_op_offseason`` outcome (not a failure).
    """


class SeasonNotPublishedError(Exception):
    """A season-parameterized source's file 404'd for the requested season
    and (when ``fallback_latest`` opted in) every fallback season tried.

    Raised by ``scripts/load_flat_files.py``'s ``_fetch_seasoned``, never by
    a parser. The driver maps this to a ``not_published`` outcome (not a
    failure) -- see the module docstring's "404 fallback" section.
    """


class SeasonFingerprintError(Exception):
    """The file's contents provably contradict the season it was claimed as.

    PFF Premium Stats exports carry no season column and identical filenames
    across seasons (three observed upload batches had three different
    browser-numbering orders), so the operator-supplied ``--season`` is
    verified against FBS-membership facts found in the file (marker teams,
    team counts). Raised by the pff parsers; a hard failure (misfiled uploads
    are silent data corruption otherwise), never auto-corrected.
    """


class UnmappedNamesError(Exception):
    """Too many team names failed crosswalk resolution.

    Carries the distinct unmapped names so the failure output is actionable.
    """

    def __init__(self, source: str, unmapped: dict[str, int], total_rows: int):
        self.source = source
        self.unmapped = unmapped
        self.total_rows = total_rows
        names = ", ".join(sorted(unmapped)[:20])
        super().__init__(
            f"{source}: {len(unmapped)} unmapped team names over {total_rows} rows: {names}"
        )


@dataclass(frozen=True)
class ParseContext:
    """Facts about a single fetch, passed to the pure parser.

    Attributes:
        source: Registry name of the source being parsed.
        snapshot_date: Date stamp for snapshot-grain tables (fetch date).
        season: Season hint (e.g. from --season or inferred); parsers may
            override from file contents when the file self-declares its season.
        source_url: Where the bytes came from (URL or local path), for context
            in error messages only -- parsers must not fetch anything.
        file_name: Basename of the fetched file (season inference for SBR).
    """

    source: str
    snapshot_date: date
    season: int | None = None
    source_url: str | None = None
    file_name: str | None = None


@dataclass(frozen=True)
class FlatFileSpec:
    """Declarative registry entry for one flat-file source.

    Attributes:
        name: Registry key; also the ledger `source` value and the
            ``--source`` CLI argument.
        parser: Dotted ref "<module>.<function>" under
            ``src.pipelines.sources.flatfile_parsers`` (e.g. "massey.parse",
            "nflverse.parse_combine"). Ignored for kind="archiver".
        schema: Target Postgres schema (dlt ``dataset_name``).
        table: Main target table (pre-created by migration 041).
        primary_key: Merge key of the main table.
        cadence: "weekly" | "annual" | "manual". Manual sources never run
            under ``--due``; weekly sources are also gated on in-season months.
        fetch_url: Default URL; None means the source requires ``--file``, an
            ``url_template``, or does its own discovery (archiver).
        url_template: URL string with a literal ``{season}`` slot, for
            sources that publish one file per season under one release tag
            (see the module docstring's "Per-season multi-file sources"
            section). Mutually exclusive with ``fetch_url`` in practice --
            ``resolve_fetch_url()`` prefers this when set.
        fallback_latest: When True and ``url_template`` is set, a 404 on the
            resolved season falls back to earlier seasons (bounded by
            ``load_flat_files.FALLBACK_MAX_STEPS`` and ``min_season``) --
            but only when that season was not explicitly requested via
            ``--season``. No effect without ``url_template``.
        min_season: Floor for fallback probing (and documentation of the
            source's actual historical coverage). None means no known floor.
        write_disposition: dlt disposition for the main + child resources.
        child_table: Optional secondary table populated via rows tagged with
            ``_table`` (Massey per-system ranks).
        child_primary_key: Merge key of the child table.
        uses_xwalk: Whether team-name resolution applies.
        xwalk_fields: Row fields containing source-spelled team names.
        keep_source_names: When True, the original spelling of each xwalk
            field F is preserved in column ``{F}_source`` (SBR).
        unmapped_fail_rate: Max tolerated fraction of rows with unmapped
            names before the source fails loud. 0.0 means any unmapped row
            fails the whole load (the pff posture: an unmapped team name
            means realignment and needs a human, never a silent drop).
        kind: "dlt" (fetch -> parse -> dlt merge) or "archiver" (module runs
            its own discovery + raw-bytes archival, bypassing dlt).
        requires_season: The source's files carry no self-declared season at
            all (PFF exports), so a load must be told the season explicitly
            -- the driver rejects a ``--file`` load without ``--season`` --
            and the ledger keys per ``{name}:{season}`` (same as
            ``url_template`` specs) so each season's file hash-skips
            independently.
        xwalk_map: Alternative store for the team-name resolver: a
            ``(schema_qualified_table, source_name_col, cfbd_name_col)``
            triple loaded via ``XwalkResolver.load_map_table`` instead of
            ``ref.team_name_xwalk`` keyed by ``spec.name`` (pff commits its
            map as ``pff.team_map``, seeded by migration 061).
        xwalk_resolved_field: When set, the crosswalk writes the resolved
            CFBD name to this NEW row field and leaves the source field
            untouched (pff keeps ``team_name`` verbatim and adds
            ``school``), instead of the default overwrite-in-place (+
            optional ``{field}_source`` copy) behavior. Only meaningful with
            a single entry in ``xwalk_fields``.
    """

    name: str
    parser: str
    schema: str
    table: str
    primary_key: tuple[str, ...]
    cadence: str
    fetch_url: str | None = None
    url_template: str | None = None
    fallback_latest: bool = False
    min_season: int | None = None
    write_disposition: str = "merge"
    child_table: str | None = None
    child_primary_key: tuple[str, ...] = ()
    uses_xwalk: bool = False
    xwalk_fields: tuple[str, ...] = ()
    keep_source_names: bool = False
    unmapped_fail_rate: float = 0.02
    kind: str = "dlt"
    requires_season: bool = False
    xwalk_map: tuple[str, str, str] | None = None
    xwalk_resolved_field: str | None = None


REGISTRY: dict[str, FlatFileSpec] = {
    "massey": FlatFileSpec(
        name="massey",
        parser="massey.parse",
        schema="ratings",
        table="massey_composite",
        primary_key=("season", "snapshot_date", "team"),
        cadence="weekly",
        fetch_url="https://masseyratings.com/cf/compare.csv",
        child_table="massey_system_ratings",
        child_primary_key=("season", "snapshot_date", "team", "system_code"),
        uses_xwalk=True,
        xwalk_fields=("team",),
    ),
    "nflverse_combine": FlatFileSpec(
        name="nflverse_combine",
        parser="nflverse.parse_combine",
        schema="draft",
        table="combine",
        primary_key=("season", "player_name", "pos"),
        cadence="annual",
        fetch_url=(
            "https://github.com/nflverse/nflverse-data/releases/download/combine/combine.parquet"
        ),
    ),
    "nflverse_draft": FlatFileSpec(
        name="nflverse_draft",
        parser="nflverse.parse_draft_picks",
        schema="draft",
        table="nflverse_draft_picks",
        primary_key=("season", "round", "pick"),
        cadence="annual",
        fetch_url=(
            "https://github.com/nflverse/nflverse-data/releases/download/"
            "draft_picks/draft_picks.parquet"
        ),
    ),
    "sbr": FlatFileSpec(
        name="sbr",
        parser="sbr.parse",
        schema="betting",
        table="sbr_historical",
        primary_key=("season", "game_date", "home_team", "away_team"),
        cadence="manual",
        fetch_url=None,  # manually downloaded Excel files via --file
        uses_xwalk=True,
        xwalk_fields=("home_team", "away_team"),
        keep_source_names=True,
        unmapped_fail_rate=0.05,
    ),
    "availability": FlatFileSpec(
        name="availability",
        parser="availability.archive",
        schema="raw",
        table="availability_reports",
        primary_key=("sha256",),
        cadence="weekly",
        kind="archiver",
    ),
    # sportsdataverse-data (B2 items 1-2, migrated to the url_template
    # mechanism in B6a): nflverse-pattern clones -- PK-verified,
    # ParserStructureError on structural surprise -- but NOT cumulative like
    # nflverse's combine/draft_picks: cfbfastR-cfb-data publishes one asset
    # per season (`{dataset}_{season}.parquet`) under one release tag, so
    # these use `url_template` + `fallback_latest=True` instead of a pinned
    # `fetch_url` (this removed the B2-era manual-bump-to-`_2026` debt -- see
    # flatfile_parsers/sportsdataverse.py's module docstring for the full B2
    # finding, including why there is deliberately no `sdv_player_xwalk`
    # entry (no such asset exists in the real release) and why none of these
    # four use the team-name xwalk (ids already line up with CFBD's numeric
    # id namespace -- verified against the live warehouse 2026-08-29:
    # ref.teams.id 333=Alabama / 2483=Oregon match ESPN ids, 2024 core.games
    # ids are ESPN event ids (incl. 401632103), and the ESPN-style athlete id
    # 5083552 exists in core.roster).
    "sdv_team_xwalk": FlatFileSpec(
        name="sdv_team_xwalk",
        parser="sportsdataverse.parse_team_xwalk",
        schema="ref",
        table="team_id_xwalk",
        # PK is (season, xwalk_key), not (season, norm_key): norm_key is not
        # unique upstream (the "roosevelt lakers" collision -- two distinct
        # schools, different espn_team_id) and would otherwise silently drop
        # one under dlt merge. xwalk_key is a parser-derived tiebreak column
        # (norm_key + the first non-null of espn/fox/yahoo_team_id) -- see
        # parse_team_xwalk's docstring.
        primary_key=("season", "xwalk_key"),
        cadence="weekly",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_crosswalk/cfb_teams_crosswalk_{season}.parquet"
        ),
        fallback_latest=True,
    ),
    "sdv_game_xwalk": FlatFileSpec(
        name="sdv_game_xwalk",
        parser="sportsdataverse.parse_game_xwalk",
        schema="ref",
        table="game_id_xwalk",
        primary_key=("season", "matchup_key", "yahoo_date"),
        cadence="weekly",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_crosswalk/cfb_schedule_crosswalk_{season}.parquet"
        ),
        fallback_latest=True,
    ),
    "sdv_fpi_weekly": FlatFileSpec(
        name="sdv_fpi_weekly",
        parser="sportsdataverse.parse_fpi_weekly",
        schema="ratings",
        # Renamed from fpi_weekly (schema-architect review): this is ESPN's
        # FPI at weekly-snapshot grain, distinct provenance from CFBD's own
        # season-grain ratings.fpi_ratings -- the table name must say so.
        table="espn_fpi_weekly",
        primary_key=("season", "season_type", "week", "team_id"),
        cadence="weekly",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_fpi_weekly/cfb_fpi_weekly_{season}.parquet"
        ),
        fallback_latest=True,
    ),
    "sdv_ratings_weekly": FlatFileSpec(
        name="sdv_ratings_weekly",
        parser="sportsdataverse.parse_ratings_weekly",
        schema="ratings",
        # Renamed from external_weekly (schema-architect review) to match
        # the repo's source-prefix table-naming convention (massey_composite,
        # nflverse_draft_picks).
        table="sdv_ratings_weekly",
        primary_key=("season", "through_week", "team_id"),
        cadence="weekly",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_ratings_weekly/cfb_ratings_weekly_{season}.parquet"
        ),
        fallback_latest=True,
    ),
    # NCAA (stats.ncaa.org) bundle, via sportsdataverse-data's ncaa_mfb_*
    # release tags (B6a). Own `ncaa` Postgres schema -- migration 053 --
    # deliberately ungranted (no anon/authenticated access): stats.ncaa.org's
    # own id space (contest/team/player ids below) is DISJOINT from
    # CFBD/ESPN's, and unlike the sdv_* crosswalks above there is NO verified
    # id equivalence to lean on here, so the schema stays reachable only by
    # the pipeline's service-role connection until a deliberate crosswalk/
    # exposure decision is made. See flatfile_parsers/ncaa.py's module
    # docstring for the full column/PK findings, including that
    # `ncaa_team_id` and `ncaa_player_id` are RE-ISSUED EVERY SEASON (not
    # stable cross-season identifiers -- verified: Alabama's ncaa_team_id is
    # 62682 in the 2013 file and 606070 in 2025; a specific Alabama player
    # confirmed on both the 2024 and 2025 rosters carries two different
    # ncaa_player_id values). min_season=2013: stats.ncaa.org has no football
    # data before the 2013 season (confirmed via the ncaa_mfb_pbp release
    # notes and by every ncaa_mfb_* tag's asset list starting at `_2013`).
    # pbp is included (not commented out): the 2025 season file is ~11.8MB,
    # far under the ~200MB single-file scope-call threshold.
    "ncaa_schedule": FlatFileSpec(
        name="ncaa_schedule",
        parser="ncaa.parse_schedule",
        schema="ncaa",
        table="schedule",
        primary_key=("season", "ncaa_contest_id", "ncaa_team_id"),
        cadence="weekly",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "ncaa_mfb_schedule/ncaa_mfb_schedule_{season}.parquet"
        ),
        fallback_latest=True,
        min_season=2013,
    ),
    "ncaa_teams": FlatFileSpec(
        name="ncaa_teams",
        parser="ncaa.parse_teams",
        schema="ncaa",
        table="teams",
        primary_key=("season", "ncaa_team_id"),
        cadence="annual",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "ncaa_mfb_teams/ncaa_mfb_teams_{season}.parquet"
        ),
        fallback_latest=True,
        min_season=2013,
    ),
    "ncaa_rosters": FlatFileSpec(
        name="ncaa_rosters",
        parser="ncaa.parse_rosters",
        schema="ncaa",
        table="rosters",
        primary_key=("season", "ncaa_team_id", "ncaa_player_id"),
        cadence="annual",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "ncaa_mfb_rosters/ncaa_mfb_rosters_{season}.parquet"
        ),
        fallback_latest=True,
        min_season=2013,
    ),
    "ncaa_linescores": FlatFileSpec(
        name="ncaa_linescores",
        parser="ncaa.parse_linescores",
        schema="ncaa",
        table="linescores",
        # No team-id column in this file at all (only a team name) -- see
        # parse_linescores' docstring for why (contest_id, team_name, period)
        # is still a safe PK (a contest's two team names never collide with
        # each other within that same contest_id).
        primary_key=("season", "ncaa_contest_id", "team_name", "period"),
        cadence="weekly",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "ncaa_mfb_linescore/ncaa_mfb_linescore_{season}.parquet"
        ),
        fallback_latest=True,
        min_season=2013,
    ),
    "ncaa_player_stats": FlatFileSpec(
        name="ncaa_player_stats",
        parser="ncaa.parse_player_stats",
        schema="ncaa",
        table="player_stats",
        # row_seq disambiguates a real upstream quirk: stats.ncaa.org's
        # per-category player box score is itself a concatenation of several
        # physical sub-tables (e.g. category="other" holds both "total
        # offense" and "kickoff returns" as separate rows for the same
        # player) -- see parse_player_stats' docstring.
        primary_key=(
            "season",
            "ncaa_contest_id",
            "ncaa_team_id",
            "player_number",
            "category",
            "row_seq",
        ),
        cadence="weekly",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "ncaa_mfb_player_stats/ncaa_mfb_player_stats_{season}.parquet"
        ),
        fallback_latest=True,
        min_season=2013,
    ),
    "ncaa_team_stats": FlatFileSpec(
        name="ncaa_team_stats",
        parser="ncaa.parse_team_stats",
        schema="ncaa",
        table="team_stats",
        # row_seq disambiguates a confirmed upstream data-quality quirk in
        # overtime games (an "1stOT"-style label leaks into the `stat` column
        # instead of `period`, producing genuine duplicate (contest,
        # category, stat, period) keys) -- see parse_team_stats' docstring.
        primary_key=("season", "ncaa_contest_id", "category", "stat", "period", "row_seq"),
        cadence="weekly",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "ncaa_mfb_team_stats/ncaa_mfb_team_stats_{season}.parquet"
        ),
        fallback_latest=True,
        min_season=2013,
    ),
    "ncaa_pbp": FlatFileSpec(
        name="ncaa_pbp",
        parser="ncaa.parse_pbp",
        schema="ncaa",
        table="pbp",
        primary_key=("season", "ncaa_contest_id", "drive_number", "play_number"),
        cadence="weekly",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "ncaa_mfb_pbp/ncaa_mfb_pbp_{season}.parquet"
        ),
        fallback_latest=True,
        min_season=2013,
    ),
    # ESPN player-grain bundle (B6b), via sportsdataverse-data's
    # espn_cfb_adv_*/espn_cfb_play_participants release tags. Own tables in
    # the EXISTING `stats` schema (already USAGE-granted) -- no new `espn`
    # schema: the schema-architect verdict is that ESPN's numeric ids ARE
    # CFBD's (verified against the live warehouse 2026-08-29). Exact asset
    # URLs (e.g. `adv_passing_{season}.parquet`, NOT
    # `espn_cfb_adv_passing_{season}.parquet`) were read out of the real
    # `sportsdataverse` 0.1.3 PyPI package's generated cfb_loaders.py, then
    # verified live -- see flatfile_parsers/espn.py's module docstring for
    # the full finding, including why there is deliberately no
    # `espn_pbp_2002_2003` entry here (the dataset's own minimum season is
    # 2004, verified live -- the task's 2002-03 gap-fill premise does not
    # hold) and why none of the four adv_* sources carries an athlete id or
    # position column at all (only espn_play_participants does, in its
    # per-participant-type `{type}_player_id` columns).
    "espn_player_passing": FlatFileSpec(
        name="espn_player_passing",
        parser="espn.parse_player_passing",
        schema="stats",
        table="espn_player_passing",
        primary_key=("season", "game_id", "pos_team_id", "passer_player_name"),
        cadence="weekly",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "espn_cfb_adv_passing/adv_passing_{season}.parquet"
        ),
        fallback_latest=True,
        min_season=2004,
    ),
    "espn_player_rushing": FlatFileSpec(
        name="espn_player_rushing",
        parser="espn.parse_player_rushing",
        schema="stats",
        table="espn_player_rushing",
        primary_key=("season", "game_id", "pos_team_id", "rusher_player_name"),
        cadence="weekly",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "espn_cfb_adv_rushing/adv_rushing_{season}.parquet"
        ),
        fallback_latest=True,
        min_season=2004,
    ),
    "espn_player_receiving": FlatFileSpec(
        name="espn_player_receiving",
        parser="espn.parse_player_receiving",
        schema="stats",
        table="espn_player_receiving",
        primary_key=("season", "game_id", "pos_team_id", "receiver_player_name"),
        cadence="weekly",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "espn_cfb_adv_receiving/adv_receiving_{season}.parquet"
        ),
        fallback_latest=True,
        min_season=2004,
    ),
    "espn_player_defense": FlatFileSpec(
        name="espn_player_defense",
        parser="espn.parse_player_defense",
        schema="stats",
        table="espn_player_defense",
        primary_key=("season", "game_id", "def_pos_team_id", "player_name"),
        cadence="weekly",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "espn_cfb_adv_defensive_players/adv_defensive_players_{season}.parquet"
        ),
        fallback_latest=True,
        min_season=2004,
    ),
    "espn_play_participants": FlatFileSpec(
        name="espn_play_participants",
        parser="espn.parse_play_participants",
        schema="stats",
        table="espn_play_participants",
        primary_key=("season", "game_id", "play_id"),
        cadence="weekly",
        url_template=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "espn_cfb_play_participants/play_participants_{season}.parquet"
        ),
        fallback_latest=True,
        min_season=2014,
    ),
    # PFF Premium Stats manual-CSV lane (design:
    # docs/brainstorms/2026-09-01-pff-plus-api.md section 4 option A1).
    # Hand-exported, auth-gated browser downloads -- no fetch URL exists, so
    # these are --file-only manual sources like sbr, loaded one
    # (family, season) at a time:
    #   scripts/load_flat_files.py --source pff_passing_summary \
    #       --season 2024 --file <csv>
    # requires_season: PFF exports carry NO season column and identical
    # filenames across seasons; the operator's --season is mandatory and is
    # verified by the parser's FBS-membership fingerprint guard
    # (SeasonFingerprintError on a provable contradiction -- see
    # flatfile_parsers/pff.py). Ledger keys per {name}:{season} so each
    # season's file hash-skips independently (a byte-identical re-drop of an
    # already-loaded file is a skipped_hash, the observed duplicate-upload
    # case). Team names resolve through the committed pff.team_map
    # (migration 061) into a NEW `school` column, keeping PFF's verbatim
    # team_name; unmapped_fail_rate=0.0 -- any unmapped name is realignment
    # and fails the whole load. Tables/columns: migration 061, derived from
    # the real 2023-2025 exports. Licensed data: the pff schema is
    # deliberately ungranted and outside the api/public contract.
    "pff_passing_summary": FlatFileSpec(
        name="pff_passing_summary",
        parser="pff.parse_passing_summary",
        schema="pff",
        table="passing_summary",
        primary_key=("player_id", "season"),
        cadence="manual",
        min_season=2014,
        uses_xwalk=True,
        xwalk_fields=("team_name",),
        unmapped_fail_rate=0.0,
        requires_season=True,
        xwalk_map=("pff.team_map", "pff_team_name", "cfbd_school"),
        xwalk_resolved_field="school",
    ),
    "pff_receiving_summary": FlatFileSpec(
        name="pff_receiving_summary",
        parser="pff.parse_receiving_summary",
        schema="pff",
        table="receiving_summary",
        primary_key=("player_id", "season"),
        cadence="manual",
        min_season=2014,
        uses_xwalk=True,
        xwalk_fields=("team_name",),
        unmapped_fail_rate=0.0,
        requires_season=True,
        xwalk_map=("pff.team_map", "pff_team_name", "cfbd_school"),
        xwalk_resolved_field="school",
    ),
    "pff_rushing_summary": FlatFileSpec(
        name="pff_rushing_summary",
        parser="pff.parse_rushing_summary",
        schema="pff",
        table="rushing_summary",
        primary_key=("player_id", "season"),
        cadence="manual",
        min_season=2014,
        uses_xwalk=True,
        xwalk_fields=("team_name",),
        unmapped_fail_rate=0.0,
        requires_season=True,
        xwalk_map=("pff.team_map", "pff_team_name", "cfbd_school"),
        xwalk_resolved_field="school",
    ),
    "pff_offense_blocking": FlatFileSpec(
        name="pff_offense_blocking",
        parser="pff.parse_offense_blocking",
        schema="pff",
        table="offense_blocking",
        primary_key=("player_id", "season"),
        cadence="manual",
        min_season=2014,
        uses_xwalk=True,
        xwalk_fields=("team_name",),
        unmapped_fail_rate=0.0,
        requires_season=True,
        xwalk_map=("pff.team_map", "pff_team_name", "cfbd_school"),
        xwalk_resolved_field="school",
    ),
    "pff_defense_summary": FlatFileSpec(
        name="pff_defense_summary",
        parser="pff.parse_defense_summary",
        schema="pff",
        table="defense_summary",
        primary_key=("player_id", "season"),
        cadence="manual",
        min_season=2014,
        uses_xwalk=True,
        xwalk_fields=("team_name",),
        unmapped_fail_rate=0.0,
        requires_season=True,
        xwalk_map=("pff.team_map", "pff_team_name", "cfbd_school"),
        xwalk_resolved_field="school",
    ),
}

# Months in which weekly sources are considered live (Aug preseason polls
# through the January title game). verify_load's is_in_season() is the
# strict {9..12, 1} gate; loading starts a month earlier to catch preseason
# composite snapshots.
LOAD_SEASON_MONTHS = frozenset({8, 9, 10, 11, 12, 1})


def season_for_date(d: date) -> int:
    """Map a calendar date to its CFB season year (Aug-Dec -> year, Jan-Jul -> year-1)."""
    return d.year if d.month >= 8 else d.year - 1


def resolve_fetch_url(spec: FlatFileSpec, season: int) -> str | None:
    """The URL to fetch for one (spec, season) pair.

    ``url_template`` wins when set (season-parameterized sources);
    otherwise falls back to the single-file ``fetch_url`` (None for
    archiver/--file-only specs, unchanged from pre-B6a behavior).
    """
    if spec.url_template:
        return spec.url_template.format(season=season)
    return spec.fetch_url


def ledger_key(spec: FlatFileSpec, season: int) -> str:
    """The ledger `source` value for one (spec, season) fetch attempt.

    Single-file specs key on the bare registry name, exactly as before B6a.
    Season-parameterized specs (``url_template`` set, or ``requires_season``
    manual-drop specs whose files carry no season of their own) key on
    ``f"{name}:{season}"`` -- see the module docstring's "Per-season
    multi-file sources" section for why a shared name would corrupt
    `is_due()`'s weekly-cadence semantics; for ``requires_season`` specs the
    per-season key is what lets each season's file hash-skip independently.
    """
    if spec.url_template or spec.requires_season:
        return f"{spec.name}:{season}"
    return spec.name


def unmapped_gate(total_rows: int, unmapped_rows: int, threshold: float) -> bool:
    """Return True when the unmapped fraction breaches the threshold (=> fail).

    Zero-row loads never trip the gate (nothing was dropped); any unmapped
    row trips a zero threshold.
    """
    if total_rows <= 0 or unmapped_rows <= 0:
        return False
    return (unmapped_rows / total_rows) > threshold


def build_flat_file_source(
    spec: FlatFileSpec,
    raw: bytes,
    ctx: ParseContext,
    resolver=None,
) -> DltSource:
    """Materialize parsed rows into a dlt source ready for ``pipeline.run()``.

    Implemented in T3. Behavior contract:
    1. Resolve ``spec.parser`` under ``flatfile_parsers`` and run it over
       ``raw`` (materialize -- these files are small).
    2. If ``spec.uses_xwalk``: resolve each ``xwalk_fields`` value through
       ``resolver`` (an ``XwalkResolver``); drop rows with any unmapped field,
       preserving originals to ``{field}_source`` when ``keep_source_names``;
       raise ``UnmappedNamesError`` if ``unmapped_gate(...)`` trips.
    3. Split rows by the reserved ``_table`` key (main table vs child_table),
       strip the key, and return a ``@dlt.source`` with one ``@dlt.resource``
       per table using ``spec.write_disposition`` and the matching primary key.

    Args:
        spec: Registry entry (must be kind="dlt").
        raw: Fetched file bytes.
        ctx: Parse context handed through to the parser.
        resolver: XwalkResolver bound to ``spec.name`` (required when
            ``spec.uses_xwalk``).
    """
    parser = resolve_parser(spec.parser)
    rows = list(parser(raw, ctx))

    if spec.uses_xwalk:
        if resolver is None:
            raise ValueError(f"{spec.name}: uses_xwalk=True requires a resolver")

        total_rows = len(rows)
        unmapped_rows = 0
        kept_rows = []
        for row in rows:
            row_unmapped = False
            resolved_fields: dict[str, tuple[object, str]] = {}
            for field in spec.xwalk_fields:
                original = row[field]
                resolved = resolver.resolve(original)
                if resolved is None:
                    row_unmapped = True
                else:
                    resolved_fields[field] = (original, resolved)

            if row_unmapped:
                unmapped_rows += 1
                continue

            for field, (original, resolved) in resolved_fields.items():
                if spec.xwalk_resolved_field:
                    # pff posture: source spelling stays verbatim in `field`,
                    # the CFBD name lands in a new column (e.g. `school`).
                    row[spec.xwalk_resolved_field] = resolved
                else:
                    if spec.keep_source_names:
                        row[f"{field}_source"] = original
                    row[field] = resolved
            kept_rows.append(row)

        rows = kept_rows

        if unmapped_gate(total_rows, unmapped_rows, spec.unmapped_fail_rate):
            raise UnmappedNamesError(spec.name, resolver.misses, total_rows)

    main_rows: list[dict] = []
    child_rows: list[dict] = []
    for row in rows:
        table = row.pop(TABLE_KEY, None)
        if table is None or table == spec.table:
            main_rows.append(row)
        elif table == spec.child_table:
            child_rows.append(row)
        else:
            raise ParserStructureError(
                f"{spec.name}: row names unknown table {table!r} "
                f"(expected {spec.table!r} or {spec.child_table!r})"
            )

    resources = []
    if main_rows:
        resources.append(
            dlt.resource(
                main_rows,
                name=spec.table,
                write_disposition=spec.write_disposition,
                primary_key=list(spec.primary_key),
            )
        )
    if child_rows:
        resources.append(
            dlt.resource(
                child_rows,
                name=spec.child_table,
                write_disposition=spec.write_disposition,
                primary_key=list(spec.child_primary_key),
            )
        )

    def _resources() -> list:
        return resources

    source_factory = dlt.source(_resources, name=f"flatfile_{spec.name}")
    return source_factory()


def resolve_parser(parser_ref: str):
    """Import "<module>.<function>" under flatfile_parsers and return the callable.

    Implemented in T3.
    """
    if "." not in parser_ref:
        raise ValueError(f"Invalid parser ref (expected '<module>.<function>'): {parser_ref!r}")

    module_name, func_name = parser_ref.rsplit(".", 1)
    try:
        module = importlib.import_module(f"src.pipelines.sources.flatfile_parsers.{module_name}")
    except ImportError as e:
        raise ValueError(f"Invalid parser ref (module not found): {parser_ref!r}") from e

    try:
        return getattr(module, func_name)
    except AttributeError as e:
        raise ValueError(f"Invalid parser ref (function not found): {parser_ref!r}") from e


__all__ = [
    "LOAD_SEASON_MONTHS",
    "REGISTRY",
    "TABLE_KEY",
    "FlatFileSpec",
    "ParseContext",
    "ParserStructureError",
    "SeasonFingerprintError",
    "SeasonNotPublishedError",
    "StaleSnapshotError",
    "UnmappedNamesError",
    "build_flat_file_source",
    "ledger_key",
    "resolve_fetch_url",
    "resolve_parser",
    "season_for_date",
    "unmapped_gate",
]
