"""Flat-file ingestion framework: registry, contracts, and dlt source builder.

This module is the contract surface for the flat-file subsystem
(docs/brainstorms/2026-07-23-warehouse-extension-data-sources.md). The pieces:

- ``FlatFileSpec`` -- declarative registry entry per source (what to fetch, how
  to parse, where to load, on what cadence).
- ``ParseContext`` -- everything a pure parser is allowed to know about a fetch.
- ``REGISTRY`` -- the five launch sources (massey, nflverse_combine,
  nflverse_draft, sbr, availability) plus the sportsdataverse-data additions
  (sdv_team_xwalk, sdv_game_xwalk, sdv_fpi_weekly, sdv_ratings_weekly).
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
        fetch_url: Default URL; None means the source requires ``--file`` or
            does its own discovery (archiver).
        write_disposition: dlt disposition for the main + child resources.
        child_table: Optional secondary table populated via rows tagged with
            ``_table`` (Massey per-system ranks).
        child_primary_key: Merge key of the child table.
        uses_xwalk: Whether team-name resolution applies.
        xwalk_fields: Row fields containing source-spelled team names.
        keep_source_names: When True, the original spelling of each xwalk
            field F is preserved in column ``{F}_source`` (SBR).
        unmapped_fail_rate: Max tolerated fraction of rows with unmapped
            names before the source fails loud.
        kind: "dlt" (fetch -> parse -> dlt merge) or "archiver" (module runs
            its own discovery + raw-bytes archival, bypassing dlt).
    """

    name: str
    parser: str
    schema: str
    table: str
    primary_key: tuple[str, ...]
    cadence: str
    fetch_url: str | None = None
    write_disposition: str = "merge"
    child_table: str | None = None
    child_primary_key: tuple[str, ...] = ()
    uses_xwalk: bool = False
    xwalk_fields: tuple[str, ...] = ()
    keep_source_names: bool = False
    unmapped_fail_rate: float = 0.02
    kind: str = "dlt"


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
    # sportsdataverse-data (B2 items 1-2): nflverse-pattern clones -- single
    # parquet, PK-verified, ParserStructureError on structural surprise -- but
    # NOT cumulative like nflverse's combine/draft_picks: cfbfastR-cfb-data
    # publishes one asset per season (`{dataset}_{season}.parquet`), so
    # fetch_url below is season-pinned to 2025 (the latest published at write
    # time) and will need a manual bump to `_2026` once that season's asset
    # appears -- see flatfile_parsers/sportsdataverse.py's module docstring
    # for the full finding, including why there is deliberately no
    # `sdv_player_xwalk` entry (no such asset exists in the real release) and
    # why none of these four use the team-name xwalk (ids already line up
    # with CFBD's numeric id namespace -- verified against the live warehouse
    # 2026-08-29: ref.teams.id 333=Alabama / 2483=Oregon match ESPN ids,
    # 2024 core.games ids are ESPN event ids (incl. 401632103), and the
    # ESPN-style athlete id 5083552 exists in core.roster).
    "sdv_team_xwalk": FlatFileSpec(
        name="sdv_team_xwalk",
        parser="sportsdataverse.parse_team_xwalk",
        schema="ref",
        table="team_id_xwalk",
        primary_key=("season", "norm_key"),
        cadence="weekly",
        fetch_url=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_crosswalk/cfb_teams_crosswalk_2025.parquet"
        ),
    ),
    "sdv_game_xwalk": FlatFileSpec(
        name="sdv_game_xwalk",
        parser="sportsdataverse.parse_game_xwalk",
        schema="ref",
        table="game_id_xwalk",
        primary_key=("season", "matchup_key", "yahoo_date"),
        cadence="weekly",
        fetch_url=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_crosswalk/cfb_schedule_crosswalk_2025.parquet"
        ),
    ),
    "sdv_fpi_weekly": FlatFileSpec(
        name="sdv_fpi_weekly",
        parser="sportsdataverse.parse_fpi_weekly",
        schema="ratings",
        table="fpi_weekly",
        primary_key=("season", "season_type", "week", "team_id"),
        cadence="weekly",
        fetch_url=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_fpi_weekly/cfb_fpi_weekly_2025.parquet"
        ),
    ),
    "sdv_ratings_weekly": FlatFileSpec(
        name="sdv_ratings_weekly",
        parser="sportsdataverse.parse_ratings_weekly",
        schema="ratings",
        table="external_weekly",
        primary_key=("season", "through_week", "team_id"),
        cadence="weekly",
        fetch_url=(
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_ratings_weekly/cfb_ratings_weekly_2025.parquet"
        ),
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
    "StaleSnapshotError",
    "UnmappedNamesError",
    "build_flat_file_source",
    "resolve_parser",
    "season_for_date",
    "unmapped_gate",
]
