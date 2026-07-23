"""Flat-file ingestion framework: registry, contracts, and dlt source builder.

This module is the contract surface for the flat-file subsystem
(docs/brainstorms/2026-07-23-warehouse-extension-data-sources.md). The pieces:

- ``FlatFileSpec`` -- declarative registry entry per source (what to fetch, how
  to parse, where to load, on what cadence).
- ``ParseContext`` -- everything a pure parser is allowed to know about a fetch.
- ``REGISTRY`` -- the four launch sources: massey, nflverse_combine,
  nflverse_draft, sbr, availability.
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

from dataclasses import dataclass
from datetime import date

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
    raise NotImplementedError("T3 implements build_flat_file_source")


def resolve_parser(parser_ref: str):
    """Import "<module>.<function>" under flatfile_parsers and return the callable.

    Implemented in T3.
    """
    raise NotImplementedError("T3 implements resolve_parser")


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
