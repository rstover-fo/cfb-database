#!/usr/bin/env python3
"""Flat-file source driver (T9): fetch/parse/load loop over ``flat_files.REGISTRY``.

Wraps the frozen framework in ``src/pipelines/sources/flat_files.py`` --
``build_flat_file_source``, ``ParseContext``, the ledger in
``src/pipelines/utils/load_ledger.py`` -- with a CLI that mirrors
``scripts/load_season.py``'s orchestration style: per-source try/except that
never lets one source's failure abort the others, a summary block, and a
non-zero exit code when anything failed.

Usage:
    python scripts/load_flat_files.py --dry-run                  # show the plan, touch nothing
    python scripts/load_flat_files.py --due                      # run whatever cadence says is due
    python scripts/load_flat_files.py --source massey            # force one source
    python scripts/load_flat_files.py --source sbr --file odds.xlsx  # feed a local file
    python scripts/load_flat_files.py --season 2025 --due         # override the season hint

Row counting (kind="dlt"): ``build_flat_file_source`` already materializes the
parsed rows into in-memory list resources, so re-iterating those resources
here (after ``pipeline.run()`` has already consumed them once for extraction)
would risk double-counting or -- if dlt ever swaps the list-backed resource
for a one-shot generator -- silently coming back empty. Instead we read
``pipeline.last_trace.last_normalize_info.row_counts``, dlt's own per-table
counts from the normalize step that just ran, and sum the main/child table
entries. This is a side-effect-free read of data dlt already computed.

Ledger unavailability: ``--dry-run`` must work with no DB configured (used in
CI/sandboxes with no Supabase credentials) -- due-status lookups swallow any
``last_success()`` failure (including ``get_db_url``'s ``RuntimeError`` for
missing creds) and fall back to "never loaded". Real runs make no such
allowance: a missing DB surfaces as a per-source ``status=failed`` result
(via the same try/except that catches parser errors), never a driver crash.

Per-season multi-file sources (B6a): see ``flat_files.py``'s module
docstring for the full design (``url_template``, ``ledger_key()``,
``fallback_latest``). This driver's piece is ``_fetch_seasoned()``, which
wraps ``fetch_file()`` with the 404-fallback probe -- it is the only place
that walks backward through seasons, and it only does so when the season
was NOT explicitly requested via ``--season`` (an explicit request that
404s is a loud ``SeasonNotPublishedError``, never a silent substitution).
"""

import argparse
import logging
import os
import sys
import time
from datetime import UTC, date, datetime

import dlt
import httpx

from src.pipelines.sources.flat_files import (
    LOAD_SEASON_MONTHS,
    REGISTRY,
    FlatFileSpec,
    ParseContext,
    SeasonNotPublishedError,
    StaleSnapshotError,
    build_flat_file_source,
    ledger_key,
    resolve_fetch_url,
    resolve_parser,
    season_for_date,
)
from src.pipelines.utils.file_fetcher import FetchedFile, fetch_file
from src.pipelines.utils.load_ledger import already_loaded, last_success, record_load
from src.pipelines.utils.team_xwalk import XwalkResolver

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Ledger error messages can carry an unbounded amount of detail (e.g. an
# UnmappedNamesError's row-by-row breakdown); cap what we persist/print.
ERROR_MESSAGE_LIMIT = 500

# How many seasons back _fetch_seasoned() will probe on a 404 before giving
# up (fallback_latest specs only, and only when the season was not
# explicitly requested via --season). 3 is a generous cushion for "the
# upstream cron is running late" without turning into an unbounded crawl.
FALLBACK_MAX_STEPS = 3


def _fetch_seasoned(
    spec: FlatFileSpec, season: int, *, allow_fallback: bool
) -> tuple[FetchedFile, int]:
    """Fetch ``spec``'s file for ``season``, honoring 404 fallback.

    Returns ``(fetched, resolved_season)`` -- ``resolved_season`` is
    ``season`` itself unless a fallback season's file is the one that
    actually loaded. A 404 with fallback disabled or exhausted raises
    ``SeasonNotPublishedError`` (mapped by ``run_source`` to a
    ``not_published`` outcome, not a failure). Any other exception
    (non-404 HTTP error, connection error, ...) propagates unchanged.

    ``allow_fallback`` gates whether fallback applies at all -- it is False
    whenever the caller explicitly requested this season via ``--season``,
    per ``flat_files.py``'s module docstring: an explicit request that
    404s should never be silently substituted with a different season's
    data.
    """
    url = resolve_fetch_url(spec, season)
    try:
        return fetch_file(url), season
    except httpx.HTTPStatusError as e:
        if e.response.status_code != 404:
            raise
        if not (allow_fallback and spec.url_template and spec.fallback_latest):
            raise SeasonNotPublishedError(
                f"{spec.name}: season {season} file not published (404): {url}"
            ) from e

        for step in range(1, FALLBACK_MAX_STEPS + 1):
            candidate = season - step
            if spec.min_season is not None and candidate < spec.min_season:
                break
            candidate_url = resolve_fetch_url(spec, candidate)
            logger.info(
                f"{spec.name}: season {season} not yet published (404); falling back to {candidate}"
            )
            try:
                return fetch_file(candidate_url), candidate
            except httpx.HTTPStatusError as fallback_error:
                if fallback_error.response.status_code != 404:
                    raise
                continue

        raise SeasonNotPublishedError(
            f"{spec.name}: no published file found for season {season} or the "
            f"{FALLBACK_MAX_STEPS} season(s) before it"
        ) from e


def is_due(spec: FlatFileSpec, last: datetime | None, today: date) -> bool:
    """Pure cadence check driving ``--due`` (and the ``--dry-run`` due column).

    - "manual": never due.
    - "weekly": due only in-season (``today.month in LOAD_SEASON_MONTHS``) and
      when never loaded or the last success was more than 6 days ago.
    - "annual": due when never loaded or the last success was more than 300
      days ago (no season gating -- the ledger hash-skip makes an off-cadence
      trigger free).
    """
    if spec.cadence == "manual":
        return False

    if spec.cadence == "weekly":
        if today.month not in LOAD_SEASON_MONTHS:
            return False
        return last is None or (today - last.date()).days > 6

    if spec.cadence == "annual":
        return last is None or (today - last.date()).days > 300

    raise ValueError(f"{spec.name}: unknown cadence {spec.cadence!r}")


def _safe_last_success(name: str) -> datetime | None:
    """``last_success()`` tolerating a missing/unreachable DB.

    Used only for informational due-status (``--dry-run``, ``--due``
    selection) -- a real load's ledger calls are left to raise and fail that
    source loudly inside ``run_source``.
    """
    try:
        return last_success(name)
    except Exception as e:
        logger.debug(f"{name}: last_success unavailable ({e}); treating as never loaded")
        return None


def _safe_record_load(source: str, sha: str | None, **kwargs) -> None:
    """Best-effort ledger write from inside an except block.

    Called only after a source has already failed; if the ledger itself is
    unreachable (e.g. no DB creds) this must not raise a second exception on
    top of the first and abort the whole run. Skipped entirely when there is
    no sha to key on (the failure happened before any bytes were fetched).
    """
    if not sha:
        return
    try:
        record_load(source, sha, **kwargs)
    except Exception as e:
        logger.warning(f"{source}: could not write ledger row after failure: {e}")


def _gate_line(result: dict) -> str:
    sha = result.get("sha")
    sha_display = sha[:12] if sha else "-"
    line = (
        f"FLATFILE_LOAD source={result['source']} status={result['status']} "
        f"rows={result['rows']} sha={sha_display} duration_s={result['duration_s']:.1f}"
    )
    if result.get("unmapped") is not None:
        line += f" unmapped={result['unmapped']}"
    gaps = result.get("gaps")
    if gaps:
        line += f" gaps={','.join(gaps)}"
    return line


def run_source(
    spec: FlatFileSpec,
    *,
    file_path: str | None = None,
    season: int | None = None,
    season_explicit: bool = False,
    today: date | None = None,
) -> dict:
    """Fetch/parse/load one registry source.

    Never raises -- every failure mode lands in the returned dict's
    ``status``/``error`` fields (mirrors ``load_season.py``'s per-source
    try/except) so a multi-source run continues past any single source's
    failure. Also prints the ``FLATFILE_LOAD ...`` gate line as its last
    action, once ``duration_s`` is known.

    ``season_explicit`` gates 404 fallback for ``url_template`` sources
    (``_fetch_seasoned``): True means the caller asked for this exact season
    (``--season``), so a 404 there is a loud failure, never a silent
    substitution -- see ``flat_files.py``'s module docstring.

    Returns:
        {"source", "status", "rows", "sha", "duration_s", "error"}, plus
        "unmapped" (misses count, when a crosswalk resolver was used) and
        "gaps" (archiver conference gaps) when applicable.
    """
    today = today or date.today()
    season = season if season is not None else season_for_date(today)
    start = time.time()

    result: dict = {
        "source": spec.name,
        "status": "failed",
        "rows": 0,
        "sha": None,
        "duration_s": 0.0,
        "error": None,
        "unmapped": None,
        "gaps": None,
    }
    resolver: XwalkResolver | None = None
    # Ledger source key for this attempt -- starts at the requested season's
    # key and is corrected to the fallback season's key the moment a fetch
    # actually resolves one (see _fetch_seasoned). Used by both the success
    # path and any post-fetch failure's ledger write.
    ledger_source = ledger_key(spec, season)

    try:
        if spec.kind == "archiver":
            archiver = resolve_parser(spec.parser)
            archive_result = archiver(None, season=season)
            rows = archive_result.get("new", 0)
            gaps = list(archive_result.get("gaps") or [])
            # Archiver runs aren't file-hash keyed (per-PDF dedupe happens
            # inside the archiver itself); the ledger row is a run marker.
            # Timestamped per run: a date-only marker would trip the ledger's
            # unique (source, file_sha256) WHERE status='loaded' index on a
            # same-day rerun, failing an otherwise harmless re-invocation.
            sha = f"archiver-{datetime.now(UTC).isoformat()}"
            record_load(ledger_source, sha, status="loaded", row_count=rows)
            result.update(
                status="gap" if gaps else "loaded",
                rows=rows,
                sha=sha,
                gaps=gaps,
            )

        elif spec.kind == "dlt":
            fetch_target = file_path or resolve_fetch_url(spec, season)
            if not fetch_target:
                result["error"] = f"{spec.name}: no fetch target -- pass --file or add fetch_url"
            else:
                if file_path:
                    fetched = fetch_file(file_path)
                    resolved_season = season
                else:
                    fetched, resolved_season = _fetch_seasoned(
                        spec, season, allow_fallback=not season_explicit
                    )
                ledger_source = ledger_key(spec, resolved_season)
                result["sha"] = fetched.sha256

                if already_loaded(ledger_source, fetched.sha256):
                    record_load(
                        ledger_source,
                        fetched.sha256,
                        status="skipped",
                        source_url=fetched.source_url,
                    )
                    result["status"] = "skipped_hash"
                else:
                    ctx = ParseContext(
                        source=spec.name,
                        snapshot_date=today,
                        season=resolved_season,
                        source_url=fetched.source_url,
                        file_name=os.path.basename(fetched.source_url),
                    )
                    resolver = XwalkResolver.load(spec.name) if spec.uses_xwalk else None

                    source_obj = build_flat_file_source(spec, fetched.content, ctx, resolver)

                    pipeline = dlt.pipeline(
                        pipeline_name=f"flatfile_{spec.name}",
                        destination="postgres",
                        dataset_name=spec.schema,
                    )
                    pipeline.run(source_obj)

                    row_counts = pipeline.last_trace.last_normalize_info.row_counts
                    rows = row_counts.get(spec.table, 0)
                    if spec.child_table:
                        rows += row_counts.get(spec.child_table, 0)

                    record_load(
                        ledger_source,
                        fetched.sha256,
                        status="loaded",
                        source_url=fetched.source_url,
                        row_count=rows,
                    )
                    result.update(status="loaded", rows=rows)
        else:
            result["error"] = f"{spec.name}: unknown spec.kind {spec.kind!r}"

    except StaleSnapshotError as e:
        msg = str(e)[:ERROR_MESSAGE_LIMIT]
        result["error"] = msg
        result["status"] = "no_op_offseason"
        _safe_record_load(ledger_source, result["sha"], status="skipped", error=msg)
    except SeasonNotPublishedError as e:
        # A season-parameterized source's file isn't out yet (and fallback
        # didn't apply or was exhausted) -- expected, self-healing state,
        # not an operational failure. No sha was ever obtained, so (per
        # _safe_record_load) nothing is written to the ledger, same as the
        # "missing fetch target" case above.
        msg = str(e)[:ERROR_MESSAGE_LIMIT]
        result["error"] = msg
        result["status"] = "not_published"
    except Exception as e:
        msg = str(e)[:ERROR_MESSAGE_LIMIT]
        result["error"] = msg
        result["status"] = "failed"
        _safe_record_load(ledger_source, result["sha"], status="failed", error=msg)
    finally:
        if resolver is not None:
            result["unmapped"] = len(resolver.misses)
        result["duration_s"] = time.time() - start
        print(_gate_line(result))

    return result


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch, parse, and load the flat-file sources (massey, nflverse, sbr, "
        "availability) into their target Postgres schemas."
    )
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument(
        "--source",
        action="append",
        choices=sorted(REGISTRY),
        help="Force this source regardless of cadence (repeatable)",
    )
    selection.add_argument(
        "--due",
        action="store_true",
        help="Run every source whose cadence says it's due",
    )
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Local file path fed to the single --source instead of its fetch_url",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Season override passed to the parser (default: inferred from today's date). "
        "For a url_template source this targets that exact season's file and disables "
        "404 fallback -- a backfill request is never silently substituted.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned sources (cadence, due-status, fetch target) and exit; "
        "touches no DB and fetches/loads nothing",
    )
    return parser


def _planned_sources(args: argparse.Namespace, today: date, season: int) -> list[str]:
    if args.source:
        return list(args.source)
    if args.due:
        return [
            name
            for name, spec in REGISTRY.items()
            if is_due(spec, _safe_last_success(ledger_key(spec, season)), today)
        ]
    return list(REGISTRY)


def _fetch_target_display(spec: FlatFileSpec, file_override: str | None, season: int) -> str:
    if file_override:
        return file_override
    fetch_url = resolve_fetch_url(spec, season)
    if fetch_url:
        return fetch_url
    if spec.kind == "archiver":
        return "<archiver: auto-discovery>"
    return "<requires --file>"


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.file is not None and (not args.source or len(args.source) != 1):
        parser.error("--file requires exactly one --source")

    today = date.today()
    season_explicit = args.season is not None
    season = args.season if season_explicit else season_for_date(today)
    names = _planned_sources(args, today, season)

    if args.dry_run:
        print(f"[DRY RUN] {len(names)} flat-file source(s) planned for season {season}")
        for name in names:
            spec = REGISTRY[name]
            due = is_due(spec, _safe_last_success(ledger_key(spec, season)), today)
            fetch_target = _fetch_target_display(spec, args.file, season)
            print(f"  {name:20s} cadence={spec.cadence:8s} due={due!s:5s} fetch={fetch_target}")
        return 0

    results = [
        run_source(
            REGISTRY[name],
            file_path=args.file,
            season=season,
            season_explicit=season_explicit,
            today=today,
        )
        for name in names
    ]

    print(f"\n{'=' * 60}")
    print("Flat-File Load Summary")
    print(f"{'=' * 60}")
    for r in results:
        print(
            f"  [{r['status']:16s}] {r['source']:20s} rows={r['rows']:>8} {r['duration_s']:>6.1f}s"
        )
    failed = sum(1 for r in results if r["status"] == "failed")
    print(f"{'=' * 60}")
    print(f"  Total: {len(results)} source(s) | {failed} failed")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
