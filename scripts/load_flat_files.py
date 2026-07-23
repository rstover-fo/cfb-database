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
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime

import dlt

from src.pipelines.sources.flat_files import (
    LOAD_SEASON_MONTHS,
    REGISTRY,
    FlatFileSpec,
    ParseContext,
    StaleSnapshotError,
    build_flat_file_source,
    resolve_parser,
    season_for_date,
)
from src.pipelines.utils.file_fetcher import fetch_file
from src.pipelines.utils.load_ledger import already_loaded, last_success, record_load
from src.pipelines.utils.team_xwalk import XwalkResolver

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Ledger error messages can carry an unbounded amount of detail (e.g. an
# UnmappedNamesError's row-by-row breakdown); cap what we persist/print.
ERROR_MESSAGE_LIMIT = 500


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
    today: date | None = None,
) -> dict:
    """Fetch/parse/load one registry source.

    Never raises -- every failure mode lands in the returned dict's
    ``status``/``error`` fields (mirrors ``load_season.py``'s per-source
    try/except) so a multi-source run continues past any single source's
    failure. Also prints the ``FLATFILE_LOAD ...`` gate line as its last
    action, once ``duration_s`` is known.

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

    try:
        if spec.kind == "archiver":
            archiver = resolve_parser(spec.parser)
            archive_result = archiver(None, season=season)
            rows = archive_result.get("new", 0)
            gaps = list(archive_result.get("gaps") or [])
            # Archiver runs aren't file-hash keyed (per-PDF dedupe happens
            # inside the archiver itself); the ledger row is a run marker.
            sha = f"archiver-{today.isoformat()}"
            record_load(spec.name, sha, status="loaded", row_count=rows)
            result.update(
                status="gap" if gaps else "loaded",
                rows=rows,
                sha=sha,
                gaps=gaps,
            )

        elif spec.kind == "dlt":
            fetch_target = file_path or spec.fetch_url
            if not fetch_target:
                result["error"] = f"{spec.name}: no fetch target -- pass --file or add fetch_url"
            else:
                fetched = fetch_file(fetch_target)
                result["sha"] = fetched.sha256

                if already_loaded(spec.name, fetched.sha256):
                    record_load(
                        spec.name,
                        fetched.sha256,
                        status="skipped",
                        source_url=fetched.source_url,
                    )
                    result["status"] = "skipped_hash"
                else:
                    ctx = ParseContext(
                        source=spec.name,
                        snapshot_date=today,
                        season=season,
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
                        spec.name,
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
        _safe_record_load(spec.name, result["sha"], status="skipped", error=msg)
    except Exception as e:
        msg = str(e)[:ERROR_MESSAGE_LIMIT]
        result["error"] = msg
        result["status"] = "failed"
        _safe_record_load(spec.name, result["sha"], status="failed", error=msg)
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
        help="Season override passed to the parser (default: inferred from today's date)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned sources (cadence, due-status, fetch target) and exit; "
        "touches no DB and fetches/loads nothing",
    )
    return parser


def _planned_sources(args: argparse.Namespace, today: date) -> list[str]:
    if args.source:
        return list(args.source)
    if args.due:
        return [
            name for name, spec in REGISTRY.items() if is_due(spec, _safe_last_success(name), today)
        ]
    return list(REGISTRY)


def _fetch_target_display(spec: FlatFileSpec, file_override: str | None) -> str:
    if file_override:
        return file_override
    if spec.fetch_url:
        return spec.fetch_url
    if spec.kind == "archiver":
        return "<archiver: auto-discovery>"
    return "<requires --file>"


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.file is not None and (not args.source or len(args.source) != 1):
        parser.error("--file requires exactly one --source")

    today = date.today()
    season = args.season if args.season is not None else season_for_date(today)
    names = _planned_sources(args, today)

    if args.dry_run:
        print(f"[DRY RUN] {len(names)} flat-file source(s) planned for season {season}")
        for name in names:
            spec = REGISTRY[name]
            due = is_due(spec, _safe_last_success(name), today)
            fetch_target = _fetch_target_display(spec, args.file)
            print(f"  {name:20s} cadence={spec.cadence:8s} due={due!s:5s} fetch={fetch_target}")
        return 0

    results = [
        run_source(REGISTRY[name], file_path=args.file, season=season, today=today)
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
