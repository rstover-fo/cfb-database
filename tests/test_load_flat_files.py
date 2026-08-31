"""Unit tests for the flat-file driver script (T9): pure functions, arg parsing,
and monkeypatched run_source paths -- no live DB, no network.
"""

import re
from datetime import date, datetime, timedelta

import pytest

import scripts.load_flat_files as load_flat_files
import src.pipelines.sources.flat_files as flat_files_module
from src.pipelines.sources.flat_files import REGISTRY, FlatFileSpec, StaleSnapshotError
from src.pipelines.utils import load_ledger
from src.pipelines.utils.file_fetcher import FetchedFile

# ---------------------------------------------------------------------------
# is_due truth table
# ---------------------------------------------------------------------------


def _spec(cadence: str, name: str = "toy") -> FlatFileSpec:
    return FlatFileSpec(
        name=name,
        parser="toy.parse",
        schema="test_schema",
        table="toy_main",
        primary_key=("id",),
        cadence=cadence,
    )


IN_SEASON_DAY = date(2025, 9, 15)  # September: in LOAD_SEASON_MONTHS
OFF_SEASON_DAY = date(2025, 6, 15)  # June: not in LOAD_SEASON_MONTHS


class TestIsDueManual:
    def test_never_due_with_no_history(self):
        assert load_flat_files.is_due(_spec("manual"), None, IN_SEASON_DAY) is False

    def test_never_due_with_stale_history(self):
        last = datetime(2020, 1, 1)
        assert load_flat_files.is_due(_spec("manual"), last, IN_SEASON_DAY) is False


class TestIsDueWeekly:
    def test_never_loaded_in_season_is_due(self):
        assert load_flat_files.is_due(_spec("weekly"), None, IN_SEASON_DAY) is True

    def test_never_loaded_off_season_is_not_due(self):
        assert load_flat_files.is_due(_spec("weekly"), None, OFF_SEASON_DAY) is False

    def test_exactly_six_days_ago_is_not_due(self):
        last = datetime.combine(IN_SEASON_DAY - timedelta(days=6), datetime.min.time())
        assert load_flat_files.is_due(_spec("weekly"), last, IN_SEASON_DAY) is False

    def test_seven_days_ago_is_due(self):
        last = datetime.combine(IN_SEASON_DAY - timedelta(days=7), datetime.min.time())
        assert load_flat_files.is_due(_spec("weekly"), last, IN_SEASON_DAY) is True

    def test_recent_in_season_is_not_due(self):
        last = datetime.combine(IN_SEASON_DAY - timedelta(days=1), datetime.min.time())
        assert load_flat_files.is_due(_spec("weekly"), last, IN_SEASON_DAY) is False

    def test_overdue_but_off_season_is_not_due(self):
        last = datetime(2024, 1, 1)
        assert load_flat_files.is_due(_spec("weekly"), last, OFF_SEASON_DAY) is False


class TestIsDueAnnual:
    def test_never_loaded_is_due(self):
        assert load_flat_files.is_due(_spec("annual"), None, IN_SEASON_DAY) is True

    def test_exactly_three_hundred_days_ago_is_not_due(self):
        last = datetime.combine(IN_SEASON_DAY - timedelta(days=300), datetime.min.time())
        assert load_flat_files.is_due(_spec("annual"), last, IN_SEASON_DAY) is False

    def test_three_hundred_one_days_ago_is_due(self):
        last = datetime.combine(IN_SEASON_DAY - timedelta(days=301), datetime.min.time())
        assert load_flat_files.is_due(_spec("annual"), last, IN_SEASON_DAY) is True

    def test_not_gated_by_season_month(self):
        last = datetime.combine(OFF_SEASON_DAY - timedelta(days=301), datetime.min.time())
        assert load_flat_files.is_due(_spec("annual"), last, OFF_SEASON_DAY) is True


class TestIsDueUnknownCadence:
    def test_raises(self):
        with pytest.raises(ValueError, match="cadence"):
            load_flat_files.is_due(_spec("bogus"), None, IN_SEASON_DAY)


# ---------------------------------------------------------------------------
# Arg parsing
# ---------------------------------------------------------------------------


class TestArgParsing:
    def test_source_and_due_mutually_exclusive(self, monkeypatch):
        monkeypatch.setattr(load_flat_files, "last_checked", lambda *a, **k: None)
        with pytest.raises(SystemExit):
            load_flat_files.main(["--source", "massey", "--due"])

    def test_file_with_zero_sources_rejected(self):
        with pytest.raises(SystemExit):
            load_flat_files.main(["--file", "somefile.csv"])

    def test_file_with_multiple_sources_rejected(self):
        with pytest.raises(SystemExit):
            load_flat_files.main(
                ["--file", "somefile.csv", "--source", "massey", "--source", "sbr"]
            )

    def test_file_with_exactly_one_source_is_accepted_by_parser(self, monkeypatch):
        # Only checking the arg-validation gate doesn't reject this combo --
        # short-circuit before any fetch/DB work happens via --dry-run.
        monkeypatch.setattr(load_flat_files, "last_checked", lambda *a, **k: None)
        rc = load_flat_files.main(["--file", "somefile.csv", "--source", "massey", "--dry-run"])
        assert rc == 0

    def test_unknown_source_choice_rejected(self):
        with pytest.raises(SystemExit):
            load_flat_files.main(["--source", "not_a_real_source"])


class TestDryRun:
    def test_dry_run_prints_all_registry_sources_and_exits_zero(self, monkeypatch, capsys):
        # No DB creds in this sandbox -- due-status lookups must degrade
        # gracefully rather than raising.
        monkeypatch.setattr(
            load_flat_files,
            "last_checked",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no db creds")),
        )

        rc = load_flat_files.main(["--dry-run"])

        assert rc == 0
        # 5 launch sources + 4 sdv_* + 7 ncaa_* (B6a; ncaa_pbp included --
        # its 2025 file is ~11.8MB, well under the ~200MB scope threshold)
        # + 5 espn_* (B6b; espn_pbp_2002_2003 dropped -- the dataset's real
        # minimum season is 2004, verified live, so no pre-CFBD gap-fill
        # table exists).
        assert len(REGISTRY) == 21
        captured = capsys.readouterr()
        for name in REGISTRY:
            assert name in captured.out

    def test_dry_run_does_not_call_fetch_or_build(self, monkeypatch):
        def boom(*a, **k):
            raise AssertionError("dry-run must not fetch or build a source")

        monkeypatch.setattr(load_flat_files, "fetch_file", boom)
        monkeypatch.setattr(load_flat_files, "build_flat_file_source", boom)
        monkeypatch.setattr(load_flat_files, "record_load", boom)
        monkeypatch.setattr(load_flat_files, "last_checked", lambda *a, **k: None)

        rc = load_flat_files.main(["--dry-run"])
        assert rc == 0


# ---------------------------------------------------------------------------
# _planned_sources: explicit-season backfills must not be cadence-gated
# (PR #81 review finding -- an off-season `--due --season 2016` used to plan
# nothing weekly and exit 0)
# ---------------------------------------------------------------------------


def _parse(argv: list[str]):
    return load_flat_files.build_arg_parser().parse_args(argv)


class TestPlannedSources:
    def test_explicit_season_with_due_plans_all_non_manual_sources(self, monkeypatch):
        # Cadence must not even be consulted: an explicit season is a
        # backfill request, and is_due describes only the current season.
        def boom(*a, **k):
            raise AssertionError("explicit-season --due must not consult cadence")

        monkeypatch.setattr(load_flat_files, "is_due", boom)
        monkeypatch.setattr(load_flat_files, "_cadence_last_checked", boom)

        args = _parse(["--due", "--season", "2016"])
        names = load_flat_files._planned_sources(args, OFF_SEASON_DAY, 2016, season_explicit=True)

        expected = [name for name, spec in REGISTRY.items() if spec.cadence != "manual"]
        assert names == expected
        assert all(REGISTRY[name].cadence != "manual" for name in names)
        # The registry has at least one manual source, so the exclusion is
        # actually exercised, and the plan is never empty.
        assert 0 < len(names) < len(REGISTRY)

    def test_due_without_explicit_season_keeps_cadence_filter(self, monkeypatch):
        consulted: list[str] = []

        def fake_is_due(spec, last, today):
            consulted.append(spec.name)
            return spec.name == "massey"

        monkeypatch.setattr(load_flat_files, "is_due", fake_is_due)
        monkeypatch.setattr(load_flat_files, "_cadence_last_checked", lambda spec, season: None)

        args = _parse(["--due"])
        names = load_flat_files._planned_sources(args, IN_SEASON_DAY, 2025, season_explicit=False)

        assert names == ["massey"]
        assert set(consulted) == set(REGISTRY)

    def test_explicit_source_wins_unchanged(self, monkeypatch):
        def boom(*a, **k):
            raise AssertionError("--source selection must not consult cadence")

        monkeypatch.setattr(load_flat_files, "is_due", boom)
        monkeypatch.setattr(load_flat_files, "_cadence_last_checked", boom)

        args = _parse(["--source", "massey", "--season", "2016"])
        names = load_flat_files._planned_sources(args, OFF_SEASON_DAY, 2016, season_explicit=True)
        assert names == ["massey"]

        args = _parse(["--source", "massey", "--source", "sbr"])
        names = load_flat_files._planned_sources(args, IN_SEASON_DAY, 2025, season_explicit=False)
        assert names == ["massey", "sbr"]


# ---------------------------------------------------------------------------
# run_source
# ---------------------------------------------------------------------------

FAKE_FETCHED = FetchedFile(
    content=b"raw-bytes",
    sha256="deadbeef" * 8,
    source_url="https://example.com/data.csv",
)


class TestRunSourceSkippedHash:
    def test_skipped_hash_skips_parse_and_records_skipped(self, monkeypatch):
        def resolve_parser_boom(ref):
            raise AssertionError("parser should never be resolved on a hash-skip")

        monkeypatch.setattr(flat_files_module, "resolve_parser", resolve_parser_boom)
        monkeypatch.setattr(load_flat_files, "fetch_file", lambda target, **kw: FAKE_FETCHED)
        monkeypatch.setattr(load_flat_files, "already_loaded", lambda *a, **k: True)

        record_calls = []
        monkeypatch.setattr(
            load_flat_files, "record_load", lambda *a, **k: record_calls.append((a, k))
        )

        spec = REGISTRY["nflverse_combine"]
        result = load_flat_files.run_source(spec, season=2025, today=date(2025, 9, 1))

        assert result["status"] == "skipped_hash"
        assert result["sha"] == FAKE_FETCHED.sha256
        assert len(record_calls) == 1
        args, kwargs = record_calls[0]
        assert args[0] == "nflverse_combine"
        assert args[1] == FAKE_FETCHED.sha256
        assert kwargs["status"] == "skipped"


class TestRunSourceStaleSnapshot:
    def test_stale_snapshot_maps_to_no_op_offseason(self, monkeypatch):
        monkeypatch.setattr(load_flat_files, "fetch_file", lambda target, **kw: FAKE_FETCHED)
        monkeypatch.setattr(load_flat_files, "already_loaded", lambda *a, **k: False)

        def raise_stale(spec, raw, ctx, resolver):
            raise StaleSnapshotError("last season's data")

        monkeypatch.setattr(load_flat_files, "build_flat_file_source", raise_stale)

        record_calls = []
        monkeypatch.setattr(
            load_flat_files, "record_load", lambda *a, **k: record_calls.append((a, k))
        )

        spec = REGISTRY["nflverse_combine"]
        result = load_flat_files.run_source(spec, season=2025, today=date(2025, 9, 1))

        assert result["status"] == "no_op_offseason"
        assert "last season" in result["error"]
        assert len(record_calls) == 1
        args, kwargs = record_calls[0]
        assert kwargs["status"] == "skipped"
        assert kwargs["error"]


class TestRunSourceGenericFailure:
    def test_generic_parser_exception_is_failed(self, monkeypatch):
        monkeypatch.setattr(load_flat_files, "fetch_file", lambda target, **kw: FAKE_FETCHED)
        monkeypatch.setattr(load_flat_files, "already_loaded", lambda *a, **k: False)

        def raise_boom(spec, raw, ctx, resolver):
            raise ValueError("structurally unexpected file")

        monkeypatch.setattr(load_flat_files, "build_flat_file_source", raise_boom)

        record_calls = []
        monkeypatch.setattr(
            load_flat_files, "record_load", lambda *a, **k: record_calls.append((a, k))
        )

        spec = REGISTRY["nflverse_draft"]
        result = load_flat_files.run_source(spec, season=2025, today=date(2025, 9, 1))

        assert result["status"] == "failed"
        assert "structurally unexpected" in result["error"]
        args, kwargs = record_calls[0]
        assert kwargs["status"] == "failed"

    def test_missing_fetch_target_is_failed_without_ledger_write(self, monkeypatch):
        monkeypatch.setattr(
            load_flat_files,
            "record_load",
            lambda *a, **k: (_ for _ in ()).throw(
                AssertionError("no sha to key on -- ledger must not be touched")
            ),
        )

        spec = REGISTRY["sbr"]  # fetch_url=None, cadence manual
        result = load_flat_files.run_source(spec, season=2025, today=date(2025, 9, 1))

        assert result["status"] == "failed"
        assert result["sha"] is None
        assert "fetch target" in result["error"]

    def test_multi_source_run_continues_past_failure_and_exits_one(self, monkeypatch):
        monkeypatch.setattr(load_flat_files, "fetch_file", lambda target, **kw: FAKE_FETCHED)
        monkeypatch.setattr(load_flat_files, "already_loaded", lambda *a, **k: False)
        monkeypatch.setattr(load_flat_files, "record_load", lambda *a, **k: None)

        def fake_build(spec, raw, ctx, resolver):
            if spec.name == "nflverse_draft":
                raise ValueError("boom")
            return object()  # unused: dlt.pipeline.run is faked below

        monkeypatch.setattr(load_flat_files, "build_flat_file_source", fake_build)

        class _FakeNormalizeInfo:
            def __init__(self, row_counts):
                self.row_counts = row_counts

        class _FakeTrace:
            def __init__(self, row_counts):
                self.last_normalize_info = _FakeNormalizeInfo(row_counts)

        class _FakePipeline:
            def __init__(self, row_counts):
                self.last_trace = _FakeTrace(row_counts)

            def run(self, source_obj):
                return None

        monkeypatch.setattr(
            load_flat_files.dlt, "pipeline", lambda **kw: _FakePipeline({"combine": 3})
        )

        rc = load_flat_files.main(["--source", "nflverse_combine", "--source", "nflverse_draft"])

        assert rc == 1


# ---------------------------------------------------------------------------
# Gate-line format
# ---------------------------------------------------------------------------


class TestGateLineFormat:
    def test_gate_line_printed_for_skipped_hash(self, monkeypatch, capsys):
        monkeypatch.setattr(load_flat_files, "fetch_file", lambda target, **kw: FAKE_FETCHED)
        monkeypatch.setattr(load_flat_files, "already_loaded", lambda *a, **k: True)
        monkeypatch.setattr(load_flat_files, "record_load", lambda *a, **k: None)

        spec = REGISTRY["nflverse_combine"]
        load_flat_files.run_source(spec, season=2025, today=date(2025, 9, 1))

        captured = capsys.readouterr()
        assert "FLATFILE_LOAD source=nflverse_combine" in captured.out
        assert "status=skipped_hash" in captured.out
        assert "rows=0" in captured.out
        assert f"sha={FAKE_FETCHED.sha256[:12]}" in captured.out
        assert "duration_s=" in captured.out

    def test_gate_line_shows_dash_sha_when_no_fetch_happened(self, monkeypatch, capsys):
        spec = REGISTRY["sbr"]  # fetch_url=None, no --file given
        load_flat_files.run_source(spec, season=2025, today=date(2025, 9, 1))

        captured = capsys.readouterr()
        assert "FLATFILE_LOAD source=sbr status=failed" in captured.out
        assert "sha=-" in captured.out


class TestArchiverLedgerMarker:
    """Same-day archiver reruns must not collide on the ledger's unique
    (source, file_sha256) WHERE status='loaded' index -- markers are
    timestamped per run, not per day."""

    def _run_archiver(self, monkeypatch, record_calls):
        def fake_archiver(db_url, *, season):
            return {"fetched": 1, "new": 0, "gaps": ["SEC", "B12", "CFP"]}

        monkeypatch.setattr(load_flat_files, "resolve_parser", lambda ref: fake_archiver)
        monkeypatch.setattr(
            load_flat_files, "record_load", lambda *a, **k: record_calls.append((a, k))
        )
        spec = REGISTRY["availability"]
        return load_flat_files.run_source(spec, season=2025, today=date(2025, 9, 1))

    def test_marker_is_iso_timestamp_not_date_only(self, monkeypatch):
        record_calls = []
        result = self._run_archiver(monkeypatch, record_calls)
        assert result["status"] == "gap"
        (args, kwargs) = record_calls[0]
        sha = args[1]
        # Full ISO timestamp with time component, not the bare run date.
        assert re.match(r"^archiver-\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", sha)
        assert sha != "archiver-2025-09-01"

    def test_two_runs_record_distinct_markers(self, monkeypatch):
        record_calls = []
        self._run_archiver(monkeypatch, record_calls)
        self._run_archiver(monkeypatch, record_calls)
        shas = [args[1] for args, _ in record_calls]
        assert len(shas) == 2 and shas[0] != shas[1]


# ---------------------------------------------------------------------------
# load_ledger.last_checked SQL drift (PR #75 review finding B): a hash-skip
# (status='skipped', error IS NULL) must count as a freshness check; a
# stale-snapshot skip (status='skipped', error set) and a failed attempt
# must not. Asserted against the query text itself, no live DB, mirroring
# how the rest of this driver is tested against mocked psycopg2.
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, captured: dict):
        self._captured = captured

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, query, params):
        self._captured["query"] = query
        self._captured["params"] = params

    def fetchone(self):
        return (None,)


class _FakeConn:
    def __init__(self, captured: dict):
        self._captured = captured

    def cursor(self):
        return _FakeCursor(self._captured)

    def close(self):
        pass


class TestLastCheckedSqlDrift:
    def _run(self, monkeypatch) -> dict:
        captured: dict = {}
        monkeypatch.setattr(load_ledger, "get_db_url", lambda: "postgres://fake")
        monkeypatch.setattr(load_ledger.psycopg2, "connect", lambda dsn: _FakeConn(captured))
        load_ledger.last_checked("massey")
        return captured

    def test_counts_loaded_status(self, monkeypatch):
        captured = self._run(monkeypatch)
        query = " ".join(captured["query"].split())
        assert "status = 'loaded'" in query
        assert captured["params"] == ("massey",)

    def test_counts_skipped_with_null_error(self, monkeypatch):
        captured = self._run(monkeypatch)
        query = " ".join(captured["query"].split())
        # The skipped branch must be gated by error IS NULL, not a bare
        # status check -- a stale-snapshot skip also has status='skipped'
        # but carries a non-NULL error and must NOT count.
        assert "status = 'skipped' AND error IS NULL" in query

    def test_does_not_count_failed(self, monkeypatch):
        captured = self._run(monkeypatch)
        query = " ".join(captured["query"].split())
        assert "'failed'" not in query
