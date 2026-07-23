"""Unit tests for the flat-file driver script (T9): pure functions, arg parsing,
and monkeypatched run_source paths -- no live DB, no network.
"""

import re
from datetime import date, datetime, timedelta

import pytest

import scripts.load_flat_files as load_flat_files
import src.pipelines.sources.flat_files as flat_files_module
from src.pipelines.sources.flat_files import REGISTRY, FlatFileSpec, StaleSnapshotError
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
        monkeypatch.setattr(load_flat_files, "last_success", lambda *a, **k: None)
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
        monkeypatch.setattr(load_flat_files, "last_success", lambda *a, **k: None)
        rc = load_flat_files.main(["--file", "somefile.csv", "--source", "massey", "--dry-run"])
        assert rc == 0

    def test_unknown_source_choice_rejected(self):
        with pytest.raises(SystemExit):
            load_flat_files.main(["--source", "not_a_real_source"])


class TestDryRun:
    def test_dry_run_prints_all_five_registry_sources_and_exits_zero(self, monkeypatch, capsys):
        # No DB creds in this sandbox -- due-status lookups must degrade
        # gracefully rather than raising.
        monkeypatch.setattr(
            load_flat_files,
            "last_success",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no db creds")),
        )

        rc = load_flat_files.main(["--dry-run"])

        assert rc == 0
        assert len(REGISTRY) == 5
        captured = capsys.readouterr()
        for name in REGISTRY:
            assert name in captured.out

    def test_dry_run_does_not_call_fetch_or_build(self, monkeypatch):
        def boom(*a, **k):
            raise AssertionError("dry-run must not fetch or build a source")

        monkeypatch.setattr(load_flat_files, "fetch_file", boom)
        monkeypatch.setattr(load_flat_files, "build_flat_file_source", boom)
        monkeypatch.setattr(load_flat_files, "record_load", boom)
        monkeypatch.setattr(load_flat_files, "last_success", lambda *a, **k: None)

        rc = load_flat_files.main(["--dry-run"])
        assert rc == 0


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
