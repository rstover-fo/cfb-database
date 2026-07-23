"""Unit tests for the flat-file ingestion framework (T3): pure functions and
stub-based tests only -- no live DB, no network.
"""

from datetime import date

import httpx
import pytest

from src.pipelines.sources import flat_files
from src.pipelines.sources.flat_files import (
    FlatFileSpec,
    ParseContext,
    ParserStructureError,
    UnmappedNamesError,
    build_flat_file_source,
    resolve_parser,
)
from src.pipelines.utils import load_ledger
from src.pipelines.utils.file_fetcher import fetch_file
from src.pipelines.utils.team_xwalk import XwalkResolver, normalize_name

# ---------------------------------------------------------------------------
# file_fetcher.fetch_file
# ---------------------------------------------------------------------------


class TestFetchFileLocalPath:
    def test_reads_bytes_and_computes_sha256(self, tmp_path):
        import hashlib

        p = tmp_path / "sample.csv"
        p.write_bytes(b"hello,world\n1,2\n")

        result = fetch_file(str(p))

        assert result.content == b"hello,world\n1,2\n"
        assert result.sha256 == hashlib.sha256(b"hello,world\n1,2\n").hexdigest()
        assert result.source_url == str(p.resolve())

    def test_missing_local_path_raises(self, tmp_path):
        missing = tmp_path / "does_not_exist.csv"
        with pytest.raises(FileNotFoundError):
            fetch_file(str(missing))


class TestFetchFileHttp:
    def test_retries_on_500_then_succeeds(self, monkeypatch):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(500, text="server error")
            return httpx.Response(200, content=b"ok-bytes")

        transport = httpx.MockTransport(handler)
        real_client_cls = httpx.Client

        import src.pipelines.utils.file_fetcher as ff_mod

        def fake_client(*, follow_redirects, timeout):
            return real_client_cls(
                follow_redirects=follow_redirects, timeout=timeout, transport=transport
            )

        monkeypatch.setattr(ff_mod.httpx, "Client", fake_client)
        monkeypatch.setattr(ff_mod.time, "sleep", lambda *_: None)

        result = fetch_file("https://example.com/file.csv")

        assert calls["n"] == 2
        assert result.content == b"ok-bytes"
        assert result.source_url == "https://example.com/file.csv"

    def test_terminal_404_raises(self, monkeypatch):
        def handler(request):
            return httpx.Response(404, text="not found")

        transport = httpx.MockTransport(handler)
        real_client_cls = httpx.Client

        import src.pipelines.utils.file_fetcher as ff_mod

        def fake_client(*, follow_redirects, timeout):
            return real_client_cls(
                follow_redirects=follow_redirects, timeout=timeout, transport=transport
            )

        monkeypatch.setattr(ff_mod.httpx, "Client", fake_client)

        with pytest.raises(httpx.HTTPStatusError):
            fetch_file("https://example.com/missing.csv")


# ---------------------------------------------------------------------------
# load_ledger
# ---------------------------------------------------------------------------


class TestRecordLoadValidation:
    def test_invalid_status_raises_without_touching_db(self, monkeypatch):
        def boom(*args, **kwargs):
            raise AssertionError("psycopg2.connect should not be called for a bad status")

        monkeypatch.setattr(load_ledger.psycopg2, "connect", boom)

        with pytest.raises(ValueError):
            load_ledger.record_load("massey", "deadbeef", status="bogus")

    def test_valid_statuses_are_accepted_values(self):
        assert load_ledger.VALID_STATUSES == ("loaded", "skipped", "failed")


# ---------------------------------------------------------------------------
# team_xwalk
# ---------------------------------------------------------------------------


class TestNormalizeName:
    def test_strips_and_casefolds(self):
        assert normalize_name("  Ohio State  ") == "ohio state"

    def test_collapses_internal_whitespace(self):
        assert normalize_name("Ohio    State") == "ohio state"

    def test_case_insensitive(self):
        assert normalize_name("OHIO STATE") == normalize_name("ohio state")

    def test_tabs_and_newlines_collapse(self):
        assert normalize_name("Ohio\tState\n") == "ohio state"


class TestXwalkResolver:
    def test_resolve_hit(self):
        resolver = XwalkResolver("massey", {"ohio st": "Ohio State"})
        assert resolver.resolve("Ohio St") == "Ohio State"

    def test_resolve_hit_is_whitespace_and_case_insensitive(self):
        resolver = XwalkResolver("massey", {"ohio st": "Ohio State"})
        assert resolver.resolve("  ohio   ST  ") == "Ohio State"

    def test_resolve_miss_returns_none(self):
        resolver = XwalkResolver("massey", {"ohio st": "Ohio State"})
        assert resolver.resolve("Nonexistent Tech") is None

    def test_misses_counted_by_original_spelling(self):
        resolver = XwalkResolver("massey", {})
        resolver.resolve("Foo Tech")
        resolver.resolve("Foo Tech")
        resolver.resolve("Bar U")

        assert resolver.misses == {"Foo Tech": 2, "Bar U": 1}

    def test_misses_property_is_a_copy(self):
        resolver = XwalkResolver("massey", {})
        resolver.resolve("Foo Tech")
        snapshot = resolver.misses
        snapshot["Foo Tech"] = 999
        assert resolver.misses == {"Foo Tech": 1}


# ---------------------------------------------------------------------------
# flat_files.resolve_parser
# ---------------------------------------------------------------------------


class TestResolveParser:
    def test_good_ref_returns_callable(self):
        fn = resolve_parser("massey.parse")
        assert callable(fn)

    def test_bad_format_no_dot_raises(self):
        with pytest.raises(ValueError, match="massey"):
            resolve_parser("massey")

    def test_missing_module_raises(self):
        with pytest.raises(ValueError):
            resolve_parser("does_not_exist.parse")

    def test_missing_function_raises(self):
        with pytest.raises(ValueError):
            resolve_parser("massey.does_not_exist")


# ---------------------------------------------------------------------------
# flat_files.build_flat_file_source
# ---------------------------------------------------------------------------


def _toy_spec(**overrides) -> FlatFileSpec:
    defaults = dict(
        name="toy",
        parser="toy.parse",
        schema="test_schema",
        table="toy_main",
        primary_key=("id",),
        cadence="manual",
        child_table="toy_child",
        child_primary_key=("id", "sub"),
        uses_xwalk=True,
        xwalk_fields=("team",),
        keep_source_names=True,
        unmapped_fail_rate=0.5,
    )
    defaults.update(overrides)
    return FlatFileSpec(**defaults)


def _toy_ctx() -> ParseContext:
    return ParseContext(source="toy", snapshot_date=date(2026, 7, 23))


class TestBuildFlatFileSource:
    def test_end_to_end_xwalk_split_and_drop(self, monkeypatch):
        def toy_parse(raw, ctx):
            yield {"id": 1, "team": "Ohio St", "value": 10}
            yield {"id": 1, "sub": 1, "team": "Ohio St", "_table": "toy_child"}
            yield {"id": 2, "team": "Unknown Team", "value": 20}

        monkeypatch.setattr(flat_files, "resolve_parser", lambda ref: toy_parse)

        spec = _toy_spec()
        resolver = XwalkResolver("toy", {"ohio st": "Ohio State"})

        source = build_flat_file_source(spec, b"raw-bytes", _toy_ctx(), resolver=resolver)

        assert set(source.resources.keys()) == {"toy_main", "toy_child"}

        main_rows = list(source.resources["toy_main"])
        child_rows = list(source.resources["toy_child"])

        # The unmapped row (id=2) was dropped entirely.
        assert len(main_rows) == 1
        assert main_rows[0]["id"] == 1
        assert main_rows[0]["team"] == "Ohio State"
        assert main_rows[0]["team_source"] == "Ohio St"
        assert main_rows[0]["value"] == 10
        assert "_table" not in main_rows[0]

        assert len(child_rows) == 1
        assert child_rows[0]["team"] == "Ohio State"
        assert child_rows[0]["team_source"] == "Ohio St"
        assert "_table" not in child_rows[0]

        # One distinct unmapped name, counted once.
        assert resolver.misses == {"Unknown Team": 1}

        assert source.resources["toy_main"].write_disposition == spec.write_disposition
        assert source.resources["toy_child"].write_disposition == spec.write_disposition

    def test_missing_resolver_when_xwalk_required_raises(self, monkeypatch):
        def toy_parse(raw, ctx):
            yield {"id": 1, "team": "Ohio St"}

        monkeypatch.setattr(flat_files, "resolve_parser", lambda ref: toy_parse)

        spec = _toy_spec()
        with pytest.raises(ValueError):
            build_flat_file_source(spec, b"raw-bytes", _toy_ctx(), resolver=None)

    def test_unmapped_gate_trips_raises(self, monkeypatch):
        def toy_parse(raw, ctx):
            yield {"id": 1, "team": "Ohio St"}
            yield {"id": 2, "team": "Nowhere State"}

        monkeypatch.setattr(flat_files, "resolve_parser", lambda ref: toy_parse)

        spec = _toy_spec(unmapped_fail_rate=0.1, child_table=None, child_primary_key=())
        resolver = XwalkResolver("toy", {"ohio st": "Ohio State"})

        with pytest.raises(UnmappedNamesError) as exc_info:
            build_flat_file_source(spec, b"raw-bytes", _toy_ctx(), resolver=resolver)

        assert exc_info.value.total_rows == 2
        assert "Nowhere State" in exc_info.value.unmapped

    def test_unknown_table_key_raises_parser_structure_error(self, monkeypatch):
        def toy_parse(raw, ctx):
            yield {"id": 1, "_table": "totally_unrelated_table"}

        monkeypatch.setattr(flat_files, "resolve_parser", lambda ref: toy_parse)

        spec = _toy_spec(uses_xwalk=False, xwalk_fields=())
        with pytest.raises(ParserStructureError):
            build_flat_file_source(spec, b"raw-bytes", _toy_ctx(), resolver=None)

    def test_no_xwalk_source_passes_rows_through(self, monkeypatch):
        def toy_parse(raw, ctx):
            yield {"id": 1, "value": "anything"}

        monkeypatch.setattr(flat_files, "resolve_parser", lambda ref: toy_parse)

        spec = _toy_spec(
            uses_xwalk=False,
            xwalk_fields=(),
            child_table=None,
            child_primary_key=(),
        )
        source = build_flat_file_source(spec, b"raw-bytes", _toy_ctx(), resolver=None)

        assert set(source.resources.keys()) == {"toy_main"}
        rows = list(source.resources["toy_main"])
        assert rows == [{"id": 1, "value": "anything"}]
