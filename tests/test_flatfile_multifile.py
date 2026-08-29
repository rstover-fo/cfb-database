"""Unit tests for the per-season multi-file framework (B6a): template
resolution, per-file ledger keying, --due current-season-only behavior, and
the 404 fallback path in ``scripts/load_flat_files.py``'s
``_fetch_seasoned``. Mocks ``fetch_file``/the ledger; no live DB, no network.
"""

from dataclasses import replace
from datetime import date, datetime

import httpx
import pytest

import scripts.load_flat_files as load_flat_files
from scripts.load_flat_files import FALLBACK_MAX_STEPS, _fetch_seasoned
from src.pipelines.sources.flat_files import (
    REGISTRY,
    FlatFileSpec,
    SeasonNotPublishedError,
    ledger_key,
    resolve_fetch_url,
)
from src.pipelines.utils.file_fetcher import FetchedFile


def _http_error(status_code: int, url: str = "https://example.com/file.parquet"):
    request = httpx.Request("GET", url)
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(f"{status_code}", request=request, response=response)


def _template_spec(**overrides) -> FlatFileSpec:
    base = FlatFileSpec(
        name="toy_seasoned",
        parser="toy.parse",
        schema="test_schema",
        table="toy_main",
        primary_key=("season", "id"),
        cadence="weekly",
        url_template="https://example.com/toy_{season}.parquet",
        fallback_latest=True,
    )
    return replace(base, **overrides)


def _single_file_spec(**overrides) -> FlatFileSpec:
    base = FlatFileSpec(
        name="toy_single",
        parser="toy.parse",
        schema="test_schema",
        table="toy_main",
        primary_key=("id",),
        cadence="weekly",
        fetch_url="https://example.com/toy.parquet",
    )
    return replace(base, **overrides)


# ---------------------------------------------------------------------------
# resolve_fetch_url
# ---------------------------------------------------------------------------


class TestResolveFetchUrl:
    def test_single_file_spec_ignores_season(self):
        spec = _single_file_spec()
        assert resolve_fetch_url(spec, 2020) == "https://example.com/toy.parquet"
        assert resolve_fetch_url(spec, 2099) == "https://example.com/toy.parquet"

    def test_template_spec_substitutes_season(self):
        spec = _template_spec()
        assert resolve_fetch_url(spec, 2025) == "https://example.com/toy_2025.parquet"
        assert resolve_fetch_url(spec, 2026) == "https://example.com/toy_2026.parquet"

    def test_archiver_spec_with_neither_returns_none(self):
        spec = _single_file_spec(fetch_url=None, kind="archiver")
        assert resolve_fetch_url(spec, 2025) is None


# ---------------------------------------------------------------------------
# ledger_key
# ---------------------------------------------------------------------------


class TestLedgerKey:
    def test_single_file_spec_uses_bare_name(self):
        spec = _single_file_spec()
        assert ledger_key(spec, 2020) == "toy_single"
        assert ledger_key(spec, 2099) == "toy_single"  # season irrelevant

    def test_template_spec_scopes_by_season(self):
        spec = _template_spec()
        assert ledger_key(spec, 2025) == "toy_seasoned:2025"
        assert ledger_key(spec, 2026) == "toy_seasoned:2026"
        assert ledger_key(spec, 2025) != ledger_key(spec, 2026)


# ---------------------------------------------------------------------------
# --due current-season-only semantics (the actual bug this design prevents)
# ---------------------------------------------------------------------------


class TestDueIsPerSeasonNotPerSource:
    def test_recent_historical_backfill_does_not_suppress_current_season_due_check(
        self, monkeypatch
    ):
        """A --season 2019 backfill loaded TODAY must not make the CURRENT
        season's file look 'recently loaded' -- is_due for the current
        season must check ITS OWN ledger key, not the source's most recent
        load of any season."""
        spec = _template_spec()
        today = date(2025, 9, 15)
        current_season = 2025

        def fake_last_success(name: str):
            # Only the 2019 backfill has ever been recorded -- moments ago.
            if name == ledger_key(spec, 2019):
                return datetime(2025, 9, 15, 0, 1)
            return None

        monkeypatch.setattr(load_flat_files, "last_success", fake_last_success)

        current_last = load_flat_files._safe_last_success(ledger_key(spec, current_season))
        assert current_last is None
        assert load_flat_files.is_due(spec, current_last, today) is True

    def test_shared_bare_name_would_have_incorrectly_suppressed_it(self, monkeypatch):
        """Demonstrates the bug ledger_key() avoids: checking under the bare
        source name instead of the season-scoped key picks up the
        historical backfill's recency and wrongly reports not-due."""
        spec = _template_spec()
        today = date(2025, 9, 15)

        def fake_last_success(name: str):
            if name == spec.name:  # the old, un-scoped behavior
                return datetime(2025, 9, 15, 0, 1)
            return None

        monkeypatch.setattr(load_flat_files, "last_success", fake_last_success)

        wrong_last = load_flat_files._safe_last_success(spec.name)
        assert load_flat_files.is_due(spec, wrong_last, today) is False  # the bug


# ---------------------------------------------------------------------------
# _fetch_seasoned: 404 fallback
# ---------------------------------------------------------------------------

FAKE_FETCHED = FetchedFile(content=b"bytes", sha256="a" * 64, source_url="https://x/y.parquet")


class TestFetchSeasonedSuccess:
    def test_no_fallback_needed_when_season_available(self, monkeypatch):
        spec = _template_spec()
        calls = []

        def fake_fetch_file(url, **kw):
            calls.append(url)
            return FAKE_FETCHED

        monkeypatch.setattr(load_flat_files, "fetch_file", fake_fetch_file)

        fetched, resolved = _fetch_seasoned(spec, 2025, allow_fallback=True)
        assert fetched is FAKE_FETCHED
        assert resolved == 2025
        assert calls == ["https://example.com/toy_2025.parquet"]


class TestFetchSeasonedFallback:
    def test_404_falls_back_one_season(self, monkeypatch):
        spec = _template_spec()

        def fake_fetch_file(url, **kw):
            if "2026" in url:
                raise _http_error(404, url)
            return FAKE_FETCHED

        monkeypatch.setattr(load_flat_files, "fetch_file", fake_fetch_file)

        fetched, resolved = _fetch_seasoned(spec, 2026, allow_fallback=True)
        assert fetched is FAKE_FETCHED
        assert resolved == 2025

    def test_404_falls_back_multiple_seasons_within_bound(self, monkeypatch):
        spec = _template_spec()

        def fake_fetch_file(url, **kw):
            if "2023" in url:
                return FAKE_FETCHED
            raise _http_error(404, url)

        monkeypatch.setattr(load_flat_files, "fetch_file", fake_fetch_file)

        fetched, resolved = _fetch_seasoned(spec, 2026, allow_fallback=True)
        assert resolved == 2023  # 3 steps back: 2025, 2024, 2023

    def test_fallback_exhausted_raises_season_not_published(self, monkeypatch):
        spec = _template_spec()
        monkeypatch.setattr(
            load_flat_files,
            "fetch_file",
            lambda url, **kw: (_ for _ in ()).throw(_http_error(404, url)),
        )

        with pytest.raises(SeasonNotPublishedError):
            _fetch_seasoned(spec, 2026, allow_fallback=True)

    def test_fallback_bounded_by_max_steps(self, monkeypatch):
        spec = _template_spec()
        attempted_seasons = []

        def fake_fetch_file(url, **kw):
            for s in range(2013, 2030):
                if str(s) in url:
                    attempted_seasons.append(s)
                    break
            raise _http_error(404, url)

        monkeypatch.setattr(load_flat_files, "fetch_file", fake_fetch_file)

        with pytest.raises(SeasonNotPublishedError):
            _fetch_seasoned(spec, 2026, allow_fallback=True)

        # original season + FALLBACK_MAX_STEPS fallback attempts, no more
        assert len(attempted_seasons) == 1 + FALLBACK_MAX_STEPS

    def test_fallback_bounded_by_min_season(self, monkeypatch):
        spec = _template_spec(min_season=2025)  # only the requested season is in-bounds
        monkeypatch.setattr(
            load_flat_files,
            "fetch_file",
            lambda url, **kw: (_ for _ in ()).throw(_http_error(404, url)),
        )

        with pytest.raises(SeasonNotPublishedError):
            _fetch_seasoned(spec, 2026, allow_fallback=True)


class TestFetchSeasonedNoFallback:
    def test_fallback_latest_false_raises_immediately_on_404(self, monkeypatch):
        spec = _template_spec(fallback_latest=False)
        calls = []

        def fake_fetch_file(url, **kw):
            calls.append(url)
            raise _http_error(404, url)

        monkeypatch.setattr(load_flat_files, "fetch_file", fake_fetch_file)

        with pytest.raises(SeasonNotPublishedError):
            _fetch_seasoned(spec, 2026, allow_fallback=True)
        assert len(calls) == 1  # no fallback probing attempted

    def test_explicit_season_disables_fallback_even_if_spec_opts_in(self, monkeypatch):
        """An explicit --season request must never be silently substituted --
        allow_fallback=False (season_explicit) wins over fallback_latest=True."""
        spec = _template_spec(fallback_latest=True)
        calls = []

        def fake_fetch_file(url, **kw):
            calls.append(url)
            raise _http_error(404, url)

        monkeypatch.setattr(load_flat_files, "fetch_file", fake_fetch_file)

        with pytest.raises(SeasonNotPublishedError):
            _fetch_seasoned(spec, 2020, allow_fallback=False)
        assert len(calls) == 1

    def test_non_404_error_propagates_unchanged(self, monkeypatch):
        spec = _template_spec()

        def fake_fetch_file(url, **kw):
            raise _http_error(500, url)

        monkeypatch.setattr(load_flat_files, "fetch_file", fake_fetch_file)

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            _fetch_seasoned(spec, 2026, allow_fallback=True)
        assert exc_info.value.response.status_code == 500


# ---------------------------------------------------------------------------
# run_source integration: fallback success records under the resolved
# season's ledger key, and SeasonNotPublishedError maps to not_published
# ---------------------------------------------------------------------------


class TestRunSourceSeasonFallback:
    def test_fallback_success_records_under_resolved_season_ledger_key(self, monkeypatch):
        spec = _template_spec()

        def fake_fetch_file(url, **kw):
            if "2026" in url:
                raise _http_error(404, url)
            return FAKE_FETCHED

        monkeypatch.setattr(load_flat_files, "fetch_file", fake_fetch_file)
        monkeypatch.setattr(load_flat_files, "already_loaded", lambda *a, **k: False)

        record_calls = []
        monkeypatch.setattr(
            load_flat_files, "record_load", lambda *a, **k: record_calls.append((a, k))
        )

        class _FakeNormalizeInfo:
            row_counts = {"toy_main": 5}

        class _FakeTrace:
            last_normalize_info = _FakeNormalizeInfo()

        class _FakePipeline:
            last_trace = _FakeTrace()

            def run(self, source_obj):
                return None

        monkeypatch.setattr(load_flat_files.dlt, "pipeline", lambda **kw: _FakePipeline())
        monkeypatch.setattr(load_flat_files, "build_flat_file_source", lambda *a, **k: object())

        result = load_flat_files.run_source(
            spec, season=2026, season_explicit=False, today=date(2026, 9, 1)
        )

        assert result["status"] == "loaded"
        assert result["rows"] == 5
        args, kwargs = record_calls[0]
        assert args[0] == "toy_seasoned:2025"  # resolved season, not the requested 2026

    def test_season_not_published_maps_to_not_published_status(self, monkeypatch):
        spec = _template_spec(fallback_latest=False)
        monkeypatch.setattr(
            load_flat_files,
            "fetch_file",
            lambda url, **kw: (_ for _ in ()).throw(_http_error(404, url)),
        )
        monkeypatch.setattr(
            load_flat_files,
            "record_load",
            lambda *a, **k: (_ for _ in ()).throw(
                AssertionError("no sha to key on -- ledger must not be touched")
            ),
        )

        result = load_flat_files.run_source(
            spec, season=2026, season_explicit=False, today=date(2026, 9, 1)
        )

        assert result["status"] == "not_published"
        assert result["sha"] is None

    def test_explicit_season_request_that_404s_fails_loud_not_published(self, monkeypatch):
        """--season 2026 explicitly on a fallback_latest=True spec: still no
        silent substitution -- season_explicit disables fallback."""
        spec = _template_spec(fallback_latest=True)
        calls = []

        def fake_fetch_file(url, **kw):
            calls.append(url)
            raise _http_error(404, url)

        monkeypatch.setattr(load_flat_files, "fetch_file", fake_fetch_file)

        result = load_flat_files.run_source(
            spec, season=2026, season_explicit=True, today=date(2026, 9, 1)
        )

        assert result["status"] == "not_published"
        assert len(calls) == 1  # no fallback probing


# ---------------------------------------------------------------------------
# main(): --season sets season_explicit through to run_source
# ---------------------------------------------------------------------------


class TestMainThreadsSeasonExplicit:
    def test_due_run_allows_fallback(self, monkeypatch):
        captured = {}

        def fake_run_source(
            spec, *, file_path=None, season=None, season_explicit=False, today=None
        ):
            captured["season_explicit"] = season_explicit
            return {
                "source": spec.name,
                "status": "loaded",
                "rows": 0,
                "sha": None,
                "duration_s": 0.0,
                "error": None,
                "unmapped": None,
                "gaps": None,
            }

        monkeypatch.setattr(load_flat_files, "run_source", fake_run_source)
        monkeypatch.setattr(load_flat_files, "_safe_last_success", lambda *a, **k: None)

        load_flat_files.main(["--source", "sdv_team_xwalk"])
        assert captured["season_explicit"] is False

    def test_explicit_season_flag_disables_fallback(self, monkeypatch):
        captured = {}

        def fake_run_source(
            spec, *, file_path=None, season=None, season_explicit=False, today=None
        ):
            captured["season_explicit"] = season_explicit
            captured["season"] = season
            return {
                "source": spec.name,
                "status": "loaded",
                "rows": 0,
                "sha": None,
                "duration_s": 0.0,
                "error": None,
                "unmapped": None,
                "gaps": None,
            }

        monkeypatch.setattr(load_flat_files, "run_source", fake_run_source)

        load_flat_files.main(["--source", "sdv_team_xwalk", "--season", "2020"])
        assert captured["season_explicit"] is True
        assert captured["season"] == 2020


# ---------------------------------------------------------------------------
# The four migrated sdv_* specs and seven new ncaa_* specs use the template
# mechanism correctly (registry-level sanity, no network)
# ---------------------------------------------------------------------------


class TestMigratedAndNewRegistryEntries:
    @pytest.mark.parametrize(
        "name",
        [
            "sdv_team_xwalk",
            "sdv_game_xwalk",
            "sdv_fpi_weekly",
            "sdv_ratings_weekly",
            "ncaa_schedule",
            "ncaa_teams",
            "ncaa_rosters",
            "ncaa_linescores",
            "ncaa_player_stats",
            "ncaa_team_stats",
            "ncaa_pbp",
        ],
    )
    def test_uses_template_with_fallback(self, name):
        spec = REGISTRY[name]
        assert spec.fetch_url is None
        assert spec.url_template is not None
        assert "{season}" in spec.url_template
        assert spec.fallback_latest is True

    def test_ncaa_specs_share_min_season_floor(self):
        for name in (
            "ncaa_schedule",
            "ncaa_teams",
            "ncaa_rosters",
            "ncaa_linescores",
            "ncaa_player_stats",
            "ncaa_team_stats",
            "ncaa_pbp",
        ):
            assert REGISTRY[name].min_season == 2013

    def test_ncaa_specs_target_ncaa_schema(self):
        for name in (
            "ncaa_schedule",
            "ncaa_teams",
            "ncaa_rosters",
            "ncaa_linescores",
            "ncaa_player_stats",
            "ncaa_team_stats",
            "ncaa_pbp",
        ):
            assert REGISTRY[name].schema == "ncaa"

    def test_ncaa_cadences(self):
        assert REGISTRY["ncaa_teams"].cadence == "annual"
        assert REGISTRY["ncaa_rosters"].cadence == "annual"
        for name in (
            "ncaa_schedule",
            "ncaa_linescores",
            "ncaa_player_stats",
            "ncaa_team_stats",
            "ncaa_pbp",
        ):
            assert REGISTRY[name].cadence == "weekly"
