"""Unit tests for load_season's season-selection helpers (no DB, no API)."""

from scripts.load_season import (
    ESTIMATED_CALLS,
    IMMUTABLE_ONCE_FINAL,
    MIN_GAMES_FOR_FINISHED_SEASON,
    SEASON_COMPLETE_THRESHOLD,
    SOURCE_ORDER,
    load_season,
    season_is_final,
    sources_to_skip,
    upcoming_schedule_season,
)


class TestUpcomingScheduleSeason:
    def test_pre_august_months_refresh_next_schedule(self):
        # Jan-Jul: get_current_season() points at last calendar year's season,
        # so the upcoming season's schedule needs its own refresh (same month
        # boundary as get_current_season)
        for month in (1, 2, 3, 4, 5, 6, 7):
            assert upcoming_schedule_season(2025, month) == 2026

    def test_in_season_months_skip(self):
        for month in (8, 9, 10, 11, 12):
            assert upcoming_schedule_season(2026, month) is None


class TestMetricsWpWiring:
    """P3.2 Lane B: metrics_wp must be wired into the same places every other
    source is (SOURCE_ORDER, ESTIMATED_CALLS, runners, active-by-default),
    with zero workflow-file changes -- the daily workflow already runs
    load_season.py --weekly over all default-active sources."""

    def test_metrics_wp_in_source_order(self):
        assert "metrics_wp" in SOURCE_ORDER

    def test_metrics_wp_has_estimated_calls(self):
        assert "metrics_wp" in ESTIMATED_CALLS
        assert ESTIMATED_CALLS["metrics_wp"] == 70

    def test_metrics_wp_active_by_default(self):
        """Only "rosters" is excluded from the default active-source list
        (it requires --teams); metrics_wp must NOT be excluded the same way,
        or the daily workflow silently never runs it."""
        default_active = [s for s in SOURCE_ORDER if s != "rosters"]
        assert "metrics_wp" in default_active

    def test_dry_run_includes_metrics_wp_estimate(self, capsys):
        """load_season's dry-run path is pure printing (rate limiter reads a
        local state file only) -- no DB, no API -- so it's safe to exercise
        directly and confirm metrics_wp's estimate surfaces in the plan."""
        summary = load_season(season=2024, sources=["metrics_wp"], dry_run=True)

        assert summary["dry_run"] is True
        assert summary["estimated_calls"] == ESTIMATED_CALLS["metrics_wp"]

        captured = capsys.readouterr()
        assert "metrics_wp" in captured.out


class FakeCursor:
    def __init__(self, row):
        self._row = row

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *a, **k):
        pass

    def fetchone(self):
        return self._row


class FakeConn:
    def __init__(self, row):
        self._row = row

    def cursor(self):
        return FakeCursor(self._row)


class TestSeasonIsFinal:
    """The daily workflow runs with no --season, so get_current_season()
    resolves to `year - 1` until August: every off-season run re-ingested the
    entire, complete, immutable previous season. plays fans out to one
    /plays/stats call PER GAME and rosters to one per team -- roughly 2,000
    calls a day against a 75,000/month budget, for data that cannot change.
    That is what exhausted the quota behind the 2026-07-25 three-hour run."""

    def test_a_completed_season_is_final(self):
        assert season_is_final(FakeConn((900, 1.0)), 2025) is True

    def test_tolerance_allows_a_stray_uncompleted_game(self):
        """A cancellation that never completes must not freeze a season as
        unfinished forever -- that would permanently disable the skip."""
        assert season_is_final(FakeConn((900, 0.995)), 2025) is True

    def test_a_season_in_progress_is_not_final(self):
        assert season_is_final(FakeConn((900, 0.60)), 2026) is False

    def test_just_below_threshold_is_not_final(self):
        assert season_is_final(FakeConn((900, SEASON_COMPLETE_THRESHOLD - 0.01)), 2026) is False

    def test_too_few_games_is_not_final(self):
        """Week 1 of a new season is 100% complete over three games. Without a
        floor that would read as finished and skip the entire ingest."""
        assert season_is_final(FakeConn((3, 1.0)), 2026) is False
        assert season_is_final(FakeConn((MIN_GAMES_FOR_FINISHED_SEASON - 1, 1.0)), 2026) is False

    def test_an_unloaded_season_is_not_final(self):
        assert season_is_final(FakeConn((0, None)), 2027) is False

    def test_null_percentage_does_not_crash(self):
        assert season_is_final(FakeConn((500, None)), 2027) is False


class TestSourcesToSkip:
    def test_immutable_sources_are_skipped_for_a_finished_season(self):
        skipped = sources_to_skip(list(SOURCE_ORDER), season_final=True, allow_skip=True)
        assert "plays" in skipped
        assert "rosters" in skipped

    def test_nothing_is_skipped_mid_season(self):
        assert sources_to_skip(list(SOURCE_ORDER), season_final=False, allow_skip=True) == []

    def test_explicit_request_never_skips(self):
        """The hazard this guard must not introduce: `--season 2019 --sources
        plays` targets a finished season BY DEFINITION, so skipping there
        would turn every backfill into a silent no-op."""
        assert sources_to_skip(["plays"], season_final=True, allow_skip=False) == []

    def test_reference_is_never_skipped(self):
        """No year filter and ~10 calls -- always cheap, and it is how new
        teams and venues arrive."""
        skipped = sources_to_skip(list(SOURCE_ORDER), season_final=True, allow_skip=True)
        assert "reference" not in skipped

    def test_metrics_wp_is_skipped_once_the_season_is_final(self):
        """It was exempted as "already self-limiting: it fetches only games
        still missing win probability, so a fully-backfilled season costs
        nothing". The 2026-07-26 daily load disproved that -- it reported 2,517
        of 3,829 games still missing for a season that ended in January,
        because games CFBD has no win-probability data for are missing forever
        and come back every single day. Self-limiting requires the missing set
        to drain; this one does not.
        """
        skipped = sources_to_skip(list(SOURCE_ORDER), season_final=True, allow_skip=True)
        assert "metrics_wp" in skipped

    def test_metrics_wp_still_runs_during_a_live_season(self):
        """The skip is conditional on the season being FINISHED. In-game win
        probability for a game that has not been played yet is exactly the data
        this source exists to collect."""
        skipped = sources_to_skip(list(SOURCE_ORDER), season_final=False, allow_skip=True)
        assert "metrics_wp" not in skipped

    def test_every_immutable_source_is_a_real_source(self):
        """A typo here would silently skip nothing at all."""
        assert IMMUTABLE_ONCE_FINAL <= set(SOURCE_ORDER)

    def test_the_expensive_sources_are_covered(self):
        """The point of the change. plays fans out per game and rosters per
        team; if either escaped the list the daily burn would be unchanged."""
        for src in ("plays", "rosters", "game_stats"):
            assert src in IMMUTABLE_ONCE_FINAL

    def test_skipping_removes_the_bulk_of_the_daily_budget(self):
        """Quantifies the fix: the default daily source set drops to a small
        fraction of its estimated calls once the season is finished."""
        default = [s for s in SOURCE_ORDER if s != "rosters"]
        before = sum(ESTIMATED_CALLS.get(s, 50) for s in default)
        skipped = sources_to_skip(default, season_final=True, allow_skip=True)
        after = sum(ESTIMATED_CALLS.get(s, 50) for s in default if s not in skipped)
        assert after < before * 0.15, f"expected a large reduction, got {before} -> {after}"
