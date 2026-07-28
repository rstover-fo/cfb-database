"""Unit tests for load_season's season-selection helpers (no DB, no API)."""

import pytest

from scripts.load_season import (
    ESTIMATED_CALLS,
    IMMUTABLE_ONCE_FINAL,
    MIN_GAMES_FOR_FINISHED_SEASON,
    PRESEASON_ESTIMATED_CALLS,
    PRESEASON_INPUT_SOURCES,
    PRESEASON_STATS_RESOURCES,
    SEASON_COMPLETE_THRESHOLD,
    SOURCE_ORDER,
    load_season,
    parse_source_specs,
    season_is_final,
    sources_to_skip,
    upcoming_schedule_season,
    validate_resource_filters,
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


class TestParseSourceSpecs:
    """`--sources stats` costs ~1,640 calls for a season with a full schedule
    because play_stats is one request per game. A one-off load of returning
    production should not have to pay that."""

    def test_plain_sources_have_no_filter(self):
        assert parse_source_specs(["games", "stats"]) == (["games", "stats"], {})

    def test_a_resource_filter_is_split_out(self):
        names, filters = parse_source_specs(["stats:player_returning"])
        assert names == ["stats"]
        assert filters == {"stats": ["player_returning"]}

    def test_multiple_resources_use_plus(self):
        """Comma already separates sources, so it cannot separate resources."""
        _, filters = parse_source_specs(["stats:player_returning+player_usage"])
        assert filters["stats"] == ["player_returning", "player_usage"]

    def test_filtering_an_unfilterable_source_is_an_error(self):
        """Silently ignoring the filter would load the whole source at full
        price while the operator believed they had narrowed it."""
        with pytest.raises(ValueError, match="does not support a resource filter"):
            parse_source_specs(["games:schedule"])


class TestPreseasonInputRefresh:
    """The upcoming season's preseason inputs had no ingest path at all.

    Off-season the unattended run targets `year - 1`, which is finished, so
    every IMMUTABLE_ONCE_FINAL source is skipped -- and only games/betting
    were refreshed for the upcoming season. On 2026-07-28 the 2026 schedule
    had been loaded for months while stats.player_returning,
    ratings.sp_ratings, recruiting.team_talent and recruiting.team_recruiting
    all had zero 2026 rows, so 2026 returning production could not be
    answered at all.
    """

    def test_preseason_sources_are_real_sources(self):
        assert set(PRESEASON_INPUT_SOURCES) <= set(SOURCE_ORDER)

    def test_returning_production_source_is_covered(self):
        """stats carries /player/returning -> stats.player_returning ->
        marts.returning_production, the table that was empty for 2026."""
        assert "stats" in PRESEASON_INPUT_SOURCES

    def test_rosters_is_not_a_preseason_input(self):
        """One call per team (~150/day) and it does not firm up until August,
        when the normal in-season path picks it up anyway."""
        assert "rosters" not in PRESEASON_INPUT_SOURCES

    def test_the_refresh_is_cheap(self):
        """It runs every off-season day, so it has to stay far away from the
        per-game fan-out that exhausted the quota in the first place."""
        assert PRESEASON_ESTIMATED_CALLS <= 100

    def test_stats_is_restricted_to_named_resources(self):
        """The trap this class exists to avoid. The stats source is not
        uniformly priced: play_stats is one /plays/stats request PER GAME, so
        a source-grain daily refresh of `stats` for the upcoming season costs
        ~1,640 calls a day -- the same fan-out that exhausted the quota. Only
        the resources carrying preseason inputs may run."""
        assert "play_stats" not in PRESEASON_STATS_RESOURCES
        assert "player_returning" in PRESEASON_STATS_RESOURCES
        assert PRESEASON_ESTIMATED_CALLS < ESTIMATED_CALLS["stats"] / 10

    def test_named_stats_resources_exist(self):
        """A typo would raise at load time, inside the daily workflow."""
        from src.pipelines.sources.stats import stats_source

        source = stats_source(years=[2026], only=list(PRESEASON_STATS_RESOURCES))
        assert {r.name for r in source.resources.values()} == set(PRESEASON_STATS_RESOURCES)

    def test_dry_run_reports_the_preseason_refresh(self, capsys):
        load_season(season=2025, sources=["games"], dry_run=True, upcoming_schedule=2026)

        out = capsys.readouterr().out
        assert "2026 preseason inputs" in out
        for src in PRESEASON_INPUT_SOURCES:
            assert src in out
        assert "player_returning" in out


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
    calls a day against the then-75,000/month budget, for data that cannot change.
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

    def test_metrics_wp_stays_eligible_even_on_a_finished_season(self):
        """PR #54 review, P1.

        It was briefly added to the skip set because the 2026-07-26 daily load
        queued 2,517 games of a season that ended in January. But
        run_metrics_wp_pipeline derives missing games as completed-minus-loaded,
        so that backlog includes requests that failed during the quota
        exhaustion -- recoverable data, not just games CFBD has nothing for.
        The unattended daily path passes no --season, so skipping the source
        would strand those rows permanently.

        The cost problem is bounded in the pipeline instead
        (run.MAX_WP_GAMES_PER_RUN), which caps a run without making anything
        ineligible.
        """
        skipped = sources_to_skip(list(SOURCE_ORDER), season_final=True, allow_skip=True)
        assert "metrics_wp" not in skipped

        from src.pipelines.run import MAX_WP_GAMES_PER_RUN

        assert MAX_WP_GAMES_PER_RUN > 0, "the cap is what replaces the skip"

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


class TestWinProbabilityBacklogIsBounded:
    """PR #54 review, P1.

    The 2026-07-26 daily load queued 2,517 win-probability calls in one run --
    ~3% of the monthly budget, every day, for a season that ended in January.
    The first fix skipped the source once a season was final, which would have
    stranded any game whose request failed during the quota exhaustion. Capping
    the run bounds the cost without making anything permanently ineligible.
    """

    def test_the_cap_is_smaller_than_the_backlog_that_motivated_it(self):
        from src.pipelines.run import MAX_WP_GAMES_PER_RUN

        assert 0 < MAX_WP_GAMES_PER_RUN < 2517

    def test_newest_games_are_fetched_first(self):
        """Once the cap truncates, newly-completed games must not be starved
        behind a historical backlog -- and recency is the best proxy available
        for 'CFBD actually has win probability for this game'."""
        from src.pipelines.run import _METRICS_WP_GAMES_QUERY

        order_by = _METRICS_WP_GAMES_QUERY.upper().split("ORDER BY")[1]
        assert "SEASON DESC" in order_by
        assert "START_DATE DESC" in order_by

    def test_truncation_is_reported_not_silent(self):
        """A capped backlog that reads like a completed one is how '2,517
        missing' went unnoticed for months."""
        import inspect

        from src.pipelines.run import run_metrics_wp_pipeline

        body = inspect.getsource(run_metrics_wp_pipeline)
        assert "deferred" in body
        assert "Backlog capped" in body

    def test_summary_reports_the_full_backlog_not_the_capped_slice(self):
        """`missing` has to keep meaning "how much is left", or the cap makes
        the backlog look like it is draining when it is not."""
        import inspect

        from src.pipelines.run import run_metrics_wp_pipeline

        body = inspect.getsource(run_metrics_wp_pipeline)
        assert '"missing": total_missing' in body
        assert '"loaded_this_run"' in body


class TestResourceFilterValidation:
    """A 2026-07-28 backfill passed `stats:returning_production` -- the MART
    name -- and got through ratings and recruiting before stats failed, with
    the valid-names hint buried under dlt's load logs."""

    def test_the_mart_name_resolves_to_the_resource(self):
        _, filters = parse_source_specs(["stats:returning_production"])
        assert filters["stats"] == ["player_returning"]

    def test_a_real_typo_still_fails(self):
        with pytest.raises(ValueError, match="Unknown stats resource"):
            validate_resource_filters({"stats": ["player_retuning"]})

    def test_valid_names_are_listed_in_the_error(self):
        """The whole point: the fix has to be readable off the error."""
        with pytest.raises(ValueError, match="player_returning"):
            validate_resource_filters({"stats": ["nope"]})

    def test_known_resources_pass(self):
        validate_resource_filters({"stats": ["player_returning", "play_stats"]})

    def test_load_season_rejects_before_running_anything(self):
        """It must fail at parse time, not after other sources have run."""
        summary = load_season(season=2026, sources=["stats:nope"], dry_run=True)

        assert "error" in summary
        assert "Unknown stats resource" in summary["error"]
