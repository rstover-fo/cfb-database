"""Unit tests for load_season's season-selection helpers (no DB, no API)."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
import yaml

from scripts.load_season import (
    ESTIMATED_CALLS,
    IMMUTABLE_ONCE_FINAL,
    PRESEASON_ESTIMATED_CALLS,
    PRESEASON_INPUT_SOURCES,
    PRESEASON_STATS_RESOURCES,
    SOURCE_ORDER,
    apply_aggregate_timeout_tolerance,
    load_season,
    load_tolerated_streak_state,
    parse_source_specs,
    sources_to_skip,
    upcoming_schedule_season,
    update_tolerated_streaks,
    validate_resource_filters,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS_DIR = PROJECT_ROOT / ".github" / "workflows"


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


class TestExpansionUnitWiring:
    """A2 unit (2026-08-29): playoffs, coaches, conferences wired into
    load_season.py's automated orchestration; coach_tenures deliberately is
    not (per-team fan-out, backfill/preseason only)."""

    def test_new_sources_are_in_source_order(self):
        for src in ("conferences", "coaches", "playoffs"):
            assert src in SOURCE_ORDER

    def test_coach_tenures_is_not_a_source_order_member(self):
        """Per-team fan-out -- reachable only via run.py --source
        coach_tenures, never the automated daily/backfill path."""
        assert "coach_tenures" not in SOURCE_ORDER

    def test_playoffs_runs_after_games(self):
        """CFP participants/games reference core.games rows conceptually
        (though not via a DB FK); games loads first regardless."""
        assert SOURCE_ORDER.index("games") < SOURCE_ORDER.index("playoffs")

    def test_conferences_and_coaches_run_before_games(self):
        assert SOURCE_ORDER.index("conferences") < SOURCE_ORDER.index("games")
        assert SOURCE_ORDER.index("coaches") < SOURCE_ORDER.index("games")

    def test_new_sources_have_estimated_calls(self):
        assert ESTIMATED_CALLS["conferences"] == 2
        assert ESTIMATED_CALLS["coaches"] == 1
        assert ESTIMATED_CALLS["playoffs"] == 3

    def test_ratings_estimate_includes_srs_expanded(self):
        assert ESTIMATED_CALLS["ratings"] == 13

    def test_stats_estimate_includes_the_three_new_resources(self):
        assert ESTIMATED_CALLS["stats"] == 1_675

    def test_playoffs_is_immutable_once_final(self):
        """CFP results for a finished season cannot change."""
        assert "playoffs" in IMMUTABLE_ONCE_FINAL

    def test_coaches_and_conferences_are_deliberately_not_immutable(self):
        """Coaching carousel and conference realignment both mutate
        off-season, well after a prior season is "final" -- skipping them
        would freeze data exactly when it changes. Same reasoning as the
        existing `reference` exclusion."""
        assert "coaches" not in IMMUTABLE_ONCE_FINAL
        assert "conferences" not in IMMUTABLE_ONCE_FINAL

    def test_new_immutable_source_is_a_real_source(self):
        assert IMMUTABLE_ONCE_FINAL <= set(SOURCE_ORDER)

    def test_new_sources_are_active_by_default(self):
        """Only "rosters" is excluded from the default active-source list;
        conferences/coaches/playoffs must not be excluded the same way."""
        default_active = [s for s in SOURCE_ORDER if s != "rosters"]
        for src in ("conferences", "coaches", "playoffs"):
            assert src in default_active

    def test_dry_run_includes_the_new_sources(self, capsys):
        summary = load_season(
            season=2024, sources=["conferences", "coaches", "playoffs"], dry_run=True
        )

        assert summary["dry_run"] is True
        assert summary["estimated_calls"] == (
            ESTIMATED_CALLS["conferences"]
            + ESTIMATED_CALLS["coaches"]
            + ESTIMATED_CALLS["playoffs"]
        )
        out = capsys.readouterr().out
        assert "conferences" in out
        assert "coaches" in out
        assert "playoffs" in out


class TestFanoutDrainerWiring:
    """A4 unit (2026-08-29): the two per-entity fan-out drainers,
    coach_profiles (ref.coach_profiles) and player_overview
    (stats.player_season_overview), wired into load_season.py's automated
    orchestration the same way metrics_wp was -- SOURCE_ORDER, ESTIMATED_CALLS
    (a cap, not a steady-state estimate), active-by-default, and deliberately
    excluded from IMMUTABLE_ONCE_FINAL for opposite reasons from each other."""

    def test_new_sources_are_in_source_order(self):
        for src in ("coach_profiles", "player_overview"):
            assert src in SOURCE_ORDER

    def test_coach_profiles_runs_after_coaches(self):
        """coach_profiles' candidates come from ref.coach_seasons (coaches),
        though the drainer itself re-queries the DB rather than taking a
        team/id list from the coaches step -- ordering documents the
        conceptual dependency the same way playoffs/games does."""
        assert SOURCE_ORDER.index("coaches") < SOURCE_ORDER.index("coach_profiles")

    def test_player_overview_runs_before_rosters(self):
        """rosters is excluded from the default active-source list (see
        test_new_sources_are_active_by_default); player_overview must stay
        ahead of it in SOURCE_ORDER regardless."""
        assert SOURCE_ORDER.index("player_overview") < SOURCE_ORDER.index("rosters")

    def test_new_sources_have_estimated_calls(self):
        """Both are caps on a backlog drainer, not steady-state estimates --
        see the ESTIMATED_CALLS comments for coach_profiles/player_overview."""
        assert ESTIMATED_CALLS["coach_profiles"] == 200
        assert ESTIMATED_CALLS["player_overview"] == 250

    def test_new_sources_are_active_by_default(self):
        """Only "rosters" is excluded from the default active-source list;
        coach_profiles/player_overview must not be excluded the same way."""
        default_active = [s for s in SOURCE_ORDER if s != "rosters"]
        for src in ("coach_profiles", "player_overview"):
            assert src in default_active

    def test_coach_profiles_is_not_immutable_once_final(self):
        """The candidate set (every coach id ever seen in coach_seasons)
        keeps growing regardless of whether any one season is finished -- a
        coaching change is itself the event that adds a new id. Cost is
        bounded by run.MAX_COACH_PROFILES_PER_RUN instead, same remedy as
        metrics_wp."""
        assert "coach_profiles" not in IMMUTABLE_ONCE_FINAL

    def test_player_overview_is_not_immutable_once_final(self):
        """Opposite reasoning from coach_profiles: player_overview's data
        genuinely IS immutable once a season is final -- which is exactly
        why finished seasons are its entire duty cycle rather than an
        exemption from one. The completed-season gate lives inside
        run_player_overview_pipeline, not this frozenset."""
        assert "player_overview" not in IMMUTABLE_ONCE_FINAL

    def test_both_new_sources_are_real_sources(self):
        assert IMMUTABLE_ONCE_FINAL <= set(SOURCE_ORDER)

    def test_dry_run_includes_the_new_sources(self, capsys):
        summary = load_season(
            season=2024, sources=["coach_profiles", "player_overview"], dry_run=True
        )

        assert summary["dry_run"] is True
        assert summary["estimated_calls"] == (
            ESTIMATED_CALLS["coach_profiles"] + ESTIMATED_CALLS["player_overview"]
        )
        out = capsys.readouterr().out
        assert "coach_profiles" in out
        assert "player_overview" in out

    def test_player_overview_max_env_var_is_forwarded(self, monkeypatch):
        """PLAYER_OVERVIEW_MAX_PER_RUN (wired from backfill-sources.yml's
        player_overview_max input) raises the drainer's CAP only -- the
        runner lambda must read it at call time (not bind a stale default
        at import time) and forward it as max_players."""
        captured = {}

        def fake_run_player_overview_pipeline(**kwargs):
            captured.update(kwargs)
            return {"missing": 0}

        monkeypatch.setattr(
            "src.pipelines.run.run_player_overview_pipeline",
            fake_run_player_overview_pipeline,
        )
        monkeypatch.setenv("PLAYER_OVERVIEW_MAX_PER_RUN", "9000")

        load_season(season=2024, sources=["player_overview"], dry_run=False, skip_refresh=True)

        assert captured == {"max_players": 9000}

    def test_player_overview_max_defaults_when_env_absent(self, monkeypatch):
        """No env var (the daily path) must stay byte-identical to today:
        max_players=250."""
        captured = {}

        def fake_run_player_overview_pipeline(**kwargs):
            captured.update(kwargs)
            return {"missing": 0}

        monkeypatch.setattr(
            "src.pipelines.run.run_player_overview_pipeline",
            fake_run_player_overview_pipeline,
        )
        monkeypatch.delenv("PLAYER_OVERVIEW_MAX_PER_RUN", raising=False)

        load_season(season=2024, sources=["player_overview"], dry_run=False, skip_refresh=True)

        assert captured == {"max_players": 250}

    def test_player_overview_max_defaults_when_env_empty(self, monkeypatch):
        """GitHub sets an empty-string env var for an unset workflow input
        (backfill-sources.yml's player_overview_max defaults to ""), and
        int("") would crash -- the empty case must fall back to 250 exactly
        like the absent case, not raise."""
        captured = {}

        def fake_run_player_overview_pipeline(**kwargs):
            captured.update(kwargs)
            return {"missing": 0}

        monkeypatch.setattr(
            "src.pipelines.run.run_player_overview_pipeline",
            fake_run_player_overview_pipeline,
        )
        monkeypatch.setenv("PLAYER_OVERVIEW_MAX_PER_RUN", "")

        load_season(season=2024, sources=["player_overview"], dry_run=False, skip_refresh=True)

        assert captured == {"max_players": 250}


class TestPassingWiring:
    """Passing unit (spec v5.25.0, 2026-08-30): the five /passing charting
    endpoints wired into load_season.py's automated orchestration the same
    way the A2 unit's sources were -- SOURCE_ORDER, ESTIMATED_CALLS,
    active-by-default, and IMMUTABLE_ONCE_FINAL membership (same policy as
    `stats`, since parseStatus="partial" rows may be re-charted)."""

    def test_passing_is_in_source_order(self):
        assert "passing" in SOURCE_ORDER

    def test_passing_runs_after_stats(self):
        """Grouped with the other stats-domain aggregation sources -- not a
        hard dependency, just documenting where it conceptually belongs."""
        assert SOURCE_ORDER.index("stats") < SOURCE_ORDER.index("passing")

    def test_passing_has_estimated_calls(self):
        """3 week-iterated resources x ~20 calls. Was 62 until 2026-10-22,
        when the 2 season-grain aggregates moved into their own
        `passing_season` unit."""
        assert ESTIMATED_CALLS["passing"] == 60

    def test_passing_is_active_by_default(self):
        """Only "rosters" is excluded from the default active-source list;
        passing must not be excluded the same way."""
        default_active = [s for s in SOURCE_ORDER if s != "rosters"]
        assert "passing" in default_active

    def test_passing_is_immutable_once_final(self):
        """Same policy as stats -- a finished season's charting is only
        immutable-ish (parseStatus="partial" rows may be re-charted), but it
        rides explicit --season re-runs rather than a daily re-ingest."""
        assert "passing" in IMMUTABLE_ONCE_FINAL

    def test_passing_is_a_real_source(self):
        assert IMMUTABLE_ONCE_FINAL <= set(SOURCE_ORDER)

    def test_passing_is_not_in_preseason_or_resource_filterable(self):
        """Passing data starts 2025 and is week-or-team validated, not a
        preseason input, and has no per-resource cost skew like stats --
        it must not be added to either set."""
        from scripts.load_season import PRESEASON_INPUT_SOURCES, RESOURCE_FILTERABLE

        assert "passing" not in PRESEASON_INPUT_SOURCES
        assert "passing" not in RESOURCE_FILTERABLE

    def test_dry_run_includes_passing(self, capsys):
        summary = load_season(season=2025, sources=["passing"], dry_run=True)

        assert summary["dry_run"] is True
        assert summary["estimated_calls"] == ESTIMATED_CALLS["passing"]
        out = capsys.readouterr().out
        assert "passing" in out


class TestPassingSeasonWiring:
    """2026-10-22 split: passing_season (stats.passing_player_season +
    stats.passing_team_season) is its own source/load unit so a
    /passing/players/season timeout cannot discard the week-grain passing
    extract (Daily Season Load #99-#101). It must be wired into every place
    `passing` is -- SOURCE_ORDER, ESTIMATED_CALLS, IMMUTABLE_ONCE_FINAL,
    runners, active-by-default -- or the daily path silently stops loading
    the two season tables."""

    def test_in_source_order_after_passing(self):
        assert "passing_season" in SOURCE_ORDER
        assert SOURCE_ORDER.index("passing") < SOURCE_ORDER.index("passing_season")

    def test_has_an_estimated_call_count(self):
        """Two bare-year season-aggregate calls per season."""
        assert ESTIMATED_CALLS["passing_season"] == 2

    def test_split_preserves_the_combined_estimate(self):
        """60 + 2 must equal the pre-split 62 -- the split moved cost, it did
        not change it."""
        assert ESTIMATED_CALLS["passing"] + ESTIMATED_CALLS["passing_season"] == 62

    def test_is_immutable_once_final(self):
        """Same policy as passing: charting for a finished season is
        immutable-ish; corrections ride explicit --season re-runs."""
        assert "passing_season" in IMMUTABLE_ONCE_FINAL

    def test_is_active_by_default(self):
        """Only "rosters" is excluded from the default active-source list;
        the split must not quietly drop the two season tables from the daily
        load."""
        default_active = [s for s in SOURCE_ORDER if s != "rosters"]
        assert "passing_season" in default_active

    def test_is_non_critical_alongside_passing_and_rushing(self):
        """The point of the split plus tolerance: the slow aggregate's unit
        may be tolerated on the opt-in daily path."""
        from scripts.load_season import NON_CRITICAL_SOURCES

        assert NON_CRITICAL_SOURCES == frozenset({"passing", "passing_season", "rushing"})

    def test_dry_run_prices_the_split_units_separately(self, capsys):
        summary = load_season(season=2025, sources=["passing_season"], dry_run=True)

        assert summary["dry_run"] is True
        assert summary["estimated_calls"] == 2
        assert "passing_season" in capsys.readouterr().out


class TestRushingWiring:
    """Rushing unit (U3, 2026-09-03): the five /rushing charting endpoints
    wired into load_season.py's automated orchestration the same way the
    passing unit's sources were -- SOURCE_ORDER, ESTIMATED_CALLS,
    active-by-default, and IMMUTABLE_ONCE_FINAL membership (same policy as
    `passing`/`stats`, since 2025 is partially charted upstream and may be
    re-charted after the fact)."""

    def test_rushing_is_in_source_order(self):
        assert "rushing" in SOURCE_ORDER

    def test_rushing_runs_after_passing(self):
        """Grouped with the other stats-domain charting sources -- not a
        hard dependency, just documenting where it conceptually belongs."""
        assert SOURCE_ORDER.index("passing") < SOURCE_ORDER.index("rushing")

    def test_rushing_has_estimated_calls(self):
        """3 week-iterated resources x ~20 calls + 2 season-grain x 1 call."""
        assert ESTIMATED_CALLS["rushing"] == 62

    def test_rushing_is_active_by_default(self):
        """Only "rosters" is excluded from the default active-source list;
        rushing must not be excluded the same way."""
        default_active = [s for s in SOURCE_ORDER if s != "rosters"]
        assert "rushing" in default_active

    def test_rushing_is_immutable_once_final(self):
        """Same policy as passing/stats -- a finished season's charting is
        only immutable-ish (2025 is partially charted upstream and improves
        only via explicit --sources rushing re-pulls), so corrections ride
        explicit --season re-runs rather than a daily re-ingest."""
        assert "rushing" in IMMUTABLE_ONCE_FINAL

    def test_rushing_is_a_real_source(self):
        assert IMMUTABLE_ONCE_FINAL <= set(SOURCE_ORDER)

    def test_rushing_is_not_in_preseason_or_resource_filterable(self):
        """Rushing data starts 2025 and is week-or-team validated, not a
        preseason input, and has no per-resource cost skew like stats --
        it must not be added to either set."""
        from scripts.load_season import PRESEASON_INPUT_SOURCES, RESOURCE_FILTERABLE

        assert "rushing" not in PRESEASON_INPUT_SOURCES
        assert "rushing" not in RESOURCE_FILTERABLE

    def test_dry_run_includes_rushing(self, capsys):
        summary = load_season(season=2025, sources=["rushing"], dry_run=True)

        assert summary["dry_run"] is True
        assert summary["estimated_calls"] == ESTIMATED_CALLS["rushing"]
        out = capsys.readouterr().out
        assert "rushing" in out

    def test_rushing_is_in_run_py_source_choices(self):
        """run.py's --source argparse choices and dispatch dict must include
        rushing, mirroring passing. argparse raises SystemExit for an
        unrecognized choice, so a successful parse is the assertion."""
        from src.pipelines.run import create_parser

        parser = create_parser()
        args = parser.parse_args(["--source", "rushing"])
        assert args.source == "rushing"


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

    def test_games_schedule_reconciliation_is_never_skipped(self):
        skipped = sources_to_skip(list(SOURCE_ORDER), season_final=True, allow_skip=True)

        assert "games" not in skipped


class TestFinalSeasonScheduleReconciliation:
    def test_final_unattended_dispatch_uses_schedule_only_games(self):
        run_games = MagicMock(return_value="loaded")
        conn = MagicMock()
        limiter = MagicMock()
        limiter.get_status.return_value = {"remaining": 10_000}

        with (
            patch("scripts.load_season.season_is_final", return_value=True),
            patch("psycopg2.connect", return_value=conn),
            patch("scripts.compute_predictions.get_db_url", return_value="postgres://fake"),
            patch("src.pipelines.run.run_games_pipeline", run_games),
            patch("src.pipelines.utils.rate_limiter.get_rate_limiter", return_value=limiter),
        ):
            result = load_season(
                2025,
                sources=["games"],
                skip_refresh=True,
                allow_skip_final=True,
            )

        run_games.assert_called_once_with(years=[2025], schedule_only=True)
        assert result["results"]["games"]["status"] == "ok"

    def test_explicit_load_keeps_full_games_bundle(self):
        run_games = MagicMock(return_value="loaded")
        limiter = MagicMock()
        limiter.get_status.return_value = {"remaining": 10_000}

        with (
            patch("src.pipelines.run.run_games_pipeline", run_games),
            patch("src.pipelines.utils.rate_limiter.get_rate_limiter", return_value=limiter),
        ):
            load_season(2025, sources=["games"], skip_refresh=True, allow_skip_final=False)

        run_games.assert_called_once_with(years=[2025], schedule_only=False)

    def test_schedule_only_runner_does_not_fetch_drives_or_ancillary_resources(self):
        from src.pipelines.run import run_games_pipeline

        pipeline = MagicMock()
        pipeline.run.return_value = "loaded"
        source = MagicMock()

        with (
            patch("src.pipelines.run.dlt.pipeline", return_value=pipeline),
            patch("src.pipelines.run.games_source", return_value=source) as games_source,
        ):
            result = run_games_pipeline(years=[2025], schedule_only=True)

        assert result == "loaded"
        games_source.assert_called_once()
        source.with_resources.assert_called_once_with("games")
        pipeline.run.assert_called_once()

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
        fraction of its estimated calls once the season is finished.

        The floor is higher than it used to be (A4 unit, 2026-08-29):
        coach_profiles and player_overview are both drainer CAPS
        (MAX_COACH_PROFILES_PER_RUN=200, MAX_PLAYER_OVERVIEW_PER_RUN=250)
        that stay eligible on a finished season by design -- see
        IMMUTABLE_ONCE_FINAL's comment block -- so they add up to 450 to the
        post-skip total on top of the pre-existing floor (reference,
        coaches, conferences, metrics_wp). The invariant is still "skipping
        removes most of the cost," just not to <15% now that two more
        legitimately-non-skippable sources carry real per-run caps.
        """
        default = [s for s in SOURCE_ORDER if s != "rosters"]
        before = sum(ESTIMATED_CALLS.get(s, 50) for s in default)
        skipped = sources_to_skip(default, season_final=True, allow_skip=True)
        after = sum(ESTIMATED_CALLS.get(s, 50) for s in default if s not in skipped)
        assert after < before * 0.25, f"expected a large reduction, got {before} -> {after}"


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


class TestDependencyOrdering:
    """SOURCE_ORDER is dependency order -- "Source loading order matters:
    dependencies first" -- so the run order cannot be whatever the operator
    happened to type."""

    def test_sources_run_in_source_order_not_caller_order(self, capsys):
        """`--sources rosters,games` must load the schedule first: roster
        teams resolve FROM core.games, so the reverse order fails the roster
        load and then loads the games it needed."""
        load_season(season=2026, sources=["rosters", "games"], dry_run=True)

        out = capsys.readouterr().out
        assert out.index("games") < out.index("rosters")

    def test_ordering_survives_resource_filters(self, capsys):
        """A "source:res" spec still has to sort by its source name."""
        load_season(season=2026, sources=["stats:player_returning", "games"], dry_run=True)

        out = capsys.readouterr().out
        assert out.index("games") < out.index("stats:player_returning")

    def test_unknown_sources_are_reported_before_sorting(self):
        """SOURCE_ORDER.index would raise on an unknown name; the operator
        should get the named error instead of a ValueError."""
        summary = load_season(season=2026, sources=["games", "nonsense"], dry_run=True)

        assert "Unknown sources" in summary["error"]
        assert "nonsense" in summary["error"]


class TestMainExitCode:
    """A validation failure returns {"error": str} before any source runs;
    main() must exit nonzero on it, not just on per-source {"errors": int}.
    Otherwise a mistyped --sources in CI reports green having loaded nothing
    (found by review on the backfill-sources workflow)."""

    def test_unknown_source_exits_nonzero(self, monkeypatch):
        from scripts.load_season import main

        monkeypatch.setattr(
            "sys.argv",
            ["load_season.py", "--season", "2026", "--sources", "nonsense", "--dry-run"],
        )
        with pytest.raises(SystemExit) as excinfo:
            main()
        assert excinfo.value.code == 1

    def test_valid_dry_run_exits_zero(self, monkeypatch):
        from scripts.load_season import main

        monkeypatch.setattr(
            "sys.argv",
            ["load_season.py", "--season", "2026", "--sources", "games", "--dry-run"],
        )
        assert main() is None


class TestCountFailures:
    """_count_failures backs the summary tally, the row icon, and (via
    summary["errors"]) main()'s exit code -- all three must agree. Mart
    refresh reports status "partial" when some matviews refresh and others
    don't; that is not "ok" and must fail the run. Counting only
    status == "error" let a run with every source "ok" and a partial mart
    refresh print "[FAIL] _mart_refresh" plus "0 failed" and exit 0 -- a
    green run hiding a stale mart layer (found by review, 2026-09-01)."""

    def test_all_ok_is_zero_failures(self):
        from scripts.load_season import _count_failures

        results = {"games": {"status": "ok"}, "stats": {"status": "ok"}}

        assert _count_failures(results) == 0

    def test_partial_mart_refresh_counts_as_a_failure(self):
        from scripts.load_season import _count_failures

        results = {
            "games": {"status": "ok"},
            "_mart_refresh": {"status": "partial", "failures": 3},
        }

        assert _count_failures(results) == 1

    def test_literal_error_status_still_counts(self):
        from scripts.load_season import _count_failures

        results = {"games": {"status": "ok"}, "stats": {"status": "error"}}

        assert _count_failures(results) == 1

    def test_matches_the_row_icons_fail_predicate(self):
        """The tally and the printed [FAIL] icon must never disagree --
        anything counted as a failure here must also print FAIL, and vice
        versa."""
        from scripts.load_season import _count_failures

        results = {
            "a": {"status": "ok"},
            "b": {"status": "partial"},
            "c": {"status": "error"},
        }
        icon_fail_count = sum(1 for r in results.values() if r["status"] != "ok")

        assert _count_failures(results) == icon_fail_count == 2


def _raise_dlt_timeout(*_args, **_kwargs):
    try:
        raise httpx.ReadTimeout("The read operation timed out")
    except httpx.ReadTimeout as exc:
        raise RuntimeError("Pipeline execution failed at step=extract") from exc


def _http_status_error(status_code: int, path: str) -> httpx.HTTPStatusError:
    url = f"https://api.collegefootballdata.com{path}"
    request = httpx.Request("GET", url)
    response = httpx.Response(status_code, request=request)
    reason = response.reason_phrase
    return httpx.HTTPStatusError(
        f"Server error '{status_code} {reason}' for url '{url}'",
        request=request,
        response=response,
    )


def _raise_dlt_504(*_args, **_kwargs):
    """Daily Season Load run 39796553707 (2026-10-22): /passing/players/season
    answered 504 Gateway Timeout on every retry -- PR #155's 120s read budget
    now outlasts CFBD's own gateway, so the same slow endpoint that used to
    read-timeout client-side arrives as an exhausted 5xx instead."""
    try:
        raise _http_status_error(504, "/passing/players/season?year=2026")
    except httpx.HTTPStatusError as exc:
        raise RuntimeError("Pipeline execution failed at step=extract") from exc


def _raise_dlt_502(*_args, **_kwargs):
    """A 502 is an outage, not a slow season aggregate -- it must stay strict."""
    try:
        raise _http_status_error(502, "/passing/players/season?year=2026")
    except httpx.HTTPStatusError as exc:
        raise RuntimeError("Pipeline execution failed at step=extract") from exc


def _mock_rate_limiter():
    limiter = MagicMock()
    limiter.get_status.return_value = {"remaining": 10_000}
    return limiter


_GET_RATE_LIMITER = "src.pipelines.utils.rate_limiter.get_rate_limiter"


def _utc_today() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _utc_yesterday() -> str:
    return (datetime.now(UTC) - timedelta(days=1)).strftime("%Y-%m-%d")


def _load_with_tolerance(**kwargs):
    kwargs.setdefault("tolerate_aggregate_timeouts", True)
    kwargs.setdefault("skip_refresh", True)
    return load_season(season=2026, **kwargs)


class TestToleratedNonCriticalTimeout:
    """Revision 2: opt-in tolerance for non-critical season-aggregate timeouts."""

    def test_both_non_critical_timeouts_tolerated_with_flag(self):
        with (
            patch("src.pipelines.run.run_passing_pipeline", side_effect=_raise_dlt_timeout),
            patch("src.pipelines.run.run_rushing_pipeline", side_effect=_raise_dlt_timeout),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing", "rushing"])

        assert summary["results"]["passing"]["status"] == "tolerated"
        assert summary["results"]["rushing"]["status"] == "tolerated"
        assert summary["errors"] == 0
        assert summary["tolerated"] == ["passing", "rushing"]

    def test_both_non_critical_timeouts_strict_without_flag(self):
        with (
            patch("src.pipelines.run.run_passing_pipeline", side_effect=_raise_dlt_timeout),
            patch("src.pipelines.run.run_rushing_pipeline", side_effect=_raise_dlt_timeout),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = load_season(season=2026, sources=["passing", "rushing"], skip_refresh=True)

        assert summary["results"]["passing"]["status"] == "error"
        assert summary["results"]["rushing"]["status"] == "error"
        assert summary["errors"] == 2
        assert summary["tolerated"] == []

    def test_games_timeout_stays_error_with_flag(self):
        with (
            patch("src.pipelines.run.run_games_pipeline", side_effect=_raise_dlt_timeout),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["games"])

        assert summary["results"]["games"]["status"] == "error"
        assert summary["errors"] == 1

    def test_stats_timeout_stays_error_with_flag(self):
        with (
            patch("src.pipelines.run.run_stats_pipeline", side_effect=_raise_dlt_timeout),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["stats"])

        assert summary["results"]["stats"]["status"] == "error"
        assert summary["errors"] == 1

    def test_mixed_tolerable_and_critical_timeouts_not_downgraded(self):
        with (
            patch("src.pipelines.run.run_passing_pipeline", side_effect=_raise_dlt_timeout),
            patch("src.pipelines.run.run_games_pipeline", side_effect=_raise_dlt_timeout),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing", "games"])

        assert summary["results"]["passing"]["status"] == "error"
        assert summary["results"]["games"]["status"] == "error"
        assert summary["errors"] == 2

    def test_non_timeout_passing_failure_stays_error_with_flag(self):
        with (
            patch("src.pipelines.run.run_passing_pipeline", side_effect=ValueError("bad")),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing"])

        assert summary["results"]["passing"]["status"] == "error"
        assert summary["errors"] == 1

    def test_streak_limit_trips_on_third_consecutive_day(self, monkeypatch, tmp_path):
        streak_file = tmp_path / "streaks.json"
        streak_file.write_text(
            json.dumps(
                {
                    "version": 1,
                    "streaks": {"passing": {"count": 2, "last_date": _utc_yesterday()}},
                }
            )
        )
        monkeypatch.setenv("TOLERATED_STREAK_FILE", str(streak_file))

        with (
            patch("src.pipelines.run.run_passing_pipeline", side_effect=_raise_dlt_timeout),
            patch("src.pipelines.run.run_games_pipeline", return_value="ok"),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing", "games"], tolerated_streak_limit=3)

        assert summary["results"]["passing"]["status"] == "error"
        assert summary["errors"] == 1
        saved = json.loads(streak_file.read_text())
        assert saved["streaks"]["passing"]["count"] == 3
        assert saved["streaks"]["passing"]["last_date"] == _utc_today()

    def test_streak_increments_on_tolerated_run(self, monkeypatch, tmp_path):
        streak_file = tmp_path / "streaks.json"
        streak_file.write_text(
            json.dumps(
                {
                    "version": 1,
                    "streaks": {"passing": {"count": 1, "last_date": _utc_yesterday()}},
                }
            )
        )
        monkeypatch.setenv("TOLERATED_STREAK_FILE", str(streak_file))

        with (
            patch("src.pipelines.run.run_passing_pipeline", side_effect=_raise_dlt_timeout),
            patch("src.pipelines.run.run_games_pipeline", return_value="ok"),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing", "games"], tolerated_streak_limit=3)

        assert summary["results"]["passing"]["status"] == "tolerated"
        assert summary["errors"] == 0
        saved = json.loads(streak_file.read_text())
        assert saved["streaks"]["passing"]["count"] == 2
        assert saved["streaks"]["passing"]["last_date"] == _utc_today()

    def test_streak_cleared_when_source_loads_ok(self, monkeypatch, tmp_path):
        streak_file = tmp_path / "streaks.json"
        streak_file.write_text(
            json.dumps(
                {
                    "version": 1,
                    "streaks": {"passing": {"count": 2, "last_date": _utc_yesterday()}},
                }
            )
        )
        monkeypatch.setenv("TOLERATED_STREAK_FILE", str(streak_file))

        with (
            patch("src.pipelines.run.run_passing_pipeline", return_value="ok"),
            patch("src.pipelines.run.run_games_pipeline", return_value="ok"),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            _load_with_tolerance(sources=["passing", "games"])

        saved = json.loads(streak_file.read_text())
        assert "passing" not in saved["streaks"]

    def test_streak_with_a_gap_day_restarts_instead_of_continuing(self, monkeypatch, tmp_path):
        """Review finding on revision 2: "consecutive" is literal. A count
        preserved from an older run (gap days, skipped sources, non-timeout
        failures in between) must restart at 1, not inherit and veto."""
        streak_file = tmp_path / "streaks.json"
        three_days_ago = (datetime.now(UTC) - timedelta(days=3)).strftime("%Y-%m-%d")
        streak_file.write_text(
            json.dumps(
                {
                    "version": 1,
                    "streaks": {"passing": {"count": 2, "last_date": three_days_ago}},
                }
            )
        )
        monkeypatch.setenv("TOLERATED_STREAK_FILE", str(streak_file))

        with (
            patch("src.pipelines.run.run_passing_pipeline", side_effect=_raise_dlt_timeout),
            patch("src.pipelines.run.run_games_pipeline", return_value="ok"),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing", "games"], tolerated_streak_limit=3)

        assert summary["results"]["passing"]["status"] == "tolerated"
        assert summary["errors"] == 0
        saved = json.loads(streak_file.read_text())
        assert saved["streaks"]["passing"]["count"] == 1
        assert saved["streaks"]["passing"]["last_date"] == _utc_today()

    def test_malformed_streak_entries_are_dropped_not_crashing(self, monkeypatch, tmp_path, caplog):
        """Review finding on revision 2: valid JSON with malformed nested
        entries (null entry, non-int count) must degrade per-entry with a
        warning instead of raising out of the load."""
        streak_file = tmp_path / "streaks.json"
        streak_file.write_text(
            json.dumps(
                {
                    "version": 1,
                    "streaks": {
                        "passing": None,
                        "rushing": {"count": "bad", "last_date": "2026-10-18"},
                        "wepa": {"count": 1, "last_date": _utc_yesterday()},
                    },
                }
            )
        )
        monkeypatch.setenv("TOLERATED_STREAK_FILE", str(streak_file))

        with (
            patch("src.pipelines.run.run_passing_pipeline", side_effect=_raise_dlt_timeout),
            patch("src.pipelines.run.run_games_pipeline", return_value="ok"),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing", "games"])

        assert summary["results"]["passing"]["status"] == "tolerated"
        assert summary["errors"] == 0
        assert any("Dropping malformed tolerated streak entry" in r.message for r in caplog.records)
        saved = json.loads(streak_file.read_text())
        assert saved["streaks"]["passing"]["count"] == 1

    def test_same_utc_day_rerun_does_not_double_increment(self, monkeypatch, tmp_path):
        streak_file = tmp_path / "streaks.json"
        streak_file.write_text(
            json.dumps(
                {
                    "version": 1,
                    "streaks": {"passing": {"count": 1, "last_date": _utc_today()}},
                }
            )
        )
        monkeypatch.setenv("TOLERATED_STREAK_FILE", str(streak_file))

        with (
            patch("src.pipelines.run.run_passing_pipeline", side_effect=_raise_dlt_timeout),
            patch("src.pipelines.run.run_games_pipeline", return_value="ok"),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing", "games"])

        assert summary["results"]["passing"]["status"] == "tolerated"
        saved = json.loads(streak_file.read_text())
        assert saved["streaks"]["passing"]["count"] == 1

    def test_corrupt_streak_file_degrades_with_warning(self, monkeypatch, tmp_path, caplog):
        streak_file = tmp_path / "streaks.json"
        streak_file.write_text("{not json")
        monkeypatch.setenv("TOLERATED_STREAK_FILE", str(streak_file))

        with (
            patch("src.pipelines.run.run_passing_pipeline", side_effect=_raise_dlt_timeout),
            patch("src.pipelines.run.run_games_pipeline", return_value="ok"),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing", "games"])

        assert summary["results"]["passing"]["status"] == "tolerated"
        assert any("Could not load tolerated streak state" in r.message for r in caplog.records)

    def test_unwritable_streak_file_does_not_crash_run(self, monkeypatch, caplog):
        monkeypatch.setenv("TOLERATED_STREAK_FILE", "/nonexistent-dir/nested/streaks.json")

        with (
            patch("src.pipelines.run.run_passing_pipeline", side_effect=_raise_dlt_timeout),
            patch("src.pipelines.run.run_games_pipeline", return_value="ok"),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing", "games"])

        assert summary["results"]["passing"]["status"] == "tolerated"
        assert any("Could not persist tolerated streak state" in r.message for r in caplog.records)

    def test_github_step_summary_lists_every_tolerated_source(self, monkeypatch, tmp_path):
        summary_path = tmp_path / "step_summary.md"
        monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary_path))

        with (
            patch("src.pipelines.run.run_passing_pipeline", side_effect=_raise_dlt_timeout),
            patch("src.pipelines.run.run_rushing_pipeline", side_effect=_raise_dlt_timeout),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            _load_with_tolerance(sources=["passing", "rushing"])

        text = summary_path.read_text()
        assert "passing" in text
        assert "rushing" in text
        assert "Warning" in text

    def test_failed_step_summary_write_does_not_crash_the_run(self, monkeypatch, caplog):
        monkeypatch.setenv("GITHUB_STEP_SUMMARY", "/nonexistent-dir/step_summary.md")

        with (
            patch("src.pipelines.run.run_passing_pipeline", side_effect=_raise_dlt_timeout),
            patch("src.pipelines.run.run_games_pipeline", return_value="ok"),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing", "games"])

        assert summary["results"]["passing"]["status"] == "tolerated"
        assert summary["errors"] == 0
        assert any(
            "Could not write GITHUB_STEP_SUMMARY" in record.message for record in caplog.records
        )

    def test_count_failures_ignores_tolerated_still_counts_partial(self):
        from scripts.load_season import _count_failures

        results = {
            "passing": {"status": "tolerated"},
            "games": {"status": "ok"},
            "_mart_refresh": {"status": "partial", "failures": 1},
        }
        assert _count_failures(results) == 1

    def test_apply_aggregate_timeout_tolerance_pure_function(self):
        state = {"version": 1, "streaks": {}}
        today = "2026-10-19"
        both = {
            "passing": {"status": "error", "tolerable_timeout": True},
            "rushing": {"status": "error", "tolerable_timeout": True},
        }
        tolerated, vetoed = apply_aggregate_timeout_tolerance(
            both,
            tolerate_aggregate_timeouts=True,
            tolerated_streak_limit=3,
            streak_state=state,
            today=today,
        )
        assert tolerated == ["passing", "rushing"]
        assert vetoed == []
        assert both["passing"]["status"] == "tolerated"

        mixed = {
            "passing": {"status": "error", "tolerable_timeout": True},
            "games": {"status": "error"},
        }
        state2 = {"version": 1, "streaks": {}}
        tolerated, vetoed = apply_aggregate_timeout_tolerance(
            mixed,
            tolerate_aggregate_timeouts=True,
            tolerated_streak_limit=3,
            streak_state=state2,
            today=today,
        )
        assert tolerated == []
        assert mixed["passing"]["status"] == "error"
        assert state2["streaks"]["passing"]["count"] == 1

        veto_state = {
            "version": 1,
            "streaks": {"passing": {"count": 3, "last_date": "2026-10-18"}},
        }
        lone = {"passing": {"status": "error", "tolerable_timeout": True}}
        tolerated, vetoed = apply_aggregate_timeout_tolerance(
            lone,
            tolerate_aggregate_timeouts=True,
            tolerated_streak_limit=3,
            streak_state=veto_state,
            today=today,
        )
        assert tolerated == []
        assert vetoed == ["passing"]
        assert lone["passing"]["status"] == "error"

    def test_update_tolerated_streaks_pure_function(self):
        state = {
            "version": 1,
            "streaks": {"passing": {"count": 2, "last_date": "2026-10-18"}},
        }
        update_tolerated_streaks(
            state,
            {
                "passing": {"status": "ok"},
                "rushing": {"status": "error", "tolerable_timeout": True},
            },
            "2026-10-19",
        )
        assert "passing" not in state["streaks"]
        assert state["streaks"]["rushing"]["count"] == 1

    def test_update_tolerated_streaks_consecutive_vs_gap(self):
        state = {
            "version": 1,
            "streaks": {
                "passing": {"count": 2, "last_date": "2026-10-18"},  # yesterday: continues
                "rushing": {"count": 2, "last_date": "2026-10-01"},  # gap: restarts
                "wepa": {"count": 4, "last_date": "2026-09-01"},  # stale, untouched: pruned
            },
        }
        update_tolerated_streaks(
            state,
            {
                "passing": {"status": "error", "tolerable_timeout": True},
                "rushing": {"status": "error", "tolerable_timeout": True},
            },
            "2026-10-19",
        )
        assert state["streaks"]["passing"]["count"] == 3
        assert state["streaks"]["rushing"]["count"] == 1
        assert "wepa" not in state["streaks"]

    def test_load_tolerated_streak_state_corrupt_returns_empty(self, tmp_path, caplog):
        path = tmp_path / "bad.json"
        path.write_text("[]")
        state = load_tolerated_streak_state(str(path))
        assert state == {"version": 1, "streaks": {}}
        assert any("Could not load tolerated streak state" in r.message for r in caplog.records)


class TestPassingSeasonTimeoutTolerance:
    """Regression for Daily Season Load #99-#101 (runs 39253582408 ..
    39796553707): /passing/players/season timed out after retries three daily
    runs straight and failed the whole job every time. Two failure shapes,
    both of which must now end tolerated on the opt-in daily path: the
    original client-side read timeout, and the 504 Gateway Timeout that
    replaced it once PR #155's 120s read budget outlasted CFBD's own
    gateway."""

    def test_504_on_passing_season_with_flag_is_tolerated(self):
        """The 2026-10-22 shape (run 39796553707): an exhausted upstream 504
        on the season aggregate downgrades to tolerated, and the week-grain
        passing unit -- now a separate extract -- stays loaded OK."""
        with (
            patch("src.pipelines.run.run_passing_pipeline", return_value="ok") as week_runner,
            patch("src.pipelines.run.run_passing_season_pipeline", side_effect=_raise_dlt_504),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing", "passing_season"])

        assert week_runner.call_count == 1
        assert summary["results"]["passing"]["status"] == "ok"
        assert summary["results"]["passing_season"]["status"] == "tolerated"
        assert summary["errors"] == 0
        assert summary["tolerated"] == ["passing_season"]

    def test_read_timeout_on_passing_season_with_flag_is_tolerated(self):
        """The 2026-10-19/20/21 shape (run 39253582408): the original client
        read timeout, now isolated in the split unit."""
        with (
            patch("src.pipelines.run.run_passing_pipeline", return_value="ok"),
            patch("src.pipelines.run.run_passing_season_pipeline", side_effect=_raise_dlt_timeout),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing", "passing_season"])

        assert summary["results"]["passing"]["status"] == "ok"
        assert summary["results"]["passing_season"]["status"] == "tolerated"
        assert summary["errors"] == 0

    def test_504_without_flag_stays_strict(self):
        """Backfills and manual runs keep today's strict behavior."""
        with (
            patch("src.pipelines.run.run_passing_season_pipeline", side_effect=_raise_dlt_504),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = load_season(season=2026, sources=["passing_season"], skip_refresh=True)

        assert summary["results"]["passing_season"]["status"] == "error"
        assert summary["errors"] == 1
        assert summary["tolerated"] == []

    def test_502_on_passing_season_stays_error_with_flag(self):
        """Only 504 is timeout-shaped. A 502 (or any other exhausted 5xx) is
        an outage and must stay loud even on a non-critical unit."""
        with (
            patch("src.pipelines.run.run_passing_season_pipeline", side_effect=_raise_dlt_502),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing_season"])

        assert summary["results"]["passing_season"]["status"] == "error"
        assert summary["errors"] == 1

    def test_504_on_a_critical_source_stays_error_with_flag(self):
        """The 504 downgrade rides the same NON_CRITICAL_SOURCES gate as the
        read-timeout downgrade -- a critical source's 504 still fails the
        run."""
        with (
            patch("src.pipelines.run.run_games_pipeline", side_effect=_raise_dlt_504),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["games"])

        assert summary["results"]["games"]["status"] == "error"
        assert summary["errors"] == 1

    def test_504_on_the_whole_passing_unit_is_also_tolerated(self):
        """Belt and suspenders: even if a 504 hits a week-grain passing
        endpoint, the passing unit is non-critical and the downgrade applies."""
        with (
            patch("src.pipelines.run.run_passing_pipeline", side_effect=_raise_dlt_504),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing"])

        assert summary["results"]["passing"]["status"] == "tolerated"
        assert summary["errors"] == 0

    def test_streak_guard_applies_to_the_new_unit(self, monkeypatch, tmp_path):
        """Two primed tolerated days + a third 504 trips the streak veto, so
        a chronically dead endpoint cannot be tolerated forever."""
        streak_file = tmp_path / "streaks.json"
        streak_file.write_text(
            json.dumps(
                {
                    "version": 1,
                    "streaks": {"passing_season": {"count": 2, "last_date": _utc_yesterday()}},
                }
            )
        )
        monkeypatch.setenv("TOLERATED_STREAK_FILE", str(streak_file))

        with (
            patch("src.pipelines.run.run_passing_season_pipeline", side_effect=_raise_dlt_504),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = _load_with_tolerance(sources=["passing_season"], tolerated_streak_limit=3)

        assert summary["results"]["passing_season"]["status"] == "error"
        assert summary["errors"] == 1
        saved = json.loads(streak_file.read_text())
        assert saved["streaks"]["passing_season"]["count"] == 3


class TestFailureDetailInSummary:
    """Run 39796553707: the mid-run traceback did not survive GitHub's log
    rendering, leaving only `[FAIL] passing 1017.6s` -- three runs were
    misdiagnosed from an older failure's shape as a result. Each failed
    unit's exception type and message must now print in the end-of-run
    summary block and in the GITHUB_STEP_SUMMARY markdown."""

    def test_failed_unit_prints_error_type_message_and_root_cause(self, capsys):
        with (
            patch("src.pipelines.run.run_games_pipeline", side_effect=_raise_dlt_504),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            load_season(season=2026, sources=["games"], skip_refresh=True)

        out = capsys.readouterr().out
        assert "[FAIL] games" in out
        assert "RuntimeError: Pipeline execution failed at step=extract" in out
        assert "cause: HTTPStatusError: Server error '504 Gateway Timeout'" in out

    def test_tolerated_unit_prints_no_failure_detail(self, capsys):
        with (
            patch("src.pipelines.run.run_passing_season_pipeline", side_effect=_raise_dlt_504),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            _load_with_tolerance(sources=["passing_season"])

        out = capsys.readouterr().out
        assert "[WARN] passing_season" in out
        assert "RuntimeError" not in out

    def test_long_error_messages_are_collapsed_and_truncated(self, capsys):
        from scripts.load_season import MAX_SUMMARY_ERROR_CHARS

        def _raise_long(*_args, **_kwargs):
            raise RuntimeError("Pipeline\nexecution\nfailed " + "x" * 2000)

        with (
            patch("src.pipelines.run.run_games_pipeline", side_effect=_raise_long),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            load_season(season=2026, sources=["games"], skip_refresh=True)

        out = capsys.readouterr().out
        detail_lines = [ln for ln in out.splitlines() if "RuntimeError" in ln]
        assert detail_lines, "expected a failure detail line in the summary"
        assert all(
            len(ln.strip()) <= MAX_SUMMARY_ERROR_CHARS + len("cause: ") for ln in detail_lines
        )
        assert "Pipeline execution failed" in detail_lines[0]  # newlines collapsed

    def test_github_step_summary_gets_failed_unit_detail(self, monkeypatch, tmp_path):
        summary_path = tmp_path / "step_summary.md"
        monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary_path))

        with (
            patch("src.pipelines.run.run_games_pipeline", side_effect=_raise_dlt_504),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            load_season(season=2026, sources=["games"], skip_refresh=True)

        text = summary_path.read_text()
        assert "Failed load unit(s)" in text
        assert "`games`" in text
        assert "RuntimeError" in text
        assert "504 Gateway Timeout" in text

    def test_github_step_summary_has_no_failure_section_when_all_tolerated(
        self, monkeypatch, tmp_path
    ):
        summary_path = tmp_path / "step_summary.md"
        monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary_path))

        with (
            patch("src.pipelines.run.run_passing_season_pipeline", side_effect=_raise_dlt_504),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            _load_with_tolerance(sources=["passing_season"])

        text = summary_path.read_text()
        assert "Failed load unit(s)" not in text
        assert "Warning" in text  # the tolerated warning still writes

    def test_unwritable_step_summary_does_not_mask_the_failure(self, monkeypatch, caplog):
        monkeypatch.setenv("GITHUB_STEP_SUMMARY", "/nonexistent-dir/step_summary.md")

        with (
            patch("src.pipelines.run.run_games_pipeline", side_effect=_raise_dlt_504),
            patch(_GET_RATE_LIMITER, return_value=_mock_rate_limiter()),
        ):
            summary = load_season(season=2026, sources=["games"], skip_refresh=True)

        assert summary["errors"] == 1
        assert any(
            "Could not write GITHUB_STEP_SUMMARY failure detail" in record.message
            for record in caplog.records
        )


class TestWorkflowYaml:
    """Sanity checks on the two workflow files touched alongside the
    player_overview cap override -- catches a YAML syntax error (bad
    indentation, an unescaped `:` in a description) before it reaches CI,
    where a broken workflow file fails silently until dispatched."""

    def test_flat_files_workflow_parses(self):
        doc = yaml.safe_load((WORKFLOWS_DIR / "flat-files.yml").read_text())

        dispatch_inputs = doc[True]["workflow_dispatch"]["inputs"]
        assert "seasons" in dispatch_inputs
        assert dispatch_inputs["seasons"]["required"] is False
        assert doc["jobs"]["load"]["timeout-minutes"] == 300

    def test_backfill_sources_workflow_parses(self):
        doc = yaml.safe_load((WORKFLOWS_DIR / "backfill-sources.yml").read_text())

        dispatch_inputs = doc[True]["workflow_dispatch"]["inputs"]
        assert "player_overview_max" in dispatch_inputs
        assert dispatch_inputs["player_overview_max"]["required"] is False
        assert doc["jobs"]["backfill"]["timeout-minutes"] == 300

    def test_daily_load_workflow_tolerate_flag_and_streak_cache(self):
        text = (WORKFLOWS_DIR / "daily-load.yml").read_text()
        doc = yaml.safe_load(text)
        load_job = doc["jobs"]["load"]
        step_names = [step.get("name", "") for step in load_job["steps"]]
        assert "Restore tolerated-timeout streak state" in step_names
        assert "Save tolerated-timeout streak state" in step_names

        restore = next(
            s
            for s in load_job["steps"]
            if s.get("name") == "Restore tolerated-timeout streak state"
        )
        assert restore["uses"] == "actions/cache/restore@v4"
        assert restore["with"]["path"] == ".tolerated-streaks.json"
        assert restore["with"]["key"].startswith("cfbd-tolerated-streaks-")
        assert "cfbd-tolerated-streaks-" in restore["with"]["restore-keys"]

        save = next(
            s for s in load_job["steps"] if s.get("name") == "Save tolerated-timeout streak state"
        )
        assert save["uses"] == "actions/cache/save@v4"
        assert save["if"] == "always()"
        assert save["with"]["key"].startswith("cfbd-tolerated-streaks-")

        # Greptile P1 (PR #155): cache entries are immutable per key and a
        # rerun reuses run_id, so a run_id-only key could never save a rerun's
        # updated (reset) state -- the next timeout would read a stale streak.
        # Both keys must include run_attempt, stay identical to each other,
        # and still match the shared restore-keys prefix so the newest state
        # from ANY prior run or attempt is restored.
        for key in (restore["with"]["key"], save["with"]["key"]):
            assert "${{ github.run_id }}" in key
            assert "${{ github.run_attempt }}" in key
        assert restore["with"]["key"] == save["with"]["key"]
        prefix = restore["with"]["restore-keys"].strip().splitlines()[0].strip()
        assert save["with"]["key"].startswith(prefix)

        load_step = next(
            s
            for s in load_job["steps"]
            if s.get("name") == "Load season data (includes mart refresh)"
        )
        assert load_step["env"]["TOLERATED_STREAK_FILE"].endswith("/.tolerated-streaks.json")
        run_script = load_step["run"]
        assert run_script.count("--tolerate-aggregate-timeouts") == 1
        else_branch = run_script.split("else")[1]
        assert "--tolerate-aggregate-timeouts" in else_branch
        season_branch = run_script.split("else")[0]
        assert "--tolerate-aggregate-timeouts" not in season_branch
