"""Unit tests for build_features's pure helpers (no DB, no I/O).

Covers the design doc's (docs/brainstorms/2026-07-21-team-week-feature-design.md)
leak rules directly against plain dicts/lists: week_index derivation
(section 0), adjusted-EPA as-of/fallback resolution (section 1c),
games_played_to_date counting (section 1a), the house-Elo fallback ladder
(section 1b), and the as-of leak predicate season-to-date aggregation relies
on (section 1d). Follows tests/test_predictions.py's style.
"""

import pytest

from scripts.build_features import (
    MIN_TEAM_PLAYS,
    POSTSEASON_WEEK_OFFSET,
    compute_week_index,
    games_played_to_date,
    leak_free_week_index,
    resolve_adj_epa,
    resolve_team_week_elo,
)


class TestComputeWeekIndex:
    def test_regular_season_passthrough(self):
        assert compute_week_index(1, "regular") == 1
        assert compute_week_index(14, "regular") == 14

    def test_postseason_offset(self):
        assert POSTSEASON_WEEK_OFFSET == 100
        assert compute_week_index(1, "postseason") == 101
        assert compute_week_index(2, "postseason") == 102

    def test_postseason_always_sorts_after_regular(self):
        assert compute_week_index(1, "postseason") > compute_week_index(15, "regular")


class TestGamesPlayedToDate:
    def test_week1_is_zero_not_none(self):
        games = [{"week_index": 1, "completed": True}]
        result = games_played_to_date(games, 1)
        assert result == 0
        assert result is not None

    def test_no_games_at_all_is_zero(self):
        assert games_played_to_date([], 1) == 0

    def test_counts_only_completed_games_before_week_index(self):
        games = [
            {"week_index": 1, "completed": True},
            {"week_index": 2, "completed": True},
            {"week_index": 3, "completed": False},  # scheduled, not yet played
        ]
        assert games_played_to_date(games, 4) == 2

    def test_excludes_the_current_week_own_game(self):
        games = [
            {"week_index": 1, "completed": True},
            {"week_index": 2, "completed": True},
        ]
        # A row keyed at week_index=2 must not count week 2's own game.
        assert games_played_to_date(games, 2) == 1

    def test_excludes_future_weeks(self):
        games = [
            {"week_index": 1, "completed": True},
            {"week_index": 5, "completed": True},
        ]
        assert games_played_to_date(games, 3) == 1


class TestLeakFreeWeekIndex:
    """The predicate season-to-date aggregation (and games_played_to_date)
    relies on: a row is includable iff it happened strictly before the as-of
    week_index."""

    def test_strictly_prior_week_is_included(self):
        assert leak_free_week_index(4, 5) is True

    def test_same_week_is_excluded(self):
        # A play/game at week_index == WI (the team's own game that week)
        # must never leak into its own pregame season-to-date aggregate.
        assert leak_free_week_index(5, 5) is False

    def test_future_week_is_excluded(self):
        assert leak_free_week_index(6, 5) is False


class TestResolveAdjEpa:
    """team's week rows: week_index 1 (0 plays), 2 (70 plays, < MIN_TEAM_PLAYS),
    3 (160 plays, first qualifying week), 8 (520 plays)."""

    def week_rows(self):
        return {
            "Big State": [
                {"week_index": 1, "off_coef": 0.00, "def_coef": 0.00, "hfa_coef": 0.02, "plays": 0},
                {
                    "week_index": 2,
                    "off_coef": 0.02,
                    "def_coef": -0.02,
                    "hfa_coef": 0.02,
                    "plays": 70,
                },
                {
                    "week_index": 3,
                    "off_coef": 0.05,
                    "def_coef": -0.04,
                    "hfa_coef": 0.02,
                    "plays": 160,
                },
                {
                    "week_index": 8,
                    "off_coef": 0.08,
                    "def_coef": -0.06,
                    "hfa_coef": 0.03,
                    "plays": 520,
                },
            ]
        }

    def prior_season_rows(self):
        return {"Big State": {"off_coef": 0.03, "def_coef": -0.03, "hfa_coef": 0.015}}

    def test_min_team_plays_constant(self):
        assert MIN_TEAM_PLAYS == 150

    def test_before_any_qualifying_week_falls_back_to_prior_season(self):
        # week_index=2's own row exists but plays=70 < MIN_TEAM_PLAYS, so
        # both week_index 1 and 2 fail the predicate -> prior-season fallback.
        result = resolve_adj_epa("Big State", 2019, 2, self.week_rows(), self.prior_season_rows())
        assert result["source"] == "prior_season"
        assert result["off"] == pytest.approx(0.03)
        assert result["def"] == pytest.approx(-0.03)
        assert result["net"] == pytest.approx(0.03 - (-0.03))
        assert result["hfa"] == pytest.approx(0.015)

    def test_qualifying_week_at_or_before_wi_is_used(self):
        # WI=3 exactly matches the first qualifying (plays >= 150) row.
        result = resolve_adj_epa("Big State", 2019, 3, self.week_rows(), self.prior_season_rows())
        assert result["source"] == "week"
        assert result["off"] == pytest.approx(0.05)
        assert result["def"] == pytest.approx(-0.04)
        assert result["net"] == pytest.approx(0.05 - (-0.04))
        assert result["hfa"] == pytest.approx(0.02)

    def test_greatest_qualifying_week_index_leq_wi_is_selected(self):
        # WI=10: both week_index 3 and 8 qualify (plays >= 150); the greater
        # (8) must win, not week_index 8's row leaking into WI=5 or similar.
        result = resolve_adj_epa("Big State", 2019, 10, self.week_rows(), self.prior_season_rows())
        assert result["source"] == "week"
        assert result["off"] == pytest.approx(0.08)
        assert result["def"] == pytest.approx(-0.06)
        assert result["hfa"] == pytest.approx(0.03)

    def test_week_index_greater_than_wi_is_never_selected(self):
        # WI=5: week_index 8 qualifies on plays but is AFTER WI, so it must
        # not be used -- week_index 3 (the greatest one <= WI) wins instead.
        result = resolve_adj_epa("Big State", 2019, 5, self.week_rows(), self.prior_season_rows())
        assert result["source"] == "week"
        assert result["off"] == pytest.approx(0.05)

    def test_neither_week_fit_nor_prior_season_is_all_null(self):
        result = resolve_adj_epa("Nobody U", 2019, 3, {}, {})
        assert result == {"off": None, "def": None, "net": None, "hfa": None, "source": None}

    def test_no_week_rows_at_all_falls_back_to_prior_season(self):
        result = resolve_adj_epa("Big State", 2019, 3, {}, self.prior_season_rows())
        assert result["source"] == "prior_season"


class TestResolveTeamWeekElo:
    def test_house_elo_game_value_wins_outright(self):
        # Even if house_elo_current would resolve to something else, the
        # stored per-game pregame value always wins.
        elo_current = {"Big State": (1800.0, 2019)}
        result = resolve_team_week_elo(1650.25, "Big State", 2020, elo_current)
        assert result == pytest.approx(1650.25)

    def test_missing_house_elo_game_falls_back_to_current_with_carryover(self):
        elo_current = {"Big State": (1700.0, 2019)}
        result = resolve_team_week_elo(None, "Big State", 2020, elo_current)
        assert result == pytest.approx(1500 + 200 * (2 / 3))

    def test_missing_house_elo_game_and_current_season_snapshot_used_as_is(self):
        elo_current = {"Big State": (1650.0, 2024)}
        result = resolve_team_week_elo(None, "Big State", 2024, elo_current)
        assert result == pytest.approx(1650.0)

    def test_unknown_team_falls_back_to_seed(self):
        result = resolve_team_week_elo(None, "Nobody U", 2024, {})
        assert result == pytest.approx(1500.0)

    def test_never_returns_none(self):
        assert resolve_team_week_elo(None, "Nobody U", 2024, {}) is not None
        assert resolve_team_week_elo(1500.0, "Big State", 2024, {}) is not None


class TestMigration042Columns:
    """The five preseason columns that cleared the section 2.5 screen.

    Their value depends on being computed the SAME way the screen measured
    them: the partial correlations that justified shipping them were produced
    under a specific decay and window, so drift between the two would leave
    features.team_week populated with a quantity nothing ever validated.
    """

    COLUMNS = (
        "recruiting_points_3yr",
        "blue_chip_pipeline",
        "hc_first_year",
        "hc_first_year_unproven",
        "prior_def_line_yards",
        "prior_def_stuff_rate",
    )

    def test_proven_boundary_matches_the_screen_that_validated_it(self):
        """hc_first_year_unproven's -0.1844 was measured at exactly this cut.
        A drift here would populate the column with a quantity the screen never
        evaluated -- the same failure the decay/window check above prevents."""
        from scripts.build_features import HC_PROVEN_SP_PLUS
        from scripts.screen_preseason_features import (
            HC_PROVEN_SP_PLUS as SCREEN_BOUNDARY,
        )

        assert HC_PROVEN_SP_PLUS == SCREEN_BOUNDARY

    def test_career_prior_cannot_see_season_s(self):
        """The career prior averages the coach's PREVIOUS stops. A non-strict
        bound would fold season S's own SP+ into a feature that claims to be
        preseason-known, which is the whole reason the column is admissible."""
        from scripts.build_features import FEATURE_ROWS_QUERY

        assert "prev.year < cy.year" in FEATURE_ROWS_QUERY
        assert "prev.year <= cy.year" not in FEATURE_ROWS_QUERY

    def test_an_unratable_prior_career_is_null_not_unproven(self):
        """A coach with prior head-coaching seasons that carry no SP+ row (an
        FCS stop, or older than ratings coverage) is UNKNOWN, not unproven --
        but a coach with ZERO prior seasons is a genuine first-timer and IS
        unproven. The rookie branch must therefore be tested BEFORE the
        unratable guard, or every first-time head coach falls through to NULL
        and the column loses its largest subgroup."""
        from scripts.build_features import FEATURE_ROWS_QUERY

        block = FEATURE_ROWS_QUERY[FEATURE_ROWS_QUERY.index("AS hc_first_year,") :]
        block = block[: block.index("AS hc_first_year_unproven")]
        rookie = block.index("cpr.prior_seasons = 0")
        unratable = block.index("cpr.prior_sp_mean IS NULL")
        assert rookie < unratable, "the rookie branch must precede the unratable guard"
        assert "THEN NULL" in block[unratable:], "an unratable career must yield NULL"

    def test_recruiting_window_matches_the_screen_that_validated_it(self):
        from scripts.build_features import CLASS_DECAY, CLASS_WINDOW
        from scripts.screen_preseason_features import (
            CLASS_DECAY as SCREEN_DECAY,
        )
        from scripts.screen_preseason_features import (
            CLASS_WINDOW as SCREEN_WINDOW,
        )

        assert (CLASS_DECAY, CLASS_WINDOW) == (SCREEN_DECAY, SCREEN_WINDOW)

    def test_columns_are_written(self):
        from scripts.build_features import _INSERT_COLUMNS

        for col in self.COLUMNS:
            assert col in _INSERT_COLUMNS

    def test_columns_are_selected(self):
        from scripts.build_features import FEATURE_ROWS_QUERY

        for col in self.COLUMNS:
            assert f"AS {col}" in FEATURE_ROWS_QUERY or f'"{col}"' in FEATURE_ROWS_QUERY

    def test_nothing_is_zero_filled(self):
        """Design doc section 1i. These are rates and decayed sums whose zero
        is a fabricated extreme, and the teams whose source rows are missing
        skew weak -- so COALESCE(...,0) here would plant the floor value
        exactly where the outcome is low and manufacture signal.

        Scoped to end at the migration-047 draft columns, which are the
        documented exception: they are COUNTS, where a zero is a true
        measurement, and they get their own guard test below."""
        from scripts.build_features import FEATURE_ROWS_QUERY

        block = FEATURE_ROWS_QUERY[FEATURE_ROWS_QUERY.index("recruiting_points_3yr") :]
        block = block[: block.index("-- MIGRATION 047")]
        assert "COALESCE" not in block.upper()

    def test_a_draft_zero_is_only_written_where_the_drafts_exist(self):
        """The exception to 1i, and the reason it is safe.

        A count's zero can be true -- a program that sent nobody to the NFL
        produced zero picks -- but only if the drafts were LOADED. Before the
        2000-2019 backfill a bare COALESCE(...,0) read an uningested draft as
        "produced zero picks" on 54.2%% of the screening frame, and the column
        screened as a null without anything erroring.

        So each COALESCE must sit behind an EXISTS over the draft years, and
        that EXISTS must NOT be filtered to the team: a team absent from a
        draft that was loaded is a real zero, and collapsing the two cases is
        precisely the defect."""
        from scripts.build_features import FEATURE_ROWS_QUERY

        block = FEATURE_ROWS_QUERY[FEATURE_ROWS_QUERY.index("-- MIGRATION 047") :]
        block = block[: block.index("AS draft_departures")]
        assert "COUNT(DISTINCT d.season)" in block, (
            "the three-year window needs all three years, not any one"
        )
        assert ") = 3" in block, "the source guard must require exactly three drafts"
        assert block.count("EXISTS (") == 1, "draft_departures is a single year, so EXISTS is exact"
        assert "d.team" not in block, (
            "the source-year guard must not be filtered to the team, or a real "
            "zero and an unloaded draft become indistinguishable again"
        )
        # The team predicate belongs on the inner sum, not the guard.
        assert "d2.team = s.team" in block

    def test_coach_tenure_uses_islands(self):
        """A coach returning to a school must not inherit his first stint's
        start year, or hc_first_year silently misses every re-hire."""
        from scripts.build_features import FEATURE_ROWS_QUERY

        assert "coach_islands" in FEATURE_ROWS_QUERY
        assert "PARTITION BY i.school, i.coach_id, i.grp" in FEATURE_ROWS_QUERY

    def test_prior_season_join_is_leak_free(self):
        """S-1, never S: season S's own advanced stats are not knowable in
        week 1 and would leak the outcome into its own predictor."""
        from scripts.build_features import FEATURE_ROWS_QUERY

        assert "ats.season = s.season - 1" in FEATURE_ROWS_QUERY

    def test_query_still_binds_with_only_the_season_parameter(self):
        """FEATURE_ROWS_QUERY became an f-string to interpolate the class
        window. A stray literal % or brace would break psycopg2 binding at
        runtime -- the exact failure that left the feature screen unable to
        execute its own query for months."""
        import re

        from scripts.build_features import FEATURE_ROWS_QUERY

        stripped = re.sub(r"%\([a-z_]+\)s", "", FEATURE_ROWS_QUERY).replace("%%", "")
        assert "%" not in stripped
        assert "{" not in FEATURE_ROWS_QUERY and "}" not in FEATURE_ROWS_QUERY
        # Exercise the real binding path.
        FEATURE_ROWS_QUERY % {"season": 2026}


class TestFittedVectorContract:
    def test_every_shipped_screen_column_is_a_model_feature(self):
        from scripts.screen_preseason_features import SHIPPED_BY_DECISION, SHIPPED_COLUMNS
        from scripts.train_model import TEAM_WEEK_SOURCE_COLUMNS

        for col in list(SHIPPED_COLUMNS) + list(SHIPPED_BY_DECISION):
            assert col in TEAM_WEEK_SOURCE_COLUMNS, (
                f"{col} cleared the gate but never reached the model vector"
            )

    def test_no_rejected_column_leaked_into_the_vector(self):
        from scripts.screen_preseason_features import REJECTED_COLUMNS, UNTESTABLE_COLUMNS
        from scripts.train_model import TEAM_WEEK_SOURCE_COLUMNS

        for col in list(REJECTED_COLUMNS) + list(UNTESTABLE_COLUMNS):
            assert col not in TEAM_WEEK_SOURCE_COLUMNS, f"{col} failed the screen but is in the fit"

    def test_no_superseded_column_is_still_in_the_vector(self):
        """SUPERSEDED means "cleared the screen, deliberately held back" -- for
        these columns that is because a shipped column is a linear combination
        of them. Leaving one in the fit alongside its replacement would put an
        exact dependence in the design matrix, which is the thing the bucket
        exists to prevent."""
        from scripts.screen_preseason_features import SUPERSEDED_COLUMNS
        from scripts.train_model import TEAM_WEEK_SOURCE_COLUMNS

        for col in SUPERSEDED_COLUMNS:
            assert col not in TEAM_WEEK_SOURCE_COLUMNS, (
                f"{col} was superseded but is still in the fit"
            )

    def test_feature_names_are_unique_and_positional(self):
        from scripts.train_model import DIFF_FEATURE_COLUMNS, FEATURE_NAMES

        assert len(FEATURE_NAMES) == len(set(FEATURE_NAMES))
        assert len(FEATURE_NAMES) == len(DIFF_FEATURE_COLUMNS) + 2


class TestBackfillCoversTheUpcomingSeason:
    """--from must not stop one season short of the one being asked about.

    get_current_season() is a CALENDAR rule returning year-1 until August, and
    it is correct only for ingest year-windows. Phase 1 rewired --incremental
    onto get_projection_seasons for exactly this reason and left --from behind.
    On 2026-07-26 the migration-042 rebuild ran `--from 2015`, built 2015-2025,
    skipped 2026 entirely, and exited 0 -- so the five new preseason columns
    were NULL for the only season anyone wanted them for, while every gate line
    reported success.
    """

    def test_from_branch_does_not_use_the_calendar_rule_alone(self):
        import inspect

        from scripts import build_features

        src = inspect.getsource(build_features.main)
        from_branch = src[src.index("else:") :]
        assert "_resolve_projection_seasons()" in from_branch, (
            "--from must bound on core.games (get_projection_seasons), not the "
            "calendar's get_current_season"
        )

    def test_upper_bound_is_the_max_of_both_sources(self):
        """Belt and braces: whichever is later wins, so the range can never be
        shorter than either rule alone would give."""
        import inspect

        from scripts import build_features

        src = inspect.getsource(build_features.main)
        assert "max([*projection_seasons, get_current_season()])" in src
