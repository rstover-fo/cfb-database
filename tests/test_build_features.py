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
