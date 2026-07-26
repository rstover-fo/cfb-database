"""Unit tests for the preseason backtest's pure metrics (no DB)."""

import numpy as np
import pytest

from scripts.backtest_preseason import (
    MIN_SIGMA_GAMES,
    baseline_prior_rate,
    brier,
    calibration_table,
    drop_outcome_dependent,
    interval_coverage,
    preseason_sigma,
    residual_quantiles,
    win_error_metrics,
)


class TestWinErrorMetrics:
    def test_perfect_prediction(self):
        m = win_error_metrics([7.0, 9.0, 3.0], [7.0, 9.0, 3.0])
        assert m["mae"] == 0.0
        assert m["rmse"] == 0.0
        assert m["bias"] == 0.0

    def test_mae_and_rmse(self):
        m = win_error_metrics([8.0, 6.0], [7.0, 8.0])
        assert m["mae"] == pytest.approx(1.5)
        assert m["rmse"] == pytest.approx(np.sqrt((1 + 4) / 2))

    def test_bias_is_signed(self):
        """A model can be accurate on average and still systematically
        optimistic; only the signed term shows it."""
        over = win_error_metrics([9.0, 9.0], [7.0, 7.0])
        under = win_error_metrics([5.0, 5.0], [7.0, 7.0])
        assert over["bias"] == pytest.approx(2.0)
        assert under["bias"] == pytest.approx(-2.0)
        assert over["mae"] == under["mae"]

    def test_empty_is_not_a_crash(self):
        m = win_error_metrics([], [])
        assert m == {"n": 0, "mae": None, "rmse": None, "bias": None}


class TestIntervalCoverage:
    def test_all_inside(self):
        assert interval_coverage([7, 8], [5, 6], [9, 10]) == 1.0

    def test_all_outside(self):
        assert interval_coverage([2, 12], [5, 6], [9, 10]) == 0.0

    def test_boundaries_count_as_inside(self):
        assert interval_coverage([5, 9], [5, 6], [9, 10]) == 1.0

    def test_partial(self):
        assert interval_coverage([7, 12], [5, 6], [9, 10]) == 0.5

    def test_empty(self):
        assert interval_coverage([], [], []) is None


class TestCalibrationTable:
    def test_perfectly_calibrated_bucket(self):
        probs = [0.75] * 100
        outcomes = [True] * 75 + [False] * 25
        rows = calibration_table(probs, outcomes)
        assert len(rows) == 1
        assert rows[0]["mean_predicted"] == pytest.approx(0.75)
        assert rows[0]["observed"] == pytest.approx(0.75)

    def test_overconfident_shows_gap(self):
        probs = [0.9] * 10
        outcomes = [True] * 3 + [False] * 7
        rows = calibration_table(probs, outcomes)
        assert rows[0]["mean_predicted"] > rows[0]["observed"]

    def test_probability_one_lands_in_final_bucket(self):
        """p == 1.0 must not fall through every half-open bucket."""
        rows = calibration_table([1.0], [True])
        assert len(rows) == 1
        assert rows[0]["bucket"] == "0.9-1.0"

    def test_empty_buckets_are_omitted(self):
        rows = calibration_table([0.05, 0.95], [False, True])
        assert [r["bucket"] for r in rows] == ["0.0-0.1", "0.9-1.0"]

    def test_empty_input(self):
        assert calibration_table([], []) == []


class TestBaselinePriorRate:
    def test_scales_rate_to_this_seasons_slate(self):
        """The whole point: a 12-game record projected onto a 9-game slate."""
        assert baseline_prior_rate(9.0, 12, 9) == pytest.approx(6.75)

    def test_no_prior_season_falls_back_to_500(self):
        assert baseline_prior_rate(None, None, 12) == pytest.approx(6.0)

    def test_zero_prior_games_does_not_divide_by_zero(self):
        assert baseline_prior_rate(0.0, 0, 12) == pytest.approx(6.0)

    def test_undefeated_prior_projects_full_slate(self):
        assert baseline_prior_rate(12.0, 12, 11) == pytest.approx(11.0)


class TestPreseasonSigma:
    def test_requires_minimum_sample(self):
        assert preseason_sigma([1.0] * (MIN_SIGMA_GAMES - 1)) is None

    def test_returns_sd_once_sample_is_large_enough(self):
        rng = np.random.default_rng(0)
        residuals = list(rng.normal(0.0, 17.0, size=MIN_SIGMA_GAMES * 20))
        sigma = preseason_sigma(residuals)
        assert sigma == pytest.approx(17.0, abs=0.6)

    def test_degenerate_zero_variance_is_rejected(self):
        """A zero sigma makes every simulated game deterministic; that is a
        broken distribution, not a confident one."""
        assert preseason_sigma([5.0] * (MIN_SIGMA_GAMES * 2)) is None

    def test_empty(self):
        assert preseason_sigma([]) is None


class TestBrier:
    def test_perfect_forecast(self):
        assert brier([1.0, 0.0], [True, False]) == pytest.approx(0.0)

    def test_worst_forecast(self):
        assert brier([0.0, 1.0], [True, False]) == pytest.approx(1.0)

    def test_coin_flip(self):
        assert brier([0.5, 0.5], [True, False]) == pytest.approx(0.25)

    def test_empty(self):
        assert brier([], []) is None


class TestLeakGuards:
    """The backtest's value rests entirely on these two properties, so they are
    asserted against the module rather than trusted from the docstring."""

    def test_season_is_scored_with_the_prior_seasons_fit(self):
        from scripts.score_fitted import select_train_through

        assert select_train_through("backfill", 2022) == 2021

    def test_games_are_simulated_as_pending(self):
        """simulate_wins hands a completed game a deterministic win, so a
        backtest that left completed=True would reproduce the actual record and
        report zero error."""
        from scripts.simulate_season import simulate_wins

        games = [
            {
                "home_team": "A",
                "away_team": "B",
                "completed": True,
                "home_win": True,
                "conference_game": False,
            }
        ]
        sim = simulate_wins(games, n_sims=50, sigma=17.0, seed=1)
        assert set(sim["wins"]["A"]) == {1}, "completed games are deterministic -- must be pending"

        pending = [
            {
                "home_team": "A",
                "away_team": "B",
                "completed": False,
                "expected_home_margin": 0.0,
                "conference_game": False,
            }
        ]
        sim2 = simulate_wins(pending, n_sims=500, sigma=17.0, seed=1)
        wins = sim2["wins"]["A"]
        assert set(wins) == {0, 1}, "a pending coin-flip game must vary across simulations"


class TestDropOutcomeDependent:
    """Codex PR #51 P1: season_type='regular' still admits conference
    championship games (and, below FBS, whole playoff brackets), whose
    participants were decided by that season's results."""

    @staticmethod
    def _slate(team, n, start_id=0, opponent_prefix="opp"):
        return [
            {
                "game_id": start_id + i,
                "home_team": team,
                "away_team": f"{opponent_prefix}{i}",
                "start_date": f"2024-09-{i + 1:02d}",
            }
            for i in range(n)
        ]

    def test_keeps_a_standard_twelve_game_slate(self):
        kept, dropped = drop_outcome_dependent(self._slate("A", 12))
        assert len(kept) == 12
        assert dropped == 0

    def test_drops_the_thirteenth_game(self):
        """The conference championship: earned, never scheduled."""
        kept, dropped = drop_outcome_dependent(self._slate("A", 13))
        assert len(kept) == 12
        assert dropped == 1

    def test_drops_a_deep_playoff_run(self):
        kept, dropped = drop_outcome_dependent(self._slate("A", 15))
        assert len(kept) == 12
        assert dropped == 3

    def test_drops_when_beyond_the_cap_for_either_side(self):
        """Both participants must keep identical slates or the actual-wins
        comparison stops being matched."""
        games = self._slate("A", 12)
        # B's first game, but A's thirteenth.
        games.append(
            {"game_id": 99, "home_team": "A", "away_team": "B", "start_date": "2024-12-07"}
        )
        kept, dropped = drop_outcome_dependent(games)
        assert dropped == 1
        assert all(g["game_id"] != 99 for g in kept)

    def test_ordering_is_chronological_not_insertion_order(self):
        games = self._slate("A", 13)
        shuffled = list(reversed(games))
        kept, dropped = drop_outcome_dependent(shuffled)
        assert dropped == 1
        # The dropped game is the LAST by date, whatever order it arrived in.
        assert all(g["start_date"] != "2024-09-13" for g in kept)

    def test_null_start_dates_do_not_crash(self):
        games = [
            {"game_id": 1, "home_team": "A", "away_team": "X", "start_date": None},
            {"game_id": 2, "home_team": "A", "away_team": "Y", "start_date": "2024-09-01"},
        ]
        kept, dropped = drop_outcome_dependent(games)
        assert len(kept) == 2
        assert dropped == 0

    def test_empty(self):
        assert drop_outcome_dependent([]) == ([], 0)


class TestResidualQuantiles:
    """Codex PR #51 P1: MAE is an average loss, not an interval half-width.
    Quoting `point +/- MAE` as an honest range overstates the coverage badly."""

    def test_quantiles_of_a_known_distribution(self):
        rng = np.random.default_rng(7)
        actual = rng.normal(0.0, 1.0, size=20000)
        projected = np.zeros_like(actual)
        q = residual_quantiles(list(projected), list(actual))
        assert q["p10"] == pytest.approx(-1.2816, abs=0.05)
        assert q["p90"] == pytest.approx(1.2816, abs=0.05)
        assert q["p50"] == pytest.approx(0.0, abs=0.05)

    def test_sign_convention_is_actual_minus_projected(self):
        """A model that under-projects must show POSITIVE residuals."""
        q = residual_quantiles([5.0] * 100, [7.0] * 100)
        assert q["p50"] == pytest.approx(2.0)

    def test_mae_is_a_narrower_span_than_the_80pct_interval(self):
        """The actual defect, stated as a test: for a normal error the +/-MAE
        band is materially narrower than p10..p90, so the two must not be used
        interchangeably."""
        rng = np.random.default_rng(11)
        actual = list(rng.normal(0.0, 2.24, size=20000))
        projected = [0.0] * len(actual)
        q = residual_quantiles(projected, actual)
        mae = win_error_metrics(projected, actual)["mae"]
        assert (q["p90"] - q["p10"]) > 2 * mae * 1.3, (
            "p10..p90 must be clearly wider than the +/- MAE band"
        )

    def test_empty(self):
        assert residual_quantiles([], []) is None
