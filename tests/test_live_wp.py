"""Unit tests for the house live win-probability model (no DB, no network).

Covers scripts/poll_scoreboard.py's pure core -- house_live_home_wp (the
closed-form formula from migration 029's header:
src/schemas/migrations/029_live_schema.sql), clock parsing (including the
documented overtime rule), and the snapshot dedup hash -- plus
scripts/calibrate_live_wp.py's pure sigma grid search, recovered against
synthetic data generated from a known ground-truth sigma, per
docs/plans/2026-07-21-tier3-analytics-plan.md, Pillar D.
"""

import random

import pytest

from scripts.calibrate_live_wp import brier_score, grid_search_sigma
from scripts.poll_scoreboard import (
    clamp,
    house_live_home_wp,
    parse_clock,
    snapshot_hash,
)


class TestClamp:
    def test_within_range(self):
        assert clamp(0.5, 0.0, 1.0) == pytest.approx(0.5)

    def test_below_range(self):
        assert clamp(-1.0, 0.0, 1.0) == pytest.approx(0.0)

    def test_above_range(self):
        assert clamp(2.0, 0.0, 1.0) == pytest.approx(1.0)


class TestHouseLiveHomeWp:
    """f = clamp(seconds_remaining/3600, eps, 1); projected = current_margin +
    pregame_expected_margin*f; wp = Phi(projected / (sigma*sqrt(f)))."""

    def test_seconds_zero_positive_margin_near_one(self):
        wp = house_live_home_wp(
            current_margin=7, pregame_expected_margin=3, seconds_remaining=0, sigma=16
        )
        assert wp == pytest.approx(1.0, abs=1e-6)

    def test_seconds_zero_negative_margin_near_zero(self):
        wp = house_live_home_wp(
            current_margin=-7, pregame_expected_margin=-3, seconds_remaining=0, sigma=16
        )
        assert wp == pytest.approx(0.0, abs=1e-6)

    def test_seconds_zero_tied_margin_near_half(self):
        wp = house_live_home_wp(
            current_margin=0, pregame_expected_margin=0, seconds_remaining=0, sigma=16
        )
        assert wp == pytest.approx(0.5, abs=1e-9)

    def test_full_game_remaining_matches_pregame_sign(self):
        # f=1, current_margin=0 -> projected = pregame_expected_margin,
        # z = pregame_expected_margin / sigma -- consistent in *sign* and
        # *monotonicity* with the pregame margin (symmetric around 0.5),
        # though not numerically identical to the Elo-logistic pregame win
        # prob (a different model family).
        wp_favored = house_live_home_wp(
            current_margin=0, pregame_expected_margin=10, seconds_remaining=3600, sigma=16
        )
        wp_underdog = house_live_home_wp(
            current_margin=0, pregame_expected_margin=-10, seconds_remaining=3600, sigma=16
        )
        assert wp_favored > 0.5
        assert wp_underdog < 0.5
        assert wp_favored == pytest.approx(1 - wp_underdog, abs=1e-9)

    def test_monotone_in_current_margin(self):
        wps = [
            house_live_home_wp(
                current_margin=m, pregame_expected_margin=0, seconds_remaining=1800, sigma=16
            )
            for m in (-10, -3, 0, 3, 10)
        ]
        assert wps == sorted(wps)
        assert len(set(wps)) == len(wps)  # strictly increasing, no ties

    def test_monotone_in_pregame_expected_margin(self):
        wps = [
            house_live_home_wp(
                current_margin=0, pregame_expected_margin=m, seconds_remaining=3600, sigma=16
            )
            for m in (-10, -3, 0, 3, 10)
        ]
        assert wps == sorted(wps)
        assert len(set(wps)) == len(wps)

    def test_sigma_pulls_toward_half(self):
        small_sigma_wp = house_live_home_wp(
            current_margin=7, pregame_expected_margin=0, seconds_remaining=1800, sigma=8
        )
        large_sigma_wp = house_live_home_wp(
            current_margin=7, pregame_expected_margin=0, seconds_remaining=1800, sigma=32
        )
        assert small_sigma_wp > large_sigma_wp > 0.5


class TestParseClock:
    def test_period_1_kickoff(self):
        assert parse_clock("15:00", 1) == 3600

    def test_period_4_final(self):
        assert parse_clock("00:00", 4) == 0

    def test_period_2_midway(self):
        assert parse_clock("07:30", 2) == 2250

    def test_overtime_treated_as_epsilon(self):
        # Documented OT rule: any period > 4 collapses to seconds_remaining=0
        # regardless of the clock string -- house_live_home_wp's own
        # clamp(..., eps, 1) then floors that up to the eps epsilon, since
        # OT's untimed, sudden-death-after-2OT format doesn't map onto a
        # 3600s regulation clock. Deliberate simplification, not a bug.
        assert parse_clock("15:00", 5) == 0
        assert parse_clock("00:00", 7) == 0

    def test_malformed_clock_returns_none(self):
        assert parse_clock("garbage", 2) is None
        assert parse_clock(None, 2) is None
        assert parse_clock("07:30", None) is None


class TestSnapshotHash:
    def test_identical_state_same_hash(self):
        h1 = snapshot_hash(401520281, 2, "07:30", 14, 10, "home")
        h2 = snapshot_hash(401520281, 2, "07:30", 14, 10, "home")
        assert h1 == h2

    def test_field_change_yields_different_hash(self):
        base = snapshot_hash(401520281, 2, "07:30", 14, 10, "home")
        assert snapshot_hash(401520281, 2, "07:29", 14, 10, "home") != base  # clock
        assert snapshot_hash(401520281, 2, "07:30", 21, 10, "home") != base  # home_points
        assert snapshot_hash(401520281, 2, "07:30", 14, 17, "home") != base  # away_points
        assert snapshot_hash(401520281, 2, "07:30", 14, 10, "away") != base  # possession
        assert snapshot_hash(401520281, 3, "07:30", 14, 10, "home") != base  # period
        assert snapshot_hash(401520282, 2, "07:30", 14, 10, "home") != base  # game_id


class TestCalibrationGridRecovery:
    """Synthetic-data recovery: generate states + Bernoulli outcomes from
    house_live_home_wp at a KNOWN sigma=16, then check grid_search_sigma's
    Brier-minimizer lands within +/-1 of 16, per the Gate D calibration
    protocol scripts/calibrate_live_wp.py implements against real data."""

    def test_grid_recovers_known_sigma(self):
        true_sigma = 16.0
        rng = random.Random(20260721)
        states = []
        for _ in range(4000):
            current_margin = rng.uniform(-21, 21)
            pregame_expected_margin = rng.uniform(-14, 14)
            seconds_remaining = rng.uniform(0, 3600)
            true_wp = house_live_home_wp(
                current_margin, pregame_expected_margin, seconds_remaining, true_sigma
            )
            home_win = 1.0 if rng.random() < true_wp else 0.0
            states.append(
                {
                    "current_margin": current_margin,
                    "pregame_expected_margin": pregame_expected_margin,
                    "seconds_remaining": seconds_remaining,
                    "home_win": home_win,
                }
            )

        sigma_grid = [float(s) for s in range(10, 25)]  # 10..24 step 1
        results = grid_search_sigma(states, sigma_grid)
        winning_sigma, _winning_brier = min(results, key=lambda r: r[1])

        assert abs(winning_sigma - true_sigma) <= 1.0

    def test_brier_score_perfect_predictions_is_zero(self):
        assert brier_score([1.0, 0.0, 1.0], [1.0, 0.0, 1.0]) == pytest.approx(0.0)

    def test_brier_score_worst_case_is_one(self):
        assert brier_score([1.0, 0.0], [0.0, 1.0]) == pytest.approx(1.0)

    def test_brier_score_empty_is_nan(self):
        assert brier_score([], []) != brier_score([], [])  # NaN != NaN
