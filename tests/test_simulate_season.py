"""Unit tests for the season-simulation math (no DB, no I/O).

Covers scripts/simulate_season.py's pure core: the Monte Carlo win draw,
distribution/percentile summaries, schedule strength, conference title odds,
SOS ranking and the row builder. Everything runs on plain dicts and numpy;
nothing touches Postgres.

Two tests are load-bearing:

- ``test_completed_games_are_not_re_rolled`` -- an in-season projection must be
  "record so far + simulated remainder". Re-simulating games already played
  would let a 6-0 team project to 3-3.
- ``test_unscored_pending_games_are_skipped_not_coin_flipped`` -- treating a
  game with no prediction as 50/50 would bias every projection toward .500 in
  proportion to how much of the schedule is unscored, which is exactly the
  silent-degradation failure this project already hit once.
"""

import pytest

pytest.importorskip("numpy")

import numpy as np  # noqa: E402

from scripts.simulate_season import (  # noqa: E402
    BOWL_ELIGIBLE_WINS,
    COMPLETE_SCHEDULE_GAMES,
    assign_sos_ranks,
    build_projection_row,
    conference_title_probs,
    schedule_strength,
    simulate_wins,
    summarize,
    win_distribution,
)


def _game(home, away, completed=False, home_win=None, margin=None, season=2026):
    return {
        "season": season,
        "home_team": home,
        "away_team": away,
        "completed": completed,
        "home_win": home_win,
        "expected_home_margin": margin,
        "home_conference": "TestConf",
        "away_conference": "TestConf",
    }


class TestSimulateWins:
    def test_deterministic_under_fixed_seed(self):
        games = [_game("A", "B", margin=3.0), _game("C", "A", margin=-7.0)]
        a = simulate_wins(games, 500, 18.5, seed=42)
        b = simulate_wins(games, 500, 18.5, seed=42)
        for team in a:
            np.testing.assert_array_equal(a[team], b[team])

    def test_different_seeds_differ(self):
        games = [_game("A", "B", margin=0.0)]
        a = simulate_wins(games, 500, 18.5, seed=1)
        b = simulate_wins(games, 500, 18.5, seed=2)
        assert not np.array_equal(a["A"], b["A"])

    def test_completed_games_are_not_re_rolled(self):
        # A won all three completed games; every simulation must show 3 wins.
        games = [
            _game("A", "B", completed=True, home_win=True),
            _game("A", "C", completed=True, home_win=True),
            _game("D", "A", completed=True, home_win=False),
        ]
        wins = simulate_wins(games, 200, 18.5)
        assert np.all(wins["A"] == 3)
        assert np.all(wins["B"] == 0)

    def test_unscored_pending_games_are_skipped_not_coin_flipped(self):
        # One completed win plus a pending game with NO prediction. The
        # unscored game must contribute nothing rather than a 50/50 draw.
        games = [
            _game("A", "B", completed=True, home_win=True),
            _game("A", "C", margin=None),
        ]
        wins = simulate_wins(games, 500, 18.5)
        assert np.all(wins["A"] == 1)
        assert np.all(wins["C"] == 0)

    def test_huge_favorite_wins_nearly_always(self):
        games = [_game("A", "B", margin=100.0)]
        wins = simulate_wins(games, 2000, 18.5)
        assert wins["A"].mean() > 0.99
        assert wins["B"].mean() < 0.01

    def test_pickem_is_about_even(self):
        games = [_game("A", "B", margin=0.0)]
        wins = simulate_wins(games, 20000, 18.5)
        assert wins["A"].mean() == pytest.approx(0.5, abs=0.02)

    def test_every_game_produces_exactly_one_winner(self):
        games = [_game("A", "B", margin=5.0), _game("B", "C", margin=-2.0)]
        wins = simulate_wins(games, 300, 18.5)
        total = wins["A"] + wins["B"] + wins["C"]
        assert np.all(total == len(games))

    def test_larger_sigma_widens_the_outcome_spread(self):
        games = [_game("A", "B", margin=14.0)]
        tight = simulate_wins(games, 5000, 5.0, seed=3)["A"].mean()
        loose = simulate_wins(games, 5000, 30.0, seed=3)["A"].mean()
        # A 14-point favorite is less certain under a wider error distribution.
        assert tight > loose


class TestWinDistribution:
    def test_sums_to_one(self):
        wins = np.array([3, 4, 4, 5, 6, 6, 6, 7])
        dist = win_distribution(wins, 12)
        assert sum(dist.values()) == pytest.approx(1.0)

    def test_covers_full_range_including_zeros(self):
        dist = win_distribution(np.array([2, 2, 3]), 5)
        assert set(dist.keys()) == set(range(6))
        assert dist[0] == 0.0
        assert dist[2] == pytest.approx(2 / 3)

    def test_probabilities_are_non_negative(self):
        dist = win_distribution(np.array([0, 12, 6]), 12)
        assert all(p >= 0 for p in dist.values())


class TestSummarize:
    def test_median_lies_within_p10_p90(self):
        rng = np.random.default_rng(5)
        wins = rng.integers(0, 13, size=5000)
        s = summarize(wins, 12)
        assert s["wins_p10"] <= s["median_wins"] <= s["wins_p90"]
        assert s["wins_p25"] <= s["median_wins"] <= s["wins_p75"]

    def test_wins_plus_losses_equals_schedule(self):
        s = summarize(np.array([6, 7, 8]), 12)
        assert s["projected_wins"] + s["projected_losses"] == pytest.approx(12)

    def test_perfect_team_projects_full_schedule(self):
        s = summarize(np.full(100, 12), 12)
        assert s["projected_wins"] == pytest.approx(12.0)
        assert s["projected_losses"] == pytest.approx(0.0)
        assert s["p_ten_plus"] == pytest.approx(1.0)

    def test_bowl_eligibility_threshold(self):
        wins = np.array([5, 5, 6, 7])  # half at or above 6
        s = summarize(wins, 12)
        assert s["p_bowl_eligible"] == pytest.approx(0.5)
        assert BOWL_ELIGIBLE_WINS == 6

    def test_probabilities_are_monotone_in_strength(self):
        weak = summarize(np.full(1000, 4), 12)
        strong = summarize(np.full(1000, 9), 12)
        assert strong["p_bowl_eligible"] > weak["p_bowl_eligible"]


class TestScheduleStrength:
    def test_averages_opponent_ratings_both_sides(self):
        games = [_game("A", "B"), _game("C", "A")]
        sos = schedule_strength("A", games, {"B": 1600.0, "C": 1400.0})
        assert sos == pytest.approx(1500.0)

    def test_unrated_opponents_are_skipped_not_zeroed(self):
        # An FCS opponent with no rating must not drag the average toward 0.
        games = [_game("A", "B"), _game("A", "FCS Team")]
        sos = schedule_strength("A", games, {"B": 1600.0})
        assert sos == pytest.approx(1600.0)

    def test_returns_none_when_no_opponent_is_rated(self):
        assert schedule_strength("A", [_game("A", "B")], {}) is None


class TestConferenceTitleProbs:
    def test_probabilities_sum_to_one_within_a_conference(self):
        wins = {"A": np.array([8, 9]), "B": np.array([7, 6]), "C": np.array([5, 10])}
        conf = dict.fromkeys(("A", "B", "C"), "Big Test")
        played = dict.fromkeys(("A", "B", "C"), 12)
        probs = conference_title_probs(wins, conf, played)
        assert sum(probs.values()) == pytest.approx(1.0)

    def test_ties_split_evenly(self):
        wins = {"A": np.array([9, 9]), "B": np.array([9, 9])}
        conf = {"A": "C1", "B": "C1"}
        played = {"A": 12, "B": 12}
        probs = conference_title_probs(wins, conf, played)
        assert probs["A"] == pytest.approx(0.5)
        assert probs["B"] == pytest.approx(0.5)

    def test_dominant_team_takes_the_title(self):
        wins = {"A": np.array([12, 12]), "B": np.array([3, 2])}
        conf = {"A": "C1", "B": "C1"}
        played = {"A": 12, "B": 12}
        probs = conference_title_probs(wins, conf, played)
        assert probs["A"] == pytest.approx(1.0)

    def test_uses_win_pct_so_uneven_schedules_compare_fairly(self):
        # B wins fewer games but plays fewer, at a better rate.
        wins = {"A": np.array([6, 6]), "B": np.array([5, 5])}
        conf = {"A": "C1", "B": "C1"}
        played = {"A": 12, "B": 8}
        probs = conference_title_probs(wins, conf, played)
        assert probs["B"] > probs["A"]

    def test_single_team_conference_is_skipped(self):
        wins = {"Independent": np.array([6, 6])}
        probs = conference_title_probs(wins, {"Independent": "FBS Ind"}, {"Independent": 12})
        assert probs == {}


class TestSosRanks:
    def test_toughest_schedule_ranks_first(self):
        rows = [
            {"team": "A", "sos_rating": 1500.0, "sos_rank": None},
            {"team": "B", "sos_rating": 1700.0, "sos_rank": None},
            {"team": "C", "sos_rating": 1600.0, "sos_rank": None},
        ]
        ranked = {r["team"]: r["sos_rank"] for r in assign_sos_ranks(rows)}
        assert ranked == {"B": 1, "C": 2, "A": 3}

    def test_unrated_rows_keep_none(self):
        rows = [
            {"team": "A", "sos_rating": 1500.0, "sos_rank": None},
            {"team": "B", "sos_rating": None, "sos_rank": None},
        ]
        ranked = {r["team"]: r["sos_rank"] for r in assign_sos_ranks(rows)}
        assert ranked["A"] == 1
        assert ranked["B"] is None


class TestBuildProjectionRow:
    def _row(self):
        games = [
            _game("A", "B", completed=True, home_win=True),
            _game("C", "A", completed=True, home_win=False),
            _game("A", "D", margin=7.0),
        ]
        wins = simulate_wins(games, 500, 18.5)
        return build_projection_row(
            "A", wins["A"], games, {"B": 1500.0}, "TestConf", "fitted_v1", 500, 18.5, 0.25
        )

    def test_counts_actual_wins_from_both_sides(self):
        row = self._row()
        assert row["actual_wins"] == 2  # home win vs B, away win at C
        assert row["games_completed"] == 2
        assert row["games_scheduled"] == 3

    def test_short_schedule_is_flagged_incomplete(self):
        row = self._row()
        assert row["schedule_complete"] is False
        assert COMPLETE_SCHEDULE_GAMES == 11

    def test_win_distribution_keys_are_strings_for_jsonb(self):
        row = self._row()
        assert all(isinstance(k, str) for k in row["p_win_dist"])
        assert sum(row["p_win_dist"].values()) == pytest.approx(1.0)

    def test_playoff_prob_is_null_in_v1(self):
        assert self._row()["playoff_prob"] is None

    def test_carries_simulation_provenance(self):
        row = self._row()
        assert row["n_sims"] == 500
        assert row["residual_sigma"] == 18.5
        assert row["model_version"] == "fitted_v1"
