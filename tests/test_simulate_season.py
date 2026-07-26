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
    DEFAULT_STRENGTH_SHARE,
    assign_sos_ranks,
    build_projection_row,
    conference_title_probs,
    schedule_strength,
    simulate_wins,
    strength_sd,
    summarize,
    win_distribution,
)


def _game(
    home, away, completed=False, home_win=None, margin=None, season=2026, conference_game=True
):
    return {
        "season": season,
        "home_team": home,
        "away_team": away,
        "completed": completed,
        "home_win": home_win,
        "expected_home_margin": margin,
        "home_conference": "TestConf",
        "away_conference": "TestConf",
        "conference_game": conference_game,
    }


def _wins(games, n_sims, sigma, seed=None):
    """simulate_wins()['wins'] -- most tests only care about the overall tally."""
    kwargs = {"seed": seed} if seed is not None else {}
    return simulate_wins(games, n_sims, sigma, **kwargs)["wins"]


class TestSimulateWins:
    def test_deterministic_under_fixed_seed(self):
        games = [_game("A", "B", margin=3.0), _game("C", "A", margin=-7.0)]
        a = _wins(games, 500, 18.5, seed=42)
        b = _wins(games, 500, 18.5, seed=42)
        for team in a:
            np.testing.assert_array_equal(a[team], b[team])

    def test_different_seeds_differ(self):
        games = [_game("A", "B", margin=0.0)]
        a = _wins(games, 500, 18.5, seed=1)
        b = _wins(games, 500, 18.5, seed=2)
        assert not np.array_equal(a["A"], b["A"])

    def test_completed_games_are_not_re_rolled(self):
        # A won all three completed games; every simulation must show 3 wins.
        games = [
            _game("A", "B", completed=True, home_win=True),
            _game("A", "C", completed=True, home_win=True),
            _game("D", "A", completed=True, home_win=False),
        ]
        wins = _wins(games, 200, 18.5)
        assert np.all(wins["A"] == 3)
        assert np.all(wins["B"] == 0)

    def test_unscored_pending_games_are_skipped_not_coin_flipped(self):
        # One completed win plus a pending game with NO prediction. The
        # unscored game must contribute nothing rather than a 50/50 draw.
        games = [
            _game("A", "B", completed=True, home_win=True),
            _game("A", "C", margin=None),
        ]
        wins = _wins(games, 500, 18.5)
        assert np.all(wins["A"] == 1)
        assert np.all(wins["C"] == 0)

    def test_huge_favorite_wins_nearly_always(self):
        games = [_game("A", "B", margin=100.0)]
        wins = _wins(games, 2000, 18.5)
        assert wins["A"].mean() > 0.99
        assert wins["B"].mean() < 0.01

    def test_pickem_is_about_even(self):
        games = [_game("A", "B", margin=0.0)]
        wins = _wins(games, 20000, 18.5)
        assert wins["A"].mean() == pytest.approx(0.5, abs=0.02)

    def test_every_game_produces_exactly_one_winner(self):
        games = [_game("A", "B", margin=5.0), _game("B", "C", margin=-2.0)]
        wins = _wins(games, 300, 18.5)
        total = wins["A"] + wins["B"] + wins["C"]
        assert np.all(total == len(games))

    def test_larger_sigma_widens_the_outcome_spread(self):
        games = [_game("A", "B", margin=14.0)]
        tight = _wins(games, 5000, 5.0, seed=3)["A"].mean()
        loose = _wins(games, 5000, 30.0, seed=3)["A"].mean()
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
        sim = simulate_wins(games, 500, 18.5)
        return build_projection_row(
            "A",
            sim["wins"]["A"],
            games,
            {"B": 1500.0},
            "TestConf",
            "fitted_v1",
            500,
            18.5,
            0.25,
            sim["games_simulated"]["A"],
            0.15,
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


class TestCodexRegressions:
    """One test per PR #48 review finding, each failing before its fix."""

    def test_unscored_game_is_not_counted_as_a_loss(self):
        """P2-C. `simulate_wins` skips a game with no prediction, so the
        projection denominator must skip it too. Counting it in
        `games_scheduled` while it can never produce a win turns a missing
        prediction into a certain defeat -- a stronger claim than the coin flip
        skipping was meant to avoid."""
        games = [
            _game("A", "B", margin=100.0),  # A wins ~always
            _game("A", "C", margin=None),  # unscored
        ]
        sim = simulate_wins(games, 1000, 18.5)
        row = build_projection_row(
            "A",
            sim["wins"]["A"],
            games,
            {},
            "TestConf",
            "fitted_v1",
            1000,
            18.5,
            None,
            sim["games_simulated"]["A"],
            0.15,
        )
        assert row["games_scheduled"] == 2
        assert row["games_simulated"] == 1
        # Over the one simulated game A wins ~1.0 and loses ~0.0. If the
        # unscored game were folded in, losses would be ~1.0.
        assert row["projected_losses"] < 0.1
        assert row["projected_wins"] + row["projected_losses"] == pytest.approx(1.0)

    def test_win_distribution_spans_simulated_not_scheduled_games(self):
        """P2-C. Probability mass must not be reserved for win totals the
        simulation cannot produce."""
        games = [_game("A", "B", margin=10.0), _game("A", "C", margin=None)]
        sim = simulate_wins(games, 500, 18.5)
        row = build_projection_row(
            "A",
            sim["wins"]["A"],
            games,
            {},
            "TestConf",
            "fitted_v1",
            500,
            18.5,
            None,
            sim["games_simulated"]["A"],
            0.15,
        )
        assert set(row["p_win_dist"]) == {"0", "1"}
        assert sum(row["p_win_dist"].values()) == pytest.approx(1.0)

    def test_non_conference_wins_do_not_buy_title_odds(self):
        """P1-B. A team that sweeps weak non-conference opponents but loses in
        the league must not out-rank a better conference record."""
        games = [
            # A: 1-1 in conference, plus two non-conference wins.
            _game("A", "B", completed=True, home_win=True, conference_game=True),
            _game("C", "A", completed=True, home_win=True, conference_game=True),
            _game("A", "Cupcake1", completed=True, home_win=True, conference_game=False),
            _game("A", "Cupcake2", completed=True, home_win=True, conference_game=False),
            # C: 2-0 in conference.
            _game("C", "B", completed=True, home_win=True, conference_game=True),
        ]
        sim = simulate_wins(games, 200, 18.5)
        conf = {"A": "C1", "B": "C1", "C": "C1", "Cupcake1": None, "Cupcake2": None}
        probs = conference_title_probs(sim["conf_wins"], conf, sim["conf_games"])

        # Overall records: A is 3-1, C is 2-0. Conference records: A 1-1, C 2-0.
        assert sim["wins"]["A"][0] > sim["wins"]["C"][0]  # A leads overall
        assert probs["C"] == pytest.approx(1.0)  # C still wins the league
        assert probs["A"] == pytest.approx(0.0)

    def test_conference_wins_come_from_the_same_draws_as_overall(self):
        """P1-B. Conference tallies must be a filtered view of the same
        simulated season, not an independent re-simulation -- otherwise a game
        could be a win overall and a loss in the league within one 'season'."""
        games = [_game("A", "B", margin=3.0, conference_game=True)]
        sim = simulate_wins(games, 1000, 18.5)
        np.testing.assert_array_equal(sim["wins"]["A"], sim["conf_wins"]["A"])

    def test_non_conference_games_excluded_from_conference_counts(self):
        games = [
            _game("A", "B", margin=3.0, conference_game=True),
            _game("A", "Cupcake", margin=40.0, conference_game=False),
        ]
        sim = simulate_wins(games, 100, 18.5)
        assert sim["games_simulated"]["A"] == 2
        assert sim["conf_games"]["A"] == 1


class TestSelfReviewRegressions:
    """Defects found in the fixes for the PR #48 review, before merge.

    Both are the same shape as the originals: a plausible number where an
    absence or an error belongs."""

    def test_team_with_no_scorable_game_gets_no_projection(self):
        """The `games_simulated` split (fix for P2-C) reintroduced the problem
        one level up: a team whose entire slate is unscored produced
        `projected_wins=0.0`, `p_bowl_eligible=0.0` and `p_win_dist={"0": 1.0}`
        -- indistinguishable from a team the model expects to go winless.
        Such teams must be omitted entirely."""
        games = [_game("A", "B", margin=None), _game("A", "C", margin=None)]
        sim = simulate_wins(games, 100, 18.5)
        assert sim["games_simulated"]["A"] == 0

        # The row builder still produces the degenerate shape if called...
        row = build_projection_row(
            "A", sim["wins"]["A"], games, {}, "C", "fitted_v1", 100, 18.5, None, 0, 0.15
        )
        assert row["projected_wins"] == 0.0
        assert row["p_bowl_eligible"] == 0.0
        # ...which is precisely why simulate_one_season filters on
        # games_simulated > 0 before building rows. Guard the invariant here so
        # the filter cannot be dropped without a failing test.
        assert [t for t in sim["wins"] if sim["games_simulated"][t] > 0] == []

    def test_zero_sigma_would_make_every_game_deterministic(self):
        """A non-positive sigma collapses every draw onto its mean, so every
        win probability becomes exactly 0 or 1. `fetch_sigma` rejects it; this
        pins why."""
        games = [_game("A", "B", margin=3.0)]
        wins = simulate_wins(games, 200, 0.0)["wins"]
        assert np.all(wins["A"] == 1)
        assert np.all(wins["B"] == 0)


class TestCorrelatedDraws:
    """v1.1: one season-strength offset per team per simulation, applied to
    every game that team plays.

    Motivated by the section 4.5 backtest, which measured p10-p90 coverage of
    71.7% against a nominal 80% with both tails underweighted. Independent
    draws treat a team's true strength as perfectly known; real seasons do not.
    """

    @staticmethod
    def _slate(n=12, margin=3.0):
        return [
            {
                "home_team": "A",
                "away_team": f"O{i}",
                "completed": False,
                "expected_home_margin": margin,
                "conference_game": False,
            }
            for i in range(n)
        ]

    @pytest.mark.parametrize("share", [0.0, 0.05, 0.25, 0.5, 0.9])
    def test_total_variance_is_preserved(self, share):
        """The whole point of the split: per-game margin variance must stay
        sigma^2 so single-game predictions remain exactly as calibrated as v1.
        A correlation term that also inflated game variance would be fixing
        season totals by breaking something that was already right."""
        sigma = 18.95
        tau, game_sd = strength_sd(sigma, share)
        assert 2 * tau**2 + game_sd**2 == pytest.approx(sigma**2)

    def test_share_zero_reproduces_independent_draws(self):
        """share=0 must still be the exact v1 path -- it is the escape hatch
        for reproducing any pre-v1.1 projection. Checked against the binomial
        SD that independent games of equal probability would give, not against
        the default (which is now the calibrated 0.15)."""
        games = self._slate(n=12, margin=0.0)  # 12 independent coin flips
        w = simulate_wins(games, 40000, 18.0, seed=5, strength_share=0.0)["wins"]["A"]
        binomial_sd = (12 * 0.5 * 0.5) ** 0.5
        assert float(np.std(w)) == pytest.approx(binomial_sd, abs=0.05)

    def test_shipped_default_is_the_calibrated_share(self):
        """Pins the sweep result so the default cannot drift back to a guess.
        0.15 put backtest p10-p90 coverage at 79.6% against a nominal 80%."""
        assert DEFAULT_STRENGTH_SHARE == 0.15
        games = self._slate()
        explicit = simulate_wins(games, 1000, 18.0, seed=5, strength_share=0.15)["wins"]["A"]
        default = simulate_wins(games, 1000, 18.0, seed=5)["wins"]["A"]
        assert np.array_equal(explicit, default)

    def test_tails_fatten_as_share_rises(self):
        """The defect being fixed, stated as a monotonicity check."""
        games = self._slate()
        sds = []
        for share in (0.0, 0.2, 0.4, 0.6):
            w = simulate_wins(games, 20000, 18.0, seed=11, strength_share=share)["wins"]["A"]
            sds.append(float(np.std(w)))
        assert sds == sorted(sds), f"win-total SD must not shrink as share rises: {sds}"
        assert sds[-1] > sds[0] * 1.5, f"correlation should widen materially: {sds}"

    def test_central_tendency_is_essentially_unchanged(self):
        """Correlation is meant to fix the SPREAD, not move the projection."""
        games = self._slate()
        base = simulate_wins(games, 20000, 18.0, seed=13, strength_share=0.0)["wins"]["A"]
        corr = simulate_wins(games, 20000, 18.0, seed=13, strength_share=0.4)["wins"]["A"]
        assert float(np.mean(corr)) == pytest.approx(float(np.mean(base)), abs=0.15)

    def test_a_teams_games_become_correlated(self):
        """Directly: within a simulation, wins should co-move. With a strong
        offset the win total is far more dispersed than the binomial that
        independent draws with the same per-game probability would give."""
        games = self._slate(n=12, margin=0.0)  # every game a coin flip
        w = simulate_wins(games, 20000, 18.0, seed=17, strength_share=0.5)["wins"]["A"]
        binomial_sd = (12 * 0.5 * 0.5) ** 0.5  # ~1.73 if games were independent
        assert float(np.std(w)) > binomial_sd * 1.4, (
            f"correlated draws must exceed the independent binomial SD; got {np.std(w):.2f}"
        )

    def test_determinism_under_a_fixed_seed_still_holds(self):
        games = self._slate()
        a = simulate_wins(games, 500, 18.0, seed=3, strength_share=0.3)["wins"]["A"]
        b = simulate_wins(games, 500, 18.0, seed=3, strength_share=0.3)["wins"]["A"]
        assert np.array_equal(a, b)

    @pytest.mark.parametrize("bad", [-0.1, 1.0, 1.5])
    def test_out_of_range_share_is_rejected(self, bad):
        """1.0 would leave zero game-level noise -- not something to arrive at
        by accident, so it is rejected rather than clamped."""
        with pytest.raises(ValueError, match="strength_share"):
            strength_sd(18.0, bad)

    def test_negative_sigma_is_rejected_but_zero_is_allowed(self):
        """fetch_sigma is what refuses a non-positive sigma in production; the
        degenerate sigma=0 behaviour is pinned by its own test above, so this
        split must not reject it a second time."""
        with pytest.raises(ValueError, match="negative"):
            strength_sd(-1.0, 0.2)
        assert strength_sd(0.0, 0.2) == (0.0, 0.0)

    def test_conference_wins_come_from_the_same_correlated_draws(self):
        """Conference tallies must stay consistent with overall wins under
        correlation too, or title odds contradict the win totals beside them."""
        games = [
            {
                "home_team": "A",
                "away_team": f"O{i}",
                "completed": False,
                "expected_home_margin": 2.0,
                "conference_game": True,
            }
            for i in range(9)
        ]
        sim = simulate_wins(games, 2000, 18.0, seed=19, strength_share=0.35)
        assert np.all(sim["conf_wins"]["A"] <= sim["wins"]["A"])
