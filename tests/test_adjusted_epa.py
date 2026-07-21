"""Unit tests for the ridge-adjusted EPA solver core (no DB).

Covers scripts/compute_adjusted_epa.py's RidgeAccumulator: synthetic-league
coefficient recovery, lambda -> infinity shrinkage, home-field-advantage
recovery, and exact XtX/Xty accumulation against an independently built
dense design matrix.
"""

import pytest

pytest.importorskip("numpy")

import numpy as np  # noqa: E402

from scripts.compute_adjusted_epa import RidgeAccumulator  # noqa: E402

TEAMS = ["Alpha", "Bravo", "Charlie", "Delta"]


def _generate_synthetic_league(
    n_plays: int,
    seed: int,
    mu_true: float,
    hfa_true: float,
    off_true: dict[str, float],
    def_true: dict[str, float],
    noise_sd: float,
    teams: list[str] = TEAMS,
) -> list[tuple[str, str, bool, float]]:
    """Deterministically generate plays for a small synthetic league.

    epa = mu + off_true[off] + def_true[def] + hfa * is_home_offense + noise
    """
    rng = np.random.default_rng(seed)
    plays = []
    for _ in range(n_plays):
        off_team, def_team = rng.choice(teams, size=2, replace=False)
        is_home_offense = bool(rng.integers(0, 2))
        noise = rng.normal(0.0, noise_sd)
        epa = mu_true + off_true[off_team] + def_true[def_team] + hfa_true * is_home_offense + noise
        plays.append((str(off_team), str(def_team), is_home_offense, float(epa)))
    return plays


MU_TRUE = 0.05
HFA_TRUE = 0.05
OFF_TRUE = {"Alpha": 0.12, "Bravo": -0.05, "Charlie": 0.20, "Delta": -0.15}
DEF_TRUE = {"Alpha": -0.08, "Bravo": 0.03, "Charlie": -0.02, "Delta": 0.10}
NOISE_SD = 0.02
N_PLAYS = 2000


def _fit_synthetic_league(lam: float):
    plays = _generate_synthetic_league(
        n_plays=N_PLAYS,
        seed=42,
        mu_true=MU_TRUE,
        hfa_true=HFA_TRUE,
        off_true=OFF_TRUE,
        def_true=DEF_TRUE,
        noise_sd=NOISE_SD,
    )
    accumulator = RidgeAccumulator(TEAMS)
    accumulator.add_plays(plays)
    mu, hfa, off_coef, def_coef, n_plays = accumulator.solve(lam)
    return plays, mu, hfa, off_coef, def_coef, n_plays


class TestSyntheticRecovery:
    """A small ridge fit (lam=1.0) should recover team effects closely."""

    def test_recovers_centered_offense_effects(self):
        _, _mu, _hfa, off_coef, _def_coef, _n = _fit_synthetic_league(lam=1.0)

        off_mean = np.mean(list(off_coef.values()))
        true_mean = np.mean(list(OFF_TRUE.values()))
        for team in TEAMS:
            fitted_centered = off_coef[team] - off_mean
            true_centered = OFF_TRUE[team] - true_mean
            assert abs(fitted_centered - true_centered) < 0.02, (
                f"{team}: fitted_centered={fitted_centered:.4f} true_centered={true_centered:.4f}"
            )

    def test_recovers_centered_defense_effects(self):
        _, _mu, _hfa, _off_coef, def_coef, _n = _fit_synthetic_league(lam=1.0)

        def_mean = np.mean(list(def_coef.values()))
        true_mean = np.mean(list(DEF_TRUE.values()))
        for team in TEAMS:
            fitted_centered = def_coef[team] - def_mean
            true_centered = DEF_TRUE[team] - true_mean
            assert abs(fitted_centered - true_centered) < 0.02, (
                f"{team}: fitted_centered={fitted_centered:.4f} true_centered={true_centered:.4f}"
            )

    def test_n_plays_matches_generated_count(self):
        _, _mu, _hfa, _off_coef, _def_coef, n_plays = _fit_synthetic_league(lam=1.0)
        assert n_plays == N_PLAYS


class TestHomeFieldAdvantageRecovery:
    """hfa is unpenalized and should recover close to its true value."""

    def test_hfa_recovered_within_tolerance(self):
        _, _mu, hfa, _off_coef, _def_coef, _n = _fit_synthetic_league(lam=1.0)
        assert abs(hfa - HFA_TRUE) < 0.02


class TestShrinkage:
    """As lam -> infinity, team coefficients vanish and mu -> overall mean EPA."""

    def test_team_coefficients_shrink_to_zero(self):
        _, _mu, _hfa, off_coef, def_coef, _n = _fit_synthetic_league(lam=1e9)
        for team in TEAMS:
            assert abs(off_coef[team]) < 1e-3, f"off[{team}]={off_coef[team]}"
            assert abs(def_coef[team]) < 1e-3, f"def[{team}]={def_coef[team]}"

    def test_mu_approaches_overall_mean_epa(self):
        # With team coefficients pinned to ~0, mu/hfa reduce to the OLS fit of
        # epa ~ mu + hfa * is_home_offense alone (a 2-level ANOVA): mu is the
        # away-offense (is_home_offense=False) group mean and mu + hfa is the
        # home-offense group mean. hfa_true is nonzero here, so mu converges
        # to that away-group mean rather than the raw all-plays average.
        plays, mu, _hfa, _off_coef, _def_coef, _n = _fit_synthetic_league(lam=1e9)
        away_epa = [epa for _off, _def, is_home, epa in plays if not is_home]
        mean_away_epa = float(np.mean(away_epa))
        assert abs(mu - mean_away_epa) < 0.01


class TestAccumulatorCorrectness:
    """A tiny 5-play fixture: accumulator XtX/Xty must exactly match a
    dense design matrix built independently in the test."""

    def test_xtx_xty_match_dense_construction(self):
        teams = ["Alpha", "Bravo", "Charlie"]
        # (off_team, def_team, is_home_offense, epa)
        plays = [
            ("Alpha", "Bravo", True, 0.5),
            ("Bravo", "Alpha", False, -0.2),
            ("Charlie", "Alpha", True, 0.1),
            ("Alpha", "Charlie", False, 0.3),
            ("Bravo", "Charlie", True, -0.4),
        ]

        # Columns: [mu, hfa, off_Alpha, off_Bravo, off_Charlie,
        #           def_Alpha, def_Bravo, def_Charlie]
        off_col = {"Alpha": 2, "Bravo": 3, "Charlie": 4}
        def_col = {"Alpha": 5, "Bravo": 6, "Charlie": 7}

        rows = []
        y = []
        for off_team, def_team, is_home_offense, epa in plays:
            row = [0.0] * 8
            row[0] = 1.0
            row[1] = 1.0 if is_home_offense else 0.0
            row[off_col[off_team]] = 1.0
            row[def_col[def_team]] = 1.0
            rows.append(row)
            y.append(epa)

        x_dense = np.array(rows, dtype=np.float64)
        y_dense = np.array(y, dtype=np.float64)
        xtx_expected = x_dense.T @ x_dense
        xty_expected = x_dense.T @ y_dense

        accumulator = RidgeAccumulator(teams)
        accumulator.add_plays(plays)

        assert accumulator.xtx.shape == (8, 8)
        assert accumulator.xty.shape == (8,)
        np.testing.assert_allclose(accumulator.xtx, xtx_expected, atol=1e-10)
        np.testing.assert_allclose(accumulator.xty, xty_expected, atol=1e-10)

    def test_off_play_counts_track_offensive_plays_per_team(self):
        teams = ["Alpha", "Bravo", "Charlie"]
        plays = [
            ("Alpha", "Bravo", True, 0.5),
            ("Bravo", "Alpha", False, -0.2),
            ("Charlie", "Alpha", True, 0.1),
            ("Alpha", "Charlie", False, 0.3),
            ("Bravo", "Charlie", True, -0.4),
        ]
        accumulator = RidgeAccumulator(teams)
        accumulator.add_plays(plays)

        counts = dict(zip(teams, accumulator.off_play_counts.tolist(), strict=True))
        assert counts == {"Alpha": 2, "Bravo": 2, "Charlie": 1}
        assert accumulator.n_plays == 5

    def test_add_play_matches_add_plays(self):
        teams = ["Alpha", "Bravo", "Charlie"]
        plays = [
            ("Alpha", "Bravo", True, 0.5),
            ("Bravo", "Alpha", False, -0.2),
            ("Charlie", "Alpha", True, 0.1),
        ]

        via_add_play = RidgeAccumulator(teams)
        for off_team, def_team, is_home_offense, epa in plays:
            via_add_play.add_play(off_team, def_team, is_home_offense, epa)

        via_add_plays = RidgeAccumulator(teams)
        via_add_plays.add_plays(plays)

        np.testing.assert_allclose(via_add_play.xtx, via_add_plays.xtx)
        np.testing.assert_allclose(via_add_play.xty, via_add_plays.xty)
