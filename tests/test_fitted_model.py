"""Unit tests for the fitted_v1 math core (no DB, no I/O).

Covers scripts/train_model.py's pure math -- ridge normal equations, IRLS
logistic, Platt calibration, the leak-free imputation/standardization contract
(design doc sections 2b/2c) -- plus scripts/score_fitted.py's pure frozen-fit
selector, and an end-to-end mini walk-forward on synthetic data. Everything
here runs on numpy arrays / plain dicts; nothing touches Postgres.
"""

import pytest

pytest.importorskip("numpy")

import numpy as np  # noqa: E402

from scripts.score_fitted import select_train_through  # noqa: E402
from scripts.train_model import (  # noqa: E402
    FEATURE_NAMES,
    INTERCEPT_IDX,
    NEUTRAL_SITE_IDX,
    RIDGE_ALPHA,
    TEAM_WEEK_SOURCE_COLUMNS,
    WINPROB_ALPHA,
    build_design,
    build_feature_vector,
    collect_team_week_rows,
    compute_diff_stats,
    compute_feature_means,
    irls_logistic,
    penalty_mask,
    platt_fit,
    platt_transform,
    ridge_fit,
    sigmoid,
    standardize,
)

# =============================================================================
# 1. ridge_fit -- coefficient recovery and shrinkage.
# =============================================================================


class TestRidgeFit:
    def _centered_design(self):
        rng = np.random.default_rng(1)
        n = 200
        x1 = rng.normal(0.0, 1.0, n)
        x1 -= x1.mean()
        x2 = rng.normal(0.0, 1.0, n)
        x2 -= x2.mean()
        X = np.column_stack([np.ones(n), x1, x2])
        beta_true = np.array([5.0, 3.0, 2.0])
        y = X @ beta_true  # noiseless
        mask = np.array([0.0, 1.0, 1.0])  # intercept unpenalized
        return X, y, beta_true, mask

    def test_recovers_known_coefficients_as_alpha_to_zero(self):
        X, y, beta_true, mask = self._centered_design()
        beta = ridge_fit(X, y, alpha=1e-9, penalize_mask=mask)
        np.testing.assert_allclose(beta, beta_true, atol=1e-4)

    def test_penalized_coefficients_shrink_toward_zero(self):
        X, y, _beta_true, mask = self._centered_design()
        beta_small = ridge_fit(X, y, alpha=1e-2, penalize_mask=mask)
        beta_big = ridge_fit(X, y, alpha=1e6, penalize_mask=mask)
        # Each penalized slope shrinks with alpha, toward ~0 at huge alpha.
        assert abs(beta_big[1]) < abs(beta_small[1])
        assert abs(beta_big[2]) < abs(beta_small[2])
        assert abs(beta_big[1]) < 1e-2
        assert abs(beta_big[2]) < 1e-2

    def test_intercept_is_unshrunk(self):
        # Features are mean-centered, so the unpenalized intercept stays at the
        # mean of y (=5) even as the penalized slopes are driven to 0.
        X, y, _beta_true, mask = self._centered_design()
        beta_big = ridge_fit(X, y, alpha=1e6, penalize_mask=mask)
        assert beta_big[0] == pytest.approx(float(np.mean(y)), abs=1e-6)
        assert beta_big[0] == pytest.approx(5.0, abs=1e-3)


# =============================================================================
# 2. irls_logistic -- direction, reference gradient descent, valid probs.
# =============================================================================


def _reference_gd_logistic(X, y, lr=0.3, n_iter=60000):
    """Independent (mean-gradient) logistic MLE, as a cross-check on IRLS."""
    beta = np.zeros(X.shape[1])
    n = len(y)
    for _ in range(n_iter):
        p = 1.0 / (1.0 + np.exp(-(X @ beta)))
        beta = beta - lr * (X.T @ (p - y)) / n
    return beta


class TestIrlsLogistic:
    def _data(self):
        rng = np.random.default_rng(2)
        n = 600
        x = rng.normal(0.0, 1.0, n)
        X = np.column_stack([np.ones(n), x])
        true_logit = 0.8 * x + 0.3
        y = (rng.uniform(0.0, 1.0, n) < sigmoid(true_logit)).astype(float)
        return X, y

    def test_recovers_direction(self):
        X, y = self._data()
        beta = irls_logistic(X, y, alpha=0.0, penalize_mask=np.zeros(2))
        assert beta[1] > 0.0  # positive true slope
        assert beta[1] == pytest.approx(0.8, abs=0.3)

    def test_matches_reference_gradient_descent(self):
        X, y = self._data()
        beta_irls = irls_logistic(X, y, alpha=0.0, penalize_mask=np.zeros(2))
        beta_gd = _reference_gd_logistic(X, y)
        # Both target the same (unpenalized) MLE stationary point.
        np.testing.assert_allclose(beta_irls, beta_gd, atol=2e-2)

    def test_probabilities_strictly_between_zero_and_one(self):
        X, y = self._data()
        beta = irls_logistic(X, y, alpha=WINPROB_ALPHA, penalize_mask=np.array([0.0, 1.0]))
        p = sigmoid(X @ beta)
        assert np.all(p > 0.0)
        assert np.all(p < 1.0)


# =============================================================================
# 3. platt_fit -- recovers identity and corrects overconfidence.
# =============================================================================


class TestPlattFit:
    def _draw(self, seed=3, n=8000):
        rng = np.random.default_rng(seed)
        true_logits = rng.uniform(-3.0, 3.0, n)
        y = (rng.uniform(0.0, 1.0, n) < sigmoid(true_logits)).astype(float)
        return true_logits, y

    def test_calibrated_logits_give_identity(self):
        true_logits, y = self._draw()
        a, b = platt_fit(true_logits, y)
        assert a == pytest.approx(1.0, abs=0.12)
        assert b == pytest.approx(0.0, abs=0.12)

    def test_overconfident_logits_get_halved_slope(self):
        # If the model reports 2*logit but truth is sigmoid(logit), the Platt
        # slope should recover ~0.5 (a * 2*logit ~= logit).
        true_logits, y = self._draw()
        a, b = platt_fit(2.0 * true_logits, y)
        assert a == pytest.approx(0.5, abs=0.1)
        assert b == pytest.approx(0.0, abs=0.12)


# =============================================================================
# 4. Imputation -- NULL side filled with the frozen mean BEFORE differencing.
# =============================================================================


class TestImputation:
    def test_null_away_value_imputes_mean_before_diff(self):
        means = {c: 0.0 for c in TEAM_WEEK_SOURCE_COLUMNS}
        means["elo_pregame"] = 1500.0
        home_tw = {c: 0.0 for c in TEAM_WEEK_SOURCE_COLUMNS}
        home_tw["elo_pregame"] = 1600.0
        away_tw = {c: 0.0 for c in TEAM_WEEK_SOURCE_COLUMNS}
        away_tw["elo_pregame"] = None  # missing away side

        x = build_feature_vector({"neutral_site": False}, home_tw, away_tw, means)
        d_elo = x[FEATURE_NAMES.index("d_elo")]

        # diff = home(1600) - imputed_mean(1500) = 100, NOT home - 0 = 1600.
        assert d_elo == pytest.approx(1600.0 - 1500.0)
        assert d_elo != pytest.approx(1600.0)

    def test_intercept_and_neutral_site_set_directly(self):
        means = {c: 0.0 for c in TEAM_WEEK_SOURCE_COLUMNS}
        tw = {c: 0.0 for c in TEAM_WEEK_SOURCE_COLUMNS}
        x_home = build_feature_vector({"neutral_site": True}, tw, tw, means)
        assert x_home[INTERCEPT_IDX] == pytest.approx(1.0)
        assert x_home[NEUTRAL_SITE_IDX] == pytest.approx(1.0)
        x_away = build_feature_vector({"neutral_site": False}, tw, tw, means)
        assert x_away[NEUTRAL_SITE_IDX] == pytest.approx(0.0)


# =============================================================================
# 5. Standardization -- skips intercept + neutral_site, z uses frozen stats.
# =============================================================================


class TestStandardize:
    def _raw(self):
        rng = np.random.default_rng(5)
        n = 300
        X = np.zeros((n, len(FEATURE_NAMES)))
        X[:, INTERCEPT_IDX] = 1.0
        X[:, NEUTRAL_SITE_IDX] = rng.integers(0, 2, n).astype(float)
        for i in range(2, len(FEATURE_NAMES)):
            X[:, i] = rng.normal(loc=2.0 * i, scale=float(i + 1), size=n)
        return X

    def test_intercept_and_neutral_pass_through(self):
        X = self._raw()
        dm, ds = compute_diff_stats(X)
        Z = standardize(X, dm, ds)
        np.testing.assert_allclose(Z[:, INTERCEPT_IDX], 1.0)
        np.testing.assert_allclose(Z[:, NEUTRAL_SITE_IDX], X[:, NEUTRAL_SITE_IDX])

    def test_train_identical_matrix_is_zero_mean_unit_std(self):
        X = self._raw()
        dm, ds = compute_diff_stats(X)
        Z = standardize(X, dm, ds)
        for i in range(2, len(FEATURE_NAMES)):
            assert Z[:, i].mean() == pytest.approx(0.0, abs=1e-9)
            assert Z[:, i].std() == pytest.approx(1.0, abs=1e-9)

    def test_z_uses_provided_means_and_stds(self):
        X = self._raw()
        dm, ds = compute_diff_stats(X)
        Z = standardize(X, dm, ds)
        j = 3  # an arbitrary standardized diff feature
        name = FEATURE_NAMES[j]
        expected = (X[:, j] - dm[name]) / ds[name]
        np.testing.assert_allclose(Z[:, j], expected)

    def test_single_row_matches_matrix_and_input_untouched(self):
        X = self._raw()
        dm, ds = compute_diff_stats(X)
        Z = standardize(X, dm, ds)
        before = X[0].copy()
        z0 = standardize(X[0], dm, ds)
        np.testing.assert_allclose(z0, Z[0])
        np.testing.assert_allclose(X[0], before)  # not mutated


# =============================================================================
# 6. Frozen-model selection -- backfill S-1, upcoming MAX.
# =============================================================================


class TestSelectTrainThrough:
    def test_backfill_uses_prior_season(self):
        assert select_train_through("backfill", score_season=2019) == 2018
        assert select_train_through("backfill", score_season=2025) == 2024

    def test_upcoming_uses_latest_fit(self):
        assert select_train_through("upcoming", available_train_through=[2017, 2024, 2019]) == 2024

    def test_upcoming_without_fits_raises(self):
        with pytest.raises(ValueError):
            select_train_through("upcoming", available_train_through=[])

    def test_unknown_mode_raises(self):
        with pytest.raises(ValueError):
            select_train_through("bogus")


# =============================================================================
# 7. End-to-end mini walk-forward on synthetic data.
# =============================================================================


def _make_game(rng, strength, neutral=False):
    n_teams = len(strength)
    home_i, away_i = rng.choice(n_teams, size=2, replace=False)
    s_home = float(strength[home_i])
    s_away = float(strength[away_i])

    def _tw(s):
        # Every source column tracks team strength (+ small noise); collinear on
        # purpose -- ridge must still produce a well-defined margin prediction.
        return {c: float(s + rng.normal(0.0, 0.05)) for c in TEAM_WEEK_SOURCE_COLUMNS}

    margin = 10.0 * (s_home - s_away) + (0.0 if neutral else 2.5) + rng.normal(0.0, 3.0)
    return {
        "neutral_site": neutral,
        "home_points": 60.0 + margin,
        "away_points": 60.0,
        "home_tw": _tw(s_home),
        "away_tw": _tw(s_away),
        "true_signal": s_home - s_away,
    }


class TestEndToEndWalkForward:
    def test_train_three_seasons_score_fourth(self):
        rng = np.random.default_rng(7)
        strength = rng.normal(0.0, 1.0, 20)

        def _season(n_games):
            return [_make_game(rng, strength) for _ in range(n_games)]

        train_games = _season(120) + _season(120) + _season(120)
        test_games = _season(120)

        # Train (frozen stats) on the first three synthetic seasons.
        feature_means = compute_feature_means(collect_team_week_rows(train_games))
        X_raw, y_margin, y_win = build_design(train_games, feature_means)
        diff_means, diff_stds = compute_diff_stats(X_raw)
        X_std = standardize(X_raw, diff_means, diff_stds)

        mask = penalty_mask()
        beta_margin = ridge_fit(X_std, y_margin, RIDGE_ALPHA, mask)
        beta_winprob = irls_logistic(X_std, y_win, WINPROB_ALPHA, mask)
        platt_a, platt_b = platt_fit(X_std @ beta_winprob, y_win)

        # Score the held-out fourth season with the FROZEN train stats.
        X4_raw, y4_margin, _y4_win = build_design(test_games, feature_means)
        X4_std = standardize(X4_raw, diff_means, diff_stds)
        pred_margin = X4_std @ beta_margin
        true_signal = np.array([g["true_signal"] for g in test_games])

        r_pred_actual = float(np.corrcoef(pred_margin, y4_margin)[0, 1])
        r_pred_signal = float(np.corrcoef(pred_margin, true_signal)[0, 1])
        assert r_pred_actual > 0.5
        assert r_pred_signal > 0.7

        # Win-prob path runs clean and yields valid probabilities out of sample.
        logits4 = X4_std @ beta_winprob
        probs = np.array([platt_transform(float(z), platt_a, platt_b) for z in logits4])
        assert np.all(probs > 0.0)
        assert np.all(probs < 1.0)
