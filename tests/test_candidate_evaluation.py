"""Unit tests for the U4 isolated walk-forward candidate-evaluation harness
(starter-pack plan, docs/plans/2026-07-28-001-feat-starter-pack-model-features-plan.md).

Everything here runs on numpy arrays / plain dicts; nothing touches Postgres.
Covers: the parametrized feature-name/vectorization helpers still reproduce
production behavior when called with their defaults, a candidate fit/score
round-trip on synthetic data, the MAE/Brier/ATS aggregation, and the R5 gate
logic (KTD5).
"""

import inspect

import pytest

pytest.importorskip("numpy")

import numpy as np  # noqa: E402

from scripts.train_model import (  # noqa: E402
    CANDIDATE_DIFF_FEATURE_COLUMNS,
    DEFAULT_SCORE_END,
    DEFAULT_SCORE_START,
    DIFF_FEATURE_COLUMNS,
    FEATURE_NAMES,
    OFF_PPD_FEATURE,
    RIDGE_ALPHA,
    TEAM_WEEK_SOURCE_COLUMNS,
    WINPROB_ALPHA,
    aggregate_candidate_metrics,
    build_design,
    build_feature_vector,
    collect_team_week_rows,
    compute_diff_stats,
    compute_feature_means,
    evaluate_gate,
    feature_names_for,
    irls_logistic,
    penalty_mask,
    platt_fit,
    ridge_fit,
    standardize,
)

# =============================================================================
# 1. feature_names_for / parametrized helpers -- defaults reproduce production.
# =============================================================================


class TestFeatureNamesFor:
    def test_matches_production_feature_names_by_default(self):
        assert feature_names_for(DIFF_FEATURE_COLUMNS) == FEATURE_NAMES

    def test_candidate_list_appends_one_name(self):
        names = feature_names_for(CANDIDATE_DIFF_FEATURE_COLUMNS)
        assert names == FEATURE_NAMES + ["d_off_ppd"]

    def test_off_ppd_is_the_sole_candidate(self):
        assert OFF_PPD_FEATURE == ("d_off_ppd", "off_ppd")
        assert CANDIDATE_DIFF_FEATURE_COLUMNS == [*DIFF_FEATURE_COLUMNS, OFF_PPD_FEATURE]

    def test_off_ppd_is_not_in_the_shipped_vector(self):
        """U4 evaluates the candidate in memory; it must not already be in the
        production list, or the evaluation would compare a model against
        itself."""
        assert "off_ppd" not in TEAM_WEEK_SOURCE_COLUMNS
        assert "d_off_ppd" not in FEATURE_NAMES


class TestParametrizedHelpersDefaultToProduction:
    def test_penalty_mask_default_matches_explicit_feature_names(self):
        np.testing.assert_array_equal(penalty_mask(), penalty_mask(FEATURE_NAMES))

    def test_build_feature_vector_default_matches_explicit_diff_columns(self):
        means = {c: 0.0 for c in TEAM_WEEK_SOURCE_COLUMNS}
        tw = {c: 1.0 for c in TEAM_WEEK_SOURCE_COLUMNS}
        game = {"neutral_site": False}
        default = build_feature_vector(game, tw, tw, means)
        explicit = build_feature_vector(game, tw, tw, means, DIFF_FEATURE_COLUMNS)
        np.testing.assert_array_equal(default, explicit)

    def test_candidate_vector_has_one_more_column(self):
        means = {c: 0.0 for c in TEAM_WEEK_SOURCE_COLUMNS}
        means["off_ppd"] = 2.0
        tw = {c: 1.0 for c in TEAM_WEEK_SOURCE_COLUMNS}
        tw["off_ppd"] = 3.0
        game = {"neutral_site": False}
        vector = build_feature_vector(game, tw, tw, means, CANDIDATE_DIFF_FEATURE_COLUMNS)
        assert len(vector) == len(FEATURE_NAMES) + 1
        # home == away on off_ppd, so the new diff is exactly 0.
        assert vector[-1] == pytest.approx(0.0)


# =============================================================================
# 2. In-memory candidate fit + score round-trip (synthetic data, no DB).
# =============================================================================


def _make_game(rng, strength, off_ppd_signal):
    n_teams = len(strength)
    home_i, away_i = rng.choice(n_teams, size=2, replace=False)
    s_home, s_away = float(strength[home_i]), float(strength[away_i])

    def _tw(s, ppd_signal):
        row = {c: float(s + rng.normal(0.0, 0.05)) for c in TEAM_WEEK_SOURCE_COLUMNS}
        row["off_ppd"] = float(ppd_signal + rng.normal(0.0, 0.05))
        return row

    margin = 10.0 * (s_home - s_away) + rng.normal(0.0, 3.0)
    return {
        "game_id": int(rng.integers(1, 1_000_000)),
        "neutral_site": False,
        "home_points": 60.0 + margin,
        "away_points": 60.0,
        "home_tw": _tw(s_home, off_ppd_signal[home_i]),
        "away_tw": _tw(s_away, off_ppd_signal[away_i]),
    }


class _FakeConn:
    """Stands in for a psycopg2 connection: fit_candidate/evaluate_candidate_diff_columns
    only ever call fetch_games(conn, seasons, columns), so a fake conn that
    hands back pre-built synthetic games by season is enough."""

    def __init__(self, games_by_season):
        self.games_by_season = games_by_season


def _fake_fetch_games(conn, seasons, source_columns=None):
    return [g for s in seasons for g in conn.games_by_season.get(s, [])]


class TestFitCandidateRoundTrip:
    def test_candidate_fit_scores_held_out_games_reasonably(self, monkeypatch):
        import scripts.train_model as train_model

        monkeypatch.setattr(train_model, "fetch_games", _fake_fetch_games)

        rng = np.random.default_rng(11)
        strength = rng.normal(0.0, 1.0, 20)
        off_ppd_signal = strength * 2.0  # collinear with strength on purpose

        def _season(n_games):
            return [_make_game(rng, strength, off_ppd_signal) for _ in range(n_games)]

        conn = _FakeConn({2015: _season(120), 2016: _season(120), 2017: _season(120)})
        fit = train_model.fit_candidate(
            conn, 2017, [2015, 2016, 2017], CANDIDATE_DIFF_FEATURE_COLUMNS
        )
        assert fit is not None
        assert fit["train_through"] == 2017
        assert fit["n_train"] == 360
        assert set(fit["feature_means"]) == {col for _, col in CANDIDATE_DIFF_FEATURE_COLUMNS}

        test_games = _season(60)
        preds = [
            train_model.score_candidate_game(g, fit, CANDIDATE_DIFF_FEATURE_COLUMNS)
            for g in test_games
        ]
        pred_margins = np.array([p[0] for p in preds])
        actual_margins = np.array([g["home_points"] - g["away_points"] for g in test_games])
        assert float(np.corrcoef(pred_margins, actual_margins)[0, 1]) > 0.5
        win_probs = np.array([p[1] for p in preds])
        assert np.all(win_probs > 0.0) and np.all(win_probs < 1.0)

    def test_no_train_games_returns_none(self, monkeypatch):
        import scripts.train_model as train_model

        monkeypatch.setattr(train_model, "fetch_games", _fake_fetch_games)
        conn = _FakeConn({})
        fit = train_model.fit_candidate(
            conn, 2017, [2015, 2016, 2017], CANDIDATE_DIFF_FEATURE_COLUMNS
        )
        assert fit is None

    def test_candidate_fit_reproduces_production_math_when_columns_match(self, monkeypatch):
        """fit_candidate must be the SAME math as fit_one's steps, just
        parametrized -- verified by reproducing it inline with the production
        DIFF_FEATURE_COLUMNS and checking beta_margin matches exactly."""
        rng = np.random.default_rng(13)
        strength = rng.normal(0.0, 1.0, 16)
        off_ppd_signal = np.zeros_like(strength)

        def _season(n_games):
            return [_make_game(rng, strength, off_ppd_signal) for _ in range(n_games)]

        games = _season(100) + _season(100) + _season(100)

        feature_means = compute_feature_means(collect_team_week_rows(games))
        X_raw, y_margin, y_win = build_design(games, feature_means)
        diff_means, diff_stds = compute_diff_stats(X_raw)
        X_std = standardize(X_raw, diff_means, diff_stds)
        mask = penalty_mask()
        beta_margin_direct = ridge_fit(X_std, y_margin, RIDGE_ALPHA, mask)
        beta_winprob_direct = irls_logistic(X_std, y_win, WINPROB_ALPHA, mask)
        platt_direct = platt_fit(X_std @ beta_winprob_direct, y_win)

        import scripts.train_model as train_model

        monkeypatch.setattr(train_model, "fetch_games", _fake_fetch_games)
        conn = _FakeConn({2015: games[:100], 2016: games[100:200], 2017: games[200:]})
        fit = train_model.fit_candidate(conn, 2017, [2015, 2016, 2017], DIFF_FEATURE_COLUMNS)

        np.testing.assert_allclose(fit["beta_margin"], beta_margin_direct)
        np.testing.assert_allclose(fit["beta_winprob"], beta_winprob_direct)
        assert fit["platt_a"] == pytest.approx(platt_direct[0])
        assert fit["platt_b"] == pytest.approx(platt_direct[1])


# =============================================================================
# 3. aggregate_candidate_metrics -- MAE / Brier / ATS over scored games.
# =============================================================================


class TestAggregateCandidateMetrics:
    def test_empty_input_is_all_none(self):
        result = aggregate_candidate_metrics([], {})
        assert result["n_games"] == 0
        assert result["margin_mae"] is None
        assert result["brier"] is None
        assert result["ats_hit_rate"] is None

    def test_margin_mae_and_brier_ignore_market(self):
        scored = [
            {
                "game_id": 1,
                "expected_margin": 10.0,
                "actual_margin": 7.0,
                "win_prob": 0.8,
                "actual_win": 1.0,
            },
            {
                "game_id": 2,
                "expected_margin": -3.0,
                "actual_margin": -3.0,
                "win_prob": 0.4,
                "actual_win": 0.0,
            },
        ]
        result = aggregate_candidate_metrics(scored, {})
        assert result["margin_mae"] == pytest.approx((3.0 + 0.0) / 2)
        assert result["brier"] == pytest.approx(((0.8 - 1.0) ** 2 + (0.4 - 0.0) ** 2) / 2)
        assert result["ats_hit_rate"] is None  # no market at all

    def test_ats_hit_rate_excludes_games_without_a_market_spread(self):
        scored = [
            {
                "game_id": 1,
                "expected_margin": 10.0,
                "actual_margin": 10.0,
                "win_prob": 0.5,
                "actual_win": 1.0,
            },
            {
                "game_id": 2,
                "expected_margin": 5.0,
                "actual_margin": 5.0,
                "win_prob": 0.5,
                "actual_win": 1.0,
            },
        ]
        # Only game_id 1 has a market line.
        market = {1: {"spread": -3.0}}
        result = aggregate_candidate_metrics(scored, market)
        assert result["ats_wins"] + result["ats_losses"] + result["ats_pushes"] == 1

    def test_ats_win_loss_push_math(self):
        # market_spread = -7 means home favored by 7 (market_home_margin = 7).
        # edge = expected_margin + spread; pick home iff edge >= 0.
        scored = [
            # expected 10, spread -7 -> edge=3 -> pick home; actual_cover = actual+spread
            {
                "game_id": 1,
                "expected_margin": 10.0,
                "actual_margin": 10.0,
                "win_prob": 0.6,
                "actual_win": 1.0,
            },
            {
                "game_id": 2,
                "expected_margin": 10.0,
                "actual_margin": 3.0,
                "win_prob": 0.6,
                "actual_win": 1.0,
            },
            {
                "game_id": 3,
                "expected_margin": 10.0,
                "actual_margin": 7.0,
                "win_prob": 0.6,
                "actual_win": 1.0,
            },
        ]
        market = {1: {"spread": -7.0}, 2: {"spread": -7.0}, 3: {"spread": -7.0}}
        result = aggregate_candidate_metrics(scored, market)
        # game1: actual_cover = 10-7=3 > 0 -> pick home wins.
        # game2: actual_cover = 3-7=-4 < 0 -> pick home loses.
        # game3: actual_cover = 7-7=0 -> push.
        assert result["ats_wins"] == 1
        assert result["ats_losses"] == 1
        assert result["ats_pushes"] == 1
        assert result["ats_hit_rate"] == pytest.approx(0.5)


# =============================================================================
# 4. evaluate_gate -- the R5 adoption rule (KTD5).
# =============================================================================


class TestEvaluateGate:
    def _baseline(self):
        return {"margin_mae": 10.0, "brier": 0.20, "ats_hit_rate": 0.52}

    def test_mae_improves_brier_and_ats_flat_passes(self):
        candidate = {"margin_mae": 9.5, "brier": 0.20, "ats_hit_rate": 0.52}
        gate = evaluate_gate(candidate, self._baseline())
        assert gate["mae_improves"] is True
        assert gate["brier_holds"] is True
        assert gate["ats_holds"] is True
        assert gate["verdict"] == "PASS"

    def test_mae_improves_but_ats_degrades_fails(self):
        candidate = {"margin_mae": 9.5, "brier": 0.20, "ats_hit_rate": 0.50}
        gate = evaluate_gate(candidate, self._baseline())
        assert gate["mae_improves"] is True
        assert gate["ats_holds"] is False
        assert gate["verdict"] == "FAIL"

    def test_mae_flat_fails(self):
        candidate = {"margin_mae": 10.0, "brier": 0.19, "ats_hit_rate": 0.55}
        gate = evaluate_gate(candidate, self._baseline())
        assert gate["mae_improves"] is False
        assert gate["verdict"] == "FAIL"

    def test_mae_improves_but_brier_degrades_fails(self):
        candidate = {"margin_mae": 9.0, "brier": 0.21, "ats_hit_rate": 0.55}
        gate = evaluate_gate(candidate, self._baseline())
        assert gate["brier_holds"] is False
        assert gate["verdict"] == "FAIL"

    def test_deltas_are_signed_candidate_minus_baseline(self):
        candidate = {"margin_mae": 9.0, "brier": 0.18, "ats_hit_rate": 0.55}
        gate = evaluate_gate(candidate, self._baseline())
        assert gate["mae_delta"] == pytest.approx(-1.0)
        assert gate["brier_delta"] == pytest.approx(-0.02)
        assert gate["ats_delta"] == pytest.approx(0.03)


# =============================================================================
# 5. No-write guard (U4 execution note): the evaluation must never touch
# features.model_coefficients / features.model_metadata.
# =============================================================================


class TestNoWriteGuard:
    def test_fit_candidate_never_calls_persist_fit(self):
        import scripts.train_model as train_model

        src = inspect.getsource(train_model.fit_candidate)
        assert "persist_fit" not in src

    def test_evaluate_candidate_diff_columns_never_calls_persist_fit(self):
        import scripts.train_model as train_model

        src = inspect.getsource(train_model.evaluate_candidate_diff_columns)
        assert "persist_fit" not in src

    def test_run_candidate_evaluation_never_calls_persist_fit(self):
        import scripts.train_model as train_model

        src = inspect.getsource(train_model.run_candidate_evaluation)
        assert "persist_fit" not in src


class TestDefaultScoreRange:
    def test_matches_production_walk_forward_range(self):
        assert (DEFAULT_SCORE_START, DEFAULT_SCORE_END) == (2018, 2025)
