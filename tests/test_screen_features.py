"""Unit tests for the preseason-feature screen math (no DB, no I/O).

Covers scripts/screen_preseason_features.py's pure core: partial correlation,
the normal-approximation p-value, Benjamini-Hochberg FDR adjustment, the
ship/reject rule, and the Tier A composite derivation. Everything here runs on
plain Python lists and stdlib `random`; nothing touches Postgres or numpy.

The load-bearing test is TestPartialCorrelation.test_pure_proxy_of_control_
scores_near_zero: a candidate that merely restates prior-season quality must
screen out even though its RAW correlation with the outcome is large. That
discrimination is the entire reason the screen replaced raw-correlation
screening (plan section 2.5).
"""

import math
import random

import pytest

from scripts.screen_preseason_features import (
    FDR_ALPHA,
    MIN_PARTIAL_R,
    benjamini_hochberg,
    derive_composites,
    partial_corr_pvalue,
    partial_correlation,
    screen_verdict,
)


def _correlated_frame(n=1400, seed=7):
    """Synthetic team-seasons.

    `z` is prior-season rating. `y` is season-S rating: mostly persistence of
    `z` plus a genuine `signal` component plus noise. `proxy` is a noisy
    restatement of `z` with NO independent information about `y`; `signal` is
    the honest predictor. A correct screen ranks signal high and proxy ~0.
    """
    rng = random.Random(seed)
    z, y, proxy, signal = [], [], [], []
    for _ in range(n):
        z_i = rng.gauss(0.0, 1.0)
        signal_i = rng.gauss(0.0, 1.0)
        y_i = 0.75 * z_i + 0.40 * signal_i + rng.gauss(0.0, 0.35)
        z.append(z_i)
        y.append(y_i)
        proxy.append(z_i + rng.gauss(0.0, 0.15))
        signal.append(signal_i)
    return {"z": z, "y": y, "proxy": proxy, "signal": signal}


class TestPartialCorrelation:
    def test_pure_proxy_of_control_scores_near_zero(self):
        """A feature that only re-expresses the control adds nothing.

        This is the failure mode raw-correlation screening could not detect:
        `proxy` correlates strongly with the outcome, but every bit of that is
        already carried by prior-season rating.
        """
        f = _correlated_frame()
        raw = abs(_raw_corr(f["proxy"], f["y"]))
        partial = partial_correlation(f["proxy"], f["y"], f["z"])

        assert raw > 0.5, "fixture should give the proxy a large raw correlation"
        assert abs(partial) < 0.1, (
            f"proxy should screen out (partial={partial:.4f}) despite raw={raw:.4f}"
        )

    def test_genuine_signal_survives_the_control(self):
        f = _correlated_frame()
        partial = partial_correlation(f["signal"], f["y"], f["z"])
        assert partial > 0.5

    def test_matches_residual_correlation_definition(self):
        """Partial correlation == correlation of the two residuals after
        regressing each on the control. The formula is a shortcut for that."""
        f = _correlated_frame(n=400, seed=11)
        via_formula = partial_correlation(f["signal"], f["y"], f["z"])
        via_residuals = _raw_corr(_residualize(f["signal"], f["z"]), _residualize(f["y"], f["z"]))
        assert via_formula == pytest.approx(via_residuals, abs=1e-9)

    def test_constant_feature_has_no_signal(self):
        f = _correlated_frame(n=300, seed=3)
        constant = [1.0] * 300
        assert partial_correlation(constant, f["y"], f["z"]) == pytest.approx(0.0)

    def test_unequal_lengths_raise(self):
        with pytest.raises(ValueError, match="equal-length"):
            partial_correlation([1.0, 2.0], [1.0, 2.0, 3.0], [1.0, 2.0, 3.0])

    def test_too_few_observations_raise(self):
        with pytest.raises(ValueError, match="at least 3"):
            partial_correlation([1.0], [1.0], [1.0])


class TestPartialCorrPvalue:
    def test_small_sample_returns_none_rather_than_a_bad_number(self):
        # Below the df floor the normal approximation is unsafe, so the honest
        # answer is "cannot test", not a plausible-looking p-value.
        assert partial_corr_pvalue(0.5, n=50) is None

    def test_zero_correlation_gives_p_near_one(self):
        assert partial_corr_pvalue(0.0, n=1400) == pytest.approx(1.0, abs=1e-9)

    def test_strong_correlation_gives_tiny_p(self):
        p = partial_corr_pvalue(0.5, n=1400)
        assert p is not None
        assert p < 1e-10

    def test_threshold_effect_is_significant_at_screen_sample_size(self):
        # An effect exactly at the pre-registered floor should be detectable
        # with ~1,400 team-seasons -- otherwise the floor and the sample are
        # mismatched and the screen could never ship anything.
        p = partial_corr_pvalue(MIN_PARTIAL_R, n=1400)
        assert p is not None
        assert p < 0.01

    def test_two_sided_is_sign_symmetric(self):
        assert partial_corr_pvalue(0.2, n=1400) == pytest.approx(partial_corr_pvalue(-0.2, n=1400))


class TestBenjaminiHochberg:
    def test_empty_input(self):
        assert benjamini_hochberg([]) == []

    def test_preserves_input_order(self):
        # Largest p is last in input; its q must also be the largest.
        q = benjamini_hochberg([0.01, 0.001, 0.5])
        assert q[2] == max(q)
        assert q[1] == min(q)

    def test_monotone_in_p(self):
        pvalues = [0.001, 0.008, 0.02, 0.04, 0.3, 0.7]
        q = benjamini_hochberg(pvalues)
        # Deliberately not strict: this is a pairwise (offset) zip.
        assert all(a <= b for a, b in zip(q, q[1:]))  # noqa: B905

    def test_q_never_below_p(self):
        pvalues = [0.001, 0.01, 0.02, 0.2]
        for p, q in zip(pvalues, benjamini_hochberg(pvalues), strict=True):
            assert q >= p - 1e-12

    def test_capped_at_one(self):
        assert all(q <= 1.0 for q in benjamini_hochberg([0.9, 0.95, 0.99]))

    def test_single_value_is_unchanged(self):
        assert benjamini_hochberg([0.03]) == pytest.approx([0.03])

    def test_penalizes_a_lone_marginal_result_among_many_nulls(self):
        # One p just under 0.05 among 19 nulls is what an uncorrected screen
        # would ship; FDR should pull it well above alpha.
        pvalues = [0.045] + [0.6] * 19
        assert benjamini_hochberg(pvalues)[0] > FDR_ALPHA


class TestScreenVerdict:
    def test_ships_when_both_conditions_hold(self):
        assert screen_verdict(0.15, 0.01) == "ship"

    def test_rejects_small_effect_even_when_significant(self):
        # Large n makes tiny effects significant; the effect floor is what
        # stops the screen shipping statistically-real irrelevance.
        assert screen_verdict(0.02, 1e-12) == "reject"

    def test_rejects_large_effect_that_fails_fdr(self):
        assert screen_verdict(0.30, 0.5) == "reject"

    def test_rejects_untestable_candidate(self):
        # q is None when df was too small -- unverifiable is not valuable.
        assert screen_verdict(0.30, None) == "reject"

    def test_negative_effects_are_judged_on_magnitude(self):
        assert screen_verdict(-0.15, 0.01) == "ship"

    def test_exactly_at_both_thresholds_ships(self):
        assert screen_verdict(MIN_PARTIAL_R, FDR_ALPHA) == "ship"


class TestDeriveComposites:
    def _row(self, **overrides):
        row = {
            "recruiting_points_3yr": 500.0,
            "portal_net_rating": 20.0,
            "draft_departures": 5.0,
            "draft_picks_3yr": 10.0,
        }
        row.update(overrides)
        return row

    def test_talent_stock_nets_arrivals_against_departures(self):
        out = derive_composites(self._row())
        assert out["talent_stock"] == pytest.approx(500.0 + 20.0 - 5.0)

    def test_conversion_is_a_rate_not_a_count(self):
        out = derive_composites(self._row())
        assert out["conversion"] == pytest.approx(10.0 / 500.0)

    def test_pipeline_index_is_the_product(self):
        out = derive_composites(self._row())
        assert out["pipeline_index"] == pytest.approx(out["talent_stock"] * out["conversion"])

    def test_no_recruiting_inputs_yields_zero_conversion_not_a_crash(self):
        out = derive_composites(self._row(recruiting_points_3yr=0.0))
        assert out["conversion"] == 0.0
        assert out["pipeline_index"] == 0.0

    def test_draft_yield_tracks_conversion(self):
        out = derive_composites(self._row())
        assert out["draft_yield"] == pytest.approx(out["conversion"])

    def test_two_programs_same_stock_different_development(self):
        """The distinction the composite exists to make: equal raw material,
        different ability to turn it into NFL players."""
        developer = derive_composites(self._row(draft_picks_3yr=15.0))
        squanderer = derive_composites(self._row(draft_picks_3yr=2.0))
        assert developer["talent_stock"] == squanderer["talent_stock"]
        assert developer["pipeline_index"] > squanderer["pipeline_index"]


# --- helpers -----------------------------------------------------------------


def _raw_corr(a, b):
    n = len(a)
    ma, mb = sum(a) / n, sum(b) / n
    da = [v - ma for v in a]
    db = [v - mb for v in b]
    den = math.sqrt(sum(p * p for p in da) * sum(q * q for q in db))
    return sum(p * q for p, q in zip(da, db, strict=True)) / den


def _residualize(v, control):
    """Residuals of v after an OLS fit on control (with intercept)."""
    n = len(v)
    mc = sum(control) / n
    mv = sum(v) / n
    dc = [c - mc for c in control]
    slope = sum(c * (x - mv) for c, x in zip(dc, v, strict=True)) / sum(c * c for c in dc)
    return [x - (mv + slope * c) for x, c in zip(v, dc, strict=True)]
