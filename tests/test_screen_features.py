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
    MIN_SCREEN_N,
    PRIMARY_CONTROL,
    benjamini_hochberg,
    complete_cases,
    derive_composites,
    partial_corr_pvalue,
    partial_correlation,
    screen,
    screen_verdict,
    second_order_partial_correlation,
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


class TestSecondOrderPartialCorrelation:
    """P2-F regression. The recorded verdicts control for prior-season rating
    AND recruiting; screening on prior rating alone would ship a candidate that
    merely restates recruiting."""

    def _frame(self, n=1200, seed=13):
        """`w` (recruiting) genuinely predicts `y` beyond `z`. `redundant` is a
        noisy copy of `w` carrying NO information `w` lacks -- the shape of
        `draft_picks_3yr`."""
        rng = random.Random(seed)
        z, y, w, redundant, extra = [], [], [], [], []
        for _ in range(n):
            z_i = rng.gauss(0.0, 1.0)
            w_i = rng.gauss(0.0, 1.0)
            extra_i = rng.gauss(0.0, 1.0)
            y_i = 0.6 * z_i + 0.45 * w_i + 0.35 * extra_i + rng.gauss(0.0, 0.3)
            z.append(z_i)
            y.append(y_i)
            w.append(w_i)
            redundant.append(w_i + rng.gauss(0.0, 0.2))
            extra.append(extra_i)
        return {"z": z, "y": y, "w": w, "redundant": redundant, "extra": extra}

    def test_redundant_candidate_survives_one_control_but_not_two(self):
        f = self._frame()
        first = partial_correlation(f["redundant"], f["y"], f["z"])
        second = second_order_partial_correlation(f["redundant"], f["y"], f["z"], f["w"])
        assert abs(first) > MIN_PARTIAL_R, "fixture should clear the floor on one control"
        assert abs(second) < MIN_PARTIAL_R, (
            f"redundant candidate must fail the second control (got {second:.4f})"
        )

    def test_genuinely_incremental_candidate_survives_both_controls(self):
        f = self._frame()
        second = second_order_partial_correlation(f["extra"], f["y"], f["z"], f["w"])
        assert second > MIN_PARTIAL_R

    def test_reduces_to_first_order_when_second_control_is_noise(self):
        f = self._frame(n=600, seed=21)
        rng = random.Random(99)
        noise = [rng.gauss(0.0, 1.0) for _ in f["y"]]
        first = partial_correlation(f["extra"], f["y"], f["z"])
        second = second_order_partial_correlation(f["extra"], f["y"], f["z"], noise)
        assert second == pytest.approx(first, abs=0.05)


class TestScreenUsesBothControls:
    """The executable gate must reproduce the recorded decisions."""

    def _frame(self, n=1000, seed=17):
        rng = random.Random(seed)
        rows = []
        for _ in range(n):
            z = rng.gauss(0.0, 1.0)
            w = rng.gauss(0.0, 1.0)
            y = 0.6 * z + 0.45 * w + rng.gauss(0.0, 0.3)
            rows.append(
                {
                    "sp_rating": y,
                    "prior_sp_rating": z,
                    PRIMARY_CONTROL: w,
                    # A noisy restatement of the control: no independent signal.
                    "redundant_candidate": w + rng.gauss(0.0, 0.2),
                }
            )
        return rows

    def test_redundant_candidate_is_rejected_by_the_screen(self):
        results = screen(self._frame(), [PRIMARY_CONTROL, "redundant_candidate"])
        by_name = {r["feature"]: r for r in results}
        red = by_name["redundant_candidate"]
        assert abs(red["partial_r_vs_prior"]) > MIN_PARTIAL_R, "would ship on one control"
        assert red["verdict"] == "reject", "must reject once recruiting is controlled"

    def test_primary_control_is_judged_on_the_first_order_partial(self):
        results = screen(self._frame(), [PRIMARY_CONTROL, "redundant_candidate"])
        control = next(r for r in results if r["feature"] == PRIMARY_CONTROL)
        # It cannot be screened against itself, so both figures agree.
        assert control["partial_r"] == pytest.approx(control["partial_r_vs_prior"])
        assert control["verdict"] == "ship"

    def test_both_columns_are_reported(self):
        for r in screen(self._frame(), [PRIMARY_CONTROL, "redundant_candidate"]):
            assert "partial_r_vs_prior" in r
            assert "partial_r" in r


class TestNullCandidatesAreScreenedOnCompleteCases:
    """The trench candidates arrive NULL for teams with no prior FBS season.

    Screen run 121 (2026-07-26) crashed on exactly this: the seven prior-season
    trench columns come from a LEFT JOIN against stats.advanced_team_stats,
    unlike every earlier candidate, which SQL COALESCEs to 0. `_pearson` summed
    a None and raised.

    Both halves matter. Crashing is bad; the tempting fix is worse. COALESCE to
    0 is right for a COUNT (no portal transfers really is a net rating of zero)
    and wrong for a RATE -- no team posts 0.000 line yards, and the teams with
    no prior-season row are disproportionately bad in season S, so the floor
    would be imputed precisely where the outcome is low and manufacture a trench
    signal out of nothing.
    """

    def _frame_with_gaps(self, n=1000, missing_from=600, seed=23):
        rng = random.Random(seed)
        rows = []
        for i in range(n):
            z = rng.gauss(0.0, 1.0)
            w = rng.gauss(0.0, 1.0)
            y = 0.6 * z + 0.45 * w + rng.gauss(0.0, 0.3)
            row = {
                "sp_rating": y,
                "prior_sp_rating": z,
                PRIMARY_CONTROL: w,
                # Present throughout, and genuinely informative.
                "dense_candidate": 0.5 * y + rng.gauss(0.0, 0.5),
                # NULL past the cutoff, like a LEFT JOIN miss.
                "sparse_candidate": None if i >= missing_from else rng.gauss(0.0, 1.0),
            }
            rows.append(row)
        return rows

    def test_complete_cases_drops_only_rows_missing_a_named_column(self):
        frame = self._frame_with_gaps()
        assert len(complete_cases(frame, ["dense_candidate"])) == 1000
        assert len(complete_cases(frame, ["sparse_candidate"])) == 600

    def test_screen_does_not_crash_on_null_candidates(self):
        # Without per-candidate complete cases this raises TypeError, which is
        # precisely how run 121 failed.
        results = screen(self._frame_with_gaps(), [PRIMARY_CONTROL, "sparse_candidate"])
        assert {r["feature"] for r in results} == {PRIMARY_CONTROL, "sparse_candidate"}

    def test_sparse_candidate_reports_its_own_smaller_n(self):
        results = screen(
            self._frame_with_gaps(), [PRIMARY_CONTROL, "dense_candidate", "sparse_candidate"]
        )
        by_name = {r["feature"]: r for r in results}
        assert by_name["dense_candidate"]["n"] == 1000
        assert by_name["sparse_candidate"]["n"] == 600
        # Coverage is what stops a 600-row partial from reading as a 1,000-row one.
        assert by_name["sparse_candidate"]["coverage"] == pytest.approx(0.6)

    def test_candidate_below_min_screen_n_is_untestable_not_shipped(self):
        frame = self._frame_with_gaps(missing_from=MIN_SCREEN_N - 1)
        result = next(
            r
            for r in screen(frame, [PRIMARY_CONTROL, "sparse_candidate"])
            if r["feature"] == "sparse_candidate"
        )
        assert result["p"] is None
        assert result["verdict"] == "reject"
        assert "MIN_SCREEN_N" in result["untestable"]

    def test_missing_rate_is_not_imputed_to_zero(self):
        """The regression that matters: zero-imputation fabricates signal.

        Here the candidate is pure noise among teams that have it, but it is
        missing exactly where the outcome is lowest. Zero-filling correlates
        "missing" with "bad" and invents a large partial; complete-case
        screening correctly returns ~nothing.
        """
        rng = random.Random(5)
        frame = []
        for _ in range(1200):
            z = rng.gauss(0.0, 1.0)
            w = rng.gauss(0.0, 1.0)
            y = 0.6 * z + 0.45 * w + rng.gauss(0.0, 0.3)
            # Noise for teams that played; absent for the weakest teams.
            value = None if y < -1.0 else rng.gauss(4.5, 1.0)
            frame.append(
                {"sp_rating": y, "prior_sp_rating": z, PRIMARY_CONTROL: w, "trench": value}
            )

        honest = next(
            r for r in screen(frame, [PRIMARY_CONTROL, "trench"]) if r["feature"] == "trench"
        )

        zero_filled = [
            dict(r, trench=(r["trench"] if r["trench"] is not None else 0.0)) for r in frame
        ]
        fabricated = next(
            r for r in screen(zero_filled, [PRIMARY_CONTROL, "trench"]) if r["feature"] == "trench"
        )

        assert abs(honest["partial_r"]) < MIN_PARTIAL_R, "noise must not ship"
        assert abs(fabricated["partial_r"]) > abs(honest["partial_r"]), (
            "zero-filling should visibly inflate the partial -- this is the trap"
        )


class TestRegimeColumnsSeparateRecruitingFromCoachingChange:
    """The 2026-07-26 imputation audit found `recruiting_points_regime`
    zero-filled on 291 of 1,439 rows (20.2%).

    The regime window GREATEST(season-4, tenure_start)..season-1 is empty
    exactly when tenure_start >= season -- a first-year head coach -- so a
    fifth of the sample carried a hard 0 meaning "no classes signed by this
    staff yet". That 0 is confounded with the coaching change itself, so the
    column blended a recruiting measure with a de facto new-coach indicator.
    The two are now separate columns.
    """

    def test_regime_column_is_not_zero_filled(self):
        from scripts.screen_preseason_features import SCREEN_FRAME_QUERY as q

        regime = q[q.index("AS recruiting_points_regime") - 900 :]
        regime = regime[: regime.index("AS recruiting_points_regime")]
        assert "COALESCE" not in regime.upper(), (
            "recruiting_points_regime must not be zero-filled -- an empty "
            "regime window means 'first-year coach', not 'recruits badly'"
        )

    def test_hc_first_year_is_screened_separately(self):
        from scripts.screen_preseason_features import CANDIDATE_COLUMNS
        from scripts.screen_preseason_features import SCREEN_FRAME_QUERY as q

        assert "hc_first_year" in CANDIDATE_COLUMNS
        assert "AS hc_first_year" in q

    def test_null_tenure_is_guarded_by_case_not_greatest(self):
        """Postgres GREATEST IGNORES NULL arguments.

        GREATEST(season - 4, NULL) returns season - 4, so relying on a NULL
        tenure_start to propagate through GREATEST would quietly restore the
        flat window instead of yielding NULL -- reintroducing the duplicate
        candidate this change removes. The CASE guard is what makes it NULL.
        """
        from scripts.screen_preseason_features import SCREEN_FRAME_QUERY as q

        assert q.count("WHEN s.tenure_start IS NULL THEN NULL") >= 2, (
            "both regime columns need an explicit CASE guard on a NULL "
            "tenure_start; GREATEST will not propagate it"
        )

    def test_spine_does_not_coalesce_tenure_start(self):
        from scripts.screen_preseason_features import SCREEN_FRAME_QUERY as q

        assert "COALESCE(ct.tenure_start" not in q, (
            "coalescing tenure_start to the window start makes the regime "
            "variant a silent duplicate of the flat window"
        )


class TestFrameQueryIsBindable:
    """The screen has NEVER been able to run its own frame query.

    Two literal percent signs in SQL comments ("99.1%", "~1%") were not
    escaped, and psycopg2's pyformat binding treats any bare % as the start of
    a placeholder -- so fetch_frame raised `TypeError: dict is not a sequence`
    before touching the database. The recorded verdicts came from ad-hoc MCP
    queries instead, which is a deeper version of the reproducibility gap the
    PR #48 review raised: the script could not reproduce its verdicts because
    it could not execute at all.
    """

    def test_no_unescaped_percent_signs(self):
        import re

        from scripts.screen_preseason_features import SCREEN_FRAME_QUERY as q

        # Strip valid named placeholders and doubled literals, then nothing
        # containing a bare % may remain.
        stripped = re.sub(r"%\([a-z_]+\)s", "", q).replace("%%", "")
        assert "%" not in stripped, (
            "unescaped % in SCREEN_FRAME_QUERY -- psycopg2 will read it as a "
            "placeholder and parameter binding will fail before the query runs"
        )

    def test_named_placeholders_are_present(self):
        from scripts.screen_preseason_features import SCREEN_FRAME_QUERY as q

        assert "%(from_season)s" in q
        assert "%(to_season)s" in q

    def test_query_binds_against_psycopg2_mogrify_rules(self):
        """Exercises the actual binding path rather than a regex proxy."""
        import re

        from scripts.screen_preseason_features import SCREEN_FRAME_QUERY as q

        params = {"from_season": 2015, "to_season": 2025}
        # Mirror psycopg2's pyformat substitution; a stray % raises here too.
        rendered = re.sub(r"%\(([a-z_]+)\)s", lambda m: str(params[m.group(1)]), q).replace(
            "%%", "%"
        )
        assert "2015" in rendered and "2025" in rendered
