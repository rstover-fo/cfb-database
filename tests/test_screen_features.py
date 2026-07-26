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
    check_one_row_per_team_season,
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


def _case_expression(column):
    """The exact CASE expression SCREEN_FRAME_QUERY builds `column` from.

    Bounded by the aliasing `AS <column>` and the nearest preceding CASE, so a
    branch-order assertion cannot accidentally read a neighbouring column's
    arms -- which a fixed character window silently does, since these CASEs sit
    directly next to one another and share most of their text.
    """
    from scripts.screen_preseason_features import SCREEN_FRAME_QUERY as q

    end = q.index(f"AS {column}")
    start = q.rfind("CASE WHEN", 0, end)
    assert start != -1, f"{column} is not built from a CASE expression"
    return q[start:end]


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


class TestRecordedVerdictsAreInternallyConsistent:
    """The recorded ledger drifted from the screen once already.

    The docstring table said blue_chip_pipeline shipped at +0.0931 and
    recruiting_points_regime at +0.0955; the first executable run scored them
    +0.0782 and -0.0620. These assertions cannot verify the numbers without a
    database, but they can keep the bookkeeping honest -- every candidate
    accounted for exactly once, and human overrides kept out of the set that is
    supposed to mirror the screen's own output.
    """

    def test_every_candidate_has_exactly_one_recorded_verdict(self):
        from scripts.screen_preseason_features import (
            CANDIDATE_COLUMNS,
            PENDING_COLUMNS,
            REJECTED_COLUMNS,
            SHIPPED_BY_DECISION,
            SHIPPED_COLUMNS,
            SUPERSEDED_COLUMNS,
            UNTESTABLE_COLUMNS,
        )

        recorded = (
            list(SHIPPED_COLUMNS)
            + list(SHIPPED_BY_DECISION)
            + list(REJECTED_COLUMNS)
            + list(UNTESTABLE_COLUMNS)
            + list(SUPERSEDED_COLUMNS)
            + list(PENDING_COLUMNS)
        )
        assert len(recorded) == len(set(recorded)), "a candidate has two verdicts"
        assert set(recorded) == set(CANDIDATE_COLUMNS), (
            "every screened candidate needs a recorded verdict and vice versa"
        )

    def test_pending_candidates_carry_no_verdict(self):
        """PENDING is the "screened, unadjudicated" state, and it only means
        anything if it is exclusive.

        A candidate that appears both here and in a verdict bucket would let a
        guessed outcome sit in the ledger wearing a real verdict's clothes --
        which is how the pre-executable table came to disagree with the screen
        in the first place."""
        from scripts.screen_preseason_features import (
            PENDING_COLUMNS,
            REJECTED_COLUMNS,
            SHIPPED_BY_DECISION,
            SHIPPED_COLUMNS,
            SUPERSEDED_COLUMNS,
            UNTESTABLE_COLUMNS,
        )

        adjudicated = (
            set(SHIPPED_COLUMNS)
            | set(SHIPPED_BY_DECISION)
            | set(REJECTED_COLUMNS)
            | set(UNTESTABLE_COLUMNS)
            | set(SUPERSEDED_COLUMNS)
        )
        assert not adjudicated & set(PENDING_COLUMNS), (
            "a pending candidate has been given a verdict the screen did not produce"
        )

    def test_every_pending_candidate_says_what_it_is_for(self):
        from scripts.screen_preseason_features import PENDING_COLUMNS

        for column, rationale in PENDING_COLUMNS.items():
            assert len(rationale) > 40, f"{column} enters the candidate set without a reason"

    def test_overrides_are_not_laundered_into_the_shipped_set(self):
        from scripts.screen_preseason_features import SHIPPED_BY_DECISION, SHIPPED_COLUMNS

        assert not set(SHIPPED_COLUMNS) & set(SHIPPED_BY_DECISION), (
            "a column shipped against the screen's verdict must stay visible as "
            "an override, not merge into the set that mirrors the screen"
        )

    def test_every_override_records_its_argument(self):
        from scripts.screen_preseason_features import SHIPPED_BY_DECISION

        for column, rationale in SHIPPED_BY_DECISION.items():
            assert len(rationale) > 40, f"{column} overrides the gate without an argument"

    def test_a_held_back_ship_is_not_filed_as_a_rejection(self):
        """SUPERSEDED is the other direction of override and needs the same
        protection.

        A column that cleared the floor and was held back for a structural
        reason -- collinearity with a shipped column, or an owner decision
        pending out-of-sample evidence -- must not be filed among the
        rejections, where the record would read as "we measured it and it was
        nothing". That is the same laundering
        test_overrides_are_not_laundered_into_the_shipped_set prevents, run the
        other way."""
        from scripts.screen_preseason_features import (
            REJECTED_COLUMNS,
            SHIPPED_COLUMNS,
            SUPERSEDED_COLUMNS,
        )

        assert not set(SUPERSEDED_COLUMNS) & set(REJECTED_COLUMNS)
        assert not set(SUPERSEDED_COLUMNS) & set(SHIPPED_COLUMNS)
        for column, rationale in SUPERSEDED_COLUMNS.items():
            assert len(rationale) > 40, f"{column} is held back without a stated reason"


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

    def test_audit_and_screen_share_one_coach_tenure_definition(self):
        """PR #54 review, P2. The audit's hand-copied coach CTE omitted the
        gaps-and-islands grouping, so a coach returning to a school inherited
        his FIRST stint's start year. That widens the regime window, so the
        audit found recruiting classes where the screen saw none and
        under-reported exactly the quantity it exists to measure. Sharing one
        definition is what makes the two unable to disagree.
        """
        from scripts.screen_preseason_features import (
            _COACH_TENURE_CTE,
            AUDIT_QUERY,
            SCREEN_FRAME_QUERY,
        )

        assert "coach_islands" in _COACH_TENURE_CTE, "islands grouping is the point"
        for query in (SCREEN_FRAME_QUERY, AUDIT_QUERY):
            assert _COACH_TENURE_CTE.strip() in query, (
                "both queries must embed the shared coach-tenure CTE verbatim"
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
        """Both parameter-bound queries, not just the frame query.

        AUDIT_QUERY binds the same two parameters and embeds the same shared
        coach CTE -- which is where the two original stray percent signs lived
        -- so testing only SCREEN_FRAME_QUERY left half the blast radius
        uncovered.
        """
        import re

        from scripts.screen_preseason_features import AUDIT_QUERY, SCREEN_FRAME_QUERY

        # Strip valid named placeholders and doubled literals, then nothing
        # containing a bare % may remain.
        for name, q in (("SCREEN_FRAME_QUERY", SCREEN_FRAME_QUERY), ("AUDIT_QUERY", AUDIT_QUERY)):
            stripped = re.sub(r"%\([a-z_]+\)s", "", q).replace("%%", "")
            assert "%" not in stripped, (
                f"unescaped % in {name} -- psycopg2 will read it as a "
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


class TestCoachCareerPriorInventsNothing:
    """Section 6.0b, second pass.

    The flat `hc_first_year` binary cannot tell a proven hire inheriting a
    roster from an unproven one, and the obvious fix -- a continuous career
    prior -- needs a number for a coach who has no career, which does not
    exist. The ad-hoc exploration used -8.0. That is the same class of error as
    the zero-filled regime window this screen already removed: a fabricated
    extreme, landing on exactly the rows most likely to be weak anyway.

    These assertions pin the properties that keep the fabrication out.
    """

    def test_every_regime_variant_reaches_the_screen(self):
        from scripts.screen_preseason_features import CANDIDATE_COLUMNS
        from scripts.screen_preseason_features import SCREEN_FRAME_QUERY as q

        for column in (
            "hc_first_year_rookie",
            "hc_first_year_prior_below",
            "hc_first_year_unproven",
            "hc_first_year_proven",
            "hc_career_prior",
        ):
            assert column in CANDIDATE_COLUMNS, f"{column} is defined but never screened"
            assert f"AS {column}" in q, f"{column} is screened but never built"

    def test_career_prior_reads_only_seasons_strictly_before_the_screened_one(self):
        """The leak surface. `<` rather than `<=` is the whole guard: at `<=`
        the average would include the coach's season-S row, which at this
        school IS the outcome."""
        import re

        from scripts.screen_preseason_features import _COACH_TENURE_CTE as cte

        assert re.search(r"prev\.year\s*<\s*cy\.year", cte), (
            "career prior must be windowed strictly before the season"
        )
        assert not re.search(r"prev\.year\s*<=", cte), (
            "a <= lookback lets season S into its own predictor"
        )

    def test_no_fabricated_rating_stands_in_for_a_missing_career(self):
        """No COALESCE and no magic number on the career prior.

        The only literal the prior may be compared against is the
        pre-registered threshold; anything else is a value invented for
        coaches who have no record.
        """
        import re

        from scripts.screen_preseason_features import HC_PROVEN_SP_PLUS
        from scripts.screen_preseason_features import SCREEN_FRAME_QUERY as q

        assert "COALESCE(s.hc_prior_sp_mean" not in q
        assert "COALESCE(cp.prior_sp_mean" not in q
        assert "COALESCE(prev.rating" not in q

        compared = re.findall(r"hc_prior_sp_mean\s*(?:<|>=|>|<=|=)\s*(-?[\d.]+)", q)
        assert compared, "the proven/unproven split should compare the career prior to something"
        assert set(compared) == {str(HC_PROVEN_SP_PLUS)}, (
            f"career prior compared against un-preregistered literal(s): {set(compared)}"
        )

    def test_a_first_time_head_coach_gets_an_indicator_not_a_value(self):
        """Absence is carried as a category, where it needs no number."""
        assert "s.hc_prior_seasons = 0" in _case_expression("hc_first_year_rookie"), (
            "rookie status must come from the count of prior seasons, which is "
            "always known, not from a rating that may not exist"
        )

    def test_an_unratable_career_is_null_rather_than_assumed_unproven(self):
        """A hire with head-coaching seasons the warehouse cannot rate (an FCS
        stop, or a year before its SP+ coverage) is UNKNOWN on the proven /
        unproven split.

        Defaulting him to "unproven" would be the -8.0 again in categorical
        dress: FCS and older-record hires skew toward weaker programs, so the
        default would land on low-outcome rows and manufacture exactly the
        signal the split is trying to measure.
        """
        for column in (
            "hc_first_year_prior_below",
            "hc_first_year_unproven",
            "hc_first_year_proven",
        ):
            assert "WHEN s.hc_prior_sp_mean IS NULL THEN NULL" in _case_expression(column), (
                f"{column} must yield NULL, not a side, for an unratable record"
            )

    def test_a_rookie_is_classified_before_the_unratable_guard(self):
        """Branch ORDER is load-bearing, and the wrong order is silent.

        A first-time head coach also has no career prior, so if the unratable
        guard came first every rookie would go NULL -- dropping the largest
        subgroup in the decomposition out of its own complete cases while the
        column still looked populated.
        """
        for column, rookie_value in (
            ("hc_first_year_rookie", "1.0"),
            ("hc_first_year_unproven", "1.0"),
            ("hc_first_year_proven", "0.0"),
            ("hc_first_year_prior_below", "0.0"),
        ):
            case = _case_expression(column)
            rookie_arm = case.index("s.hc_prior_seasons = 0")
            assert f"THEN {rookie_value}" in case[rookie_arm : rookie_arm + 40], (
                f"{column} must resolve a rookie to {rookie_value}"
            )
            if "hc_prior_sp_mean IS NULL" in case:
                assert rookie_arm < case.index("hc_prior_sp_mean IS NULL"), (
                    f"{column} nulls rookies out by testing the rating first"
                )

    def test_career_prior_join_cannot_change_the_frame_row_count(self):
        """Every other candidate's n must be untouched by this addition.

        coach_prior is grouped to one row per (school, year) and LEFT JOINed,
        so it widens the spine without lengthening it. A fan-out here would
        silently re-weight every partial in the screen -- including the ones
        already on the record.
        """
        from scripts.screen_preseason_features import _COACH_TENURE_CTE as cte
        from scripts.screen_preseason_features import SCREEN_FRAME_QUERY as q

        assert "GROUP BY cy.school, cy.year" in cte
        assert "LEFT JOIN coach_prior cp ON cp.school = sp.team AND cp.year = sp.year" in q

    def test_audit_reports_the_subgroup_sizes(self):
        """A partial correlation does not say how many rows carry the
        indicator, and these subgroups differ by an order of magnitude."""
        from scripts.screen_preseason_features import AUDIT_QUERY

        for count in (
            "hc_first_year_rookie_rows",
            "hc_first_year_prior_below_rows",
            "hc_first_year_proven_rows",
            "hc_career_prior_unratable",
            "frame_prior_sp_at_or_above_zero",
        ):
            assert f"AS {count}" in AUDIT_QUERY


class TestFrameGrainGuard:
    """A fanned-out join does not fail, it re-weights.

    The section 6.0b columns needed a second coach-side join into the spine.
    Joined on a unique key it adds columns only, but if that key ever stops
    being unique the duplicated teams get counted twice in EVERY candidate at
    once -- the recorded ones included -- and the only symptom is a bigger n.
    """

    def test_a_clean_frame_passes(self):
        frame = [{"season": 2020, "team": "A"}, {"season": 2020, "team": "B"}]
        check_one_row_per_team_season(frame)

    def test_the_same_team_twice_in_one_season_raises(self):
        frame = [{"season": 2020, "team": "A"}, {"season": 2020, "team": "A"}]
        with pytest.raises(RuntimeError, match="fanned out"):
            check_one_row_per_team_season(frame)

    def test_the_same_team_in_different_seasons_is_fine(self):
        check_one_row_per_team_season(
            [{"season": 2020, "team": "A"}, {"season": 2021, "team": "A"}]
        )


class TestDecomposingAConcentratedPenalty:
    """Why a decomposition can beat the flat binary, in the screen's own math.

    If a penalty applies to only part of the flagged group, the flat indicator
    averages the penalised and unpenalised halves together and reports the
    diluted figure. Splitting the indicator recovers the undiluted one -- and
    reports the other half as the null it is.
    """

    def _frame(self, n=1600, seed=31, proven_share=0.33, penalty=-1.0):
        """20 per cent of team-seasons hire a new head coach; `proven_share` of
        those hires carry no penalty at all."""
        rng = random.Random(seed)
        rows = []
        for _ in range(n):
            z = rng.gauss(0.0, 1.0)
            w = rng.gauss(0.0, 1.0)
            first_year = rng.random() < 0.20
            proven = first_year and rng.random() < proven_share
            unproven = first_year and not proven
            y = 0.6 * z + 0.45 * w + rng.gauss(0.0, 0.5) + (penalty if unproven else 0.0)
            rows.append(
                {
                    "sp_rating": y,
                    "prior_sp_rating": z,
                    PRIMARY_CONTROL: w,
                    "hc_first_year": 1.0 if first_year else 0.0,
                    "hc_first_year_unproven": 1.0 if unproven else 0.0,
                    "hc_first_year_proven": 1.0 if proven else 0.0,
                }
            )
        return rows

    def _screened(self, frame):
        columns = ["hc_first_year", "hc_first_year_unproven", "hc_first_year_proven"]
        return {r["feature"]: r for r in screen(frame, [PRIMARY_CONTROL, *columns])}

    def test_the_flat_binary_understates_a_penalty_it_averages_over(self):
        by_name = self._screened(self._frame())
        flat = by_name["hc_first_year"]["partial_r"]
        unproven = by_name["hc_first_year_unproven"]["partial_r"]

        assert flat < 0.0 and unproven < 0.0, "both should carry the penalty's sign"
        assert abs(unproven) > abs(flat), (
            f"splitting must recover a stronger signal than the average "
            f"(unproven={unproven:.4f}, flat={flat:.4f})"
        )

    def test_the_unpenalised_half_screens_as_a_null(self):
        """The substantive claim. It is only a finding if it is measured, which
        is why the proven indicator is a screened candidate rather than a
        remark in a docstring."""
        proven = self._screened(self._frame())["hc_first_year_proven"]
        assert abs(proven["partial_r"]) < MIN_PARTIAL_R
        assert proven["verdict"] == "reject"

    def test_a_flat_penalty_gives_the_split_nothing_to_recover(self):
        """The control case: when the penalty really is flat, the split must
        NOT look better. Otherwise the test above would be measuring an
        artefact of splitting rather than a concentrated effect."""
        rng = random.Random(41)
        rows = []
        for _ in range(1600):
            z = rng.gauss(0.0, 1.0)
            w = rng.gauss(0.0, 1.0)
            first_year = rng.random() < 0.20
            proven = first_year and rng.random() < 0.33
            y = 0.6 * z + 0.45 * w + rng.gauss(0.0, 0.5) + (-1.0 if first_year else 0.0)
            rows.append(
                {
                    "sp_rating": y,
                    "prior_sp_rating": z,
                    PRIMARY_CONTROL: w,
                    "hc_first_year": 1.0 if first_year else 0.0,
                    "hc_first_year_unproven": 1.0 if (first_year and not proven) else 0.0,
                    "hc_first_year_proven": 1.0 if proven else 0.0,
                }
            )
        by_name = self._screened(rows)
        assert abs(by_name["hc_first_year_unproven"]["partial_r"]) < abs(
            by_name["hc_first_year"]["partial_r"]
        ), "a genuinely flat penalty is best measured by the flat indicator"


class TestSmallSubgroupsAreDisciplinedByTheEffectFloor:
    """The most eye-catching subgroup mean in the ad-hoc exploration rested on
    32 rows.

    A binary candidate's correlation scales with sqrt(p * (1 - p)), so a group
    a tenth the size needs a mean shift roughly three times larger merely to
    score the same partial. The pre-registered effect floor already encodes
    that -- which is the argument for putting the tiny subgroup through the
    gate rather than quoting its subgroup mean.
    """

    def test_a_bigger_mean_shift_on_a_tiny_group_can_score_lower(self):
        rng = random.Random(53)
        rows = []
        for _ in range(2000):
            z = rng.gauss(0.0, 1.0)
            w = rng.gauss(0.0, 1.0)
            roll = rng.random()
            tiny = roll < 0.02
            broad = 0.02 <= roll < 0.27
            shift = -3.0 if tiny else (-2.0 if broad else 0.0)
            rows.append(
                {
                    "sp_rating": 0.6 * z + 0.45 * w + rng.gauss(0.0, 0.5) + shift,
                    "prior_sp_rating": z,
                    PRIMARY_CONTROL: w,
                    "tiny_group": 1.0 if tiny else 0.0,
                    "broad_group": 1.0 if broad else 0.0,
                }
            )

        by_name = {
            r["feature"]: r for r in screen(rows, [PRIMARY_CONTROL, "tiny_group", "broad_group"])
        }
        assert abs(by_name["tiny_group"]["partial_r"]) < abs(by_name["broad_group"]["partial_r"]), (
            "the group with the larger subgroup mean shift must still score "
            "lower, because prevalence bounds the correlation"
        )


class TestDraftCoverageIsAudited:
    """The split-window re-run found draft.draft_picks held 2020-2026 only,
    while years.py configures 2000-2026.

    Nothing errored. `draft_out` is not year-filtered, so the S-1..S-3 lookback
    simply found no rows and COALESCE(..., 0) turned "this draft was never
    ingested" into "this program produced zero NFL picks" on 54.2% of the
    frame. Every draft verdict was measuring the load state of the warehouse.

    --audit-imputation had counters for recruiting and blue-chip zero-fill and
    none for draft, which is why it ran clean while the defect was live. These
    tests pin the counters that close that gap."""

    def test_audit_counts_draft_source_years(self):
        from scripts.screen_preseason_features import AUDIT_QUERY

        assert "draft_picks_3yr_no_source_year" in AUDIT_QUERY
        assert "draft_departures_no_source_year" in AUDIT_QUERY

    def test_audit_defines_the_draft_cte_it_reads(self):
        """The counters live in AUDIT_QUERY, which is a separate statement from
        SCREEN_FRAME_QUERY -- a counter referencing a CTE only the screen
        defines would fail at runtime, and only when --audit-imputation ran."""
        from scripts.screen_preseason_features import AUDIT_QUERY

        assert "draft_out AS" in AUDIT_QUERY
        assert AUDIT_QUERY.index("draft_out AS") < AUDIT_QUERY.index(
            "draft_picks_3yr_no_source_year"
        )

    def test_coverage_is_measured_on_source_years_not_team_rows(self):
        """The distinction the counter exists to draw. A team absent from a
        draft that WAS ingested really did produce no picks -- a true zero. A
        team absent because the draft was never loaded is not a measurement.
        Testing EXISTS over the season alone, with no team predicate, is what
        separates them; adding `d.team = s.team` would collapse the two back
        together and make the counter agree with the bug."""
        from scripts.screen_preseason_features import AUDIT_QUERY

        block = AUDIT_QUERY[AUDIT_QUERY.index("draft_picks_3yr_no_source_year") - 400 :]
        block = block[: block.index("draft_departures_no_source_year")]
        assert "d.team" not in block, (
            "source-year coverage must not be filtered to the team, or a real "
            "zero and a missing draft become indistinguishable again"
        )

    def test_every_void_verdict_says_why_it_is_void(self):
        """A rejection and a void are different claims. The first says we
        measured it and it was nothing; the second says the measurement was
        never valid. Recording a void as a plain rejection is how a fixable
        load gap becomes a closed question."""
        from scripts.screen_preseason_features import REJECTED_COLUMNS

        for column in (
            "draft_picks_3yr",
            "conversion",
            "draft_yield",
            "draft_departures",
        ):
            assert column in REJECTED_COLUMNS
            assert "VOID" in REJECTED_COLUMNS[column], (
                f"{column} rests on fabricated zeros and must not read as a measurement"
            )


class TestMidSeasonCoachingChangesAreExcluded:
    """PR #55 review, P1 -- a leak, not a rounding error.

    ref.coaches__seasons attributes a whole season to each coach it lists and
    cannot split a mid-season change (src/schemas/api/038_coach_records.sql).
    Picking the most-games coach therefore resolves an EARLY firing to the
    replacement, whose tenure starts that year, marking week 1 as "first-year
    coach" when it was not.

    The bias runs one way: the earlier the firing, the more games the
    replacement inherits, and the worse the season was going. So the
    contaminated cases are exactly the ones most correlated with a bad season,
    and the flag would carry season S's own outcome into a feature that claims
    to be preseason-known. 24 of 300 positives on 2015-2025.
    """

    def test_screen_and_build_share_the_guard(self):
        from scripts.build_features import FEATURE_ROWS_QUERY
        from scripts.screen_preseason_features import AUDIT_QUERY, SCREEN_FRAME_QUERY

        for query in (SCREEN_FRAME_QUERY, AUDIT_QUERY, FEATURE_ROWS_QUERY):
            assert "coach_counts" in query, "ambiguity guard missing"
            assert "WHEN cc.n_coaches > 1 THEN NULL" in query, (
                "a school-year with several listed coaches must yield NULL tenure, "
                "not a guessed one"
            )

    def test_guard_nulls_tenure_rather_than_dropping_the_row(self):
        """The team-week row still has to exist -- every other feature on it is
        fine. Only the coaching signal is unknown."""
        from scripts.build_features import FEATURE_ROWS_QUERY

        assert "LEFT JOIN coach_tenure" in FEATURE_ROWS_QUERY

    def test_career_prior_is_not_year_filtered_either(self):
        """Same argument as the tenure history, one step further.

        A coach's career prior is the mean rating of his PREVIOUS stops, so
        clipping the coach CTE to the screened window would truncate careers at
        the window's first year and reclassify long-serving coaches as having no
        record -- pushing them into the rookie bucket, which is the single
        classification error that would most flatter this decomposition.
        """
        from scripts.screen_preseason_features import _COACH_TENURE_CTE

        assert "coach_prior" in _COACH_TENURE_CTE
        assert "%(to_season)s" not in _COACH_TENURE_CTE

    def test_coach_history_is_not_year_filtered(self):
        """Filtering the coach CTE to the screened window would left-censor
        tenure, making every long-tenured coach look like he started in the
        window's first year -- which inflates hc_first_year wholesale."""
        from scripts.screen_preseason_features import _COACH_TENURE_CTE

        assert "%(from_season)s" not in _COACH_TENURE_CTE
        assert "BETWEEN" not in _COACH_TENURE_CTE.upper()
