"""Unit tests for the drive-chain EP engine (no DB, no API).

Design: docs/plans/2026-08-08-drive-chain-ep-model-plan.md. Everything here
exercises the pure layer of scripts/compute_drive_chain.py.
"""

from collections import Counter

import pytest

from scripts.compute_drive_chain import (
    ABSORB_MAP,
    ABSORB_VALUES,
    ABSORBING,
    TD_VALUE,
    build_transitions,
    check_monotone_down,
    check_monotone_zone,
    distance_bucket,
    field_zone,
    parent_key,
    shrink,
    solve_ep,
    state_key,
)


class TestStateBucketing:
    def test_zone_bands(self):
        assert field_zone(1) == 1
        assert field_zone(10) == 1
        assert field_zone(11) == 2
        assert field_zone(75) == 8
        assert field_zone(99) == 10

    def test_first_down_vocabulary_is_down_aware(self):
        """The fix for the 38 starved states: 1st down does not use the
        2nd-4th short/med/long ladder."""
        assert distance_bucket(1, 10, 75) == "standard"
        assert distance_bucket(1, 5, 75) == "short"  # post-penalty
        assert distance_bucket(1, 15, 75) == "long"
        assert distance_bucket(1, 8, 5) == "goal"

    def test_late_down_ladder(self):
        assert distance_bucket(3, 2, 60) == "short"
        assert distance_bucket(3, 5, 60) == "med"
        assert distance_bucket(3, 9, 60) == "long"
        assert distance_bucket(3, 14, 60) == "xlong"

    def test_goal_to_go_overrides(self):
        assert distance_bucket(2, 4, 3) == "goal"

    def test_state_key_shape(self):
        assert state_key(1, 10, 75) == "d1|standard|z8"
        assert parent_key("d1|standard|z8") == "d1|*|z8"


class TestAbsorbMap:
    def test_every_mapped_target_is_a_known_absorbing_state(self):
        assert set(ABSORB_MAP.values()) <= set(ABSORBING)

    def test_live_vocabulary_is_covered(self):
        """The drive_result values observed in production (2026-08-08 check,
        seasons >= 2014) that the build must not drop."""
        live = [
            "PUNT",
            "TD",
            "FG",
            "DOWNS",
            "INT",
            "FUMBLE",
            "MISSED FG",
            "END OF HALF",
            "END OF GAME",
            "INT TD",
            "END OF 4TH QUARTER",
            "SF",
            "FUMBLE RETURN TD",
            "FUMBLE TD",
            "PUNT TD",
            "PUNT RETURN TD",
            "MISSED FG TD",
            "DOWNS TD",
        ]
        for result in live:
            assert result in ABSORB_MAP, f"unmapped live drive_result: {result}"

    def test_defensive_scores_are_negative(self):
        assert ABSORB_VALUES["TURNOVER_TD"] == -TD_VALUE


class TestSolveEP:
    def test_single_state_analytic(self):
        """P(TD)=0.3, P(PUNT)=0.7 => EP = 0.3 * TD_VALUE exactly."""
        probs = {("d1|standard|z5", "TD"): 0.3, ("d1|standard|z5", "PUNT"): 0.7}
        ep, absorb = solve_ep(probs)
        assert ep["d1|standard|z5"] == pytest.approx(0.3 * TD_VALUE)
        assert absorb[("d1|standard|z5", "TD")] == pytest.approx(0.3)

    def test_two_state_chain_analytic(self):
        """A -> {B: 0.5, TD: 0.5}; B -> PUNT. EP(A) = 0.5 * TD_VALUE."""
        probs = {
            ("A", "B"): 0.5,
            ("A", "TD"): 0.5,
            ("B", "PUNT"): 1.0,
        }
        ep, absorb = solve_ep(probs)
        assert ep["A"] == pytest.approx(0.5 * TD_VALUE)
        assert ep["B"] == pytest.approx(0.0)
        assert absorb[("A", "PUNT")] == pytest.approx(0.5)

    def test_self_loop_converges(self):
        """A stays in A half the time: geometric absorption still sums to 1."""
        probs = {("A", "A"): 0.5, ("A", "TD"): 0.25, ("A", "PUNT"): 0.25}
        ep, absorb = solve_ep(probs)
        assert absorb[("A", "TD")] == pytest.approx(0.5)
        assert ep["A"] == pytest.approx(0.5 * TD_VALUE)


class TestShrinkage:
    def test_healthy_state_barely_moves(self):
        counts = Counter({("d2|med|z5", "TD"): 4000, ("d2|med|z5", "PUNT"): 6000})
        shrunk = shrink(counts, alpha=50.0)
        assert shrunk[("d2|med|z5", "TD")] == pytest.approx(0.4, abs=0.01)

    def test_starved_state_inherits_parent(self):
        """One observation must not become a 100% transition; the sibling
        bucket in the same down x zone dominates through the parent."""
        counts = Counter(
            {
                ("d1|short|z5", "TD"): 1,  # starved: saw only a TD
                ("d1|standard|z5", "TD"): 300,
                ("d1|standard|z5", "PUNT"): 700,
            }
        )
        shrunk = shrink(counts, alpha=50.0)
        assert shrunk[("d1|short|z5", "TD")] < 0.5
        assert shrunk[("d1|short|z5", "PUNT")] > 0.4  # inherited, never observed

    def test_rows_sum_to_one(self):
        counts = Counter(
            {
                ("d3|long|z4", "TD"): 10,
                ("d3|long|z4", "PUNT"): 30,
                ("d3|short|z4", "DOWNS"): 5,
            }
        )
        shrunk = shrink(counts, alpha=25.0)
        for state in {a for (a, _) in shrunk}:
            total = sum(p for (a, _), p in shrunk.items() if a == state)
            assert total == pytest.approx(1.0)


class TestBuildTransitions:
    def _drive(self, game_id, drive_id, result, snapshots):
        return [
            {
                "game_id": game_id,
                "drive_id": drive_id,
                "play_number": i + 1,
                "down": d,
                "distance": dist,
                "yards_to_goal": ytg,
                "drive_result": result,
            }
            for i, (d, dist, ytg) in enumerate(snapshots)
        ]

    def test_last_play_absorbs_into_drive_result(self):
        plays = self._drive(1, "d1", "PUNT", [(1, 10, 75), (2, 6, 71), (3, 6, 71)])
        transitions, per_game, outcomes, n_mapped, unmapped = build_transitions(plays)

        assert unmapped == 0
        assert n_mapped == 1
        assert outcomes[(state_key(1, 10, 75), "PUNT")] == 1
        assert transitions[(state_key(1, 10, 75), state_key(2, 6, 71))] == 1
        assert transitions[(state_key(3, 6, 71), "PUNT")] == 1

    def test_unmapped_drive_result_is_dropped_and_counted(self):
        plays = self._drive(1, "d1", "Uncategorized", [(1, 10, 75)])
        transitions, _, _, n_mapped, unmapped = build_transitions(plays)

        assert unmapped == 1
        assert n_mapped == 0
        assert not transitions

    def test_penalty_gap_is_absorbed_into_next_snapshot(self):
        """A penalty between two scrimmage plays is invisible: the next
        snapshot's state carries its effect (play_number 3 follows 1)."""
        plays = self._drive(1, "d1", "TD", [(1, 10, 75)]) + [
            {
                "game_id": 1,
                "drive_id": "d1",
                "play_number": 3,  # play 2 was a penalty row, excluded upstream
                "down": 1,
                "distance": 10,
                "yards_to_goal": 60,
                "drive_result": "TD",
            }
        ]
        transitions, _, _, _, _ = build_transitions(plays)

        assert transitions[(state_key(1, 10, 75), state_key(1, 10, 60))] == 1

    def test_drives_are_counted_as_drives(self):
        """PR #66 review, P2: the unmapped-share denominator is drives.
        One drive visiting three states is still ONE drive, and two drives
        revisiting the same state are still TWO."""
        plays = (
            self._drive(1, "a", "PUNT", [(1, 10, 75), (2, 6, 71), (3, 6, 71)])
            + self._drive(1, "b", "TD", [(1, 10, 75)])
            + self._drive(1, "c", "Uncategorized", [(1, 10, 75)])
        )
        _, _, _, n_mapped, unmapped = build_transitions(plays)

        assert n_mapped == 2
        assert unmapped == 1


class TestShrinkageGrandparentTargets:
    def test_grandparent_only_target_is_emitted(self):
        """PR #66 review, P1: a target observed only elsewhere in the same
        down carries grandparent probability through p_parent(); if the row
        never emits it, the row sums to less than 1 and solve_ep() leaks
        that mass out of the chain."""
        counts = Counter(
            {
                ("d2|med|z3", "TD"): 100,  # zone 3 parent only ever sees TD
                ("d2|med|z8", "PUNT"): 100,  # zone 8 parent only ever sees PUNT
            }
        )
        shrunk = shrink(counts, alpha=50.0)

        # Each row must emit BOTH targets and sum to exactly 1.
        for state in ("d2|med|z3", "d2|med|z8"):
            row = {b: p for (a, b), p in shrunk.items() if a == state}
            assert "TD" in row and "PUNT" in row, f"{state} missing a grandparent target"
            assert sum(row.values()) == pytest.approx(1.0)


class TestCalibration:
    def test_perfect_model_scores_zero(self):
        from scripts.compute_drive_chain import calibration_mae

        outcomes = Counter({("d1|standard|z8", "TD"): 30, ("d1|standard|z8", "PUNT"): 70})
        absorb = {("d1|standard|z8", "TD"): 0.3}
        assert calibration_mae(absorb, outcomes) == pytest.approx(0.0)

    def test_miscalibration_is_measured_against_realized_outcomes(self):
        """PR #66 review, P2: the gate compares model vs REALIZED drive
        outcomes, not model vs itself -- a systematically wrong chain must
        score badly."""
        from scripts.compute_drive_chain import calibration_mae

        outcomes = Counter({("d1|standard|z8", "TD"): 30, ("d1|standard|z8", "PUNT"): 70})
        absorb = {("d1|standard|z8", "TD"): 0.6}  # model says 60%, reality 30%
        assert calibration_mae(absorb, outcomes) == pytest.approx(0.3)

    def test_thin_start_states_are_skipped(self):
        from scripts.compute_drive_chain import calibration_mae

        outcomes = Counter({("d4|xlong|z2", "TD"): 3})  # 3 drives: noise, not signal
        absorb = {("d4|xlong|z2", "TD"): 0.9}
        import math

        assert math.isnan(calibration_mae(absorb, outcomes))


class TestMonotoneGates:
    def test_clean_curve_passes(self):
        ep = {f"d1|standard|z{z}": 6.0 - 0.5 * z for z in range(1, 11)}
        assert check_monotone_zone(ep) == []

    def test_rising_curve_fails(self):
        ep = {f"d1|standard|z{z}": 6.0 - 0.5 * z for z in range(1, 11)}
        ep["d1|standard|z7"] = ep["d1|standard|z6"] + 1.0
        assert check_monotone_zone(ep)

    def test_down_ordering_gate(self):
        ep = {}
        for z in range(2, 10):
            ep[f"d2|med|z{z}"] = 3.0
            ep[f"d3|med|z{z}"] = 2.5
        assert check_monotone_down(ep) == []
        ep["d3|med|z5"] = 9.0
        assert check_monotone_down(ep)

    def test_fourth_down_is_exempt_by_design(self):
        """d4 snapshots are go-for-it-conditional (punts/FGs exit from the
        3rd-down play), so d4 above d3 is legitimate -- observed on the real
        2021+ chain -- and must NOT fail the gate."""
        ep = {}
        for z in range(2, 10):
            ep[f"d2|med|z{z}"] = 3.0
            ep[f"d3|med|z{z}"] = 2.5
            ep[f"d4|med|z{z}"] = 2.6  # above d3: allowed
        assert check_monotone_down(ep) == []


class TestLegacyVocabulary:
    """First full-era run (deploy 31257283280): 2004-2013 spoke a different
    drive_result dialect and 18.24% of its drives went unmapped."""

    def test_legacy_synonyms_are_mapped(self):
        for legacy, absorb in [
            ("FG GOOD", "FG"),
            ("MADE FG", "FG"),
            ("FG MISSED", "MISSED_FG"),
            ("TURNOVER ON DOWNS", "DOWNS"),
            ("POSS. ON DOWNS", "DOWNS"),
            ("INT RETURN TOUCH", "TURNOVER_TD"),
            ("PUNT RETURN TD TD", "TURNOVER_TD"),
        ]:
            assert ABSORB_MAP.get(legacy) == absorb, legacy

    def test_ambiguous_legacy_results_stay_unmapped(self):
        """KICKOFF / last-play labels are verified non-scoring but their
        outcomes are unrecoverable -- dropping them is deliberate, mapping
        them to a guess would fabricate outcomes."""
        for ambiguous in ("KICKOFF", "RUSH", "SACK", "PASS COMPLETE", "INCOMPLETE"):
            assert ambiguous not in ABSORB_MAP

    def test_legacy_era_guard_is_looser_but_bounded(self):
        from scripts.compute_drive_chain import MAX_UNMAPPED_SHARE, MAX_UNMAPPED_SHARE_BY_ERA

        assert MAX_UNMAPPED_SHARE_BY_ERA["2004-2013"] == 0.05
        assert MAX_UNMAPPED_SHARE_BY_ERA["2004-2013"] > MAX_UNMAPPED_SHARE
        assert set(MAX_UNMAPPED_SHARE_BY_ERA) == {"2004-2013"}, (
            "modern eras must stay on the tight guard"
        )


class TestZoneGateFourthDownExemption:
    def test_d4_rising_zone_curve_does_not_fail(self):
        """2014-2020 era run: d4|short rose z9->z10 (0.68->1.02) -- selection
        of who goes for it at their own goal line, not estimation failure.
        Same go-conditional exemption as the down gate."""
        ep = {f"d1|standard|z{z}": 6.0 - 0.5 * z for z in range(1, 11)}
        ep.update({f"d3|short|z{z}": 4.0 - 0.3 * z for z in range(1, 11)})
        ep["d4|short|z9"] = 0.68
        ep["d4|short|z10"] = 1.02  # rises: allowed, d4 is exempt
        assert check_monotone_zone(ep) == []

    def test_d3_short_is_now_checked_instead(self):
        ep = {f"d3|short|z{z}": 4.0 - 0.3 * z for z in range(1, 11)}
        ep["d3|short|z7"] = ep["d3|short|z6"] + 1.0
        assert check_monotone_zone(ep)


def _uniform_handoffs(target_zone):
    """Every handoff class sends the opponent to exactly target_zone."""
    from scripts.compute_drive_chain import HANDOFF_ABSORBING

    return {
        cls: {
            ez: {z: (1.0 if z == target_zone else 0.0) for z in range(1, 11)} for ez in range(1, 11)
        }
        for cls in HANDOFF_ABSORBING
    }


def _full_d1_grid(extra=None):
    """Minimal chain containing every handoff state (z1 goal + z2-10 standard),
    each defaulting to END_OF_HALF so the system is well-posed."""
    from scripts.compute_drive_chain import handoff_state

    probs = {}
    for z in range(1, 11):
        probs[(handoff_state(z), "END_OF_HALF")] = 1.0
    if extra:
        for (a, b), p in extra.items():
            if (a, "END_OF_HALF") in probs and b != "END_OF_HALF":
                probs[(a, "END_OF_HALF")] = max(
                    0.0, 1.0 - sum(q for (x, y), q in extra.items() if x == a)
                )
            probs[(a, b)] = p
    return probs


class TestSolveNetEP:
    def test_certain_td_is_full_value(self):
        from scripts.compute_drive_chain import solve_net_ep

        probs = _full_d1_grid({("d1|standard|z5", "TD"): 1.0})
        net = solve_net_ep(probs, _uniform_handoffs(8))
        assert net["d1|standard|z5"] == pytest.approx(TD_VALUE)

    def test_end_of_half_is_zero(self):
        from scripts.compute_drive_chain import solve_net_ep

        net = solve_net_ep(_full_d1_grid(), _uniform_handoffs(8))
        for state, val in net.items():
            assert val == pytest.approx(0.0), state

    def test_symmetric_punt_alternation_nets_to_zero(self):
        """A state that always punts to an opponent in the SAME state must be
        worth exactly 0 on the next-score basis: x = -x. A small half-end
        leak keeps the system nonsingular without breaking the symmetry."""
        from scripts.compute_drive_chain import solve_net_ep

        probs = _full_d1_grid(
            {("d1|standard|z6", "PUNT"): 0.9, ("d1|standard|z6", "END_OF_HALF"): 0.1}
        )
        net = solve_net_ep(probs, _uniform_handoffs(6))
        assert net["d1|standard|z6"] == pytest.approx(0.0, abs=1e-12)

    def test_score_or_punt_analytic(self):
        """x = 0.5*TD + 0.5*(-x)  =>  x = TD/3."""
        from scripts.compute_drive_chain import solve_net_ep

        probs = _full_d1_grid({("d1|standard|z6", "TD"): 0.5, ("d1|standard|z6", "PUNT"): 0.5})
        net = solve_net_ep(probs, _uniform_handoffs(6))
        assert net["d1|standard|z6"] == pytest.approx(TD_VALUE / 3)

    def test_asymmetric_alternation_analytic(self):
        """A(z6): 80% punt->opp z10, 20% half-end. B(z10): 80% punt->opp z6,
        20% TD. Hand-solved: b = 0.2*TD - 0.8a, a = -0.8b =>
        b = 0.2*TD/(1-0.64), a = -0.8b."""
        from scripts.compute_drive_chain import HANDOFF_ABSORBING, solve_net_ep

        probs = _full_d1_grid(
            {
                ("d1|standard|z6", "PUNT"): 0.8,
                ("d1|standard|z6", "END_OF_HALF"): 0.2,
                ("d1|standard|z10", "PUNT"): 0.8,
                ("d1|standard|z10", "TD"): 0.2,
            }
        )
        handoffs = {
            cls: {
                ez: {z: (1.0 if z == (10 if ez == 6 else 6) else 0.0) for z in range(1, 11)}
                for ez in range(1, 11)
            }
            for cls in HANDOFF_ABSORBING
        }
        net = solve_net_ep(probs, handoffs)
        b = 0.2 * TD_VALUE / (1 - 0.64)
        assert net["d1|standard|z10"] == pytest.approx(b)
        assert net["d1|standard|z6"] == pytest.approx(-0.8 * b)

    def test_missing_handoff_state_raises(self):
        from scripts.compute_drive_chain import solve_net_ep

        with pytest.raises(ValueError, match="handoff states absent"):
            solve_net_ep({("d1|standard|z5", "TD"): 1.0}, _uniform_handoffs(8))


class TestBuildHandoffs:
    def test_observed_rows_dominate_with_support(self):
        from scripts.compute_drive_chain import build_handoffs

        pairs = {("PUNT", 8, 6): 900, ("PUNT", 8, 7): 100}
        h = build_handoffs(pairs, alpha=10.0)
        assert h["PUNT"][8][6] > 0.85

    def test_starved_exit_zone_inherits_marginal(self):
        """Punts from inside the opponent 30 (zones 1-3) had 11/33/85
        observations in 2021+ -- their rows must come from the marginal."""
        from scripts.compute_drive_chain import build_handoffs

        pairs = {("PUNT", 8, 6): 500, ("PUNT", 7, 7): 500}
        h = build_handoffs(pairs, alpha=10.0)
        assert h["PUNT"][2][6] == pytest.approx(0.5, abs=0.01)
        assert h["PUNT"][2][7] == pytest.approx(0.5, abs=0.01)

    def test_rows_are_distributions(self):
        from scripts.compute_drive_chain import HANDOFF_ABSORBING, build_handoffs

        pairs = {("PUNT", 8, 6): 10, ("TURNOVER", 5, 5): 3, ("DOWNS", 4, 96 // 10): 2}
        h = build_handoffs(pairs)
        for cls in HANDOFF_ABSORBING:
            for ez in range(1, 11):
                assert sum(h[cls][ez].values()) == pytest.approx(1.0)

    def test_scoring_classes_are_ignored(self):
        from scripts.compute_drive_chain import build_handoffs

        h = build_handoffs({("TD", 3, 8): 100})
        assert "TD" not in h
