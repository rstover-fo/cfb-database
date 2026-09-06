"""Pure tests for schedule-driven, as-of adjusted EPA week boundaries.

The target schedule and play stream are independent inputs. Every emitted
target W must use exactly the qualifying plays with week_index < W, including
when W has no plays, while preserving RidgeAccumulator's layout and math.
"""

import pytest

pytest.importorskip("numpy")

from scripts.compute_adjusted_epa import LAMBDA, RidgeAccumulator  # noqa: E402
from scripts.compute_adjusted_epa_week import compute_week_boundaries  # noqa: E402

TEAMS = ["Alpha", "Bravo", "Charlie", "Delta"]

# (offense, defense, is_home_offense, epa, week_index)
WEEK1_PLAYS = [
    ("Alpha", "Bravo", True, 0.40, 1),
    ("Bravo", "Alpha", False, -0.10, 1),
    ("Charlie", "Delta", True, 0.20, 1),
    ("Delta", "Charlie", False, -0.30, 1),
    ("Alpha", "Charlie", False, 0.10, 1),
    ("Bravo", "Delta", True, 0.05, 1),
]

WEEK2_PLAYS = [
    ("Delta", "Alpha", True, 0.15, 2),
    ("Charlie", "Bravo", False, -0.05, 2),
    ("Alpha", "Delta", False, 0.25, 2),
    ("Bravo", "Charlie", True, -0.10, 2),
]

WEEK3_PLAYS = [
    ("Alpha", "Bravo", True, 0.30, 3),
    ("Charlie", "Delta", False, -0.20, 3),
]


def _without_week(plays):
    return [(off, deff, home, epa) for off, deff, home, epa, _week in plays]


def _rows_at(boundaries, week_index):
    return {row["team"]: row for row in boundaries if row["week_index"] == week_index}


def _reference(plays):
    accumulator = RidgeAccumulator(TEAMS)
    accumulator.add_plays(_without_week(plays))
    return accumulator, accumulator.solve(LAMBDA)


def _assert_matches_reference(actual_rows, reference_plays):
    accumulator, (mu, hfa, off_coef, def_coef, _n_plays) = _reference(reference_plays)
    assert set(actual_rows) == set(TEAMS)
    for index, team in enumerate(TEAMS):
        row = actual_rows[team]
        assert row["off_coef"] == pytest.approx(off_coef[team])
        assert row["def_coef"] == pytest.approx(def_coef[team])
        assert row["mu"] == pytest.approx(mu)
        assert row["hfa_coef"] == pytest.approx(hfa)
        assert row["plays"] == int(accumulator.off_play_counts[index])
        assert row["lambda"] == LAMBDA
        assert row["n_teams"] == len(TEAMS)


class TestStrictEarlierPlaySemantics:
    def test_numerical_parity_with_direct_ridge_fit(self):
        plays = WEEK1_PLAYS + WEEK2_PLAYS + WEEK3_PLAYS
        boundaries = compute_week_boundaries(
            plays,
            TEAMS,
            target_week_indices=[2, 3],
            lam=LAMBDA,
            season=2024,
        )

        _assert_matches_reference(_rows_at(boundaries, 2), WEEK1_PLAYS)
        _assert_matches_reference(_rows_at(boundaries, 3), WEEK1_PLAYS + WEEK2_PLAYS)
        assert all(row["season"] == 2024 for row in boundaries)

    def test_midweek_target_excludes_same_week_plays(self):
        same_week_outlier = [("Delta", "Bravo", True, 999.0, 2)]
        boundaries = compute_week_boundaries(
            WEEK1_PLAYS + same_week_outlier + WEEK2_PLAYS,
            TEAMS,
            target_week_indices=[2],
        )

        _assert_matches_reference(_rows_at(boundaries, 2), WEEK1_PLAYS)

    def test_correction_rebuild_changes_every_later_applicable_target(self):
        original = compute_week_boundaries(
            WEEK1_PLAYS + WEEK2_PLAYS,
            TEAMS,
            target_week_indices=[2, 3],
        )
        corrected_week1 = [*WEEK1_PLAYS[:-1], ("Bravo", "Delta", True, 4.50, 1)]
        rebuilt = compute_week_boundaries(
            corrected_week1 + WEEK2_PLAYS,
            TEAMS,
            target_week_indices=[2, 3],
        )

        for week_index in (2, 3):
            assert _rows_at(rebuilt, week_index)["Bravo"]["off_coef"] != pytest.approx(
                _rows_at(original, week_index)["Bravo"]["off_coef"]
            )
        _assert_matches_reference(_rows_at(rebuilt, 3), corrected_week1 + WEEK2_PLAYS)


class TestScheduleDrivenTargets:
    def test_one_played_week_emits_scheduled_unplayed_week_two(self):
        boundaries = compute_week_boundaries(
            WEEK1_PLAYS,
            TEAMS,
            target_week_indices=[1, 2],
        )

        assert {row["week_index"] for row in boundaries} == {2}
        _assert_matches_reference(_rows_at(boundaries, 2), WEEK1_PLAYS)

    def test_sparse_and_bye_targets_use_only_explicit_schedule_weeks(self):
        boundaries = compute_week_boundaries(
            WEEK1_PLAYS + WEEK3_PLAYS,
            TEAMS,
            target_week_indices=[1, 2, 4],
        )

        assert {row["week_index"] for row in boundaries} == {2, 4}
        _assert_matches_reference(_rows_at(boundaries, 2), WEEK1_PLAYS)
        _assert_matches_reference(_rows_at(boundaries, 4), WEEK1_PLAYS + WEEK3_PLAYS)

    def test_unordered_duplicate_targets_are_deduplicated_and_sorted(self):
        boundaries = compute_week_boundaries(
            WEEK1_PLAYS,
            TEAMS,
            target_week_indices=[4, 2, 4, 3, 2],
        )

        emitted = [
            boundaries[index]["week_index"] for index in range(0, len(boundaries), len(TEAMS))
        ]
        assert emitted == [2, 3, 4]

    def test_empty_plays_or_only_preplay_targets_emit_no_fit(self):
        assert compute_week_boundaries([], TEAMS, target_week_indices=[1, 2, 101]) == []
        assert compute_week_boundaries(WEEK1_PLAYS, TEAMS, target_week_indices=[1]) == []

    def test_empty_targets_emit_no_fit_and_do_not_invent_max_plus_one(self):
        assert compute_week_boundaries(WEEK1_PLAYS, TEAMS, target_week_indices=[]) == []


class TestPostseasonOrdering:
    REGULAR_WEEK13 = [
        ("Delta", "Alpha", True, 0.15, 13),
        ("Charlie", "Bravo", False, -0.05, 13),
        ("Alpha", "Delta", False, 0.25, 13),
    ]
    POSTSEASON_WEEK1 = [
        ("Bravo", "Delta", True, 0.50, 101),
        ("Delta", "Bravo", False, -0.15, 101),
    ]

    def test_regular_to_101_uses_all_available_regular_plays(self):
        regular = WEEK1_PLAYS + self.REGULAR_WEEK13
        boundaries = compute_week_boundaries(
            regular + self.POSTSEASON_WEEK1,
            TEAMS,
            target_week_indices=[101],
        )

        _assert_matches_reference(_rows_at(boundaries, 101), regular)

    def test_101_to_102_includes_strictly_earlier_postseason_plays(self):
        plays = WEEK1_PLAYS + self.REGULAR_WEEK13 + self.POSTSEASON_WEEK1
        boundaries = compute_week_boundaries(
            plays,
            TEAMS,
            target_week_indices=[101, 102],
        )

        _assert_matches_reference(
            _rows_at(boundaries, 102),
            WEEK1_PLAYS + self.REGULAR_WEEK13 + self.POSTSEASON_WEEK1,
        )


class TestLayoutAndStreaming:
    def test_team_without_offensive_plays_keeps_zero_count_layout_row(self):
        plays = [
            ("Alpha", "Bravo", True, 0.40, 1),
            ("Bravo", "Alpha", False, -0.10, 1),
        ]
        boundaries = compute_week_boundaries(
            plays,
            TEAMS,
            target_week_indices=[2],
        )

        entering_week2 = _rows_at(boundaries, 2)
        assert set(entering_week2) == set(TEAMS)
        assert entering_week2["Charlie"]["plays"] == 0
        assert entering_week2["Delta"]["plays"] == 0

    def test_targets_with_unchanged_state_reuse_one_ridge_solve(self, monkeypatch):
        solve_calls = 0
        real_solve = RidgeAccumulator.solve

        def counted_solve(self, lam):
            nonlocal solve_calls
            solve_calls += 1
            return real_solve(self, lam)

        monkeypatch.setattr(RidgeAccumulator, "solve", counted_solve)
        boundaries = compute_week_boundaries(
            WEEK1_PLAYS,
            TEAMS,
            target_week_indices=[2, 3, 4, 101],
        )

        assert len(boundaries) == len(TEAMS) * 4
        assert solve_calls == 1

    def test_play_iterable_is_consumed_once_even_with_no_targets(self):
        class SinglePassPlays:
            def __init__(self, rows):
                self.rows = rows
                self.iterations = 0
                self.yielded = 0

            def __iter__(self):
                self.iterations += 1
                if self.iterations > 1:
                    raise AssertionError("play stream was iterated more than once")
                for row in self.rows:
                    self.yielded += 1
                    yield row

        plays = SinglePassPlays(WEEK1_PLAYS + WEEK2_PLAYS)
        assert compute_week_boundaries(plays, TEAMS, target_week_indices=[]) == []
        assert plays.iterations == 1
        assert plays.yielded == len(WEEK1_PLAYS + WEEK2_PLAYS)

    def test_rejects_out_of_order_play_stream(self):
        with pytest.raises(ValueError, match="nondecreasing week_index"):
            compute_week_boundaries(
                WEEK2_PLAYS + WEEK1_PLAYS,
                TEAMS,
                target_week_indices=[3],
            )

    def test_repeated_calls_are_deterministic_and_do_not_mutate_inputs(self):
        teams = list(TEAMS)
        plays = WEEK1_PLAYS + WEEK2_PLAYS
        targets = [3, 2, 3]
        expected_teams = list(teams)
        expected_plays = list(plays)
        expected_targets = list(targets)

        first = compute_week_boundaries(plays, teams, target_week_indices=targets, season=2024)
        second = compute_week_boundaries(plays, teams, target_week_indices=targets, season=2024)

        assert first == second
        assert teams == expected_teams
        assert plays == expected_plays
        assert targets == expected_targets
