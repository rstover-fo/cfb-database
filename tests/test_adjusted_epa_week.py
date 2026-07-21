"""Unit tests for the walk-forward per-week ridge-adjusted EPA boundary walker (no DB).

Covers scripts/compute_adjusted_epa_week.py's compute_week_boundaries: boundary
semantics (coefficients entering week W depend only on strictly-earlier
week_index plays), boundary counting / no-boundary-before-the-first-week,
week_index postseason ordering (the entering-postseason boundary reflects the
full regular season), per-team offensive play counts at each boundary, and
determinism of the pure function. RidgeAccumulator and its LAMBDA penalty are
imported from scripts.compute_adjusted_epa, not re-implemented, per
docs/plans/2026-07-21-tier3-analytics-plan.md, Pillar A. Fixture/style mirrors
tests/test_adjusted_epa.py.
"""

import pytest

pytest.importorskip("numpy")

from scripts.compute_adjusted_epa import LAMBDA, RidgeAccumulator  # noqa: E402
from scripts.compute_adjusted_epa_week import compute_week_boundaries  # noqa: E402

TEAMS = ["Alpha", "Bravo", "Charlie", "Delta"]

# --- Regular-season fixture: 3 weeks, week_index == week (season_type='regular') ---
# (off_team, def_team, is_home_offense, epa, week_index)

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

REGULAR_SEASON_PLAYS = WEEK1_PLAYS + WEEK2_PLAYS + WEEK3_PLAYS


def _strip_week_index(plays):
    return [(off, deff, home, epa) for off, deff, home, epa, _week_index in plays]


class TestBoundarySemantics:
    """Coefficients entering week W must reflect exactly the plays with week_index < W."""

    def test_entering_week2_matches_week1_only_fit(self):
        boundaries = compute_week_boundaries(REGULAR_SEASON_PLAYS, TEAMS, lam=LAMBDA, season=2024)
        entering_week2 = {r["team"]: r for r in boundaries if r["week_index"] == 2}

        reference = RidgeAccumulator(TEAMS)
        reference.add_plays(_strip_week_index(WEEK1_PLAYS))
        mu, hfa, off_coef, def_coef, _n = reference.solve(LAMBDA)

        assert set(entering_week2) == set(TEAMS)
        for team in TEAMS:
            assert entering_week2[team]["off_coef"] == pytest.approx(off_coef[team])
            assert entering_week2[team]["def_coef"] == pytest.approx(def_coef[team])
            assert entering_week2[team]["mu"] == pytest.approx(mu)
            assert entering_week2[team]["hfa_coef"] == pytest.approx(hfa)
            assert entering_week2[team]["season"] == 2024
            assert entering_week2[team]["lambda"] == LAMBDA
            assert entering_week2[team]["n_teams"] == len(TEAMS)

    def test_entering_week3_matches_week1_and_week2_fit(self):
        boundaries = compute_week_boundaries(REGULAR_SEASON_PLAYS, TEAMS, lam=LAMBDA)
        entering_week3 = {r["team"]: r for r in boundaries if r["week_index"] == 3}

        reference = RidgeAccumulator(TEAMS)
        reference.add_plays(_strip_week_index(WEEK1_PLAYS + WEEK2_PLAYS))
        _mu, _hfa, off_coef, def_coef, _n = reference.solve(LAMBDA)

        for team in TEAMS:
            assert entering_week3[team]["off_coef"] == pytest.approx(off_coef[team])
            assert entering_week3[team]["def_coef"] == pytest.approx(def_coef[team])

    def test_entering_week3_is_not_affected_by_week3_plays(self):
        # Perturbing week 3's plays must not change the week-3 boundary: it is
        # solved BEFORE week 3's plays are folded into the accumulator.
        perturbed = WEEK1_PLAYS + WEEK2_PLAYS + [("Delta", "Bravo", True, 999.0, 3)] + WEEK3_PLAYS
        boundaries_a = compute_week_boundaries(REGULAR_SEASON_PLAYS, TEAMS, lam=LAMBDA)
        boundaries_b = compute_week_boundaries(perturbed, TEAMS, lam=LAMBDA)

        entering_week3_a = {r["team"]: r for r in boundaries_a if r["week_index"] == 3}
        entering_week3_b = {r["team"]: r for r in boundaries_b if r["week_index"] == 3}
        for team in TEAMS:
            assert entering_week3_a[team]["off_coef"] == pytest.approx(
                entering_week3_b[team]["off_coef"]
            )
            assert entering_week3_a[team]["def_coef"] == pytest.approx(
                entering_week3_b[team]["def_coef"]
            )


class TestBoundaryCounting:
    """No boundary before the first week; one boundary per later week_index transition."""

    def test_no_boundary_before_first_week(self):
        boundaries = compute_week_boundaries(REGULAR_SEASON_PLAYS, TEAMS, lam=LAMBDA)
        week_indices = {r["week_index"] for r in boundaries}
        assert 1 not in week_indices

    def test_boundaries_are_exactly_entering_week2_and_week3(self):
        # weeks [1, 2, 3] -> boundaries entering 2 and 3 only (not 1): a
        # boundary is emitted at each week_index transition AFTER at least
        # one play has already been accumulated.
        boundaries = compute_week_boundaries(REGULAR_SEASON_PLAYS, TEAMS, lam=LAMBDA)
        week_indices = {r["week_index"] for r in boundaries}
        assert week_indices == {2, 3}

    def test_row_count_is_teams_times_boundary_count(self):
        boundaries = compute_week_boundaries(REGULAR_SEASON_PLAYS, TEAMS, lam=LAMBDA)
        assert len(boundaries) == len(TEAMS) * 2

    def test_single_week_season_emits_no_boundaries(self):
        boundaries = compute_week_boundaries(WEEK1_PLAYS, TEAMS, lam=LAMBDA)
        assert boundaries == []

    def test_empty_play_list_emits_no_boundaries(self):
        boundaries = compute_week_boundaries([], TEAMS, lam=LAMBDA)
        assert boundaries == []


class TestPostseasonOrdering:
    """week_index, not raw CFBD week, orders postseason after every regular week.

    (week=1, 'postseason') -> week_index=101 sorts after (week=13, 'regular')
    -> week_index=13, so the entering-postseason boundary (week_index=101)
    must reflect the ENTIRE regular season, including the final regular week.
    """

    WEEK1 = [
        ("Alpha", "Bravo", True, 0.40, 1),
        ("Bravo", "Alpha", False, -0.10, 1),
        ("Charlie", "Delta", True, 0.20, 1),
        ("Delta", "Charlie", False, -0.30, 1),
    ]
    WEEK13 = [
        ("Delta", "Alpha", True, 0.15, 13),
        ("Charlie", "Bravo", False, -0.05, 13),
        ("Alpha", "Delta", False, 0.25, 13),
    ]
    # Bowl game: raw CFBD week=1, season_type='postseason' -> week_index = 100 + 1 = 101.
    POSTSEASON_WEEK1 = [
        ("Bravo", "Delta", True, 0.50, 101),
        ("Delta", "Bravo", False, -0.15, 101),
    ]

    def test_entering_postseason_boundary_is_full_regular_season(self):
        plays_in_week_index_order = self.WEEK1 + self.WEEK13 + self.POSTSEASON_WEEK1
        boundaries = compute_week_boundaries(plays_in_week_index_order, TEAMS, lam=LAMBDA)

        entering_postseason = {r["team"]: r for r in boundaries if r["week_index"] == 101}
        assert set(entering_postseason) == set(TEAMS)

        reference = RidgeAccumulator(TEAMS)
        reference.add_plays(_strip_week_index(self.WEEK1 + self.WEEK13))
        _mu, _hfa, off_coef, def_coef, _n = reference.solve(LAMBDA)

        for team in TEAMS:
            assert entering_postseason[team]["off_coef"] == pytest.approx(off_coef[team])
            assert entering_postseason[team]["def_coef"] == pytest.approx(def_coef[team])

    def test_boundaries_are_entering_week13_and_entering_postseason(self):
        plays_in_week_index_order = self.WEEK1 + self.WEEK13 + self.POSTSEASON_WEEK1
        boundaries = compute_week_boundaries(plays_in_week_index_order, TEAMS, lam=LAMBDA)
        week_indices = {r["week_index"] for r in boundaries}
        assert week_indices == {13, 101}


class TestPerTeamPlayCounts:
    """`plays` at a boundary = that team's offensive plays folded so far."""

    def test_plays_counter_matches_week1_offensive_counts(self):
        boundaries = compute_week_boundaries(REGULAR_SEASON_PLAYS, TEAMS, lam=LAMBDA)
        entering_week2 = {r["team"]: r for r in boundaries if r["week_index"] == 2}

        expected_off_plays = {"Alpha": 2, "Bravo": 2, "Charlie": 1, "Delta": 1}
        for team, expected in expected_off_plays.items():
            assert entering_week2[team]["plays"] == expected

    def test_plays_counter_accumulates_across_weeks(self):
        boundaries = compute_week_boundaries(REGULAR_SEASON_PLAYS, TEAMS, lam=LAMBDA)
        entering_week3 = {r["team"]: r for r in boundaries if r["week_index"] == 3}

        # week1 + week2 offensive play counts, by team.
        expected_off_plays = {"Alpha": 3, "Bravo": 3, "Charlie": 2, "Delta": 2}
        for team, expected in expected_off_plays.items():
            assert entering_week3[team]["plays"] == expected

    def test_team_with_zero_plays_so_far_reports_zero(self):
        # A team present in the season-wide layout but absent from week 1's
        # plays must still get a boundary row, with plays=0.
        plays = [
            ("Alpha", "Bravo", True, 0.40, 1),
            ("Bravo", "Alpha", False, -0.10, 1),
            ("Charlie", "Delta", True, 0.0, 2),
        ]
        boundaries = compute_week_boundaries(plays, TEAMS, lam=LAMBDA)
        entering_week2 = {r["team"]: r for r in boundaries if r["week_index"] == 2}
        assert entering_week2["Charlie"]["plays"] == 0
        assert entering_week2["Delta"]["plays"] == 0


class TestDeterminism:
    """The pure boundary walker is deterministic and does not mutate its inputs."""

    def test_same_input_yields_identical_output(self):
        boundaries_a = compute_week_boundaries(REGULAR_SEASON_PLAYS, TEAMS, lam=LAMBDA, season=2024)
        boundaries_b = compute_week_boundaries(REGULAR_SEASON_PLAYS, TEAMS, lam=LAMBDA, season=2024)
        assert boundaries_a == boundaries_b

    def test_repeated_calls_do_not_mutate_shared_inputs(self):
        teams_copy = list(TEAMS)
        plays_copy = list(REGULAR_SEASON_PLAYS)
        compute_week_boundaries(REGULAR_SEASON_PLAYS, TEAMS, lam=LAMBDA)
        assert TEAMS == teams_copy
        assert REGULAR_SEASON_PLAYS == plays_copy
