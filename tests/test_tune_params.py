"""Unit tests for scripts/tune_params.py (no DB, no I/O).

Covers grid construction (full + --quick sizes), the MAE/ATS scoring helpers
against the compute_predictions.compute_edge ATS-cover convention, the
in-memory Elo replay against a directly-driven EloEngine, the multi-lambda
week-boundary solve (single season-pass, multiple lambdas from the same
accumulated state), and MAE-based ranking. Everything here is pure
Python/numpy; nothing touches Postgres. Fixture/style mirrors
tests/test_house_elo.py and tests/test_adjusted_epa_week.py.
"""

import math

import pytest

pytest.importorskip("numpy")

from scripts.compute_adjusted_epa import LAMBDA, RidgeAccumulator  # noqa: E402
from scripts.compute_house_elo import EloEngine  # noqa: E402
from scripts.compute_predictions import BLEND_ELO, compute_edge  # noqa: E402
from scripts.tune_params import (  # noqa: E402
    BASELINE_DIVISOR,
    BASELINE_HFA,
    BASELINE_K,
    BASELINE_LAMBDA,
    BASELINE_WEIGHT,
    MIN_EPA_PLAYS,
    ats_hit_rate,
    ats_result,
    blend_margin_weighted,
    build_epa_index,
    build_scoring_games,
    compute_week_boundaries_multi_lambda,
    elo_grid,
    game_week_index,
    lambda_grid,
    lookup_epa_boundary,
    mean_abs_error,
    rank_baseline,
    rank_by_mae,
    replay_elo,
    score_margins,
    weight_grid,
)


def make_game(
    game_id,
    season,
    home_team,
    away_team,
    home_points,
    away_points,
    week=1,
    season_type="regular",
    neutral_site=False,
):
    """Engine-shaped game dict (matches compute_house_elo.to_engine_game's
    output -- the shape replay_elo's games_by_season values must have)."""
    return {
        "game_id": game_id,
        "season": season,
        "week": week,
        "season_type": season_type,
        "start_date": None,
        "neutral_site": neutral_site,
        "home_team": home_team,
        "away_team": away_team,
        "home_points": home_points,
        "away_points": away_points,
        "cfbd_home_pregame_elo": None,
        "cfbd_away_pregame_elo": None,
    }


# =============================================================================
# 1. Grid construction.
# =============================================================================


class TestGridConstruction:
    def test_full_elo_grid_has_36_distinct_combos(self):
        combos = elo_grid(quick=False)
        assert len(combos) == 36
        assert len(set(combos)) == 36
        ks = {c[0] for c in combos}
        divisors = {c[1] for c in combos}
        hfas = {c[2] for c in combos}
        assert ks == {16.0, 20.0, 24.0, 28.0}
        assert divisors == {22.0, 25.0, 28.0}
        assert hfas == {55.0, 65.0, 75.0}

    def test_quick_elo_grid_has_8_distinct_combos(self):
        combos = elo_grid(quick=True)
        assert len(combos) == 8
        assert len(set(combos)) == 8

    def test_full_lambda_and_weight_grids(self):
        assert lambda_grid(quick=False) == [100.0, 200.0, 400.0]
        assert weight_grid(quick=False) == [0.5, 0.6, 0.7]

    def test_quick_lambda_and_weight_grids_shrink_to_two(self):
        assert len(lambda_grid(quick=True)) == 2
        assert len(weight_grid(quick=True)) == 2
        # quick grids are subsets of the full grids, not disjoint values.
        assert set(lambda_grid(quick=True)) <= set(lambda_grid(quick=False))
        assert set(weight_grid(quick=True)) <= set(weight_grid(quick=False))

    def test_baseline_ledger_values_are_members_of_the_full_grid(self):
        # TUNE_BASELINE's design assumes the current ledger's config is a
        # literal point in the swept grid -- guard that assumption here.
        assert (BASELINE_K, BASELINE_DIVISOR, BASELINE_HFA) in elo_grid(quick=False)
        assert BASELINE_LAMBDA in lambda_grid(quick=False)
        assert BASELINE_WEIGHT in weight_grid(quick=False)
        # And they match the live production defaults, not hardcoded copies.
        assert BASELINE_K == EloEngine.K == 28.0
        assert BASELINE_DIVISOR == EloEngine.DIVISOR == 22.0
        assert BASELINE_HFA == EloEngine.HFA == 65.0
        assert BASELINE_LAMBDA == LAMBDA == 100.0
        assert BASELINE_WEIGHT == BLEND_ELO == 0.6


# =============================================================================
# 2. MAE / ATS scoring helpers -- against compute_predictions.compute_edge's
#    edge/pick convention and the ATS-cover convention (marts/038).
# =============================================================================


class TestMeanAbsError:
    def test_known_values(self):
        pairs = [(10.0, 7.0), (3.0, 3.0), (-2.0, 2.0)]
        mae, n = mean_abs_error(pairs)
        assert n == 3
        assert mae == pytest.approx((3.0 + 0.0 + 4.0) / 3.0)

    def test_empty_input_is_nan_with_zero_n(self):
        mae, n = mean_abs_error([])
        assert n == 0
        assert math.isnan(mae)


class TestAtsResult:
    """ats_result's cover convention must agree with compute_edge's pick."""

    def test_home_pick_that_covers_wins(self):
        _edge, pick = compute_edge(10.0, -3.0)
        assert pick == "home"
        # actual_home_margin=5, spread=-3 -> cover_margin = 5 - 3 = 2 > 0.
        assert ats_result(pick, 5.0, -3.0) == "win"

    def test_home_pick_that_fails_to_cover_loses(self):
        _edge, pick = compute_edge(10.0, -3.0)
        assert pick == "home"
        # actual_home_margin=1, spread=-3 -> cover_margin = 1 - 3 = -2 < 0.
        assert ats_result(pick, 1.0, -3.0) == "loss"

    def test_away_pick_that_covers_wins(self):
        _edge, pick = compute_edge(-10.0, -3.0)
        assert pick == "away"
        # actual_home_margin=-8, spread=-3 -> cover_margin = -11 < 0 -> away covers.
        assert ats_result(pick, -8.0, -3.0) == "win"

    def test_away_pick_that_fails_to_cover_loses(self):
        _edge, pick = compute_edge(-10.0, -3.0)
        assert pick == "away"
        # actual_home_margin=6, spread=-3 -> cover_margin = 3 > 0 -> home covers.
        assert ats_result(pick, 6.0, -3.0) == "loss"

    def test_exact_push(self):
        assert ats_result("home", 3.0, -3.0) == "push"
        assert ats_result("away", 3.0, -3.0) == "push"


class TestAtsHitRate:
    def test_threshold_filters_and_pushes_excluded(self):
        graded = [
            # |edge|=5 >= 3: home pick, cover_margin=10-3=7>0 -> win.
            {"edge": 5.0, "edge_pick": "home", "actual_home_margin": 10.0, "market_spread": -3.0},
            # |edge|=2 < 3: excluded by threshold.
            {"edge": 2.0, "edge_pick": "home", "actual_home_margin": 10.0, "market_spread": -3.0},
            # |edge|=4 >= 3: away pick, cover_margin=-1-3=-4<0 -> away covers -> win.
            {"edge": -4.0, "edge_pick": "away", "actual_home_margin": -1.0, "market_spread": -3.0},
            # push (cover_margin=0): excluded regardless of |edge|.
            {"edge": 4.0, "edge_pick": "home", "actual_home_margin": -4.0, "market_spread": 4.0},
            # no market: excluded.
            {"edge": None, "edge_pick": None, "actual_home_margin": 3.0, "market_spread": None},
        ]
        hit_rate, n = ats_hit_rate(graded, 3.0)
        assert n == 2
        assert hit_rate == pytest.approx(1.0)

    def test_no_qualifying_games_returns_none(self):
        hit_rate, n = ats_hit_rate([], 3.0)
        assert hit_rate is None
        assert n == 0


class TestScoreMargins:
    def test_mae_over_all_rows_ats_over_qualifying_rows(self):
        rows = [
            # edge = 10 + (-3) = 7 -> home pick; cover_margin = 12-3=9>0 -> win.
            {"expected": 10.0, "actual": 12.0, "market_spread": -3.0},
            # edge = 1 + (-3) = -2 -> away pick; |edge| below both thresholds.
            {"expected": 1.0, "actual": -1.0, "market_spread": -3.0},
        ]
        metrics = score_margins(rows)
        assert metrics["n"] == 2
        assert metrics["mae"] == pytest.approx((2.0 + 2.0) / 2.0)
        assert metrics["ats3"] == pytest.approx(1.0)
        assert metrics["ats3_n"] == 1
        assert metrics["ats6"] == pytest.approx(1.0)
        assert metrics["ats6_n"] == 1

    def test_no_market_gives_none_ats_but_still_scores_mae(self):
        rows = [{"expected": 5.0, "actual": 3.0, "market_spread": None}]
        metrics = score_margins(rows)
        assert metrics["mae"] == pytest.approx(2.0)
        assert metrics["n"] == 1
        assert metrics["ats3"] is None
        assert metrics["ats6"] is None


class TestBlendMarginWeighted:
    def test_weighted_average(self):
        assert blend_margin_weighted(10.0, 4.0, 0.6) == pytest.approx(0.6 * 10.0 + 0.4 * 4.0)

    def test_weight_one_is_elo_only(self):
        assert blend_margin_weighted(7.0, 999.0, 1.0) == pytest.approx(7.0)

    def test_weight_zero_is_epa_only(self):
        assert blend_margin_weighted(7.0, 999.0, 0.0) == pytest.approx(999.0)


class TestGameWeekIndex:
    def test_regular_season_is_passthrough(self):
        assert game_week_index(5, "regular") == 5

    def test_postseason_offsets_by_100(self):
        assert game_week_index(1, "postseason") == 101


# =============================================================================
# 3. In-memory Elo replay matches EloEngine driven directly.
# =============================================================================


class TestReplayEloMatchesEngine:
    def test_replay_matches_manual_engine_drive(self):
        games_by_season = {
            2020: [
                make_game(1, 2020, "A", "B", 30, 10, week=1),
                make_game(2, 2020, "C", "A", 14, 21, week=2),
                make_game(3, 2020, "B", "C", 17, 17, week=3),
            ]
        }
        scheduled_counts = {2020: {"A": 4, "B": 4, "C": 4}}

        replayed = replay_elo(
            games_by_season, scheduled_counts, EloEngine.K, EloEngine.DIVISOR, EloEngine.HFA
        )

        manual = EloEngine()
        manual.start_season(2020, {"A": 4, "B": 4, "C": 4})
        expected_rows = {g["game_id"]: manual.process_game(g) for g in games_by_season[2020]}

        assert set(replayed) == set(expected_rows)
        for gid, expected_row in expected_rows.items():
            for field in (
                "home_pregame_elo",
                "away_pregame_elo",
                "home_postgame_elo",
                "away_postgame_elo",
                "expected_home_margin",
                "home_win_prob",
            ):
                assert replayed[gid][field] == pytest.approx(expected_row[field])

    def test_overridden_divisor_and_hfa_change_expected_margin(self):
        games_by_season = {2020: [make_game(1, 2020, "A", "B", 30, 10, week=1)]}
        scheduled_counts = {2020: {"A": 4, "B": 4}}

        baseline = replay_elo(games_by_season, scheduled_counts, 20.0, 25.0, 65.0)
        alt = replay_elo(games_by_season, scheduled_counts, 20.0, 22.0, 55.0)

        # Both teams start at SEED (equal ratings): expected_home_margin = HFA/DIVISOR.
        assert baseline[1]["expected_home_margin"] == pytest.approx(65.0 / 25.0)
        assert alt[1]["expected_home_margin"] == pytest.approx(55.0 / 22.0)
        assert baseline[1]["expected_home_margin"] != pytest.approx(alt[1]["expected_home_margin"])

    def test_seasons_are_replayed_in_chronological_order_not_dict_order(self):
        games_by_season = {
            2021: [make_game(10, 2021, "A", "B", 20, 10, week=1)],
            2020: [make_game(1, 2020, "A", "B", 30, 10, week=1)],
        }
        scheduled_counts = {2020: {"A": 4, "B": 4}, 2021: {"A": 4, "B": 4}}
        rows = replay_elo(games_by_season, scheduled_counts, 20.0, 25.0, 65.0)
        # 2021's pregame rating must carry 2020's postgame rating forward
        # (through one season of carryover regression toward SEED) -- proving
        # 2020 was replayed BEFORE 2021 despite the dict's insertion order.
        expected_2021_pregame = (
            EloEngine.SEED + (rows[1]["home_postgame_elo"] - EloEngine.SEED) * EloEngine.CARRYOVER
        )
        assert rows[10]["home_pregame_elo"] == pytest.approx(expected_2021_pregame)


# =============================================================================
# 4. Multi-lambda week-boundary solve: same accumulated state, N lambdas.
# =============================================================================

TEAMS = ["Alpha", "Bravo", "Charlie", "Delta"]

WEEK1_PLAYS = [
    ("Alpha", "Bravo", True, 0.40, 1),
    ("Bravo", "Alpha", False, -0.10, 1),
    ("Charlie", "Delta", True, 0.20, 1),
    ("Delta", "Charlie", False, -0.30, 1),
]
WEEK2_PLAYS = [
    ("Delta", "Alpha", True, 0.15, 2),
    ("Charlie", "Bravo", False, -0.05, 2),
]
ALL_PLAYS = WEEK1_PLAYS + WEEK2_PLAYS


def _strip_week_index(plays):
    return [(off, deff, home, epa) for off, deff, home, epa, _wi in plays]


class TestMultiLambdaBoundary:
    def test_two_lambdas_yield_different_coefficients(self):
        boundaries = compute_week_boundaries_multi_lambda(
            ALL_PLAYS, TEAMS, [100.0, 500.0], season=2024
        )
        entering_week2_100 = {r["team"]: r for r in boundaries[100.0] if r["week_index"] == 2}
        entering_week2_500 = {r["team"]: r for r in boundaries[500.0] if r["week_index"] == 2}

        differs = any(
            entering_week2_100[team]["off_coef"]
            != pytest.approx(entering_week2_500[team]["off_coef"])
            for team in TEAMS
        )
        assert differs

    def test_matches_independent_ridge_accumulator_solve_per_lambda(self):
        boundaries = compute_week_boundaries_multi_lambda(
            ALL_PLAYS, TEAMS, [100.0, 500.0], season=2024
        )
        reference = RidgeAccumulator(TEAMS)
        reference.add_plays(_strip_week_index(WEEK1_PLAYS))

        for lam in (100.0, 500.0):
            _mu, _hfa, off_coef, def_coef, _n = reference.solve(lam)
            entering_week2 = {r["team"]: r for r in boundaries[lam] if r["week_index"] == 2}
            for team in TEAMS:
                assert entering_week2[team]["off_coef"] == pytest.approx(off_coef[team])
                assert entering_week2[team]["def_coef"] == pytest.approx(def_coef[team])
                assert entering_week2[team]["lambda"] == lam

    def test_same_lambda_across_separate_calls_is_deterministic(self):
        boundaries_a = compute_week_boundaries_multi_lambda(ALL_PLAYS, TEAMS, [200.0], season=2024)
        boundaries_b = compute_week_boundaries_multi_lambda(ALL_PLAYS, TEAMS, [200.0], season=2024)
        assert boundaries_a == boundaries_b

    def test_no_boundary_before_the_first_week(self):
        boundaries = compute_week_boundaries_multi_lambda(ALL_PLAYS, TEAMS, [100.0], season=2024)
        week_indices = {r["week_index"] for r in boundaries[100.0]}
        assert 1 not in week_indices
        assert week_indices == {2}


class TestEpaIndexLookup:
    ROWS = [
        {"team": "A", "season": 2020, "week_index": 2, "plays": 200, "off_coef": 0.1},
        {"team": "A", "season": 2020, "week_index": 3, "plays": 50, "off_coef": 0.15},  # thin
        {"team": "A", "season": 2020, "week_index": 5, "plays": 400, "off_coef": 0.2},
    ]

    def test_min_plays_default_matches_module_constant(self):
        assert MIN_EPA_PLAYS == 150

    def test_greatest_qualifying_boundary_at_or_below_week(self):
        idx = build_epa_index(self.ROWS)
        row = lookup_epa_boundary(idx, "A", 2020, 4)
        # Week 3's boundary has plays=50 < MIN_EPA_PLAYS, so week 2 wins.
        assert row["week_index"] == 2

    def test_exact_match_boundary_is_used(self):
        idx = build_epa_index(self.ROWS)
        row = lookup_epa_boundary(idx, "A", 2020, 5)
        assert row["week_index"] == 5

    def test_before_any_boundary_returns_none(self):
        idx = build_epa_index(self.ROWS)
        assert lookup_epa_boundary(idx, "A", 2020, 1) is None

    def test_missing_team_or_season_returns_none(self):
        idx = build_epa_index(self.ROWS)
        assert lookup_epa_boundary(idx, "Nobody", 2020, 10) is None
        assert lookup_epa_boundary(idx, "A", 1999, 10) is None


class TestBuildScoringGames:
    def test_filters_window_and_computes_margin_and_week_index(self):
        games_by_season = {
            2019: [make_game(1, 2019, "A", "B", 20, 10)],
            2020: [
                make_game(2, 2020, "A", "B", 14, 21, week=1, season_type="postseason"),
            ],
        }
        scoring = build_scoring_games(games_by_season, 2020, 2020)
        assert len(scoring) == 1
        row = scoring[0]
        assert row["game_id"] == 2
        assert row["actual_home_margin"] == -7
        assert row["week_index"] == 101


# =============================================================================
# 5. Ranking: ordered by MAE.
# =============================================================================


class TestRankByMae:
    def test_orders_ascending_by_mae(self):
        results = [
            {"mae": 5.0, "id": "c"},
            {"mae": 1.0, "id": "a"},
            {"mae": 3.0, "id": "b"},
        ]
        ranked = rank_by_mae(results)
        assert [r["id"] for r in ranked] == ["a", "b", "c"]

    def test_nan_mae_sorts_last(self):
        results = [
            {"mae": float("nan"), "id": "bad"},
            {"mae": 2.0, "id": "good"},
        ]
        ranked = rank_by_mae(results)
        assert [r["id"] for r in ranked] == ["good", "bad"]


class TestRankBaseline:
    def test_rank_and_total(self):
        rank, total = rank_baseline(3.0, [1.0, 2.0, 4.0, 5.0])
        assert rank == 3  # 1.0 and 2.0 beat 3.0
        assert total == 5

    def test_best_baseline_ranks_first(self):
        rank, total = rank_baseline(0.5, [1.0, 2.0])
        assert rank == 1
        assert total == 3

    def test_worst_baseline_ranks_last(self):
        rank, total = rank_baseline(10.0, [1.0, 2.0])
        assert rank == 3
        assert total == 3
