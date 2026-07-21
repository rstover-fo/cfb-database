"""Unit tests for compute_predictions's pure prediction math (no DB, no I/O).

Covers elo_margin/epa_margin/blend_margin/elo_win_prob/compute_edge against
hand-computed values, plus the small lookup helpers (carryover_rating,
resolve_elo, lookup_epa_coefs) and the build_predictions_for_game contract
(two model_version rows per game, elo_v1's epa_margin always NULL), per
docs/plans/2026-07-21-tier2-analytics-plan.md's "Expected margin, win prob,
edge" design.
"""

import pytest

from scripts.compute_house_elo import EloEngine, expected_score
from scripts.compute_predictions import (
    BLEND_ELO,
    BLEND_EPA,
    MODEL_BLEND,
    MODEL_ELO,
    blend_margin,
    build_predictions_for_game,
    carryover_rating,
    compute_edge,
    compute_week_index,
    elo_margin,
    elo_win_prob,
    epa_margin,
    lookup_epa_coefs,
    lookup_epa_coefs_asof,
    resolve_elo,
)


class TestEloMargin:
    def test_equal_ratings_non_neutral(self):
        # (1500 - 1500) / 25 + 65 / 25 = 2.6
        assert elo_margin(1500, 1500, neutral=False) == pytest.approx(2.6)

    def test_equal_ratings_neutral(self):
        assert elo_margin(1500, 1500, neutral=True) == pytest.approx(0.0)

    def test_home_favorite_scales_with_diff(self):
        # (1600 - 1500 + 65) / 25 = 6.6
        assert elo_margin(1600, 1500, neutral=False) == pytest.approx(6.6)


class TestEpaMargin:
    """off_h=0.10, def_h=-0.05, off_a=0.08, def_a=-0.02, hfa_coef=0.03.

    margin = ((off_h + def_a) - (off_a + def_h)) * 68 + hfa_coef * 68 * (not neutral)
           = ((0.10 + -0.02) - (0.08 + -0.05)) * 68 + 0.03 * 68
           = (0.08 - 0.03) * 68 + 2.04
           = 0.05 * 68 + 2.04 = 3.4 + 2.04 = 5.44
    """

    def test_hand_computed_non_neutral(self):
        result = epa_margin(
            off_h=0.10, def_h=-0.05, off_a=0.08, def_a=-0.02, hfa_coef=0.03, neutral=False
        )
        assert result == pytest.approx(5.44)

    def test_neutral_zeroes_hfa_term(self):
        result = epa_margin(
            off_h=0.10, def_h=-0.05, off_a=0.08, def_a=-0.02, hfa_coef=0.03, neutral=True
        )
        assert result == pytest.approx(3.4)

    def test_defense_sign_handling(self):
        # A stingier (more negative) home defense should RAISE the home
        # margin (def_h is subtracted); a stingier away defense should
        # LOWER it (def_a is added).
        base = epa_margin(off_h=0.0, def_h=0.0, off_a=0.0, def_a=0.0, hfa_coef=0.0, neutral=True)
        better_home_def = epa_margin(
            off_h=0.0, def_h=-0.05, off_a=0.0, def_a=0.0, hfa_coef=0.0, neutral=True
        )
        better_away_def = epa_margin(
            off_h=0.0, def_h=0.0, off_a=0.0, def_a=-0.05, hfa_coef=0.0, neutral=True
        )
        assert better_home_def > base
        assert better_away_def < base


class TestBlendMargin:
    def test_exact_weights(self):
        assert BLEND_ELO == pytest.approx(0.6)
        assert BLEND_EPA == pytest.approx(0.4)
        assert blend_margin(10.0, 5.0) == pytest.approx(0.6 * 10.0 + 0.4 * 5.0)
        assert blend_margin(10.0, 5.0) == pytest.approx(8.0)

    def test_none_epa_passes_through_elo(self):
        assert blend_margin(10.0, None) == pytest.approx(10.0)
        assert blend_margin(-3.5, None) == pytest.approx(-3.5)


class TestEloWinProb:
    def test_equal_ratings_neutral_is_half(self):
        assert elo_win_prob(1500, 1500, neutral=True) == pytest.approx(0.5)

    def test_equal_ratings_non_neutral_favors_home(self):
        assert elo_win_prob(1500, 1500, neutral=False) > 0.5

    def test_matches_expected_score_with_hfa(self):
        assert elo_win_prob(1500, 1500, neutral=False) == pytest.approx(expected_score(65))
        assert elo_win_prob(1600, 1500, neutral=True) == pytest.approx(expected_score(100))


class TestComputeEdge:
    def test_home_favored_market_underestimates_home(self):
        # expected +7 (house: home by 7), spread -3 (market: home favored by
        # 3, market_home_margin = +3) -> edge = 7 + (-3) = 4 -> pick home.
        edge, pick = compute_edge(7, -3)
        assert edge == pytest.approx(4)
        assert pick == "home"

    def test_home_underdog_but_house_more_bullish_than_market(self):
        # expected +1 (house: home by 1), spread +3 (market: home a 3-point
        # underdog, market_home_margin = -3) -> edge = 1 + 3 = 4 -> pick home.
        edge, pick = compute_edge(1, 3)
        assert edge == pytest.approx(4)
        assert pick == "home"

    def test_away_side_edge(self):
        # expected -10 (house: away by 10), spread -3 (market: home favored
        # by 3) -> edge = -10 + (-3) = -13 -> pick away.
        edge, pick = compute_edge(-10, -3)
        assert edge == pytest.approx(-13)
        assert pick == "away"

    def test_zero_edge_picks_home(self):
        edge, pick = compute_edge(3, -3)
        assert edge == pytest.approx(0)
        assert pick == "home"

    def test_no_market_returns_none_none(self):
        assert compute_edge(7, None) == (None, None)


class TestCarryoverRating:
    def test_no_gap_is_passthrough(self):
        assert carryover_rating(1700.0, elapsed=0) == pytest.approx(1700.0)
        assert carryover_rating(1700.0, elapsed=-1) == pytest.approx(1700.0)

    def test_one_season_gap_matches_engine_formula(self):
        assert carryover_rating(1700.0, elapsed=1) == pytest.approx(1500 + 200 * (2 / 3))

    def test_two_season_gap_matches_engine_formula(self):
        assert carryover_rating(1700.0, elapsed=2) == pytest.approx(1500 + 200 * (2 / 3) ** 2)


class TestResolveElo:
    def test_missing_team_defaults_to_seed(self):
        assert resolve_elo("Nobody", 2024, {}) == pytest.approx(EloEngine.SEED)

    def test_current_season_snapshot_used_as_is(self):
        elo_current = {"Big State": (1650.0, 2024)}
        assert resolve_elo("Big State", 2024, elo_current) == pytest.approx(1650.0)

    def test_stale_snapshot_gets_carryover(self):
        elo_current = {"Big State": (1700.0, 2019)}
        assert resolve_elo("Big State", 2020, elo_current) == pytest.approx(1500 + 200 * (2 / 3))


class TestLookupEpaCoefs:
    ROW_2023 = {"off_coef": 0.1, "def_coef": -0.05, "hfa_coef": 0.02}
    ROW_2024 = {"off_coef": 0.2, "def_coef": -0.03, "hfa_coef": 0.02}

    def build_epa(self):
        return {("Big State", 2023): self.ROW_2023, ("Big State", 2024): self.ROW_2024}

    def test_prefers_current_season(self):
        epa = self.build_epa()
        assert lookup_epa_coefs(epa, "Big State", 2024, lookback=1) == self.ROW_2024

    def test_falls_back_to_previous_season_within_lookback(self):
        epa = {("Big State", 2023): self.ROW_2023}
        assert lookup_epa_coefs(epa, "Big State", 2024, lookback=1) == self.ROW_2023

    def test_backfill_lookback_zero_is_same_season_only(self):
        epa = {("Big State", 2023): self.ROW_2023}
        assert lookup_epa_coefs(epa, "Big State", 2024, lookback=0) is None

    def test_missing_team_returns_none(self):
        epa = self.build_epa()
        assert lookup_epa_coefs(epa, "Tiny College", 2024, lookback=1) is None


class TestBuildPredictionsForGame:
    GAME = {
        "game_id": 1,
        "season": 2024,
        "week": 5,
        "season_type": "regular",
        "home_team": "Home U",
        "away_team": "Away Tech",
        "neutral_site": False,
    }

    def test_two_rows_one_per_model(self):
        rows = build_predictions_for_game(self.GAME, 1600, 1500, {}, epa_lookback=1, market=None)
        assert {r["model_version"] for r in rows} == {MODEL_ELO, MODEL_BLEND}

    def test_elo_row_epa_margin_always_none(self):
        epa = {
            ("Home U", 2024): {"off_coef": 0.1, "def_coef": -0.05, "hfa_coef": 0.02},
            ("Away Tech", 2024): {"off_coef": 0.05, "def_coef": -0.02, "hfa_coef": 0.02},
        }
        game_rows = build_predictions_for_game(
            self.GAME, 1600, 1500, epa, epa_lookback=1, market=None
        )
        rows = {r["model_version"]: r for r in game_rows}
        assert rows[MODEL_ELO]["epa_margin"] is None
        assert rows[MODEL_ELO]["expected_home_margin"] == pytest.approx(
            elo_margin(1600, 1500, neutral=False)
        )
        assert rows[MODEL_BLEND]["epa_margin"] is not None
        assert rows[MODEL_BLEND]["expected_home_margin"] != rows[MODEL_ELO]["expected_home_margin"]

    def test_missing_epa_falls_back_blend_to_elo_only(self):
        game_rows = build_predictions_for_game(
            self.GAME, 1600, 1500, {}, epa_lookback=1, market=None
        )
        rows = {r["model_version"]: r for r in game_rows}
        assert rows[MODEL_BLEND]["epa_margin"] is None
        assert rows[MODEL_BLEND]["expected_home_margin"] == pytest.approx(
            rows[MODEL_ELO]["expected_home_margin"]
        )

    def test_win_prob_identical_across_models(self):
        rows = build_predictions_for_game(self.GAME, 1600, 1500, {}, epa_lookback=1, market=None)
        probs = {r["home_win_prob"] for r in rows}
        assert len(probs) == 1

    def test_no_market_yields_none_edge_for_both_rows(self):
        rows = build_predictions_for_game(self.GAME, 1600, 1500, {}, epa_lookback=1, market=None)
        for r in rows:
            assert r["edge"] is None
            assert r["edge_pick"] is None
            assert r["market_spread"] is None
            assert r["market_home_margin"] is None

    def test_market_home_margin_is_negated_spread(self):
        market = {"provider": "consensus", "spread": -7.0, "captured_at": None}
        rows = build_predictions_for_game(self.GAME, 1600, 1500, {}, epa_lookback=1, market=market)
        for r in rows:
            assert r["market_home_margin"] == pytest.approx(7.0)
            assert r["market_spread"] == pytest.approx(-7.0)


class TestComputeWeekIndex:
    def test_regular_passthrough(self):
        assert compute_week_index(1, "regular") == 1
        assert compute_week_index(15, "regular") == 15

    def test_postseason_offset(self):
        assert compute_week_index(1, "postseason") == 101


class TestLookupEpaCoefsAsof:
    WEEK_ROWS = {
        "Alabama": [
            (2, {"off_coef": 0.05, "def_coef": -0.02, "hfa_coef": 0.1, "plays": 70}),
            (4, {"off_coef": 0.10, "def_coef": -0.04, "hfa_coef": 0.1, "plays": 210}),
            (8, {"off_coef": 0.20, "def_coef": -0.06, "hfa_coef": 0.1, "plays": 500}),
        ]
    }
    PRIOR = {("Alabama", 2018): {"off_coef": 0.15, "def_coef": -0.05, "hfa_coef": 0.1}}

    def test_greatest_qualifying_week_at_or_before_wi(self):
        row, src = lookup_epa_coefs_asof(self.WEEK_ROWS, self.PRIOR, "Alabama", 2019, 6)
        assert src == "week"
        assert row["off_coef"] == 0.10

    def test_thin_plays_routes_to_prior_season(self):
        # entering week 2: only the 70-play row qualifies by position but
        # fails the plays >= 150 predicate -> prior-season fallback
        row, src = lookup_epa_coefs_asof(self.WEEK_ROWS, self.PRIOR, "Alabama", 2019, 2)
        assert src == "prior_season"
        assert row["off_coef"] == 0.15

    def test_future_weeks_excluded(self):
        row, src = lookup_epa_coefs_asof(self.WEEK_ROWS, self.PRIOR, "Alabama", 2019, 8)
        assert src == "week"
        assert row["off_coef"] == 0.20  # wi=8 row IS the entering-week-8 fit
        row6, _ = lookup_epa_coefs_asof(self.WEEK_ROWS, self.PRIOR, "Alabama", 2019, 7)
        assert row6["off_coef"] == 0.10  # week-8 boundary not visible at wi=7

    def test_unknown_team_no_prior_returns_none(self):
        row, src = lookup_epa_coefs_asof(self.WEEK_ROWS, {}, "Rice", 2019, 6)
        assert row is None
        assert src == "none"

    def test_postseason_wi_resolves_to_last_boundary(self):
        row, src = lookup_epa_coefs_asof(self.WEEK_ROWS, self.PRIOR, "Alabama", 2019, 101)
        assert src == "week"
        assert row["off_coef"] == 0.20


class TestBuildPredictionsAsOfBypass:
    GAME = {
        "game_id": 1,
        "season": 2019,
        "week": 6,
        "season_type": "regular",
        "home_team": "Alabama",
        "away_team": "Auburn",
        "neutral_site": False,
    }

    def test_epa_rows_bypass_ignores_lookup_dict(self):
        home = {"off_coef": 0.2, "def_coef": -0.1, "hfa_coef": 0.1}
        away = {"off_coef": 0.1, "def_coef": -0.05, "hfa_coef": 0.1}
        rows = build_predictions_for_game(
            self.GAME, 1600, 1500, {}, epa_lookback=0, market=None, epa_rows=(home, away)
        )
        blend = next(r for r in rows if r["model_version"] == MODEL_BLEND)
        assert blend["epa_margin"] is not None

    def test_elo_row_identical_with_and_without_epa_rows(self):
        home = {"off_coef": 0.2, "def_coef": -0.1, "hfa_coef": 0.1}
        away = {"off_coef": 0.1, "def_coef": -0.05, "hfa_coef": 0.1}
        with_rows = build_predictions_for_game(
            self.GAME, 1600, 1500, {}, epa_lookback=0, market=None, epa_rows=(home, away)
        )
        without = build_predictions_for_game(self.GAME, 1600, 1500, {}, epa_lookback=0, market=None)
        elo_a = next(r for r in with_rows if r["model_version"] == MODEL_ELO)
        elo_b = next(r for r in without if r["model_version"] == MODEL_ELO)
        assert elo_a == elo_b

    def test_none_side_falls_back_to_elo_only_blend(self):
        rows = build_predictions_for_game(
            self.GAME, 1600, 1500, {}, epa_lookback=0, market=None, epa_rows=(None, None)
        )
        blend = next(r for r in rows if r["model_version"] == MODEL_BLEND)
        elo = next(r for r in rows if r["model_version"] == MODEL_ELO)
        assert blend["epa_margin"] is None
        assert blend["expected_home_margin"] == elo["expected_home_margin"]
