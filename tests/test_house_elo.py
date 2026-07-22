"""Unit tests for compute_house_elo's pure Elo engine (no DB, no I/O).

Covers the pure math (expected_score, mov_multiplier, pearson_r) and the
EloEngine's stateful behavior (single-game deltas, season carryover,
low-sample pooling, and multi-game bookkeeping) directly against plain
dicts, per docs/plans/2026-07-21-tier2-analytics-plan.md's House Elo design.
"""

import math

import pytest

from scripts.compute_house_elo import (
    EloEngine,
    expected_score,
    mov_multiplier,
    pearson_r,
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
    start_date=None,
    neutral_site=False,
):
    return {
        "game_id": game_id,
        "season": season,
        "week": week,
        "season_type": season_type,
        "start_date": start_date,
        "neutral_site": neutral_site,
        "home_team": home_team,
        "away_team": away_team,
        "home_points": home_points,
        "away_points": away_points,
        "cfbd_home_pregame_elo": None,
        "cfbd_away_pregame_elo": None,
    }


class TestExpectedScore:
    def test_zero_diff_is_half(self):
        assert expected_score(0) == pytest.approx(0.5)

    def test_plus_400_favorite(self):
        assert expected_score(400) == pytest.approx(1 / 1.1, abs=1e-9)

    def test_symmetry(self):
        for diff in (65, 133.33, 400, 1000):
            assert expected_score(diff) + expected_score(-diff) == pytest.approx(1.0)


class TestMovMultiplier:
    def test_margin_7_zero_diff(self):
        assert mov_multiplier(7, 0) == pytest.approx(math.log(8), abs=1e-4)
        assert mov_multiplier(7, 0) == pytest.approx(2.0794, abs=1e-4)

    def test_damping_reduces_multiplier_for_bigger_favorite(self):
        neutral = mov_multiplier(7, 0)
        favorite = mov_multiplier(7, 400)
        assert favorite < neutral

    def test_tie_margin_is_zero(self):
        assert mov_multiplier(0, 0) == 0.0

    def test_sign_of_margin_does_not_matter(self):
        assert mov_multiplier(-7, 0) == pytest.approx(mov_multiplier(7, 0))


class TestPearsonR:
    def test_perfect_positive_correlation(self):
        xs = [1.0, 2.0, 3.0, 4.0]
        ys = [10.0, 20.0, 30.0, 40.0]
        assert pearson_r(xs, ys) == pytest.approx(1.0)

    def test_empty_input_is_nan(self):
        assert math.isnan(pearson_r([], []))


class TestEngineSingleGame:
    """Two teams, equal (seed) ratings, single non-pooled game."""

    def test_hand_computed_delta_and_zero_sum(self):
        engine = EloEngine()
        # Keep both teams out of the pooled bucket (>= POOL_THRESHOLD games).
        engine.start_season(2020, {"Home": 4, "Away": 4})
        game = make_game(1, 2020, "Home", "Away", home_points=30, away_points=23)

        row = engine.process_game(game)

        elo_diff_home = 65.0  # 1500 - 1500 + HFA(65), non-neutral
        exp_home = expected_score(elo_diff_home)
        mult = math.log(8) * (2.2 / (0.001 * elo_diff_home + 2.2))
        delta = EloEngine.K * mult * (1 - exp_home)

        assert row["home_postgame_elo"] == pytest.approx(1500 + delta, abs=1e-6)
        assert row["away_postgame_elo"] == pytest.approx(1500 - delta, abs=1e-6)
        # Zero-sum: the two deltas are exact opposites.
        home_delta = row["home_postgame_elo"] - row["home_pregame_elo"]
        away_delta = row["away_postgame_elo"] - row["away_pregame_elo"]
        assert home_delta == pytest.approx(-away_delta, abs=1e-9)
        assert row["mov_multiplier"] == pytest.approx(mult, abs=1e-9)

    def test_neutral_site_removes_hfa(self):
        engine = EloEngine()
        engine.start_season(2020, {"Home": 4, "Away": 4})
        game = make_game(2, 2020, "Home", "Away", home_points=21, away_points=20, neutral_site=True)

        row = engine.process_game(game)

        assert row["home_win_prob"] == pytest.approx(0.5)
        assert row["expected_home_margin"] == pytest.approx(0.0)


class TestCarryover:
    def test_one_season_gap(self):
        engine = EloEngine()
        engine.ratings["Team A"] = 1700.0
        engine.last_season["Team A"] = 2019
        engine.start_season(2020, {"Team A": 10})
        assert engine.ratings["Team A"] == pytest.approx(1500 + 200 * (2 / 3))

    def test_two_season_gap(self):
        engine = EloEngine()
        engine.ratings["Team A"] = 1700.0
        engine.last_season["Team A"] = 2018
        engine.start_season(2020, {"Team A": 10})
        assert engine.ratings["Team A"] == pytest.approx(1500 + 200 * (2 / 3) ** 2)

    def test_pooled_resets_to_seed_every_season(self):
        engine = EloEngine()
        engine.ratings[EloEngine.POOLED] = 1650.0
        engine.start_season(2020, {"Team A": 10})
        assert engine.ratings[EloEngine.POOLED] == pytest.approx(1500.0)

    def test_brand_new_team_gets_no_carryover(self):
        engine = EloEngine()
        # "New Team" has never been seen before -- no last_season entry.
        engine.start_season(2020, {"New Team": 10})
        assert "New Team" not in engine.ratings


class TestPooling:
    def test_low_sample_team_aliases_to_pooled(self):
        engine = EloEngine()
        engine.start_season(2020, {"Big State": 10, "Tiny College": 2})
        assert engine.resolve("Tiny College") == EloEngine.POOLED
        assert engine.resolve("Big State") == "Big State"

    def test_row_records_real_team_name_not_alias(self):
        engine = EloEngine()
        engine.start_season(2020, {"Big State": 4, "Tiny College": 2})
        game = make_game(3, 2020, "Big State", "Tiny College", home_points=45, away_points=3)

        row = engine.process_game(game)

        assert row["home_team"] == "Big State"
        assert row["away_team"] == "Tiny College"

    def test_snapshot_low_confidence_rules(self):
        engine = EloEngine()
        # Team A: modern era, thin sample -> low confidence via games_played.
        engine.last_season["Team A"] = 2020
        engine.games_played["Team A"] = 2
        engine.ratings["Team A"] = 1550.0
        # Team B: modern era, plenty of games -> confident.
        engine.last_season["Team B"] = 2020
        engine.games_played["Team B"] = 10
        engine.ratings["Team B"] = 1600.0
        # Team C: pre-1900 era, plenty of games -> low confidence via era.
        engine.last_season["Team C"] = 1899
        engine.games_played["Team C"] = 10
        engine.ratings["Team C"] = 1500.0

        snapshot = {row["team"]: row for row in engine.current_snapshot(2020)}

        assert snapshot["Team A"]["low_confidence"] is True
        assert snapshot["Team B"]["low_confidence"] is False
        assert snapshot["Team C"]["low_confidence"] is True

    def test_snapshot_reports_pooled_rating_for_pooled_team(self):
        engine = EloEngine()
        engine.last_season["Tiny"] = 2021
        engine.games_played["Tiny"] = 2
        engine.ratings[EloEngine.POOLED] = 1523.4
        engine.alias = {"Tiny": EloEngine.POOLED}

        rows = engine.current_snapshot(2021)

        assert len(rows) == 1
        assert rows[0]["team"] == "Tiny"
        assert rows[0]["rating"] == pytest.approx(1523.4)
        assert rows[0]["low_confidence"] is True


class TestMiniSeasonEndToEnd:
    def test_three_game_mini_season_bookkeeping(self):
        engine = EloEngine()
        # Inflate team_game_counts above POOL_THRESHOLD so this test exercises
        # plain (non-pooled) bookkeeping, independent of how many games this
        # scenario actually simulates.
        engine.start_season(2021, {"A": 4, "B": 4, "C": 4})

        g1 = make_game(101, 2021, "A", "B", home_points=20, away_points=10, week=1)
        g2 = make_game(102, 2021, "A", "C", home_points=14, away_points=21, week=2)
        g3 = make_game(103, 2021, "B", "C", home_points=17, away_points=17, week=3)

        engine.process_game(g1)
        assert engine.games_played == {"A": 1, "B": 1}
        assert engine.last_game_id == {"A": 101, "B": 101}

        engine.process_game(g2)
        assert engine.games_played == {"A": 2, "B": 1, "C": 1}
        assert engine.last_game_id == {"A": 102, "B": 101, "C": 102}

        engine.process_game(g3)
        assert engine.games_played == {"A": 2, "B": 2, "C": 2}
        # A didn't play g3, so its last_game_id stays at g2.
        assert engine.last_game_id == {"A": 102, "B": 103, "C": 103}

        snapshot = {row["team"]: row for row in engine.current_snapshot(2021)}
        assert snapshot["A"]["games_played"] == 2
        assert snapshot["B"]["games_played"] == 2
        assert snapshot["C"]["games_played"] == 2
        assert snapshot["A"]["last_game_id"] == 102
        assert snapshot["B"]["last_game_id"] == 103
        assert snapshot["C"]["last_game_id"] == 103
