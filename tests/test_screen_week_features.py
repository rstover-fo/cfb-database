"""Pure unit tests for the in-season team-week feature screen."""

import math
import random

import pytest

import scripts.screen_week_features as screen_week_features
from scripts.screen_week_features import (
    CANDIDATE_COLUMNS,
    CONTROL_ADJ_EPA_NET,
    CONTROL_ELO,
    CONTROL_MODE_FEATURES,
    CONTROL_MODE_MODEL_MARGIN,
    aggregate_bucketed_drive_features,
    apply_verdicts,
    assert_screen_coverage,
    build_arg_parser,
    build_model_margin_frame,
    build_screen_frame,
    compute_bucketed_as_of_features,
    compute_bucketed_form_and_volatility,
    drive_points,
    group_screen_inputs,
    screen_frame,
)


class TestFormAndVolatility:
    def test_matches_hand_computed_population_values(self):
        weekly_net_epa = [{"week_index": week, "net_epa": float(week)} for week in range(1, 7)]

        form, volatility = compute_bucketed_form_and_volatility(weekly_net_epa, as_of_week_index=7)

        # Season mean = 3.5; last-four mean = 4.5; population variance = 35/12.
        assert form == pytest.approx(1.0)
        assert volatility == pytest.approx(math.sqrt(35 / 12))


class TestAsOfBoundaries:
    def test_week_w_row_is_excluded_from_week_w_features(self):
        drives = [
            {
                "week_index": 1,
                "offense": "A",
                "defense": "B",
                "start_offense_score": 0,
                "end_offense_score": 7,
                "start_yards_to_goal": 80,
            },
            {
                "week_index": 2,
                "offense": "A",
                "defense": "B",
                "start_offense_score": 7,
                "end_offense_score": 14,
                "start_yards_to_goal": 60,
            },
        ]
        weekly_net_epa = [
            {"week_index": 1, "net_epa": 1.0},
            {"week_index": 2, "net_epa": 9.0},
        ]

        features = compute_bucketed_as_of_features(2, drives, [], weekly_net_epa)

        assert features["off_ppd"] == pytest.approx(7.0)
        assert features["off_field_pos"] == pytest.approx(80.0)
        assert features["form_net_epa_last4"] is None
        assert features["vol_net_epa"] is None

    def test_form_and_volatility_have_strict_maturity_gates(self):
        three_prior_weeks = [{"week_index": week, "net_epa": float(week)} for week in range(1, 4)]
        one_prior_week = [{"week_index": 1, "net_epa": 2.0}]

        form, volatility = compute_bucketed_form_and_volatility(three_prior_weeks, 4)
        assert form is None
        assert volatility == pytest.approx(0.8164965809)

        form, volatility = compute_bucketed_form_and_volatility(one_prior_week, 2)
        assert form is None
        assert volatility is None


class TestGrouping:
    def test_buckets_by_season_and_team_and_sorts_each_bucket(self):
        drives = [
            {
                "season": 2024,
                "week_index": 3,
                "offense": "A",
                "defense": "B",
            },
            {
                "season": 2024,
                "week_index": 1,
                "offense": "A",
                "defense": "B",
            },
            {
                "season": 2023,
                "week_index": 2,
                "offense": "A",
                "defense": "B",
            },
        ]
        weekly_net_epa = [
            {"season": 2024, "team": "A", "week_index": 3, "net_epa": 3.0},
            {"season": 2024, "team": "A", "week_index": 1, "net_epa": 1.0},
        ]

        offense, defense, weekly = group_screen_inputs(drives, weekly_net_epa)

        assert [row["week_index"] for row in offense[(2024, "A")]] == [1, 3]
        assert [row["week_index"] for row in defense[(2024, "B")]] == [1, 3]
        assert [row["week_index"] for row in weekly[(2024, "A")]] == [1, 3]
        assert (2023, "A") in offense


class TestDrivePoints:
    def test_uses_score_delta_including_non_touchdown_scoring(self):
        drive = {
            "start_offense_score": 20,
            "end_offense_score": 26,
        }

        # A TD-shaped drive_result would suggest seven, but the recorded score
        # delta is six and is the pre-registered source of drive points.
        assert drive_points(drive) == pytest.approx(6.0)

    def test_points_per_drive_aggregates_score_deltas(self):
        drives = [
            {
                "week_index": 1,
                "offense": "A",
                "defense": "B",
                "drive_result": "Defensive Touchdown",
                "start_offense_score": 20,
                "end_offense_score": 26,
                "start_yards_to_goal": 70,
            },
            {
                "week_index": 1,
                "offense": "A",
                "defense": "B",
                "start_offense_score": 26,
                "end_offense_score": 29,
                "start_yards_to_goal": 50,
            },
        ]

        features = aggregate_bucketed_drive_features(2, drives, [])

        assert features["off_ppd"] == pytest.approx(4.5)
        assert features["off_field_pos"] == pytest.approx(60.0)


class TestVerdicts:
    def test_rejections_remain_in_table_for_floor_and_bh_failures(self):
        raw_results = [
            {
                "candidate": "below_floor",
                "n_games": 1000,
                "partial_r": 0.01,
                "p_value": 0.001,
            },
            {
                "candidate": "bh_fail",
                "n_games": 1000,
                "partial_r": 0.20,
                "p_value": 0.50,
            },
            *(
                {
                    "candidate": name,
                    "n_games": 1000,
                    "partial_r": 0.0,
                    "p_value": 0.20,
                }
                for name in CANDIDATE_COLUMNS[2:]
            ),
        ]

        results = apply_verdicts(raw_results)
        by_name = {row["candidate"]: row for row in results}

        assert set(by_name) == {row["candidate"] for row in raw_results}
        assert by_name["below_floor"]["floor_pass"] is False
        assert by_name["below_floor"]["verdict"] == "REJECT"
        assert by_name["bh_fail"]["floor_pass"] is True
        assert by_name["bh_fail"]["bh_pass"] is False
        assert by_name["bh_fail"]["verdict"] == "REJECT"

    def test_ship_verdict_when_floor_and_bh_both_pass(self):
        raw_results = [
            {
                "candidate": "strong",
                "n_games": 1000,
                "partial_r": 0.20,
                "p_value": 1e-9,
            },
            {
                "candidate": "weak",
                "n_games": 1000,
                "partial_r": 0.01,
                "p_value": 0.90,
            },
        ]

        by_name = {row["candidate"]: row for row in apply_verdicts(raw_results)}

        assert by_name["strong"]["floor_pass"] is True
        assert by_name["strong"]["bh_pass"] is True
        assert by_name["strong"]["verdict"] == "SHIP"
        assert by_name["weak"]["verdict"] == "REJECT"


def _game(game_id, home, away, week, home_points=28, away_points=21):
    return {
        "game_id": game_id,
        "season": 2024,
        "season_type": "regular",
        "week": week,
        "home_team": home,
        "away_team": away,
        "home_points": home_points,
        "away_points": away_points,
        "completed": True,
        "home_elo_pregame": 1600.0,
        "away_elo_pregame": 1500.0,
        "home_adj_epa_net": 0.20,
        "away_adj_epa_net": 0.05,
    }


class TestBuildScreenFrame:
    def test_wires_margin_controls_and_candidate_diffs(self):
        games = [_game(1, "A", "B", week=2)]
        drives = [
            {
                "season": 2024,
                "week_index": 1,
                "offense": "A",
                "defense": "B",
                "start_offense_score": 0,
                "end_offense_score": 7,
                "start_yards_to_goal": 80,
            },
            {
                "season": 2024,
                "week_index": 1,
                "offense": "B",
                "defense": "A",
                "start_offense_score": 0,
                "end_offense_score": 3,
                "start_yards_to_goal": 60,
            },
        ]

        frame = build_screen_frame(games, drives, [])

        assert len(frame) == 1
        row = frame[0]
        assert row["home_margin"] == pytest.approx(7.0)
        assert row[CONTROL_ELO] == pytest.approx(100.0)
        assert row[CONTROL_ADJ_EPA_NET] == pytest.approx(0.15)
        # Home offense scored 7/drive, away offense 3/drive.
        assert row["off_ppd"] == pytest.approx(4.0)
        # Home defense allowed 3/drive, away defense allowed 7/drive.
        assert row["def_ppd_allowed"] == pytest.approx(-4.0)
        # No weekly EPA rows: trajectory diffs stay None.
        assert row["form_net_epa_last4"] is None
        assert row["vol_net_epa"] is None

    def test_excludes_incomplete_and_scoreless_games(self):
        games = [
            _game(1, "A", "B", week=2) | {"completed": False},
            _game(2, "A", "B", week=3, home_points=None),
        ]

        assert build_screen_frame(games, [], []) == []


class TestScreenFrame:
    def test_small_sample_rejects_without_p_value(self):
        games = [_game(1, "A", "B", week=2)]
        results = screen_frame(build_screen_frame(games, [], []))

        by_name = {row["candidate"]: row for row in results}
        assert set(by_name) == set(CANDIDATE_COLUMNS)
        for row in results:
            assert row["p_value"] is None
            assert row["partial_r"] == pytest.approx(0.0)
            assert row["verdict"] == "REJECT"


def _model_frame(n=250, candidate_values=None):
    rng = random.Random(19)
    control = [rng.gauss(0.0, 1.0) for _ in range(n)]
    if candidate_values is None:
        candidate_values = [rng.gauss(0.0, 1.0) for _ in range(n)]
    margin = [0.8 * x + 0.3 * z + rng.gauss(0.0, 0.2) for x, z in zip(candidate_values, control)]
    rows = []
    for i, (z, x, y) in enumerate(zip(control, candidate_values, margin)):
        row = {
            "home_margin": y,
            "model_margin": z,
        }
        row.update(
            {name: (x if name == "off_ppd" else float(i % 11)) for name in CANDIDATE_COLUMNS}
        )
        rows.append(row)
    return rows


class TestModelMarginControl:
    def test_cli_defaults_to_v1_features_mode(self):
        assert build_arg_parser().parse_args([]).control_mode == CONTROL_MODE_FEATURES

    def test_exact_function_of_frozen_margin_control_has_no_incremental_signal(self):
        frame = _model_frame()
        for i, row in enumerate(frame):
            row["off_ppd"] = 2.0 * row["model_margin"]
            row["home_margin"] = 0.8 * row["model_margin"] + float(i % 7)

        result = next(
            row
            for row in screen_frame(frame, CONTROL_MODE_MODEL_MARGIN)
            if row["candidate"] == "off_ppd"
        )

        assert result["partial_r"] == pytest.approx(0.0)

    def test_orthogonal_candidate_tracks_margin_residual(self):
        frame = _model_frame()

        result = next(
            row
            for row in screen_frame(frame, CONTROL_MODE_MODEL_MARGIN)
            if row["candidate"] == "off_ppd"
        )

        assert result["partial_r"] > 0.7

    def test_model_margin_p_value_uses_one_control(self, monkeypatch):
        calls = []

        def fake_pvalue(partial_r, n, n_controls=1):
            calls.append(n_controls)
            return 0.01

        monkeypatch.setattr(screen_week_features, "partial_corr_pvalue", fake_pvalue)
        screen_frame(_model_frame(), CONTROL_MODE_MODEL_MARGIN)

        assert calls == [1] * len(CANDIDATE_COLUMNS)

    def test_games_without_prior_vintage_drop_from_model_margin_frame(self):
        games = [
            _game(1, "A", "B", week=2) | {"season": 2017},
            _game(2, "A", "B", week=2) | {"season": 2018},
        ]

        frame = build_model_margin_frame(
            games,
            [],
            [],
            {2017: object()},
            scorer=lambda game, fit: (12.5, 0.5),
        )

        assert [row["game_id"] for row in frame] == [2]
        assert frame[0]["model_margin"] == pytest.approx(12.5)


class TestCoverageFloors:
    def test_empty_frame_is_untestable_not_rejected(self):
        with pytest.raises(RuntimeError, match="UNTESTABLE"):
            assert_screen_coverage([], screen_frame([]))

    def test_thin_candidate_sample_is_untestable(self):
        frame = [{"home_margin": 1.0}] * 2000
        results = [
            {"candidate": name, "n_games": 0, "partial_r": 0.0, "p_value": None}
            for name in CANDIDATE_COLUMNS
        ]
        with pytest.raises(RuntimeError, match="UNTESTABLE"):
            assert_screen_coverage(frame, results)
