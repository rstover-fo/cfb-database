"""Pure unit tests for the in-season team-week feature screen."""

import math

import pytest

from scripts.screen_week_features import (
    CANDIDATE_COLUMNS,
    apply_verdicts,
    compute_as_of_features,
    compute_form_and_volatility,
    drive_points,
    group_screen_inputs,
)


class TestFormAndVolatility:
    def test_matches_hand_computed_population_values(self):
        weekly_net_epa = [{"week_index": week, "net_epa": float(week)} for week in range(1, 7)]

        form, volatility = compute_form_and_volatility(weekly_net_epa, as_of_week_index=7)

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

        features = compute_as_of_features("A", 2, drives, weekly_net_epa)

        assert features["off_ppd"] == pytest.approx(7.0)
        assert features["off_field_pos"] == pytest.approx(80.0)
        assert features["form_net_epa_last4"] is None
        assert features["vol_net_epa"] is None

    def test_form_and_volatility_have_strict_maturity_gates(self):
        three_prior_weeks = [{"week_index": week, "net_epa": float(week)} for week in range(1, 4)]
        one_prior_week = [{"week_index": 1, "net_epa": 2.0}]

        form, volatility = compute_form_and_volatility(three_prior_weeks, 4)
        assert form is None
        assert volatility == pytest.approx(0.8164965809)

        form, volatility = compute_form_and_volatility(one_prior_week, 2)
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

        features = compute_as_of_features("A", 2, drives, [])

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
