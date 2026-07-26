"""Unit tests for the preseason backtest's pure metrics (no DB)."""

import numpy as np
import pytest

from scripts.backtest_preseason import (
    MIN_SIGMA_GAMES,
    RESPECTABLE_WIN_MAE,
    baseline_prior_rate,
    brier,
    build_backtest_row,
    calibration_table,
    drop_outcome_dependent,
    interval_coverage,
    preseason_sigma,
    residual_quantiles,
    win_error_metrics,
)


class TestWinErrorMetrics:
    def test_perfect_prediction(self):
        m = win_error_metrics([7.0, 9.0, 3.0], [7.0, 9.0, 3.0])
        assert m["mae"] == 0.0
        assert m["rmse"] == 0.0
        assert m["bias"] == 0.0

    def test_mae_and_rmse(self):
        m = win_error_metrics([8.0, 6.0], [7.0, 8.0])
        assert m["mae"] == pytest.approx(1.5)
        assert m["rmse"] == pytest.approx(np.sqrt((1 + 4) / 2))

    def test_bias_is_signed(self):
        """A model can be accurate on average and still systematically
        optimistic; only the signed term shows it."""
        over = win_error_metrics([9.0, 9.0], [7.0, 7.0])
        under = win_error_metrics([5.0, 5.0], [7.0, 7.0])
        assert over["bias"] == pytest.approx(2.0)
        assert under["bias"] == pytest.approx(-2.0)
        assert over["mae"] == under["mae"]

    def test_empty_is_not_a_crash(self):
        m = win_error_metrics([], [])
        assert m == {"n": 0, "mae": None, "rmse": None, "bias": None}


class TestIntervalCoverage:
    def test_all_inside(self):
        assert interval_coverage([7, 8], [5, 6], [9, 10]) == 1.0

    def test_all_outside(self):
        assert interval_coverage([2, 12], [5, 6], [9, 10]) == 0.0

    def test_boundaries_count_as_inside(self):
        assert interval_coverage([5, 9], [5, 6], [9, 10]) == 1.0

    def test_partial(self):
        assert interval_coverage([7, 12], [5, 6], [9, 10]) == 0.5

    def test_empty(self):
        assert interval_coverage([], [], []) is None


class TestCalibrationTable:
    def test_perfectly_calibrated_bucket(self):
        probs = [0.75] * 100
        outcomes = [True] * 75 + [False] * 25
        rows = calibration_table(probs, outcomes)
        assert len(rows) == 1
        assert rows[0]["mean_predicted"] == pytest.approx(0.75)
        assert rows[0]["observed"] == pytest.approx(0.75)

    def test_overconfident_shows_gap(self):
        probs = [0.9] * 10
        outcomes = [True] * 3 + [False] * 7
        rows = calibration_table(probs, outcomes)
        assert rows[0]["mean_predicted"] > rows[0]["observed"]

    def test_probability_one_lands_in_final_bucket(self):
        """p == 1.0 must not fall through every half-open bucket."""
        rows = calibration_table([1.0], [True])
        assert len(rows) == 1
        assert rows[0]["bucket"] == "0.9-1.0"

    def test_empty_buckets_are_omitted(self):
        rows = calibration_table([0.05, 0.95], [False, True])
        assert [r["bucket"] for r in rows] == ["0.0-0.1", "0.9-1.0"]

    def test_empty_input(self):
        assert calibration_table([], []) == []


class TestBaselinePriorRate:
    def test_scales_rate_to_this_seasons_slate(self):
        """The whole point: a 12-game record projected onto a 9-game slate."""
        assert baseline_prior_rate(9.0, 12, 9) == pytest.approx(6.75)

    def test_no_prior_season_falls_back_to_500(self):
        assert baseline_prior_rate(None, None, 12) == pytest.approx(6.0)

    def test_zero_prior_games_does_not_divide_by_zero(self):
        assert baseline_prior_rate(0.0, 0, 12) == pytest.approx(6.0)

    def test_undefeated_prior_projects_full_slate(self):
        assert baseline_prior_rate(12.0, 12, 11) == pytest.approx(11.0)


class TestPreseasonSigma:
    def test_requires_minimum_sample(self):
        assert preseason_sigma([1.0] * (MIN_SIGMA_GAMES - 1)) is None

    def test_returns_sd_once_sample_is_large_enough(self):
        rng = np.random.default_rng(0)
        residuals = list(rng.normal(0.0, 17.0, size=MIN_SIGMA_GAMES * 20))
        sigma = preseason_sigma(residuals)
        assert sigma == pytest.approx(17.0, abs=0.6)

    def test_degenerate_zero_variance_is_rejected(self):
        """A zero sigma makes every simulated game deterministic; that is a
        broken distribution, not a confident one."""
        assert preseason_sigma([5.0] * (MIN_SIGMA_GAMES * 2)) is None

    def test_empty(self):
        assert preseason_sigma([]) is None


class TestBrier:
    def test_perfect_forecast(self):
        assert brier([1.0, 0.0], [True, False]) == pytest.approx(0.0)

    def test_worst_forecast(self):
        assert brier([0.0, 1.0], [True, False]) == pytest.approx(1.0)

    def test_coin_flip(self):
        assert brier([0.5, 0.5], [True, False]) == pytest.approx(0.25)

    def test_empty(self):
        assert brier([], []) is None


class TestLeakGuards:
    """The backtest's value rests entirely on these two properties, so they are
    asserted against the module rather than trusted from the docstring."""

    def test_season_is_scored_with_the_prior_seasons_fit(self):
        from scripts.score_fitted import select_train_through

        assert select_train_through("backfill", 2022) == 2021

    def test_games_are_simulated_as_pending(self):
        """simulate_wins hands a completed game a deterministic win, so a
        backtest that left completed=True would reproduce the actual record and
        report zero error."""
        from scripts.simulate_season import simulate_wins

        games = [
            {
                "home_team": "A",
                "away_team": "B",
                "completed": True,
                "home_win": True,
                "conference_game": False,
            }
        ]
        sim = simulate_wins(games, n_sims=50, sigma=17.0, seed=1)
        assert set(sim["wins"]["A"]) == {1}, "completed games are deterministic -- must be pending"

        pending = [
            {
                "home_team": "A",
                "away_team": "B",
                "completed": False,
                "expected_home_margin": 0.0,
                "conference_game": False,
            }
        ]
        sim2 = simulate_wins(pending, n_sims=500, sigma=17.0, seed=1)
        wins = sim2["wins"]["A"]
        assert set(wins) == {0, 1}, "a pending coin-flip game must vary across simulations"


class TestDropOutcomeDependent:
    """Codex PR #51 P1: season_type='regular' still admits conference
    championship games (and, below FBS, whole playoff brackets), whose
    participants were decided by that season's results."""

    @staticmethod
    def _slate(team, n, start_id=0, opponent_prefix="opp"):
        return [
            {
                "game_id": start_id + i,
                "home_team": team,
                "away_team": f"{opponent_prefix}{i}",
                "start_date": f"2024-09-{i + 1:02d}",
            }
            for i in range(n)
        ]

    def test_keeps_a_standard_twelve_game_slate(self):
        kept, dropped = drop_outcome_dependent(self._slate("A", 12))
        assert len(kept) == 12
        assert dropped == 0

    def test_drops_the_thirteenth_game(self):
        """The conference championship: earned, never scheduled."""
        kept, dropped = drop_outcome_dependent(self._slate("A", 13))
        assert len(kept) == 12
        assert dropped == 1

    def test_drops_a_deep_playoff_run(self):
        kept, dropped = drop_outcome_dependent(self._slate("A", 15))
        assert len(kept) == 12
        assert dropped == 3

    def test_drops_when_beyond_the_cap_for_either_side(self):
        """Both participants must keep identical slates or the actual-wins
        comparison stops being matched."""
        games = self._slate("A", 12)
        # B's first game, but A's thirteenth.
        games.append(
            {"game_id": 99, "home_team": "A", "away_team": "B", "start_date": "2024-12-07"}
        )
        kept, dropped = drop_outcome_dependent(games)
        assert dropped == 1
        assert all(g["game_id"] != 99 for g in kept)

    def test_ordering_is_chronological_not_insertion_order(self):
        games = self._slate("A", 13)
        shuffled = list(reversed(games))
        kept, dropped = drop_outcome_dependent(shuffled)
        assert dropped == 1
        # The dropped game is the LAST by date, whatever order it arrived in.
        assert all(g["start_date"] != "2024-09-13" for g in kept)

    def test_null_start_dates_do_not_crash(self):
        games = [
            {"game_id": 1, "home_team": "A", "away_team": "X", "start_date": None},
            {"game_id": 2, "home_team": "A", "away_team": "Y", "start_date": "2024-09-01"},
        ]
        kept, dropped = drop_outcome_dependent(games)
        assert len(kept) == 2
        assert dropped == 0

    def test_empty(self):
        assert drop_outcome_dependent([]) == ([], 0)


class TestResidualQuantiles:
    """Codex PR #51 P1: MAE is an average loss, not an interval half-width.
    Quoting `point +/- MAE` as an honest range overstates the coverage badly."""

    def test_quantiles_of_a_known_distribution(self):
        rng = np.random.default_rng(7)
        actual = rng.normal(0.0, 1.0, size=20000)
        projected = np.zeros_like(actual)
        q = residual_quantiles(list(projected), list(actual))
        assert q["p10"] == pytest.approx(-1.2816, abs=0.05)
        assert q["p90"] == pytest.approx(1.2816, abs=0.05)
        assert q["p50"] == pytest.approx(0.0, abs=0.05)

    def test_sign_convention_is_actual_minus_projected(self):
        """A model that under-projects must show POSITIVE residuals."""
        q = residual_quantiles([5.0] * 100, [7.0] * 100)
        assert q["p50"] == pytest.approx(2.0)

    def test_mae_is_a_narrower_span_than_the_80pct_interval(self):
        """The actual defect, stated as a test: for a normal error the +/-MAE
        band is materially narrower than p10..p90, so the two must not be used
        interchangeably."""
        rng = np.random.default_rng(11)
        actual = list(rng.normal(0.0, 2.24, size=20000))
        projected = [0.0] * len(actual)
        q = residual_quantiles(projected, actual)
        mae = win_error_metrics(projected, actual)["mae"]
        assert (q["p90"] - q["p10"]) > 2 * mae * 1.3, (
            "p10..p90 must be clearly wider than the +/- MAE band"
        )

    def test_empty(self):
        assert residual_quantiles([], []) is None


# =============================================================================
# Persistence -- predictions.model_backtest (migration 045)
# =============================================================================


def _agg(
    proj=None,
    act=None,
    per_season=None,
    strength_share=0.15,
):
    """An aggregate shaped exactly like ``_simulate_pass`` returns.

    Built with the module's own metric functions rather than hand-written
    numbers, so a test asserting "the row matches the report" is asserting
    agreement and not re-deriving the same arithmetic twice.
    """
    from scripts.backtest_preseason import brier as _brier

    proj = [8.0, 6.0, 3.0, 10.0] if proj is None else proj
    act = [7.0, 8.0, 4.0, 9.0] if act is None else act
    p10 = [p - 2.0 for p in proj]
    p90 = [p + 2.0 for p in proj]
    base_prior = [p + 0.5 for p in proj]
    base_flat = [6.0] * len(proj)
    bowl_probs = [0.9, 0.5, 0.1, 0.95][: len(proj)]
    bowl_out = [a >= 6 for a in act]
    ten_probs = [0.2, 0.05, 0.01, 0.6][: len(proj)]
    ten_out = [a >= 10 for a in act]
    if per_season is None:
        per_season = [
            {
                "season": 2025,
                "train_through": 2024,
                "max_games_played": 0,
                "dropped_outcome_dependent": 5,
            },
            {
                "season": 2024,
                "train_through": 2023,
                "max_games_played": 0,
                "dropped_outcome_dependent": 3,
            },
        ]
    return {
        "strength_share": strength_share,
        "per_season": per_season,
        "proj": proj,
        "act": act,
        "p10": p10,
        "p90": p90,
        "base_prior": base_prior,
        "base_flat": base_flat,
        "bowl_probs": bowl_probs,
        "bowl_out": bowl_out,
        "ten_probs": ten_probs,
        "ten_out": ten_out,
        "overall": win_error_metrics(proj, act),
        "coverage": interval_coverage(act, p10, p90),
        "quantiles": residual_quantiles(proj, act),
        "bowl_brier": _brier(bowl_probs, bowl_out),
        "ten_brier": _brier(ten_probs, ten_out),
    }


def _row(**kwargs):
    defaults = {
        "scope": "fbs",
        "season_start": 2018,
        "season_end": 2025,
        "n_sims": 10000,
        "seed": 20260726,
        "feature_build_version": "tw_v2",
    }
    agg = kwargs.pop("agg", None) or _agg()
    return build_backtest_row(agg, **{**defaults, **kwargs})


class TestBuildBacktestRow:
    """The row and the BACKTEST_GATE log line must never be able to disagree --
    cfb-app attaches these numbers to every season-outlook answer it gives."""

    def test_metrics_match_the_reported_aggregate(self):
        agg = _agg()
        row = _row(agg=agg)
        assert row["n"] == agg["overall"]["n"]
        assert row["win_mae"] == pytest.approx(agg["overall"]["mae"])
        assert row["rmse"] == pytest.approx(agg["overall"]["rmse"])
        assert row["bias"] == pytest.approx(agg["overall"]["bias"])
        assert row["coverage"] == pytest.approx(agg["coverage"])
        assert row["resid_p10"] == pytest.approx(agg["quantiles"]["p10"])
        assert row["resid_p90"] == pytest.approx(agg["quantiles"]["p90"])
        assert row["bowl_brier"] == pytest.approx(agg["bowl_brier"])
        assert row["ten_plus_brier"] == pytest.approx(agg["ten_brier"])

    def test_baselines_use_the_same_denominator_as_the_model(self):
        """An MAE alone means nothing; the baselines are how it is read."""
        agg = _agg()
        row = _row(agg=agg)
        assert row["baseline_prior_mae"] == pytest.approx(
            win_error_metrics(agg["base_prior"], agg["act"])["mae"]
        )
        assert row["baseline_flat_mae"] == pytest.approx(
            win_error_metrics(agg["base_flat"], agg["act"])["mae"]
        )

    def test_every_conflict_key_column_is_populated(self):
        """A NULL in a unique index does not conflict with another NULL, so a
        missing key column would let two runs of the same configuration on the
        same day insert two rows instead of converging onto one."""
        from scripts.backtest_preseason import _BACKTEST_CONFLICT_COLUMNS

        row = _row()
        for col in _BACKTEST_CONFLICT_COLUMNS:
            if col == "run_date":
                continue  # server-side DEFAULT, deliberately not in the row
            assert row.get(col) is not None, f"key column {col} is NULL -- duplicates possible"

    def test_provenance_records_which_model_and_seasons(self):
        from scripts.train_model import MODEL_VERSION

        row = _row()
        assert row["model_version"] == MODEL_VERSION
        assert row["feature_build_version"] == "tw_v2"
        assert row["seasons_covered"] == [2024, 2025]
        # Season S is scored with the frozen S-1 fit.
        assert row["train_through_min"] == 2023
        assert row["train_through_max"] == 2024

    def test_requested_range_is_kept_distinct_from_seasons_covered(self):
        """2018-2025 was requested; only 2024-2025 had a usable S-1 fit. If the
        two collapsed, a partially-measured range would read as a full one."""
        row = _row(season_start=2018, season_end=2025)
        assert (row["season_start"], row["season_end"]) == (2018, 2025)
        assert row["seasons_covered"] == [2024, 2025]

    def test_scope_distinguishes_the_two_populations(self):
        assert _row(scope="fbs")["scope"] == "fbs"
        assert _row(scope="all_divisions")["scope"] == "all_divisions"

    def test_leak_evidence_travels_with_the_numbers(self):
        """maxGP > 0 means a 'week-1' vector already held that season's own
        games, so every error metric on the row is understated."""
        per_season = [
            {
                "season": 2025,
                "train_through": 2024,
                "max_games_played": 4,
                "dropped_outcome_dependent": 5,
            },
            {
                "season": 2024,
                "train_through": 2023,
                "max_games_played": 0,
                "dropped_outcome_dependent": 3,
            },
        ]
        row = _row(agg=_agg(per_season=per_season))
        assert row["max_games_played_to_date"] == 4
        assert row["games_dropped_outcome_dependent"] == 8

    def test_stores_the_bar_not_the_verdict(self):
        """A stored measurement outlives a stored judgement: the bar can be
        revised, win_mae cannot."""
        row = _row()
        assert row["respectable_win_mae"] == RESPECTABLE_WIN_MAE
        assert not any("verdict" in k for k in row)

    def test_calibration_covers_both_published_probabilities(self):
        """api.season_outlook publishes p_bowl_eligible and p_ten_plus as
        probability claims; a Brier scalar cannot say which bucket missed."""
        import json

        row = _row()
        assert set(row["calibration"]) == {"p_bowl_eligible", "p_ten_plus"}
        # Must survive json.dumps -- the writer stores it as JSONB.
        json.loads(json.dumps(row["calibration"]))

    def test_strength_share_is_the_value_the_simulation_used(self):
        """_simulate_pass normalizes before simulating and puts the normalized
        share in the aggregate; the row must copy THAT, not re-round."""
        row = _row(agg=_agg(strength_share=0.15))
        assert row["strength_share"] == pytest.approx(0.15)

    def test_no_rows_compared_writes_nothing(self):
        """A row with n=0 and every metric NULL asserts a backtest ran and
        measured nothing, which reads to a consumer as 'no interval known' for
        a model that is actually fine. Absence is the honest answer."""
        assert (
            build_backtest_row(
                _agg(per_season=[]),
                scope="fbs",
                season_start=2025,
                season_end=2025,
                n_sims=10,
                seed=1,
                feature_build_version=None,
            )
            is None
        )
        assert (
            build_backtest_row(
                _agg(proj=[], act=[]),
                scope="fbs",
                season_start=2025,
                season_end=2025,
                n_sims=10,
                seed=1,
                feature_build_version=None,
            )
            is None
        )

    def test_null_feature_build_version_is_allowed(self):
        """Unknown, never invented -- the column is nullable for this case."""
        assert _row(feature_build_version=None)["feature_build_version"] is None


class TestBacktestUpsert:
    """The write shape, asserted rather than trusted -- a same-day re-run must
    converge, and a column added to the table but not to the UPDATE list would
    silently keep a stale value on that re-run."""

    def test_row_keys_match_the_insert_column_list_exactly(self):
        from scripts.backtest_preseason import _BACKTEST_COLUMNS

        assert set(_row()) == set(_BACKTEST_COLUMNS)

    def test_every_non_key_column_is_refreshed_on_conflict(self):
        from scripts.backtest_preseason import (
            _BACKTEST_COLUMNS,
            _BACKTEST_CONFLICT_COLUMNS,
            _BACKTEST_UPDATE_ASSIGNMENTS,
        )

        for col in _BACKTEST_COLUMNS:
            if col in _BACKTEST_CONFLICT_COLUMNS:
                assert f"{col} = EXCLUDED.{col}" not in _BACKTEST_UPDATE_ASSIGNMENTS
            else:
                assert f"{col} = EXCLUDED.{col}" in _BACKTEST_UPDATE_ASSIGNMENTS, (
                    f"{col} would keep a stale value on a same-day re-run"
                )

    def test_computed_at_is_refreshed_so_a_rerun_is_visible(self):
        from scripts.backtest_preseason import _BACKTEST_UPSERT_SQL

        assert "computed_at = now()" in _BACKTEST_UPSERT_SQL

    def test_conflict_target_matches_the_unique_index(self):
        """ON CONFLICT must name exactly model_backtest_daily_key's columns, in
        its order, or the same-day upsert raises instead of converging."""
        from scripts.backtest_preseason import _BACKTEST_UPSERT_SQL

        assert (
            "ON CONFLICT (model_version, run_date, scope, season_start, "
            "season_end, strength_share) DO UPDATE SET" in _BACKTEST_UPSERT_SQL
        )

    def test_run_date_is_left_to_the_server_default(self):
        """The UTC day must be decided server-side; a client clock filing a
        snapshot under the wrong day breaks the append-only guarantee."""
        from scripts.backtest_preseason import _BACKTEST_COLUMNS

        assert "run_date" not in _BACKTEST_COLUMNS
        assert "computed_at" not in _BACKTEST_COLUMNS

    def test_placeholder_count_matches_the_column_count(self):
        import re

        from scripts.backtest_preseason import _BACKTEST_COLUMNS, _BACKTEST_UPSERT_SQL

        values_clause = _BACKTEST_UPSERT_SQL.split("VALUES", 1)[1].split("ON CONFLICT", 1)[0]
        assert len(re.findall(r"%\([a-z_0-9]+\)s", values_clause)) == len(_BACKTEST_COLUMNS)

    def test_no_unescaped_percent_signs(self):
        """A literal % in SQL passed with parameters must be %% or psycopg2
        reads it as a placeholder; that silently disabled a whole script in
        this repo for months."""
        import re

        from scripts.backtest_preseason import _BACKTEST_UPSERT_SQL as q

        stripped = re.sub(r"%\([a-z_0-9]+\)s", "", q).replace("%%", "")
        assert "%" not in stripped

    def test_json_and_array_columns_are_explicitly_cast(self):
        from scripts.backtest_preseason import _BACKTEST_UPSERT_SQL

        assert "%(calibration)s::jsonb" in _BACKTEST_UPSERT_SQL
        assert "%(seasons_covered)s::bigint[]" in _BACKTEST_UPSERT_SQL


class TestStoredPrecision:
    """NUMERIC(6,3) on the quantile columns must not silently truncate a value
    the gate line prints -- e.g. resid_p10 = -2.684."""

    @staticmethod
    def _numeric(value, precision, scale):
        from decimal import ROUND_HALF_UP, Decimal

        quantized = Decimal(repr(value)).quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
        assert len(quantized.as_tuple().digits) <= precision, (
            f"{value} overflows NUMERIC({precision},{scale})"
        )
        return quantized

    def test_gate_line_quantiles_round_trip_exactly(self):
        from decimal import Decimal

        for printed in ("-2.684", "3.022", "1.743", "2.168", "-0.126"):
            assert self._numeric(float(printed), 6, 3) == Decimal(printed)

    def test_win_error_metrics_fit_the_declared_precision(self):
        """Six digits at scale 3 tops out at 999.999 wins of error; a backtest
        that produced more than that has a different problem."""
        row = _row()
        for col in ("win_mae", "rmse", "bias", "resid_p05", "resid_p95"):
            self._numeric(row[col], 6, 3)

    def test_coverage_keeps_more_precision_than_the_gate_prints(self):
        """Stored at 4 decimals against the gate's 3, so nothing is lost."""
        from decimal import Decimal

        assert self._numeric(0.7996, 5, 4) == Decimal("0.7996")

    def test_brier_scale_holds_the_printed_four_decimals(self):
        from decimal import Decimal

        assert self._numeric(0.1637, 6, 5) == Decimal("0.16370")
