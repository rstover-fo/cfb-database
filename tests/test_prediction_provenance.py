"""F04 regression coverage for immutable prediction writer provenance."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, Mock

import numpy as np
import pytest

from scripts import compute_predictions as closed_form
from scripts import score_fitted as fitted
from scripts import train_model as training
from scripts.prediction_provenance import (
    HINDSIGHT_EXPERIMENT,
    PUBLISHED_FORECAST,
    WALK_FORWARD_RECONSTRUCTION,
    attach_prediction_provenance,
    canonical_hash,
    canonical_json,
    identify_model_artifact,
)


def _closed_form_game(*, start_date=None):
    return {
        "game_id": 101,
        "season": 2026,
        "week": 3,
        "season_type": "regular",
        "start_date": start_date,
        "neutral_site": False,
        "home_team": "Alpha",
        "away_team": "Bravo",
        "home_pregame_elo": 1610.0,
        "away_pregame_elo": 1490.0,
    }


def _fit(train_through=2025):
    return {
        "train_through": train_through,
        "feature_means": dict.fromkeys(training.TEAM_WEEK_SOURCE_COLUMNS, 0.0),
        "diff_means": dict.fromkeys(training.FEATURE_NAMES, 0.0),
        "diff_stds": dict.fromkeys(training.FEATURE_NAMES, 1.0),
        "beta_margin": np.arange(len(training.FEATURE_NAMES), dtype=float),
        "beta_winprob": np.arange(len(training.FEATURE_NAMES), dtype=float) / 10,
        "platt_a": 0.75,
        "platt_b": -0.1,
    }


def test_canonical_hash_is_order_independent_and_lowercase_sha256():
    first = {"b": [2, 3], "a": {"kickoff": datetime(2026, 9, 5, 17, tzinfo=UTC)}}
    second = {"a": {"kickoff": datetime(2026, 9, 5, 17, tzinfo=UTC)}, "b": [2, 3]}
    digest = canonical_hash(first)
    assert digest == canonical_hash(second)
    assert len(digest) == 64
    assert digest == digest.lower()
    assert set(digest) <= set("0123456789abcdef")


def test_canonical_json_handles_arrays_and_preserves_decimal_precision():
    value = {"vector": np.array([1, 2]), "numeric": Decimal("1.2300000000000000001")}
    assert canonical_json(value) == '{"numeric":"1.2300000000000000001","vector":[1,2]}'


def test_fit_id_addresses_model_version_and_exact_artifact():
    fit_id, normalized = identify_model_artifact("elo_v1", {"weight": 0.6})
    same_id, _ = identify_model_artifact("elo_v1", {"weight": 0.6})
    changed_id, _ = identify_model_artifact("elo_v2", {"weight": 0.6})
    assert fit_id == same_id
    assert fit_id != changed_id
    assert normalized == {"weight": 0.6}


def test_mode_validation_and_input_hash_cover_exact_snapshot():
    snapshot = {"market": {"spread": -3.5, "captured_at": "2026-09-01T12:00:00+00:00"}}
    row = attach_prediction_provenance(
        {"model_version": "elo_v1"},
        evaluation_mode=PUBLISHED_FORECAST,
        fit_id="a" * 64,
        model_artifact={"parameters": {}},
        input_snapshot=snapshot,
    )
    assert row["input_hash"] == canonical_hash(snapshot)
    assert row["simulated_as_of_at"] is None
    assert row["experiment_label"] is None

    with pytest.raises(ValueError, match="known simulated_as_of_at"):
        attach_prediction_provenance(
            {},
            evaluation_mode=WALK_FORWARD_RECONSTRUCTION,
            fit_id="a" * 64,
            model_artifact={},
            input_snapshot={},
        )
    with pytest.raises(ValueError, match="nonempty label"):
        attach_prediction_provenance(
            {},
            evaluation_mode=HINDSIGHT_EXPERIMENT,
            fit_id="a" * 64,
            model_artifact={},
            input_snapshot={},
            simulated_as_of_at=datetime(2026, 9, 1, tzinfo=UTC),
            experiment_label="   ",
        )


def test_prediction_insert_is_append_only_and_omits_db_owned_timestamps():
    sql = closed_form._INSERT_SQL.upper()
    assert "ON CONFLICT" not in sql
    assert "DO UPDATE" not in sql
    columns = closed_form._INSERT_SQL.split(") VALUES", 1)[0]
    assert "created_at" not in columns
    assert "published_at" not in columns


def test_upcoming_closed_form_rows_capture_consumed_inputs(monkeypatch):
    game = _closed_form_game(start_date=datetime(2026, 9, 12, 18, tzinfo=UTC))
    market_time = datetime(2026, 9, 6, 12, 30, tzinfo=UTC)
    market = {"provider": "consensus", "spread": -4.5, "captured_at": market_time}
    epa = {
        ("Alpha", 2025): {
            "team": "Alpha",
            "season": 2025,
            "off_coef": 0.2,
            "def_coef": -0.1,
            "hfa_coef": 0.03,
        },
        ("Bravo", 2025): {
            "team": "Bravo",
            "season": 2025,
            "off_coef": 0.1,
            "def_coef": -0.05,
            "hfa_coef": 0.02,
        },
    }
    write = Mock()
    monkeypatch.setattr(closed_form, "fetch_target_games", lambda _conn: [game])
    monkeypatch.setattr(
        closed_form,
        "fetch_elo_current",
        lambda _conn: {"Alpha": (1610.0, 2026), "Bravo": (1490.0, 2026)},
    )
    monkeypatch.setattr(closed_form, "fetch_epa_coefs", lambda _conn: epa)
    monkeypatch.setattr(closed_form, "table_exists", lambda *_args: True)
    monkeypatch.setattr(
        closed_form, "fetch_market_from_snapshots", lambda _conn, _ids: {101: market}
    )
    monkeypatch.setattr(closed_form, "write_upcoming", write)

    closed_form.run_upcoming(object())

    rows = write.call_args.args[1]
    assert {row["evaluation_mode"] for row in rows} == {PUBLISHED_FORECAST}
    assert all(len(row["fit_id"]) == 64 for row in rows)
    elo = next(row for row in rows if row["model_version"] == closed_form.MODEL_ELO)
    blend = next(row for row in rows if row["model_version"] == closed_form.MODEL_BLEND)
    assert elo["input_snapshot"]["elo"] == {"home": 1610.0, "away": 1490.0}
    assert elo["input_snapshot"]["market"]["captured_at"] == market_time.isoformat()
    assert "epa" not in elo["input_snapshot"]
    assert blend["input_snapshot"]["epa"]["home"]["off_coef"] == 0.2
    assert blend["input_hash"] == canonical_hash(blend["input_snapshot"])


def test_closed_form_backfill_defaults_to_reconstruction(monkeypatch):
    kickoff = datetime(2026, 9, 5, 19, tzinfo=UTC)
    game = _closed_form_game(start_date=kickoff)
    write = Mock()
    week = {
        "Alpha": [(3, {"off_coef": 0.2, "def_coef": -0.1, "hfa_coef": 0.03, "plays": 200})],
        "Bravo": [(3, {"off_coef": 0.1, "def_coef": -0.05, "hfa_coef": 0.02, "plays": 200})],
    }
    monkeypatch.setattr(closed_form, "fetch_backfill_games", lambda _conn, _season: [game])
    monkeypatch.setattr(closed_form, "fetch_epa_week_coefs", lambda _conn, _season: week)
    monkeypatch.setattr(closed_form, "fetch_epa_coefs", lambda _conn, season=None: {})
    monkeypatch.setattr(closed_form, "fetch_market_from_lines", lambda _conn, _ids: {})
    monkeypatch.setattr(closed_form, "write_backfill_season", write)

    closed_form.run_backfill(object(), 2026, 2026)

    rows = write.call_args.args[1]
    assert {row["evaluation_mode"] for row in rows} == {WALK_FORWARD_RECONSTRUCTION}
    assert {row["simulated_as_of_at"] for row in rows} == {kickoff}
    assert {row["prediction_date"] for row in rows} == {kickoff.date()}


def test_closed_form_hindsight_requires_label_and_is_explicit(monkeypatch):
    kickoff = datetime(2026, 9, 5, 19, tzinfo=UTC)
    game = _closed_form_game(start_date=kickoff)
    write = Mock()
    full = {
        ("Alpha", 2026): {"off_coef": 0.2, "def_coef": -0.1, "hfa_coef": 0.03},
        ("Bravo", 2026): {"off_coef": 0.1, "def_coef": -0.05, "hfa_coef": 0.02},
    }
    monkeypatch.setattr(closed_form, "fetch_backfill_games", lambda _conn, _season: [game])
    monkeypatch.setattr(closed_form, "fetch_epa_coefs", lambda _conn, season=None: full)
    monkeypatch.setattr(closed_form, "fetch_market_from_lines", lambda _conn, _ids: {})
    monkeypatch.setattr(closed_form, "write_backfill_season", write)

    closed_form.run_backfill(
        object(), 2026, 2026, as_of_week=False, hindsight_experiment="full-season-v1"
    )
    assert {row["evaluation_mode"] for row in write.call_args.args[1]} == {HINDSIGHT_EXPERIMENT}
    assert {row["experiment_label"] for row in write.call_args.args[1]} == {"full-season-v1"}

    with pytest.raises(ValueError, match="requires --hindsight-experiment"):
        closed_form.run_backfill(object(), 2026, 2026, as_of_week=False)


def test_backfill_missing_kickoff_fails_before_any_write(monkeypatch):
    write = Mock()
    monkeypatch.setattr(
        closed_form, "fetch_backfill_games", lambda _conn, _season: [_closed_form_game()]
    )
    monkeypatch.setattr(closed_form, "fetch_epa_week_coefs", lambda _conn, _season: {})
    monkeypatch.setattr(closed_form, "fetch_epa_coefs", lambda _conn, season=None: {})
    monkeypatch.setattr(closed_form, "fetch_market_from_lines", lambda _conn, _ids: {})
    monkeypatch.setattr(closed_form, "write_backfill_season", write)
    with pytest.raises(ValueError, match="has no kickoff"):
        closed_form.run_backfill(object(), 2026, 2026)
    write.assert_not_called()


def test_fitted_artifact_captures_loaded_coefficients_scaling_and_calibration():
    fit = _fit()
    fit_id, artifact = fitted.fitted_model_artifact(fit)
    assert len(fit_id) == 64
    assert artifact["feature_means"] == fit["feature_means"]
    assert artifact["feature_diff_stds"] == fit["diff_stds"]
    assert artifact["calibration"] == {"platt_a": 0.75, "platt_b": -0.1}
    assert artifact["coefficients"][3] == {
        "feature_order": 3,
        "feature_name": training.FEATURE_NAMES[3],
        "margin": 3.0,
        "winprob": 0.3,
    }


def test_fitted_artifact_references_immutable_training_fit_when_known():
    fit = {**_fit(), "training_fit_id": "b" * 64}

    _, artifact = fitted.fitted_model_artifact(fit)

    assert artifact["training_fit_id"] == "b" * 64


def test_fitted_fit_id_changes_when_consumed_runtime_constant_changes(monkeypatch):
    fit = _fit()
    original_id, _ = fitted.fitted_model_artifact(fit)
    monkeypatch.setattr(training, "_MAX_LOGIT", 2.0)
    changed_id, artifact = fitted.fitted_model_artifact(fit)
    assert changed_id != original_id
    assert artifact["implementation"]["runtime_constants"]["max_logit"] == 2.0


def _registry_parameters():
    return {
        "feature_names": list(training.FEATURE_NAMES),
        "feature_means": dict.fromkeys(training.TEAM_WEEK_SOURCE_COLUMNS, 0.0),
        "diff_means": dict.fromkeys(training.FEATURE_NAMES[2:], 0.0),
        "diff_stds": dict.fromkeys(training.FEATURE_NAMES[2:], 1.0),
        "beta_margin": [0.0] * len(training.FEATURE_NAMES),
        "beta_winprob": [0.0] * len(training.FEATURE_NAMES),
        "platt_a": 1.0,
        "platt_b": 0.0,
    }


def test_load_fit_reads_selected_registry_parameters_and_returns_training_fit_id(monkeypatch):
    selected = {
        "training_fit_id": "c" * 64,
        "manifest": {"lineage": "native"},
        "parameters": _registry_parameters(),
    }
    load = Mock(return_value=selected)
    monkeypatch.setattr(fitted, "load_selected_fit", load)
    conn = object()

    fit = fitted.load_fit(conn, 2025)

    load.assert_called_once_with(conn, training.MODEL_VERSION, 2025)
    assert fit["training_fit_id"] == "c" * 64
    assert fit["train_through"] == 2025
    assert fit["beta_margin"].shape == (len(training.FEATURE_NAMES),)


def test_fetch_available_train_through_uses_only_selected_registry_fits(monkeypatch):
    selected = Mock(
        return_value=[
            {"train_through_season": 2023},
            {"train_through_season": 2025},
        ]
    )
    monkeypatch.setattr(fitted, "selected_fits", selected)
    conn = object()

    assert fitted.fetch_available_train_through(conn) == [2023, 2025]
    selected.assert_called_once_with(conn, training.MODEL_VERSION)


def test_scoring_locks_deployment_pointers_until_writer_commit():
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value

    fitted.lock_selected_fits(conn)

    cursor.execute.assert_called_once_with("LOCK TABLE features.model_deployments IN SHARE MODE")
    conn.commit.assert_not_called()


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda p: p.update(feature_names=list(reversed(p["feature_names"]))), "FEATURE_NAMES"),
        (lambda p: p.update(beta_margin=p["beta_margin"][:-1]), "dimensions"),
        (lambda p: p["diff_means"].update(d_elo=float("nan")), "non-finite"),
        (lambda p: p["diff_stds"].update(d_elo=-1.0), "negative diff_stds"),
    ],
)
def test_load_fit_rejects_incompatible_or_invalid_selected_parameters(
    monkeypatch, mutation, message
):
    parameters = _registry_parameters()
    mutation(parameters)
    monkeypatch.setattr(
        fitted,
        "load_selected_fit",
        lambda *_args: {
            "training_fit_id": "d" * 64,
            "manifest": {},
            "parameters": parameters,
        },
    )

    with pytest.raises(ValueError, match=message):
        fitted.load_fit(object(), 2025)
