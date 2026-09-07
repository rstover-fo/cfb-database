import subprocess
from pathlib import Path

import pytest

from scripts.recover_season_projections import COMMANDS, execute_commands


def test_compute_failure_stops_downstream_writes(monkeypatch):
    calls = []

    def fail(command, *, check):
        calls.append(command)
        assert check
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        execute_commands((("compute_adjusted_epa",), ("simulate_season",)))
    assert len(calls) == 1


def test_recovery_replays_repaired_elo_prefix_before_2026_dependents(monkeypatch):
    calls = []

    def record(command, *, check):
        assert check
        calls.append((Path(command[1]).stem, *command[2:]))

    monkeypatch.setattr(subprocess, "run", record)

    execute_commands()

    assert calls[1:4] == [
        ("compute_house_elo", "--season", "2024"),
        ("compute_house_elo", "--season", "2025"),
        ("compute_house_elo", "--season", "2026"),
    ]
    assert calls.index(("compute_house_elo", "--season", "2026")) < calls.index(
        ("compute_adjusted_epa", "--season", "2026")
    )
    assert calls.index(("compute_house_elo", "--season", "2026")) < calls.index(
        ("build_features", "--season", "2026")
    )


def test_final_refresh_updates_data_freshness_after_consumer_marts():
    final_refresh = COMMANDS[-1]

    assert final_refresh[0] == "refresh_marts"
    assert final_refresh[1] == "--views"
    assert final_refresh[2].endswith(",marts.data_freshness")


@pytest.mark.parametrize(
    "frontier,fits,pending",
    [
        (2024, [2024], {2026: 10}),
        (None, [], {2026: 10}),
        (2025, [2025], {2026: 10, 2027: 1}),
    ],
)
def test_preflight_rejects_ineligible_rebuild_before_command_execution(
    monkeypatch, frontier, fits, pending
):
    from scripts.recover_season_projections import check_recovery_state

    load_fit_calls = []
    monkeypatch.setattr("scripts.train_model.fetch_refit_state", lambda conn: (frontier, fits))
    monkeypatch.setattr("scripts.score_fitted.fetch_available_train_through", lambda conn: fits)
    monkeypatch.setattr("scripts.score_fitted.fetch_pending_game_counts", lambda conn: pending)
    monkeypatch.setattr(
        "scripts.score_fitted.load_fit",
        lambda conn, season: load_fit_calls.append((conn, season)),
    )
    with pytest.raises(RuntimeError, match="Recovery requires"):
        check_recovery_state(object())
    assert load_fit_calls == []


def test_preflight_accepts_approved_fit_and_target(monkeypatch):
    from scripts.recover_season_projections import check_recovery_state

    conn = object()
    load_fit_calls = []
    score_calls = []
    fit = object()
    monkeypatch.setattr("scripts.train_model.fetch_refit_state", lambda conn: (2025, [2024, 2025]))
    monkeypatch.setattr(
        "scripts.score_fitted.fetch_available_train_through", lambda conn: [2024, 2025]
    )
    monkeypatch.setattr("scripts.score_fitted.fetch_pending_game_counts", lambda conn: {2026: 10})
    monkeypatch.setattr(
        "scripts.score_fitted.load_fit",
        lambda conn, season: (load_fit_calls.append((conn, season)), fit)[1],
    )
    monkeypatch.setattr(
        "scripts.score_fitted.score_game",
        lambda game, loaded_fit: (score_calls.append((game, loaded_fit)), (1.0, 0.5))[1],
    )

    check_recovery_state(conn)

    assert load_fit_calls == [(conn, 2025)]
    assert len(score_calls) == 1
    game, loaded_fit = score_calls[0]
    assert game["season"] == 2026
    assert game["home_tw"] == game["away_tw"]
    assert loaded_fit is fit


def test_preflight_propagates_invalid_frozen_fit_before_recovery(monkeypatch):
    from scripts.recover_season_projections import check_recovery_state

    monkeypatch.setattr("scripts.train_model.fetch_refit_state", lambda conn: (2025, [2024, 2025]))
    monkeypatch.setattr(
        "scripts.score_fitted.fetch_available_train_through", lambda conn: [2024, 2025]
    )
    monkeypatch.setattr("scripts.score_fitted.fetch_pending_game_counts", lambda conn: {2026: 10})

    def invalid_fit(conn, season):
        raise ValueError(f"missing coefficient vector for {season}")

    monkeypatch.setattr("scripts.score_fitted.load_fit", invalid_fit)

    with pytest.raises(ValueError, match="missing coefficient vector for 2025"):
        check_recovery_state(object())


def test_missing_fit_scaling_stats_stop_before_commands(monkeypatch):
    import scripts.recover_season_projections as recovery
    from scripts.train_model import FEATURE_NAMES, TEAM_WEEK_SOURCE_COLUMNS

    monkeypatch.setattr("scripts.train_model.fetch_refit_state", lambda conn: (2025, [2025]))
    monkeypatch.setattr("scripts.score_fitted.fetch_available_train_through", lambda conn: [2025])
    monkeypatch.setattr("scripts.score_fitted.fetch_pending_game_counts", lambda conn: {2026: 10})
    monkeypatch.setattr(
        "scripts.score_fitted.load_fit",
        lambda conn, season: {
            "train_through": season,
            "feature_means": dict.fromkeys(TEAM_WEEK_SOURCE_COLUMNS, 0.0),
            "diff_means": {},
            "diff_stds": {},
            "platt_a": 1.0,
            "platt_b": 0.0,
            "beta_margin": [0.0] * len(FEATURE_NAMES),
            "beta_winprob": [0.0] * len(FEATURE_NAMES),
        },
    )
    command_calls = []
    monkeypatch.setattr(recovery, "execute_commands", lambda: command_calls.append(True))

    with pytest.raises(KeyError, match="d_elo"):
        recovery.execute_recovery(object())

    assert command_calls == []


def test_preflight_rejects_unpromoted_fit_before_loading(monkeypatch):
    from scripts.recover_season_projections import check_recovery_state

    load_fit = []
    monkeypatch.setattr("scripts.train_model.fetch_refit_state", lambda conn: (2025, [2025]))
    monkeypatch.setattr("scripts.score_fitted.fetch_available_train_through", lambda conn: [])
    monkeypatch.setattr("scripts.score_fitted.fetch_pending_game_counts", lambda conn: {2026: 10})
    monkeypatch.setattr(
        "scripts.score_fitted.load_fit", lambda conn, season: load_fit.append((conn, season))
    )

    with pytest.raises(RuntimeError, match="selected 2025 registry fit"):
        check_recovery_state(object())

    assert load_fit == []


def test_recovery_locks_registry_fit_and_deployment_tables():
    import inspect

    import scripts.recover_season_projections as recovery

    source = inspect.getsource(recovery.main)
    assert "features.training_fits, features.model_deployments" in source
    assert "features.model_metadata" not in source
    assert "features.model_coefficients" not in source
