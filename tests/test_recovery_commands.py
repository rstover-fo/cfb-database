import subprocess

import pytest

from scripts.recover_season_projections import execute_commands


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

    monkeypatch.setattr("scripts.train_model.fetch_refit_state", lambda conn: (frontier, fits))
    monkeypatch.setattr("scripts.score_fitted.fetch_pending_game_counts", lambda conn: pending)
    with pytest.raises(RuntimeError, match="Recovery requires"):
        check_recovery_state(object())


def test_preflight_accepts_approved_fit_and_target(monkeypatch):
    from scripts.recover_season_projections import check_recovery_state

    monkeypatch.setattr("scripts.train_model.fetch_refit_state", lambda conn: (2025, [2024, 2025]))
    monkeypatch.setattr("scripts.score_fitted.fetch_pending_game_counts", lambda conn: {2026: 10})
    check_recovery_state(object())
