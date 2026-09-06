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
