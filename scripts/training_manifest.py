"""Deterministic manifests for immutable fitted-model training runs.

The manifest describes the inputs and implementation that produced a fit.  It
does not archive source rows or claim that an older warehouse revision can be
reconstructed.  Canonical serialization is shared with prediction provenance
so training and scoring use one content-addressing contract.
"""

from __future__ import annotations

import platform
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np

from scripts.prediction_provenance import canonical_hash, json_value

MANIFEST_SCHEMA_VERSION = 1
KNOWN_LINEAGE = "known"
LEGACY_UNKNOWN_LINEAGE = "legacy_unknown"


def source_text_fingerprint(sources: Mapping[str, str]) -> dict[str, str]:
    """Hash named source text with the repository's canonical JSON contract."""
    return {"algorithm": "sha256", "digest": canonical_hash(dict(sources))}


def runtime_contract() -> dict[str, str]:
    """Runtime versions whose numerical behavior is part of fit freshness."""
    return {"python": platform.python_version(), "numpy": np.__version__}


def git_revision() -> str:
    """Return the checked-out commit, or ``unknown`` outside a git checkout."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            cwd=Path(__file__).resolve().parents[1],
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    revision = result.stdout.strip().lower()
    return revision if len(revision) == 40 else "unknown"


def training_data_fingerprint(games: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Identify the exact ordered game rows consumed by a training run.

    The digest covers every value passed to vectorization, including both
    team-week sides and the targets.  Only the digest and count are retained;
    this intentionally makes no input-archive claim.
    """
    normalized = json_value(list(games))
    return {
        "algorithm": "sha256",
        "digest": canonical_hash(normalized),
        "row_count": len(normalized),
    }


def build_training_manifest(
    *,
    feature_names: Sequence[str],
    team_week_source_columns: Sequence[str],
    diff_feature_columns: Sequence[Sequence[str]],
    implementation: Mapping[str, Any],
    train_seasons: Sequence[int],
    games: Sequence[Mapping[str, Any]],
    ridge_alpha: float,
    winprob_ridge_alpha: float,
    runtime: Mapping[str, str] | None = None,
    code_revision: str | None = None,
) -> dict[str, Any]:
    """Build a complete known-lineage training manifest."""
    seasons = [int(season) for season in train_seasons]
    if not seasons:
        raise ValueError("training manifest requires at least one training season")
    if seasons != list(range(seasons[0], seasons[-1] + 1)):
        raise ValueError("training seasons must be contiguous and ordered")

    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "lineage": KNOWN_LINEAGE,
        "feature_contract": {
            "feature_names": list(feature_names),
            "team_week_source_columns": list(team_week_source_columns),
            "diff_feature_columns": [list(pair) for pair in diff_feature_columns],
        },
        "implementation": json_value(implementation),
        "training_window": {
            "start_season": seasons[0],
            "train_through_season": seasons[-1],
            "seasons": seasons,
            "cutoff": {
                "kind": "season_end",
                "season": seasons[-1],
                "inclusive": True,
            },
        },
        "training_data": training_data_fingerprint(games),
        "hyperparameters": {
            "ridge_alpha": float(ridge_alpha),
            "winprob_ridge_alpha": float(winprob_ridge_alpha),
        },
        "runtime": dict(runtime or runtime_contract()),
        "code_revision": code_revision or git_revision(),
        "calibration": {
            "method": "platt",
            "population": "training_logits",
            "transform": "sigmoid(a * logit + b)",
        },
    }


def freshness_payload(manifest: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return fields that determine freshness, excluding git bookkeeping.

    The commit is retained in immutable identity and audit history.  Excluding
    it here prevents an unrelated repository commit from forcing every vintage
    to retrain when inputs, transformations, numerical contract, and runtime
    are unchanged.
    """
    if manifest.get("lineage") != KNOWN_LINEAGE:
        return None
    required = (
        "schema_version",
        "feature_contract",
        "implementation",
        "training_window",
        "training_data",
        "hyperparameters",
        "runtime",
        "calibration",
    )
    if any(key not in manifest for key in required):
        return None
    return {key: json_value(manifest[key]) for key in required}


def manifests_match_for_freshness(stored: Mapping[str, Any], current: Mapping[str, Any]) -> bool:
    """Whether a selected fit was produced from the current reproducible spec."""
    stored_payload = freshness_payload(stored)
    current_payload = freshness_payload(current)
    return stored_payload is not None and stored_payload == current_payload
