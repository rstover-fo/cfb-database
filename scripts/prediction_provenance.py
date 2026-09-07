"""Canonical provenance helpers shared by prediction writers.

F04 stores two separate immutable records: a content-addressed model artifact
describing the implementation and parameters actually consumed, and a
per-prediction input snapshot.  This module keeps their serialization and
hashing contract identical across the closed-form and fitted writers.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import math
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any

ARTIFACT_SCHEMA_VERSION = 1
INPUT_SCHEMA_VERSION = 1

PUBLISHED_FORECAST = "published_forecast"
WALK_FORWARD_RECONSTRUCTION = "walk_forward_reconstruction"
HINDSIGHT_EXPERIMENT = "hindsight_experiment"

NEW_EVALUATION_MODES = frozenset(
    {PUBLISHED_FORECAST, WALK_FORWARD_RECONSTRUCTION, HINDSIGHT_EXPERIMENT}
)


def json_value(value: Any) -> Any:
    """Return a deterministic JSON-compatible representation of ``value``.

    Database timestamps are retained as ISO-8601 strings, mappings are copied
    with string keys, and numpy-like scalar/array values are converted through
    their public ``item``/``tolist`` protocols.  Non-finite numbers are rejected
    because PostgreSQL JSONB cannot preserve them as ordinary JSON numbers.
    """
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        # Preserve the exact database numeric representation. Scoring code may
        # separately coerce it to float, but the source snapshot must not lose
        # digits before it is hashed and retained.
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("prediction provenance cannot contain non-finite numbers")
        return value
    if isinstance(value, Mapping):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [json_value(item) for item in value]
    if hasattr(value, "tolist"):
        return json_value(value.tolist())
    if hasattr(value, "item"):
        return json_value(value.item())
    raise TypeError(f"unsupported prediction provenance value: {type(value).__name__}")


def canonical_json(value: Any) -> str:
    """Serialize ``value`` with the stable F04 canonical JSON encoding."""
    return json.dumps(
        json_value(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def canonical_hash(value: Any) -> str:
    """Return the lowercase SHA-256 hex digest of canonical JSON ``value``."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def implementation_fingerprint(*objects: Callable[..., Any] | Any) -> dict[str, Any]:
    """Fingerprint complete modules or individual callables consumed by a model."""
    sources = []
    for obj in objects:
        if inspect.ismodule(obj):
            module = obj.__name__
            qualname = "<module>"
        else:
            module = obj.__module__
            qualname = obj.__qualname__
        sources.append(
            {
                "module": module,
                "qualname": qualname,
                "source": inspect.getsource(obj),
            }
        )
    return {"algorithm": "sha256", "digest": canonical_hash(sources)}


def identify_model_artifact(model_version: str, artifact: Mapping[str, Any]) -> tuple[str, dict]:
    """Normalize an artifact and return its content-addressed ``fit_id``."""
    normalized = json_value(artifact)
    address = {"model_version": model_version, "artifact": normalized}
    return canonical_hash(address), normalized


def attach_prediction_provenance(
    row: Mapping[str, Any],
    *,
    evaluation_mode: str,
    fit_id: str,
    model_artifact: Mapping[str, Any],
    input_snapshot: Mapping[str, Any],
    simulated_as_of_at: datetime | None = None,
    experiment_label: str | None = None,
) -> dict[str, Any]:
    """Return ``row`` with validated F04 provenance fields attached.

    ``_model_artifact`` is an internal writer value.  It is inserted into
    ``predictions.model_artifacts`` in the same transaction and is deliberately
    excluded from ``predictions.game_predictions``.
    """
    if evaluation_mode not in NEW_EVALUATION_MODES:
        raise ValueError(f"unsupported new prediction evaluation mode {evaluation_mode!r}")
    if not fit_id:
        raise ValueError("new predictions require fit_id")
    if evaluation_mode == PUBLISHED_FORECAST:
        if simulated_as_of_at is not None:
            raise ValueError("published forecasts cannot have simulated_as_of_at")
        if experiment_label is not None:
            raise ValueError("published forecasts cannot have experiment_label")
    else:
        if simulated_as_of_at is None:
            raise ValueError("historical predictions require a known simulated_as_of_at")
        if evaluation_mode == HINDSIGHT_EXPERIMENT:
            experiment_label = (experiment_label or "").strip()
            if not experiment_label:
                raise ValueError("hindsight experiments require a nonempty label")
        elif experiment_label is not None:
            raise ValueError("walk-forward reconstructions cannot have experiment_label")

    normalized_snapshot = json_value(input_snapshot)
    return {
        **row,
        "evaluation_mode": evaluation_mode,
        "simulated_as_of_at": simulated_as_of_at,
        "experiment_label": experiment_label,
        "fit_id": fit_id,
        "input_hash": canonical_hash(normalized_snapshot),
        "input_snapshot": normalized_snapshot,
        "_model_artifact": json_value(model_artifact),
    }


def insert_model_artifacts(conn, rows: Sequence[Mapping[str, Any]]) -> None:
    """Insert each distinct content-addressed artifact without mutating one."""
    from psycopg2.extras import Json, execute_values

    artifacts: dict[str, tuple[str, dict]] = {}
    for row in rows:
        fit_id = row["fit_id"]
        model_version = row["model_version"]
        artifact = row["_model_artifact"]
        candidate = (model_version, artifact)
        prior = artifacts.setdefault(fit_id, candidate)
        if prior != candidate:
            raise ValueError(f"fit_id {fit_id} maps to conflicting artifacts in one write")
    if not artifacts:
        return

    values = [
        (fit_id, model_version, Json(artifact))
        for fit_id, (model_version, artifact) in artifacts.items()
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO predictions.model_artifacts (fit_id, model_version, artifact)
            VALUES %s
            ON CONFLICT (fit_id) DO NOTHING
            """,
            values,
        )
