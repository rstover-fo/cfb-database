"""Immutable fitted-model registry and explicit deployment pointers.

The registry deliberately leaves transaction ownership to its callers.  A
training command can therefore insert a candidate, optionally promote it, and
commit those changes atomically only after its own lifecycle checks pass.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from scripts.prediction_provenance import canonical_hash, canonical_json, json_value

PARAMETER_KEYS = frozenset(
    {
        "feature_names",
        "feature_means",
        "diff_means",
        "diff_stds",
        "beta_margin",
        "beta_winprob",
        "platt_a",
        "platt_b",
    }
)


def _nonempty_model_version(model_version: str) -> str:
    if not isinstance(model_version, str) or not model_version.strip():
        raise ValueError("model_version must be a nonempty string")
    return model_version.strip()


def _train_through(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("train_through must be an integer season")
    return value


def _normalize_object(value: Mapping[str, Any], name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a JSON object")
    normalized = json_value(value)
    if not isinstance(normalized, dict):  # defensive: json_value preserves mappings
        raise ValueError(f"{name} must be a JSON object")
    return normalized


def _normalize_parameters(parameters: Mapping[str, Any]) -> dict[str, Any]:
    normalized = _normalize_object(parameters, "parameters")
    keys = frozenset(normalized)
    if keys != PARAMETER_KEYS:
        missing = sorted(PARAMETER_KEYS - keys)
        extra = sorted(keys - PARAMETER_KEYS)
        raise ValueError(
            f"parameters must have the exact registry keys; missing={missing}, extra={extra}"
        )
    return normalized


def identify_training_fit(
    model_version: str,
    train_through: int,
    manifest: Mapping[str, Any],
    parameters: Mapping[str, Any],
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    """Normalize a training snapshot and return its canonical SHA-256 address."""
    version = _nonempty_model_version(model_version)
    season = _train_through(train_through)
    normalized_manifest = _normalize_object(manifest, "manifest")
    normalized_parameters = _normalize_parameters(parameters)
    address = {
        "model_version": version,
        "train_through_season": season,
        "manifest": normalized_manifest,
        "parameters": normalized_parameters,
    }
    return canonical_hash(address), normalized_manifest, normalized_parameters


def insert_fit(
    conn,
    model_version: str,
    train_through: int,
    manifest: Mapping[str, Any],
    parameters: Mapping[str, Any],
) -> str:
    """Insert one immutable fit, verifying an existing content address on conflict.

    This function never commits or rolls back.  PostgreSQL JSONB receives the
    same normalized JSON used by the content-address calculation.
    """
    version = _nonempty_model_version(model_version)
    season = _train_through(train_through)
    training_fit_id, normalized_manifest, normalized_parameters = identify_training_fit(
        version, season, manifest, parameters
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO features.training_fits
                (training_fit_id, model_version, train_through_season, manifest, parameters)
            VALUES (%s, %s, %s, %s::jsonb, %s::jsonb)
            ON CONFLICT (training_fit_id) DO NOTHING
            RETURNING training_fit_id
            """,
            (
                training_fit_id,
                version,
                season,
                canonical_json(normalized_manifest),
                canonical_json(normalized_parameters),
            ),
        )
        inserted = cur.fetchone()
        if inserted is not None:
            return training_fit_id

        cur.execute(
            """
            SELECT model_version, train_through_season, manifest, parameters
            FROM features.training_fits
            WHERE training_fit_id = %s
            """,
            (training_fit_id,),
        )
        existing = cur.fetchone()

    if existing is None:
        raise RuntimeError(f"training fit {training_fit_id} conflicted but could not be loaded")
    actual = (
        tuple(existing)
        if not isinstance(existing, Mapping)
        else (
            existing["model_version"],
            existing["train_through_season"],
            existing["manifest"],
            existing["parameters"],
        )
    )
    expected = (version, season, normalized_manifest, normalized_parameters)
    if actual != expected:
        raise ValueError(f"training_fit_id {training_fit_id} maps to conflicting contents")
    return training_fit_id


def load_selected_fit(conn, model_version: str, train_through: int) -> dict[str, Any]:
    """Load the explicitly selected fit for one model vintage."""
    version = _nonempty_model_version(model_version)
    season = _train_through(train_through)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.training_fit_id, f.manifest, f.parameters
            FROM features.model_deployments AS d
            JOIN features.training_fits AS f
              ON (f.training_fit_id, f.model_version, f.train_through_season) =
                 (d.training_fit_id, d.model_version, d.train_through_season)
            WHERE d.model_version = %s AND d.train_through_season = %s
            """,
            (version, season),
        )
        row = cur.fetchone()
    if row is None:
        raise LookupError(f"no selected fit for model_version={version!r}, train_through={season}")
    return {
        "training_fit_id": row[0],
        "manifest": row[1],
        "parameters": row[2],
    }


def load_fit(conn, training_fit_id: str) -> dict[str, Any]:
    """Load one candidate by content address for promotion-time validation."""
    if not isinstance(training_fit_id, str) or not training_fit_id:
        raise ValueError("training_fit_id must be a nonempty string")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT training_fit_id, model_version, train_through_season,
                   manifest, parameters
            FROM features.training_fits
            WHERE training_fit_id = %s
            """,
            (training_fit_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise LookupError(f"unknown training_fit_id {training_fit_id}")
    return {
        "training_fit_id": row[0],
        "model_version": row[1],
        "train_through_season": row[2],
        "manifest": row[3],
        "parameters": row[4],
    }


def selected_fits(conn, model_version: str) -> list[dict[str, Any]]:
    """Return every selected vintage for ``model_version``, oldest first."""
    version = _nonempty_model_version(model_version)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT d.train_through_season, f.training_fit_id, f.manifest, f.parameters
            FROM features.model_deployments AS d
            JOIN features.training_fits AS f
              ON (f.training_fit_id, f.model_version, f.train_through_season) =
                 (d.training_fit_id, d.model_version, d.train_through_season)
            WHERE d.model_version = %s
            ORDER BY d.train_through_season
            """,
            (version,),
        )
        rows = cur.fetchall()
    return [
        {
            "train_through_season": row[0],
            "training_fit_id": row[1],
            "manifest": row[2],
            "parameters": row[3],
        }
        for row in rows
    ]


def promote_fit(conn, training_fit_id: str, reason: str) -> None:
    """Select a candidate at its own model/vintage key and retain DB audit history."""
    if not isinstance(training_fit_id, str) or not training_fit_id:
        raise ValueError("training_fit_id must be a nonempty string")
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("promotion reason must be nonempty")
    reason = reason.strip()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT model_version, train_through_season
            FROM features.training_fits
            WHERE training_fit_id = %s
            """,
            (training_fit_id,),
        )
        fit = cur.fetchone()
        if fit is None:
            raise LookupError(f"unknown training_fit_id {training_fit_id}")
        cur.execute(
            """
            INSERT INTO features.model_deployments
                (model_version, train_through_season, training_fit_id, promotion_reason)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (model_version, train_through_season) DO UPDATE
            SET training_fit_id = EXCLUDED.training_fit_id,
                promotion_reason = EXCLUDED.promotion_reason,
                promoted_at = statement_timestamp()
            """,
            (fit[0], fit[1], training_fit_id, reason),
        )


def _legacy_parameters(coefficient_rows: list[tuple], metadata: tuple) -> dict[str, Any]:
    components: dict[str, list[tuple[int, str, Any]]] = {"margin": [], "winprob": []}
    for component, order, name, coefficient in coefficient_rows:
        if component not in components:
            raise ValueError(f"unsupported legacy model component {component!r}")
        components[component].append((int(order), name, coefficient))
    margin = sorted(components["margin"])
    winprob = sorted(components["winprob"])
    if not margin or [(o, n) for o, n, _ in margin] != [(o, n) for o, n, _ in winprob]:
        raise ValueError("legacy margin/winprob coefficient contracts do not match")
    return {
        "feature_names": [name for _, name, _ in margin],
        "feature_means": metadata[6],
        "diff_means": metadata[7],
        "diff_stds": metadata[8],
        "beta_margin": [value for _, _, value in margin],
        "beta_winprob": [value for _, _, value in winprob],
        "platt_a": metadata[2],
        "platt_b": metadata[3],
    }


def import_legacy_fits(conn, model_version: str) -> list[str]:
    """Snapshot compatible legacy fit rows without selecting them for deployment."""
    version = _nonempty_model_version(model_version)
    with conn.cursor() as cur:
        # Keep the metadata and coefficient reads on the same legacy generation
        # even if an older trainer is still connected during an import.
        cur.execute("LOCK TABLE features.model_metadata, features.model_coefficients IN SHARE MODE")
        cur.execute(
            """
            SELECT train_through_season, train_seasons, platt_a, platt_b,
                   ridge_alpha, winprob_ridge_alpha, feature_means,
                   feature_diff_means, feature_diff_stds, n_train_games
            FROM features.model_metadata
            WHERE model_version = %s
            ORDER BY train_through_season
            """,
            (version,),
        )
        metadata_rows = cur.fetchall()
        cur.execute(
            """
            SELECT train_through_season, model_component, feature_order,
                   feature_name, coefficient
            FROM features.model_coefficients
            WHERE model_version = %s
            ORDER BY train_through_season, model_component, feature_order
            """,
            (version,),
        )
        coefficient_rows = cur.fetchall()

    coefficients_by_season: dict[int, list[tuple]] = {}
    for season, component, order, name, coefficient in coefficient_rows:
        coefficients_by_season.setdefault(int(season), []).append(
            (component, order, name, coefficient)
        )

    imported: list[str] = []
    for metadata in metadata_rows:
        season = int(metadata[0])
        parameters = _legacy_parameters(coefficients_by_season.get(season, []), metadata)
        manifest = {
            "schema_version": 1,
            "lineage": "legacy_unknown",
            "feature_contract": {
                "feature_names": parameters["feature_names"],
                "team_week_source_columns": None,
                "diff_feature_columns": None,
            },
            "implementation": None,
            "training_window": {
                "start_season": min(metadata[1]) if metadata[1] else None,
                "train_through_season": season,
                "seasons": metadata[1],
            },
            "training_data": {
                "algorithm": "sha256",
                "digest": None,
                "row_count": metadata[9],
            },
            "hyperparameters": {
                "ridge_alpha": metadata[4],
                "winprob_ridge_alpha": metadata[5],
            },
            "runtime": None,
            "code_revision": None,
            "calibration": {
                "method": "platt",
                "population": None,
                "transform": "sigmoid(a * logit + b)",
            },
        }
        imported.append(insert_fit(conn, version, season, manifest, parameters))
    return imported
