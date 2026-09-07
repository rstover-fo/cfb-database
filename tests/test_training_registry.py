"""Pure/mocked regression coverage for the F05 training registry API."""

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from scripts import training_registry as registry


class _Cursor:
    def __init__(self, conn):
        self.conn = conn
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, sql, args=None):
        self.conn.executed.append((sql, args))
        self.rows = list(self.conn.results.pop(0))

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows


class _Connection:
    def __init__(self, *results):
        self.results = list(results)
        self.executed = []
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def _parameters(value=Decimal("1.000000")):
    return {
        "feature_names": ["intercept"],
        "feature_means": {"elo": Decimal("1500.000000")},
        "diff_means": {"intercept": Decimal("0.000000")},
        "diff_stds": {"intercept": Decimal("1.000000")},
        "beta_margin": [value],
        "beta_winprob": [Decimal("0.200000")],
        "platt_a": Decimal("0.900000"),
        "platt_b": Decimal("-0.100000"),
    }


def test_identity_is_canonical_and_covers_version_vintage_manifest_and_parameters():
    manifest = {"lineage": "known", "built_at": datetime(2026, 9, 7, tzinfo=UTC)}
    first, normalized_manifest, normalized_parameters = registry.identify_training_fit(
        "fitted_v1", 2025, manifest, _parameters()
    )
    reordered, _, _ = registry.identify_training_fit(
        "fitted_v1", 2025, dict(reversed(list(manifest.items()))), _parameters()
    )
    changed, _, _ = registry.identify_training_fit("fitted_v1", 2024, manifest, _parameters())

    assert first == reordered
    assert first != changed
    assert len(first) == 64 and set(first) <= set("0123456789abcdef")
    assert normalized_manifest["built_at"] == "2026-09-07T00:00:00+00:00"
    assert normalized_parameters["beta_margin"] == ["1.000000"]


def test_parameters_require_the_exact_eight_keys():
    with pytest.raises(ValueError, match="exact registry keys"):
        registry.identify_training_fit("fitted_v1", 2025, {}, {**_parameters(), "unexpected": 1})
    missing = _parameters()
    del missing["platt_b"]
    with pytest.raises(ValueError, match="platt_b"):
        registry.identify_training_fit("fitted_v1", 2025, {}, missing)


def test_insert_uses_canonical_payload_and_leaves_transaction_to_caller():
    conn = _Connection([("inserted",)])
    fit_id = registry.insert_fit(conn, "fitted_v1", 2025, {"lineage": "known"}, _parameters())

    sql, args = conn.executed[0]
    assert "ON CONFLICT (training_fit_id) DO NOTHING" in sql
    assert args[0] == fit_id
    assert json.loads(args[3]) == {"lineage": "known"}
    assert json.loads(args[4])["platt_a"] == "0.900000"
    assert not conn.committed and not conn.rolled_back


def test_insert_conflict_verifies_existing_contents():
    manifest = {"lineage": "known"}
    parameters = registry.json_value(_parameters())
    conn = _Connection([], [("fitted_v1", 2025, manifest, parameters)])
    fit_id = registry.insert_fit(conn, "fitted_v1", 2025, manifest, _parameters())
    assert conn.executed[1][1] == (fit_id,)


def test_insert_conflict_rejects_a_mismatched_stored_row():
    conn = _Connection([], [("fitted_v1", 2025, {"lineage": "corrupt"}, {})])
    with pytest.raises(ValueError, match="conflicting contents"):
        registry.insert_fit(conn, "fitted_v1", 2025, {"lineage": "known"}, _parameters())


def test_selected_readers_return_stable_public_shapes():
    params = registry.json_value(_parameters())
    one = _Connection([("a" * 64, {"lineage": "known"}, params)])
    assert registry.load_selected_fit(one, "fitted_v1", 2025) == {
        "training_fit_id": "a" * 64,
        "manifest": {"lineage": "known"},
        "parameters": params,
    }

    many = _Connection(
        [
            (2024, "a" * 64, {"lineage": "known"}, params),
            (2025, "b" * 64, {"lineage": "known"}, params),
        ]
    )
    assert [row["train_through_season"] for row in registry.selected_fits(many, "fitted_v1")] == [
        2024,
        2025,
    ]


def test_candidate_loader_and_missing_errors():
    params = registry.json_value(_parameters())
    conn = _Connection([("a" * 64, "fitted_v1", 2025, {"lineage": "known"}, params)])
    assert registry.load_fit(conn, "a" * 64)["train_through_season"] == 2025
    with pytest.raises(LookupError, match="unknown training_fit_id"):
        registry.load_fit(_Connection([]), "b" * 64)
    with pytest.raises(LookupError, match="no selected fit"):
        registry.load_selected_fit(_Connection([]), "fitted_v1", 2025)


def test_promote_validates_reason_and_updates_the_fit_owned_key_without_commit():
    conn = _Connection([("fitted_v1", 2025)], [])
    registry.promote_fit(conn, "a" * 64, "  reviewed candidate  ")
    sql, args = conn.executed[1]
    assert "ON CONFLICT (model_version, train_through_season) DO UPDATE" in sql
    assert args == ("fitted_v1", 2025, "a" * 64, "reviewed candidate")
    assert not conn.committed and not conn.rolled_back

    with pytest.raises(ValueError, match="reason"):
        registry.promote_fit(_Connection(), "a" * 64, "  ")
    with pytest.raises(LookupError, match="unknown training_fit_id"):
        registry.promote_fit(_Connection([]), "b" * 64, "reviewed")


def test_legacy_import_preserves_exact_database_numerics_and_does_not_promote():
    metadata = (
        2025,
        [2023, 2024, 2025],
        Decimal("0.900000"),
        Decimal("-0.100000"),
        Decimal("7.000"),
        Decimal("0.001"),
        {"elo": 1500},
        {"intercept": 0},
        {"intercept": 1},
        123,
    )
    coefficients = [
        (2025, "margin", 0, "intercept", Decimal("1.230000")),
        (2025, "winprob", 0, "intercept", Decimal("0.200000")),
    ]
    conn = _Connection([], [metadata], coefficients, [("inserted",)])

    imported = registry.import_legacy_fits(conn, "fitted_v1")

    assert len(imported) == 1
    insert_sql, insert_args = conn.executed[3]
    assert "IN SHARE MODE" in conn.executed[0][0]
    assert "features.training_fits" in insert_sql
    assert all("model_deployments" not in sql for sql, _ in conn.executed)
    manifest = json.loads(insert_args[3])
    parameters = json.loads(insert_args[4])
    assert manifest["lineage"] == "legacy_unknown"
    assert manifest["training_data"] == {
        "algorithm": "sha256",
        "digest": None,
        "row_count": 123,
    }
    assert manifest["code_revision"] is None
    assert manifest["calibration"]["population"] is None
    assert parameters["beta_margin"] == ["1.230000"]
    assert parameters["platt_b"] == "-0.100000"
    assert not conn.committed and not conn.rolled_back
