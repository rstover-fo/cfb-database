"""Behavioral coverage for the remaining SDV receipt scheduler."""

from __future__ import annotations

import argparse
import json
import uuid
from datetime import date

import psycopg2
import pytest

from scripts import run_scheduled_sdv_receipts as scheduled

PROJECT_REF = "ibobsbwlewpqslkqbrjd"
SHA256 = "a" * 64


def args() -> argparse.Namespace:
    return argparse.Namespace(expected_project_ref=PROJECT_REF)


def activation(source: str) -> scheduled.Activation:
    return next(item for item in scheduled.ACTIVATIONS if item.source == source)


def loaded_result(source: str, *, rows: int = 12) -> dict:
    return {
        "status": "loaded",
        "source": source,
        "rows": rows,
        "sha": SHA256,
        "duration_s": 1.25,
        "run_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"run-{source}")),
        "generation_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"generation-{source}")),
    }


def selected_environment(*sources: str) -> dict[str, str]:
    selected = set(sources)
    return {
        item.variable: str(item.season) for item in scheduled.ACTIVATIONS if item.source in selected
    }


@pytest.mark.parametrize("source", [item.source for item in scheduled.ACTIVATIONS])
def test_activation_selects_only_exact_nonempty_values(source):
    assert scheduled._selected_activations(selected_environment(source)) == [activation(source)]


@pytest.mark.parametrize("value", ["2024", "2026 ", "all"])
def test_activation_rejects_wrong_nonempty_value(value):
    with pytest.raises(scheduled.ScheduledSdvError, match="must be exactly 2026"):
        scheduled._selected_activations({"SDV_RATINGS_RECEIPT_SEASON": value})


def test_empty_activation_values_are_inactive():
    assert (
        scheduled._selected_activations({item.variable: "" for item in scheduled.ACTIVATIONS}) == []
    )


def test_cli_requires_exactly_one_project_reference():
    parser = scheduled.build_parser()
    with pytest.raises(SystemExit, match="2"):
        parser.parse_args([])
    with pytest.raises(SystemExit, match="2"):
        parser.parse_args(
            [
                "--expected-project-ref",
                PROJECT_REF,
                "--expected-project-ref",
                PROJECT_REF,
            ]
        )


def test_rollover_stops_before_database_or_provider_io(monkeypatch, tmp_path):
    outputs = tmp_path / "outputs"
    monkeypatch.setenv("GITHUB_OUTPUT", str(outputs))
    monkeypatch.setattr(
        scheduled.publication,
        "get_db_url",
        lambda: pytest.fail("rollover must stop before database resolution"),
    )
    monkeypatch.setattr(
        scheduled,
        "_run_one",
        lambda *values: pytest.fail("rollover must stop before provider work"),
    )

    payload, status = scheduled.run_scheduled(
        args(),
        today=date(2027, 8, 1),
        environ=selected_environment("sdv_ratings_weekly"),
    )

    assert status == 0
    assert payload == {
        "status": "skipped",
        "operating_season": 2027,
        "reason": "remaining SDV receipt rollout is limited to operating season 2026",
        "sources": [],
    }
    assert outputs.read_text().splitlines() == ["ratings_publication_attempted=false"]


def test_no_active_sources_stop_before_database_resolution(monkeypatch):
    monkeypatch.setattr(
        scheduled.publication,
        "get_db_url",
        lambda: pytest.fail("inactive scheduler must not resolve a database"),
    )
    payload, status = scheduled.run_scheduled(args(), today=date(2026, 9, 10), environ={})
    assert status == 0
    assert payload == {"status": "inactive", "operating_season": 2026, "sources": []}


@pytest.mark.parametrize(
    "policy_rows,accepted", [([(True,)], True), ([], False), ([(False,)], False)]
)
def test_policy_preflight_requires_one_exact_eight_day_row(policy_rows, accepted):
    selected = activation("sdv_team_xwalk")

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *unused):
            return False

        def execute(self, statement, parameters=None):
            if parameters is not None:
                assert parameters == ("ref.team_id_xwalk", "season:2025")
                assert "interval '8 days'" in statement

        def fetchall(self):
            return policy_rows

    class Connection:
        def set_session(self, **options):
            assert options == {"readonly": True, "autocommit": False}

        def cursor(self):
            return Cursor()

        def rollback(self):
            pass

        def close(self):
            pass

    def call(*values, **options):
        return Connection()

    if accepted:
        scheduled._require_policy("dsn", selected, connector=call)
    else:
        with pytest.raises(scheduled.ScheduledSdvError, match="exactly 8 days"):
            scheduled._require_policy("dsn", selected, connector=call)


def test_all_selected_sources_run_serially_on_same_inspected_dsn(monkeypatch, tmp_path):
    outputs = tmp_path / "outputs"
    monkeypatch.setenv("GITHUB_OUTPUT", str(outputs))
    monkeypatch.setattr(scheduled.publication, "get_db_url", lambda: "inspected-dsn")
    monkeypatch.setattr(
        scheduled.sdv_ratings_publication,
        "get_db_url",
        lambda: "adversarial-ratings-dsn",
    )
    events = []

    def inspect(dsn, project, source, season):
        events.append(("inspect", source, season, dsn, project))
        return {
            "can_set_role": True,
            "assumed_role": scheduled.canary.PUBLISHER_ROLE,
        }, {"source": source}

    monkeypatch.setattr(scheduled.canary, "_inspect_target", inspect)
    monkeypatch.setattr(
        scheduled,
        "_require_policy",
        lambda dsn, item: events.append(("policy", item.source, item.season, dsn)),
    )

    def ratings_publish(spec, *, season):
        events.append(
            (
                "publish",
                spec.name,
                season,
                scheduled.publication.get_db_url(),
                scheduled.sdv_ratings_publication.get_db_url(),
            )
        )
        return loaded_result(spec.name)

    def batch_publish(spec, *, season):
        events.append(
            (
                "publish",
                spec.name,
                season,
                scheduled.publication.get_db_url(),
                scheduled.sdv_ratings_publication.get_db_url(),
            )
        )
        return loaded_result(spec.name)

    monkeypatch.setattr(
        scheduled.sdv_ratings_publication,
        "run_sdv_ratings_publication",
        ratings_publish,
    )
    monkeypatch.setattr(scheduled.publication, "run_source_publication", batch_publish)
    monkeypatch.setattr(
        scheduled,
        "_verify_private_receipt",
        lambda dsn, item, result: events.append(("private", item.source, dsn)),
    )
    monkeypatch.setattr(
        scheduled,
        "_verify_public_freshness",
        lambda dsn, item, result: events.append(("public", item.source, dsn)),
    )

    payload, status = scheduled.run_scheduled(
        args(),
        today=date(2026, 9, 10),
        environ=selected_environment(*(item.source for item in scheduled.ACTIVATIONS)),
    )

    assert status == 0
    assert payload["status"] == "loaded"
    assert [row["source"] for row in payload["sources"]] == [
        item.source for item in scheduled.ACTIVATIONS
    ]
    assert [event[1] for event in events if event[0] == "publish"] == [
        item.source for item in scheduled.ACTIVATIONS
    ]
    assert all(
        event[3:] == ("inspected-dsn", "inspected-dsn") for event in events if event[0] == "publish"
    )
    assert scheduled.sdv_ratings_publication.get_db_url() == "adversarial-ratings-dsn"
    assert outputs.read_text().splitlines() == [
        "ratings_publication_attempted=false",
        "ratings_publication_attempted=true",
    ]


def test_source_failures_do_not_suppress_later_selected_sources(monkeypatch):
    monkeypatch.setattr(scheduled.publication, "get_db_url", lambda: "dsn")
    attempted = []
    monkeypatch.setattr(
        scheduled.canary,
        "_inspect_target",
        lambda *values: (
            {"can_set_role": True, "assumed_role": scheduled.canary.PUBLISHER_ROLE},
            {},
        ),
    )
    monkeypatch.setattr(scheduled, "_require_policy", lambda *values: None)

    failed_ratings = {
        **loaded_result("sdv_ratings_weekly"),
        "status": "failed",
        "rows": 0,
        "sha": None,
    }

    def ratings_publish(spec, *, season):
        attempted.append(spec.name)
        return failed_ratings

    def batch_publish(spec, *, season):
        attempted.append(spec.name)
        if spec.name == "sdv_team_xwalk":
            raise scheduled.publication.SourcePublicationError("private detail")
        return loaded_result(spec.name)

    monkeypatch.setattr(
        scheduled.sdv_ratings_publication,
        "run_sdv_ratings_publication",
        ratings_publish,
    )
    monkeypatch.setattr(scheduled.publication, "run_source_publication", batch_publish)
    monkeypatch.setattr(scheduled, "_verify_private_receipt", lambda *values: None)
    monkeypatch.setattr(scheduled, "_verify_public_freshness", lambda *values: None)

    payload, status = scheduled.run_scheduled(
        args(),
        today=date(2026, 9, 10),
        environ=selected_environment(*(item.source for item in scheduled.ACTIVATIONS)),
    )

    assert status == 1
    assert payload["status"] == "failed"
    assert attempted == [item.source for item in scheduled.ACTIVATIONS]
    assert [item["status"] for item in payload["sources"]] == [
        "failed",
        "failed",
        "loaded",
    ]
    assert payload["sources"][0]["run_id"] == failed_ratings["run_id"]
    assert payload["sources"][1]["error"] == "source publication validation failed"


def _receipt_row(item: scheduled.Activation, result: dict) -> tuple:
    inputs = {
        "publisher": "load_flat_files",
        "protocol": item.asset.protocol,
        "parser_contract": item.asset.parser_contract,
        "stage_schema": "warehouse_source_stage",
        "artifact_origin": "registered_url",
    }
    if item.source != "sdv_ratings_weekly":
        inputs.update(
            source_name=item.source,
            season_basis="registered_artifact_name",
        )
    return (
        result["run_id"],
        result["generation_id"],
        result["sha"],
        "succeeded",
        {
            "complete": True,
            "scope": "season",
            "season": item.season,
            "mode": "full_file_for_season",
            "source_rows": result["rows"],
            "published_rows": result["rows"],
        },
        inputs,
        {"published_rows": result["rows"]},
        result["generation_id"],
    )


@pytest.mark.parametrize("item", scheduled.ACTIVATIONS)
def test_private_receipt_matches_source_operation_generation_and_provenance(item):
    result = loaded_result(item.source)

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *unused):
            return False

        def execute(self, statement, parameters=None):
            if parameters is not None:
                assert parameters == (
                    item.asset.asset_key,
                    item.asset.coverage_key(item.season),
                    result["generation_id"],
                )

        def fetchall(self):
            return [_receipt_row(item, result)]

    class Connection:
        def set_session(self, **options):
            assert options == {"readonly": True, "autocommit": False}

        def cursor(self):
            return Cursor()

        def rollback(self):
            pass

        def close(self):
            pass

    scheduled._verify_private_receipt(
        "dsn", item, result, connector=lambda *values, **options: Connection()
    )


def freshness_row(item: scheduled.Activation, result: dict, **overrides) -> dict:
    basis = "artifact_field" if item.source == "sdv_ratings_weekly" else "registered_artifact_name"
    row = {
        "source_name": item.source,
        "asset_key": item.asset.asset_key,
        "season": item.season,
        "coverage_key": item.asset.coverage_key(item.season),
        "generation_id": result["generation_id"],
        "published_at": "2026-09-10T15:00:00+00:00",
        "age_seconds": 2,
        "expected_refresh_interval": "8 days",
        "is_stale": False,
        "publication_state": "current",
        "current_outcome": "succeeded",
        "is_complete": True,
        "source_rows": result["rows"],
        "published_rows": result["rows"],
        "artifact_origin": "registered_url",
        "season_basis": basis,
        "latest_outcome": "succeeded",
        "latest_recorded_at": "2026-09-10T15:00:00+00:00",
        "last_failure_outcome": None,
        "last_failure_at": None,
        "last_failure_category": None,
    }
    row.update(overrides)
    return row


@pytest.mark.parametrize("item", scheduled.ACTIVATIONS)
def test_public_verification_checks_both_roles_and_source_specific_basis(monkeypatch, item):
    result = loaded_result(item.source)
    calls = []

    def read(dsn, role, selected):
        calls.append((dsn, role, selected))
        return freshness_row(item, result), True

    monkeypatch.setattr(scheduled, "_read_public_freshness", read)
    scheduled._verify_public_freshness("dsn", item, result)
    assert calls == [
        ("dsn", "anon", item),
        ("dsn", "authenticated", item),
    ]


def test_public_freshness_read_assumes_actual_role_and_exact_source_scope():
    item = activation("sdv_team_xwalk")
    result = loaded_result(item.source)

    class Cursor:
        def __init__(self):
            self.statement = ""
            self.calls = []

        def __enter__(self):
            return self

        def __exit__(self, *unused):
            return False

        def execute(self, statement, parameters=None):
            self.statement = str(statement)
            self.calls.append((self.statement, parameters))

        def fetchone(self):
            assert "current_user" in self.statement
            return ("anon",)

        def fetchall(self):
            return [(freshness_row(item, result), True)]

    class Connection:
        def __init__(self):
            self.cursor_object = Cursor()

        def set_session(self, **options):
            assert options == {"readonly": True, "autocommit": False}

        def cursor(self):
            return self.cursor_object

        def rollback(self):
            pass

        def close(self):
            pass

    connection = Connection()
    row, interval_matches = scheduled._read_public_freshness(
        "dsn",
        "anon",
        item,
        connector=lambda *values, **options: connection,
    )

    assert row["source_name"] == item.source
    assert interval_matches is True
    freshness_call = next(
        call for call in connection.cursor_object.calls if "get_source_freshness" in call[0]
    )
    assert freshness_call[1] == (2025, "sdv_team_xwalk")
    assert "interval '8 days'" in freshness_call[0]
    assert any("SET LOCAL ROLE" in statement for statement, _ in connection.cursor_object.calls)


@pytest.mark.parametrize("source", [item.source for item in scheduled.ACTIVATIONS])
def test_public_verification_rejects_wrong_source_provenance(monkeypatch, source):
    item = activation(source)
    result = loaded_result(source)
    wrong_basis = "registered_artifact_name" if source == "sdv_ratings_weekly" else "artifact_field"
    monkeypatch.setattr(
        scheduled,
        "_read_public_freshness",
        lambda *values: (freshness_row(item, result, season_basis=wrong_basis), True),
    )
    with pytest.raises(scheduled.ScheduledSdvError, match="invalid season evidence"):
        scheduled._verify_public_freshness("dsn", item, result)


def test_postpublication_failure_preserves_returned_identifiers(monkeypatch):
    item = activation("sdv_game_xwalk")
    result = loaded_result(item.source)
    monkeypatch.setattr(
        scheduled.canary,
        "_inspect_target",
        lambda *values: (
            {"can_set_role": True, "assumed_role": scheduled.canary.PUBLISHER_ROLE},
            {},
        ),
    )
    monkeypatch.setattr(scheduled, "_require_policy", lambda *values: None)
    monkeypatch.setattr(scheduled, "_publish", lambda selected: result)
    monkeypatch.setattr(
        scheduled,
        "_verify_private_receipt",
        lambda *values: (_ for _ in ()).throw(scheduled.ScheduledSdvError("receipt mismatch")),
    )
    monkeypatch.setattr(scheduled, "_verify_public_freshness", lambda *values: None)

    payload, succeeded = scheduled._run_one("dsn", PROJECT_REF, item)

    assert succeeded is False
    assert payload["status"] == "verification_failed"
    assert payload["run_id"] == result["run_id"]
    assert payload["generation_id"] == result["generation_id"]
    assert payload["sha"] == result["sha"]


def test_main_sanitizes_unexpected_exception_details(monkeypatch, capsys):
    monkeypatch.setattr(
        scheduled,
        "run_scheduled",
        lambda selected: (_ for _ in ()).throw(
            RuntimeError("postgresql://user:secret@private-host/database")
        ),
    )
    status = scheduled.main(["--expected-project-ref", PROJECT_REF])
    output = capsys.readouterr().out

    assert status == 1
    assert json.loads(output)["error"] == "unexpected failure (RuntimeError)"
    assert "secret" not in output
    assert "private-host" not in output


def test_database_failure_is_sanitized():
    error = psycopg2.OperationalError("password=secret host=private-host")
    assert str(scheduled._database_failure(error, "receipt check")) == (
        "receipt check failed (type=OperationalError, code=none)"
    )
