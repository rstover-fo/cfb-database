import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.bootstrap_warehouse as cli
import scripts.run_migrations as legacy
from scripts.warehouse_migrations import (
    Manifest,
    Migration,
    MigrationPlan,
    MigrationStateError,
    MigrationStep,
)


class FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def make_manifest(tmp_path: Path) -> Manifest:
    sql_path = tmp_path / "001.sql"
    sql_path.write_text("SELECT 1;\n")
    migration = Migration(
        id="001-baseline",
        path="001.sql",
        kind="immutable",
        checksum="a" * 64,
        absolute_path=sql_path,
        sql="SELECT 1;\n",
    )
    return Manifest(
        version=1,
        migrations=(migration,),
        source_path=tmp_path / "manifest.json",
        root=tmp_path,
    )


def make_plan(*, valid: bool = True, mode: str = "upgrade") -> MigrationPlan:
    step = MigrationStep(
        id="001-baseline",
        path="001.sql",
        kind="immutable",
        checksum="a" * 64,
        order=1,
        action="apply",
    )
    diagnostics = () if valid else ("applied immutable migration 001-baseline checksum changed",)
    return MigrationPlan(
        mode=mode,
        target="001-baseline",
        ledger_installed=True,
        steps=(step,),
        diagnostics=diagnostics,
    )


def test_direct_cli_help_resolves_repository_imports_from_another_working_directory(tmp_path):
    script = Path(__file__).parents[1] / "scripts" / "bootstrap_warehouse.py"

    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "{bootstrap,upgrade,plan,status}" in result.stdout


def test_upgrade_dry_run_passes_explicit_read_only_request_and_prints_plan(
    monkeypatch, tmp_path, capsys
):
    manifest = make_manifest(tmp_path)
    connection = FakeConnection()
    calls = []
    monkeypatch.setenv("WAREHOUSE_DB_URL", "postgresql://fixture")
    monkeypatch.setattr(cli, "load_manifest", lambda path, root: manifest)
    monkeypatch.setattr(cli, "connect_database", lambda url: connection)

    def apply(conn, selected_manifest, *, mode, target, dry_run):
        calls.append((conn, selected_manifest, mode, target, dry_run))
        return make_plan(mode=mode)

    monkeypatch.setattr(cli, "apply_manifest", apply)

    result = cli.main(["upgrade", "--target", "001-baseline", "--dry-run"])

    assert result == 0
    assert calls == [(connection, manifest, "upgrade", "001-baseline", True)]
    assert connection.closed
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["read_only"] is True
    assert payload["valid"] is True
    assert payload["summary"] == {
        "apply": 1,
        "deferred": 0,
        "pending": 1,
        "reapply": 0,
        "skip": 0,
        "total": 1,
    }


def test_missing_target_fails_before_credentials_or_connection(monkeypatch, tmp_path, capsys):
    monkeypatch.delenv("WAREHOUSE_DB_URL", raising=False)
    monkeypatch.setattr(cli, "load_manifest", lambda path, root: make_manifest(tmp_path))
    monkeypatch.setattr(
        cli,
        "connect_database",
        lambda url: pytest.fail("missing target must be rejected before connection"),
    )

    result = cli.main(["plan", "--target", "999-missing"])

    assert result == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["valid"] is False
    assert "999-missing" in payload["error"]["message"]


def test_only_warehouse_database_url_is_accepted(monkeypatch, tmp_path, capsys):
    monkeypatch.delenv("WAREHOUSE_DB_URL", raising=False)
    monkeypatch.setenv("SUPABASE_DB_URL", "postgresql://must-not-be-used")
    monkeypatch.setattr(cli, "load_manifest", lambda path, root: make_manifest(tmp_path))
    monkeypatch.setattr(
        cli,
        "connect_database",
        lambda url: pytest.fail("fallback credentials must not be used"),
    )

    result = cli.main(["status"])

    assert result == 1
    error = capsys.readouterr().err
    assert "WAREHOUSE_DB_URL" in error
    assert "must-not-be-used" not in error


def test_database_url_and_password_are_redacted_from_connection_errors(
    monkeypatch, tmp_path, capsys
):
    database_url = "postgresql://warehouse:super-secret@db.example/test"
    monkeypatch.setenv("WAREHOUSE_DB_URL", database_url)
    monkeypatch.setattr(cli, "load_manifest", lambda path, root: make_manifest(tmp_path))

    def fail_connect(url):
        raise RuntimeError(f"could not connect with {url}; password=super-secret")

    monkeypatch.setattr(cli, "connect_database", fail_connect)

    result = cli.main(["plan"])

    assert result == 1
    error = capsys.readouterr().err
    assert database_url not in error
    assert "super-secret" not in error
    assert "[REDACTED]" in error


def test_database_url_is_redacted_if_connection_close_fails(monkeypatch, tmp_path, capsys):
    database_url = "postgresql://warehouse:close-secret@db.example/test"

    class BadCloseConnection:
        def close(self):
            raise RuntimeError(f"failed to close {database_url}")

    monkeypatch.setenv("WAREHOUSE_DB_URL", database_url)
    monkeypatch.setattr(cli, "load_manifest", lambda path, root: make_manifest(tmp_path))
    monkeypatch.setattr(cli, "connect_database", lambda url: BadCloseConnection())
    monkeypatch.setattr(
        cli,
        "apply_manifest",
        lambda *args, **kwargs: make_plan(mode=kwargs["mode"]),
    )

    result = cli.main(["plan"])

    assert result == 1
    error = capsys.readouterr().err
    assert database_url not in error
    assert "close-secret" not in error


def test_drift_plan_is_structured_and_exits_nonzero(monkeypatch, tmp_path, capsys):
    connection = FakeConnection()
    monkeypatch.setenv("WAREHOUSE_DB_URL", "postgresql://fixture")
    monkeypatch.setattr(cli, "load_manifest", lambda path, root: make_manifest(tmp_path))
    monkeypatch.setattr(cli, "connect_database", lambda url: connection)
    monkeypatch.setattr(
        cli,
        "apply_manifest",
        lambda *args, **kwargs: make_plan(valid=False, mode=kwargs["mode"]),
    )

    result = cli.main(["status"])

    assert result == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["valid"] is False
    assert payload["read_only"] is True
    assert payload["diagnostics"] == ["applied immutable migration 001-baseline checksum changed"]


def test_executable_state_error_still_prints_its_plan(monkeypatch, tmp_path, capsys):
    connection = FakeConnection()
    plan = make_plan(valid=False, mode="upgrade")
    monkeypatch.setenv("WAREHOUSE_DB_URL", "postgresql://fixture")
    monkeypatch.setattr(cli, "load_manifest", lambda path, root: make_manifest(tmp_path))
    monkeypatch.setattr(cli, "connect_database", lambda url: connection)

    def reject(*args, **kwargs):
        raise MigrationStateError(plan)

    monkeypatch.setattr(cli, "apply_manifest", reject)

    assert cli.main(["upgrade"]) == 1
    assert json.loads(capsys.readouterr().out)["diagnostics"]
    assert connection.closed


@pytest.mark.parametrize("arguments", [[], ["--from", "002"], ["--only", "009"]])
def test_legacy_chain_requires_opt_in_before_credentials_or_connection(
    arguments, monkeypatch, capsys
):
    monkeypatch.setattr(sys, "argv", ["run_migrations.py", *arguments])
    monkeypatch.setattr(
        legacy,
        "get_db_url",
        lambda: pytest.fail("blocked legacy replay must not inspect credentials"),
    )

    with pytest.raises(SystemExit) as exc_info:
        legacy.main()

    assert exc_info.value.code == 2
    assert "--legacy-history" in capsys.readouterr().err


@pytest.mark.parametrize(
    "arguments",
    [
        ["--file", "missing.sql", "--from", "002"],
        ["--file", "missing.sql", "--only", "009"],
        ["--file", "missing.sql", "--legacy-history"],
    ],
)
def test_explicit_file_rejects_ambiguous_history_selectors(arguments, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run_migrations.py", *arguments])
    monkeypatch.setattr(
        legacy,
        "get_db_url",
        lambda: pytest.fail("ambiguous arguments must be rejected before credentials"),
    )

    with pytest.raises(SystemExit) as exc_info:
        legacy.main()

    assert exc_info.value.code == 2


def test_explicit_file_dry_run_remains_available_without_legacy_opt_in(monkeypatch, tmp_path):
    sql_path = tmp_path / "diagnostic.sql"
    sql_path.write_text("SELECT 1;\n")
    calls = []
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_migrations.py", "--file", str(sql_path), "--dry-run"],
    )
    monkeypatch.setattr(
        legacy,
        "run_migration",
        lambda path, conn, dry_run: calls.append((path, conn, dry_run)),
    )

    legacy.main()

    assert calls == [(sql_path, None, True)]


def test_legacy_history_opt_in_allows_selected_execution(monkeypatch):
    connection = FakeConnection()
    calls = []
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_migrations.py", "--only", "009", "--legacy-history"],
    )
    monkeypatch.setitem(
        sys.modules,
        "psycopg2",
        SimpleNamespace(connect=lambda url: connection),
    )
    monkeypatch.setattr(legacy, "get_db_url", lambda: "postgresql://fixture")
    monkeypatch.setattr(legacy, "_disable_statement_timeout", lambda conn: None)
    monkeypatch.setattr(
        legacy,
        "run_migration",
        lambda path, conn, dry_run=False: calls.append((path.name, conn, dry_run)),
    )

    legacy.main()

    assert calls == [("009_variant_columns.sql", connection, False)]
    assert connection.closed
