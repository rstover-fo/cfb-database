"""Regression coverage for the managed mart release CLI boundaries."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.run_marts as run_marts
import scripts.run_migrations as run_migrations


@pytest.mark.parametrize(
    "arguments",
    [[], ["--all"], ["--from", "029"], ["--only", "029"]],
)
def test_real_legacy_mart_paths_fail_before_credentials(monkeypatch, arguments):
    monkeypatch.setattr(
        run_marts,
        "get_release_db_url",
        lambda: pytest.fail("release credential lookup must not run"),
    )

    with pytest.raises(SystemExit, match="2"):
        run_marts.main(arguments)


def test_run_mart_public_helper_rejects_real_unmanaged_execution(tmp_path):
    sql_path = tmp_path / "mart.sql"
    sql_path.write_text("select 1;")

    with pytest.raises(RuntimeError, match="requires --release"):
        run_marts.run_mart(sql_path, conn=SimpleNamespace(), dry_run=False)


def test_legacy_dry_run_still_displays_selected_sql(monkeypatch, tmp_path, capsys):
    first = tmp_path / "001_first.sql"
    second = tmp_path / "002_second.sql"
    first.write_text("select 1;")
    second.write_text("select 2;")
    monkeypatch.setattr(run_marts, "get_mart_files", lambda: [first, second])

    assert run_marts.main(["--only", "002", "--dry-run"]) == 0

    output = capsys.readouterr().out
    assert "002_second.sql" in output
    assert "select 2;" in output
    assert "001_first.sql" not in output


def test_list_remains_read_only_without_credentials(monkeypatch, tmp_path, capsys):
    sql_path = tmp_path / "001_example.sql"
    sql_path.write_text("select 1;")
    monkeypatch.setattr(run_marts, "get_mart_files", lambda: [sql_path])
    monkeypatch.setattr(
        run_marts,
        "get_release_db_url",
        lambda: pytest.fail("listing must not inspect credentials"),
    )

    assert run_marts.main(["--list"]) == 0
    assert "001_example.sql" in capsys.readouterr().out


def _valid_plan():
    return SimpleNamespace(
        valid=True,
        roots=("marts.upstream",),
        declared_consumers=("api.dynamic_reader()",),
        live_closure=(SimpleNamespace(kind="view", identity="api.consumer"),),
        declared_restores=(SimpleNamespace(kind="view", identity="api.consumer"),),
        files=(SimpleNamespace(path=Path("src/schemas/marts/001.sql"), sha256="a" * 64),),
        warnings=(),
        blockers=(),
    )


class _Connection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_plan_calls_only_read_only_engine_path(monkeypatch, capsys):
    manifest = object()
    conn = _Connection()
    calls = []
    monkeypatch.setattr(run_marts, "load_release", lambda path, root: manifest)
    monkeypatch.setattr(run_marts, "get_release_db_url", lambda: "postgresql://complete")
    monkeypatch.setattr("psycopg2.connect", lambda database_url: conn)
    monkeypatch.setattr(
        run_marts,
        "plan_release",
        lambda actual_conn, actual_manifest: (
            calls.append(("plan", actual_conn, actual_manifest)) or _valid_plan()
        ),
    )
    monkeypatch.setattr(
        run_marts,
        "execute_release",
        lambda *_: pytest.fail("read-only preview must not execute the release"),
    )

    assert run_marts.main(["--release", "release.json", "--plan"]) == 0

    assert calls == [("plan", conn, manifest)]
    assert conn.closed is True
    output = capsys.readouterr().out
    assert '"read_only": true' in output
    assert '"api.dynamic_reader()"' in output


def test_release_execution_calls_atomic_engine_once(monkeypatch):
    manifest = object()
    conn = _Connection()
    calls = []
    monkeypatch.setattr(run_marts, "load_release", lambda path, root: manifest)
    monkeypatch.setattr(run_marts, "get_release_db_url", lambda: "postgresql://complete")
    monkeypatch.setattr("psycopg2.connect", lambda database_url: conn)
    monkeypatch.setattr(
        run_marts,
        "execute_release",
        lambda actual_conn, actual_manifest: (
            calls.append((actual_conn, actual_manifest)) or _valid_plan()
        ),
    )
    monkeypatch.setattr(
        run_marts,
        "plan_release",
        lambda *_: pytest.fail("execution must use the engine's single atomic call"),
    )

    assert run_marts.main(["--release", "release.json"]) == 0
    assert calls == [(conn, manifest)]
    assert conn.closed is True


def test_invalid_release_manifest_fails_before_credentials_and_redacts(monkeypatch, caplog):
    leaked_url = "postgresql://alice:secret@example.test:5432/warehouse"
    monkeypatch.setattr(
        run_marts,
        "load_release",
        lambda path, root: (_ for _ in ()).throw(ValueError(f"invalid source {leaked_url}")),
    )
    monkeypatch.setattr(
        run_marts,
        "get_release_db_url",
        lambda: pytest.fail("invalid manifests must fail before credentials"),
    )

    with caplog.at_level(logging.ERROR):
        assert run_marts.main(["--release", "invalid.json"]) == 1

    assert leaked_url not in caplog.text
    assert "secret" not in caplog.text
    assert "[REDACTED_DATABASE_URL]" in caplog.text


def test_connection_error_redacts_explicit_database_url(monkeypatch, caplog):
    leaked_url = "postgresql://alice:secret@example.test:5432/warehouse"
    monkeypatch.setattr(run_marts, "load_release", lambda path, root: object())
    monkeypatch.setattr(run_marts, "get_release_db_url", lambda: leaked_url)
    monkeypatch.setattr(
        "psycopg2.connect",
        lambda database_url: (_ for _ in ()).throw(
            RuntimeError(f"cannot connect to {database_url}")
        ),
    )

    with caplog.at_level(logging.ERROR):
        assert run_marts.main(["--release", "release.json"]) == 1

    assert leaked_url not in caplog.text
    assert "secret" not in caplog.text
    assert "[REDACTED]" in caplog.text


def test_empty_release_manifest_fails_before_credentials(monkeypatch, tmp_path, caplog):
    release_path = tmp_path / "empty-release.json"
    release_path.write_text(
        json.dumps({"version": 1, "files": [], "roots": [], "restores": [], "validations": []})
    )
    monkeypatch.setattr(
        run_marts,
        "get_release_db_url",
        lambda: pytest.fail("an empty manifest must fail before credentials"),
    )

    with caplog.at_level(logging.ERROR):
        assert run_marts.main(["--release", str(release_path)]) == 1

    assert "files must be a non-empty list" in caplog.text


def test_release_credentials_require_explicit_supabase_url(monkeypatch):
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql://fallback:unused@example.test:5432/db")

    with pytest.raises(RuntimeError, match="SUPABASE_DB_URL is required"):
        run_marts.get_release_db_url()


def test_release_credentials_use_complete_uri_validator(monkeypatch):
    monkeypatch.setenv("SUPABASE_DB_URL", "postgresql://example.test/warehouse")

    with pytest.raises(ValueError, match="SUPABASE_DB_URL.*explicitly include"):
        run_marts.get_release_db_url()


def test_run_migrations_rejects_canonical_mart_file_before_credentials(monkeypatch, tmp_path):
    schemas_dir = tmp_path / "src" / "schemas"
    marts_dir = schemas_dir / "marts"
    api_dir = schemas_dir / "api"
    marts_dir.mkdir(parents=True)
    api_dir.mkdir()
    mart_file = marts_dir / "999_cli_boundary_test.sql"
    # The canonical-path check must also catch traversal through a non-mart parent.
    spelled_path = api_dir / ".." / "marts" / mart_file.name
    mart_file.write_text("select 1;")
    monkeypatch.setattr(run_migrations, "MARTS_DIR", marts_dir.resolve())
    monkeypatch.setattr(sys, "argv", ["run_migrations.py", "--file", str(spelled_path)])
    monkeypatch.setattr(
        run_migrations,
        "get_db_url",
        lambda: pytest.fail("blocked mart files must fail before credentials"),
    )

    with pytest.raises(SystemExit, match="2"):
        run_migrations.main()


def test_run_migrations_allows_mart_file_sql_preview(monkeypatch, tmp_path):
    mart_file = tmp_path / "preview.sql"
    mart_file.write_text("select 1;")
    monkeypatch.setattr(run_migrations, "MARTS_DIR", tmp_path.resolve())
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_migrations.py", "--file", str(mart_file), "--dry-run"],
    )
    calls = []
    monkeypatch.setattr(
        run_migrations,
        "run_migration",
        lambda path, conn, dry_run: calls.append((path, conn, dry_run)),
    )

    run_migrations.main()

    assert calls == [(mart_file, None, True)]


def test_run_migrations_preserves_real_nonmart_file_route(monkeypatch, tmp_path):
    sql_path = tmp_path / "diagnostic.sql"
    sql_path.write_text("select 1;")
    conn = _Connection()
    monkeypatch.setattr(sys, "argv", ["run_migrations.py", "--file", str(sql_path)])
    monkeypatch.setattr(run_migrations, "get_db_url", lambda: "postgresql://fixture")
    monkeypatch.setattr("psycopg2.connect", lambda database_url: conn)
    monkeypatch.setattr(run_migrations, "_disable_statement_timeout", lambda actual_conn: None)
    calls = []
    monkeypatch.setattr(
        run_migrations,
        "run_migration",
        lambda path, actual_conn: calls.append((path, actual_conn)),
    )

    run_migrations.main()

    assert calls == [(sql_path, conn)]
    assert conn.closed is True
