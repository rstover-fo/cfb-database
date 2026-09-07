"""Catalog capture stays schema-only and bounds the selected namespaces."""

import json
from unittest.mock import MagicMock

import pytest

from scripts import export_warehouse_catalog as export


def connection(monkeypatch, schemas):
    conn = MagicMock()
    conn.__enter__.return_value = conn
    cur = conn.cursor.return_value.__enter__.return_value
    cur.fetchall.side_effect = [schemas, [("vector", "0.8.0", "public")], [], [], [], [], []]
    cur.fetchone.side_effect = [("17.6",), ("snapshot-id",)]
    monkeypatch.setattr(export.psycopg2, "connect", lambda _: conn)
    return conn


def test_schema_only_capture_uses_scoped_read_only_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("SUPABASE_DB_URL", "postgresql://reader:secret@database:5432/warehouse")
    monkeypatch.setenv("PGHOST", "unrelated-host")
    monkeypatch.setattr(export, "OUTPUT", tmp_path)
    conn = connection(monkeypatch, [("core",), ("core_staging",), ("auth",), ("scouting",)])
    calls = []
    monkeypatch.setattr(
        export.subprocess, "run", lambda args, **kwargs: calls.append((args, kwargs))
    )
    export.main()
    conn.set_session.assert_called_once_with(readonly=True, isolation_level="REPEATABLE READ")
    args, kwargs = calls[0]
    assert "--schema-only" in args and "--data-only" not in args
    assert args[args.index("--snapshot") + 1] == "snapshot-id"
    assert [args[i + 1] for i, arg in enumerate(args) if arg == "--schema"] == [
        "core",
        "core_staging",
        "scouting",
    ]
    assert all("secret" not in arg and "database" not in arg for arg in args)
    assert kwargs["env"]["PGHOST"] == "database"
    assert "default_transaction_read_only=on" in kwargs["env"]["PGOPTIONS"]
    metadata = json.loads((tmp_path / "catalog.json").read_text())
    assert metadata["excluded_schemas"] == ["auth"]
    assert "secret" not in (tmp_path / "catalog.json").read_text()


def test_empty_scope_cannot_dump_every_schema(monkeypatch, tmp_path):
    monkeypatch.setenv("SUPABASE_DB_URL", "postgresql://reader:secret@database/warehouse")
    monkeypatch.setattr(export, "OUTPUT", tmp_path)
    connection(monkeypatch, [("auth",), ("storage",)])
    run = MagicMock()
    monkeypatch.setattr(export.subprocess, "run", run)
    with pytest.raises(RuntimeError, match="unrestricted dump"):
        export.main()
    run.assert_not_called()
    assert not (tmp_path / "schema.sql").exists()
