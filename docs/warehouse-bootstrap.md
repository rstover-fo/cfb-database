# Managed warehouse bootstrap

F06 is in development. The default catalog baseline is pending approved
production schema-only inventory; the commands below are the new interface,
not evidence that full warehouse bootstrap is complete.

## Local database

Install development dependencies with `bash scripts/setup_dev.sh`, then start
an isolated PostgreSQL 17 with pgvector. Use a dedicated container and loopback
port; do not reuse a production tunnel or another project's database.

```bash
docker run --name cfb-fixture-postgres \
  -e POSTGRES_PASSWORD=local-password -p 127.0.0.1:55436:5432 -d \
  pgvector/pgvector:pg17@sha256:cf134a767f474095eeba57e0117be8e568e011a63f33fbf252f14c9b760f8e6f
export WAREHOUSE_DB_URL='postgresql://postgres:local-password@127.0.0.1:55436/postgres'
```

The CLI requires an explicit action and reads only `WAREHOUSE_DB_URL`:

```bash
.venv/bin/python scripts/bootstrap_warehouse.py bootstrap
.venv/bin/python scripts/bootstrap_warehouse.py plan
.venv/bin/python scripts/bootstrap_warehouse.py upgrade
.venv/bin/python scripts/bootstrap_warehouse.py status
```

`--manifest` selects a reviewed version-1 JSON manifest. `--target` stops at an
existing migration ID; it cannot roll back applied history. `--dry-run`, `plan`
and `status` use read-only transactions and never install the ledger.

## Release semantics

Manifest entries have unique `id`, repository-relative SQL `path`, and `kind`
(`immutable` or `repeatable`). Immutable entries retain exact bytes and order
forever once applied. Forward prerequisites run before changed repeatables;
repeatables then run in manifest order. Add prerequisite objects as immutable
forward migrations when a changed view/function needs them.

The private `warehouse_control` ledger stores identities, paths, exact SHA256
checksums and timestamps. Repeatable execution history retains each checksum.
A transaction-scoped advisory lock covers state validation, all migration SQL,
and ledger changes. Any SQL failure rolls back the entire batch. Migration SQL
cannot contain top-level transaction controls. Unchanged invocations are no-ops;
changed applied immutable files, deleted/reordered identities, and unmanaged
upgrade targets fail clearly. No command silently adopts production history.

The old `run_migrations.py --file` interface deliberately continues to execute
explicit diagnostics/repairs each time. It is not ledger-managed. Historical
001–018 chain execution now needs `--legacy-history`; inspecting it with
`--dry-run` remains available without that opt-in.

## Executed integration

The ledger tests create uniquely named databases in an explicitly selected
loopback cluster and drop only those databases afterward:

```bash
F06_TEST_DB_URL="$WAREHOUSE_DB_URL" F06_REQUIRE_DB=1 \
  .venv/bin/python -m pytest tests/test_warehouse_migrations_sql.py -q
```

These tests prove migration behavior; until the catalog baseline is reviewed,
they do not establish that every warehouse object can be reconstructed. Full
baseline, representative dlt fixtures and actual consumer-role checks remain
F06 acceptance work. Ordinary dependency installation does not provision data.
