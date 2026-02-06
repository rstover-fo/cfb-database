---
title: "psycopg2 cascading transaction failures in pytest"
category: test-failures
tags: [psycopg2, pytest, transactions, autocommit, fixtures]
module: tests
symptoms:
  - "InFailedSqlTransaction: current transaction is aborted"
  - "One test failure causes all subsequent tests to fail"
  - "commands ignored until end of transaction block"
severity: high
date: 2026-02-06
---

# psycopg2 cascading transaction failures in pytest

## Problem

When using a module-scoped `db_conn` fixture shared across many parametrized
tests, a single SQL error causes ALL subsequent tests to fail with
`InFailedSqlTransaction`.

## Symptoms

```
FAILED test_marts.py::TestMartViewsHaveData::test_view_has_rows[marts.data_quality_dashboard]
  psycopg2.errors.UndefinedTable: relation "marts.data_quality_dashboard" does not exist

FAILED test_marts.py::TestMartViewsHaveData::test_view_has_rows[marts.defensive_havoc]
  psycopg2.errors.InFailedSqlTransaction: current transaction is aborted

FAILED test_marts.py::TestMartViewsHaveData::test_view_has_rows[marts.matchup_edges]
  psycopg2.errors.InFailedSqlTransaction: current transaction is aborted

# ... 30+ more failures, all with InFailedSqlTransaction
```

One real failure (data_quality_dashboard doesn't exist) cascades into 30+
false failures.

## Investigation

1. First test hit a real SQL error (undefined table)
2. psycopg2 put the connection into a failed transaction state
3. Every subsequent query on the same connection failed with
   `InFailedSqlTransaction`
4. The `db_conn` fixture was module-scoped, so all tests in the module
   shared the poisoned connection

## Root Cause

psycopg2 connections default to **transactional mode**. Each statement runs
inside an implicit transaction. When a statement fails, PostgreSQL marks the
transaction as aborted and refuses all further commands until a `ROLLBACK`.

Since the `db_conn` fixture was shared (module scope) and no rollback occurred
between tests, one failure poisoned every subsequent test.

## Solution

Set `autocommit = True` on the shared connection in `conftest.py`:

```python
@pytest.fixture(scope="module")
def db_conn():
    """Module-scoped Postgres connection for database integration tests."""
    dsn = _load_postgres_dsn()
    conn = psycopg2.connect(dsn)
    conn.autocommit = True  # Each statement is its own transaction
    yield conn
    conn.close()
```

With autocommit, each SQL statement runs as its own transaction. A failure in
one statement does not affect subsequent statements.

## Alternatives Considered

1. **Per-test connections**: Too slow for 50+ parametrized tests (connection
   overhead per test)
2. **Rollback after each test**: Would require a wrapper or autouse fixture
   that calls `conn.rollback()` in a try/except — more complex than autocommit
3. **Savepoints**: `SAVEPOINT` before each test + `ROLLBACK TO SAVEPOINT` on
   failure — overkill for read-only tests

## Prevention

- **Always use `autocommit = True` for read-only test connections.** There is
  no benefit to transactional mode when tests only run SELECT queries.
- **If you need transactions in tests** (e.g., testing INSERT/UPDATE), use
  per-test connections or explicit savepoint management.
- **Consolidate db fixtures in `conftest.py`** so the autocommit setting is
  applied consistently across all test modules.

## Related

- PostgreSQL docs: transaction error handling
- psycopg2 docs: `connection.autocommit`
- `tests/conftest.py` — shared fixture with autocommit enabled
- `tests/test_marts.py`, `tests/test_api_views.py` — both use this fixture
