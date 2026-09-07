"""Verify standalone and transactional runners enforce the F06 SQL safeguards."""

from pathlib import Path

import psycopg2
import pytest

from tests.test_warehouse_migrations_sql import query
from tests.test_warehouse_migrations_sql import warehouse_db as _warehouse_db_fixture  # noqa: F401

ROOT = Path(__file__).parents[1]


@pytest.fixture(name="warehouse_db")
def _verification_warehouse_db(request):
    return request.getfixturevalue("_warehouse_db_fixture")


@pytest.mark.parametrize("autocommit", [True, False])
@pytest.mark.parametrize("suffix", ["access", "ledger"])
def test_verification_guards_work_with_both_runner_modes(warehouse_db, autocommit, suffix):
    conn, _ = warehouse_db
    query(conn, "CREATE TABLE public.verification_write_probe(id integer)")
    source = (ROOT / f"docs/plans/2026-09-07-f06-production-{suffix}.sql").read_text()
    preamble, _ = source.split("DO $verify$", 1)
    conn.autocommit = autocommit
    try:
        with conn.cursor() as cur:
            # Execute the simple setup statements separately, as psql does. Sending the
            # entire file in one execute call can conceal missing transaction boundaries.
            for statement in preamble.split(";"):
                if statement.strip():
                    cur.execute(statement)
            cur.execute("SHOW transaction_read_only")
            assert cur.fetchone() == ("on",)
            cur.execute("SELECT setting::integer FROM pg_settings WHERE name='statement_timeout'")
            assert cur.fetchone()[0] > 0
            with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
                cur.execute("INSERT INTO public.verification_write_probe VALUES (1)")
            closing_statement = source.strip().splitlines()[-1]
            assert closing_statement == "ROLLBACK;"
            cur.execute(closing_statement)
            cur.execute("SHOW transaction_read_only")
            assert cur.fetchone() == ("off",)
    finally:
        conn.rollback()
        conn.autocommit = False
    assert query(conn, "SELECT count(*) FROM public.verification_write_probe") == [(0,)]
