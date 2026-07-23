"""meta.flat_file_loads ledger: hash-skip + load recording (T3).

The ledger makes the daily cron idempotent: a (source, sha256) pair recorded
with status='loaded' means those exact bytes are already in the warehouse, so
re-fetching an unchanged weekly/annual file is a cheap no-op
(status=skipped_hash in the driver). psycopg2 (repo idiom B); DSN resolution
mirrors the copy-pasted get_db_url() convention (dlt secrets first, then
SUPABASE_DB_URL / DATABASE_URL env). Lives in src/ so both the dlt source
layer and scripts/ can import it (src never imports scripts).
"""

import logging
import os
from datetime import datetime

import psycopg2

logger = logging.getLogger(__name__)

VALID_STATUSES = ("loaded", "skipped", "failed")


def get_db_url() -> str:
    """Resolve the Postgres DSN: dlt secrets, then SUPABASE_DB_URL/DATABASE_URL env.

    Implemented in T3 (mirror scripts/compute_house_elo.py:299-327 semantics).
    """
    import dlt

    url = None
    try:
        creds = dlt.secrets.get("destination.postgres.credentials")
        if creds:
            url = str(creds)
    except Exception:
        pass

    if not url:
        url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")

    if not url:
        raise RuntimeError(
            "No database URL found. Set destination.postgres.credentials in "
            ".dlt/secrets.toml or SUPABASE_DB_URL environment variable."
        )

    return url


def already_loaded(source: str, sha256: str, db_url: str | None = None) -> bool:
    """True if (source, sha256) has a status='loaded' ledger row. Implemented in T3."""
    dsn = db_url or get_db_url()
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM meta.flat_file_loads
                    WHERE source = %s AND file_sha256 = %s AND status = 'loaded'
                )
                """,
                (source, sha256),
            )
            (exists,) = cur.fetchone()
            return bool(exists)
    finally:
        conn.close()


def record_load(
    source: str,
    sha256: str,
    *,
    status: str,
    source_url: str | None = None,
    row_count: int | None = None,
    error: str | None = None,
    db_url: str | None = None,
) -> None:
    """Insert a ledger row (append-only; status in VALID_STATUSES). Implemented in T3."""
    if status not in VALID_STATUSES:
        raise ValueError(f"Invalid status {status!r}; must be one of {VALID_STATUSES}")

    dsn = db_url or get_db_url()
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO meta.flat_file_loads
                    (source, file_sha256, source_url, row_count, status, error)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (source, sha256, source_url, row_count, status, error),
            )
        conn.commit()
    finally:
        conn.close()


def last_success(source: str, db_url: str | None = None) -> datetime | None:
    """Latest loaded_at with status='loaded' for the source (drives --due). Implemented in T3."""
    dsn = db_url or get_db_url()
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT MAX(loaded_at) FROM meta.flat_file_loads
                WHERE source = %s AND status = 'loaded'
                """,
                (source,),
            )
            (loaded_at,) = cur.fetchone()
            return loaded_at
    finally:
        conn.close()
