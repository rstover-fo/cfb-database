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
from datetime import datetime

logger = logging.getLogger(__name__)

VALID_STATUSES = ("loaded", "skipped", "failed")


def get_db_url() -> str:
    """Resolve the Postgres DSN: dlt secrets, then SUPABASE_DB_URL/DATABASE_URL env.

    Implemented in T3 (mirror scripts/compute_house_elo.py:299-327 semantics).
    """
    raise NotImplementedError("T3 implements get_db_url")


def already_loaded(source: str, sha256: str, db_url: str | None = None) -> bool:
    """True if (source, sha256) has a status='loaded' ledger row. Implemented in T3."""
    raise NotImplementedError("T3 implements already_loaded")


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
    raise NotImplementedError("T3 implements record_load")


def last_success(source: str, db_url: str | None = None) -> datetime | None:
    """Latest loaded_at with status='loaded' for the source (drives --due). Implemented in T3."""
    raise NotImplementedError("T3 implements last_success")
