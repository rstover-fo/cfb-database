"""Opt-in durable CFBD admission for enrolled command invocations.

The active invocation is process-wide so dlt extraction threads cannot lose it.
One invocation per process is supported; overlapping/nested invocations fail.
Every SQL call uses its own committed transaction under warehouse_ingest.
"""

from __future__ import annotations

import os
import re
import threading
import uuid
from contextlib import contextmanager
from typing import Any

import psycopg2
from psycopg2.extras import Json


class QuotaAdmissionError(RuntimeError):
    """Durable accounting could not safely authorize continued transport."""


class QuotaDeniedError(QuotaAdmissionError):
    """A committed database decision denied this attempt, not other budget classes."""


_ACTIVE: QuotaOperation | None = None
_OPERATION_LOCK = threading.Lock()
_PATH = re.compile(r"/[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*\Z")


def durable_quota_enabled() -> bool:
    mode = os.environ.get("CFBD_QUOTA_MODE", "legacy")
    if mode not in {"legacy", "durable"}:
        raise QuotaAdmissionError("CFBD_QUOTA_MODE must be legacy or durable")
    return mode == "durable"


def active_operation() -> QuotaOperation | None:
    return _ACTIVE


def transport_operation() -> QuotaOperation | None:
    operation = active_operation()
    if operation is None and durable_quota_enabled():
        raise QuotaAdmissionError("Durable CFBD transport requires an enrolled operation")
    return operation


class QuotaOperation:
    def __init__(self, dsn: str, account: str, initiator: str, scope: dict[str, Any]):
        self._dsn = dsn
        self.account = account
        self.run_id = str(uuid.uuid4())
        self.initiator = initiator
        self.scope = scope
        self.outcome = "succeeded"
        self._unavailable = threading.Event()
        self.transport_lock = threading.RLock()

    def _call(self, statement: str, args: tuple = (), *, allow_denial: bool = False):
        conn = None
        try:
            conn = psycopg2.connect(self._dsn, connect_timeout=10)
            with conn.cursor() as cur:
                cur.execute("SET LOCAL ROLE warehouse_ingest")
                cur.execute("SET LOCAL statement_timeout = '15s'")
                cur.execute(statement, args)
                rows = cur.fetchall() if cur.description else None
            conn.commit()
            return rows
        except Exception as error:
            if (
                allow_denial
                and isinstance(error, psycopg2.Error)
                and error.pgcode in {"22023", "P0001"}
            ):
                self.outcome = "failed"
                raise QuotaDeniedError("Durable quota denied this attempt") from None
            # SQL messages/connection exceptions can contain credentials, request
            # context, or internal server details. Do not chain them to CLI logs.
            self._unavailable.set()
            self.outcome = "failed"
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise QuotaAdmissionError(
                "Durable quota transaction failed; no further HTTP allowed"
            ) from None
        finally:
            if conn is not None:
                conn.close()

    def start(self) -> None:
        self._call(
            "SELECT warehouse_quota.start_operation_run(%s,'extract',%s,%s,NULL,NULL)",
            (self.run_id, self.initiator, Json(self.scope)),
        )

    def reserve(self, endpoint: str, retry: int, params: dict | None) -> str:
        if self._unavailable.is_set():
            raise QuotaAdmissionError("Durable quota accounting is unavailable; HTTP denied")
        if not _PATH.fullmatch(endpoint):
            self.outcome = "failed"
            raise QuotaAdmissionError("CFBD endpoint must be a canonical API path")
        attempt_id = str(uuid.uuid4())
        # Store only numeric/boolean work-unit identifiers. Tokens, free text,
        # URLs, headers and other arbitrary request values never enter the ledger.
        context = {
            key: value
            for key, value in (params or {}).items()
            if key in {"year", "week", "gameId", "teamId", "playerId"}
            and isinstance(value, (int, bool))
        }
        self._call(
            "SELECT * FROM warehouse_quota.reserve_cfbd_transport_attempt(%s,%s,%s,%s,%s,%s)",
            (attempt_id, self.run_id, self.account, endpoint, retry, Json(context)),
            allow_denial=True,
        )
        return attempt_id

    def dispatch(self, attempt_id: str) -> None:
        if self._unavailable.is_set():
            raise QuotaAdmissionError("Durable quota accounting is unavailable; HTTP denied")
        self._call("SELECT warehouse_quota.mark_cfbd_attempt_dispatched(%s)", (attempt_id,))

    def result(self, attempt_id: str, state: str, status: int | None, category: str | None) -> None:
        self._call(
            "SELECT warehouse_quota.record_cfbd_attempt_result(%s,%s,%s,%s)",
            (attempt_id, state, status, category),
        )

    def note_transport_failure(self, outcome: str) -> None:
        with self.transport_lock:
            if self.outcome != "failed":
                self.outcome = outcome

    def finish(self, failed: bool = False) -> None:
        outcome = "failed" if failed or self._unavailable.is_set() else self.outcome
        self._call(
            "SELECT warehouse_quota.finish_operation_run(%s,%s,%s)",
            (self.run_id, outcome, "invocation_failed" if outcome == "failed" else None),
        )


@contextmanager
def quota_operation(initiator: str, scope: dict[str, Any], *, enabled: bool = True):
    """Enroll only selected jobs; opt-in is separate from migration rollout.

    Legacy is the unchanged deployment default until a separately authorized
    rollout sets CFBD_QUOTA_MODE=durable. Once selected there is no fallback.
    Planning-only paths pass enabled=False and perform no quota writes.
    """
    global _ACTIVE
    if not enabled or not durable_quota_enabled():
        yield None
        return
    dsn = os.environ.get("CFBD_QUOTA_DB_URL")
    account = os.environ.get("CFBD_QUOTA_ACCOUNT")
    if not dsn or not account or not account.strip():
        raise QuotaAdmissionError("Durable quota requires CFBD_QUOTA_DB_URL and CFBD_QUOTA_ACCOUNT")
    if not _OPERATION_LOCK.acquire(blocking=False):
        raise QuotaAdmissionError("Only one durable CFBD invocation per process is supported")
    operation = QuotaOperation(dsn, account, initiator, scope)
    try:
        operation.start()  # Committed before it can become visible to clients.
        _ACTIVE = operation
        try:
            yield operation
        except BaseException:
            try:
                operation.finish(failed=True)
            except QuotaAdmissionError:
                pass  # Preserve original failure; unclosed parent is never success.
            raise
        else:
            operation.finish()
    finally:
        _ACTIVE = None
        _OPERATION_LOCK.release()
