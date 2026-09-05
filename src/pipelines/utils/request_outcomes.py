"""Request-level outcomes for bounded CFBD resource invocations."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any


class ResponseValidationError(ValueError):
    """A successful HTTP response did not contain the resource's required shape."""


class SourceRequestError(Exception):
    """A failed request with the progress of its current resource invocation."""

    def __init__(
        self,
        endpoint: str,
        params: Mapping[str, Any],
        error: BaseException,
        counts: Mapping[str, int],
    ) -> None:
        self.endpoint = endpoint
        self.params = dict(params)
        self.error_type = type(error).__name__
        self.counts = dict(counts)
        counts_text = ", ".join(f"{name}={count}" for name, count in self.counts.items())
        super().__init__(
            f"CFBD request failed for endpoint={endpoint} params={self.params}; "
            "fetched request outcomes for this resource invocation: "
            f"{counts_text}; error_type={self.error_type}: {error}"
        )

    def to_summary(self) -> dict[str, Any]:
        """Return a JSON-serializable failure summary for the job boundary."""
        return {
            "endpoint": self.endpoint,
            "params": dict(self.params),
            "outcome": "failed",
            "error_type": self.error_type,
            "counts_scope": "resource_invocation",
            "counts_unit": "requests",
            "counts": dict(self.counts),
        }


def request_failure_summary(error: BaseException) -> dict[str, Any] | None:
    """Find a request failure summary anywhere in an exception chain."""
    pending: list[BaseException] = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, SourceRequestError):
            return current.to_summary()
        if current.__context__ is not None:
            pending.append(current.__context__)
        if current.__cause__ is not None:
            pending.append(current.__cause__)
    return None


class RequestOutcomeTracker:
    """Track fetched request outcomes within one bounded resource invocation."""

    def __init__(self, endpoint: str, planned_requests: int, logger: logging.Logger) -> None:
        if planned_requests < 0:
            raise ValueError("planned_requests must be non-negative")
        self.endpoint = endpoint
        self.planned_requests = planned_requests
        self.logger = logger
        self._succeeded = 0
        self._expected_no_data = 0
        self._failed = 0

    def record_response(self, params: Mapping[str, Any], records: list[dict]) -> None:
        """Record a validated response as successful data or expected no-data."""
        if records:
            self._succeeded += 1
            outcome = "succeeded"
        else:
            self._expected_no_data += 1
            outcome = "expected_no_data"
        self.logger.info(
            "CFBD request receipt outcome=%s endpoint=%s params=%s fetched_rows=%d "
            "resource_invocation_progress=%s",
            outcome,
            self.endpoint,
            dict(params),
            len(records),
            self._progress(),
        )

    def failure(self, params: Mapping[str, Any], error: BaseException) -> SourceRequestError:
        """Record one failed request and return its contextual wrapper."""
        self._failed += 1
        counts = self._counts()
        self.logger.error(
            "CFBD request receipt outcome=failed endpoint=%s params=%s error_type=%s "
            "resource_invocation_counts=%s; counts describe fetched requests, not loaded rows",
            self.endpoint,
            dict(params),
            type(error).__name__,
            counts,
        )
        return SourceRequestError(self.endpoint, params, error, counts)

    def _counts(self) -> dict[str, int]:
        attempted = self._succeeded + self._expected_no_data + self._failed
        return {
            "succeeded": self._succeeded,
            "expected_no_data": self._expected_no_data,
            "failed": self._failed,
            "deferred": max(0, self.planned_requests - attempted),
        }

    def _progress(self) -> dict[str, int]:
        attempted = self._succeeded + self._expected_no_data + self._failed
        return {
            "succeeded": self._succeeded,
            "expected_no_data": self._expected_no_data,
            "failed": self._failed,
            "pending": max(0, self.planned_requests - attempted),
        }


def validate_record_list(data: object, required_id: str = "id") -> list[dict]:
    """Validate a complete request payload before any of its rows are yielded."""
    if not isinstance(data, list):
        raise ResponseValidationError(f"expected a list of records, got {type(data).__name__}")

    for index, record in enumerate(data):
        if not isinstance(record, dict):
            raise ResponseValidationError(
                f"expected record {index} to be a dict, got {type(record).__name__}"
            )
        if required_id not in record or record[required_id] is None:
            raise ResponseValidationError(
                f"record {index} is missing required non-null provider id {required_id!r}"
            )
    return data
