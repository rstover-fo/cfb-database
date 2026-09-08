"""Shared HTTP retry helpers that do not define provider retry policy."""

import logging
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

logger = logging.getLogger(__name__)

DEFAULT_RETRY_AFTER_SECONDS = 60
MAX_RETRY_AFTER_SECONDS = 120


def http_date_delay(text: str, *, now: datetime | None = None) -> int | None:
    """Return whole seconds until an HTTP-date, or ``None`` if it is invalid."""
    try:
        when = parsedate_to_datetime(text)
    except (OverflowError, TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        # RFC 9110 fixes HTTP-dates to GMT; a missing offset is not local time.
        when = when.replace(tzinfo=UTC)
    reference = now if now is not None else datetime.now(UTC)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=UTC)
    return max(0, int((when - reference).total_seconds()))


def parse_retry_after(
    raw: str | None,
    *,
    default_seconds: int = DEFAULT_RETRY_AFTER_SECONDS,
    max_seconds: int = MAX_RETRY_AFTER_SECONDS,
    now: datetime | None = None,
) -> int:
    """Parse and bound an RFC 9110 ``Retry-After`` header.

    The header may be either non-negative integer seconds or an HTTP-date.
    Missing, malformed, and negative values use ``default_seconds``. Past
    HTTP-dates return zero. Every valid or default delay is capped so a remote
    server cannot create an unbounded sleep.
    """
    if default_seconds < 0:
        raise ValueError("default_seconds must be non-negative")
    if max_seconds < 0:
        raise ValueError("max_seconds must be non-negative")

    fallback = min(default_seconds, max_seconds)
    if raw is None:
        return fallback
    text = str(raw).strip()
    if not text:
        return fallback
    try:
        seconds = int(text)
    except (TypeError, ValueError):
        seconds = http_date_delay(text, now=now)
        if seconds is None:
            logger.warning("Unparseable Retry-After header %r; using %ds", raw, fallback)
            return fallback
    if seconds < 0:
        return fallback
    return min(seconds, max_seconds)
