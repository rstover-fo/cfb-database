"""Tests for provider-neutral HTTP retry header parsing."""

from datetime import UTC, datetime

import pytest

from src.pipelines.utils.http_retries import (
    MAX_RETRY_AFTER_SECONDS,
    http_date_delay,
    parse_retry_after,
)

NOW = datetime(2026, 10, 21, 7, 28, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("30", 30),
        (" 45 ", 45),
        (None, 60),
        ("", 60),
        ("soon", 60),
        ("-5", 60),
        ("86400", MAX_RETRY_AFTER_SECONDS),
        ("Wed, 21 Oct 2026 07:29:30 GMT", 90),
        ("Wed, 21 Oct 2026 07:00:00 GMT", 0),
        ("Fri, 25 Dec 2026 00:00:00 GMT", MAX_RETRY_AFTER_SECONDS),
    ],
)
def test_parse_retry_after_is_defensive_and_bounded(raw, expected):
    assert parse_retry_after(raw, now=NOW) == expected


def test_naive_http_date_and_clock_are_interpreted_as_gmt():
    naive_now = NOW.replace(tzinfo=None)
    assert http_date_delay("Wed, 21 Oct 2026 07:29:00", now=naive_now) == 60


@pytest.mark.parametrize(
    "default_seconds,max_seconds",
    [(-1, 120), (60, -1)],
)
def test_invalid_bounds_are_rejected(default_seconds, max_seconds):
    with pytest.raises(ValueError):
        parse_retry_after(
            "1",
            default_seconds=default_seconds,
            max_seconds=max_seconds,
            now=NOW,
        )


def test_default_is_also_capped():
    assert parse_retry_after(None, default_seconds=600, max_seconds=120) == 120
