# F18 flat-file retry hardening

## Problem

The flat-file HTTP fetcher converted `Retry-After` directly with `int()`, so
valid HTTP-dates and malformed values escaped from the rate-limit handler. It
also accepted arbitrarily large delays and slept after the final 429 before
raising an unrelated `RuntimeError`. This made a remote header capable of
parking the job and discarded the terminal HTTP response needed for diagnosis.

## Implementation

- Put RFC 9110 `Retry-After` parsing in a provider-neutral utility shared by
  the flat-file fetcher and `CFBDClient`.
- Accept integer seconds and HTTP-dates, treat past dates as zero, use the
  configured default for missing, malformed, and negative values, and cap all
  outcomes at 120 seconds.
- Keep the flat-file fetcher's existing three-retry budget and 1/2/3-second
  transient backoff. Its default maximum 429 wait is therefore 360 seconds.
- Check the flat-file attempt budget before parsing or sleeping, then re-raise
  the terminal `httpx.HTTPStatusError` with its response intact.
- Keep CFBD's separate rate-limit and transient retry budgets, circuit breaker,
  exception types, and public compatibility methods unchanged.

## Verification

Unit tests use `httpx.MockTransport` and a fake clock to cover successful and
terminal retries without network access or real sleeping. Cases include
numeric, HTTP-date, past, negative, malformed, and excessive headers; terminal
429 error preservation; the 360-second default wait bound; and unchanged 5xx
and connection-error attempt and backoff budgets. Existing CFBD retry tests
exercise the shared parser through `CFBDClient`.

No provider requests or database operations are required for this change.
