"""Conference availability-report PDF archiver (T7).

Archive-only (kind="archiver"): discover per-conference report PDFs, store raw
bytes in raw.availability_reports (sha256 PK, ON CONFLICT DO NOTHING) plus a
meta.flat_file_loads ledger row. Structured parsing of report contents is an
explicit follow-up -- the archive is the time-sensitive asset (history only
exists from 2023+ and old PDFs are not guaranteed to stay up).

Conference discovery (URL patterns verified in T2 -- see
tests/fixtures/flatfiles/FINDINGS.md):
- Big Ten: S3-hosted per-week PDFs, e.g.
  s3.amazonaws.com/bigten.org/documents/{y}/{m}/{d}/FB_Reporting_Week_{n}.pdf,
  discoverable from bigten.org availability-report index pages.
- SEC: secsports.com/fbreports -- JS-rendered; use the underlying JSON/XHR
  endpoint if T2 found one, otherwise report the conference as a gap.
- Big 12 / CFP: per T2 findings.

Contract:
- ``archive(db_url, season, fetch)`` performs discovery + archival for all
  configured conferences and returns
  ``{"fetched": int, "new": int, "gaps": list[str]}`` -- ``gaps`` names
  conferences that could not be discovered (driver emits status=gap; never
  silently skip).
- Discovery/filename-hint helpers (URL pattern building, conference/date/week
  parsing from URLs) are pure functions, unit-tested without network.
- Politeness: only fetch URLs not already present (by source_url or sha256);
  a full weekly run is tens of requests at most.
"""

from datetime import date

from ..flat_files import ParseContext

__all__ = ["archive"]

_ = ParseContext  # imported for signature parity; archiver defines its own flow
_ = date


def archive(db_url: str | None, *, season: int, fetch=None) -> dict:
    """Discover and archive availability-report PDFs for all conferences.

    Args:
        db_url: Postgres DSN; None resolves via the ledger module's get_db_url().
        season: Season to discover reports for.
        fetch: Injectable fetcher (defaults to file_fetcher.fetch_file) for tests.

    Returns:
        {"fetched": n_requests, "new": n_new_rows, "gaps": [conference, ...]}
    """
    raise NotImplementedError("T7 implements availability.archive")
