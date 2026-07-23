"""Conference availability-report PDF archiver (T7).

Archive-only (kind="archiver"): discover per-conference report PDFs, store raw
bytes in raw.availability_reports (sha256 PK, ON CONFLICT DO NOTHING).
Structured parsing of report contents is an explicit follow-up -- the archive
is the time-sensitive asset (history only exists from 2023+ and old PDFs are
not guaranteed to stay up).

Conference discovery (URL patterns verified in T2 -- see
tests/fixtures/flatfiles/FINDINGS.md section 3):
- Big Ten: two real, directly-fetchable patterns. Legacy S3:
  ``s3.amazonaws.com/bigten.org/documents/{y}/{m}/{d}/FB_Reporting_Week_{n}.pdf``.
  Newer CDN: ``bigten.org/api/media/file/{uuid}-FB_Reporting_Week_{n}.pdf``.
  Both are discoverable (when linked at all) from the ``bigten.org/fb/
  availability-reports/`` and ``bigten.org/fb/archive/`` index pages -- FINDINGS
  notes the index pages are normally a client-rendered SPA shell with no
  static links, but we still probe the plain and season-suffixed variants of
  both bases and extract whatever anchors/CDN URLs a given snapshot happens to
  expose, tolerating 404s along the way.
- SEC, Big 12, CFP: all three route through the same third-party JS-only
  iframe widget (``confinjrepxyz.hdintelligence-app.com``) with no first-party
  JSON/PDF endpoint discoverable via static fetch (FINDINGS 3b-3d). These are
  permanent ``gaps`` entries until a headless-browser step exists.

Contract:
- ``archive(db_url, season, fetch)`` performs discovery + archival for Big Ten
  and returns ``{"fetched": int, "new": int, "gaps": list[str]}``. ``gaps``
  always includes SEC/B12/CFP (documented JS-widget block) and additionally
  names "B1G" when Big Ten index discovery itself fails outright (every index
  variant errors, or none yield any PDF links) -- a real discovery failure
  must surface as a gap, never silently produce zero rows.
- Discovery/filename-hint helpers (URL pattern building, link extraction,
  conference/date/week parsing from URLs) are pure functions, unit-tested
  without network.
- Politeness: a PDF whose ``source_url`` is already archived is never
  re-fetched; a full weekly run is a handful of index requests plus at most a
  few new PDFs.
- Failure isolation: one PDF failing to fetch logs a warning and the run
  continues with the rest; only total Big Ten index discovery failure adds
  the "B1G" gap.
"""

import logging
import re
from datetime import date
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import psycopg2

from ...utils import load_ledger
from ...utils.file_fetcher import fetch_file
from ..flat_files import ParseContext

__all__ = ["archive", "bigten_index_urls", "extract_pdf_links", "parse_report_hint"]

_ = ParseContext  # imported for signature parity; archiver defines its own flow

logger = logging.getLogger(__name__)

CONFERENCE = "B1G"
PERMANENT_GAPS = ("SEC", "B12", "CFP")

_BIGTEN_INDEX_BASES = (
    "https://bigten.org/fb/availability-reports/",
    "https://bigten.org/fb/archive/",
)

_LEGACY_PATH_RE = re.compile(
    r"/documents/(?P<year>\d{4})/(?P<month>\d{1,2})/(?P<day>\d{1,2})/(?P<name>[^/]+)\.pdf$",
    re.IGNORECASE,
)
_WEEK_RE = re.compile(r"week[_\s]?(?P<week>\d+)", re.IGNORECASE)
_BOWL_RE = re.compile(r"bowl", re.IGNORECASE)
_CDN_LINK_RE = re.compile(
    r'https?://[a-zA-Z0-9.\-]*bigten\.org/api/media/file/[^\s"\'<>)]+',
    re.IGNORECASE,
)


class _AnchorHrefExtractor(HTMLParser):
    """Collects every ``<a href="...">`` value in document order."""

    def __init__(self):
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name.lower() == "href" and value:
                self.hrefs.append(value)


def bigten_index_urls(season: int) -> list[str]:
    """Index page URL variants to probe for a season's Big Ten reports.

    Two known bases (``availability-reports``, ``archive``), each tried plain
    and with a season-suffixed path segment -- FINDINGS.md notes these pages
    are sometimes season-scoped SPA routes and sometimes not, and year-variant
    paths 404 rather than list-and-fail, so all four are worth a shot.
    """
    urls = []
    for base in _BIGTEN_INDEX_BASES:
        urls.append(base)
        urls.append(f"{base}{season}/")
    return urls


def extract_pdf_links(html: str, base_url: str) -> list[str]:
    """Pull availability-report PDF links out of a Big Ten index page.

    Two forms observed (FINDINGS.md sec 3a): legacy ``<a href=...pdf>``
    anchors (resolved against ``base_url``), and CDN
    ``.../api/media/file/{uuid}-...pdf`` URLs that may appear as bare text
    (e.g. inside inline JSON/script data) rather than anchor hrefs. Returns
    links in document order, deduplicated.
    """
    parser = _AnchorHrefExtractor()
    parser.feed(html)

    links: list[str] = []
    seen: set[str] = set()

    for href in parser.hrefs:
        if href.lower().endswith(".pdf"):
            url = urljoin(base_url, href)
            if url not in seen:
                seen.add(url)
                links.append(url)

    for match in _CDN_LINK_RE.finditer(html):
        url = match.group(0).rstrip(").,;")
        if url not in seen:
            seen.add(url)
            links.append(url)

    return links


def parse_report_hint(url: str) -> dict:
    """Best-effort conference/date/week extraction from a report URL.

    Returns ``{"conference": str | None, "report_date": date | None,
    "hint": str | None}``. Only the legacy S3 path
    (``.../documents/{y}/{m}/{d}/FB_Reporting_Week_{n}.pdf``) reliably yields
    a date; CDN URLs (opaque UUID prefix) and anything else fall back to
    filename-tail sniffing for a week/bowl hint, or all-``None`` when nothing
    is parseable.
    """
    result: dict = {"conference": None, "report_date": None, "hint": None}

    parsed = urlparse(url)
    # bigten.org shows up as the netloc for the CDN pattern but as an S3
    # bucket-name path segment for the legacy pattern -- check the whole URL.
    if "bigten.org" in url.lower():
        result["conference"] = "B1G"

    path = parsed.path
    match = _LEGACY_PATH_RE.search(path)
    if match:
        try:
            result["report_date"] = date(int(match["year"]), int(match["month"]), int(match["day"]))
        except ValueError:
            pass
        name = match["name"]
    else:
        tail = path.rsplit("/", 1)[-1]
        name = tail[: -len(".pdf")] if tail.lower().endswith(".pdf") else None

    if name:
        week_match = _WEEK_RE.search(name)
        if week_match:
            result["hint"] = f"Week {week_match['week']}"
        elif _BOWL_RE.search(name):
            result["hint"] = "Bowls"

    return result


def _discover_bigten_links(season: int, fetch) -> tuple[list[str], int, bool]:
    """Fetch every index URL variant and merge extracted PDF links.

    Returns (links, requests_made, any_index_fetch_succeeded).
    """
    links: list[str] = []
    seen: set[str] = set()
    requests_made = 0
    any_ok = False

    for index_url in bigten_index_urls(season):
        requests_made += 1
        try:
            fetched = fetch(index_url)
        except Exception as e:
            logger.warning("B1G index fetch failed for %s: %s", index_url, e)
            continue

        any_ok = True
        html = fetched.content.decode("utf-8", errors="replace")
        for link in extract_pdf_links(html, index_url):
            if link not in seen:
                seen.add(link)
                links.append(link)

    return links, requests_made, any_ok


def archive(db_url: str | None, *, season: int, fetch=None) -> dict:
    """Discover and archive availability-report PDFs for all conferences.

    Args:
        db_url: Postgres DSN; None resolves via the ledger module's get_db_url().
        season: Season to discover reports for.
        fetch: Injectable fetcher (defaults to file_fetcher.fetch_file) for tests.

    Returns:
        {"fetched": n_requests, "new": n_new_rows, "gaps": [conference, ...]}
    """
    fetch = fetch or fetch_file
    dsn = db_url or load_ledger.get_db_url()

    gaps: list[str] = list(PERMANENT_GAPS)
    fetched = 0
    new = 0

    conn = psycopg2.connect(dsn)
    try:
        links, index_requests, any_index_ok = _discover_bigten_links(season, fetch)
        fetched += index_requests

        if not any_index_ok or not links:
            gaps.append("B1G")
        else:
            with conn.cursor() as cur:
                for link in links:
                    cur.execute(
                        "SELECT 1 FROM raw.availability_reports WHERE source_url = %s",
                        (link,),
                    )
                    if cur.fetchone() is not None:
                        continue

                    try:
                        fetched += 1
                        pdf = fetch(link)
                    except Exception as e:
                        logger.warning("B1G PDF fetch failed for %s: %s", link, e)
                        continue

                    hint = parse_report_hint(link)
                    cur.execute(
                        """
                        INSERT INTO raw.availability_reports
                            (sha256, conference, source_url, report_hint, report_date, pdf)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (sha256) DO NOTHING
                        """,
                        (
                            pdf.sha256,
                            CONFERENCE,
                            link,
                            hint["hint"],
                            hint["report_date"],
                            psycopg2.Binary(pdf.content),
                        ),
                    )
                    new += cur.rowcount
            conn.commit()
    finally:
        conn.close()

    return {"fetched": fetched, "new": new, "gaps": gaps}
