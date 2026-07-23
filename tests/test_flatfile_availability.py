"""Unit tests for the availability-report archiver (T7): pure functions and
stub-based tests only -- no live DB, no network.
"""

from datetime import date

from src.pipelines.sources.flatfile_parsers import availability
from src.pipelines.utils.file_fetcher import FetchedFile

# ---------------------------------------------------------------------------
# bigten_index_urls
# ---------------------------------------------------------------------------


class TestBigtenIndexUrls:
    def test_returns_plain_and_season_suffixed_variants_for_both_bases(self):
        urls = availability.bigten_index_urls(2025)

        assert urls == [
            "https://bigten.org/fb/availability-reports/",
            "https://bigten.org/fb/availability-reports/2025/",
            "https://bigten.org/fb/archive/",
            "https://bigten.org/fb/archive/2025/",
        ]

    def test_season_is_embedded_per_year(self):
        urls_2023 = availability.bigten_index_urls(2023)
        urls_2024 = availability.bigten_index_urls(2024)

        assert "https://bigten.org/fb/availability-reports/2023/" in urls_2023
        assert "https://bigten.org/fb/availability-reports/2024/" in urls_2024
        assert urls_2023 != urls_2024


# ---------------------------------------------------------------------------
# extract_pdf_links
# ---------------------------------------------------------------------------

# Modeled on FINDINGS.md sec 3a: a legacy S3 .pdf anchor plus a CDN
# api/media/file link embedded as bare text inside inline page data (the
# index pages are normally a client-rendered SPA with no static <a href>s to
# the CDN form -- when they do surface, it's via embedded JSON, not markup).
_SAMPLE_INDEX_HTML = """
<html>
<body>
<div id="reports">
  <a href="https://s3.amazonaws.com/bigten.org/documents/2023/9/30/FB_Reporting_Week_5.pdf">
    Week 5 Report
  </a>
</div>
<script>
window.__NEXT_DATA__ = {"props": {"reports": [
  {"url": "https://bigten.org/api/media/file/899ca8f3-51fd-4db0-b152-47d7925f615c-FB_Reporting_Week_5.pdf"}
]}};
</script>
</body>
</html>
"""


class TestExtractPdfLinks:
    def test_extracts_both_anchor_pdf_and_cdn_link(self):
        links = availability.extract_pdf_links(
            _SAMPLE_INDEX_HTML, "https://bigten.org/fb/availability-reports/"
        )

        assert (
            "https://s3.amazonaws.com/bigten.org/documents/2023/9/30/FB_Reporting_Week_5.pdf"
            in links
        )
        assert (
            "https://bigten.org/api/media/file/"
            "899ca8f3-51fd-4db0-b152-47d7925f615c-FB_Reporting_Week_5.pdf" in links
        )
        assert len(links) == 2

    def test_relative_href_resolved_against_base_url(self):
        html = '<a href="/documents/2023/9/30/FB_Reporting_Week_5.pdf">Week 5</a>'
        links = availability.extract_pdf_links(html, "https://s3.amazonaws.com/bigten.org/")

        assert links == ["https://s3.amazonaws.com/documents/2023/9/30/FB_Reporting_Week_5.pdf"]

    def test_non_pdf_hrefs_ignored(self):
        html = '<a href="https://bigten.org/fb/schedule/">Schedule</a>'
        links = availability.extract_pdf_links(html, "https://bigten.org/fb/schedule/")

        assert links == []

    def test_no_links_returns_empty_list(self):
        assert availability.extract_pdf_links("<html><body>nothing here</body></html>", "x") == []

    def test_duplicate_links_deduplicated(self):
        html = (
            '<a href="https://s3.amazonaws.com/bigten.org/documents/2023/9/30/'
            'FB_Reporting_Week_5.pdf">A</a>'
            '<a href="https://s3.amazonaws.com/bigten.org/documents/2023/9/30/'
            'FB_Reporting_Week_5.pdf">B</a>'
        )
        links = availability.extract_pdf_links(html, "https://bigten.org/")

        assert links == [
            "https://s3.amazonaws.com/bigten.org/documents/2023/9/30/FB_Reporting_Week_5.pdf"
        ]


# ---------------------------------------------------------------------------
# parse_report_hint
# ---------------------------------------------------------------------------


class TestParseReportHint:
    def test_legacy_s3_pattern_yields_date_and_week_hint(self):
        result = availability.parse_report_hint(
            "https://s3.amazonaws.com/bigten.org/documents/2023/9/30/FB_Reporting_Week_5.pdf"
        )

        assert result["report_date"] == date(2023, 9, 30)
        assert result["hint"] == "Week 5"
        assert result["conference"] == "B1G"

    def test_legacy_s3_bowls_pattern_yields_bowls_hint_no_week(self):
        result = availability.parse_report_hint(
            "https://s3.amazonaws.com/bigten.org/documents/2024/1/1/FB_Reporting_Bowls_ALL.pdf"
        )

        assert result["report_date"] == date(2024, 1, 1)
        assert result["hint"] == "Bowls"

    def test_cdn_pattern_yields_week_hint_no_date(self):
        result = availability.parse_report_hint(
            "https://bigten.org/api/media/file/"
            "899ca8f3-51fd-4db0-b152-47d7925f615c-FB_Reporting_Week_5.pdf"
        )

        assert result["report_date"] is None
        assert result["hint"] == "Week 5"
        assert result["conference"] == "B1G"

    def test_unparseable_url_returns_all_nones(self):
        result = availability.parse_report_hint("https://example.com/foo/bar")

        assert result == {"conference": None, "report_date": None, "hint": None}

    def test_non_bigten_domain_has_no_conference(self):
        result = availability.parse_report_hint("https://example.com/reports/week5.pdf")

        assert result["conference"] is None


# ---------------------------------------------------------------------------
# archive() -- DB and network fully stubbed
# ---------------------------------------------------------------------------

LINK_A = "https://s3.amazonaws.com/bigten.org/documents/2023/9/30/FB_Reporting_Week_5.pdf"
LINK_B = (
    "https://bigten.org/api/media/file/899ca8f3-51fd-4db0-b152-47d7925f615c-FB_Reporting_Week_5.pdf"
)

_INDEX_HTML_BOTH_LINKS = f"""
<a href="{LINK_A}">Week 5</a>
<script>{{"url": "{LINK_B}"}}</script>
"""


class FakeCursor:
    """Records executed statements; answers SELECT/INSERT against existing_urls."""

    def __init__(self, conn):
        self.conn = conn
        self.rowcount = 0
        self._last_result = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((" ".join(sql.split()), params))
        normalized = " ".join(sql.split())

        if normalized.startswith("SELECT 1 FROM raw.availability_reports"):
            (source_url,) = params
            self._last_result = (1,) if source_url in self.conn.existing_urls else None
            self.rowcount = 1 if self._last_result else 0
        elif normalized.startswith("INSERT INTO raw.availability_reports"):
            source_url = params[2]
            if source_url in self.conn.existing_urls:
                self.rowcount = 0
            else:
                self.rowcount = 1
                self.conn.existing_urls.add(source_url)
                self.conn.inserted.append(params)
        else:  # pragma: no cover - defensive
            raise AssertionError(f"Unexpected SQL: {normalized}")

    def fetchone(self):
        return self._last_result


class FakeConnection:
    def __init__(self, existing_urls=None):
        self.executed = []
        self.inserted = []
        self.existing_urls = set(existing_urls or [])
        self.committed = False
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


def _patch_connect(monkeypatch, fake_conn):
    monkeypatch.setattr(availability.psycopg2, "connect", lambda dsn: fake_conn)


def _index_only_fetcher(*, index_html=_INDEX_HTML_BOTH_LINKS, index_url=None, pdf_responses=None):
    """Fake fetch(): the first bigten_index_urls() entry succeeds with
    index_html, all other index variants 404; PDF links resolve via
    pdf_responses (url -> FetchedFile | Exception).
    """
    pdf_responses = pdf_responses or {}
    good_index_url = index_url or availability.bigten_index_urls(2025)[0]

    def fetch(url):
        if url == good_index_url:
            return FetchedFile(
                content=index_html.encode("utf-8"), sha256="index-sha", source_url=url
            )
        if url in (u for u in availability.bigten_index_urls(2025) if u != good_index_url):
            raise RuntimeError(f"404 Not Found: {url}")
        if url in pdf_responses:
            outcome = pdf_responses[url]
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        raise AssertionError(f"Unexpected fetch() call: {url}")

    return fetch


class TestArchiveEndToEnd:
    def test_one_already_archived_only_fetches_and_inserts_the_new_one(self, monkeypatch):
        fake_conn = FakeConnection(existing_urls={LINK_A})
        _patch_connect(monkeypatch, fake_conn)

        fetch = _index_only_fetcher(
            pdf_responses={
                LINK_B: FetchedFile(
                    content=b"%PDF-1.4 fake bytes", sha256="b-sha", source_url=LINK_B
                )
            }
        )

        result = availability.archive("postgresql://fake", season=2025, fetch=fetch)

        assert result["gaps"] == ["SEC", "B12", "CFP"]
        assert result["new"] == 1
        # 4 index probes + 1 PDF fetch (LINK_A skipped -- already archived).
        assert result["fetched"] == 5

        assert len(fake_conn.inserted) == 1
        inserted_params = fake_conn.inserted[0]
        assert inserted_params[0] == "b-sha"  # sha256
        assert inserted_params[1] == "B1G"  # conference
        assert inserted_params[2] == LINK_B  # source_url
        assert fake_conn.committed
        assert fake_conn.closed

    def test_index_total_failure_adds_b1g_gap(self, monkeypatch):
        fake_conn = FakeConnection()
        _patch_connect(monkeypatch, fake_conn)

        def always_fails(url):
            raise RuntimeError(f"boom: {url}")

        result = availability.archive("postgresql://fake", season=2025, fetch=always_fails)

        assert "B1G" in result["gaps"]
        assert result["gaps"][:3] == ["SEC", "B12", "CFP"]
        assert result["new"] == 0
        assert result["fetched"] == 4  # all 4 index variants attempted
        assert fake_conn.inserted == []

    def test_zero_links_discovered_adds_b1g_gap(self, monkeypatch):
        fake_conn = FakeConnection()
        _patch_connect(monkeypatch, fake_conn)

        fetch = _index_only_fetcher(index_html="<html><body>nothing to see</body></html>")

        result = availability.archive("postgresql://fake", season=2025, fetch=fetch)

        assert "B1G" in result["gaps"]
        assert result["new"] == 0

    def test_single_pdf_fetch_failure_logs_warning_and_continues(self, monkeypatch, caplog):
        fake_conn = FakeConnection()
        _patch_connect(monkeypatch, fake_conn)

        fetch = _index_only_fetcher(
            pdf_responses={
                LINK_A: RuntimeError("connection reset"),
                LINK_B: FetchedFile(content=b"%PDF-1.4 ok", sha256="b-sha-2", source_url=LINK_B),
            }
        )

        with caplog.at_level("WARNING"):
            result = availability.archive("postgresql://fake", season=2025, fetch=fetch)

        assert result["new"] == 1
        assert result["gaps"] == ["SEC", "B12", "CFP"]
        assert len(fake_conn.inserted) == 1
        assert fake_conn.inserted[0][2] == LINK_B
        assert any("B1G PDF fetch failed" in message for message in caplog.messages)

    def test_default_fetch_is_file_fetcher_fetch_file(self, monkeypatch):
        fake_conn = FakeConnection()
        _patch_connect(monkeypatch, fake_conn)

        calls = []

        def fake_fetch_file(url):
            calls.append(url)
            raise RuntimeError("no network in tests")

        monkeypatch.setattr(availability, "fetch_file", fake_fetch_file)

        result = availability.archive("postgresql://fake", season=2025, fetch=None)

        assert calls  # fetch_file was used when fetch=None
        assert "B1G" in result["gaps"]

    def test_db_url_none_resolves_via_load_ledger(self, monkeypatch):
        fake_conn = FakeConnection()
        _patch_connect(monkeypatch, fake_conn)
        monkeypatch.setattr(availability.load_ledger, "get_db_url", lambda: "postgresql://resolved")

        def always_fails(url):
            raise RuntimeError("boom")

        result = availability.archive(None, season=2025, fetch=always_fails)

        assert "B1G" in result["gaps"]


class TestArchiveGapsArePermanent:
    def test_gaps_always_include_sec_b12_cfp_even_on_success(self, monkeypatch):
        fake_conn = FakeConnection()
        _patch_connect(monkeypatch, fake_conn)

        fetch = _index_only_fetcher(
            pdf_responses={
                LINK_A: FetchedFile(content=b"%PDF-1.4 a", sha256="a-sha", source_url=LINK_A),
                LINK_B: FetchedFile(content=b"%PDF-1.4 b", sha256="b-sha", source_url=LINK_B),
            }
        )

        result = availability.archive("postgresql://fake", season=2025, fetch=fetch)

        assert set(availability.PERMANENT_GAPS).issubset(set(result["gaps"]))
        assert "B1G" not in result["gaps"]
        assert result["new"] == 2
