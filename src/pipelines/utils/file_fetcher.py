"""Anonymous file fetcher for flat-file sources (T3).

httpx GET with the retry posture of api_client.CFBDClient (429 Retry-After,
5xx/RequestError backoff, MAX_RETRIES=3) minus auth and minus the CFBD rate
limiter (public flat files have no call budget). Also accepts local filesystem
paths so ``--file`` overrides ride the same code path.
"""

import hashlib
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 60.0
MAX_RETRIES = 3
RETRY_DELAY = 1.0
USER_AGENT = "cfb-database/0.1 (+https://github.com/rstover-fo/cfb-database)"


@dataclass(frozen=True)
class FetchedFile:
    """A fetched file: raw bytes + provenance.

    Attributes:
        content: Raw file bytes.
        sha256: Hex digest of content (ledger hash-skip key).
        source_url: URL fetched, or absolute local path for file inputs.
    """

    content: bytes
    sha256: str
    source_url: str


def _get_with_retries(
    url: str,
    *,
    timeout: float,
    retries: int,
    headers: dict[str, str] | None,
) -> bytes:
    """GET a URL, retrying on 429/5xx/connection errors. Raises on terminal failure."""
    request_headers = {"User-Agent": USER_AGENT}
    if headers:
        request_headers.update(headers)

    with httpx.Client(follow_redirects=True, timeout=timeout) as client:
        for attempt in range(retries + 1):
            try:
                response = client.get(url, headers=request_headers)
                response.raise_for_status()
                return response.content
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    retry_after = int(e.response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited fetching {url}. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                elif e.response.status_code >= 500 and attempt < retries:
                    logger.warning(
                        f"Server error {e.response.status_code} fetching {url}. "
                        f"Retry {attempt + 1}/{retries}"
                    )
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                raise
            except httpx.RequestError as e:
                if attempt < retries:
                    logger.warning(
                        f"Request failed fetching {url}: {e}. Retry {attempt + 1}/{retries}"
                    )
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                raise

    raise RuntimeError(f"Exhausted retries fetching {url}")  # pragma: no cover - unreachable


def fetch_file(
    url_or_path: str,
    *,
    timeout: float = DEFAULT_TIMEOUT,
    retries: int = MAX_RETRIES,
    headers: dict[str, str] | None = None,
) -> FetchedFile:
    """Fetch a URL (http/https) or read a local path into a FetchedFile.

    Follows redirects. Raises httpx.HTTPStatusError on terminal HTTP errors,
    FileNotFoundError for missing local paths. Implemented in T3.
    """
    if url_or_path.startswith("http://") or url_or_path.startswith("https://"):
        content = _get_with_retries(url_or_path, timeout=timeout, retries=retries, headers=headers)
        source_url = url_or_path
    else:
        path = Path(url_or_path)
        if not path.is_file():
            raise FileNotFoundError(f"No such file: {url_or_path}")
        content = path.read_bytes()
        source_url = str(path.resolve())

    sha256 = hashlib.sha256(content).hexdigest()
    return FetchedFile(content=content, sha256=sha256, source_url=source_url)
