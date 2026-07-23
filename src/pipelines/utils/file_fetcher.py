"""Anonymous file fetcher for flat-file sources (T3).

httpx GET with the retry posture of api_client.CFBDClient (429 Retry-After,
5xx/RequestError backoff, MAX_RETRIES=3) minus auth and minus the CFBD rate
limiter (public flat files have no call budget). Also accepts local filesystem
paths so ``--file`` overrides ride the same code path.
"""

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 60.0
MAX_RETRIES = 3
RETRY_DELAY = 1.0


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
    raise NotImplementedError("T3 implements fetch_file")
