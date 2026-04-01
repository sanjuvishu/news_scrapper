from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass(frozen=True)
class FetchResult:
    text: Optional[str]
    status_code: int
    content_type: str


class FeedFetcher:
    def __init__(self, timeout_seconds: int, logger: logging.Logger) -> None:
        self._timeout_seconds = timeout_seconds
        self._logger = logger
        self._session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            read=3,
            connect=3,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            }
        )
        return session

    def fetch(self, feed_url: str) -> FetchResult:
        try:
            response = self._session.get(feed_url, timeout=self._timeout_seconds)
            ctype = response.headers.get("Content-Type", "")
            self._logger.info("Fetched %s HTTP=%s bytes=%s", feed_url, response.status_code, len(response.content))
            if response.status_code in (401, 403, 407, 429):
                self._logger.warning("Feed blocked status=%s url=%s", response.status_code, feed_url)
                return FetchResult(None, response.status_code, ctype)
            response.raise_for_status()

            snippet = response.text[:500].lower()
            if "<html" in snippet and "<?xml" not in snippet and "<rss" not in snippet:
                self._logger.warning("Feed returned HTML instead of XML: %s", feed_url)
                return FetchResult(None, response.status_code, ctype)

            return FetchResult(response.text, response.status_code, ctype)
        except requests.exceptions.ConnectionError as exc:
            self._logger.error("Network error for %s: %s", feed_url, exc)
            return FetchResult(None, 0, "")
        except requests.exceptions.Timeout:
            self._logger.error("Timeout after %ss for %s", self._timeout_seconds, feed_url)
            return FetchResult(None, 0, "")
        except Exception as exc:
            self._logger.error("Fetch failed for %s: %s", feed_url, exc)
            return FetchResult(None, 0, "")

