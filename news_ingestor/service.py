from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from .config import AppConfig
from .fetcher import FeedFetcher
from .parser import FeedParserService
from .repository import MongoRepository


class RSSIngestionService:
    def __init__(
        self,
        config: AppConfig,
        fetcher: FeedFetcher,
        parser: FeedParserService,
        repository: MongoRepository,
        logger: logging.Logger,
    ) -> None:
        self._config = config
        self._fetcher = fetcher
        self._parser = parser
        self._repository = repository
        self._logger = logger

    def _filter_recent_documents(self, documents: List[Dict]) -> List[Dict]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._config.max_article_age_days)
        out: List[Dict] = []
        for doc in documents:
            published_at = doc.get("published_at")
            if isinstance(published_at, datetime):
                if published_at >= cutoff:
                    out.append(doc)
            else:
                out.append(doc)
        return out

    def process_all_feeds(self) -> None:
        all_docs: List[Dict] = []
        feed_stats: List[Tuple[str, int, str]] = []
        for feed_url in self._config.feed_urls:
            fetch_result = self._fetcher.fetch(feed_url)
            if not fetch_result.text:
                feed_stats.append((feed_url, 0, "EMPTY"))
                continue
            docs = self._parser.parse(feed_url, fetch_result.text)
            all_docs.extend(docs)
            feed_stats.append((feed_url, len(docs), "OK" if docs else "EMPTY"))

        total_parsed = sum(count for _, count, _ in feed_stats)
        ok_feeds = sum(1 for _, count, _ in feed_stats if count > 0)
        self._logger.info("Parse summary: %d/%d feeds OK, total articles=%d", ok_feeds, len(feed_stats), total_parsed)
        if not all_docs:
            self._logger.warning("No articles fetched from configured feeds.")
            return

        recent_docs = self._filter_recent_documents(all_docs)
        if not recent_docs:
            self._logger.warning("No articles remain after age filter.")
            return

        counts = self._repository.upsert_all(recent_docs)
        master_new = counts.pop(self._config.master_collection, 0)
        sector_new_total = sum(counts.values())
        self._logger.info(
            "Run complete parsed=%d master_new=%d sector_new_total=%d",
            len(recent_docs),
            master_new,
            sector_new_total,
        )

    def test_feeds(self) -> List[Tuple[str, int, int, bool]]:
        results: List[Tuple[str, int, int, bool]] = []
        for feed_url in self._config.feed_urls:
            fetch_result = self._fetcher.fetch(feed_url)
            if not fetch_result.text:
                results.append((feed_url, fetch_result.status_code, 0, False))
                continue
            docs = self._parser.parse(feed_url, fetch_result.text)
            results.append((feed_url, fetch_result.status_code, len(docs), True))
        return results

    def verify_db(self) -> None:
        self._repository.verify_write()

    def run_forever(self) -> None:
        self._repository.verify_write()
        self._repository.ensure_indexes()
        if not self._parser.feedparser_available:
            self._logger.warning("feedparser is not installed, fallback XML parser only.")

        while True:
            started = time.time()
            self.process_all_feeds()
            if self._config.run_once:
                return
            elapsed = int(time.time() - started)
            sleep_seconds = max(5, self._config.poll_interval_seconds - elapsed)
            self._logger.info("Next run in %d seconds", sleep_seconds)
            time.sleep(sleep_seconds)

