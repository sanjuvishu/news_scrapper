from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .classifier import SectorClassifier
from .config import load_config
from .fetcher import FeedFetcher
from .parser import FeedParserService


def _build_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger("rss_mongo")


def _build_service(logger: logging.Logger):
    from .repository import MongoRepository
    from .service import RSSIngestionService

    project_root = Path(__file__).resolve().parent.parent
    config = load_config(project_root=project_root)
    classifier = SectorClassifier(config.sector_keywords)
    fetcher = FeedFetcher(config.request_timeout_seconds, logger)
    parser = FeedParserService(classifier, logger)
    repository = MongoRepository(config, logger)
    return RSSIngestionService(config, fetcher, parser, repository, logger)


def main() -> int:
    parser = argparse.ArgumentParser(description="RSS to MongoDB ingestor")
    parser.add_argument("--test-feeds", action="store_true", help="Test feed connectivity and parse count.")
    parser.add_argument("--verify-db", action="store_true", help="Verify MongoDB write access and exit.")
    args = parser.parse_args()

    logger = _build_logger()

    if args.test_feeds:
        project_root = Path(__file__).resolve().parent.parent
        config = load_config(project_root=project_root)
        fetcher = FeedFetcher(config.request_timeout_seconds, logger)
        parser_service = FeedParserService(SectorClassifier(config.sector_keywords), logger)
        for feed_url in config.feed_urls:
            result = fetcher.fetch(feed_url)
            if not result.text:
                print(f"FAIL status={result.status_code} items=0 url={feed_url}")
                continue
            item_count = len(parser_service.parse(feed_url, result.text))
            print(f"OK status={result.status_code} items={item_count} url={feed_url}")
        return 0

    service = _build_service(logger)
    if args.verify_db:
        service.verify_db()
        print("MongoDB write verification passed.")
        return 0

    try:
        service.run_forever()
        return 0
    except KeyboardInterrupt:
        logger.info("Stopped by user")
        return 0

