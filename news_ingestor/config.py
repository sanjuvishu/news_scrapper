from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


@dataclass(frozen=True)
class AppConfig:
    mongo_uri: str
    db_name: str
    master_collection: str
    poll_interval_seconds: int
    request_timeout_seconds: int
    run_once: bool
    max_article_age_days: int
    feed_urls: List[str]
    sector_keywords: Dict[str, List[str]]

    @property
    def all_sectors(self) -> List[str]:
        return list(self.sector_keywords.keys()) + ["others"]


def _read_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got '{raw}'") from exc


def load_config(project_root: Path) -> AppConfig:
    config_dir = Path(os.getenv("RSS_CONFIG_DIR", str(project_root / "config")))
    feeds_path = Path(os.getenv("RSS_FEEDS_FILE", str(config_dir / "feeds.json")))
    sectors_path = Path(os.getenv("RSS_SECTORS_FILE", str(config_dir / "sector_keywords.json")))

    feed_urls = _read_json(feeds_path)
    sector_keywords = _read_json(sectors_path)

    if not isinstance(feed_urls, list) or not all(isinstance(u, str) and u.strip() for u in feed_urls):
        raise ValueError(f"Invalid feed list in '{feeds_path}'")
    if not isinstance(sector_keywords, dict):
        raise ValueError(f"Invalid sector keyword mapping in '{sectors_path}'")

    normalized_keywords: Dict[str, List[str]] = {}
    for sector, keywords in sector_keywords.items():
        if not isinstance(sector, str):
            continue
        valid_keywords = [kw for kw in keywords if isinstance(kw, str) and kw.strip()] if isinstance(keywords, list) else []
        if valid_keywords:
            normalized_keywords[sector] = valid_keywords

    if not normalized_keywords:
        raise ValueError("No valid sector keyword mapping loaded")

    return AppConfig(
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017/"),
        db_name=os.getenv("MONGO_DB_NAME", "stock_market_rss"),
        master_collection=os.getenv("MASTER_COLLECTION", "rss_master"),
        poll_interval_seconds=_int_env("POLL_INTERVAL_SECONDS", 300),
        request_timeout_seconds=_int_env("REQUEST_TIMEOUT", 20),
        run_once=os.getenv("RUN_ONCE", "0") == "1",
        max_article_age_days=_int_env("MAX_ARTICLE_AGE_DAYS", 180),
        feed_urls=feed_urls,
        sector_keywords=normalized_keywords,
    )

