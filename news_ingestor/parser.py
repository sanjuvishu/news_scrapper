from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from .classifier import SectorClassifier
from .utils import (
    feed_title_from_url,
    make_unique_key,
    parse_datetime,
    safe_str,
    sanitize_for_bson,
    source_domain_from_url,
)

try:
    import feedparser  # type: ignore
except ImportError:  # pragma: no cover
    feedparser = None


class FeedParserService:
    def __init__(self, classifier: SectorClassifier, logger: logging.Logger) -> None:
        self._classifier = classifier
        self._logger = logger

    @property
    def feedparser_available(self) -> bool:
        return feedparser is not None

    def parse(self, feed_url: str, text_content: str) -> List[Dict[str, Any]]:
        docs = self._parse_with_feedparser(feed_url, text_content)
        if docs:
            self._logger.info("feedparser parsed %d entries for %s", len(docs), feed_url)
            return docs
        docs = self._parse_with_xml(feed_url, text_content)
        if docs:
            self._logger.info("xml fallback parsed %d entries for %s", len(docs), feed_url)
            return docs
        self._logger.warning("No entries parsed for %s", feed_url)
        return []

    def _build_doc(
        self,
        feed_url: str,
        feed_title: str,
        title: str,
        summary: str,
        link: str,
        guid: str,
        published_at: Optional[datetime],
        authors: List[str],
        tags: List[str],
        raw_entry: Dict[str, Any],
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        return {
            "unique_key": make_unique_key(feed_url, title, link, guid),
            "guid": guid,
            "title": title,
            "link": link,
            "summary": summary,
            "authors": authors,
            "tags": tags,
            "sectors": self._classifier.classify(title, summary),
            "feed_source": feed_url,
            "feed_title": feed_title,
            "source_domain": source_domain_from_url(feed_url),
            "published_at": published_at,
            "raw_entry": sanitize_for_bson(raw_entry),
            "created_at": now,
            "updated_at": now,
        }

    def _parse_with_feedparser(self, feed_url: str, text_content: str) -> List[Dict[str, Any]]:
        if feedparser is None:
            return []
        parsed = feedparser.parse(text_content.encode("utf-8", errors="replace"))
        entries = getattr(parsed, "entries", []) or []
        if not entries:
            return []

        feed_obj = getattr(parsed, "feed", None) or {}
        raw_feed_title = feed_obj.get("title") if hasattr(feed_obj, "get") else getattr(feed_obj, "title", "")
        feed_title = safe_str(raw_feed_title) or feed_title_from_url(feed_url)

        docs: List[Dict[str, Any]] = []
        for entry in entries:
            title = safe_str(entry.get("title"))
            link = safe_str(entry.get("link"))
            guid = safe_str(entry.get("id") or entry.get("guid")) or link or title
            summary = safe_str(
                entry.get("summary")
                or entry.get("description")
                or (entry.get("content", [{}])[0].get("value", "") if entry.get("content") else "")
            )
            if not title and not link:
                continue
            authors = [str(a.get("name")) for a in (entry.get("authors", []) or []) if isinstance(a, dict) and a.get("name")]
            if not authors and entry.get("author"):
                authors.append(str(entry["author"]))
            tags = [str(t.get("term")) for t in (entry.get("tags", []) or []) if isinstance(t, dict) and t.get("term")]
            published_at = (
                parse_datetime(entry.get("published_parsed"))
                or parse_datetime(entry.get("updated_parsed"))
                or parse_datetime(entry.get("created_parsed"))
                or parse_datetime(entry.get("published"))
                or parse_datetime(entry.get("updated"))
            )
            docs.append(
                self._build_doc(feed_url, feed_title, title, summary, link, guid, published_at, authors, tags, dict(entry))
            )
        return docs

    def _xml_text(self, elem: Optional[ET.Element]) -> str:
        if elem is None:
            return ""
        parts = []
        if elem.text:
            parts.append(elem.text.strip())
        for child in elem:
            if child.text:
                parts.append(child.text.strip())
        return " ".join(part for part in parts if part)

    def _first_child(self, parent: ET.Element, names: Iterable[str]) -> Optional[ET.Element]:
        lookup = set(names)
        for child in list(parent):
            if child.tag.split("}")[-1].lower() in lookup:
                return child
        return None

    def _parse_with_xml(self, feed_url: str, text_content: str) -> List[Dict[str, Any]]:
        docs: List[Dict[str, Any]] = []
        try:
            root = ET.fromstring(text_content.encode("utf-8", errors="replace"))
        except ET.ParseError:
            return docs

        feed_title = feed_title_from_url(feed_url)
        root_local = root.tag.split("}")[-1].lower()
        if root_local == "rss":
            channel = self._first_child(root, ["channel"])
            if channel is None:
                return docs
            title_node = self._first_child(channel, ["title"])
            if title_node is not None and self._xml_text(title_node):
                feed_title = self._xml_text(title_node)
            items = [item for item in list(channel) if item.tag.split("}")[-1].lower() == "item"]
        elif root_local in ("feed", "atom"):
            title_node = self._first_child(root, ["title"])
            if title_node is not None and self._xml_text(title_node):
                feed_title = self._xml_text(title_node)
            items = [item for item in list(root) if item.tag.split("}")[-1].lower() == "entry"]
        else:
            return docs

        for item in items:
            title = self._xml_text(self._first_child(item, ["title"]))
            summary = self._xml_text(self._first_child(item, ["description", "summary", "content", "encoded"]))
            link = ""
            for child in list(item):
                if child.tag.split("}")[-1].lower() == "link":
                    link = child.attrib.get("href", "") or self._xml_text(child)
                    if link:
                        break
            guid = self._xml_text(self._first_child(item, ["guid", "id"])) or link or title
            published_at = parse_datetime(self._xml_text(self._first_child(item, ["pubdate", "published", "updated", "date"])))
            if not title and not link:
                continue
            docs.append(self._build_doc(feed_url, feed_title, title, summary, link, guid, published_at, [], [], {}))
        return docs

