from __future__ import annotations

import hashlib
import html
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Optional
from urllib.parse import urlparse


def normalize_text(text: str) -> str:
    cleaned = html.unescape(text or "")
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip().lower()


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    out = str(value).strip()
    return "" if out == "None" else out


def source_domain_from_url(url: str) -> str:
    return re.sub(r"^www\.", "", urlparse(url).netloc or "").lower()


def feed_title_from_url(url: str) -> str:
    return urlparse(url).netloc or url


def make_unique_key(feed_url: str, title: str, link: str, guid: str) -> str:
    raw = f"{feed_url}|{guid}|{link}|{title}".strip().lower()
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if hasattr(value, "tm_year") and hasattr(value, "tm_mon"):
        try:
            return datetime(
                value.tm_year,
                value.tm_mon,
                value.tm_mday,
                getattr(value, "tm_hour", 0),
                getattr(value, "tm_min", 0),
                getattr(value, "tm_sec", 0),
                tzinfo=timezone.utc,
            )
        except Exception:
            return None
    if isinstance(value, (tuple, list)) and len(value) >= 6:
        try:
            return datetime(*list(value)[:6], tzinfo=timezone.utc)
        except Exception:
            return None
    if isinstance(value, str) and value.strip():
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value.strip(), fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        try:
            dt = parsedate_to_datetime(value)
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def sanitize_for_bson(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str, datetime)):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if hasattr(value, "tm_year") and hasattr(value, "tm_mon"):
        parsed = parse_datetime(value)
        return parsed if parsed else str(value)
    if isinstance(value, (list, tuple, set)):
        return [sanitize_for_bson(v) for v in value]
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = str(key).replace(".", "_")
            if normalized_key.startswith("$"):
                normalized_key = f"field_{normalized_key[1:]}"
            out[normalized_key] = sanitize_for_bson(item)
        return out
    return str(value)

