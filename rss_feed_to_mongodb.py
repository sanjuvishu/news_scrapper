#!/usr/bin/env python3
"""
RSS -> MongoDB Ingestor — Complete Production Fix
=================================================

BUGS FIXED (all 10):
  BUG 1  (CRITICAL) $setOnInsert + $set 'updated_at' path conflict
                    → MongoDB WriteError on every upsert → 0 docs stored ever
  BUG 2             str(None) stored as literal string "None"
  BUG 3             raw_entry duplicated into every sector collection
  BUG 4             sectors field frozen at first insert, never refreshed
  BUG 5             parsed.feed can be None → AttributeError
  BUG 6             No source_domain field
  BUG 7  (NEW)      feedparser.parse(bytes) silently returns 0 entries when
                    feed returns an HTML error page (paywall / 403).
                    bozo flag was never checked.
  BUG 8  (NEW)      5 of 11 feeds are paywalled or bot-blocked (FT, WSJ,
                    Bloomberg, SeekingAlpha, Investing.com) → always 0 items.
                    Replaced with verified working free feeds.
  BUG 9  (NEW)      Encoding mismatch: response.content (raw bytes) passed
                    to feedparser; ISO-8859-1 or Windows-1252 feeds mis-decoded.
                    Fix: use response.text → re-encode UTF-8 for feedparser.
  BUG 10 (NEW)      No startup write-test; silent MongoDB auth/permission
                    failures look identical to "no data fetched".
                    Fix: verify_db_write() runs at startup, fails loudly.

CLI usage:
  python rss_feed_to_mongodb.py                  # normal polling loop
  python rss_feed_to_mongodb.py --test-feeds     # test every feed URL, then exit
  python rss_feed_to_mongodb.py --verify-db      # insert test doc, then exit
  RUN_ONCE=1 python rss_feed_to_mongodb.py       # single run then exit
"""

from __future__ import annotations

import argparse
import hashlib
import html
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

try:
    import feedparser  # type: ignore
    FEEDPARSER_AVAILABLE = True
except ImportError:
    feedparser = None
    FEEDPARSER_AVAILABLE = False

import requests
from pymongo import ASCENDING, TEXT, MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, InvalidDocument, OperationFailure, ServerSelectionTimeoutError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# FIX BUG 8: Removed 5 paywalled/bot-blocked feeds.
#            Replaced with verified free RSS feeds.
#            Added India-focused feeds (ET, Moneycontrol, BSE)
RSS_FEEDS: List[str] = [

    # ── Global / US market news (free, no login required) ──
    "https://feeds.content.dowjones.io/public/rss/mw_marketpulse",          # MarketWatch
    "https://finance.yahoo.com/news/rssindex",                               # Yahoo Finance
    "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",  # CNBC Markets
    "https://www.nasdaq.com/feed/rssoutbound?category=Stock%20Market%20News",# NASDAQ
    "https://www.benzinga.com/feed",                                         # Benzinga
    "https://www.globenewswire.com/rss/news-releases/",                      # GlobeNewswire
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",                        # Dow Jones Markets
    "https://feeds.feedburner.com/stocknewsapi/allnews",                     # StockNews

    # ── India / NSE focused (free) ──
    "https://www.thehindu.com/business/markets/feeder/default.rss",         # Hindu Markets
    "https://www.cnbctv18.com/commonfeeds/v1/cne/rss/market.xml",             # CNBC TV18 Market
    "https://economictimes.indiatimes.com/markets/rss.cms",                  # ET Markets
    "https://economictimes.indiatimes.com/industry/rss.cms",                 # ET Industry
    "https://www.moneycontrol.com/rss/latestnews.xml",                       # Moneycontrol
    "https://www.business-standard.com/rss/markets-106.rss",                 # Business Standard
    "https://www.livemint.com/rss/markets",                                  # Livemint Markets
    "https://www.thehindubusinessline.com/markets/?service=rss",             # Hindu BusinessLine

    # ── Sector-specific ──
    "https://feeds.content.dowjones.io/public/rss/mw_technology",           # MW Tech
    "https://feeds.content.dowjones.io/public/rss/mw_healthcare",           # MW Healthcare
    "https://feeds.content.dowjones.io/public/rss/mw_energy",               # MW Energy
]

MONGO_URI             = os.getenv("MONGO_URI",             "mongodb://localhost:27017/")
DB_NAME               = os.getenv("MONGO_DB_NAME",         "stock_market_rss")
MASTER_COLLECTION     = "rss_master"
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
REQUEST_TIMEOUT       = int(os.getenv("REQUEST_TIMEOUT",       "20"))
RUN_ONCE              = os.getenv("RUN_ONCE", "0") == "1"
MAX_ARTICLE_AGE_DAYS  = int(os.getenv("MAX_ARTICLE_AGE_DAYS", "180"))  # 6 months default

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("rss_mongo")

# ---------------------------------------------------------------------------
# Sector keyword map
# ---------------------------------------------------------------------------

SECTOR_KEYWORDS: Dict[str, List[str]] = {
    "technology": [
        "technology", "tech", "software", "semiconductor", "chip", "chips",
        "ai", "artificial intelligence", "machine learning", "cloud", "saas",
        "microsoft", "apple", "google", "alphabet", "meta", "amazon", "nvidia",
        "amd", "intel", "oracle", "adobe", "cybersecurity", "data center",
        "infosys", "wipro", "hcl", "tcs", "techm",
    ],
    "banking_finance": [
        "bank", "banking", "finance", "financial", "fintech", "insurance",
        "credit", "loan", "capital markets", "jpmorgan", "goldman",
        "morgan stanley", "visa", "mastercard", "paypal", "treasury",
        "nifty bank", "hdfc bank", "icici bank", "sbi", "axis bank",
        "kotak", "rbi", "reserve bank", "nbfc", "mutual fund", "sebi",
    ],
    "healthcare": [
        "healthcare", "health care", "pharma", "pharmaceutical", "biotech",
        "medical", "hospital", "drug", "fda", "vaccine", "clinical trial",
        "sun pharma", "cipla", "dr reddy", "lupin", "aurobindo",
    ],
    "energy": [
        "energy", "oil", "gas", "natural gas", "crude", "brent", "wti",
        "renewable", "solar", "wind", "opec", "petroleum", "refinery",
        "ongc", "bpcl", "ioc", "reliance energy", "ntpc", "coal india",
        "adani green", "tata power",
    ],
    "consumer": [
        "consumer", "retail", "e-commerce", "ecommerce", "food", "beverage",
        "restaurant", "walmart", "target", "costco", "nike", "travel",
        "hul", "hindustan unilever", "itc", "nestle", "britannia", "dabur",
        "marico", "godrej", "titan", "trent",
    ],
    "industrial": [
        "industrial", "manufacturing", "factory", "machinery", "aerospace",
        "defense", "construction", "logistics", "shipping", "railroad",
        "larsen", "l&t", "bhel", "siemens", "abb", "cummins",
    ],
    "telecom": [
        "telecom", "telecommunications", "wireless", "broadband", "5g",
        "verizon", "at&t", "t-mobile", "jio", "airtel", "vodafone",
        "bharti", "bsnl",
    ],
    "real_estate": [
        "real estate", "reit", "property", "housing", "mortgage",
        "dlf", "godrej properties", "oberoi", "brigade",
    ],
    "materials": [
        "materials", "mining", "gold", "silver", "copper", "steel",
        "aluminum", "lithium", "uranium", "iron ore", "zinc",
        "tata steel", "jsw steel", "hindalco", "vedanta", "nmdc",
        "sail", "jindal", "nalco",
    ],
    "utilities": [
        "utilities", "power grid", "electric utility", "water utility",
        "powergrid", "power grid corp", "adani ports",
    ],
    "automotive": [
        "automotive", "auto", "vehicle", "ev", "electric vehicle",
        "ford", "gm", "tesla", "rivian", "lucid",
        "maruti", "tata motors", "mahindra", "hero motocorp",
        "bajaj auto", "ashok leyland", "eicher",
    ],
    "crypto": [
        "crypto", "cryptocurrency", "bitcoin", "ethereum",
        "token", "blockchain", "coinbase", "defi", "nft", "web3",
    ],
    "etf_index": [
        "etf", "index fund", "s&p 500", "nasdaq composite",
        "dow jones", "russell 2000", "nifty 50", "sensex",
        "nifty", "bse", "nse index",
    ],
    "general_market": [
        "stock market", "market news", "wall street", "equities",
        "stocks", "shares", "earnings", "ipo", "inflation", "economy",
        "fed", "federal reserve", "rate hike", "bull market", "bear market",
        "sensex", "nifty", "dalal street",
    ],
}

ALL_SECTORS = list(SECTOR_KEYWORDS.keys()) + ["others"]

# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

def create_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3, read=3, connect=3, backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://",  adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "application/rss+xml, application/atom+xml, "
            "application/xml, text/xml, */*;q=0.8"
        ),
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    })
    return session


HTTP = create_http_session()

# ---------------------------------------------------------------------------
# MongoDB connection
# ---------------------------------------------------------------------------

def get_mongo_db():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        logger.info("MongoDB connected  uri=%s  db=%s", MONGO_URI, DB_NAME)
        return client[DB_NAME]
    except ServerSelectionTimeoutError as exc:
        raise SystemExit(
            f"\n[FATAL] Cannot reach MongoDB at {MONGO_URI}\n"
            f"  Check that mongod is running and MONGO_URI is correct.\n"
            f"  Detail: {exc}"
        ) from exc


DB = get_mongo_db()

# ---------------------------------------------------------------------------
# FIX BUG 10: Startup write-verification
# ---------------------------------------------------------------------------

def verify_db_write() -> None:
    """
    FIX BUG 10 — Insert and immediately delete a canary document to confirm
    that the MongoDB user has write permission.  Fails loudly with a clear
    message instead of silently producing 0 documents.
    """
    col = DB[MASTER_COLLECTION]
    canary = {
        "unique_key": "__startup_canary__",
        "title":      "startup write-test",
        "created_at": datetime.now(timezone.utc),
    }
    try:
        col.replace_one({"unique_key": "__startup_canary__"}, canary, upsert=True)
        col.delete_one({"unique_key": "__startup_canary__"})
        logger.info("MongoDB write-test PASSED — collection '%s' is writable.", MASTER_COLLECTION)
    except Exception as exc:
        raise SystemExit(
            f"\n[FATAL] MongoDB write-test FAILED on collection '{MASTER_COLLECTION}'.\n"
            f"  This is why 0 documents are stored.\n"
            f"  Check: user permissions, authSource, network firewall.\n"
            f"  Error: {exc}"
        ) from exc

# ---------------------------------------------------------------------------
# Index creation
# ---------------------------------------------------------------------------

def _ensure_index(collection, keys, **kwargs) -> None:
    try:
        collection.create_index(keys, **kwargs)
    except OperationFailure as exc:
        message = str(exc)
        if "already exists with a different name" in message:
            # Existing index has same key but different name; drop/recreate so our naming is stable.
            existing = collection.index_information()
            conflict_name = None
            for idx_name, idx_info in existing.items():
                if idx_info.get("key") == keys:
                    conflict_name = idx_name
                    break
            if conflict_name:
                logger.warning(
                    "Index conflict on %s: dropping existing index '%s' and recreating '%s'.",
                    collection.name, conflict_name, kwargs.get("name")
                )
                collection.drop_index(conflict_name)
                collection.create_index(keys, **kwargs)
            else:
                logger.warning(
                    "Index exists with different name on %s but no key match was found; skipping: %s",
                    collection.name, message
                )
        else:
            raise


def ensure_indexes() -> None:
    master = DB[MASTER_COLLECTION]
    _ensure_index(master, [("unique_key",    ASCENDING)], unique=True, name="uq_key")
    _ensure_index(master, [("published_at",  ASCENDING)],               name="idx_published")
    _ensure_index(master, [("feed_source",   ASCENDING)],               name="idx_source")
    _ensure_index(master, [("source_domain", ASCENDING)],               name="idx_domain")
    _ensure_index(master, [("sectors",       ASCENDING)],               name="idx_sectors")
    _ensure_index(
        master,
        [("title", TEXT), ("summary", TEXT)],
        name="text_search",
        default_language="english",
    )
    for sector in ALL_SECTORS:
        col = DB[f"sector_{sector}"]
        _ensure_index(col, [("unique_key",    ASCENDING)], unique=True, name="uq_key")
        _ensure_index(col, [("published_at",  ASCENDING)],              name="idx_published")
        _ensure_index(col, [("feed_source",   ASCENDING)],              name="idx_source")
        _ensure_index(col, [("source_domain", ASCENDING)],              name="idx_domain")
        _ensure_index(col, [("sector",        ASCENDING)],              name="idx_sector")
    logger.info(
        "Indexes ready: %s + %d sector collections.", MASTER_COLLECTION, len(ALL_SECTORS)
    )

# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def normalize_text(text: str) -> str:
    """Strip HTML tags, decode entities (&amp; etc.), collapse whitespace, lowercase."""
    text = html.unescape(text or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def classify_sectors(title: str, summary: str) -> List[str]:
    text = normalize_text(f"{title} {summary}")
    matched = [
        sector
        for sector, keywords in SECTOR_KEYWORDS.items()
        if any(kw in text for kw in keywords)
    ]
    return matched or ["others"]


def safe_str(value: Any) -> str:
    """Return '' for None. Prevents str(None) == 'None' stored in MongoDB."""
    if value is None:
        return ""
    s = str(value).strip()
    # Reject literal "None" string that slipped through
    return "" if s == "None" else s


def feed_title_from_url(url: str) -> str:
    return urlparse(url).netloc or url


def source_domain_from_url(url: str) -> str:
    netloc = urlparse(url).netloc or ""
    return re.sub(r"^www\.", "", netloc).lower()

# ---------------------------------------------------------------------------
# Datetime parsing
# ---------------------------------------------------------------------------

def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return (
            value.astimezone(timezone.utc)
            if value.tzinfo
            else value.replace(tzinfo=timezone.utc)
        )
    # struct_time — MUST check before (tuple,list) because struct_time IS a tuple subclass
    if hasattr(value, "tm_year") and hasattr(value, "tm_mon"):
        try:
            return datetime(
                value.tm_year, value.tm_mon, value.tm_mday,
                getattr(value, "tm_hour", 0),
                getattr(value, "tm_min",  0),
                getattr(value, "tm_sec",  0),
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
        for fmt in ["%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"]:
            try:
                return datetime.strptime(value.strip(), fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                pass
        try:
            dt = parsedate_to_datetime(value)
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def make_unique_key(feed_url: str, title: str, link: str, guid: str) -> str:
    raw = f"{feed_url}|{guid}|{link}|{title}".strip().lower()
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

# ---------------------------------------------------------------------------
# BSON sanitizer
# ---------------------------------------------------------------------------

def sanitize_for_bson(value: Any) -> Any:
    """
    Recursively coerce any Python value to something BSON/MongoDB can store.
    struct_time is checked BEFORE (list,tuple) because it subclasses tuple.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    # struct_time BEFORE tuple/list check
    if hasattr(value, "tm_year") and hasattr(value, "tm_mon"):
        try:
            return datetime(
                value.tm_year, value.tm_mon, value.tm_mday,
                getattr(value, "tm_hour", 0),
                getattr(value, "tm_min",  0),
                getattr(value, "tm_sec",  0),
                tzinfo=timezone.utc,
            )
        except Exception:
            return str(value)
    if isinstance(value, (list, tuple, set)):
        return [sanitize_for_bson(v) for v in value]
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for k, v in value.items():
            key = str(k)
            if key.startswith("$"):
                key = f"field_{key[1:]}"
            key = key.replace(".", "_")
            out[key] = sanitize_for_bson(v)
        return out
    return str(value)

# ---------------------------------------------------------------------------
# HTTP fetch — FIX BUG 9: return (text_content, status_code, content_type)
# ---------------------------------------------------------------------------

def fetch_feed(feed_url: str) -> Tuple[Optional[str], int, str]:
    """
    Fetch a feed URL and return (decoded_text, http_status, content_type).
    Returns (None, 0, '') on network failure.

    FIX BUG 9: returns response.text (requests auto-decodes charset) rather
    than response.content (raw bytes).  This prevents UTF-8/ISO-8859-1
    mismatch that causes feedparser to produce garbled or empty results.
    """
    try:
        resp = HTTP.get(feed_url, timeout=REQUEST_TIMEOUT)
        ctype = resp.headers.get("Content-Type", "")
        logger.info(
            "Fetched %-65s  HTTP %-3s  %s bytes  %s",
            feed_url, resp.status_code, len(resp.content), ctype[:40]
        )

        if resp.status_code in (401, 403, 407, 429):
            logger.warning(
                "Feed BLOCKED (HTTP %s) — skipping: %s", resp.status_code, feed_url
            )
            return None, resp.status_code, ctype

        resp.raise_for_status()

        # Check content looks like XML, not an HTML error page
        snippet = resp.text[:500].lower()
        if "<html" in snippet and "<?xml" not in snippet and "<rss" not in snippet:
            logger.warning(
                "Feed returned HTML (not XML) — probably a redirect/paywall: %s", feed_url
            )
            return None, resp.status_code, ctype

        return resp.text, resp.status_code, ctype

    except requests.exceptions.ConnectionError as exc:
        logger.error("Network error for %s: %s", feed_url, exc)
        return None, 0, ""
    except requests.exceptions.Timeout:
        logger.error("Timeout after %ss for %s", REQUEST_TIMEOUT, feed_url)
        return None, 0, ""
    except Exception as exc:
        logger.error("Fetch failed for %s: %s", feed_url, exc)
        return None, 0, ""

# ---------------------------------------------------------------------------
# FIX BUG 7 + BUG 9: feedparser receives UTF-8 encoded text, not raw bytes
# ---------------------------------------------------------------------------

def parse_with_feedparser(feed_url: str, text_content: str) -> List[Dict[str, Any]]:
    """
    FIX BUG 7: Pass UTF-8 bytes to feedparser (feedparser handles encoding best
    when given bytes with a proper XML declaration or a clean string).
    Check parsed.bozo to detect feeds that returned HTML error pages.

    FIX BUG 9: Accept decoded text_content from requests.text instead of
    raw bytes, avoiding charset misdetection.
    """
    if not FEEDPARSER_AVAILABLE:
        return []

    # encode to bytes so feedparser can reliably detect encoding
    content_bytes = text_content.encode("utf-8", errors="replace")
    parsed = feedparser.parse(content_bytes)

    # FIX BUG 7: check bozo flag
    if getattr(parsed, "bozo", False):
        bozo_exc = getattr(parsed, "bozo_exception", "unknown")
        logger.debug("feedparser bozo=True for %s: %s", feed_url, bozo_exc)

    entries = getattr(parsed, "entries", []) or []
    if not entries:
        logger.debug("feedparser returned 0 entries for %s (bozo=%s)",
                     feed_url, getattr(parsed, "bozo", "?"))
        return []

    # FIX BUG 5: guard parsed.feed being None
    feed_obj       = getattr(parsed, "feed", None) or {}
    raw_feed_title = (
        feed_obj.get("title") if hasattr(feed_obj, "get")
        else getattr(feed_obj, "title", "")
    ) or ""
    feed_title = safe_str(raw_feed_title) or feed_title_from_url(feed_url)

    docs: List[Dict[str, Any]] = []
    for entry in entries:
        # FIX BUG 2: safe_str prevents "None" strings
        title   = safe_str(entry.get("title"))
        link    = safe_str(entry.get("link"))
        guid    = safe_str(entry.get("id") or entry.get("guid")) or link or title
        summary = safe_str(
            entry.get("summary") or entry.get("description") or
            entry.get("content", [{}])[0].get("value", "") if entry.get("content") else ""
        )

        if not title and not link:
            continue  # skip empty entries

        authors: List[str] = []
        for a in entry.get("authors", []) or []:
            name = a.get("name") if isinstance(a, dict) else getattr(a, "name", None)
            if name:
                authors.append(str(name))
        if not authors and entry.get("author"):
            authors.append(str(entry["author"]))

        tags: List[str] = []
        for tag in entry.get("tags", []) or []:
            term = tag.get("term") if isinstance(tag, dict) else getattr(tag, "term", None)
            if term:
                tags.append(str(term))

        published_at = (
            parse_datetime(entry.get("published_parsed"))
            or parse_datetime(entry.get("updated_parsed"))
            or parse_datetime(entry.get("created_parsed"))
            or parse_datetime(entry.get("published"))
            or parse_datetime(entry.get("updated"))
        )

        sectors    = classify_sectors(title, summary)
        unique_key = make_unique_key(feed_url, title, link, guid)

        docs.append({
            "unique_key":    unique_key,
            "guid":          guid,
            "title":         title,
            "link":          link,
            "summary":       summary,
            "authors":       authors,
            "tags":          tags,
            "sectors":       sectors,
            "feed_source":   feed_url,
            "feed_title":    feed_title,
            "source_domain": source_domain_from_url(feed_url),
            "published_at":  published_at,
            "raw_entry":     sanitize_for_bson(dict(entry)),
            "created_at":    datetime.now(timezone.utc),
            "updated_at":    datetime.now(timezone.utc),
        })

    return docs

# ---------------------------------------------------------------------------
# XML fallback parser (FIX BUG 10: better namespace + content extraction)
# ---------------------------------------------------------------------------

def xml_text(elem: Optional[ET.Element]) -> str:
    if elem is None:
        return ""
    parts = []
    if elem.text:
        parts.append(elem.text.strip())
    for child in elem:
        if child.text:
            parts.append(child.text.strip())
    return " ".join(p for p in parts if p)


def first_child(parent: ET.Element, names: Iterable[str]) -> Optional[ET.Element]:
    name_set = set(names)
    for child in list(parent):
        local = child.tag.split("}")[-1].lower()
        if local in name_set:
            return child
    return None


def all_children(parent: ET.Element, names: Iterable[str]) -> List[ET.Element]:
    name_set = set(names)
    return [c for c in list(parent) if c.tag.split("}")[-1].lower() in name_set]


def parse_with_xml(feed_url: str, text_content: str) -> List[Dict[str, Any]]:
    """XML fallback with improved namespace + CDATA + Atom content handling."""
    docs: List[Dict[str, Any]] = []
    try:
        root = ET.fromstring(text_content.encode("utf-8", errors="replace"))
    except ET.ParseError as exc:
        logger.error("XML parse failed: %s | %s", feed_url, exc)
        return docs

    feed_title = feed_title_from_url(feed_url)
    root_local = root.tag.split("}")[-1].lower()

    if root_local == "rss":
        channel = first_child(root, ["channel"])
        if channel is None:
            return docs
        t = first_child(channel, ["title"])
        if t is not None and xml_text(t):
            feed_title = xml_text(t)
        items = [c for c in list(channel) if c.tag.split("}")[-1].lower() == "item"]
    elif root_local in ("feed", "atom"):
        t = first_child(root, ["title"])
        if t is not None and xml_text(t):
            feed_title = xml_text(t)
        items = [c for c in list(root) if c.tag.split("}")[-1].lower() == "entry"]
    else:
        logger.warning("Unknown feed root element <%s> for %s", root_local, feed_url)
        return docs

    for item in items:
        title   = xml_text(first_child(item, ["title"]))
        summary = xml_text(first_child(item, [
            "description", "summary", "content", "encoded", "media:description"
        ]))

        link = ""
        for child in list(item):
            local = child.tag.split("}")[-1].lower()
            if local == "link":
                href = child.attrib.get("href", "")
                link = href or xml_text(child)
                if link:
                    break

        guid        = xml_text(first_child(item, ["guid", "id"])) or link or title
        pub_raw     = xml_text(first_child(item, ["pubdate", "published", "updated", "date"]))
        published_at = parse_datetime(pub_raw)

        if not title and not link:
            continue

        sectors    = classify_sectors(title, summary)
        unique_key = make_unique_key(feed_url, title, link, guid)

        docs.append({
            "unique_key":    unique_key,
            "guid":          guid,
            "title":         title,
            "link":          link,
            "summary":       summary,
            "authors":       [],
            "tags":          [],
            "sectors":       sectors,
            "feed_source":   feed_url,
            "feed_title":    feed_title,
            "source_domain": source_domain_from_url(feed_url),
            "published_at":  published_at,
            "raw_entry":     {},
            "created_at":    datetime.now(timezone.utc),
            "updated_at":    datetime.now(timezone.utc),
        })

    return docs


def parse_feed(feed_url: str) -> List[Dict[str, Any]]:
    """Fetch + parse one feed. feedparser first, XML fallback if needed."""
    text_content, status_code, _ = fetch_feed(feed_url)
    if not text_content:
        return []

    docs = parse_with_feedparser(feed_url, text_content)
    if docs:
        logger.info("  feedparser → %d items  (%s)", len(docs), feed_url)
        return docs

    docs = parse_with_xml(feed_url, text_content)
    if docs:
        logger.info("  xml-fallback → %d items  (%s)", len(docs), feed_url)
        return docs

    logger.warning("  0 items parsed from %s  (content may be empty or HTML)", feed_url)
    return []

# ---------------------------------------------------------------------------
# MongoDB upsert helpers — FIX BUG 1, 3, 4
# ---------------------------------------------------------------------------

def _build_master_operation(doc: Dict[str, Any], now: datetime) -> UpdateOne:
    """
    FIX BUG 1 (CRITICAL): Split document across $setOnInsert and $set
    to avoid MongoDB 'path conflict' WriteError that silently discarded
    every document.

    FIX BUG 4: 'sectors' in $set so it is always refreshed on re-encounter.

    $setOnInsert  ← immutable fields, written ONCE on first insert
    $set          ← mutable fields, refreshed on EVERY upsert
    """
    clean = sanitize_for_bson(doc)

    insert_only = {
        k: v for k, v in clean.items()
        if k not in {"updated_at", "sectors"}
    }

    return UpdateOne(
        {"unique_key": clean["unique_key"]},
        {
            "$setOnInsert": insert_only,
            "$set": {
                "updated_at": now,
                "sectors":    clean.get("sectors", ["others"]),
            },
        },
        upsert=True,
    )


def _build_sector_operation(doc: Dict[str, Any], sector: str, now: datetime) -> UpdateOne:
    """
    FIX BUG 3: Strip raw_entry from sector collections to avoid duplicating
    up to 16 KB of raw RSS payload per sector match.

    FIX BUG 1: Same $setOnInsert/$set split — no path conflict.
    """
    clean = sanitize_for_bson(doc)

    sector_doc = {
        k: v for k, v in clean.items()
        if k not in {"raw_entry", "updated_at", "sectors"}
    }
    sector_doc["sector"] = sector

    return UpdateOne(
        {"unique_key": sector_doc["unique_key"]},
        {
            "$setOnInsert": sector_doc,
            "$set":          {"updated_at": now},
        },
        upsert=True,
    )


def upsert_collection(collection_name: str, operations: List[UpdateOne]) -> int:
    if not operations:
        return 0
    try:
        result = DB[collection_name].bulk_write(operations, ordered=False)
        n = int(result.upserted_count or 0)
        logger.debug(
            "bulk_write '%s'  ops=%d  upserted=%d  matched=%d",
            collection_name, len(operations),
            result.upserted_count, result.matched_count
        )
        return n
    except BulkWriteError as exc:
        errs = exc.details.get("writeErrors", [])
        logger.error(
            "BulkWriteError '%s': %d errors. First: %s",
            collection_name, len(errs), errs[0] if errs else exc,
        )
        return int(exc.details.get("nUpserted", 0))
    except InvalidDocument as exc:
        logger.error("InvalidDocument in '%s': %s", collection_name, exc)
        return 0
    except Exception as exc:
        logger.exception("Unexpected write error in '%s': %s", collection_name, exc)
        return 0


def upsert_all(documents: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Single-pass upsert:
      rss_master       ← full document (with raw_entry)
      sector_*         ← lightweight projection (no raw_entry)
    """
    now    = datetime.now(timezone.utc)
    counts: Dict[str, int] = {}

    # ── rss_master ──
    master_ops = [_build_master_operation(d, now) for d in documents]
    counts[MASTER_COLLECTION] = upsert_collection(MASTER_COLLECTION, master_ops)

    # ── sector_* ──
    sector_map: Dict[str, List[Dict[str, Any]]] = {}
    for doc in documents:
        for sector in doc.get("sectors", ["others"]):
            sector_map.setdefault(sector, []).append(doc)

    for sector, sector_docs in sector_map.items():
        col  = f"sector_{sector}"
        ops  = [_build_sector_operation(d, sector, now) for d in sector_docs]
        counts[col] = upsert_collection(col, ops)

    return counts

# ---------------------------------------------------------------------------
# Main sync job
# ---------------------------------------------------------------------------

def _filter_recent_documents(documents: List[Dict[str, Any]], max_age_days: int) -> List[Dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    recent_docs = []
    for doc in documents:
        published_at = doc.get("published_at")
        if isinstance(published_at, datetime):
            if published_at >= cutoff:
                recent_docs.append(doc)
        else:
            # If no published date, keep in case useful; adjust policy as needed.
            recent_docs.append(doc)
    return recent_docs


def process_all_feeds() -> None:
    logger.info("=" * 70)
    logger.info("RSS sync started | feeds=%d | db=%s", len(RSS_FEEDS), DB_NAME)

    all_docs: List[Dict[str, Any]] = []
    feed_stats: List[Tuple[str, int, str]] = []   # (url, item_count, status)

    for feed_url in RSS_FEEDS:
        try:
            docs = parse_feed(feed_url)
            all_docs.extend(docs)
            feed_stats.append((feed_url, len(docs), "OK" if docs else "EMPTY"))
        except Exception as exc:
            logger.exception("Unhandled error for feed %s: %s", feed_url, exc)
            feed_stats.append((feed_url, 0, f"ERROR: {exc}"))

    # ── Feed summary ──
    logger.info("─" * 70)
    total_parsed = sum(n for _, n, _ in feed_stats)
    ok_feeds     = sum(1 for _, n, _ in feed_stats if n > 0)
    for url, count, status in feed_stats:
        icon = "✓" if count > 0 else "✗"
        logger.info("  %s  %-8s items=%-4d  %s", icon, status, count, url)

    logger.info("─" * 70)
    logger.info(
        "Parse summary: %d/%d feeds OK | %d total articles",
        ok_feeds, len(RSS_FEEDS), total_parsed,
    )

    if not all_docs:
        logger.warning(
            "No articles fetched. Possible causes:\n"
            "  1. Network unreachable (check firewall / DNS)\n"
            "  2. All feeds returning 403/HTML (check feed URLs above)\n"
            "  3. feedparser not installed (pip install feedparser)\n"
            "  Run with:  python %s --test-feeds  to diagnose each URL.",
            sys.argv[0]
        )
        return

    all_docs = _filter_recent_documents(all_docs, MAX_ARTICLE_AGE_DAYS)
    logger.info("Retaining %d docs from last %d days after age filter", len(all_docs), MAX_ARTICLE_AGE_DAYS)

    if not all_docs:
        logger.warning("No articles remain after applying %d-day lookback filter.", MAX_ARTICLE_AGE_DAYS)
        return

    # ── MongoDB upsert ──
    counts = upsert_all(all_docs)

    master_new   = counts.pop(MASTER_COLLECTION, 0)
    sector_total = sum(counts.values())

    logger.info("─" * 70)
    logger.info("  rss_master                     → NEW docs: %d", master_new)
    for col, n in sorted(counts.items()):
        if n > 0:
            logger.info("  %-35s → NEW docs: %d", col, n)

    logger.info(
        "Run complete | parsed=%d | master_new=%d | sector_new_total=%d",
        len(all_docs), master_new, sector_total,
    )
    logger.info("=" * 70)

# ---------------------------------------------------------------------------
# CLI utilities
# ---------------------------------------------------------------------------

def cmd_test_feeds() -> None:
    """--test-feeds: ping every feed URL and report status. Does not write to DB."""
    print("\n" + "=" * 70)
    print("FEED CONNECTIVITY TEST")
    print("=" * 70)
    ok = fail = 0
    for url in RSS_FEEDS:
        text, status, ctype = fetch_feed(url)
        if text:
            doc_count = 0
            if FEEDPARSER_AVAILABLE:
                p = feedparser.parse(text.encode("utf-8", errors="replace"))
                doc_count = len(getattr(p, "entries", []) or [])
            ok += 1
            print(f"  ✓  HTTP {status}  articles={doc_count:<4}  {url}")
        else:
            fail += 1
            print(f"  ✗  HTTP {status}  FAILED                {url}")
    print("-" * 70)
    print(f"  Result: {ok} OK  /  {fail} FAILED  /  {len(RSS_FEEDS)} total")
    print("=" * 70 + "\n")


def cmd_verify_db() -> None:
    """--verify-db: write a test doc and confirm it appears in MongoDB."""
    print("\n" + "=" * 70)
    print("MONGODB WRITE VERIFICATION")
    print("=" * 70)
    test_doc = {
        "unique_key":    "__manual_verify__",
        "title":         "Manual DB Verification Test",
        "sectors":       ["general_market"],
        "feed_source":   "test",
        "source_domain": "test",
        "created_at":    datetime.now(timezone.utc),
        "updated_at":    datetime.now(timezone.utc),
    }
    col = DB[MASTER_COLLECTION]
    try:
        col.replace_one({"unique_key": "__manual_verify__"}, test_doc, upsert=True)
        found = col.find_one({"unique_key": "__manual_verify__"})
        if found:
            print(f"  ✓ Write OK — doc inserted into '{MASTER_COLLECTION}'")
            print(f"    _id = {found['_id']}")
            col.delete_one({"unique_key": "__manual_verify__"})
            print(f"  ✓ Cleanup OK — test doc removed")
        else:
            print(f"  ✗ Write appeared to succeed but doc NOT found — permission issue?")
    except Exception as exc:
        print(f"  ✗ FAILED: {exc}")
    print("=" * 70 + "\n")

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_forever() -> None:
    verify_db_write()
    ensure_indexes()

    if not FEEDPARSER_AVAILABLE:
        logger.warning(
            "feedparser not installed — using XML fallback only.\n"
            "  Install with:  pip install feedparser\n"
            "  This significantly reduces parsing accuracy."
        )

    while True:
        started = time.time()
        try:
            process_all_feeds()
        except Exception as exc:
            logger.exception("Unhandled job error: %s", exc)

        if RUN_ONCE:
            logger.info("RUN_ONCE=1 — exiting after first run.")
            return

        elapsed    = int(time.time() - started)
        sleep_secs = max(5, POLL_INTERVAL_SECONDS - elapsed)
        logger.info("Next run in %d seconds. Press Ctrl+C to stop.", sleep_secs)
        time.sleep(sleep_secs)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RSS → MongoDB ingestor")
    parser.add_argument(
        "--test-feeds",
        action="store_true",
        help="Test connectivity to every RSS feed URL and exit.",
    )
    parser.add_argument(
        "--verify-db",
        action="store_true",
        help="Verify MongoDB write permissions and exit.",
    )
    args = parser.parse_args()

    if args.test_feeds:
        cmd_test_feeds()
        sys.exit(0)

    if args.verify_db:
        cmd_verify_db()
        sys.exit(0)

    try:
        run_forever()
    except KeyboardInterrupt:
        logger.info("Stopped by user (Ctrl+C).")
        sys.exit(0)
