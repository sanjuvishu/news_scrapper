from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from pymongo import ASCENDING, TEXT, MongoClient, UpdateOne
from pymongo.database import Database
from pymongo.errors import BulkWriteError, InvalidDocument, OperationFailure, ServerSelectionTimeoutError

from .config import AppConfig
from .utils import sanitize_for_bson


class MongoRepository:
    def __init__(self, config: AppConfig, logger: logging.Logger) -> None:
        self._config = config
        self._logger = logger
        self._db = self._connect()

    def _connect(self) -> Database:
        try:
            client = MongoClient(self._config.mongo_uri, serverSelectionTimeoutMS=8000)
            client.admin.command("ping")
            self._logger.info("MongoDB connected uri=%s db=%s", self._config.mongo_uri, self._config.db_name)
            return client[self._config.db_name]
        except ServerSelectionTimeoutError as exc:
            raise SystemExit(f"Cannot reach MongoDB at {self._config.mongo_uri}: {exc}") from exc

    def verify_write(self) -> None:
        canary_key = "__startup_canary__"
        collection = self._db[self._config.master_collection]
        canary = {"unique_key": canary_key, "title": "startup write-test", "created_at": datetime.now(timezone.utc)}
        try:
            collection.replace_one({"unique_key": canary_key}, canary, upsert=True)
            collection.delete_one({"unique_key": canary_key})
        except Exception as exc:
            raise SystemExit(f"MongoDB write verification failed: {exc}") from exc

    def _ensure_index(self, collection_name: str, keys: list, **kwargs: Any) -> None:
        collection = self._db[collection_name]
        try:
            collection.create_index(keys, **kwargs)
        except OperationFailure as exc:
            if "already exists with a different name" not in str(exc):
                raise

    def ensure_indexes(self) -> None:
        master = self._config.master_collection
        self._ensure_index(master, [("unique_key", ASCENDING)], unique=True, name="uq_key")
        self._ensure_index(master, [("published_at", ASCENDING)], name="idx_published")
        self._ensure_index(master, [("feed_source", ASCENDING)], name="idx_source")
        self._ensure_index(master, [("source_domain", ASCENDING)], name="idx_domain")
        self._ensure_index(master, [("sectors", ASCENDING)], name="idx_sectors")
        self._ensure_index(master, [("title", TEXT), ("summary", TEXT)], name="text_search", default_language="english")
        for sector in self._config.all_sectors:
            collection = f"sector_{sector}"
            self._ensure_index(collection, [("unique_key", ASCENDING)], unique=True, name="uq_key")
            self._ensure_index(collection, [("published_at", ASCENDING)], name="idx_published")
            self._ensure_index(collection, [("feed_source", ASCENDING)], name="idx_source")
            self._ensure_index(collection, [("source_domain", ASCENDING)], name="idx_domain")
            self._ensure_index(collection, [("sector", ASCENDING)], name="idx_sector")

    def _build_master_operation(self, doc: Dict[str, Any], now: datetime) -> UpdateOne:
        clean = sanitize_for_bson(doc)
        insert_only = {key: value for key, value in clean.items() if key not in {"updated_at", "sectors"}}
        return UpdateOne(
            {"unique_key": clean["unique_key"]},
            {"$setOnInsert": insert_only, "$set": {"updated_at": now, "sectors": clean.get("sectors", ["others"])}},
            upsert=True,
        )

    def _build_sector_operation(self, doc: Dict[str, Any], sector: str, now: datetime) -> UpdateOne:
        clean = sanitize_for_bson(doc)
        sector_doc = {key: value for key, value in clean.items() if key not in {"raw_entry", "updated_at", "sectors"}}
        sector_doc["sector"] = sector
        return UpdateOne(
            {"unique_key": sector_doc["unique_key"]},
            {"$setOnInsert": sector_doc, "$set": {"updated_at": now}},
            upsert=True,
        )

    def _upsert_collection(self, collection_name: str, operations: List[UpdateOne]) -> int:
        if not operations:
            return 0
        try:
            result = self._db[collection_name].bulk_write(operations, ordered=False)
            return int(result.upserted_count or 0)
        except BulkWriteError as exc:
            self._logger.error("Bulk write error for %s: %s", collection_name, exc)
            return int(exc.details.get("nUpserted", 0))
        except InvalidDocument as exc:
            self._logger.error("Invalid document in %s: %s", collection_name, exc)
            return 0

    def upsert_all(self, documents: List[Dict[str, Any]]) -> Dict[str, int]:
        now = datetime.now(timezone.utc)
        counts: Dict[str, int] = {}
        master_ops = [self._build_master_operation(doc, now) for doc in documents]
        counts[self._config.master_collection] = self._upsert_collection(self._config.master_collection, master_ops)

        sector_map: Dict[str, List[Dict[str, Any]]] = {}
        for doc in documents:
            for sector in doc.get("sectors", ["others"]):
                sector_map.setdefault(sector, []).append(doc)

        for sector, sector_docs in sector_map.items():
            collection_name = f"sector_{sector}"
            operations = [self._build_sector_operation(doc, sector, now) for doc in sector_docs]
            counts[collection_name] = self._upsert_collection(collection_name, operations)
        return counts

