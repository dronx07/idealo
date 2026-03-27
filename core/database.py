# core/database.py

import os
import json
import asyncio
from datetime import datetime, timezone
from .logger import get_logger

logger = get_logger("Database")

DB_PATH = "data/deals.json"


class Database:
    def __init__(self):
        os.makedirs("data", exist_ok=True)

        if not os.path.exists(DB_PATH):
            with open(DB_PATH, "w", encoding="utf-8") as f:
                json.dump([], f)

        self._lock = asyncio.Lock()

    async def _read_all(self):
        async with self._lock:
            try:
                with open(DB_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                logger.error("Database missing or corrupted. Resetting.")
                return []

    async def _write_all(self, data):
        async with self._lock:
            with open(DB_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

    async def save_deal(self, deal: dict):
        deals = await self._read_all()

        for existing in deals:
            if existing["asin"] == deal["asin"] or existing["ean"] == deal["ean"]:
                logger.info(f"Duplicate skipped: ASIN {deal['asin']}")
                return False

        deal["posted"] = False
        deal["posted_at"] = None
        deal["created_at"] = datetime.now(timezone.utc).isoformat()

        deals.append(deal)
        await self._write_all(deals)

        logger.info(f"Saved ASIN {deal['asin']} to database.")
        return True

    async def get_unposted_deals(self, limit: int):
        deals = await self._read_all()

        unposted = [d for d in deals if not d.get("posted", False)]

        unposted.sort(key=lambda d: d.get("created_at", ""))

        return unposted[:limit]

    async def mark_as_posted(self, asin: str):
        deals = await self._read_all()

        for deal in deals:
            if deal["asin"] == asin:
                deal["posted"] = True
                deal["posted_at"] = datetime.now(timezone.utc).isoformat()
                break

        await self._write_all(deals)
        logger.info(f"Marked ASIN {asin} as posted.")

    @staticmethod
    async def reset_db():
        with open(DB_PATH, "w", encoding="utf-8") as f:
                json.dump([], f)
        
