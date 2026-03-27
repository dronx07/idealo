# poster.py

import os
import asyncio
import random
from core.database import Database
from core.discord_sender import DiscordSender
from core.logger import get_logger
from dotenv import load_dotenv
import json
import math

load_dotenv()

logger = get_logger("Poster")

WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK")

with open("data/deals.json", "r", encoding="utf-8") as f:
    total = len(json.loads(f.read()))

MAX_POSTS_PER_RUN = math.ceil(total/5) if total > 50 else total
MIN_DELAY = 15
MAX_DELAY = 30

async def main():
    if not WEBHOOK_URL:
        logger.error("DISCORD_WEBHOOK not set.")
        return

    logger.info("Starting hourly poster run.")

    db = Database()
    sender = DiscordSender(WEBHOOK_URL)

    deals = await db.get_unposted_deals(limit=MAX_POSTS_PER_RUN)

    if not deals:
        logger.info("No unposted deals found.")
        return

    logger.info(f"Posting {len(deals)} deals")

    for deal in deals:
        try:
            success = await sender.send_deal(deal)

            if success:
                await db.mark_as_posted(deal["asin"])
                logger.info(f"Posted ASIN {deal['asin']}")

        except Exception as e:
            logger.exception(f"Failed ASIN {deal.get('asin')} - {e}")

        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        logger.info(f"Waiting {delay}s before posting next deal.")
        await asyncio.sleep(delay)

    logger.info("Finished hourly run")


if __name__ == "__main__":
    asyncio.run(main())
