import asyncio
import httpx
import logging
import os
import random

API_URL = "https://www.idealo.fr/csr/api/v2/modules/dealsResult"
TOTAL_PAGES = 67
MAX_RETRIES = 3
CONCURRENT_REQUESTS = 5
OUTPUT_FILE = "links.txt"

PROXY = os.getenv("PROXY")

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.idealo.fr/bons-plans",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)


async def fetch_ids(client, page_index):
    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await asyncio.sleep(random.uniform(0.3, 1.0))

                r = await client.get(
                    API_URL,
                    params={
                        "locale": "fr_FR",
                        "pageIndex": page_index,
                        "itemsPerPage": 60,
                        "itemStates": "BARGAIN",
                    },
                )

                logger.info(f"Page {page_index} → {r.status_code}")

                r.raise_for_status()
                data = r.json()

                items = data.get("items", [])
                return [item["id"] for item in items if "id" in item]

            except Exception as e:
                logger.warning(f"Page {page_index} attempt {attempt} failed: {e}")
                await asyncio.sleep(2 ** attempt)

        logger.error(f"Page {page_index} failed after retries.")
        return []


async def resolve_url(client, product_id):
    url = f"https://www.idealo.fr/prix/{product_id}.html"

    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await asyncio.sleep(random.uniform(0.3, 1.0))

                r = await client.get(url, follow_redirects=True)
                logger.info(f"Resolve {product_id} → {r.status_code}")

                r.raise_for_status()
                return str(r.url)

            except Exception as e:
                logger.warning(f"Resolve {product_id} attempt {attempt} failed: {e}")
                await asyncio.sleep(2 ** attempt)

        return None


async def main():
    timeout = httpx.Timeout(15.0)

    async with httpx.AsyncClient(
        headers=HEADERS,
        proxy=PROXY,
        timeout=timeout,
        http2=False,
    ) as client:

        logger.info("Fetching product IDs...")

        id_tasks = [fetch_ids(client, i) for i in range(1, TOTAL_PAGES + 1)]
        id_results = await asyncio.gather(*id_tasks)

        product_ids = [pid for ids in id_results for pid in ids]

        logger.info(f"Collected {len(product_ids)} product IDs")

        if not product_ids:
            logger.error("No product IDs collected — likely blocked. Aborting.")
            return

        logger.info("Resolving product URLs...")

        resolve_tasks = [resolve_url(client, pid) for pid in product_ids]
        resolved_urls = await asyncio.gather(*resolve_tasks)

        final_urls = list(dict.fromkeys(url for url in resolved_urls if url))

        logger.info(f"Resolved {len(final_urls)} unique URLs")

        if not final_urls:
            logger.error("No URLs resolved — skipping file write.")
            return

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(final_urls))

        logger.info("links.txt written successfully.")


if __name__ == "__main__":
    asyncio.run(main())
