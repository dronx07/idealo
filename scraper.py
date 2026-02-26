import asyncio
import logging
import os
import random
from dotenv import load_dotenv
from curl_cffi.requests import AsyncSession

load_dotenv()

API_URL = "https://www.idealo.fr/csr/api/v2/modules/dealsResult"
TOTAL_PAGES = 67
MAX_RETRIES = 1
CONCURRENT_REQUESTS = 10
OUTPUT_FILE = "links.txt"

PROXY = os.getenv("PROXY")

HEADERS = {
    "Referer": "https://www.idealo.fr/bons-plans",
    "Sessioninfoencrypted": "a2904e53f7ede4e5c6c769b284be5a2869e06f31908ce2646116e5be1df10612cdd232b171216e50fab748cfaf0cf3c6b91f4e0f8e80cf023daa17a68ccb14560378ed5e9dd929582f9c943d0beea62dc8950313c91eb40926622bc8f31df1a1d32547d93750b5ea5c29f5096476eb5b8424fca3a48d7e2e7d618958ca4d4431703f3e37942df03a9ec240302645cf019c98971ce981c0f7edf100a5ae14edeb62d7b4cc81b9dfc7489d2c738c3a668d562da4b4a224fb6a648f516803202795be9ec9aac51bf2db5c5eda226b9d2dae221961fdb239be94244813b56133ad39a17736c8b372c9946be54670a9a9e3692e4163601a2baf1b20fdd630aa8dd694f453ff8b4d59da0861244ee3d408e0729a248f1dc108773a2fa75074d5fdaafc7e5e49e1a322f4a5ccbd39d75ceee78d1f9606407afaf6922bd248abce455627c5890f2c8ccc8568f7b150ef12357ddf6e21f53538336ba0de299a5af0063212e3fe2a12b7f798c8266ced391489d9359d897ae4f1d2d91e9a664eae592d1b15e37571125bf770d6fc43c33282e6f7b732245585607d91a5be79ffa98fa9c11e1a25b7dfdb18929a1122b5792d218726ab6c05f65264041465ccb876533ed52ba23337105a07132bcda4ff1f149bda0e371593362fb02bab197debe1561c8e5e430be32e85183cec619aba181eb3858be2d8604a9c74cad22d94bff45d7186e062a1cc893f47761669f6d1f2da3079875dc467c9f8f691fcea7d4cc17a587a0dfe60de80614ecd7ddbc2aba83fed458bd5ca63830581fd18cd34815ec75f237e06683b882d3d1c2f5d06e884ed8656024c5708fce8e161ac18140955bbbdc1d0ebb0c482ad2ac91b1b6d3bafed0ac120caf635dd072ac5b081573d38261bc3c3819c3143c6a0cec5793990aa4eee30695035b93d731b361daf6dac269d75b2a34de2a718d712d64829e006c9683dde66"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)


async def fetch_ids(session, page_index):
    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await asyncio.sleep(random.uniform(0.3, 1.0))
                r = await session.get(
                    API_URL,
                    params={
                        "locale": "fr_FR",
                        "pageIndex": page_index,
                        "itemsPerPage": 60,
                        "itemStates": "BARGAIN",
                    },
                    timeout=15,
                )
                r.raise_for_status()
                data = r.json()
                items = data.get("items", [])
                return [item["id"] for item in items if "id" in item]
            except Exception:
                await asyncio.sleep(2 ** attempt)
        return []


async def resolve_url(session, url):
    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await asyncio.sleep(random.uniform(0.3, 1.0))
                r = await session.get(url, timeout=15, allow_redirects=True)
                r.raise_for_status()
                return str(r.url)
            except Exception:
                await asyncio.sleep(2 ** attempt)
        return None


async def main():
    async with AsyncSession(
        headers=HEADERS,
        proxies={"http": PROXY, "https": PROXY} if PROXY else None,
        impersonate="chrome124",
        http_version="v2"
    ) as session:

        id_tasks = [fetch_ids(session, i) for i in range(TOTAL_PAGES)]
        id_results = await asyncio.gather(*id_tasks)
        product_ids = [pid for ids in id_results for pid in ids]

        if not product_ids:
            return

        base_urls = list(dict.fromkeys(
            f"https://www.idealo.fr/prix/{pid}.html" for pid in product_ids
        ))

        resolve_tasks = [resolve_url(session, url) for url in base_urls]
        resolved_results = await asyncio.gather(*resolve_tasks)

        final_urls = list(dict.fromkeys(
            url for url in resolved_results if url
        ))

        if not final_urls:
            return

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(final_urls))


if __name__ == "__main__":
    asyncio.run(main())
