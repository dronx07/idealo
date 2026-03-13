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
    "Accept": "application/json, text/plain, */*",
    "Acccept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Referer": "https://www.idealo.fr/bons-plans",
    "Priority": "u=1, i",
    "Ismobile": "false",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Sessioninfoencrypted": "8438ad615e15693a44e4322cf9729a40c686c3312e6320fc443d6a483451876a5b07be9faa76f513592e65a96e93970a5b835306c2db9a4c9b5d15c65ecd8f26ca757935b1d28c6d6a5125c08090e0c7a6fe34113e83abf4102ada74680ed9333a3a2a34efd6b9e9ce3b102b51b8529e7837e3183fc629fd7b77cd77e6532ff5803623d608360b0873b19e24b75eb6be428c888ac0f8e6ae87f15d91807644a6c0d590504e82f14818eb5e414a56dcb989fc22498c553e563a29248b84c34573d8ea4105eeb755e2bbe12f453a43be45c6fae6392c7149a1e157a978c7f2748255daf12f62cc77d2e423dff9f232689cc1c0c5106f9368a73abdd8fcb12f9dae21032e97d5f0d9f7cc44389ec5385e00e3910d946b3f86bbda257e6880865f783cbaa794042d18eaf948a1184f1138f745f8ea74f74bc36a17f791c5886c6e240e68169653d6f44d4149d7281d3a3fac8c70becef7fedd6a1bc48b2ac0e28ef47ff85ea6a33cf3229d682d208ddd09e96cd44ec934de18b6185e964ab2d646165ff7f5bfd8928a40f210d64e84353b5505a72c16b0fc0632abccadb214588eb5390de7d073200e56261798090921a558fc63d43a17d9cc2d2d0b371c74a91a29a4a754ce2cffe4dae444774cef803c169f289118206dd06ae5d8055a20f3f2170b60596e7f3a422ea35dde072a25dac851979521734265c152cb93cb997eb12c66a44dfe407222b8bbc3adedcf6933b841871e29b7484909aa023b0372a627a3ca060e9302b15e54020b1440aa2cfbedc1ba196775943df6038dcbf23030ff88f657f87fc0d8c15a4978796551ceaba1c9a26d778ed7291d9b10bc318281bcdb"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)


async def resolve_hash(session, token):
    try:
        r = await session.post(
            "https://www.idealo.fr/ipc/prg",
            data={"value": token},
            allow_redirects=True,
            timeout=15,
        )
        return str(r.url)
    except Exception:
        return None


async def fetch_urls(session, page_index):
    async with semaphore:
        logger.info(f"Fetching page {page_index}")

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

                logger.info(f"Page {page_index} status {r.status_code}")
                r.raise_for_status()

                data = r.json()
                items = data.get("items", [])

                urls = []
                hashes = []

                for item in items:
                    href = item.get("href")
                    if not href:
                        continue
                    if href.startswith("http"):
                        urls.append(href)
                    else:
                        hashes.append(href)

                if hashes:
                    tasks = [resolve_hash(session, h) for h in hashes]
                    resolved = await asyncio.gather(*tasks)
                    urls.extend([u for u in resolved if u])

                logger.info(f"Page {page_index} collected {len(urls)} urls")
                return urls

            except Exception as e:
                logger.warning(f"Page {page_index} attempt {attempt} failed: {e}")
                await asyncio.sleep(2 ** attempt)

        logger.error(f"Page {page_index} failed after {MAX_RETRIES} attempts")
        return []


async def main():
    logger.info("Starting scraper")

    async with AsyncSession(
        headers=HEADERS,
        proxies={"http": PROXY, "https": PROXY} if PROXY else None,
        impersonate="chrome142",
        http_version="v2"
    ) as session:

        tasks = [fetch_urls(session, i) for i in range(TOTAL_PAGES)]
        results = await asyncio.gather(*tasks)

        urls = [url for result in results for url in result]
        final_urls = list(dict.fromkeys(urls))

        logger.info(f"Collected {len(final_urls)} unique URLs")

        if not final_urls:
            logger.error("No URLs collected")
            return

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(final_urls))

        logger.info(f"{OUTPUT_FILE} written successfully")


if __name__ == "__main__":
    asyncio.run(main())
