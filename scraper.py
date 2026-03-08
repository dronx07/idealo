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
    "Sessioninfoencrypted": "a2904e53f7ede4e5c6c769b284be5a2869e06f31908ce2646116e5be1df10612cdd232b171216e50fab748cfaf0cf3c6b91f4e0f8e80cf023daa17a68ccb14560378ed5e9dd929582f9c943d0beea62dc8950313c91eb40926622bc8f31df1a1d32547d93750b5ea5c29f5096476eb5b8424fca3a48d7e2e7d618958ca4d4431703f3e37942df03a9ec240302645cf019c98971ce981c0f7edf100a5ae14edeb62d7b4cc81b9dfc7489d2c738c3a668d562da4b4a224fb6a648f516803202795be9ec9aac51bf2db5c5eda226b9d2dae221961fdb239be94244813b56133ad39a17736c8b372c9946be54670a9a9e3692e4163601a2baf1b20fdd630aa8dd694f453ff8b4d59da0861244ee3d408e0729a248f1dc108773a2fa75074d5fdaafc7e5e49e1a322f4a5ccbd39d75ceee78d1f9606407afaf6922bd248abce455627c5890f2c8ccc8568f7b150ef12357ddf6e21f53538336ba0de299a5af0063212e3fe2a12b7f798c8266ced391489d9359d897ae4f1d2d91e9a664eae592d1b15e37571125bf770d6fc43c33282e6f7b732245585607d91a5be79ffa98fa9c11e1a25b7dfdb18929a1122b5792d218726ab6c05f65264041465ccb876533ed52ba23337105a07132bcda4ff1f149bda0e371593362fb02bab197debe1561c8e5e430be32e85183cec619aba181eb3858be2d8604a9c74cad22d94bff45d7186e062a1cc893f47761669f6d1f2da3079875dc467c9f8f691fcea7d4cc17a587a0dfe60de80614ecd7ddbc2aba83fed458bd5ca63830581fd18cd34815ec75f237e06683b882d3d1c2f5d06e884ed8656024c5708fce8e161ac18140955bbbdc1d0ebb0c482ad2ac91b1b6d3bafed0ac120caf635dd072ac5b081573d38261bc3c3819c3143c6a0cec5793990aa4eee30695035b93d731b361daf6dac269d75b2a34de2a718d712d64829e006c9683dde66",
    "Cookie": 'sessionid=1772955226435_e768bea1-2e20-15a3-2786-a4185de1b861; ttrjm=ac2f3875-c714-4e29-a83a-9b3ce5ad6cd6; xp_seed_s=093da; _cmp_controllerid=0c4f1bc4cdea4d3946fc264c99dd68622e9846175fbed816f94669c823876432; xp_seed_p=093da; consentStatus=true; trackingid=019ccc5d-e99b-7511-a261-97be7e875f14; fs_sampled=false; campaignInfo=?utm_source=google&utm_medium=cpc; campaignInfoPerm=?utm_source=google&utm_medium=cpc; _gcl_au=1.1.1093449155.1772955233; crto_is_user_optout=false; crto_mapped_user_id=J8NQuF80WSUyRmxEUGdCT3l5MGhnZ1gxTnNrczE1JTJCbFZranpCSnZrcVhlaG51c1ZLVnZiUCUyRm5yJTJGcHpCTE1YUVhXRU5hS3k; sp=c5b671bb-017a-4622-92f5-de55f3ed8376; FPAU=1.1.1093449155.1772955233; _fbp=fb.1.1772955234009.921012702435100680; bm_ss=ab8e18ef4e; bm_mi=3F277556DD9E60CEBBF0F8AF56694C0C~YAAQhmE+F/7+HbecAQAAPNh2zB8he0VQoh6MwMiV0TXMx54140BUbrb/xTItzxqYqlcLWsguFayArw+TsUDVSa15k44Boy7umCk0X7kPCVaSTnAaV7dSocveGkrj6DsRHzudSdFVeQQg6tbo1d91b7lfDRi7SfHyyaF7gt7rAXQw00t6sv5YRvOrTFMclPRVs0sR+bjD0z+p6MCsS7V6I6bwVS+8nB0Nuin8+qwnSicdrsLHKr5lIDO9CXXBhVkdAyA+Kxx9d1hEqGwM3bU0OvCPc5wORN4hr+ROdNPJlDWQDkNgS12jnu2r8WRqAqZK/Qozfa1OHiRrckVgeNqPc0ZsriiyfWtyPW1keSGfTp4p~1; bm_so=C27A0B811716690A7F9AEE54594297A88F7C0A204439D0E2661689C231D75ACC~YAAQhmE+FwD/HbecAQAAPdh2zAb/IWAWDScjRxYroa4wqTMQVCfoa230vVBFFVnMi+yLEqo8Ncc78hnytGZjuVFJA2aU+8LLysLUSAi8hNlmxRAFaB0SDxOifMnRXuWIr2VkDWu0AsWqLhvqNAFatHYIqEgDiu4nueRlzkf/+e9fO1JWTAZFejp7iN6dDQt5f/WEwEJWHwLESlAUqWyM4XTim5sSwML/ENWVTrn1FSg/R5guLMUwCU6R05NC4OSxMsScHAa0T7v//NWbX2nYJlFtSfzS2yGOroYiFB3gjDkPwH82OLv8xdKVdW8b6A/HDqyEzBccTdLDT8pKnzBIfEkeD1cTh7ZokRX5bIUL4pJPsAj5zJC7RRaJA6Jz+W37O9kdV1E2MEXV/w1Az2ZEEdDuom9n4+kUR/39kxJ5r/k/8N7RaB0Axr9NYaa8GepAaoUxY2IaXP+W1QvacUxTQ0ENzpHnH0ZksftMEzkgYwVnxkkKrqk=; bm_lso=C27A0B811716690A7F9AEE54594297A88F7C0A204439D0E2661689C231D75ACC~YAAQhmE+FwD/HbecAQAAPdh2zAb/IWAWDScjRxYroa4wqTMQVCfoa230vVBFFVnMi+yLEqo8Ncc78hnytGZjuVFJA2aU+8LLysLUSAi8hNlmxRAFaB0SDxOifMnRXuWIr2VkDWu0AsWqLhvqNAFatHYIqEgDiu4nueRlzkf/+e9fO1JWTAZFejp7iN6dDQt5f/WEwEJWHwLESlAUqWyM4XTim5sSwML/ENWVTrn1FSg/R5guLMUwCU6R05NC4OSxMsScHAa0T7v//NWbX2nYJlFtSfzS2yGOroYiFB3gjDkPwH82OLv8xdKVdW8b6A/HDqyEzBccTdLDT8pKnzBIfEkeD1cTh7ZokRX5bIUL4pJPsAj5zJC7RRaJA6Jz+W37O9kdV1E2MEXV/w1Az2ZEEdDuom9n4+kUR/39kxJ5r/k/8N7RaB0Axr9NYaa8GepAaoUxY2IaXP+W1QvacUxTQ0ENzpHnH0ZksftMEzkgYwVnxkkKrqk=~1772956867919; JSESSIONID=1416BC33A8973B6DBE0512BCBA968AFF; ak_bmsc=E715CDD03DD0288987E1F93B362D1544~000000000000000000000000000000~YAAQhmE+F1n/HbecAQAAAt12zB95srNO0PYnb0DyDZC7Ift9xTNhpzZDDJBtJKX/8uYF9ZokWuq1EPjHAgMict0EbR+lCDA4fqrc0aIHkQW/1NhiU/cB2VLckomkVZ0jZFTT4+x+SbVOBBk1BWZ5ZfLQ5qDEoWu7Al74x6LnfiQuuafIy3iY+Wazvw0jur3RFI3SvrWE2lwv6YrMoQ5bma9mGewVTvPumzrq+oNLzsklX9ru/rxGXR3nkrJ55RocVsJvtGPptwjKkoAQxwuEkeoYdW4y21QCcmApmF1UZ8W0dyg9cY5c9qQg6fwIQuPINQFXO7ZOoalx/Fy//ziJrvoRh2SM7qbkykl9c4HiAxvqm/Y4RChAA565nw9yJuRI+fNx6hCZmCmxQ7LhrAOcfBU/Yi3tnxtClaz5FTunE5Htxbbsj9PllwCfmnWNBz+qalTkKOUTKe9v7ub9Xrl+d6P8+/PLWWH3Wp1Uyw/rL+kDBl2YKhsM9yVF3JdqbPnwDS9NAaaeiNwbVnwNuk7wDPuJSAD48Wbe6wknTfXGFr/kzy7R5SO9akWpNlxx8xbDndjr; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22CA9fiyJeEb3TJZJc0yeF%22%2C%22expiryDate%22%3A%222027-03-08T08%3A01%3A08.918Z%22%7D; _tq_id.TV-7245903663-1.233d=fedb43c5eff0f0de.1772955233.0.1772956869..; _rdt_uuid=1772955233524.2fed4bb8-3edf-4e6b-b3f8-b28f0ddeb6b7; _uetsid=28a289201ac111f18874cb385a59f11c; _uetvid=28a2cb701ac111f1b6637315a9ecec80; bm_sv=F51B9F68FFEA9CDEE07235307CCFF02D~YAAQiWE+F5C5hJ+cAQAAXON2zB9NDtZ8SiVh/hsZr4uq56soDljpf13zxnde2+cGjpOKUnY4pB/7r2aZJQDlSgjGRAuyEGjBF9d5d5rrnqKtw9JkCp8aPLU4ea0DR57OqJTrusC/iQBaF8+vINFSkseS1UplhHV9ycALnS4Wtm+S42uR8l2bRtHqPOcutUd5norQ9iVyXwK0tYWS4+dEOPp4bf4UXZgnKfDmlLKhY4gpCPqEi0SAvNWKgQn7EkwG~1; bm_s=YAAQhmE+FzMCHrecAQAAZgZ3zAW4L1uSOIEnrvWX3sK/JU2SctAJSu/pJ/yjlam21KqeTnlda3UxmYYaJSP8aCwrNYuof4kygVxh4/XD49Z2T1mXVJkgBjTdAjcIuiI3wuESFFKMu86U86QpCylgChGIqsu1zIDH7yMQMpRkX62Ea+RLOeHQ3UVuyISTb5CtnBei/nE3sGnhLBS/56jx1POjhPzrgZdKQG093W0fMXZ7+3JR5AEPArc884d9ZoLuPs0pfBTssOWCftspPiS4Jjm5LhPKiL492nbOAwQ8jNFOFUwwGD3pkN7a6YMWK0OaM651ylJP9P22cy2KwEe2cgXMZi+1cxXsR4qmSEFyZbBVN7ggV0vbPQHEsMYGNK2j0/nNWkvKg6UiFHUBaT2a69tx0r699WqTX+cMnl2zDas7OtIM7HCRolTZFdDIpDsVF4JTdw3QGH/6/vi9SeDWkZHkiP1HSYzbGXztYR8/+eKFslvCiQZog1pI7CoRkTvpV1RO7NjuAj/+JFCM6e/4spuQvMAomVLUfs6n4/T/7tzvGkpk82FuakCJ/UjvgjuWP+nSXKCXIkdZ6kMIJqicEDdgZzb7eqXmAq7Hf0Uqxpn4WDAWkcoRE/CZz5ljI22faU9Lyb4vMORf//KAAay+UjA4gAH1wwzN80eleQo2mnoKAIp3XBiShrn4UVnXWgUaetYaaIihFhlGMva4YylUlZHe0xhh4YgoIGpcAQjPoG6EdhG0FpTh3H+bPkPmqNc/+cbOsacLUE73vPf/4bAE8pHLVQ8HaCPI4rFIznYLJk5wJVhOADtDmhhsQNYdxwXD'
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)


async def fetch_ids(session, page_index):
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
                ids = [item["id"] for item in items if "id" in item]
                logger.info(f"Page {page_index} collected {len(ids)} ids")
                return ids
            except Exception as e:
                logger.warning(f"Page {page_index} attempt {attempt} failed: {e}")
                await asyncio.sleep(2 ** attempt)
        logger.error(f"Page {page_index} failed after {MAX_RETRIES} attempts")
        return []


async def resolve_url(session, url):
    async with semaphore:
        logger.info(f"Resolving {url}")
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await asyncio.sleep(random.uniform(0.3, 1.0))
                r = await session.get(url, timeout=15, allow_redirects=True)
                logger.info(f"{url} status {r.status_code}")
                r.raise_for_status()
                final_url = str(r.url)
                logger.info(f"Resolved {url} → {final_url}")
                return final_url
            except Exception as e:
                logger.warning(f"{url} attempt {attempt} failed: {e}")
                await asyncio.sleep(2 ** attempt)
        logger.error(f"{url} failed after {MAX_RETRIES} attempts")
        return None


async def main():
    logger.info("Starting scraper")
    
    async with AsyncSession(
        headers=HEADERS,
        proxies={"http": PROXY, "https": PROXY} if PROXY else None,
        impersonate="chrome124",
        http_version="v2"
    ) as session:

        logger.info("Fetching product IDs")
        id_tasks = [fetch_ids(session, i) for i in range(TOTAL_PAGES)]
        id_results = await asyncio.gather(*id_tasks)

        product_ids = [pid for ids in id_results for pid in ids]
        logger.info(f"Collected total {len(product_ids)} product IDs")

        if not product_ids:
            logger.error("No product IDs collected, aborting")
            return

        base_urls = list(dict.fromkeys(
            f"https://www.idealo.fr/prix/{pid}.html" for pid in product_ids
        ))

        logger.info(f"Resolving {len(base_urls)} URLs")

        resolve_tasks = [resolve_url(session, url) for url in base_urls]
        resolved_results = await asyncio.gather(*resolve_tasks)

        final_urls = list(dict.fromkeys(
            url for url in resolved_results if url
        ))

        logger.info(f"Collected {len(final_urls)} unique resolved URLs")

        if not final_urls:
            logger.error("No resolved URLs collected, aborting file write")
            return

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(final_urls))

        logger.info(f"{OUTPUT_FILE} written successfully")


if __name__ == "__main__":
    asyncio.run(main())
