# core/sales_scraper.py

import asyncio
import re
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from .logger import get_logger

logger = get_logger("Sales Scraper")


class SalesScraper:
    def __init__(
        self,
        cookies: list,
        max_pages: int = 10,
        headless: bool = False,
    ):
        self.cookies = cookies
        self.base_url = "https://sas.selleramp.com/sas/lookup?src=web&SasLookup%5Bsearch_term%5D={}"
        self.max_pages = max_pages
        self.headless = headless

        self.playwright = None
        self.browser = None
        self.context = None
        self.semaphore = asyncio.Semaphore(max_pages)

    async def start(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless
        )
        self.context = await self.browser.new_context()
        await self.context.add_cookies(self.cookies)

    async def close(self):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def get_sales(self, asin: str):
        async with self.semaphore:
            page = await self.context.new_page()

            try:
                await page.goto(self.base_url.format(asin), timeout=60000)
                await page.wait_for_load_state("domcontentloaded")

                html = await page.content()
                soup = BeautifulSoup(html, "lxml")

                sales_tag = soup.find("span", class_="estimated_sales_per_mo")
                if not sales_tag:
                    return None

                text = sales_tag.get_text(strip=True).replace(",", "")
                match = re.search(r"\d+", text)

                if not match:
                    return None

                return float(match.group())

            except Exception as e:
                logger.warning(
                    f"{e} occurred while scraping sales for ASIN: {asin}."
                )
                return None

            finally:
                await page.close()