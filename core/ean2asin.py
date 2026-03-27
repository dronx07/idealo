# core/ean2asin.py

from bs4 import BeautifulSoup
from .requester import Requester

async def convert(ean: str, cookie: str):
    async with Requester(
        url=f"https://www.amazon.fr/s?k={ean}",
        referrer="https://www.amazon.fr/",
        cookie=cookie
    ) as scraper:
        output = await scraper.fetch_get()

        if not output:
            return None

        if output.status_code != 200:
            print(f"{ean} - Blocked.")
            return None

        html = output.content.decode("utf-8", errors="ignore")
        soup = BeautifulSoup(html, "lxml")

        products = soup.find_all("div", attrs={"data-component-type": "s-search-result"})

        for product in products:
            if product.find("div", attrs={"class": "a-row a-spacing-micro"}):
                continue

            asin = product.get("data-asin")
            if asin and len(asin) == 10:
                return asin

        return None
