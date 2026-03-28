# core/ean2asin.py

from bs4 import BeautifulSoup
from .requester import Requester

async def convert(ean: str, cookie: str):
    url = f"https://www.amazon.fr/s?k={ean}"
    referrer = "https://www.amazon.fr/"

    async with Requester(url=url, referrer=referrer, cookie=cookie) as scraper:
        response = await scraper.fetch_get()

        if not response or response.status_code != 200:
            print(f"{ean} - Blocked or request failed.")
            return None

        html = response.content.decode("utf-8", errors="ignore")
        soup = BeautifulSoup(html, "lxml")

        products = soup.find_all("div", attrs={"data-component-type": "s-search-result"})

        for product in products:
            if product.find(name="span", string="Sponsored"):
                continue

            asin = product.get("data-asin")
            if asin and len(asin) == 10:
                return asin

        return None
