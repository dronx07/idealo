# scanner.py

import asyncio
import aiohttp
from core.database import Database
from core.ean2asin import convert
from core.logger import get_logger
from core.seller_central import SellerCentral
from core.sales_scraper import SalesScraper
import json

JSON_URL = "https://raw.githubusercontent.com/dronx07/idealo_best_selling/main/products.json"
COOKIE_URL = "https://raw.githubusercontent.com/dronx07/cookie_refresh/main/cookies.json"

logger = get_logger("Scanner")

async def fetch_products() -> list:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(JSON_URL, timeout=10) as response:
                    if response.status != 200:
                        return []
                    text = await response.text()
                    data = json.loads(text)
                    logger.info(f"Fetched products JSON.")
                    return data
        except Exception as e:
            logger.error(f"Failed to fetch products JSON: {e}.")
        return []


async def fetch_cookies() -> tuple:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(COOKIE_URL, timeout=10) as response:
                if response.status != 200:
                    return None,None,None
                text = await response.text()
                data = json.loads(text)
                logger.info(f"Fetched cookies JSON.")
                return data["set4"]["amazon"], data["set4"]["seller"], data["sas"]
    except Exception as e:
        logger.error(f"Failed to fetch cookies JSON: {e}.")
    return None,None,None

async def process_product(product, semaphore, amazon_cookie, seller_cookie, db, sales_scraper):
    async with semaphore:
        try:
            ean = product["product_gtin"]
            supplier_price = float(product["supplier_price"])
            supplier_cost = supplier_price * 1.20
            supplier_link = product["product_url"]

            asin = await convert(ean, amazon_cookie)
            logger.info(f"{ean, asin}")

            if not asin:
                logger.warning(
                    f"EAN {ean} skipped: No valid ASIN."
                )
                return

            sc = SellerCentral(asin, seller_cookie)

            product_data = await sc.get_product_data()
            if not product_data:
                logger.warning(f"Failed to get product data for ASIN/EAN: {asin}/{ean}.")            
                return

            title, amazon_link, gl, image_url = product_data

            price_data = await sc.get_price()
            if not price_data:
                logger.warning(f"Failed to get price for ASIN/EAN: {asin}/{ean}.")
                return

            price = price_data

            fees = await sc.get_fees(gl, price)
            if not fees:
                logger.warning(f"Failed to get fees for ASIN/EAN: {asin}/{ean}.")
                return

            profit = price - fees - supplier_cost
            roi = (profit / supplier_cost) * 100
            sas_link = sc.sas_link_gen()

            if roi < 25 or profit < 1:
                logger.info(
                    f"ASIN {asin} skipped: [ROI {roi:.2f}%, Profit {profit:.2f}]."
                )
                return

            sales = await sales_scraper.get_sales(asin)
            if not sales or sales < 5:
                logger.info(f"ASIN {asin} skipped: estimated sales = {sales}.")
                return

            deal = {
                "ean": ean,
                "asin": asin,
                "name": title,
                "supplier_cost": supplier_cost,
                "amazon_price": price,
                "fees": fees,
                "profit": profit,
                "roi": roi,
                "estimated_sales": sales,
                "amazon_link": amazon_link,
                "supplier_link": supplier_link,
                "sas_link": sas_link,
                "image_url": image_url,
            }

            saved = await db.save_deal(deal)

            if saved:
                    logger.info(
                        f"Queued ASIN {asin} [Profit: €{profit:.2f}, ROI: {roi:.2f}%]."
                    )

        except Exception as e:
            logger.exception(
                f"Error processing product {product.get('product_name', 'Unknown')} | {str(e)}."
            )


async def main():
    logger.info("Starting FBA Scanner...")
    products = await fetch_products()
    amazon_cookie, seller_cookie, sas_cookie = await fetch_cookies()
    db = Database()
    await db.reset_db()

    if not products:
        logger.error("No products fetched. Exiting.")
        return

    sales_scraper = SalesScraper(sas_cookie, headless=True)

    await sales_scraper.start()

    semaphore = asyncio.Semaphore(100)
    tasks = [process_product(p, semaphore, amazon_cookie, seller_cookie, db, sales_scraper) for p in products]
    await asyncio.gather(*tasks)

    await sales_scraper.close()

    logger.info("FBA Scanner finished.")


if __name__ == "__main__":
    asyncio.run(main())
