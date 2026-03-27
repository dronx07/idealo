# core/seller_central.py

from .requester import Requester
from datetime import datetime
import json
from .logger import get_logger

logger = get_logger("SellerCentral")


class SellerCentral:
    def __init__(self, asin: str, cookie: str):
        self.asin = asin
        self.cookie = cookie
        self.country_code = "FR"
        self.locale = "en-GB"

    async def get_product_data(self):
        url = f"https://sellercentral-europe.amazon.com/rcpublic/productmatch?searchKey={self.asin}&countryCode={self.country_code}&locale={self.locale}"
        try:
            async with Requester(
                url=url,
                referrer="https://sellercentral-europe.amazon.com/revcalpublic?mons_sel_locale=en_GB",
                cookie=self.cookie,
                api=True
            ) as scraper:
                output = await scraper.fetch_get()
                
            if not output or not hasattr(output, 'text'):
                logger.error(f"No response for product data of ASIN {self.asin}")
                return None

            data = json.loads(output.text)
            product = data.get("data", {}).get("otherProducts", {}).get("products", [])
            if not product:
                logger.warning(f"No product found for ASIN {self.asin}")
                return None

            product = product[0]
            return product.get("title"), product.get("link"), product.get("gl"), product.get("imageUrl")

        except json.JSONDecodeError:
            logger.exception(f"Failed to parse JSON for product data of ASIN {self.asin}: {output.text if output else 'No output'}")
        except Exception:
            logger.exception(f"Unexpected error fetching product data for ASIN {self.asin}")
        return None

    async def get_price(self):
        url = f"https://sellercentral-europe.amazon.com/rcpublic/getadditionalpronductinfo?countryCode={self.country_code}&asin={self.asin}&fnsku=&searchType=GENERAL&locale={self.locale}"
        try:
            async with Requester(
                url=url,
                referrer="https://sellercentral-europe.amazon.com/revcalpublic?mons_sel_locale=en_GB",
                cookie=self.cookie,
                api=True
            ) as scraper:
                output = await scraper.fetch_get()

            if not output or not hasattr(output, 'text'):
                logger.error(f"No response for price data of ASIN {self.asin}")
                return None

            data = json.loads(output.text)
            price_data = data.get("data", {})
            if not price_data:
                logger.info(f"Price data empty for ASIN {self.asin}, returning default 1")
                return 1.0

            return float(price_data.get("price", {}).get("amount", 0.0))

        except json.JSONDecodeError:
            logger.exception(f"Failed to parse JSON for price of ASIN {self.asin}: {output.text if output else 'No output'}")
        except Exception:
            logger.exception(f"Unexpected error fetching price for ASIN {self.asin}")
        return None

    async def get_fees(self, gl: str, price: float):
        url = f"https://sellercentral-europe.amazon.com/rcpublic/getfees?countryCode={self.country_code}&locale={self.locale}"
        peak = datetime.now().month in [10, 11, 12]

        payload = {
            "countryCode": self.country_code,
            "itemInfo": {
                "asin": self.asin,
                "glProductGroupName": gl,
                "packageLength": "0",
                "packageWidth": "0",
                "packageHeight": "0",
                "dimensionUnit": "",
                "packageWeight": "0",
                "weightUnit": "",
                "afnPriceStr": str(price),
                "mfnPriceStr": str(price),
                "mfnShippingPriceStr": "0",
                "currency": "EUR",
                "isNewDefined": "false"
            },
            "programIdList": ["Core#0","MFN#1"],
            "programParamMap": {}
        }

        try:
            async with Requester(
                url=url,
                referrer="https://sellercentral-europe.amazon.com/revcalpublic?mons_sel_locale=en_GB",
                cookie=self.cookie,
                api=True
            ) as scraper:
                output = await scraper.fetch_post(payload)

            if not output or not hasattr(output, 'text'):
                logger.error(f"No response for fees of ASIN {self.asin}")
                return None

            data = json.loads(output.text)
            core = data.get("data", {}).get("programFeeResultMap", {}).get("Core#0", {})

            if not core:
                logger.warning(f"No Core fee data for ASIN {self.asin}")
                return None

            storage_fee = float(core.get("perUnitPeakStorageFee", {}).get("total", {}).get("amount", 0.0 if peak else 0.0))
            fulfillment_fees = float(core.get("otherFeeInfoMap", {}).get("FulfillmentFee", {}).get("total", {}).get("amount", 0.0))
            fixed_fee = float(core.get("otherFeeInfoMap", {}).get("FixedClosingFee", {}).get("total", {}).get("amount", 0.0))
            referral_fee = float(core.get("otherFeeInfoMap", {}).get("ReferralFee", {}).get("total", {}).get("amount", 0.0))
            variable_fee = float(core.get("otherFeeInfoMap", {}).get("VariableClosingFee", {}).get("total", {}).get("amount", 0.0))
            digital_services_fee = float(core.get("otherFeeInfoMap", {}).get("DigitalServicesFee", {}).get("total", {}).get("amount", 0.0))

            total_cost = storage_fee + fulfillment_fees + fixed_fee + referral_fee + variable_fee + digital_services_fee
            return round(total_cost, 2)

        except json.JSONDecodeError:
            logger.exception(f"Failed to parse JSON for fees of ASIN {self.asin}: {output.text if output else 'No output'}")
        except Exception:
            logger.exception(f"Unexpected error fetching fees for ASIN {self.asin}")
        return None

    def sas_link_gen(self):
        return f"https://sas.selleramp.com/sas/lookup?SasLookup%5Bsearch_term%5D={self.asin}"
