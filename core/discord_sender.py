# core/discord_sender.py

import aiohttp
from .logger import get_logger

logger = get_logger("DiscordSender")


class DiscordSender:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    @staticmethod
    def roi_color(roi: float) -> int:
        if roi >= 60:
            return 0x00FF00
        elif roi >= 40:
            return 0x2ECC71
        elif roi >= 25:
            return 0xF1C40F
        else:
            return 0xE74C3C

    async def send_deal(self, deal: dict):
        embed = {
            "title": f"**{deal['name']}**",
            "color": self.roi_color(deal['roi']),
            "thumbnail": {
                "url": f"{deal['image_url']}"
            },
            "fields": [
                {"name": "EAN", "value": deal['ean'], "inline": False},
                {"name": "ASIN", "value": deal['asin'], "inline": False},
                {"name": "Supplier (incl VAT)", "value": f"€{deal['supplier_cost']:.2f}", "inline": False},
                {"name": "Amazon Price", "value": f"€{deal['amazon_price']:.2f}", "inline": False},
                {"name": "Fees + FBA", "value": f"€{deal['fees']:.2f}", "inline": False},
                {"name": "Profit", "value": f"€{deal['profit']:.2f}", "inline": False},
                {"name": "ROI", "value": f"{deal['roi']:.2f}%", "inline": False},
                {"name": "Est. Monthly Sales", "value": f"{deal['estimated_sales']}", "inline": False},
                {
                    "name": "Links",
                    "value": f"[Amazon]({deal['amazon_link']}) | [Qogita]({deal['supplier_link']}) | [SAS]({deal['sas_link']})",
                    "inline": False,
                },
            ],
        }

        payload = {"embeds": [embed]}

        async with aiohttp.ClientSession() as session:
            async with session.post(self.webhook_url, json=payload) as response:
                if response.status in (200, 204):
                    logger.info(f"Posted ASIN {deal['asin']} to Discord.")
                    return True
                else:
                    text = await response.text()
                    logger.error(f"Discord error: {response.status} - {text}")
                    return False
