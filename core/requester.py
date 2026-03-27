# core/requester.py

from typing import Optional
import os
import asyncio
from dotenv import load_dotenv
from curl_cffi.requests import AsyncSession


class Requester:
    def __init__(self, url: str, referrer: Optional[str] = None, cookie: Optional[str] = None, api: Optional[bool] = False, timeout: int = 10):
        self.url = url
        self.session: Optional[AsyncSession] = None
        self.headers = {
            "Accept": "application/json" if api else "*/*",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        }
        if cookie:
            self.headers["Cookie"] = cookie
        if referrer:
            self.headers["Referer"] = referrer

        self.proxy = os.getenv("PROXY")
        self.timeout = timeout

    async def __aenter__(self):
        self.session = AsyncSession(
            headers=self.headers,
            proxy=self.proxy,
            impersonate="chrome142",
            timeout=self.timeout,
            allow_redirects=True,
            http_version="v2"
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def fetch_get(self, retries: int = 1, delay: float = 1.0):
        for attempt in range(1, retries + 1):
            try:
                response = await self.session.get(self.url)
                response.raise_for_status()
                return response
            except Exception as e:
                if attempt < retries:
                    await asyncio.sleep(delay)
        return None

    async def fetch_post(self, data: dict, retries: int = 1, delay: float = 1.0):
        for attempt in range(1, retries + 1):
            try:
                response = await self.session.post(self.url, json=data)
                response.raise_for_status()
                return response
            except Exception as e:
                if attempt < retries:
                    await asyncio.sleep(delay)
        return None
