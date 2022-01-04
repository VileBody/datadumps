import time
import hashlib
import hmac
import base64
from datetime import datetime
import time
import asyncio
import aiohttp
import json
import asyncpg


BASE_URL = "https://api.kucoin.com"
KC_API_KEY = "61cb5565e099b20001c9587c"
KC_API_SECRET = "ec418fb6-02f0-475f-86f9-8b365d61e6fc"
KC_API_PASSPHRASE = "kcpass228"


class KucoinClient:
    def __init__(self, kc_api_key, kc_api_secret, kc_api_passphrase):
        self.kc_api_key = kc_api_key
        self.kc_api_secret = kc_api_secret
        self.kc_api_passphrase = kc_api_passphrase
        self.limit = asyncio.Semaphore(10)

    def _create_header(self, method, endpoint):
        current_ts = str(int(time.time()) * 1000)
        body = current_ts + method + endpoint
        signiture = base64.b64encode(
            hmac.new(
                self.kc_api_secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256
            ).digest()
        ).decode("utf-8")

        passphrase = base64.b64encode(
            hmac.new(
                self.kc_api_secret.encode("utf-8"),
                self.kc_api_passphrase.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")

        header = {
            "KC-API-KEY": self.kc_api_key,
            "KC-API-SIGN": signiture,
            "KC-API-TIMESTAMP": current_ts,
            "KC-API-PASSPHRASE": passphrase,
            "KC-API-KEY-VERSION": "2",
        }
        return header

    async def _insert_value(self, symbol):
        await self.db.fetch("INSERT INTO tickers VALUES ($1)", symbol)
        await asyncio.sleep(0.1)

    async def _async_request(self, session, http_request):
        method, endpoint = http_request.split(" ")
        header = self._create_header(method, endpoint)
        async with self.limit:
            resp = await session.request(
                method=method, url=BASE_URL + endpoint, headers=header, ssl=False
            )
            resp = json.loads(await resp.text())["data"]
            coros = [self._insert_value(el["symbol"]) for el in resp]
            await asyncio.gather(*coros)

    def async_call_api(self, http_requests):
        async def get_coros(http_requests):
            self.db = await asyncpg.create_pool("postgresql://127.0.0.1:5432/postgres")
            async with aiohttp.ClientSession() as session:
                coros = [
                    self._async_request(session, http_request)
                    for http_request in http_requests
                ]
                res = await asyncio.gather(*coros)
            return res

        loop = asyncio.get_event_loop()
        data = loop.run_until_complete(get_coros(http_requests))
        return data


if __name__ == "__main__":
    client = KucoinClient(KC_API_KEY, KC_API_SECRET, KC_API_PASSPHRASE)
    start = time.perf_counter()
    data = client.async_call_api(
        ["GET /api/v1/symbols"] * 20,
    )
    finish = time.perf_counter()
    print(f"Time taken to execute {finish-start}")
