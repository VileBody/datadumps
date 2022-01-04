import time
import hashlib
import hmac
import base64
import time
import asyncio
import aiohttp
import json
import asyncpg
from datetime import datetime


BASE_URL = "https://api.kucoin.com"
KC_API_KEY = "61cb5565e099b20001c9587c"
KC_API_SECRET = "ec418fb6-02f0-475f-86f9-8b365d61e6fc"
KC_API_PASSPHRASE = "kcpass228"


class KucoinClient:

    kucoin_db_loc = "postgresql://127.0.0.1:5432/postgres"

    def __init__(self, kc_api_key, kc_api_secret, kc_api_passphrase):
        self.kc_api_key = kc_api_key
        self.kc_api_secret = kc_api_secret
        self.kc_api_passphrase = kc_api_passphrase
        self.limit = asyncio.Semaphore(5)

    # Signing headers with api_secret
    def create_header(self, method, endpoint):
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

    # Asynchronous http-request to endpoint returns awaitable response text
    async def async_request(self, session, http_request, request_type):
        method, endpoint = http_request.split(" ")
        signed_headers = self.create_header(method, endpoint)
        async with self.limit:
            response = await session.request(
                method=method,
                url=BASE_URL + endpoint,
                headers=signed_headers,
                ssl=False,
            )
        return await response.text()

    # async def insert_into_db(self, query, chunksize, *args):
    #     num_chunks = len(args[0]) // chunksize + 1
    #     for i in range(num_)


client = KucoinClient(KC_API_KEY, KC_API_SECRET, KC_API_PASSPHRASE)
