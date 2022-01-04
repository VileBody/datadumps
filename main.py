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

    # Function managing connections to databases
    async def connect_to_database(self):
        self.conn = await asyncpg.create_pool(self.kucoin_db_loc)

    # Asynchronous data parsing
    async def parse_response(self, response):
        data = json.loads(response)["data"]
        if ''
        return await self.parse_orderbook(data)

    async def parse_orderbook(self, data):
        timestamp, bids, asks = data["time"], data["bids"], data["asks"]
        timestamp = [timestamp] * len(bids)
        bids_prices, bids_amounts = zip(*bids)
        asks_prices, asks_amounts = zip(*asks)
        return timestamp, bids_prices, bids_prices, asks_prices, asks_amounts

    # Asynchronous insertion of data into database
    async def multiple_insert_query(self, query, *args):
        rows = list(zip(*args))
        chunksize = 1000
        num_chunks = int(len(args[0]) / chunksize) + 1
        for i in range(num_chunks):
            chunk = [row for row in rows[i * chunksize : (i + 1) * chunksize]]
            await self.conn.executemany(query, chunk)

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
        return await response.text(), http_request

    # Creating tasks and iterating through them as they are ready
    async def async_api_call(self, http_requests, request_types):

        async with aiohttp.ClientSession() as session:
            await asyncio.create_task(self.connect_to_database())
            # Create tasks to iterate through
            tasks = [
                asyncio.create_task(self.async_request(session, http_request))
                for http_request in http_requests
            ]
            # As responses come in, parse them and query to database in chunks
            for output in asyncio.as_completed(tasks):
                data = await self.parse_response(*await output)

                await self.multiple_insert_query(
                    "INSERT INTO orderbook VALUES ($1, $2, $3, $4, $5)", *data
                )

    def start(self, http_requests):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.async_api_call(http_requests))


if __name__ == "__main__":
    client = KucoinClient(KC_API_KEY, KC_API_SECRET, KC_API_PASSPHRASE)
    start = time.perf_counter()
    client.start(["GET /api/v3/market/orderbook/level2?symbol=BTC-USDT"] * 20)
    end = time.perf_counter()
    print("Time taken to execute {:.2f}".format(end - start))
