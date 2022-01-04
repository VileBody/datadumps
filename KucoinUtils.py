import re
import requests
import time
import hashlib
import hmac
import base64
import json
import pickle
from datetime import datetime
import time
import asyncio
import aiohttp


BASE_URL = "https://api.kucoin.com"


class KucoinMasterClient:

    base_url = "https://api.kucoin.com"

    def __init__(self, kc_api_key, kc_api_secret, kc_api_passphrase):
        self.kc_api_key = kc_api_key
        self.kc_api_secret = kc_api_secret
        self.kc_api_passphrase = kc_api_passphrase

    def _create_header(self, method, endpoint):
        current_ts = str(int(time.time()) * 1000)
        body = current_ts + method + endpoint
        signiture = base64.b64encode(
            hmac.new(
                self.kc_api_secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256
            ).digest()
        )

        passphrase = base64.b64encode(
            hmac.new(
                self.kc_api_secret.encode("utf-8"),
                self.kc_api_passphrase.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        )

        header = {
            "KC-API-KEY": self.kc_api_key,
            "KC-API-SIGN": signiture,
            "KC-API-TIMESTAMP": current_ts,
            "KC-API-PASSPHRASE": passphrase,
            "KC-API-KEY-VERSION": "2",
        }
        return header

    # def call_api(self, method, endpoint, params={}):
    #     headers = self._create_header(method, endpoint)
    #     response = requests.request(
    #         method, self.base_url + endpoint, headers=headers, params=params
    #     )
    #     return response.textz


class KucoinAsyncClient:

    base_url = "https://api.kucoin.com"

    def __init__(self, kc_api_key, kc_api_secret, kc_api_passphrase, limit, rate):
        self.kc_api_key = kc_api_key
        self.kc_api_secret = kc_api_secret
        self.kc_api_passphrase = kc_api_passphrase
        self.loop = asyncio.get_event_loop()
        self.limit = asyncio.Semaphore(limit)
        self.rate = rate

    def _create_header(self, method, endpoint):
        current_ts = str(int(time.time()) * 1000)
        body = current_ts + method + endpoint
        signiture = base64.b64encode(
            hmac.new(
                self.kc_api_secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256
            ).digest()
        )

        passphrase = base64.b64encode(
            hmac.new(
                self.kc_api_secret.encode("utf-8"),
                self.kc_api_passphrase.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        )

        header = {
            "KC-API-KEY": self.kc_api_key,
            "KC-API-SIGN": signiture,
            "KC-API-TIMESTAMP": current_ts,
            "KC-API-PASSPHRASE": str(passphrase),
            "KC-API-KEY-VERSION": "2",
        }
        return header

    async def _async_request(self, session, method, endpoint, header, params={}):
        async with self.limit:
            response = await session.request(
                method=method,
                url=self.base_url + endpoint,
                headers=header,
                params=params,
            )
            await asyncio.sleep(self.rate)
            return await response.text()

    async def call_api(self, methods, endpoints):
        async def gen_coroutines(methods, endpoints):
            async with aiohttp.ClientSession() as session:
                # Подписанные headerы приватными ключами от api
                signed_headers = [
                    self._create_header(method, endpoint)
                    for method, endpoint in zip(methods, endpoints)
                ]
                # Создание асинхронных корутин для выполнения
                coros = [
                    self._async_request(session, method, endpoint, header)
                    for method, endpoint, header in zip(
                        methods, endpoints, signed_headers
                    )
                ]
                res = await asyncio.gather(*coros)
                return res

        data = self.loop.run_until_complete(gen_coroutines(methods, endpoints, headers))
        return data


class KucoinTools:
    def __init__(self, client):
        self.api_calls = self.load_api_calls()
        self.client = client

    def load_api_calls(self):
        with open("kucoin_api_calls.json", "r") as file:
            api_calls = json.load(file)
        return api_calls

    def get_trading_pairs(self):
        method, endpoint = self.api_calls["get_trading_pairs"]["call"].split(" ")
        data = self.client.call_api(method, endpoint)
        return data

    # symbol - параметр-наименование трэйд пары
    def get_24hr_stats(self, symbol="BTC-USDT"):
        method, endpoint = self.api_calls["get_24hr_stats"]["call"].split(" ")
        data = self.client.call_api(method, endpoint, {"symbol": symbol})
        return data

    def get_order_book(self, symbol="BTC-USDT"):
        method, endpoint = self.api_calls["get_order_book"]["call"].split(" ")
        endpoint += f"?symbol={symbol}"
        data = self.client.call_api(method, endpoint)
        return data

    def get_trade_history(self, symbol="BTC-USDT"):
        method, endpoint = self.api_calls["get_trade_history"]["call"].split(" ")
        data = self.client.call_api(method, endpoint, {"symbol": symbol})
        return data

    # type - интервал свечей (1min, 3min, 5min, 15min, 30min,
    # 1hour, 2hour, 4hour, 6hour, 8hour, 12hour, 1day, 1week)
    # startAt, endAt - интервал вернутых данных
    # Метод за раз возвращает 1500 данных
    def get_klines(
        self, startAt="1566703297", endAt="1566789757", symbol="BTC-USDT", type="5min"
    ):
        method, endpoint = self.api_calls["get_klines"]["call"].split(" ")
        data = self.client.call_api(
            method,
            endpoint
        )
        return data


if __name__ == "__main__":
    kmc = KucoinAsyncClient(
        "61cb5565e099b20001c9587c",
        "ec418fb6-02f0-475f-86f9-8b365d61e6fc",
        "kcpass228",
        5,
        1,
    )
    headers = kmc._create_header('GET', '/api/v3/market/orderbook/level2?symbol=BTC-USDT')
    resp = requests.get(BASE_URL + "/api/v3/market/orderbook/level2?symbol=BTC-USDT", headers=headers)
    print(resp.text)
