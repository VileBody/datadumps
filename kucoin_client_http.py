from http_client import HttpClient

import base64
import hmac
import hashlib
import time
import asyncio

from typing import List, Any


# Class with variable functions used to create parameters for requests
class KucoinClientUtils:

    _tick_to_sec_map = {
        "1min": 60,
        "3min": 180,
        "5min": 300,
        "15min": 900,
        "30min": 1800,
        "60min": 3600,
    }

    # --Example-- type=1min&symbol=BTC-USDT&startAt=1566703297&endAt=1566789757
    @classmethod
    def get_kline_reqs(
        cls, symbol: str, startAt: int, endAt: int, tick: str, n_klines_pr=1500
    ) -> list:
        kline_reqs = []
        master_interval = endAt - startAt
        small_interval = n_klines_pr * cls._tick_to_sec_map[tick]
        n_reqs = master_interval // small_interval + 1
        for i in range(n_reqs):
            loc_start = startAt + i * small_interval
            loc_end = startAt + (i + 1) * small_interval
            req = f"GET /api/v1/market/candles?type={tick}&symbol={symbol}&startAt={loc_start}&endAt={loc_end}"
            kline_reqs.append(req)
        return kline_reqs

    @classmethod
    def kline_parser(cls, response: dict) -> List[List[Any]]:
        return [response["data"]]

    @classmethod
    def orderbook_parser(cls, response: dict) -> List[List[List[Any]]]:
        data = response["data"]
        timestamp = data["time"]
        sequence = data["sequence"]  # used to match socket data to snapshot
        asks, bids = data["asks"], data["bids"]
        return [asks, bids]


class KucoinHttpClient(HttpClient):

    _base_url = "https://api.kucoin.com"

    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        super().__init__(api_key, api_secret, api_passphrase)

    def _get_headers(self, method: str, endpoint: str) -> dict:
        current_ts = str(int(time.time()) * 1000)
        body = current_ts + method + endpoint
        signiture = base64.b64encode(
            hmac.new(
                self.api_secret.encode("utf-8"),
                body.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")

        passphrase = base64.b64encode(
            hmac.new(
                self.api_secret.encode("utf-8"),
                self.api_passphrase.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")

        header = {
            "KC-API-KEY": self.api_key,
            "KC-API-SIGN": signiture,
            "KC-API-TIMESTAMP": current_ts,
            "KC-API-PASSPHRASE": passphrase,
            "KC-API-KEY-VERSION": "2",
        }
        return header


class OkexHttpClient(HttpClient):

    _base_url = "https://www.okex.com"

    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        super().__init__(api_key, api_secret, api_passphrase)

    def _get_headers(self, method, endpoint):
        current_ts = str(int(time.time()) * 1000)
        body = current_ts + method + endpoint

        signiture = base64.b64encode(
            hmac.new(
                body.encode("utf-8"),
                self.api_secret.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")

        headers = {
            "OK-ACCESS-KEY": self.api_secret,
            "OK-ACCESS-SIGN": signiture,
            "OK-ACCESS-TIMESTAMP": current_ts,
            "OK-ACCESS-PASSPHRASE": self.api_passphrase,
        }

        return headers


if __name__ == "__main__":

    KC_API_KEY = "61cb5565e099b20001c9587c"
    KC_API_SECRET = "ec418fb6-02f0-475f-86f9-8b365d61e6fc"
    KC_API_PASSPHRASE = "kcpass228"

    client = KucoinHttpClient(KC_API_KEY, KC_API_SECRET, KC_API_PASSPHRASE)

    reqs_btc = KucoinClientUtils.get_kline_reqs(
        "BTC-USDT", 1621694003, 1641727723, "1min"
    )

    async def main():
        task = asyncio.create_task(
            client.parse_reqs(
                reqs=reqs_btc,
                queries=["INSERT INTO klines VALUES ($1, $2, $3, $4, $5, $6, $7)"],
                parser=KucoinClientUtils.kline_parser,
                rps=20,
            )
        )
        await asyncio.gather(task)

    asyncio.run(main())

    # Print client to check execution info
    print(client)
