import asyncio
import functools
import aiohttp
import asyncpg
import time
import base64
import hmac
import hashlib
import logging
from typing import Callable, List, Any, Optional


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s"
)


# Asynchronous client managing connections to various databases and entitiess
class DataBaseClient:
    pass


class BadStatusException(Exception):
    pass


# Class with variable functions used to create parameters for requests
class KucoinHttpClientUtils:

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


class KucoinHttpClient:

    _base_url = "https://api.kucoin.com"
    _total_n_reqs = 0  # number of successful requests sent to the server
    _n_suc_reqs = 0  # number of all requests to the server
    _n_commit_db = 0  # number of commit to the database
    _chunk_size = 1000  # number of rows to insert at a time with bulk insert

    def __init__(self, KC_API_KEY: str, KC_API_SECRET: str, KC_API_PASSPHRASE: str):
        self.kc_api_key = KC_API_KEY
        self.kc_api_secret = KC_API_SECRET
        self.kc_api_passphrase = KC_API_PASSPHRASE

    def _get_semaphores(self):
        pass

    # Generate headers using request parameters and apikey data
    def _get_headers(self, method: str, endpoint: str) -> dict:
        current_ts = str(int(time.time()) * 1000)
        body = current_ts + method + endpoint
        signiture = base64.b64encode(
            hmac.new(
                self.kc_api_secret.encode("utf-8"),
                body.encode("utf-8"),
                hashlib.sha256,
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

    # Create request coroutine with rps managed by Semaphores
    async def _send_req(
        self, req: str, semaphore: asyncio.Semaphore
    ) -> aiohttp.ClientResponse.json:

        method, endpoint = req.split(" ")
        signed_headers = self._get_headers(method, endpoint)

        async with semaphore:

            self._total_n_reqs += 1

            logging.info(f"Sending request #{self._total_n_reqs} to {req}")

            send_req_coro = self._session.request(
                method=method,
                url=self._base_url + endpoint,
                headers=signed_headers,
                ssl=False,
            )
            resp = await send_req_coro

            if resp.ok:  # check if status is 200 otherwise raise BadStatusException
                self._n_suc_reqs += 1
                await asyncio.sleep(1)
                return await resp.json()
            else:
                logging.exception("Cancelling current task and start a new one")
                raise BadStatusException(f"Got status {resp.status}")

    # Send async queries to database
    async def _execute_query(self, query: str, data: List[List[Any]]) -> None:
        logging.info(f"Committing query to database: {query}")
        self._n_commit_db += 1
        n_chunks = len(data) // self._chunk_size + 1
        for i in range(n_chunks):
            chunk = data[i * self._chunk_size : (i + 1) * self._chunk_size]
            await self._conn.executemany(query, chunk)

    # Function parses list of requests with same parsing and writing logic
    async def parse_reqs(
        self,
        reqs: List[str],  # requests to fetch
        queries: List[str],  # corresponding queries to database
        parser: Callable,  # fuction that will convert response to rows
        rps: Optional[int] = 3,  # limit on requests per second
    ) -> None:

        async with aiohttp.ClientSession() as session:

            self._conn = await asyncpg.create_pool(
                "postgresql://127.0.0.1:5432/postgres"
            )

            logging.info("Starting execution of parser")

            self._session = session
            semaphore = asyncio.Semaphore(rps)

            tasks_map = {
                asyncio.ensure_future(
                    self._send_req(req, semaphore)
                ): functools.partial(self._send_req, req, semaphore)
                for req in reqs
            }

            pending_tasks = set(tasks_map.keys())

            while pending_tasks:
                finished, pending_tasks = await asyncio.wait(
                    pending_tasks, return_when=asyncio.FIRST_EXCEPTION
                )  # --> finished tasks with results in task.result and pending tasks
                for task in finished:
                    if task.exception():
                        # pinpoint the coro that raised BadStatusException
                        coro = tasks_map[task]
                        new_task = asyncio.ensure_future(coro())
                        tasks_map[new_task] = coro
                        # append failed task to pending tasks
                        pending_tasks.add(new_task)
                    else:
                        response = task.result()  # --> dict => we need various parsers
                        data = parser(response)
                        for query, data in zip(queries, data):
                            await self._execute_query(query, data)

    def __repr__(self):
        repr_text = (
            "----------------------------\n"
            "---------ParserInfo---------\n"
            f"Requests: {self._total_n_reqs}\n"
            f"Successful: {self._n_suc_reqs}\n"
            f"Requests to database: {self._n_commit_db}\n"
            "----------------------------"
        )
        return repr_text


if __name__ == "__main__":

    KC_API_KEY = "61cb5565e099b20001c9587c"
    KC_API_SECRET = "ec418fb6-02f0-475f-86f9-8b365d61e6fc"
    KC_API_PASSPHRASE = "kcpass228"

    client = KucoinHttpClient(KC_API_KEY, KC_API_SECRET, KC_API_PASSPHRASE)

    reqs_btc = KucoinHttpClientUtils.get_kline_reqs(
        "BTC-USDT", 1621694003, 1641727723, "1min"
    )

    async def main():
        task = asyncio.create_task(
            client.parse_reqs(
                reqs=reqs_btc,
                queries=["INSERT INTO klines VALUES ($1, $2, $3, $4, $5, $6, $7)"],
                parser=KucoinHttpClientUtils.kline_parser,
                rps=10,
            )
        )
        await asyncio.gather(task)

    asyncio.run(main())

    # Print client to check execution info
    print(client)
