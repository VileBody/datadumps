from agents import HeaderManager
from typing import Callable, List, Any, Optional

import asyncio
import functools
import aiohttp
import asyncpg
import logging
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s"
)


class BadStatusException(Exception):
    pass


class HttpClient:

    _total_n_reqs = 0  # number of successful requests sent to the server
    _n_suc_reqs = 0  # number of all requests to the server
    _n_commit_db = 0  # number of commit to the database
    _chunk_size = 1000  # number of rows to insert at a time with bulk insert
    _header_manager = HeaderManager()  # enables user-agent rotation

    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase

    def _get_semaphores(self):
        pass

    # Create request coroutine with rps managed by Semaphores
    async def _send_req(
        self, req: str, semaphore: asyncio.Semaphore
    ) -> aiohttp.ClientResponse.json:

        method, endpoint = req.split(" ")
        signed_headers = self._get_headers(method, endpoint)
        signed_headers["User-Agent"] = self._header_manager.rotate_agent()

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
                        print(task.exception())
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
