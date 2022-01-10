import websockets
import asyncio
import aiohttp
import requests
import time
import json
import ssl
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s"
)

# Websocket Kucoin Client
class KucoinClient:

    SWITCH_ENDPOINT = "https://api.kucoin.com/api/v1/bullet-public"

    def __init__(self, subs):
        self.subs = subs
        self._ws_info = self._ask_upgrade()
        self._ws_endpoint = self._get_ws_endpoint()

    # HTTP POST request to the server with prompt to upgrade to WS
    def _ask_upgrade(self):
        ws_info = requests.post(self.SWITCH_ENDPOINT).json()
        logging.info("Sending upgrade request to server")
        return ws_info

    # Kucoin requires user to send prompt request first to get data for
    # initialisation of websockets stream
    def _get_ws_endpoint(self):
        endpoint = self._ws_info["data"]["instanceServers"][0]["endpoint"]
        token = self._ws_info["data"]["token"]
        connect_id = str(int(time.time()) * 1000)
        ws_endpoint = endpoint + f"?token={token}&[connectId={connect_id}]"
        return ws_endpoint

    @property
    def _ping_interval(self):
        return self._ws_info["data"]["instanceServers"][0]["pingInterval"] / 1000

    # To keep TCP connection alive client has to ping server each period of time
    async def _ping_serv(self):
        ping_msg = json.dumps({"id": str(int(time.time() * 1000)), "type": "ping"})
        logging.info("Pinging the server")
        await self._ws.ping(ping_msg)
        self._last_ping = time.time()

    # Subscribe to datastream
    async def _apply_for_stream(self):
        for sub in self.subs:
            subscription = json.dumps(
                {
                    "id": str(int(time.time() * 1000)),
                    "type": "subscribe",
                    "topic": sub,
                    "response": True,
                    "privateChannel": False,
                }
            )
            logging.info(f"Applying for subscription: {sub}")
            await self._ws.send(subscription)

    # Start stream listening
    async def _run_ws(self):
        async with websockets.connect(
            self._ws_endpoint, ssl=ssl.SSLContext(protocol=ssl.PROTOCOL_TLS)
        ) as ws:
            self._ws = ws
            # Send hello message to the server to start stream
            await ws.send("Hello World!")
            # Ping server for the first time
            await self._ping_serv()
            # Subscribe to data channels (up to 5 can be tracked)
            await self._apply_for_stream()

            while True:
                data = await asyncio.wait_for(ws.recv(), 5)
                if time.time() - self._last_ping > self._ping_interval:
                    await self._ping_serv()


if __name__ == "__main__":
    client_1 = KucoinClient(["/market/level2:BTC-USDT", "/market/level2:ETH-USDT"])
    client_2 = KucoinClient(["/market/level2:BTC-USDT"])

    async def main():
        loop = asyncio.get_event_loop()
        coros = [client_1._run_ws(), client_2._run_ws()]
        await asyncio.gather(*coros)

    asyncio.run(main())
