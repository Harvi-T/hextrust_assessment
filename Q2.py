import asyncio
import websockets
import json
from sortedcontainers import SortedDict

class OrderBook:
    def __init__(self):
        self.bids = SortedDict(lambda x: -x)  # Prices in descending order
        self.asks = SortedDict()  # Prices in ascending order

    def update(self, side, price, quantity):
        if side == 'buy':
            if quantity == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = quantity
        elif side == 'sell':
            if quantity == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = quantity

    def top_levels(self, levels=5):
        return {
            'bids': list(self.bids.items())[:levels],
            'asks': list(self.asks.items())[:levels]
        }

class OrderBookClient:
    def __init__(self, url):
        self.url = url
        self.orderbook = OrderBook()
        self.loop = asyncio.get_event_loop()

    async def connect(self):
        async with websockets.connect(self.url) as websocket:
            await self.subscribe(websocket)
            await self.listen(websocket)

    async def subscribe(self, websocket):
        subscribe_message = json.dumps({
            "method": "SUBSCRIBE",
            "params": ["btcusdt@depth"],
            "id": 1
        })
        await websocket.send(subscribe_message)

    async def listen(self, websocket):
        async for message in websocket:
            data = json.loads(message)
            self.process_message(data)

    def process_message(self, message):
        for update in message.get('b', []):
            price, quantity = float(update[0]), float(update[1])
            self.orderbook.update('buy', price, quantity)
        for update in message.get('a', []):
            price, quantity = float(update[0]), float(update[1])
            self.orderbook.update('sell', price, quantity)
        # Print top 5 levels for debugging
        print(self.orderbook.top_levels())

    def start(self):
        self.loop.run_until_complete(self.connect())

class ResilientOrderBookClient(OrderBookClient):
    async def connect(self):
        while True:
            try:
                async with websockets.connect(self.url) as websocket:
                    await self.subscribe(websocket)
                    await self.listen(websocket)
            except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError) as e:
                print(f"Connection error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)

    def start(self):
        try:
            self.loop.run_until_complete(self.connect())
        except KeyboardInterrupt:
            print("Client stopped.")
        finally:
            tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            self.loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            self.loop.close()

if __name__ == "__main__":
    url = "wss://stream.binance.com:9443/ws/btcusdt@depth"
    client = ResilientOrderBookClient(url)
    client.start()