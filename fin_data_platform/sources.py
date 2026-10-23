"""Simulated market-data feeds.

Each feed emits raw messages in its OWN format (field names, types and
timestamp style differ) - exactly like real vendors. The normalizer turns
them into one common schema. About 3% of messages are deliberately corrupt
to demonstrate validation.
"""
import asyncio
import random
import time
from datetime import datetime, timezone


class BaseFeed:
    name = "base"
    symbols = {}          # symbol -> starting price
    volatility = 0.001    # std-dev of each random-walk step

    def __init__(self, interval=0.5, seed=None):
        self.interval = interval
        self.rng = random.Random(seed)
        self.prices = dict(self.symbols)

    def _step(self, symbol):
        """Random-walk the price of a symbol and return the new price."""
        change = self.rng.gauss(0, self.volatility)
        self.prices[symbol] = max(0.0001, self.prices[symbol] * (1 + change))
        return self.prices[symbol]

    def make_message(self):
        raise NotImplementedError

    async def stream(self):
        """Async generator yielding raw messages forever."""
        while True:
            await asyncio.sleep(self.interval * self.rng.uniform(0.5, 1.5))
            yield self.make_message()


class StockFeed(BaseFeed):
    name = "stocks"
    symbols = {"AAPL": 189.0, "MSFT": 415.0, "TSLA": 250.0, "INFY": 18.5}
    volatility = 0.0015

    def make_message(self):
        sym = self.rng.choice(list(self.symbols))
        px = round(self._step(sym), 2)
        if self.rng.random() < 0.03:
            px = -px                                  # corrupt message
        return {"sym": sym, "px": px,
                "vol": self.rng.randint(10, 500),
                "ts": time.time()}                    # epoch seconds


class ForexFeed(BaseFeed):
    name = "forex"
    symbols = {"EUR/USD": 1.085, "USD/INR": 83.2, "GBP/USD": 1.27}
    volatility = 0.0004

    def make_message(self):
        pair = self.rng.choice(list(self.symbols))
        rate = round(self._step(pair), 5)
        if self.rng.random() < 0.03:
            rate = None                               # corrupt message
        return {"pair": pair, "rate": rate,
                "ts": datetime.now(timezone.utc).isoformat()}   # ISO string


class CryptoFeed(BaseFeed):
    name = "crypto"
    symbols = {"BTC-USD": 67000.0, "ETH-USD": 3200.0}
    volatility = 0.003

    def make_message(self):
        ticker = self.rng.choice(list(self.symbols))
        last = round(self._step(ticker), 2)
        msg = {"ticker": ticker, "last": str(last),   # price sent as a string
               "qty": round(self.rng.uniform(0.01, 2.0), 4),
               "time": int(time.time() * 1000)}       # epoch milliseconds
        if self.rng.random() < 0.03:
            msg.pop("last")                           # corrupt message
        return msg


def default_feeds(interval=0.5):
    return [StockFeed(interval), ForexFeed(interval), CryptoFeed(interval)]
