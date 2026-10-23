"""Real-time ingestion pipeline.

  feeds (producers) --> asyncio.Queue --> consumer: normalize -> validate
                                          -> store -> price-move alerts
"""
import asyncio
from collections import deque
from datetime import datetime, timezone

from normalizer import normalize

ALERT_PCT = 0.30   # alert when price moves more than this % between ticks


class Pipeline:
    def __init__(self, feeds, storage, on_tick=None, queue_size=1000):
        self.feeds = feeds
        self.storage = storage
        self.on_tick = on_tick
        self.queue = asyncio.Queue(maxsize=queue_size)
        self.last_price = {}
        self.alerts = deque(maxlen=50)
        self.metrics = {"received": 0, "stored": 0, "rejected": 0}
        self.rejections = deque(maxlen=20)
        self.started_at = datetime.now(timezone.utc)

    # ---------- producers ----------
    async def _produce(self, feed):
        async for raw in feed.stream():
            await self.queue.put((feed.name, raw))
            self.metrics["received"] += 1

    # ---------- consumer ----------
    async def _consume(self):
        while True:
            source, raw = await self.queue.get()
            try:
                tick = normalize(source, raw)
            except ValueError as exc:
                self.metrics["rejected"] += 1
                self.rejections.append({"source": source, "error": str(exc)})
            else:
                self.storage.insert_tick(tick)
                self.metrics["stored"] += 1
                self._check_alert(tick)
                if self.on_tick:
                    self.on_tick(tick)
            finally:
                self.queue.task_done()

    def _check_alert(self, tick):
        prev = self.last_price.get(tick.symbol)
        self.last_price[tick.symbol] = tick.price
        if prev:
            move = (tick.price - prev) / prev * 100
            if abs(move) >= ALERT_PCT:
                self.alerts.append({
                    "symbol": tick.symbol,
                    "move_pct": round(move, 3),
                    "from": prev, "to": tick.price, "ts": tick.ts,
                })

    # ---------- lifecycle ----------
    async def run(self, duration=0):
        """Run for `duration` seconds (0 = until cancelled)."""
        producers = [asyncio.create_task(self._produce(f)) for f in self.feeds]
        consumer = asyncio.create_task(self._consume())
        try:
            if duration:
                await asyncio.sleep(duration)
            else:
                await asyncio.Event().wait()
        finally:
            for t in producers:
                t.cancel()
            await asyncio.gather(*producers, return_exceptions=True)
            await self.queue.join()          # drain anything still queued
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)

    def health(self):
        uptime = (datetime.now(timezone.utc) - self.started_at).total_seconds()
        return {"status": "ok", "uptime_seconds": round(uptime, 1), **self.metrics}
