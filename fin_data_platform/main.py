"""Real-Time Financial Data Integration Platform - entry point.

Usage:
    python main.py                    # run 20 seconds
    python main.py --duration 0       # run until Ctrl+C
    python main.py --port 9000 --interval 0.2
"""
import argparse
import asyncio

from api import start_api
from pipeline import Pipeline
from sources import default_feeds
from storage import Storage


def print_tick(tick):
    print(f"{tick.ts[11:23]}  {tick.source:<7} {tick.symbol:<8} "
          f"{tick.price:>12,.4f}  vol={tick.volume:g}")


def main():
    parser = argparse.ArgumentParser(description="Real-Time Financial Data Integration Platform")
    parser.add_argument("--duration", type=int, default=20, help="seconds to run (0 = forever)")
    parser.add_argument("--interval", type=float, default=0.5, help="avg seconds between messages per feed")
    parser.add_argument("--port", type=int, default=8000, help="REST API port")
    parser.add_argument("--db", default=":memory:", help="SQLite path (default: in-memory)")
    args = parser.parse_args()

    storage = Storage(args.db)
    pipeline = Pipeline(default_feeds(args.interval), storage, on_tick=print_tick)
    start_api(storage, pipeline, args.port)

    print(f"REST API: http://localhost:{args.port}/   (try /latest, /stats, /alerts)")
    print("Streaming from 3 feeds: stocks, forex, crypto\n")

    try:
        asyncio.run(pipeline.run(args.duration))
    except KeyboardInterrupt:
        print("\nStopped by user.")

    # ---- summary ----
    m = pipeline.metrics
    print("\n=== Pipeline summary ===")
    print(f"received={m['received']}  stored={m['stored']}  rejected={m['rejected']}")

    print("\n=== Latest prices ===")
    for t in storage.latest():
        print(f"{t['source']:<7} {t['symbol']:<8} {t['price']:>12,.4f}")

    print(f"\n=== Price-move alerts (>= {0.30}%) ===")
    alerts = list(pipeline.alerts)
    for a in alerts[-5:]:
        print(f"{a['symbol']:<8} {a['move_pct']:+.2f}%  {a['from']:.4f} -> {a['to']:.4f}")
    if not alerts:
        print("none")


if __name__ == "__main__":
    main()
