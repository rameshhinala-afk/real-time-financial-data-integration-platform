# Real-Time Financial Data Integration Platform

A simple base project that ingests live market data from several sources that
use different formats, converts everything into one unified schema, stores it,
and serves it through a REST API in real time.

## Architecture

```
 StockFeed  ─┐
 ForexFeed  ─┼─> asyncio.Queue ─> Normalizer ─> Validator ─> SQLite ─> REST API
 CryptoFeed ─┘                                       │
                                                     └─> price-move alerts
```

| File            | Purpose                                                          |
|-----------------|------------------------------------------------------------------|
| `sources.py`    | Simulated feeds, each with its own message format + corrupt data |
| `normalizer.py` | Maps vendor formats to a common `Tick` and validates it          |
| `storage.py`    | Thread-safe SQLite store (latest, history, stats)                |
| `pipeline.py`   | Async producers/consumer, metrics, alerts, rejection log         |
| `api.py`        | JSON REST API using only the standard library                    |
| `main.py`       | Entry point and console summary                                  |

## Run

No third-party packages are needed (Python 3.9+).

```bash
python main.py                  # stream for 20 seconds
python main.py --duration 0     # run until Ctrl+C
```

While it runs:

| Endpoint                        | Returns                              |
|---------------------------------|--------------------------------------|
| `/health`                       | Uptime and pipeline counters         |
| `/latest`                       | Latest tick for every symbol         |
| `/history?symbol=AAPL&limit=10` | Recent ticks for a symbol            |
| `/stats`                        | Count, low, high, average per symbol |
| `/alerts`                       | Price moves above the threshold      |
| `/rejections`                   | Recent invalid messages and why      |

## Unified schema

`source, symbol, price, volume, ts` — with `ts` as ISO-8601 UTC and symbols
formatted consistently (e.g. `EUR-USD`, `BTC-USD`, `AAPL`).

## Ideas for extending

- Replace the simulated feeds with real WebSocket/REST APIs (Binance, Alpha Vantage, etc.)
- Swap the queue for Kafka or Redis Streams
- Add WebSocket push to dashboards, or a live chart UI
- Use PostgreSQL/TimescaleDB for production-scale time-series storage
- Add unit tests and a retry/back-off layer for feed failures
