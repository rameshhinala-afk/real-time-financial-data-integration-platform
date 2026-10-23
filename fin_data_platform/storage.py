"""SQLite storage for normalized ticks (thread-safe for the API thread)."""
import sqlite3
import threading


class Storage:
    def __init__(self, path=":memory:"):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        with self.lock:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS ticks (
                    id     INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    price  REAL NOT NULL,
                    volume REAL NOT NULL,
                    ts     TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_ticks_symbol ON ticks(symbol, id);
            """)

    def insert_tick(self, tick):
        with self.lock:
            self.conn.execute(
                "INSERT INTO ticks (source, symbol, price, volume, ts) VALUES (?,?,?,?,?)",
                (tick.source, tick.symbol, tick.price, tick.volume, tick.ts),
            )
            self.conn.commit()

    def latest(self):
        """Most recent tick for every symbol."""
        with self.lock:
            rows = self.conn.execute("""
                SELECT t.source, t.symbol, t.price, t.volume, t.ts
                FROM ticks t
                JOIN (SELECT symbol, MAX(id) AS mid FROM ticks GROUP BY symbol) m
                  ON t.id = m.mid
                ORDER BY t.source, t.symbol
            """).fetchall()
        return [dict(r) for r in rows]

    def history(self, symbol, limit=20):
        with self.lock:
            rows = self.conn.execute(
                """SELECT source, symbol, price, volume, ts FROM ticks
                   WHERE symbol = ? ORDER BY id DESC LIMIT ?""",
                (symbol, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def stats(self):
        """Per-symbol summary statistics."""
        with self.lock:
            rows = self.conn.execute("""
                SELECT source, symbol, COUNT(*) AS ticks,
                       ROUND(MIN(price), 5) AS low,
                       ROUND(MAX(price), 5) AS high,
                       ROUND(AVG(price), 5) AS avg_price,
                       ROUND(SUM(volume), 4) AS total_volume
                FROM ticks GROUP BY source, symbol ORDER BY source, symbol
            """).fetchall()
        return [dict(r) for r in rows]

    def count(self):
        with self.lock:
            return self.conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
