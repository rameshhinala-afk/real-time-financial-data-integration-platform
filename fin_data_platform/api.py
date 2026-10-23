"""Tiny JSON REST API (standard library only) exposing the live data."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs


def make_handler(storage, pipeline):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, payload, code=200):
            body = json.dumps(payload, indent=2).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            url = urlparse(self.path)
            qs = parse_qs(url.query)

            if url.path == "/":
                self._send({"endpoints": ["/health", "/latest", "/history?symbol=AAPL&limit=10",
                                          "/stats", "/alerts", "/rejections"]})
            elif url.path == "/health":
                self._send(pipeline.health())
            elif url.path == "/latest":
                self._send(storage.latest())
            elif url.path == "/history":
                symbol = qs.get("symbol", [None])[0]
                if not symbol:
                    return self._send({"error": "symbol query parameter required"}, 400)
                try:
                    limit = max(1, min(int(qs.get("limit", ["20"])[0]), 500))
                except ValueError:
                    return self._send({"error": "limit must be an integer"}, 400)
                self._send(storage.history(symbol, limit))
            elif url.path == "/stats":
                self._send(storage.stats())
            elif url.path == "/alerts":
                self._send(list(pipeline.alerts))
            elif url.path == "/rejections":
                self._send(list(pipeline.rejections))
            else:
                self._send({"error": "not found"}, 404)

        def log_message(self, *args):    # keep console clean
            pass

    return Handler


def start_api(storage, pipeline, port=8000):
    server = ThreadingHTTPServer(("0.0.0.0", port), make_handler(storage, pipeline))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
