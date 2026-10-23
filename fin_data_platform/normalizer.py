"""Converts vendor-specific raw messages into one unified Tick schema."""
from dataclasses import dataclass, asdict
from datetime import datetime, timezone


@dataclass
class Tick:
    source: str
    symbol: str
    price: float
    volume: float
    ts: str          # ISO-8601 UTC timestamp

    def to_dict(self):
        return asdict(self)


def _from_epoch_seconds(v):
    return datetime.fromtimestamp(float(v), tz=timezone.utc).isoformat(timespec="milliseconds")


def _from_epoch_millis(v):
    return _from_epoch_seconds(float(v) / 1000)


def _from_iso(v):
    return datetime.fromisoformat(v).astimezone(timezone.utc).isoformat(timespec="milliseconds")


def _normalize_stock(raw):
    return Tick("stocks", raw["sym"], float(raw["px"]),
                float(raw["vol"]), _from_epoch_seconds(raw["ts"]))


def _normalize_forex(raw):
    return Tick("forex", raw["pair"].replace("/", "-"), float(raw["rate"]),
                0.0, _from_iso(raw["ts"]))          # forex quotes carry no volume


def _normalize_crypto(raw):
    return Tick("crypto", raw["ticker"], float(raw["last"]),
                float(raw["qty"]), _from_epoch_millis(raw["time"]))


NORMALIZERS = {
    "stocks": _normalize_stock,
    "forex": _normalize_forex,
    "crypto": _normalize_crypto,
}


def normalize(source, raw):
    """Return a validated Tick, or raise ValueError for bad data."""
    try:
        tick = NORMALIZERS[source](raw)
    except KeyError as exc:
        raise ValueError(f"missing or unknown field: {exc}") from exc
    except (TypeError, ValueError) as exc:
        raise ValueError(f"bad value: {exc}") from exc

    # Validation rules
    if tick.price <= 0:
        raise ValueError(f"non-positive price {tick.price}")
    if tick.volume < 0:
        raise ValueError(f"negative volume {tick.volume}")
    return tick
