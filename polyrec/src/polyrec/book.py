"""In-memory order book for one outcome token. Prices are held as integers (1e-4 USDC)."""
from __future__ import annotations

NAN = float("nan")
SCALE = 10_000


def to_int(price) -> int:
    return round(float(price) * SCALE)


class Book:
    __slots__ = ("bids", "asks", "src", "upd_ns")

    def __init__(self):
        self.bids: dict[int, float] = {}
        self.asks: dict[int, float] = {}
        self.src: str | None = None  # "ws", "rest", or None when the state is unknown
        self.upd_ns = 0

    def snapshot(self, bids, asks, src: str, ns: int) -> None:
        self.bids = {to_int(l["price"]): float(l["size"]) for l in bids if float(l["size"]) > 0}
        self.asks = {to_int(l["price"]): float(l["size"]) for l in asks if float(l["size"]) > 0}
        self.src, self.upd_ns = src, ns

    def apply(self, is_bid: bool, price, size, ns: int) -> None:
        side, p, s = (self.bids if is_bid else self.asks), to_int(price), float(size)
        if s > 0:
            side[p] = s
        else:
            side.pop(p, None)
        self.upd_ns = ns

    def invalidate(self) -> None:
        self.bids, self.asks, self.src = {}, {}, None

    def best(self) -> tuple[float, float, float, float]:
        """(best bid, size at best bid, best ask, size at best ask); NaN where a side is empty."""
        bb = max(self.bids) if self.bids else None
        ba = min(self.asks) if self.asks else None
        return (
            bb / SCALE if bb is not None else NAN,
            self.bids[bb] if bb is not None else NAN,
            ba / SCALE if ba is not None else NAN,
            self.asks[ba] if ba is not None else NAN,
        )

    def top(self, n: int) -> tuple[list[float], list[float], list[float], list[float]]:
        """Top n levels per side, best first: (bid prices, bid sizes, ask prices, ask sizes)."""
        b = sorted(self.bids.items(), reverse=True)[:n]
        a = sorted(self.asks.items())[:n]
        return ([p / SCALE for p, _ in b], [s for _, s in b], [p / SCALE for p, _ in a], [s for _, s in a])

    def depth(self, width: float = 0.05) -> tuple[float, float]:
        """USDC notional (sum of size*price) resting within `width` of the best price, per side."""
        w = round(width * SCALE)
        db = da = 0.0
        if self.bids:
            bb = max(self.bids)
            db = sum(s * p for p, s in self.bids.items() if p >= bb - w) / SCALE
        if self.asks:
            ba = min(self.asks)
            da = sum(s * p for p, s in self.asks.items() if p <= ba + w) / SCALE
        return db, da
