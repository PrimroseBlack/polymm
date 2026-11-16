"""`polyrec smoke`: prove from this host that each live feed the recorder depends on works."""
from __future__ import annotations

import asyncio
import math
import time
from collections import Counter, deque

import httpx
import orjson
from websockets.asyncio.client import connect

from .book import Book
from .chainlink import AuthFailed, authenticate, parse_polybolt, subscriptions
from .config import Config, clob_creds, series_slug
from .derive import parse_ref
from .gamma import Gamma, parse_market


class BookTracker:
    """Rebuilds books from websocket events and remembers each state, so a REST snapshot can be
    compared with the book as it stood at the same exchange instant (or the same book hash)."""

    def __init__(self, tokens, keep: int = 50_000):
        self.books = {t: Book() for t in tokens}
        self.history = {t: deque(maxlen=keep) for t in tokens}  # (exchange ms, hash, bid, bid size, ask, ask size)
        self.counts: Counter = Counter()
        self.snapshots: set[str] = set()

    def _note(self, tok: str, ts: int, book_hash) -> None:
        self.history[tok].append((ts, book_hash, *self.books[tok].best()))

    def handle(self, data) -> None:
        for e in data if isinstance(data, list) else (data,):
            et = e.get("event_type")
            self.counts[et] += 1
            ts = int(e.get("timestamp") or 0)
            if et == "book" and e.get("asset_id") in self.books:
                tok = e["asset_id"]
                self.books[tok].snapshot(e.get("bids") or (), e.get("asks") or (), "ws", 0)
                self.snapshots.add(tok)
                self._note(tok, ts, e.get("hash"))
            elif et == "price_change":
                touched = {}
                for c in e.get("price_changes") or ():
                    b = self.books.get(c.get("asset_id"))
                    if b is not None and b.src:
                        b.apply(c["side"] == "BUY", c["price"], c["size"], 0)
                        touched[c["asset_id"]] = c.get("hash")
                for tok, h in touched.items():
                    self._note(tok, ts, h)

    def compare(self, rest_book: dict) -> tuple[bool, str, str]:
        """(agrees, method, detail) for one REST book against the rebuilt book's history."""
        tok = rest_book["asset_id"]
        hist = self.history[tok]
        bids, asks = rest_book.get("bids") or [], rest_book.get("asks") or []
        rb = max(bids, key=lambda l: float(l["price"]), default=None)
        ra = min(asks, key=lambda l: float(l["price"]), default=None)
        want = (float(rb["price"]) if rb else math.nan, float(rb["size"]) if rb else math.nan,
                float(ra["price"]) if ra else math.nan, float(ra["size"]) if ra else math.nan)  # fmt: skip
        rts, rhash = int(rest_book.get("timestamp") or 0), rest_book.get("hash")
        entry = next((h for h in reversed(hist) if rhash and h[1] == rhash), None)
        method = "hash"
        if entry is None:
            entry = next((h for h in reversed(hist) if h[0] <= rts), None)
            method = "time"
        if entry is None:
            return False, "none", f"no websocket state at or before REST time {rts} ({len(hist)} states kept)"
        got = entry[2:]
        ok = all(_eq(g, w) for g, w in zip(got, want))
        newest = hist[-1]
        detail = (f"rest ts={rts} bid={want[0]}x{want[1]} ask={want[2]}x{want[3]} | ws[{method}] ts={entry[0]} "
                  f"bid={got[0]}x{got[1]} ask={got[2]}x{got[3]} | ws newest ts={newest[0]} bid={newest[2]} ask={newest[4]}")  # fmt: skip
        return ok, method, detail


def _eq(a: float, b: float) -> bool:
    return (a != a and b != b) or abs(a - b) < 1e-9


async def _current(gamma: Gamma, coin: str, dur: str):
    sid = await gamma.series_id(series_slug(coin, dur))
    events = await gamma.upcoming(sid, time.time(), limit=1) if sid else []
    return parse_market(events[0], coin, dur) if events else None


async def smoke(cfg: Config, seconds: int = 20) -> int:
    results: list[tuple[str, bool, str, bool]] = []

    def check(name: str, ok: bool, detail: str = "", required: bool = True) -> None:
        results.append((name, ok, detail, required))
        print(f"[{'PASS' if ok else 'FAIL' if required else 'WARN'}] {name}{': ' + detail if detail else ''}", flush=True)

    async with httpx.AsyncClient(timeout=15, headers={"User-Agent": "polyrec"}) as client:
        gamma = Gamma(client, cfg.gamma_url)
        try:
            btc, eth = await _current(gamma, "btc", "5m"), await _current(gamma, "eth", "5m")
            check("Gamma discovery", bool(btc and eth), btc.slug if btc else "no current BTC 5m market")
        except Exception as e:  # noqa: BLE001
            check("Gamma discovery", False, repr(e))
            btc = eth = None
        if not (btc and eth):
            return 1

        async def rest_books(tokens):
            r = await client.post(f"{cfg.clob_url}/books", json=[{"token_id": t} for t in tokens])
            r.raise_for_status()
            return {b["asset_id"]: b for b in r.json()}

        try:
            got = await rest_books(btc.tokens)
            check("REST /books", len(got) == 2, f"{len(got)} of 2 books")
        except Exception as e:  # noqa: BLE001
            check("REST /books", False, repr(e))

        # Market websocket: initial subscribe, PING/PONG, dynamic subscribe, book maintenance.
        tracker = BookTracker((*btc.tokens, *eth.tokens))
        pongs = 0
        verdicts: list[tuple[bool, str, str]] = []
        try:
            async with connect(cfg.ws_url, ping_interval=None, max_size=None, open_timeout=15) as ws:
                await ws.send(orjson.dumps({"assets_ids": list(btc.tokens), "type": "market",
                                            "custom_feature_enabled": True}).decode())  # fmt: skip
                state = {"next_ping": 0.0}

                async def pump(until: float) -> None:
                    """Read and apply websocket messages until the given monotonic time."""
                    nonlocal pongs
                    while (now := time.monotonic()) < until:
                        if now >= state["next_ping"]:
                            await ws.send("PING")
                            state["next_ping"] = now + 10
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=min(0.5, until - now))
                        except asyncio.TimeoutError:
                            continue
                        if len(msg) <= 8 and msg[:1] not in ("{", "["):
                            pongs += 1
                        else:
                            tracker.handle(orjson.loads(msg))

                t0 = time.monotonic()
                await pump(t0 + 5)
                await ws.send(orjson.dumps({"assets_ids": list(eth.tokens), "operation": "subscribe",
                                            "custom_feature_enabled": True}).decode())  # fmt: skip
                await pump(t0 + seconds)
                # Cross-check against REST three times, reading the websocket throughout so the
                # rebuilt book has caught up to (and passed) each REST snapshot's instant.
                for _ in range(3):
                    task = asyncio.create_task(rest_books(tracker.books))
                    while not task.done():
                        await pump(time.monotonic() + 0.1)
                    rest = task.result()
                    await pump(time.monotonic() + 1.5)
                    verdicts += [tracker.compare(r) for r in rest.values()]
            counts, snapshots = tracker.counts, tracker.snapshots
            check("websocket connect", True, cfg.ws_url)
            check("websocket PING/PONG", pongs > 0, f"{pongs} PONG")
            check("book snapshot on subscribe", set(btc.tokens) <= snapshots)
            check("book snapshot on dynamic subscribe", set(eth.tokens) <= snapshots)
            rate = sum(counts.values()) / max(1, seconds)
            check("order-book updates", counts["price_change"] > 0,
                  ", ".join(f"{k}={v}" for k, v in counts.items()) + f" (~{rate:.0f} events/s for 2 markets)")  # fmt: skip
            good = sum(ok for ok, _, _ in verdicts)
            how = Counter(m for _, m, _ in verdicts)
            # Advisory: recording is verbatim and does not depend on this; it validates the replay logic.
            check("rebuilt book matches REST", good == len(verdicts) and bool(verdicts),
                  f"{good} of {len(verdicts)} comparisons agree (matched by {dict(how)})", required=False)  # fmt: skip
            for ok, _, detail in [v for v in verdicts if not v[0]][:4]:
                print("       mismatch:", detail, flush=True)
        except Exception as e:  # noqa: BLE001
            check("websocket connect", False, repr(e))

    # Auxiliary feeds are optional: a failure is a warning.
    for feed in cfg.aux_feeds:
        try:
            parsed = n = 0
            async with connect(feed.url, max_size=None, open_timeout=15) as ws:
                for msg in feed.subscribe:
                    await ws.send(msg if isinstance(msg, str) else orjson.dumps(msg).decode())
                t0 = time.monotonic()
                while time.monotonic() - t0 < 6:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=2)
                    except asyncio.TimeoutError:
                        continue
                    n += 1
                    try:
                        parsed += parse_ref(orjson.loads(raw)) is not None
                    except orjson.JSONDecodeError:
                        pass
            check(f"aux feed {feed.name}", parsed > 0, f"{n} messages, {parsed} parsed as prices", required=False)
        except Exception as e:  # noqa: BLE001
            check(f"aux feed {feed.name}", False, repr(e), required=False)

    # Chainlink prices: optional, needs credentials.
    creds = clob_creds() if cfg.chainlink else None
    if cfg.chainlink and not creds:
        check("Chainlink feed", False, "skipped: no API credentials found (see README)", required=False)
    elif creds:
        try:
            got: Counter = Counter()
            async with connect(cfg.polybolt_url, ping_interval=None, max_size=None, open_timeout=15) as ws:
                await authenticate(ws, creds)
                await ws.send(orjson.dumps({"op": "subscribe", "rid": "s1", "subscriptions": subscriptions(["btc"])}).decode())
                t0 = time.monotonic()
                while time.monotonic() - t0 < 10 and not (got["chainlink"] and got["chainlink_twap"]):
                    try:
                        msg = orjson.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                    except asyncio.TimeoutError:
                        continue
                    if msg.get("op") == "error":
                        got[f"error:{msg.get('code')}"] += 1
                    for _, feed, _, _, snap in parse_polybolt(msg):
                        got[feed] += not snap
            ok = bool(got["chainlink"] and got["chainlink_twap"])
            check("Chainlink feed", ok, ", ".join(f"{k}={v}" for k, v in got.items()) or "no prices in 10s", required=False)
        except AuthFailed as e:
            check("Chainlink feed", False, f"credentials rejected ({e})", required=False)
        except Exception as e:  # noqa: BLE001
            check("Chainlink feed", False, repr(e), required=False)

    failed = [n for n, ok, _, req in results if req and not ok]
    print("\nRESULT:", "all required checks passed" if not failed else f"FAILED: {', '.join(failed)}")
    return 1 if failed else 0
