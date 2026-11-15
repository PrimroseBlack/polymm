"""`polyrec smoke`: prove from this host that each live feed the recorder depends on works."""
from __future__ import annotations

import asyncio
import math
import time
from collections import Counter

import httpx
import orjson
from websockets.asyncio.client import connect

from .book import Book
from .chainlink import AuthFailed, authenticate, parse_polybolt, subscriptions
from .config import Config, clob_creds, series_slug
from .derive import parse_ref
from .gamma import Gamma, parse_market


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
        counts: Counter = Counter()
        books = {t: Book() for t in (*btc.tokens, *eth.tokens)}
        snapshots, pongs = set(), 0
        try:
            async with connect(cfg.ws_url, ping_interval=None, max_size=None, open_timeout=15) as ws:
                await ws.send(orjson.dumps({"assets_ids": list(btc.tokens), "type": "market",
                                            "custom_feature_enabled": True}).decode())  # fmt: skip
                t0 = time.monotonic()
                next_ping, added = 0.0, False
                while time.monotonic() - t0 < seconds:
                    now = time.monotonic()
                    if now >= next_ping:
                        await ws.send("PING")
                        next_ping = now + 10
                    if not added and now - t0 > 5:
                        await ws.send(orjson.dumps({"assets_ids": list(eth.tokens), "operation": "subscribe",
                                                    "custom_feature_enabled": True}).decode())  # fmt: skip
                        added = True
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1)
                    except asyncio.TimeoutError:
                        continue
                    if msg in ("PONG", b"PONG"):
                        pongs += 1
                        continue
                    data = orjson.loads(msg)
                    for e in data if isinstance(data, list) else (data,):
                        et = e.get("event_type")
                        counts[et] += 1
                        if et == "book" and e.get("asset_id") in books:
                            books[e["asset_id"]].snapshot(e.get("bids") or (), e.get("asks") or (), "ws", 0)
                            snapshots.add(e["asset_id"])
                        elif et == "price_change":
                            for c in e.get("price_changes") or ():
                                b = books.get(c.get("asset_id"))
                                if b is not None and b.src:
                                    b.apply(c["side"] == "BUY", c["price"], c["size"], 0)
                rest = await rest_books(books)
            check("websocket connect", True, cfg.ws_url)
            check("websocket PING/PONG", pongs > 0, f"{pongs} PONG")
            check("book snapshot on subscribe", set(btc.tokens) <= snapshots)
            check("book snapshot on dynamic subscribe", set(eth.tokens) <= snapshots)
            check("order-book updates", counts["price_change"] > 0, ", ".join(f"{k}={v}" for k, v in counts.items()))
            same = 0
            for tok, b in books.items():
                r = rest.get(tok)
                if r and b.src:
                    bb, _, ba, _ = b.best()
                    rb = max((float(l["price"]) for l in r["bids"]), default=math.nan)
                    ra = min((float(l["price"]) for l in r["asks"]), default=math.nan)
                    same += (bb == rb or (bb != bb and rb != rb)) and (ba == ra or (ba != ba and ra != ra))
            check("rebuilt book matches REST", same >= len(books) - 1, f"{same} of {len(books)} tops agree")
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
