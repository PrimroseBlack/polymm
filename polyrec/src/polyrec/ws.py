"""Websocket clients: the Polymarket market channel and generic verbatim feeds."""
from __future__ import annotations

import asyncio
import logging
import random
import time

import orjson
from websockets.asyncio.client import connect

from .rawlog import RawLog, embed

log = logging.getLogger(__name__)


class MarketWS:
    """Keeps one market-channel connection subscribed to `desired` tokens and logs every message.

    Control records ({"ctl": ...}) are written to the same log so a replay knows exactly when the
    connection was up: start/connected/subscribe/unsubscribe/hb (heartbeat, every 5 s)/disconnected.
    """

    def __init__(self, url: str, rawlog: RawLog, *, shard: int = 0, stale_seconds: float = 30.0,
                 ping_interval: float = 10.0):  # fmt: skip
        self.url, self.log, self.shard = url, rawlog, shard
        self.n_bytes = 0
        self.stale_seconds, self.ping_interval = stale_seconds, ping_interval
        self.desired: set[str] = set()
        self.subscribed: set[str] = set()
        self._wake = asyncio.Event()
        self.conn_id = 0
        self.connected = False
        self.last_rx_ns = 0  # any frame, including PONG
        self.last_data_ns = 0
        self.n_msgs = 0
        self.n_connects = 0

    def set_tokens(self, tokens: set[str]) -> None:
        if tokens != self.desired:
            self.desired = set(tokens)
            self._wake.set()

    def healthy(self, max_age: float) -> bool:
        return self.connected and time.time_ns() - self.last_data_ns < max_age * 1e9

    def ctl(self, kind: str, **kw) -> None:
        self.log.record(time.time_ns(), c=self.conn_id, s=self.shard, ctl=kind, **kw)

    async def run(self) -> None:
        backoff = 0.5
        while True:
            while not self.desired:
                await asyncio.sleep(0.2)
            t0, why = time.monotonic(), "closed"
            n0, b0 = self.n_msgs, self.n_bytes
            try:
                await self._session()
            except asyncio.CancelledError:
                why = "shutdown"
                raise
            except Exception as e:  # noqa: BLE001 - any failure means reconnect
                why = f"{type(e).__name__}: {e}"[:300]
            finally:
                if self.connected:
                    self.connected = False
                    self.ctl("disconnected", why=why)
            lived = time.monotonic() - t0
            if lived > 60:  # it had been up for a while: start over
                backoff = 0.5
            log.warning(
                "market websocket %d down after %.0fs, %d msgs (%.0f/s, %.2f MB/s): %s; reconnecting in ~%.1fs",
                self.shard, lived, self.n_msgs - n0, (self.n_msgs - n0) / max(lived, 1e-3),
                (self.n_bytes - b0) / max(lived, 1e-3) / 1e6, why, backoff,
            )  # fmt: skip
            await asyncio.sleep(backoff * (0.5 + random.random()))
            backoff = min(backoff * 2, 30)

    async def _session(self) -> None:
        # Polymarket uses text PING/PONG; protocol-level keepalive is left off.
        async with connect(self.url, ping_interval=None, max_size=None, open_timeout=15, close_timeout=3) as ws:
            self.conn_id += 1
            self.n_connects += 1
            self.last_rx_ns = self.last_data_ns = time.time_ns()
            self.connected = True
            self.ctl("connected")
            ids = sorted(self.desired)
            await ws.send(orjson.dumps({"assets_ids": ids, "type": "market", "custom_feature_enabled": True}).decode())
            self.subscribed = set(ids)
            self.ctl("subscribe", ids=ids)
            compressed = bool(getattr(getattr(ws, "protocol", None), "extensions", None))
            log.info("market websocket %d connected (#%d), %d tokens, compression %s",
                     self.shard, self.conn_id, len(ids), "on" if compressed else "off")  # fmt: skip
            tasks = [asyncio.create_task(self._recv(ws)), asyncio.create_task(self._maintain(ws))]
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for t in done:
                    t.result()
                raise ConnectionError("server closed the connection")
            finally:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _recv(self, ws) -> None:
        extra = b'"c":%d,"s":%d,' % (self.conn_id, self.shard)
        async for msg in ws:
            ns = time.time_ns()
            self.last_rx_ns = ns
            if len(msg) <= 8 and msg[:1] not in ("{", "[", b"{", b"["):
                continue  # keepalive text (PONG and variants): proves the link, carries no data
            self.last_data_ns = ns
            self.n_msgs += 1
            self.n_bytes += len(msg)
            self.log.write(embed(ns, extra, msg), ns)

    async def _maintain(self, ws) -> None:
        next_ping = next_hb = 0.0
        while True:
            now = time.monotonic()
            if now >= next_ping:
                await ws.send("PING")
                next_ping = now + self.ping_interval
            if now >= next_hb:
                self.ctl("hb")
                next_hb = now + 5
            self._wake.clear()
            add, rem = self.desired - self.subscribed, self.subscribed - self.desired
            if add:
                ids = sorted(add)
                await ws.send(
                    orjson.dumps({"assets_ids": ids, "operation": "subscribe", "custom_feature_enabled": True}).decode()
                )
                self.subscribed |= add
                self.ctl("subscribe", ids=ids)
            if rem:
                ids = sorted(rem)
                await ws.send(orjson.dumps({"assets_ids": ids, "operation": "unsubscribe"}).decode())
                self.subscribed -= rem
                self.ctl("unsubscribe", ids=ids)
            ns = time.time_ns()
            if ns - self.last_rx_ns > 25e9:
                raise TimeoutError("no frames (not even PONG) for 25s")
            if self.subscribed and ns - self.last_data_ns > self.stale_seconds * 1e9:
                raise TimeoutError(f"no market data for {self.stale_seconds:.0f}s")
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass


class MarketFeed:
    """Several market-channel connections sharing one log, each market pinned to one of them.

    One connection cannot carry every market: Polymarket drops a client whose backlog grows
    ("slow consumer"), so the load is spread and a drop only interrupts that share.
    """

    def __init__(self, url: str, rawlog: RawLog, connections: int, *, stale_seconds: float = 30.0):
        self.log = rawlog
        self.conns = [MarketWS(url, rawlog, shard=i, stale_seconds=stale_seconds) for i in range(connections)]

    def set_tokens(self, by_shard: dict[int, set[str]]) -> None:
        for c in self.conns:
            c.set_tokens(by_shard.get(c.shard, set()))

    def unhealthy_tokens(self, max_age: float) -> set[str]:
        """Tokens whose connection is not currently delivering."""
        return {t for c in self.conns if c.desired and not c.healthy(max_age) for t in c.desired}

    @property
    def connected(self) -> bool:
        active = [c for c in self.conns if c.desired]
        return bool(active) and all(c.connected for c in active)

    @property
    def n_msgs(self) -> int:
        return sum(c.n_msgs for c in self.conns)

    @property
    def n_bytes(self) -> int:
        return sum(c.n_bytes for c in self.conns)

    @property
    def n_connects(self) -> int:
        return sum(c.n_connects for c in self.conns)

    @property
    def last_data_ns(self) -> int:
        """Oldest 'last data' time among the connections in use (0 if any has none yet)."""
        return min((c.last_data_ns for c in self.conns if c.desired), default=0)

    def ctl(self, kind: str) -> None:
        self.log.record(time.time_ns(), c=0, ctl=kind)  # process-wide: no shard

    async def run(self) -> None:
        await asyncio.gather(*(c.run() for c in self.conns))


class AuxWS:
    """Records an arbitrary websocket feed verbatim. Failures never affect the main recorder."""

    def __init__(self, name: str, url: str, subscribe: list, rawlog: RawLog, stale_seconds: float = 60.0):
        self.name, self.url, self.subscribe, self.log = name, url, subscribe, rawlog
        self.stale_seconds = stale_seconds
        self.connected = False
        self.n_msgs = 0
        self.last_error = ""

    async def run(self) -> None:
        backoff = 1.0
        while True:
            t0 = time.monotonic()
            try:
                async with connect(self.url, max_size=None, open_timeout=15, close_timeout=3) as ws:
                    self.connected = True
                    self.log.record(time.time_ns(), ctl="connected")
                    for msg in self.subscribe:
                        await ws.send(msg if isinstance(msg, str) else orjson.dumps(msg).decode())
                    while True:
                        msg = await asyncio.wait_for(ws.recv(), timeout=self.stale_seconds)
                        ns = time.time_ns()
                        self.n_msgs += 1
                        self.log.write(embed(ns, b"", msg), ns)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                self.last_error = f"{type(e).__name__}: {e}"[:300]
                log.warning("aux feed %s down (%s)", self.name, self.last_error)
            finally:
                if self.connected:
                    self.connected = False
                    self.log.record(time.time_ns(), ctl="disconnected")
            if time.monotonic() - t0 > 60:
                backoff = 1.0
            await asyncio.sleep(backoff * (0.5 + random.random()))
            backoff = min(backoff * 2, 300)
