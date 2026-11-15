"""Chainlink reference prices via Polymarket's PolyBolt websocket.

This is the feed the 5m, 15m and 4h markets settle on: `price.crypto` is the live Chainlink
price and `price.crypto.twap` its 60-second average (the settlement value). It needs CLOB API
credentials; they are sent only in the auth frame and never written to any log.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass

import orjson
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from .rawlog import RawLog, embed

log = logging.getLogger(__name__)
AUTH_RETRY_SECONDS = 600  # bad credentials do not fix themselves: retry slowly


@dataclass(frozen=True, repr=False)
class ClobCreds:
    api_key: str
    secret: str
    passphrase: str

    def __repr__(self) -> str:  # never leak into logs or tracebacks
        return "ClobCreds(***)"


class AuthFailed(Exception):
    pass


def subscriptions(coins) -> list[dict]:
    subs = []
    for coin in coins:
        symbol = f"{coin}usd"
        subs.append({"channel": "price.crypto", "filter": {"symbol": symbol, "provider": "chainlink"}})
        subs.append({"channel": "price.crypto.twap", "filter": {"symbol": symbol, "window_seconds": 60}})
    return subs


def parse_polybolt(msg) -> list[tuple[int, str, str, float, int]]:
    """Price points in a PolyBolt envelope as (exchange ms, feed, symbol, price, is_snapshot).

    feed is the producing vendor ("chainlink", or "pyth" if the server fell back), with "_twap"
    appended for the 60-second average. Acknowledgements and anything unrecognised give [].
    """
    try:
        channel = msg.get("channel")
        if channel not in ("price.crypto", "price.crypto.twap"):
            return []
        p = msg["payload"]
        feed = (p.get("source") or "chainlink") + ("_twap" if channel.endswith(".twap") else "")
        snap = 1 if msg.get("snapshot") else 0
        points = (p.get("data") or []) if snap else [p]
        return [
            (int(pt["timestamp"]), feed, p["symbol"], float(pt.get("full_accuracy_value") or pt["value"]), snap)
            for pt in points
        ]
    except (AttributeError, KeyError, TypeError, ValueError):
        return []


async def authenticate(ws, creds: ClobCreds, timeout: float = 10.0) -> None:
    await ws.send(
        orjson.dumps(
            {"op": "auth", "rid": "a1",
             "auth": {"apiKey": creds.api_key, "secret": creds.secret, "passphrase": creds.passphrase}}
        ).decode()
    )  # fmt: skip
    deadline = time.monotonic() + timeout
    while True:
        msg = orjson.loads(await asyncio.wait_for(ws.recv(), max(0.1, deadline - time.monotonic())))
        if msg.get("op") == "authed":
            return
        if msg.get("op") == "error":
            code = msg.get("code")
            if code in ("auth_invalid", "auth_attempts"):
                raise AuthFailed(code)
            raise ConnectionError(f"authentication error: {code}")


class PolyBoltWS:
    """Records both Chainlink channels for every coin, verbatim. Never affects the main recorder."""

    name = "chainlink"

    def __init__(self, url: str, creds: ClobCreds, coins, rawlog: RawLog, stale_seconds: float = 30.0):
        self.url, self.creds, self.coins, self.log = url, creds, tuple(coins), rawlog
        self.stale_seconds = stale_seconds
        self.connected = False
        self.n_msgs = 0
        self.last_error = ""
        self._warned: set[str] = set()

    async def run(self) -> None:
        backoff = 1.0
        while True:
            t0, wait = time.monotonic(), None
            try:
                await self._session()
            except asyncio.CancelledError:
                raise
            except AuthFailed as e:
                self.last_error = f"credentials rejected ({e})"
                wait = AUTH_RETRY_SECONDS
                log.error("Chainlink feed: Polymarket rejected the API credentials (%s); fix them and restart", e)
            except ConnectionClosed as e:
                code = e.rcvd.code if e.rcvd else None
                self.last_error = f"closed with code {code}"
                if code in (4001, 4008):  # auth failure / policy violation: retrying fast cannot help
                    wait = AUTH_RETRY_SECONDS
                elif code == 4003:  # server draining
                    wait = random.uniform(0, 10)
            except Exception as e:  # noqa: BLE001
                self.last_error = f"{type(e).__name__}: {e}"[:300]
            finally:
                if self.connected:
                    self.connected = False
                    self.log.record(time.time_ns(), ctl="disconnected")
            if time.monotonic() - t0 > 60:
                backoff = 1.0
            if wait is None:
                wait = backoff * (0.5 + random.random())
            log.warning("Chainlink feed down (%s); reconnecting in ~%.0fs", self.last_error, wait)
            await asyncio.sleep(wait)
            backoff = min(backoff * 2, 30)

    async def _session(self) -> None:
        # The server pings every 25 s and the library answers; staleness is detected by silence.
        async with connect(self.url, ping_interval=None, max_size=None, open_timeout=15, close_timeout=3) as ws:
            await authenticate(ws, self.creds)
            self.connected = True
            self.log.record(time.time_ns(), ctl="connected")
            await ws.send(orjson.dumps({"op": "subscribe", "rid": "s1", "subscriptions": subscriptions(self.coins)}).decode())
            log.info("Chainlink feed connected, %d coins", len(self.coins))
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=self.stale_seconds)
                ns = time.time_ns()
                self.n_msgs += 1
                self.log.write(embed(ns, b"", raw), ns)
                if b'"op"' in (raw if isinstance(raw, bytes) else raw.encode())[:64]:
                    self._on_ack(orjson.loads(raw))

    def _on_ack(self, msg: dict) -> None:
        if msg.get("op") != "error":
            return
        code = str(msg.get("code"))
        if code in ("auth_required", "auth_invalid"):
            raise AuthFailed(code)
        key = f"{code}:{msg.get('channel')}"
        if key not in self._warned:
            self._warned.add(key)
            log.warning("Chainlink feed: server error %s on %s", code, msg.get("channel"))
