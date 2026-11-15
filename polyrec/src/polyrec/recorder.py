"""The 24/7 capture process. It only records raw data; `polyrec derive` builds the tables."""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import time
from pathlib import Path

import httpx
import orjson

from .config import Config, series_slug
from .gamma import Gamma, Market, official_outcome, parse_market
from .rawlog import RawLog, compress_leftovers, embed
from .ws import AuxWS, MarketWS

log = logging.getLogger(__name__)
# Seconds after a window ends at which to re-read the market for its official result.
FINAL_DELAYS = (20, 60, 120, 300, 600, 1800, 3600, 7200)
BOOKS_CHUNK = 50


class BookPoller:
    """POST /books for all active tokens: every checkpoint interval, or every second in fallback."""

    def __init__(self, client: httpx.AsyncClient, cfg: Config, rawlog: RawLog, get_tokens, ws_healthy):
        self.client, self.cfg, self.log = client, cfg, rawlog
        self.get_tokens, self.ws_healthy = get_tokens, ws_healthy
        self.fallback = False
        self.n_ok = self.n_fail = 0

    async def run(self) -> None:
        next_checkpoint = 0.0
        while True:
            t = time.monotonic()
            fallback = not self.ws_healthy()
            if fallback != self.fallback:
                self.fallback = fallback
                log.warning("REST fallback %s", "ON (websocket not delivering)" if fallback else "off")
            if fallback or t >= next_checkpoint:
                if t >= next_checkpoint:
                    next_checkpoint = t + self.cfg.rest_checkpoint_interval
                await self.poll(fallback)
            spent = time.monotonic() - t
            await asyncio.sleep(max(0.05, self.cfg.rest_fallback_interval - spent) if fallback else 0.25)

    async def poll(self, fallback: bool) -> None:
        tokens = sorted(self.get_tokens())
        chunks = [tokens[i : i + BOOKS_CHUNK] for i in range(0, len(tokens), BOOKS_CHUNK)]
        await asyncio.gather(*(self._one(c, fallback) for c in chunks))

    async def _one(self, tokens: list[str], fallback: bool) -> None:
        sent = time.time_ns()
        try:
            r = await self.client.post(f"{self.cfg.clob_url}/books", json=[{"token_id": t} for t in tokens])
            r.raise_for_status()
        except httpx.HTTPError as e:
            self.n_fail += 1
            if self.n_fail % 20 == 1:
                log.warning("REST /books failed (%s); %d failures so far", type(e).__name__, self.n_fail)
            return
        ns = time.time_ns()
        self.n_ok += 1
        self.log.write(embed(ns, b'"s":%d,"fb":%d,' % (sent, fallback), r.content), ns)


class Recorder:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.root = Path(cfg.data_dir)
        self.markets: dict[str, Market] = {}  # known, not yet expired
        self.active: dict[str, Market] = {}
        self.series_ids: dict[tuple[str, str], str] = {}
        self._finalizers: set[asyncio.Task] = set()
        self.n_discovered = 0
        self.started = time.time()

    # -- active set -------------------------------------------------------------------------

    def tokens(self) -> set[str]:
        return {t for m in self.active.values() for t in m.tokens}

    def refresh_active(self) -> None:
        now, cfg = time.time(), self.cfg
        self.active = {
            cid: m
            for cid, m in self.markets.items()
            if m.start_ts - cfg.lead_seconds <= now <= m.end_ts + cfg.linger_seconds
        }
        for cid in [c for c, m in self.markets.items() if now > m.end_ts + cfg.linger_seconds + 60]:
            del self.markets[cid]
        self.ws.set_tokens(self.tokens())

    # -- discovery --------------------------------------------------------------------------

    async def resolve_series(self) -> None:
        pending = list(self.cfg.series)
        delay = 2.0
        while pending:
            for key in list(pending):
                slug = series_slug(*key)
                try:
                    sid = await self.gamma.series_id(slug)
                except Exception as e:  # noqa: BLE001
                    log.warning("series lookup %s failed: %s", slug, e)
                    continue
                pending.remove(key)
                if sid:
                    self.series_ids[key] = sid
                else:
                    log.error("series %s does not exist on Gamma; %s %s will not be recorded", slug, *key)
            if pending:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
        log.info("resolved %d of %d series", len(self.series_ids), len(self.cfg.series))

    async def discover_once(self) -> None:
        sem = asyncio.Semaphore(8)

        async def one(key, sid):
            async with sem:
                try:
                    events = await self.gamma.upcoming(sid, time.time())
                except Exception as e:  # noqa: BLE001
                    log.warning("discovery %s-%s failed: %s", *key, e)
                    return
            for ev in events:
                m = parse_market(ev, *key)
                if m is None:
                    log.warning("unparseable event %s in %s-%s", ev.get("slug"), *key)
                elif m.condition_id not in self.markets and time.time() <= m.end_ts:
                    self.markets[m.condition_id] = m
                    self.n_discovered += 1
                    self._meta("discovered", m, ev)
                    task = asyncio.create_task(self._finalize(m))
                    self._finalizers.add(task)
                    task.add_done_callback(self._finalizers.discard)

        await asyncio.gather(*(one(k, s) for k, s in self.series_ids.items()))

    async def discovery_loop(self) -> None:
        while True:
            await self.discover_once()
            self.refresh_active()
            await asyncio.sleep(self.cfg.discovery_interval)

    def _meta(self, kind: str, m: Market, ev: dict | None) -> None:
        self.meta.record(time.time_ns(), k=kind, cid=m.condition_id, coin=m.coin, dur=m.duration, slug=m.slug, ev=ev)

    async def _finalize(self, m: Market) -> None:
        """Re-read the market after it ends for the official result, price to beat and final price.

        Gamma publishes the outcome within seconds, the final price some minutes later, and for
        some series (4h) no prices at all, so stop waiting for the price after ten minutes.
        """
        ev = None
        for delay in FINAL_DELAYS:
            await asyncio.sleep(max(0.0, m.end_ts + delay - time.time()))
            try:
                ev = await self.gamma.event_by_slug(m.slug)
            except Exception as e:  # noqa: BLE001
                log.warning("re-reading %s for result failed: %s", m.slug, e)
                continue
            has_price = ev and (ev.get("eventMetadata") or {}).get("finalPrice") is not None
            if ev and official_outcome(ev) and (has_price or delay >= 600):
                self._meta("final", m, ev)
                return
        log.warning("%s still unresolved %ds after its end", m.slug, FINAL_DELAYS[-1])
        self._meta("final_timeout", m, ev)

    # -- housekeeping -----------------------------------------------------------------------

    async def housekeeping_loop(self) -> None:
        last_log, last_msgs = time.monotonic(), 0
        while True:
            await asyncio.sleep(1)
            self.refresh_active()
            for lg in self.logs:
                lg.flush()
            if int(time.time()) % 5 == 0:
                self.write_status()
            if time.monotonic() - last_log >= 60:
                log.info(
                    "ws=%s msgs/min=%d active=%d rest ok/fail=%d/%d fallback=%s",
                    "up" if self.ws.connected else "DOWN",
                    self.ws.n_msgs - last_msgs,
                    len(self.active),
                    self.poller.n_ok,
                    self.poller.n_fail,
                    self.poller.fallback,
                )
                last_log, last_msgs = time.monotonic(), self.ws.n_msgs

    def status(self) -> dict:
        now = time.time_ns()
        return {
            "time": time.time(),
            "started": self.started,
            "pid": os.getpid(),
            "ws_connected": self.ws.connected,
            "ws_connects": self.ws.n_connects,
            "ws_msgs": self.ws.n_msgs,
            "ws_data_age_s": round((now - self.ws.last_data_ns) / 1e9, 1) if self.ws.last_data_ns else None,
            "rest_fallback": self.poller.fallback,
            "rest_ok": self.poller.n_ok,
            "rest_fail": self.poller.n_fail,
            "active_markets": len(self.active),
            "markets_discovered": self.n_discovered,
            "series": len(self.series_ids),
            "aux": {a.name: {"connected": a.connected, "msgs": a.n_msgs, "last_error": a.last_error} for a in self.aux},
        }

    def write_status(self) -> None:
        tmp = self.root / "status.json.tmp"
        tmp.write_bytes(orjson.dumps(self.status(), option=orjson.OPT_INDENT_2))
        tmp.replace(self.root / "status.json")

    # -- main -------------------------------------------------------------------------------

    async def run(self) -> int:
        cfg = self.cfg
        self.root.mkdir(parents=True, exist_ok=True)
        compress_leftovers(self.root)
        ws_log, rest_log, self.meta = RawLog(self.root, "ws"), RawLog(self.root, "rest"), RawLog(self.root, "meta")
        self.logs = [ws_log, rest_log, self.meta]
        self.ws = MarketWS(cfg.ws_url, ws_log, stale_seconds=cfg.ws_stale_seconds)
        self.aux = []
        for feed in cfg.aux_feeds:
            lg = RawLog(self.root, f"aux_{feed.name}")
            self.logs.append(lg)
            self.aux.append(AuxWS(feed.name, feed.url, feed.subscribe, lg, feed.stale_seconds))

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

        limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)
        async with httpx.AsyncClient(timeout=10, limits=limits, headers={"User-Agent": "polyrec"}) as client:
            self.gamma = Gamma(client, cfg.gamma_url)
            self.poller = BookPoller(
                client, cfg, rest_log, self.tokens, lambda: self.ws.healthy(cfg.rest_fallback_after)
            )
            self.ws.ctl("start")
            await self.resolve_series()
            if not self.series_ids:
                log.error("no series resolved; nothing to record")
                return 1
            core = [
                asyncio.create_task(self.discovery_loop(), name="discovery"),
                asyncio.create_task(self.ws.run(), name="ws"),
                asyncio.create_task(self.poller.run(), name="rest"),
                asyncio.create_task(self.housekeeping_loop(), name="housekeeping"),
            ]
            extra = [asyncio.create_task(a.run(), name=f"aux_{a.name}") for a in self.aux]
            stopper = asyncio.create_task(stop.wait())
            done, _ = await asyncio.wait([*core, stopper], return_when=asyncio.FIRST_COMPLETED)
            code = 0
            for t in done:
                if t is not stopper and t.exception():
                    log.error("task %s crashed", t.get_name(), exc_info=t.exception())
                    code = 1
            log.info("shutting down")
            tasks = [*core, *extra, stopper, *self._finalizers]
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.ws.ctl("stop")
            for lg in self.logs:
                lg.close()
            return code
