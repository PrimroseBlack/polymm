import asyncio
import json
import time

from websockets.asyncio.server import serve

from polyrec.rawlog import RawLog, iter_raw
from polyrec.ws import MarketWS


class FakeMarketServer:
    """Speaks just enough of the market channel: book snapshot per subscribed asset, PING/PONG."""

    def __init__(self):
        self.frames: list = []  # (connection number, parsed frame or "PING")
        self.conns = 0
        self.drop_after_first_subscribe = False

    async def handler(self, ws):
        self.conns += 1
        n = self.conns
        async for raw in ws:
            if raw == "PING":
                self.frames.append((n, "PING"))
                await ws.send("PONG")
                await ws.send("pong")  # servers have been seen to vary the keepalive text
                continue
            msg = json.loads(raw)
            self.frames.append((n, msg))
            if msg.get("type") == "market" or msg.get("operation") == "subscribe":
                books = [
                    {"event_type": "book", "asset_id": a, "market": "0xabc", "timestamp": "1",
                     "bids": [{"price": "0.49", "size": "10"}], "asks": [{"price": "0.51", "size": "10"}]}
                    for a in msg["assets_ids"]
                ]  # fmt: skip
                await ws.send(json.dumps(books))
                if self.drop_after_first_subscribe and n == 1:
                    await ws.close()
                    return


async def wait_for(pred, timeout=8.0):
    t0 = time.monotonic()
    while not pred():
        if time.monotonic() - t0 > timeout:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.02)


def records(tmp_path):
    now = time.time_ns()  # a narrow range: iter_raw probes one file per hour
    return list(iter_raw(tmp_path, "ws", now - 3600 * 10**9, now + 3600 * 10**9))


def test_subscribe_ping_and_dynamic_updates(tmp_path):
    async def go():
        srv = FakeMarketServer()
        async with serve(srv.handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            lg = RawLog(tmp_path, "ws")
            ws = MarketWS(f"ws://127.0.0.1:{port}", lg, ping_interval=0.2)
            ws.set_tokens({"A", "B"})
            task = asyncio.create_task(ws.run())
            await wait_for(lambda: ws.n_msgs >= 1 and any(f == "PING" for _, f in srv.frames))
            assert ws.healthy(5)
            ws.set_tokens({"B", "C"})
            await wait_for(lambda: ws.n_msgs >= 2 and len([f for _, f in srv.frames if f != "PING"]) >= 3)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            lg.close()
        return srv

    srv = asyncio.run(go())
    sent = [f for _, f in srv.frames if f != "PING"]
    assert sent[0] == {"assets_ids": ["A", "B"], "type": "market", "custom_feature_enabled": True}
    assert {"assets_ids": ["C"], "operation": "subscribe", "custom_feature_enabled": True} in sent
    assert {"assets_ids": ["A"], "operation": "unsubscribe"} in sent
    recs = records(tmp_path)
    ctl = [(r["ctl"], r.get("ids")) for r in recs if "ctl" in r and r["ctl"] != "hb"]
    assert ctl[0] == ("connected", None) and ctl[1] == ("subscribe", ["A", "B"])
    assert ("subscribe", ["C"]) in ctl and ("unsubscribe", ["A"]) in ctl
    assert ctl[-1][0] == "disconnected"  # written on shutdown
    assert any(r.get("ctl") == "hb" for r in recs)
    books = [e["asset_id"] for r in recs if "m" in r for e in r["m"]]
    assert books == ["A", "B", "C"]  # PONG is not logged; messages are verbatim
    assert all(r["c"] == 1 for r in recs)


def test_reconnects_and_resubscribes_after_drop(tmp_path):
    async def go():
        srv = FakeMarketServer()
        srv.drop_after_first_subscribe = True
        async with serve(srv.handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            lg = RawLog(tmp_path, "ws")
            ws = MarketWS(f"ws://127.0.0.1:{port}", lg)
            ws.set_tokens({"A"})
            task = asyncio.create_task(ws.run())
            await wait_for(lambda: ws.conn_id >= 2 and ws.n_msgs >= 2)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            lg.close()
        return srv

    srv = asyncio.run(go())
    assert srv.conns >= 2
    recs = records(tmp_path)
    kinds = [(r["c"], r["ctl"]) for r in recs if "ctl" in r and r["ctl"] != "hb"]
    assert kinds[:5] == [(1, "connected"), (1, "subscribe"), (1, "disconnected"), (2, "connected"), (2, "subscribe")]
    assert [r["c"] for r in recs if "m" in r][:2] == [1, 2]


def test_stalled_connection_is_dropped(tmp_path):
    async def silent(ws):  # accepts frames, never answers
        async for _ in ws:
            pass

    async def go():
        async with serve(silent, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            lg = RawLog(tmp_path, "ws")
            ws = MarketWS(f"ws://127.0.0.1:{port}", lg, stale_seconds=0.5)
            ws.set_tokens({"A"})
            task = asyncio.create_task(ws.run())
            await wait_for(lambda: ws.conn_id >= 2)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            lg.close()

    asyncio.run(go())
    why = [r["why"] for r in records(tmp_path) if r.get("ctl") == "disconnected"]
    assert "no market data" in why[0]


def test_aux_feed_backoff_keeps_growing(tmp_path, monkeypatch):
    """A feed that never connects must back off further each time, not reset after a long sleep."""
    from polyrec import ws as wsmod

    sleeps, clock = [], [0.0]

    async def fake_sleep(d):
        sleeps.append(d)
        clock[0] += d
        if len(sleeps) >= 12:
            raise asyncio.CancelledError

    monkeypatch.setattr(wsmod.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(wsmod.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(wsmod.random, "random", lambda: 0.5)
    feed = wsmod.AuxWS("x", "ws://127.0.0.1:1", [], RawLog(tmp_path, "aux_x"))
    try:
        asyncio.run(feed.run())
    except asyncio.CancelledError:
        pass
    assert sleeps[:10] == [1, 2, 4, 8, 16, 32, 64, 128, 256, 300] and sleeps[-1] == 300
    assert feed.last_error and not feed.connected


def test_feed_spreads_tokens_over_connections_and_tags_records(tmp_path):
    from polyrec.ws import MarketFeed

    async def go():
        srv = FakeMarketServer()
        async with serve(srv.handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            lg = RawLog(tmp_path, "ws")
            feed = MarketFeed(f"ws://127.0.0.1:{port}", lg, 3)
            feed.ctl("start")
            feed.set_tokens({0: {"A", "B"}, 2: {"C"}})  # nothing for connection 1: it must stay closed
            task = asyncio.create_task(feed.run())
            await wait_for(lambda: feed.n_msgs >= 2 and feed.connected)
            assert feed.unhealthy_tokens(5) == set() and feed.n_connects == 2 and feed.n_bytes > 0
            feed.conns[2].last_data_ns = 0  # pretend connection 2 went quiet
            assert feed.unhealthy_tokens(5) == {"C"}
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            lg.close()
        return srv

    srv = asyncio.run(go())
    assert srv.conns == 2
    recs = records(tmp_path)
    assert recs[0]["ctl"] == "start" and "s" not in recs[0]
    subs = {r["s"]: r["ids"] for r in recs if r.get("ctl") == "subscribe"}
    assert subs == {0: ["A", "B"], 2: ["C"]}
    by_shard = {r["s"]: [e["asset_id"] for e in r["m"]] for r in recs if "m" in r}
    assert by_shard == {0: ["A", "B"], 2: ["C"]}


def test_markets_of_one_coin_land_on_different_connections(tmp_path):
    from polyrec.config import Config
    from polyrec.gamma import Market
    from polyrec.recorder import Recorder

    cfg = Config(data_dir=tmp_path)
    rec = Recorder(cfg)
    mk = lambda coin, dur: Market("c", "e", "m", "s", coin, dur, 0, 1, "u", "d")  # noqa: E731
    for coin in ("btc", "eth"):
        assert sorted(rec.shard_of(mk(coin, d)) for d in cfg.durations) == [0, 1, 2, 3]
    load = [0] * cfg.ws_connections
    for coin, dur in cfg.series:
        load[rec.shard_of(mk(coin, dur))] += 1
    assert load == [7, 7, 7, 7]
