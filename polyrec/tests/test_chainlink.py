import asyncio
import json
import os
import stat
import time

import pytest
from websockets.asyncio.server import serve

from polyrec import chainlink as cl
from polyrec.chainlink import ClobCreds, PolyBoltWS, parse_polybolt, subscriptions
from polyrec.config import CRED_VARS, clob_creds, load_env_file
from polyrec.rawlog import RawLog, iter_raw

CREDS = ClobCreds("key-123", "s3cr3t-value", "pass-phrase-xyz")


def test_parse_live_snapshot_and_acks():
    live = {"v": 1, "channel": "price.crypto", "seq": 2, "ts": 1, "payload": {
        "symbol": "btcusd", "value": 64126.0, "full_accuracy_value": "64126.12345678", "timestamp": 1788973001000, "source": "chainlink"}}  # fmt: skip
    assert parse_polybolt(live) == [(1788973001000, "chainlink", "btcusd", 64126.12345678, 0)]
    twap = {"channel": "price.crypto.twap", "payload": {"symbol": "ethusd", "value": 3000.5, "timestamp": 5, "window_seconds": 60, "source": "chainlink"}}
    assert parse_polybolt(twap) == [(5, "chainlink_twap", "ethusd", 3000.5, 0)]
    snap = {"channel": "price.crypto", "snapshot": True, "payload": {"symbol": "btcusd", "source": "pyth", "data": [
        {"timestamp": 1, "value": 1.5, "full_accuracy_value": "1.50"}, {"timestamp": 2, "value": 2.5}]}}  # fmt: skip
    assert parse_polybolt(snap) == [(1, "pyth", "btcusd", 1.5, 1), (2, "pyth", "btcusd", 2.5, 1)]
    assert parse_polybolt({"channel": "price.crypto", "snapshot": True, "payload": {"symbol": "btcusd", "data": []}}) == []
    for other in ({"op": "subscribed", "channel": "price.crypto"}, {"channel": "price.equity", "payload": {}}, {"channel": "price.crypto"}, []):
        assert parse_polybolt(other) == []


def test_subscriptions_cover_both_channels():
    subs = subscriptions(["btc", "hype"])
    assert {"channel": "price.crypto", "filter": {"symbol": "hypeusd", "provider": "chainlink"}} in subs
    assert {"channel": "price.crypto.twap", "filter": {"symbol": "btcusd", "window_seconds": 60}} in subs
    assert len(subscriptions(["btc", "eth", "sol", "xrp", "doge", "hype", "bnb"])) == 14  # server limit is 64


class FakePolyBolt:
    def __init__(self, accept=True):
        self.accept, self.frames, self.conns = accept, [], 0

    async def handler(self, ws):
        self.conns += 1
        async for raw in ws:
            msg = json.loads(raw)
            self.frames.append(msg)
            if msg["op"] == "auth":
                ok = self.accept and msg["auth"] == {"apiKey": CREDS.api_key, "secret": CREDS.secret, "passphrase": CREDS.passphrase}
                await ws.send(json.dumps({"op": "authed", "rid": msg["rid"]} if ok else {"op": "error", "code": "auth_invalid", "rid": msg["rid"]}))
            elif msg["op"] == "subscribe":
                for i, sub in enumerate(msg["subscriptions"]):
                    sym = sub["filter"]["symbol"]
                    await ws.send(json.dumps({"op": "subscribed", "channel": sub["channel"], "rid": msg["rid"]}))
                    await ws.send(json.dumps({"v": 1, "channel": sub["channel"], "seq": 1, "ts": 1, "snapshot": True,
                                              "payload": {"symbol": sym, "source": "chainlink", "data": []}}))  # fmt: skip
                    await ws.send(json.dumps({"v": 1, "channel": sub["channel"], "seq": 2, "ts": 2, "payload": {
                        "symbol": sym, "value": 100.0 + i, "full_accuracy_value": f"{100.0 + i:.8f}", "timestamp": 2, "source": "chainlink"}}))  # fmt: skip


def run_feed(tmp_path, srv, until, timeout=8.0):
    async def go():
        async with serve(srv.handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            lg = RawLog(tmp_path, "chainlink")
            feed = PolyBoltWS(f"ws://127.0.0.1:{port}", CREDS, ["btc", "eth"], lg)
            task = asyncio.create_task(feed.run())
            t0 = time.monotonic()
            while not until(feed) and time.monotonic() - t0 < timeout:
                await asyncio.sleep(0.02)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            lg.close()
            return feed

    return asyncio.run(go())


def test_feed_authenticates_subscribes_and_records(tmp_path):
    srv = FakePolyBolt()
    feed = run_feed(tmp_path, srv, lambda f: f.n_msgs >= 12)
    assert [f["op"] for f in srv.frames] == ["auth", "subscribe"]
    assert srv.frames[1]["subscriptions"] == subscriptions(["btc", "eth"])
    now = time.time_ns()
    recs = list(iter_raw(tmp_path, "chainlink", now - 3600 * 10**9, now + 3600 * 10**9))
    assert recs[0]["ctl"] == "connected" and recs[-1]["ctl"] == "disconnected"
    points = [p for r in recs if "m" in r for p in parse_polybolt(r["m"])]
    assert {(feed_, sym) for _, feed_, sym, _, _ in points} == {
        ("chainlink", "btcusd"), ("chainlink_twap", "btcusd"), ("chainlink", "ethusd"), ("chainlink_twap", "ethusd")}  # fmt: skip
    assert feed.n_msgs >= 12 and not feed.connected
    # The credentials must never reach disk.
    raw = b"".join(p.read_bytes() for p in (tmp_path / "raw").rglob("*") if p.is_file())
    for secret in (CREDS.api_key, CREDS.secret, CREDS.passphrase):
        assert secret.encode() not in raw
    assert "s3cr3t" not in repr(CREDS)


def test_rejected_credentials_back_off_slowly(tmp_path, monkeypatch):
    sleeps = []
    real_sleep = asyncio.sleep

    async def fake_sleep(d):
        sleeps.append(d)
        await real_sleep(0.05 if d >= 1 else d)

    monkeypatch.setattr(cl.asyncio, "sleep", fake_sleep)
    srv = FakePolyBolt(accept=False)
    feed = run_feed(tmp_path, srv, lambda f: sleeps.count(cl.AUTH_RETRY_SECONDS) >= 2)
    assert sleeps.count(cl.AUTH_RETRY_SECONDS) >= 2  # (the server library's own timers also sleep)
    assert srv.conns == sleeps.count(cl.AUTH_RETRY_SECONDS)  # one attempt per long wait, no fast retries
    assert "credentials rejected" in feed.last_error and feed.n_msgs == 0
    assert all(f["op"] == "auth" for f in srv.frames)  # never subscribed


def test_env_file_and_credentials(tmp_path, monkeypatch):
    for v in CRED_VARS:
        monkeypatch.delenv(v, raising=False)
    assert clob_creds() is None
    env = tmp_path / "polyrec.env"
    env.write_text("# comment\nPOLYMARKET_API_KEY=k\nPOLYMARKET_API_SECRET = 's'\n\nPOLYMARKET_API_PASSPHRASE=\"p\"\n")
    monkeypatch.setenv("POLYMARKET_API_KEY", "from-environment")  # the real environment wins
    assert load_env_file(env) and not load_env_file(tmp_path / "missing.env")
    c = clob_creds()
    assert (c.api_key, c.secret, c.passphrase) == ("from-environment", "s", "p")
    monkeypatch.delenv("POLYMARKET_API_SECRET")
    assert clob_creds() is None  # all three or nothing


def test_credential_helper_signs_as_the_wallet_and_writes_private_file(tmp_path):
    eth_account = pytest.importorskip("eth_account")
    from eth_account.messages import encode_typed_data

    from polyrec.creds import clob_auth_typed_data, l1_headers, write_env

    acct = eth_account.Account.create()
    h = l1_headers(acct, timestamp=1_700_000_000)
    assert h["POLY_ADDRESS"] == acct.address and h["POLY_TIMESTAMP"] == "1700000000" and h["POLY_NONCE"] == "0"
    signable = encode_typed_data(full_message=clob_auth_typed_data(acct.address, 1_700_000_000))
    assert eth_account.Account.recover_message(signable, signature=h["POLY_SIGNATURE"]) == acct.address
    out = tmp_path / "polyrec.env"
    write_env(out, {"POLYMARKET_API_KEY": "k", "POLYMARKET_API_SECRET": "s", "POLYMARKET_API_PASSPHRASE": "p"}, "test")
    assert stat.S_IMODE(os.stat(out).st_mode) == 0o600
    assert "POLYMARKET_API_SECRET=s" in out.read_text()
