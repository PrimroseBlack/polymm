"""End-to-end replay of a hand-built raw log: one 5m market with a websocket outage in the middle."""
import json
import math
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from polyrec.config import Config
from polyrec.derive import DELTA, DESYNC, LEVEL, RESET, derive_day, parse_ref, pending_days, print_report
from polyrec.rawlog import RawLog
from polyrec.replay import replay
from polyrec.util import day_start

DAY = "2026-01-10"
T0 = day_start(DAY) + 600  # window 00:10:00 - 00:15:00 UTC
NS = 10**9
CID, UP, DOWN = "0xc1d", "111", "222"


def ns(sec: float) -> int:
    return round((T0 + sec) * NS)


def lv(*pairs):
    return [{"price": str(p), "size": str(s)} for p, s in pairs]


def book(asset, bids, asks, ts):
    return {"event_type": "book", "asset_id": asset, "market": CID, "timestamp": str(ts), "bids": bids, "asks": asks}


def event(start, closed=False, prices='["0.5", "0.5"]', meta=None, cid=CID, up=UP, down=DOWN):
    iso = lambda t: __import__("polyrec.util").util.iso(t)  # noqa: E731
    return {
        "id": "9", "slug": f"btc-updown-5m-{start}", "startTime": iso(start), "eventMetadata": meta,
        "markets": [{
            "id": "77", "conditionId": cid, "outcomes": '["Up", "Down"]', "clobTokenIds": json.dumps([up, down]),
            "endDate": iso(start + 300), "eventStartTime": iso(start), "closed": closed, "outcomePrices": prices,
            "volumeNum": 1234.5, "orderPriceMinTickSize": 0.01, "feeSchedule": {"rate": 0.07},
            "resolutionSource": "https://example.test/stream",
        }],
    }  # fmt: skip


@pytest.fixture(scope="module")
def derived(tmp_path_factory):
    root = tmp_path_factory.mktemp("data")
    ws, rest, meta = RawLog(root, "ws"), RawLog(root, "rest"), RawLog(root, "meta")

    meta.record(ns(-200), k="discovered", cid=CID, coin="btc", dur="5m", slug="s", ev=event(T0))
    meta.record(ns(330), k="final", cid=CID, coin="btc", dur="5m", slug="s",
                ev=event(T0, closed=True, prices='["1", "0"]', meta={"priceToBeat": 65000.5}))  # fmt: skip
    # The following window exists only to supply this one's final price (= its price to beat).
    meta.record(ns(-100), k="discovered", cid="0xnext", coin="btc", dur="5m", slug="n",
                ev=event(T0 + 300, cid="0xnext", up="333", down="444"))  # fmt: skip
    meta.record(ns(640), k="final", cid="0xnext", coin="btc", dur="5m", slug="n",
                ev=event(T0 + 300, closed=True, prices='["0", "1"]', meta={"priceToBeat": 65100.25},
                         cid="0xnext", up="333", down="444"))  # fmt: skip

    w: list[tuple[int, dict]] = []  # websocket log, sorted by time below
    w.append((ns(-130), {"c": 0, "ctl": "start"}))
    w.append((ns(-125), {"c": 1, "ctl": "connected"}))
    w.append((ns(-119.5), {"c": 1, "m": [  # subscribed at start - lead; connected earlier
        book(UP, lv((0.49, 100), (0.48, 50)), lv((0.51, 80)), 1000),
        book(DOWN, lv((0.49, 80)), lv((0.51, 100), (0.52, 50)), 1000),
    ]}))  # fmt: skip
    for s in range(-125, 100, 5):
        w.append((ns(s + 0.01), {"c": 1, "ctl": "hb"}))
    w.append((ns(10.2), {"c": 1, "m": {"event_type": "price_change", "market": CID, "timestamp": "2000", "price_changes": [
        {"asset_id": UP, "price": "0.50", "size": "30", "side": "BUY"},
        {"asset_id": DOWN, "price": "0.50", "size": "30", "side": "SELL"},
        {"asset_id": "999", "price": "0.50", "size": "30", "side": "SELL"},  # another market: ignored
    ]}}))  # fmt: skip
    w.append((ns(20.3), {"c": 1, "m": {"event_type": "last_trade_price", "asset_id": UP, "market": CID, "price": "0.51",
                                        "size": "12.5", "side": "BUY", "fee_rate_bps": "0", "timestamp": "3000",
                                        "transaction_hash": "0xfeed"}}))  # fmt: skip
    w.append((ns(100), {"c": 1, "ctl": "disconnected", "why": "test"}))
    w.append((ns(111), {"c": 2, "ctl": "connected"}))
    w.append((ns(111.2), {"c": 2, "m": [book(UP, lv((0.42, 5)), lv((0.44, 5)), 5000),
                                         book(DOWN, lv((0.56, 5)), lv((0.58, 5)), 5000)]}))  # fmt: skip
    for s in range(111, 306, 5):
        w.append((ns(s + 0.01), {"c": 2, "ctl": "hb"}))
    w.append((ns(200.5), {"c": 2, "m": {"event_type": "price_change", "market": CID, "timestamp": "6000", "price_changes": [
        {"asset_id": UP, "price": "0.42", "size": "0", "side": "BUY"}]}}))  # fmt: skip
    w.append((ns(301), {"c": 2, "m": {"event_type": "market_resolved", "market": CID, "winning_outcome": "Up"}}))
    for r, rec in sorted(w, key=lambda x: x[0]):
        ws.record(r, **rec)

    # REST: one-per-second fallback snapshots during the outage, then a routine cross-check.
    for s in range(101, 111):
        rest.record(ns(s + 0.1), s=0, fb=1, m=[
            {"asset_id": UP, "timestamp": "4000", "bids": lv((0.40, 7 if s < 108 else 9)), "asks": lv((0.45, 7))},
            {"asset_id": DOWN, "timestamp": "4000", "bids": lv((0.55, 7)), "asks": lv((0.60, 7))},
        ])  # fmt: skip
    rest.record(ns(150.1), s=0, fb=0, m=[{"asset_id": UP, "timestamp": "5500", "bids": lv((0.42, 5)), "asks": lv((0.44, 5))}])
    rest.record(ns(250.1), s=0, fb=0, m=[{"asset_id": UP, "timestamp": "6500", "bids": lv((0.30, 1)), "asks": lv((0.44, 5))}])

    aux = RawLog(root, "aux_binance_spot")
    aux.record(ns(5), m={"stream": "btcusdt@kline_1s", "data": {"e": "kline", "E": 1, "s": "BTCUSDT", "k": {
        "t": 1000, "T": 1999, "s": "BTCUSDT", "i": "1s", "o": "1", "c": "65001.5", "h": "1", "l": "1", "x": True}}})  # fmt: skip
    aux.record(ns(6), ctl="connected")

    # Chainlink via PolyBolt: snapshot, live price, 60 s average, another coin, a reconnect snapshot.
    cl = RawLog(root, "chainlink")

    def env(channel, payload, snap=False):
        return {"v": 1, "channel": channel, "seq": 1, "ts": 1, "payload": payload, **({"snapshot": True} if snap else {})}

    def pt(ts, value):
        return {"timestamp": ts, "value": value, "full_accuracy_value": f"{value:.8f}"}

    cl.record(ns(1), ctl="connected")
    cl.record(ns(1.1), m={"op": "subscribed", "channel": "price.crypto", "rid": "s1"})
    cl.record(ns(1.2), m=env("price.crypto", {"symbol": "btcusd", "source": "chainlink", "data": [pt(900, 65000.0), pt(1000, 65005.0)]}, snap=True))
    cl.record(ns(5.5), m=env("price.crypto", {"symbol": "btcusd", "source": "chainlink", **pt(5500, 65010.5)}))
    cl.record(ns(6.5), m=env("price.crypto.twap", {"symbol": "btcusd", "source": "chainlink", "window_seconds": 60, **pt(6500, 65002.25)}))
    cl.record(ns(7), m=env("price.crypto", {"symbol": "ethusd", "source": "chainlink", **pt(7000, 3000.0)}))
    cl.record(ns(9), m=env("price.crypto", {"symbol": "btcusd", "source": "chainlink", "data": [pt(5500, 65010.5), pt(8000, 65020.0)]}, snap=True))
    for lg in (ws, rest, meta, aux, cl):
        lg.close()

    cfg = Config(data_dir=root)
    report = derive_day(cfg, DAY, fetch_missing=False)
    read = lambda name: pq.read_table(root / "derived" / name / f"date={DAY}" / f"{name}.parquet").to_pylist()  # noqa: E731
    return cfg, report, {n: read(n) for n in ("ticks", "l2", "trades", "markets", "ref")}


def tick(derived, sec):
    rows = [r for r in derived[2]["ticks"] if r["condition_id"] == CID and r["sec"] == sec]
    return rows[0] if rows else None


def test_ticks_cover_lead_and_window(derived):
    secs = [r["sec"] for r in derived[2]["ticks"] if r["condition_id"] == CID]
    assert secs[0] == -119 and secs[-1] == 299  # first second after the snapshot .. last second of the window
    assert secs == sorted(secs) and len(set(secs)) == len(secs)
    assert 101 not in secs  # disconnected at 100.0, first REST snapshot only at 101.1
    assert len([s for s in secs if s >= 0]) == 299


def test_tick_is_state_as_of_the_second(derived):
    a, b = tick(derived, 10), tick(derived, 11)  # the change arrived at 10.2
    assert (a["bu"], a["su"], a["au"]) == (0.49, 100.0, 0.51)
    assert (b["bu"], b["su"], b["ad"], b["sad"]) == (0.5, 30.0, 0.5, 30.0)
    assert b["bid_px"] == [0.5, 0.49, 0.48] and b["bid_sz"] == [30.0, 100.0, 50.0] and b["ask_px"] == [0.51]
    assert math.isclose(b["du"], 0.5 * 30 + 0.49 * 100 + 0.48 * 50) and math.isclose(b["dau"], 0.51 * 80)
    assert a["src"] == 0 and a["t"] == T0 + 10 and int(a["ts_utc"].timestamp()) == T0 + 10
    assert b["age_ms"] == 800


def test_rest_fallback_fills_the_outage(derived):
    assert tick(derived, 100)["bu"] == 0.5 and tick(derived, 100)["src"] == 0
    for sec in range(102, 112):
        t = tick(derived, sec)
        assert (t["bu"], t["au"], t["bd"], t["src"]) == (0.40, 0.45, 0.55, 1), sec
        assert t["su"] == (7.0 if sec <= 108 else 9.0), sec
    t = tick(derived, 112)  # websocket snapshot arrived at 111.2
    assert (t["bu"], t["au"], t["bd"], t["src"]) == (0.42, 0.44, 0.56, 0)


def test_removed_level_and_empty_side(derived):
    t = tick(derived, 201)
    assert math.isnan(t["bu"]) and math.isnan(t["su"]) and t["au"] == 0.44 and t["bid_px"] == []


def test_markets_row(derived):
    rows = {r["condition_id"]: r for r in derived[2]["markets"]}
    m = rows[CID]
    assert m["outcome"] == "Up" and m["ws_outcome"] == "Up"
    assert m["price_to_beat"] == 65000.5 and m["final_price"] == 65100.25  # from the next window
    assert (m["n_ticks_window"], m["expected_ticks"], m["rest_ticks"], m["n_trades"]) == (299, 300, 10, 1)
    assert (m["rest_checks"], m["rest_mismatches"]) == (2, 1)  # routine checks never overwrite the live book
    assert m["volume"] == 1234.5 and m["fee_rate"] == 0.07 and m["tick_size"] == 0.01
    assert int(m["market_start"].timestamp()) == T0 and m["token_up"] == UP
    assert rows["0xnext"]["outcome"] == "Down" and rows["0xnext"]["n_ticks"] == 0


def test_trades(derived):
    (t,) = derived[2]["trades"]
    assert (t["outcome"], t["side"], t["price"], t["size"], t["tx_hash"], t["ts_ms"]) == ("Up", "BUY", 0.51, 12.5, "0xfeed", 3000)
    assert t["r"] == ns(20.3) and t["coin"] == "btc" and t["duration"] == "5m"


def test_l2_replays_to_the_same_books(derived):
    rows = [r for r in derived[2]["l2"] if r["condition_id"] == CID and r["outcome"] == "Up"]
    assert [r["kind"] for r in rows[:4]] == [RESET, LEVEL, LEVEL, LEVEL]
    assert DESYNC in [r["kind"] for r in rows] and DELTA in [r["kind"] for r in rows]
    at = {}
    for row, b in replay(rows):
        at[row["r"]] = (b.src, b.best())
    assert at[ns(10.2)][1][:2] == (0.5, 30.0)
    assert at[ns(100)][0] is None  # unknown from the disconnect ...
    assert at[ns(101.1)][1][0] == 0.40  # ... until the first REST snapshot
    assert math.isnan(at[ns(200.5)][1][0]) and at[ns(200.5)][1][2] == 0.44
    outage = [r for r in rows if ns(101) < r["r"] < ns(111)]
    assert {r["src"] for r in outage} == {1}
    # Ten identical-but-one REST snapshots are stored as one snapshot plus one change.
    assert [r["kind"] for r in outage] == [RESET, LEVEL, LEVEL, DELTA]
    assert at[ns(101.1)][1][:2] == (0.40, 7.0) and at[ns(108.1)][1][:2] == (0.40, 9.0)
    assert ns(107.1) not in at  # an unchanged snapshot adds no rows


def test_chainlink_columns_are_causal_and_expire(derived):
    cl = {sec: (tick(derived, sec)["cl"], tick(derived, sec)["cl_twap"]) for sec in (1, 2, 6, 7, 10, 36, 37, 38, 39)}
    assert cl[1] == (None, None)  # the snapshot arrived at 1.2, after instant 1
    assert cl[2] == (65005.0, None)  # newest point of the snapshot
    assert cl[6] == (65010.5, None) and cl[7] == (65010.5, 65002.25)  # ETH's price is not attached to BTC
    assert cl[10] == (65020.0, 65002.25)
    assert cl[36] == (65020.0, 65002.25) and cl[37] == (65020.0, None)  # average last heard at 6.5
    assert cl[38] == (65020.0, None) and cl[39] == (None, None)  # price last heard at 9.0


def test_reference_prices(derived):
    ref = derived[2]["ref"]
    assert [r for r in ref if r["feed"] == "binance_spot"] == [
        {"r": ns(5), "ts_ms": 1999, "feed": "binance_spot", "symbol": "BTCUSDT", "price": 65001.5, "snapshot": 0}
    ]
    got = [(r["feed"], r["symbol"], r["ts_ms"], r["price"], r["snapshot"]) for r in ref if r["feed"] != "binance_spot"]
    assert got == [
        ("chainlink", "btcusd", 900, 65000.0, 1),
        ("chainlink", "btcusd", 1000, 65005.0, 1),
        ("chainlink", "btcusd", 5500, 65010.5, 0),
        ("chainlink_twap", "btcusd", 6500, 65002.25, 0),
        ("chainlink", "ethusd", 7000, 3000.0, 0),
        ("chainlink", "btcusd", 8000, 65020.0, 1),  # the reconnect snapshot adds only the point not yet seen
    ]
    assert parse_ref({"e": "markPriceUpdate", "E": 7, "s": "HYPEUSDT", "p": "31.5"}) == (7, "HYPEUSDT", 31.5)
    assert parse_ref({"e": "kline", "s": "X", "k": {"T": 1, "c": "1", "x": False}}) is None  # candle still open
    assert parse_ref({"result": None, "id": 1}) is None


def test_report(derived, capsys):
    cfg, report, _ = derived
    s = report["series"]["btc-5m"]
    assert (s["expected"], s["recorded"], s["complete"], s["resolved"]) == (288, 2, 1, 2)
    assert len(s["missing_windows"]) == 286 and s["incomplete_windows"] == [T0 + 300]
    assert s["missing_seconds"] == 1 + 300 + 286 * 300 and s["rest_seconds"] == 10
    assert report["disconnects"] == 1 and report["trades"] == 1
    assert print_report(cfg, DAY) == 0 and "btc-5m" in capsys.readouterr().out
    assert pending_days(cfg) == []  # already derived
    assert (Path(cfg.data_dir) / "derived" / "_done" / f"{DAY}.json").exists()


def test_prune_keeps_what_the_next_derive_needs(tmp_path):
    import time

    from polyrec.derive import prune
    from polyrec.util import day_of

    now = time.time()
    day = lambda n: day_of(now - n * 86400)  # noqa: E731

    def make(retention, done):
        root = tmp_path / f"r{retention}-{len(done)}"
        for n in range(0, 6):
            for stream in ("ws", "rest"):
                d = root / "raw" / stream / day(n)
                d.mkdir(parents=True)
                for hh in ("00", "22", "23"):
                    (d / f"{stream}-{day(n).replace('-', '')}T{hh}.jsonl.zst").write_text("x")
        (root / "derived" / "_done").mkdir(parents=True)
        for n in done:
            (root / "derived" / "_done" / f"{day(n)}.json").write_text("{}")
        return Config(data_dir=root, raw_retention_days=retention), root

    def left(root):
        return sorted(f"{p.parent.name[-2:]}T{p.name[-12:-10]}" for p in (root / "raw" / "ws").glob("*/*"))

    full = lambda ns: sorted(f"{day(n)[-2:]}T{hh}" for n in ns for hh in ("00", "22", "23"))  # noqa: E731

    # Days 5, 4, 3 derived; day 2 is next. It reads from 23:00 of day 3, so everything before goes.
    cfg, root = make(0, done=(5, 4, 3))
    removed = prune(cfg)
    assert left(root) == sorted([f"{day(3)[-2:]}T23", *full((2, 1, 0))])
    assert len(removed) == 2 * (3 + 3 + 2) and not (root / "raw" / "rest" / day(5)).exists()
    assert prune(cfg) == []

    # Nothing derived yet: nothing may be deleted, however old.
    cfg, root = make(0, done=())
    assert prune(cfg) == [] and left(root) == full(range(6))

    # A gap: day 4 was never derived, so days 4 onwards stay even though 3 is done.
    cfg, root = make(0, done=(5, 3))
    prune(cfg)
    assert left(root) == sorted([f"{day(5)[-2:]}T23", *full((4, 3, 2, 1, 0))])

    # Retention keeps derived days longer: with 4 days, only files older than that go.
    cfg, root = make(4, done=(5, 4, 3))
    prune(cfg)
    kept = left(root)
    assert f"{day(3)[-2:]}T00" in kept and f"{day(5)[-2:]}T00" not in kept


def test_exchange_snapshots_are_checked_against_the_rebuilt_book(tmp_path):
    """The websocket re-sends the whole book on every trade; the replay must already hold it."""
    ws, meta = RawLog(tmp_path, "ws"), RawLog(tmp_path, "meta")
    meta.record(ns(-200), k="discovered", cid=CID, coin="btc", dur="5m", slug="s", ev=event(T0))
    change = lambda asset, price, size, side: {"asset_id": asset, "price": price, "size": size, "side": side}  # noqa: E731
    w = [
        (ns(-50), {"c": 1, "s": 0, "ctl": "connected"}),
        (ns(-49), {"c": 1, "s": 0, "m": [book(UP, lv((0.49, 100)), lv((0.51, 80)), 1), book(DOWN, lv((0.49, 80)), lv((0.51, 100)), 1)]}),
        (ns(10), {"c": 1, "s": 0, "m": {"event_type": "price_change", "timestamp": "2", "price_changes": [
            change(UP, "0.50", "30", "BUY"), change(DOWN, "0.50", "30", "SELL")]}}),
        # Same book the deltas produced: agrees, and adds nothing to l2.
        (ns(20), {"c": 1, "s": 0, "m": [book(UP, lv((0.49, 100), (0.50, 30)), lv((0.51, 80)), 3)]}),
        # A book the deltas did not produce (0.50 is 31, 0.49 is gone): counted, and l2 is corrected.
        (ns(30), {"c": 1, "s": 0, "m": [book(UP, lv((0.50, 31)), lv((0.51, 80)), 4)]}),
    ]
    w += [(ns(sec + 0.5), {"c": 1, "s": 0, "ctl": "hb"}) for sec in range(-50, 60, 5)]
    for r, rec in sorted(w, key=lambda x: x[0]):
        ws.record(r, **rec)
    ws.close()
    meta.close()
    report = derive_day(Config(data_dir=tmp_path), DAY, fetch_missing=False)
    read = lambda name: pq.read_table(tmp_path / "derived" / name / f"date={DAY}" / f"{name}.parquet").to_pylist()  # noqa: E731
    (m,) = read("markets")
    assert (m["resyncs"], m["resync_dirty"]) == (2, 1) and (report["resyncs"], report["resync_dirty"]) == (2, 1)
    assert (report["resync_levels"], report["resync_top"]) == (2, 0)  # two levels differed; best bid stayed 0.50
    l2 = read("l2")
    assert {r["outcome"] for r in l2} == {"Up"}  # the mirrored Down book is not stored
    assert [(r["kind"], r["side"], r["price"], r["size"]) for r in l2] == [
        (RESET, -1, None, None), (LEVEL, 0, 0.49, 100.0), (LEVEL, 1, 0.51, 80.0),
        (DELTA, 0, 0.5, 30.0),
        (DELTA, 0, 0.49, 0.0), (DELTA, 0, 0.5, 31.0),
    ]  # fmt: skip
    ticks = {t["sec"]: t for t in read("ticks")}
    assert ticks[25]["bu"] == 0.5 and ticks[25]["su"] == 30.0 and ticks[25]["ad"] == 0.5  # Down still tracked
    assert ticks[31]["bid_px"] == [0.5] and ticks[31]["su"] == 31.0

    # With both outcomes stored, the Down book appears too.
    out2 = tmp_path / "both"
    derive_day(Config(data_dir=tmp_path, l2_both_outcomes=True), DAY, fetch_missing=False, out=out2, mark_done=False)
    both = pq.read_table(out2 / "l2" / f"date={DAY}" / "l2.parquet").to_pylist()
    assert {r["outcome"] for r in both} == {"Up", "Down"} and not (out2 / "_done").exists()


def test_trial_measures_without_touching_real_tables(tmp_path, capsys, monkeypatch):
    import time as _time

    from polyrec import derive as dv

    now = int(_time.time())
    start = (now - 900) // 300 * 300  # a 5m market that began 10-15 minutes ago
    ws, meta = RawLog(tmp_path, "ws"), RawLog(tmp_path, "meta")
    meta.record((start - 100) * NS, k="discovered", cid=CID, coin="btc", dur="5m", slug="s", ev=event(start))
    ws.record((start - 60) * NS, c=1, s=0, ctl="connected")
    ws.record((start - 59) * NS, c=1, s=0, m=[book(UP, lv((0.49, 100)), lv((0.51, 80)), 1)])
    for sec in range(-55, 300, 5):
        ws.record((start + sec) * NS, c=1, s=0, ctl="hb")
        ws.record((start + sec) * NS + 1, c=1, s=0, m=[book(UP, lv((0.49, 100)), lv((0.51, 80)), 2)])
    ws.close()
    meta.close()
    assert dv.trial(Config(data_dir=tmp_path), minutes=20) == 0
    out = capsys.readouterr().out
    assert "1 markets" in out and "book check" in out and "of 71 agree (100.00%)" in out
    assert "projected per day" in out and "replay speed" in out
    assert not (tmp_path / "derived").exists() and not (tmp_path / "trial_tmp").exists()


def test_a_dropped_connection_only_interrupts_its_own_markets(tmp_path):
    """Two markets on two websocket connections; one connection drops for ten seconds."""
    root = tmp_path
    ws, meta = RawLog(root, "ws"), RawLog(root, "meta")
    meta.record(ns(-200), k="discovered", cid="0xb", coin="btc", dur="5m", slug="b", ev=event(T0, cid="0xb", up="b1", down="b2"))
    meta.record(ns(-200), k="discovered", cid="0xe", coin="eth", dur="5m", slug="e", ev=event(T0, cid="0xe", up="e1", down="e2"))
    w = [(ns(-60), {"c": 0, "ctl": "start"})]
    for shard, tok in ((0, "b1"), (1, "e1")):
        w.append((ns(-50), {"c": 1, "s": shard, "ctl": "connected"}))
        w.append((ns(-49.5), {"c": 1, "s": shard, "m": [book(tok, lv((0.40, 1)), lv((0.60, 1)), 1)]}))
    for sec in range(-50, 120, 5):
        w.append((ns(sec + 0.01), {"c": 1, "s": 0, "ctl": "hb"}))
        if not 50 <= sec < 60:
            w.append((ns(sec + 0.02), {"c": 2 if sec >= 60 else 1, "s": 1, "ctl": "hb"}))
    w.append((ns(50), {"c": 1, "s": 1, "ctl": "disconnected", "why": "slow consumer"}))
    w.append((ns(60), {"c": 2, "s": 1, "ctl": "connected"}))
    w.append((ns(60.5), {"c": 2, "s": 1, "m": [book("e1", lv((0.45, 1)), lv((0.55, 1)), 2)]}))
    for r, rec in sorted(w, key=lambda x: x[0]):
        ws.record(r, **rec)
    ws.close()
    meta.close()
    report = derive_day(Config(data_dir=root), DAY, fetch_missing=False)
    ticks = pq.read_table(root / "derived" / "ticks" / f"date={DAY}" / "ticks.parquet").to_pylist()
    secs = lambda cid: [t["sec"] for t in ticks if t["condition_id"] == cid and 40 <= t["sec"] < 70]  # noqa: E731
    assert secs("0xb") == list(range(40, 70))  # untouched by the other connection's drop
    assert secs("0xe") == [*range(40, 51), *range(61, 70)]  # gap from the drop to the new snapshot
    bu = {t["sec"]: t["bu"] for t in ticks if t["condition_id"] == "0xe"}
    assert bu[50] == 0.40 and bu[61] == 0.45
    assert report["disconnects"] == 1


def test_l2_rolling_window_drops_only_old_l2(tmp_path):
    import time

    from polyrec.derive import prune
    from polyrec.util import day_of

    day = lambda n: day_of(time.time() - n * 86400)  # noqa: E731
    for table in ("l2", "ticks", "trades", "markets"):
        for n in range(0, 6):
            d = tmp_path / "derived" / table / f"date={day(n)}"
            d.mkdir(parents=True)
            (d / f"{table}.parquet").write_text("x")
    kept = lambda table: sorted(p.name[5:] for p in (tmp_path / "derived" / table).iterdir())  # noqa: E731

    assert prune(Config(data_dir=tmp_path)) == []  # the default keeps everything
    assert len(kept("l2")) == 6

    removed = prune(Config(data_dir=tmp_path, l2_retention_days=3))
    assert removed == [f"l2/date={day(5)}", f"l2/date={day(4)}"]
    assert kept("l2") == sorted(day(n) for n in (3, 2, 1, 0))
    for table in ("ticks", "trades", "markets"):
        assert len(kept(table)) == 6  # the small tables are never touched
