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
    for lg in (ws, rest, meta, aux):
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


def test_reference_prices(derived):
    assert derived[2]["ref"] == [{"r": ns(5), "ts_ms": 1999, "feed": "binance_spot", "symbol": "BTCUSDT", "price": 65001.5}]
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
