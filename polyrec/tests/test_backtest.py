"""Backtest on a synthetic, perfectly efficient market: nothing should beat the fee."""
import math

import numpy as np
import pytest

from polyrec import backtest as bt
from polyrec.config import Config
from polyrec.derive import MARKETS, TICKS, TRADES, Sink
from polyrec.util import day_of, day_start

DAY0 = day_start("2026-03-01")
N_DAYS, PER_DAY, DUR = 4, 120, 300
SIGMA = 1.2e-4  # per-second price volatility


def phi(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def make_market(rng, start):
    """Per-second ticks whose mid is the true probability that Up wins, +/- half a cent."""
    steps = rng.normal(0, SIGMA, DUR + 130)
    cl = 65000 * np.exp(np.cumsum(steps))
    ptb = cl[130]
    up = cl[130 + DUR - 1] >= ptb  # settle on the last price so the simple formula below is the true probability
    rows = []
    for sec in range(-120, DUR):
        price = cl[sec + 130]
        left = max(DUR - 1 - sec, 1)
        p = phi(math.log(price / ptb) / (SIGMA * math.sqrt(left))) if sec >= 0 else 0.5
        p = min(max(round(p, 2), 0.02), 0.98)
        bu, au = round(p - 0.01, 2), round(p + 0.01, 2)
        rows.append((start + sec, sec, bu, au, round(1 - au, 2), round(1 - bu, 2), float(price)))
    return rows, bool(up), float(ptb)


@pytest.fixture(scope="module")
def data(tmp_path_factory):
    root = tmp_path_factory.mktemp("bt")
    rng = np.random.default_rng(7)
    for d in range(N_DAYS):
        day = day_of(DAY0 + d * 86400)
        part = f"date={day}"
        ticks = Sink(root / "derived" / "ticks" / part / "ticks.parquet", TICKS)
        mk = Sink(root / "derived" / "markets" / part / "markets.parquet", MARKETS)
        tr = Sink(root / "derived" / "trades" / part / "trades.parquet", TRADES)
        for i in range(PER_DAY):
            start = DAY0 + d * 86400 + i * 300
            cid = f"0x{d:02d}{i:04d}"
            rows, up, ptb = make_market(rng, start)
            for t, sec, bu, au, bd, ad, price in rows:
                ticks.add(cid, "btc", "5m", t, t, sec, bu, au, bd, ad, 100.0, 100.0, 100.0, 100.0, 50.0, 50.0, 50.0, 50.0,
                          [bu], [100.0], [au], [100.0], 0, 100, price, price)
            row = dict.fromkeys(MARKETS.names)
            row.update(condition_id=cid, coin="btc", duration="5m", market_start=start, market_end=start + 300,
                       outcome="Up" if up else "Down", price_to_beat=ptb, n_ticks=420, n_ticks_window=300, expected_ticks=300)
            mk.add(*(row[n] for n in MARKETS.names))
        for s in (ticks, mk, tr):
            s.close()
    return Config(data_dir=root)


def md(up_won=True, **arrays):
    a = {k: np.full(300 + bt.PRE, np.nan) for k in ("bu", "au", "bd", "ad", "su", "sau", "sd", "sad", "cl")}
    for k, (sec, v) in arrays.items():
        a[k][sec + bt.PRE :] = v
    return bt.MarketData(0, 300, up_won, 100.0, a)


def test_taker_pays_ask_and_fee():
    m = md(up_won=True, au=(0, 0.60), ad=(0, 0.42))
    pnl, cost = bt.taker(m, True, 10)
    assert math.isclose(cost, 0.60 + 0.07 * 0.6 * 0.4) and math.isclose(pnl, 1 - cost)
    pnl, cost = bt.taker(m, False, 10)  # bought the losing side
    assert math.isclose(pnl, -(0.42 + 0.07 * 0.42 * 0.58))
    assert bt.taker(md(), True, 10) is None  # no ask recorded
    assert bt.taker(md(au=(0, 0.995)), True, 10) is None  # nothing to gain above 99c


def test_maker_fills_only_when_traded_through():
    base = dict(bu=(0, 0.50), su=(0, 200.0), au=(0, 0.52))
    m = md(up_won=True, **base)
    assert bt.maker(m, True, 100, 10) is None  # nobody sold into the bid
    m.tr["Up"] = (np.array([150.0, 160.0]), np.array([0.50, 0.50]), np.array([150.0, 50.0]))
    assert bt.maker(m, True, 100, 10) is None  # 200 traded, but 200 were queued ahead of our 10
    m.tr["Up"] = (np.array([150.0, 160.0]), np.array([0.50, 0.49]), np.array([150.0, 60.0]))
    assert bt.maker(m, True, 100, 10) == (0.5, 0.5)  # queue cleared: filled at the bid, no fee, Up won
    m.tr["Up"] = (np.array([50.0]), np.array([0.40]), np.array([999.0]))
    assert bt.maker(m, True, 100, 10) is None  # that trade happened before we posted
    m2 = md(up_won=False, **base)
    m2.a["au"][250 + bt.PRE :] = 0.45  # the ask later fell below our bid: certainly filled, and it lost
    assert bt.maker(m2, True, 100, 10) == (-0.5, 0.5)


def test_projected_gap_blends_the_final_minute():
    m = md(cl=(0, 100.0))
    m.a["cl"][240 + bt.PRE : 270 + bt.PRE] = 101.0  # first half of the final minute at 101
    m.a["cl"][270 + bt.PRE :] = 103.0
    assert math.isclose(bt.projected_gap(m, 100), 0.0)
    assert math.isclose(bt.projected_gap(m, 269), (101 * 30 + 101 * 30) / 60 / 100 - 1)  # 30 s seen, rest at current
    assert math.isclose(bt.projected_gap(m, 299), (102 * 59 + 103 * 1) / 60 / 100 - 1)  # 59 s seen (mean 102), 1 s to go


def test_summarize_range_and_empty():
    assert bt.summarize([])["n"] == 0
    s = bt.summarize([(0.4, 0.6)] * 30 + [(-0.6, 0.6)] * 30)
    assert s["n"] == 60 and math.isclose(s["win"], 0.5) and s["lo"] < s["roi"] < s["hi"]
    assert math.isclose(s["roi"], -0.2 / 0.6 * 0.5 + 0.0, abs_tol=0.2)


def test_efficient_market_gives_no_edge(data, capsys):
    assert bt.backtest(data, "btc", "5m") == 0
    out = capsys.readouterr().out
    assert "480 resolved markets" in out and "Honest test" in out and "Machine-learning model" in out
    con = __import__("duckdb").connect()
    markets = [m for d in range(N_DAYS) for m in bt.load_day(con, data.data_dir, "btc", "5m", day_of(DAY0 + d * 86400))]
    assert len(markets) == N_DAYS * PER_DAY
    # Taking the ask in a correctly priced market loses about the spread plus the fee.
    all_taker = [t for left in (120, 60) for t in bt.rule_trades(markets, 1, left, 1, 10, "taker")[0]]
    s = bt.summarize(all_taker)
    assert s["n"] > 500 and -0.08 < s["roi"] < 0.0
    assert bt.backtest(data, "eth", "5m") == 1  # no such data: says so instead of crashing
