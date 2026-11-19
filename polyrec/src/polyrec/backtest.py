"""`polyrec backtest`: replay strategies against the recorded per-second order book.

Built to avoid the usual ways a backtest flatters itself:
  * decisions use only what was known at that second, and orders execute `latency` seconds later;
  * taker orders pay the recorded ask plus Polymarket's fee, shares x 0.07 x p x (1 - p);
  * maker orders join the bid and fill only if later trades would have worked through the queue
    ahead of them (or the ask came down to the bid), so fills carry their adverse selection;
  * rules are chosen on the earlier 70% of markets and judged once on the later 30%;
  * every return comes with a bootstrap range, because a few hundred trades prove little.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from .config import DURATIONS, Config
from .util import day_start

FEE_RATE = 0.07
PRE = 130  # seconds of pre-window ticks kept (lead time)
SECS_LEFT = {"5m": (120, 60, 30, 15, 5), "15m": (300, 120, 60, 30, 10), "1h": (900, 300, 120, 60, 30),
             "4h": (1800, 600, 300, 120, 60)}  # fmt: skip
GAPS_BPS = (1, 2, 5, 10, 20)
NAN = float("nan")


def fee(p: float) -> float:
    """Taker fee per share bought at price p."""
    return FEE_RATE * p * (1 - p)


@dataclass
class MarketData:
    start: int
    dur: int
    up_won: bool
    ptb: float  # price to beat
    a: dict[str, np.ndarray]  # per-second arrays indexed by sec + PRE: bu au bd ad su sau sd sad cl
    tr: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = field(default_factory=dict)  # token -> (sec, price, size)

    def at(self, name: str, sec: int) -> float:
        i = sec + PRE
        arr = self.a[name]
        return float(arr[i]) if 0 <= i < len(arr) else NAN


def load_day(con, root: Path, coin: str, dur: str, day: str) -> list[MarketData]:
    """Markets of one coin/duration that started on `day`, with their ticks and trade prints."""
    base = root / "derived"
    files = {n: base / n / f"date={day}" / f"{n}.parquet" for n in ("markets", "ticks", "trades")}
    if not files["markets"].exists() or not files["ticks"].exists():
        return []
    n = DURATIONS[dur]
    mk = con.sql(f"""SELECT condition_id, epoch(market_start)::BIGINT AS s, outcome, price_to_beat
                     FROM read_parquet('{files['markets']}') WHERE coin = '{coin}' AND duration = '{dur}'
                     AND outcome IS NOT NULL ORDER BY s""").fetchall()
    if not mk:
        return []
    cols = ("bu", "au", "bd", "ad", "su", "sau", "sd", "sad", "cl")
    tk = con.sql(f"""SELECT condition_id, sec, {', '.join(cols)} FROM read_parquet('{files['ticks']}')
                     WHERE coin = '{coin}' AND duration = '{dur}' AND sec >= {-PRE} AND sec < {n}
                     ORDER BY condition_id, sec""").fetchnumpy()
    cids, starts = np.unique(tk["condition_id"], return_index=True)
    bounds = dict(zip(cids.tolist(), zip(starts.tolist(), [*starts[1:].tolist(), len(tk["sec"])])))
    trades: dict[str, dict[str, list]] = {}
    if files["trades"].exists():
        rows = con.sql(f"""SELECT condition_id, outcome, r, price, size FROM read_parquet('{files['trades']}')
                           WHERE coin = '{coin}' AND duration = '{dur}' ORDER BY r""").fetchall()
        for cid, outcome, r, price, size in rows:
            if price is not None and size is not None:
                trades.setdefault(cid, {}).setdefault(outcome, []).append((r, price, size))
    out = []
    for cid, start, outcome, ptb in mk:
        if cid not in bounds:
            continue
        lo, hi = bounds[cid]
        idx = tk["sec"][lo:hi].astype(np.int64) + PRE
        arrays = {}
        for c in cols:
            arr = np.full(n + PRE, np.nan)
            vals = tk[c][lo:hi]
            arr[idx] = np.ma.filled(vals.astype(float), np.nan) if isinstance(vals, np.ma.MaskedArray) else vals.astype(float)
            arrays[c] = arr
        if ptb is None or ptb != ptb:  # not published (4h markets): the first Chainlink price of the window
            first = arrays["cl"][PRE : PRE + 10]
            first = first[~np.isnan(first)]
            if not len(first):
                continue
            ptb = float(first[0])
        md = MarketData(int(start), n, outcome == "Up", float(ptb), arrays)
        for token, lst in trades.get(cid, {}).items():
            t = np.array(lst, dtype=float)
            md.tr[token] = (t[:, 0] / 1e9 - start, t[:, 1], t[:, 2])
        out.append(md)
    return out


def projected_gap(md: MarketData, sec: int) -> float:
    """Where the settlement value stands relative to the price to beat, as a fraction.

    Settlement is the average Chainlink price over the window's final 60 seconds. With more than
    60 s left that is just the current price; inside the final minute, the seconds already seen
    are averaged in and the current price stands in for the rest.
    """
    now = md.at("cl", sec)
    if now != now:
        return NAN
    left = md.dur - sec
    if left >= 60:
        return now / md.ptb - 1
    seen = md.a["cl"][md.dur - 60 + PRE : sec + PRE + 1]
    seen = seen[~np.isnan(seen)]
    if len(seen) < (60 - left) / 2:
        return NAN
    return (seen.mean() * (60 - left) + now * left) / 60 / md.ptb - 1


def taker(md: MarketData, buy_up: bool, sec: int) -> tuple[float, float] | None:
    """Buy at the ask at second `sec`. Returns (profit, cost) per share, or None if not possible."""
    p = md.at("au" if buy_up else "ad", sec)
    if not 0.01 <= p <= 0.99:
        return None
    won = md.up_won == buy_up
    return (1.0 if won else 0.0) - p - fee(p), p + fee(p)


def maker(md: MarketData, buy_up: bool, sec: int, shares: float) -> tuple[float, float] | None:
    """Join the best bid at second `sec` and wait. Returns (profit, cost) per share if it fills."""
    b = md.at("bu" if buy_up else "bd", sec)
    if not 0.01 <= b <= 0.99:
        return None
    ahead = md.at("su" if buy_up else "sd", sec)
    ahead = 0.0 if ahead != ahead else ahead
    asks = md.a["au" if buy_up else "ad"][sec + PRE + 1 :]
    filled = bool(np.nanmin(asks) <= b + 1e-9) if np.any(~np.isnan(asks)) else False  # the market traded through us
    if not filled:
        t, price, size = md.tr.get("Up" if buy_up else "Down", (np.empty(0),) * 3)
        hit = (t > sec) & (t < md.dur) & (price <= b + 1e-9)
        filled = float(size[hit].sum()) >= ahead + shares
    if not filled:
        return None
    won = md.up_won == buy_up
    return (1.0 if won else 0.0) - b, b


def summarize(trades: list[tuple[float, float]], rng=None, n_boot: int = 2000) -> dict:
    """Count, win rate, average price paid, return on money spent, and a 90% bootstrap range."""
    if not trades:
        return {"n": 0, "win": NAN, "price": NAN, "roi": NAN, "lo": NAN, "hi": NAN, "pnl": 0.0}
    t = np.array(trades)
    pnl, cost = t[:, 0], t[:, 1]
    out = {"n": len(t), "win": float((pnl > 0).mean()), "price": float(cost.mean()),
           "roi": float(pnl.sum() / cost.sum()), "pnl": float(pnl.sum()), "lo": NAN, "hi": NAN}  # fmt: skip
    if len(t) >= 20:
        rng = rng or np.random.default_rng(0)
        idx = rng.integers(0, len(t), size=(n_boot, len(t)))
        rois = pnl[idx].sum(axis=1) / cost[idx].sum(axis=1)
        out["lo"], out["hi"] = (float(x) for x in np.percentile(rois, [5, 95]))
    return out


def rule_trades(markets, gap_bps: float, left: int, latency: int, shares: float, mode: str):
    """One decision per market: `left` seconds before the end, buy the side the price favours by
    at least `gap_bps`. Returns (trades, decisions) where decisions counts orders attempted."""
    trades, attempts = [], 0
    for md in markets:
        sec = md.dur - left
        gap = projected_gap(md, sec)
        if gap != gap or abs(gap) * 1e4 < gap_bps:
            continue
        attempts += 1
        fn = taker if mode == "taker" else maker
        res = fn(md, gap > 0, sec + latency) if mode == "taker" else fn(md, gap > 0, sec + latency, shares)
        if res:
            trades.append(res)
    return trades, attempts


def features(md: MarketData, sec: int) -> list[float]:
    cl, cl30 = md.at("cl", sec), md.at("cl", sec - 30)
    bu, au, su, sau = (md.at(k, sec) for k in ("bu", "au", "su", "sau"))
    hist = md.a["cl"][max(0, sec + PRE - 60) : sec + PRE + 1]
    hist = hist[~np.isnan(hist)]
    vol = float(np.std(np.diff(np.log(hist)))) * 1e4 if len(hist) > 10 else NAN
    mid, mid30 = (bu + au) / 2, (md.at("bu", sec - 30) + md.at("au", sec - 30)) / 2
    return [projected_gap(md, sec) * 1e4, (cl / md.ptb - 1) * 1e4, (cl / cl30 - 1) * 1e4, (md.dur - sec) / md.dur,
            float(np.log10(md.dur - sec + 1)), mid, au - bu, su / (su + sau) if su + sau > 0 else NAN, mid - mid30, vol]  # fmt: skip


FEATURES = ("gap_bps", "spot_gap_bps", "move_30s_bps", "frac_left", "log_secs_left", "mid_up", "spread",
            "book_imbalance", "mid_move_30s", "vol_60s_bps")  # fmt: skip


def fmt_pct(x: float) -> str:
    return "   n/a" if x != x else f"{100 * x:+6.1f}%"


def print_grid(title: str, markets, lefts, latency, shares, mode) -> None:
    print(f"\n{title}")
    print(f"{'gap >=':>8} {'secs left':>9} {'orders':>7} {'filled':>7} {'win %':>6} {'avg price':>9} {'return':>8}   90% range")
    for left in lefts:
        for gap in GAPS_BPS:
            tr, att = rule_trades(markets, gap, left, latency, shares, mode)
            s = summarize(tr)
            if att:
                rng = f"{fmt_pct(s['lo'])} to {fmt_pct(s['hi'])}" if s["n"] >= 20 else "(too few trades)"
                win = f"{100 * s['win']:6.1f}" if s["n"] else "   n/a"
                print(f"{gap:>5} bp {left:>9} {att:>7} {s['n']:>7} {win} {s['price']:>9.3f} {fmt_pct(s['roi']):>8}   {rng}")


def backtest(cfg: Config, coin: str, dur: str, latency: int = 1, shares: float = 10.0, margin: float = 0.02) -> int:
    try:
        import duckdb
    except ImportError:
        print("this command needs duckdb:  pip install duckdb")
        return 1
    root = Path(cfg.data_dir)
    days = sorted(p.name[5:] for p in (root / "derived" / "markets").glob("date=????-??-??"))
    con = duckdb.connect()
    markets: list[MarketData] = []
    for day in days:
        markets += load_day(con, root, coin, dur, day)
    markets.sort(key=lambda m: m.start)
    if len(markets) < 50:
        print(f"only {len(markets)} resolved {coin} {dur} markets with ticks: not enough to backtest yet")
        return 1
    with_cl = sum(bool(np.any(~np.isnan(m.a["cl"]))) for m in markets)
    span = (markets[-1].start - markets[0].start) / 86400
    print(f"{coin.upper()} {dur}: {len(markets)} resolved markets over {span:.1f} days "
          f"({days[0]} to {days[-1]}); Up won {100 * np.mean([m.up_won for m in markets]):.1f}%")  # fmt: skip
    if with_cl < len(markets) * 0.5:
        print(f"only {with_cl} markets have Chainlink prices: check the Chainlink feed is recording")
        return 1
    print(f"orders execute {latency}s after the decision; taker fee {FEE_RATE} x p x (1-p); maker orders of {shares:g} shares")
    lefts = SECS_LEFT[dur]
    cut = int(len(markets) * 0.7)
    select, validate = markets[:cut], markets[cut:]

    for mode, label in (("taker", "TAKING the ask (pays the fee)"), ("maker", "POSTING at the bid (no fee, fills only if traded through)")):
        print_grid(f"Rule: buy the side the price favours - {label}. All markets (in-sample, for orientation only):",
                   markets, lefts, latency, shares, mode)  # fmt: skip
        best, best_roi = None, -9.0
        for left in lefts:
            for gap in GAPS_BPS:
                s = summarize(rule_trades(select, gap, left, latency, shares, mode)[0])
                if s["n"] >= 30 and s["roi"] > best_roi:
                    best, best_roi = (gap, left), s["roi"]
        if best is None:
            print("  no rule had 30+ trades in the earlier 70% of markets: too little data to pick one")
            continue
        tr, att = rule_trades(validate, best[0], best[1], latency, shares, mode)
        s = summarize(tr)
        print(f"  Honest test: best rule on the earlier 70% was gap >= {best[0]} bp with {best[1]}s left "
              f"(return {fmt_pct(best_roi).strip()} there).")  # fmt: skip
        print(f"  On the later 30% it placed {att} orders, {s['n']} filled, win {100 * s['win']:.1f}%, "
              f"return {fmt_pct(s['roi']).strip()}"
              + (f", 90% range {fmt_pct(s['lo']).strip()} to {fmt_pct(s['hi']).strip()}" if s["n"] >= 20 else " (too few trades for a range)")
              + f", P&L {s['pnl'] * shares:+.2f} USD at {shares:g} shares a trade." if s["n"] else
              f"  On the later 30% it placed {att} orders and none filled.")  # fmt: skip

    ml_section(select, validate, lefts, latency, shares, margin, span)
    return 0


def ml_section(select, validate, lefts, latency, shares, margin, span) -> None:
    print("\nMachine-learning model (gradient-boosted trees), trained on the earlier 70%, tested on the later 30%:")
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier
    except ImportError:
        print("  skipped: needs scikit-learn  (pip install scikit-learn)")
        return

    def rows(ms):
        X, y, ref = [], [], []
        for md in ms:
            for left in lefts:
                sec = md.dur - left
                f = features(md, sec)
                if f[0] != f[0] or f[5] != f[5]:
                    continue
                X.append(f)
                y.append(md.up_won)
                ref.append((md, sec))
        return np.array(X, dtype=float), np.array(y, dtype=int), ref

    Xs, ys, _ = rows(select)
    Xv, yv, ref = rows(validate)
    if len(Xs) < 500 or len(Xv) < 200:
        print(f"  skipped: {len(Xs)} training and {len(Xv)} test rows are too few")
        return
    model = HistGradientBoostingClassifier(max_depth=3, max_iter=150, learning_rate=0.05, l2_regularization=1.0,
                                           random_state=0).fit(Xs, ys)  # fmt: skip
    p = model.predict_proba(Xv)[:, 1]
    mid = np.clip(Xv[:, FEATURES.index("mid_up")], 0.001, 0.999)
    brier_model, brier_mkt = float(np.mean((p - yv) ** 2)), float(np.mean((mid - yv) ** 2))
    print(f"  {len(Xs)} training rows, {len(Xv)} test rows from {len(validate)} markets.")
    print(f"  Forecast error (Brier, lower is better): model {brier_model:.4f}, market price {brier_mkt:.4f} "
          f"-> model is {100 * (1 - brier_model / brier_mkt):+.1f}% vs the market.")  # fmt: skip
    print(f"  Accuracy: model {100 * np.mean((p > 0.5) == yv):.1f}%, market price {100 * np.mean((mid > 0.5) == yv):.1f}%.")
    trades, seen = [], set()
    for prob, (md, sec) in zip(p, ref):  # first qualifying signal per market, taking the ask
        if id(md) in seen:
            continue
        for buy_up, q in ((True, prob), (False, 1 - prob)):
            ask = md.at("au" if buy_up else "ad", sec + latency)
            if 0.01 <= ask <= 0.99 and q - ask - fee(ask) > margin:
                res = taker(md, buy_up, sec + latency)
                if res:
                    trades.append(res)
                    seen.add(id(md))
                break
    s = summarize(trades)
    if s["n"]:
        print(f"  Trading rule (take the ask when the model beats price + fee by {margin:.2f}): {s['n']} trades, "
              f"win {100 * s['win']:.1f}%, avg price {s['price']:.3f}, return {fmt_pct(s['roi']).strip()}"
              + (f", 90% range {fmt_pct(s['lo']).strip()} to {fmt_pct(s['hi']).strip()}" if s["n"] >= 20 else " (too few trades for a range)")
              + f", P&L {s['pnl'] * shares:+.2f} USD at {shares:g} shares a trade.")  # fmt: skip
    else:
        print(f"  Trading rule: the model never beat price + fee by {margin:.2f}, so it made no trades.")
    if span < 14:
        print(f"  PROVISIONAL: {span:.1f} days of data. A model can fit noise on this little; re-run as data accumulates.")
