"""Replay raw logs into Parquet tables, one partition per UTC day of market start.

Tables written under <data_dir>/derived/:
  markets/  one row per market (ids, timing, official result, coverage statistics)
  ticks/    one row per market per second: top of book for Up and Down, depth, top-N levels
  l2/       every order-book change, enough to rebuild the full book at any instant
  trades/   every trade print from the websocket
  ref/      reference prices parsed from the auxiliary feeds (best effort)
"""
from __future__ import annotations

import heapq
import json
import logging
import math
import shutil
import time
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from .book import NAN, Book
from .config import DURATIONS, Config
from .gamma import Market, official_outcome, parse_market
from .rawlog import iter_raw
from .util import day_of, day_start, get

log = logging.getLogger(__name__)
NS = 1_000_000_000
WS_LIVE_NS = 15 * NS  # heartbeats arrive every 5 s
REST_FRESH_NS = int(3.5 * NS)  # how long a REST snapshot stands in for the live book
TS = pa.timestamp("s", "UTC")
F8, I8 = pa.float64(), pa.int64()

TICKS = pa.schema(
    [("condition_id", pa.string()), ("coin", pa.string()), ("duration", pa.string()),
     ("t", I8), ("ts_utc", TS), ("sec", pa.int32()),
     ("bu", F8), ("au", F8), ("bd", F8), ("ad", F8),
     ("su", F8), ("sau", F8), ("sd", F8), ("sad", F8),
     ("du", F8), ("dau", F8), ("dd", F8), ("dad", F8),
     ("bid_px", pa.list_(F8)), ("bid_sz", pa.list_(F8)), ("ask_px", pa.list_(F8)), ("ask_sz", pa.list_(F8)),
     ("src", pa.int8()), ("age_ms", I8)]
)  # fmt: skip
L2 = pa.schema(
    [("r", I8), ("ts_ms", I8), ("condition_id", pa.string()), ("outcome", pa.string()),
     ("kind", pa.int8()), ("side", pa.int8()), ("price", F8), ("size", F8), ("src", pa.int8())]
)  # fmt: skip
TRADES = pa.schema(
    [("r", I8), ("ts_ms", I8), ("condition_id", pa.string()), ("coin", pa.string()), ("duration", pa.string()),
     ("outcome", pa.string()), ("side", pa.string()), ("price", F8), ("size", F8),
     ("fee_rate_bps", F8), ("tx_hash", pa.string())]
)  # fmt: skip
MARKETS = pa.schema(
    [("condition_id", pa.string()), ("event_id", pa.string()), ("market_id", pa.string()), ("slug", pa.string()),
     ("coin", pa.string()), ("duration", pa.string()), ("market_start", TS), ("market_end", TS),
     ("token_up", pa.string()), ("token_down", pa.string()),
     ("outcome", pa.string()), ("price_to_beat", F8), ("final_price", F8), ("volume", F8),
     ("tick_size", F8), ("fee_rate", F8), ("resolution_source", pa.string()),
     ("n_ticks", I8), ("n_ticks_window", I8), ("expected_ticks", I8), ("rest_ticks", I8),
     ("n_trades", I8), ("n_l2", I8), ("rest_checks", I8), ("rest_mismatches", I8),
     ("ws_outcome", pa.string())]
)  # fmt: skip
REF = pa.schema([("r", I8), ("ts_ms", I8), ("feed", pa.string()), ("symbol", pa.string()), ("price", F8)])

# l2.kind
DELTA, LEVEL, RESET, DESYNC = 0, 1, 2, 3


class Sink:
    """Buffered Parquet writer; the file appears under its final name only when closed."""

    def __init__(self, path: Path, schema: pa.Schema, batch: int = 200_000):
        self.path, self.schema, self.batch = path, schema, batch
        self.tmp = path.with_name(path.name + ".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.writer = pq.ParquetWriter(self.tmp, schema, compression="zstd")
        self.cols: list[list] = [[] for _ in schema]
        self.rows = 0

    def add(self, *values) -> None:
        for col, v in zip(self.cols, values):
            col.append(v)
        if len(self.cols[0]) >= self.batch:
            self.flush()

    def extend(self, columns: list[list]) -> None:
        for col, vals in zip(self.cols, columns):
            col.extend(vals)
        if len(self.cols[0]) >= self.batch:
            self.flush()

    def flush(self) -> None:
        n = len(self.cols[0])
        if n:
            arrays = [pa.array(c, type=f.type) for c, f in zip(self.cols, self.schema)]
            self.writer.write_table(pa.Table.from_arrays(arrays, schema=self.schema))
            self.rows += n
            self.cols = [[] for _ in self.schema]

    def close(self) -> None:
        self.flush()
        self.writer.close()
        self.tmp.replace(self.path)


class BookS(Book):
    """Book plus the bookkeeping needed to decide whether it can be trusted at a given instant."""

    __slots__ = ("synced", "known", "base_ts", "rest_ns")

    def __init__(self):
        super().__init__()
        self.synced = False  # websocket deltas have been continuous since the last snapshot
        self.known = False  # a reader of the l2 table currently holds this book's content
        self.base_ts = 0  # exchange ms of a REST base snapshot (0 for websocket snapshots)
        self.rest_ns = 0  # when a REST snapshot was last applied

    def usable(self, tns: int, ws_live: bool) -> bool:
        return self.src is not None and ((self.synced and ws_live) or tns - self.rest_ns < REST_FRESH_NS)


class MState:
    def __init__(self, m: Market, discovered: dict):
        self.m, self.discovered = m, discovered
        self.final: dict | None = None
        self.up, self.down = BookS(), BookS()
        self.rows: list[list] = [[] for _ in range(len(TICKS) - 3)]
        self.n_ticks = self.n_ticks_window = self.rest_ticks = 0
        self.n_trades = self.n_l2 = self.rest_checks = self.rest_mismatches = 0
        self.ws_outcome: str | None = None


def load_meta(root: Path, start_ts: int, end_ts: int) -> dict[str, MState]:
    out: dict[str, MState] = {}
    late: list[dict] = []
    for rec in iter_raw(root, "meta", start_ts * NS, end_ts * NS):
        ev, cid, kind = rec.get("ev"), rec.get("cid"), rec.get("k")
        if not ev or not cid:
            continue
        if kind == "discovered":
            m = parse_market(ev, rec["coin"], rec["dur"])
            if m and cid not in out:
                out[cid] = MState(m, ev)
        else:
            late.append(rec)
    for rec in late:
        ms = out.get(rec["cid"])
        if ms is None:
            continue
        if rec["k"] in ("final", "final_timeout"):
            ms.final = rec["ev"]
    return out


def _best_of(levels, pick) -> float:
    return pick(float(l["price"]) for l in levels) if levels else NAN


def _same(a: float, b: float) -> bool:
    return (math.isnan(a) and math.isnan(b)) or abs(a - b) < 1e-9


def _f(x) -> float | None:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def derive_day(cfg: Config, day: str, *, fetch_missing: bool = True) -> dict:
    """Build every table for markets that started on `day` (UTC). Safe to re-run."""
    t_run = time.monotonic()
    root = Path(cfg.data_dir)
    out = root / "derived"
    d0 = day_start(day)
    d1 = d0 + 86400
    lead, nlev = cfg.lead_seconds, cfg.depth_levels
    states = [ms for ms in load_meta(root, d0 - 86400, d1 + 8 * 3600).values() if d0 <= ms.m.start_ts < d1]
    states.sort(key=lambda ms: (ms.m.start_ts, ms.m.coin, ms.m.duration))
    log.info("%s: %d markets in the metadata log", day, len(states))

    part = f"date={day}"
    ticks = Sink(out / "ticks" / part / "ticks.parquet", TICKS, batch=50_000)
    l2 = Sink(out / "l2" / part / "l2.parquet", L2)
    trades = Sink(out / "trades" / part / "trades.parquet", TRADES)

    tokmap: dict[str, tuple[MState, bool]] = {}
    for ms in states:
        tokmap[ms.m.token_up] = (ms, True)
        tokmap[ms.m.token_down] = (ms, False)
    by_cid = {ms.m.condition_id: ms for ms in states}

    live: list[MState] = []
    nxt = 0  # index into states of the next market to activate
    ws_connected = False
    last_ws_ns = 0
    stats = {"ws_records": 0, "rest_records": 0, "deltas_skipped": 0, "disconnects": 0}

    def desync(ns: int) -> None:
        for ms in live:
            for b, name in ((ms.up, "Up"), (ms.down, "Down")):
                if b.synced:
                    b.synced = b.known = False
                    l2.add(ns, 0, ms.m.condition_id, name, DESYNC, -1, None, None, 0)

    def finish(ms: MState) -> None:
        n = len(ms.rows[0])
        if n:
            m = ms.m
            ticks.extend([[m.condition_id] * n, [m.coin] * n, [m.duration] * n, *ms.rows])
        ms.rows = [[] for _ in ms.rows]
        for bk in (ms.up, ms.down):
            bk.invalidate()
            bk.synced = bk.known = False

    def emit(ms: MState, t: int, ws_live: bool) -> None:
        tns = t * NS
        up, dn = ms.up, ms.down
        uu, ud = up.usable(tns, ws_live), dn.usable(tns, ws_live)
        if not ((uu and (up.bids or up.asks)) or (ud and (dn.bids or dn.asks))):
            return
        bu, su, au, sau = up.best() if uu else (NAN,) * 4
        bd, sd, ad, sad = dn.best() if ud else (NAN,) * 4
        du, dau = up.depth() if uu else (NAN, NAN)
        dd, dad = dn.depth() if ud else (NAN, NAN)
        bp, bs, ap, az = up.top(nlev) if uu else ([], [], [], [])
        rest = (uu and not (up.synced and ws_live)) or (ud and not (dn.synced and ws_live))
        age = (tns - max(up.upd_ns if uu else 0, dn.upd_ns if ud else 0)) // 1_000_000
        sec = t - ms.m.start_ts
        for col, v in zip(
            ms.rows,
            (t, t, sec, bu, au, bd, ad, su, sau, sd, sad, du, dau, dd, dad, bp, bs, ap, az, int(rest), age),
        ):
            col.append(v)
        ms.n_ticks += 1
        ms.rest_ticks += rest
        ms.n_ticks_window += 0 <= sec

    def advance(t: int) -> None:
        """Emit the tick for second t: the state as of instant t, before anything received later."""
        nonlocal nxt, ws_connected
        while nxt < len(states) and states[nxt].m.start_ts - lead <= t:
            live.append(states[nxt])
            nxt += 1
        ws_live = ws_connected and t * NS - last_ws_ns < WS_LIVE_NS
        if ws_connected and not ws_live:  # the log went silent without a disconnect record
            ws_connected = False
            stats["disconnects"] += 1
            desync(t * NS)
        for ms in live[:]:
            if t >= ms.m.end_ts:
                finish(ms)
                live.remove(ms)
            else:
                emit(ms, t, ws_live)

    def snapshot(ms: MState, b: BookS, name: str, bids, asks, r: int, ts: int, rest: bool) -> None:
        cid, src = ms.m.condition_id, int(rest)
        old = (b.bids, b.asks) if rest and b.known and b.src == "rest" else None
        b.snapshot(bids, asks, "rest" if rest else "ws", r)
        if old is not None:
            # Consecutive REST snapshots (one per second in fallback): store only what changed.
            for side, (was, now) in enumerate(zip(old, (b.bids, b.asks))):
                for p in was.keys() - now.keys():
                    l2.add(r, ts, cid, name, DELTA, side, p / 10_000, 0.0, src)
                    ms.n_l2 += 1
                for p, s in now.items():
                    if was.get(p) != s:
                        l2.add(r, ts, cid, name, DELTA, side, p / 10_000, s, src)
                        ms.n_l2 += 1
            return
        b.known = True
        l2.add(r, ts, cid, name, RESET, -1, None, None, src)
        for side, levels in ((0, b.bids), (1, b.asks)):
            for p, s in levels.items():
                l2.add(r, ts, cid, name, LEVEL, side, p / 10_000, s, src)
        ms.n_l2 += 1 + len(b.bids) + len(b.asks)

    def on_ws(rec: dict) -> None:
        nonlocal ws_connected, last_ws_ns
        r = rec["r"]
        last_ws_ns = r
        ctl = rec.get("ctl")
        if ctl:
            if ctl in ("start", "stop", "connected", "disconnected"):
                if ctl == "disconnected":
                    stats["disconnects"] += 1
                ws_connected = ctl == "connected"
                desync(r)
            elif ctl == "hb":  # the connection may have been opened before the replayed range
                ws_connected = True
            return
        msg = rec.get("m")
        if msg is None:
            return
        ws_connected = True
        for e in msg if isinstance(msg, list) else (msg,):
            et = e.get("event_type")
            if et == "price_change":
                ts = int(e.get("timestamp") or 0)
                changes = e.get("price_changes")
                if changes is None:  # legacy shape: one asset per message
                    changes = [{**c, "asset_id": e.get("asset_id")} for c in e.get("changes", ())]
                for c in changes:
                    hit = tokmap.get(c.get("asset_id"))
                    if hit is None:
                        continue
                    ms, is_up = hit
                    b = ms.up if is_up else ms.down
                    if b.src is None or not b.synced or ts <= b.base_ts:
                        stats["deltas_skipped"] += 1
                        continue
                    is_bid = c["side"] == "BUY"
                    price, size = float(c["price"]), float(c["size"])
                    b.apply(is_bid, price, size, r)
                    l2.add(r, ts, ms.m.condition_id, "Up" if is_up else "Down", DELTA, 0 if is_bid else 1, price, size, 0)
                    ms.n_l2 += 1
            elif et == "book":
                hit = tokmap.get(e.get("asset_id"))
                if hit:
                    ms, is_up = hit
                    b = ms.up if is_up else ms.down
                    snapshot(ms, b, "Up" if is_up else "Down", e.get("bids") or (), e.get("asks") or (), r,
                             int(e.get("timestamp") or 0), rest=False)  # fmt: skip
                    b.synced, b.base_ts = True, 0
            elif et == "last_trade_price":
                hit = tokmap.get(e.get("asset_id"))
                if hit:
                    ms, is_up = hit
                    m = ms.m
                    trades.add(r, int(e.get("timestamp") or 0), m.condition_id, m.coin, m.duration,
                               "Up" if is_up else "Down", e.get("side"), _f(e.get("price")), _f(e.get("size")),
                               _f(e.get("fee_rate_bps")), e.get("transaction_hash"))  # fmt: skip
                    ms.n_trades += 1
            elif et == "market_resolved":
                ms = by_cid.get(e.get("market"))
                if ms:
                    ms.ws_outcome = e.get("winning_outcome")

    def on_rest(rec: dict) -> None:
        r, books = rec["r"], rec.get("m")
        if not isinstance(books, list):
            return
        ws_live = ws_connected and r - last_ws_ns < WS_LIVE_NS
        for bk in books:
            hit = tokmap.get(bk.get("asset_id"))
            if hit is None:
                continue
            ms, is_up = hit
            b = ms.up if is_up else ms.down
            bids, asks = bk.get("bids") or (), bk.get("asks") or ()
            if b.src is not None and b.synced and ws_live:
                bb, _, ba, _ = b.best()
                ok = _same(bb, _best_of(bids, max)) and _same(ba, _best_of(asks, min))
                ms.rest_checks += 1
                ms.rest_mismatches += not ok
                if ok or not rec.get("fb"):
                    continue  # the live book stands; a mismatch outside fallback is just timing
            ts = int(bk.get("timestamp") or 0)
            snapshot(ms, b, "Up" if is_up else "Down", bids, asks, r, ts, rest=True)
            b.rest_ns, b.base_ts, b.synced = r, ts, ws_live

    if states:
        t_begin = states[0].m.start_ts - lead
        t_end = max(ms.m.end_ts for ms in states) + cfg.linger_seconds + 5
        merged = heapq.merge(
            ((rec["r"], 0, rec) for rec in iter_raw(root, "ws", t_begin * NS, t_end * NS)),
            ((rec["r"], 1, rec) for rec in iter_raw(root, "rest", t_begin * NS, t_end * NS)),
            key=lambda x: x[0],
        )
        next_t = t_begin
        for r, which, rec in merged:
            sec = r // NS
            while next_t <= sec:
                advance(next_t)
                next_t += 1
            if which == 0:
                stats["ws_records"] += 1
                on_ws(rec)
            else:
                stats["rest_records"] += 1
                on_rest(rec)
        for ms in live:
            finish(ms)

    # Markets table.
    client = httpx.Client(timeout=15, headers={"User-Agent": "polyrec"}) if fetch_missing else None
    now = time.time()
    rows = []
    for ms in states:
        m = ms.m
        if client and not (ms.final and official_outcome(ms.final)) and now > m.end_ts + 60:
            try:
                data = get(client, f"{cfg.gamma_url}/events", {"slug": m.slug}).json()
                ms.final = data[0] if data else ms.final
            except Exception as e:  # noqa: BLE001
                log.warning("could not fetch result for %s: %s", m.slug, e)
        ev = ms.final or ms.discovered
        m0 = ev["markets"][0]
        d0m = ms.discovered["markets"][0]
        emd = ev.get("eventMetadata") or {}
        rows.append(
            dict(condition_id=m.condition_id, event_id=m.event_id, market_id=m.market_id, slug=m.slug,
                 coin=m.coin, duration=m.duration, market_start=m.start_ts, market_end=m.end_ts,
                 token_up=m.token_up, token_down=m.token_down,
                 outcome=official_outcome(ms.final) if ms.final else None,
                 price_to_beat=_f(emd.get("priceToBeat")), final_price=_f(emd.get("finalPrice")),
                 volume=_f(m0.get("volumeNum")), tick_size=_f(d0m.get("orderPriceMinTickSize")),
                 fee_rate=_f((d0m.get("feeSchedule") or {}).get("rate")),
                 resolution_source=d0m.get("resolutionSource"),
                 n_ticks=ms.n_ticks, n_ticks_window=ms.n_ticks_window, expected_ticks=m.end_ts - m.start_ts,
                 rest_ticks=ms.rest_ticks, n_trades=ms.n_trades, n_l2=ms.n_l2,
                 rest_checks=ms.rest_checks, rest_mismatches=ms.rest_mismatches, ws_outcome=ms.ws_outcome)
        )  # fmt: skip
    if client:
        client.close()
    # A window's final price is the next window's price to beat; fill either from the other.
    by_start = {(r["coin"], r["duration"], r["market_start"]): r for r in rows}
    for r in rows:
        nxt_row = by_start.get((r["coin"], r["duration"], r["market_end"]))
        if nxt_row:
            if r["final_price"] is None:
                r["final_price"] = nxt_row["price_to_beat"]
            if nxt_row["price_to_beat"] is None:
                nxt_row["price_to_beat"] = r["final_price"]
    for sink in (ticks, l2, trades):
        sink.close()
    mk = Sink(out / "markets" / part / "markets.parquet", MARKETS)
    for row in rows:
        mk.add(*(row[name] for name in MARKETS.names))
    mk.close()
    n_ref = derive_ref(cfg, day)

    report = build_report(day, rows)
    report.update(stats, ticks=ticks.rows, l2=l2.rows, trades=trades.rows, ref=n_ref,
                  seconds=round(time.monotonic() - t_run, 1), derived_at=time.time())  # fmt: skip
    done = out / "_done" / f"{day}.json"
    done.parent.mkdir(parents=True, exist_ok=True)
    done.write_text(json.dumps(report, indent=1))
    log.info("%s: %d markets, %d ticks, %d l2 rows, %d trades in %.0fs",
             day, len(rows), ticks.rows, l2.rows, trades.rows, report["seconds"])  # fmt: skip
    return report


def parse_ref(msg) -> tuple[int, str, float] | None:
    """(exchange ms, symbol, price) from a Binance kline or mark-price message, else None."""
    try:
        d = msg["data"] if "data" in msg else msg
        if d.get("e") == "kline":
            k = d["k"]
            return (int(k["T"]), d["s"], float(k["c"])) if k.get("x") else None
        if d.get("e") == "markPriceUpdate":
            return int(d["E"]), d["s"], float(d["p"])
    except (KeyError, TypeError, ValueError):
        pass
    return None


def derive_ref(cfg: Config, day: str) -> int:
    root = Path(cfg.data_dir)
    d0 = day_start(day)
    sink = Sink(root / "derived" / "ref" / f"date={day}" / "ref.parquet", REF)
    for stream_dir in sorted((root / "raw").glob("aux_*")):
        feed = stream_dir.name[4:]
        for rec in iter_raw(root, stream_dir.name, d0 * NS, (d0 + 86400) * NS):
            hit = parse_ref(rec["m"]) if isinstance(rec.get("m"), dict) else None
            if hit:
                sink.add(rec["r"], hit[0], feed, hit[1], hit[2])
    sink.close()
    return sink.rows


def build_report(day: str, rows: list[dict]) -> dict:
    """Coverage per series: which windows are missing or incomplete."""
    d0 = day_start(day)
    series = {}
    for key in sorted({(r["coin"], r["duration"]) for r in rows}, key=lambda k: (DURATIONS[k[1]], k[0])):
        mine = [r for r in rows if (r["coin"], r["duration"]) == key]
        dur = DURATIONS[key[1]]
        offset = min(r["market_start"] for r in mine) % dur
        have = {r["market_start"] for r in mine}
        expected = [t for t in range(d0 + (offset - d0) % dur, d0 + 86400, dur)]
        full = [r for r in mine if r["n_ticks_window"] >= 0.98 * r["expected_ticks"]]
        series["-".join(key)] = {
            "expected": len(expected),
            "recorded": len(mine),
            "complete": len(full),
            "resolved": sum(r["outcome"] is not None for r in mine),
            "missing_seconds": sum(max(0, r["expected_ticks"] - r["n_ticks_window"]) for r in mine)
            + dur * sum(t not in have for t in expected),
            "rest_seconds": sum(r["rest_ticks"] for r in mine),
            "missing_windows": [t for t in expected if t not in have],
            "incomplete_windows": [r["market_start"] for r in mine if r not in full],
        }
    return {"date": day, "markets": len(rows), "series": series}


def print_report(cfg: Config, day: str) -> int:
    path = Path(cfg.data_dir) / "derived" / "_done" / f"{day}.json"
    if not path.exists():
        print(f"{day} has not been derived yet (run: polyrec derive --date {day})")
        return 1
    rep = json.loads(path.read_text())
    print(f"{day}: {rep['markets']} markets, {rep['ticks']} ticks, {rep['l2']} l2 rows, {rep['trades']} trades, "
          f"{rep['disconnects']} websocket disconnects")  # fmt: skip
    print(f"{'series':10} {'expected':>8} {'recorded':>8} {'complete':>8} {'resolved':>8} {'missing s':>9} {'REST s':>7}")
    for name, s in rep["series"].items():
        print(f"{name:10} {s['expected']:8} {s['recorded']:8} {s['complete']:8} {s['resolved']:8} "
              f"{s['missing_seconds']:9} {s['rest_seconds']:7}")  # fmt: skip
    return 0


def pending_days(cfg: Config) -> list[str]:
    """Days with raw data whose markets (and their results) are all in, and not yet derived."""
    root = Path(cfg.data_dir)
    cutoff = time.time() - 86400 - 6.5 * 3600  # last 4h window ends +4h; results can lag ~2h
    days = sorted(p.name for p in (root / "raw" / "ws").glob("????-??-??"))
    return [d for d in days if day_start(d) <= cutoff and not (root / "derived" / "_done" / f"{d}.json").exists()]


def prune(cfg: Config) -> None:
    """Delete raw day-directories older than the retention window once nothing needs them."""
    root = Path(cfg.data_dir)
    done = lambda d: (root / "derived" / "_done" / f"{d}.json").exists()  # noqa: E731
    cutoff = time.time() - cfg.raw_retention_days * 86400
    for stream_dir in sorted((root / "raw").glob("*")):
        for p in sorted(stream_dir.glob("????-??-??")):
            t = day_start(p.name)
            prev_dir = stream_dir / day_of(t - 86400)
            # A day's raw files feed three derive runs: the day before (4h spill-over), the day, the day after.
            if t + 86400 < cutoff and done(p.name) and done(day_of(t + 86400)) and (done(day_of(t - 86400)) or not prev_dir.exists()):
                shutil.rmtree(p)
                log.info("pruned %s", p)
