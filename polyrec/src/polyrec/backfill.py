"""History that can still be fetched after the fact: market results and trades (never order books)."""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pyarrow as pa

from .config import DURATIONS, Config, series_slug
from .derive import F8, I8, TS, Sink, _f
from .gamma import official_outcome, parse_market
from .util import day_range, day_start, get, iso

log = logging.getLogger(__name__)

BF_MARKETS = pa.schema(
    [("condition_id", pa.string()), ("event_id", pa.string()), ("market_id", pa.string()), ("slug", pa.string()),
     ("coin", pa.string()), ("duration", pa.string()), ("market_start", TS), ("market_end", TS),
     ("token_up", pa.string()), ("token_down", pa.string()),
     ("outcome", pa.string()), ("price_to_beat", F8), ("final_price", F8), ("volume", F8),
     ("resolution_source", pa.string())]
)  # fmt: skip
BF_TRADES = pa.schema(
    [("ts", I8), ("condition_id", pa.string()), ("coin", pa.string()), ("duration", pa.string()),
     ("outcome", pa.string()), ("side", pa.string()), ("price", F8), ("size", F8),
     ("token_id", pa.string()), ("tx_hash", pa.string()), ("wallet", pa.string())]
)  # fmt: skip


def list_events(client: httpx.Client, gamma_url: str, series_id: str, t0: int, t1: int) -> list[dict]:
    """Closed events of a series whose end time lies in [t0, t1]."""
    out, offset = [], 0
    while True:
        params = {"series_id": series_id, "closed": "true", "end_date_min": iso(t0), "end_date_max": iso(t1),
                  "order": "endDate", "ascending": "true", "limit": 100, "offset": offset}  # fmt: skip
        page = get(client, f"{gamma_url}/events", params).json()
        out.extend(page)
        if len(page) < 100:
            return out
        offset += len(page)


def fetch_trades(client: httpx.Client, data_api_url: str, condition_id: str) -> list[dict]:
    out, cursor = [], None
    while True:
        params = {"condition": condition_id, "limit": 500}
        if cursor:
            params["cursor"] = cursor
        body = get(client, f"{data_api_url}/v2/trades", params, retries=9).json()
        out.extend(body.get("data") or [])
        page = body.get("pagination") or {}
        cursor = page.get("next_cursor")
        if not page.get("has_more") or not cursor:
            return out


def backfill(cfg: Config, start: str, end: str, coins=None, durations=None, *, trades: bool = True) -> int:
    root = Path(cfg.data_dir) / "backfill"
    coins = coins or cfg.coins
    durations = durations or cfg.durations
    today = day_start(time.strftime("%Y-%m-%d", time.gmtime()))
    days = [d for d in day_range(start, end) if day_start(d) + 86400 + 4 * 3600 <= time.time()]
    if len(days) < len(day_range(start, end)):
        log.warning("skipping days that are not complete yet (on or after %s)", iso(today)[:10])
    failed: list[str] = []
    with httpx.Client(timeout=30, headers={"User-Agent": "polyrec"}) as client, ThreadPoolExecutor(4) as pool:
        for coin in coins:
            for dur in durations:
                slug = series_slug(coin, dur)
                data = get(client, f"{cfg.gamma_url}/series", {"slug": slug}).json()
                if not data:
                    log.error("series %s not found", slug)
                    continue
                sid = str(data[0]["id"])
                for day in days:
                    part = f"coin={coin}/duration={dur}"
                    mpath = root / "markets" / part / f"{day}.parquet"
                    tpath = root / "trades" / part / f"{day}.parquet"
                    if mpath.exists() and (tpath.exists() or not trades):
                        continue
                    d0 = day_start(day)
                    events = list_events(client, cfg.gamma_url, sid, d0, d0 + 86400 + DURATIONS[dur])
                    markets = []
                    for ev in events:
                        m = parse_market(ev, coin, dur)
                        if m and d0 <= m.start_ts < d0 + 86400:
                            markets.append((m, ev))
                    n_tr = 0
                    if trades:
                        sink = Sink(tpath, BF_TRADES)
                        try:
                            results = pool.map(lambda me: fetch_trades(client, cfg.data_api_url, me[0].condition_id), markets)
                            for (m, _), rows in zip(markets, results):
                                for t in sorted(rows, key=lambda t: t.get("timestamp") or 0):
                                    sink.add(t.get("timestamp"), m.condition_id, coin, dur, t.get("outcome"), t.get("side"),
                                             _f(t.get("price")), _f(t.get("size")), t.get("token_id"),
                                             t.get("transaction_hash"), t.get("proxy_wallet"))  # fmt: skip
                        except Exception as e:  # noqa: BLE001 - one bad day must not stop the rest
                            sink.abort()
                            failed.append(f"{day} {coin} {dur}")
                            log.error("%s %s %s: trades failed, will be retried on the next run: %s: %s",
                                      day, coin, dur, type(e).__name__, str(e)[:300])  # fmt: skip
                            time.sleep(5)
                            continue
                        sink.close()
                        n_tr = sink.rows
                    sink = Sink(mpath, BF_MARKETS)  # written last: its presence marks the day complete
                    for m, ev in markets:
                        m0, emd = ev["markets"][0], ev.get("eventMetadata") or {}
                        sink.add(m.condition_id, m.event_id, m.market_id, m.slug, coin, dur, m.start_ts, m.end_ts,
                                 m.token_up, m.token_down, official_outcome(ev), _f(emd.get("priceToBeat")),
                                 _f(emd.get("finalPrice")), _f(m0.get("volumeNum")), m0.get("resolutionSource"))  # fmt: skip
                    sink.close()
                    log.info("%s %s %s: %d markets, %d trades", day, coin, dur, len(markets), n_tr)
    if failed:
        log.error("%d series-days failed and are left for the next run, e.g. %s", len(failed), ", ".join(failed[:5]))
    return 1 if failed else 0


KACHO = "kachoio/polymarket-5-minute-crypto-up-down-markets"


def seed_kacho(cfg: Config, *, markets_only: bool = False) -> int:
    """Download Kacho's CC0 dataset (24 Mar - 18 May 2026, 5m markets, 1 Hz top of book)."""
    dest = Path(cfg.data_dir) / "seed" / "kacho"
    dest.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=60, follow_redirects=True, headers={"User-Agent": "polyrec"}) as client:
        tree = get(client, f"https://huggingface.co/api/datasets/{KACHO}/tree/main").json()
        for f in tree:
            name, size = f["path"], f.get("size", 0)
            if not name.endswith(".parquet") or (markets_only and "_markets" not in name):
                continue
            path = dest / name
            if path.exists() and path.stat().st_size == size:
                continue
            log.info("downloading %s (%.0f MB)", name, size / 1e6)
            tmp = path.with_name(name + ".part")
            with client.stream("GET", f"https://huggingface.co/datasets/{KACHO}/resolve/main/{name}") as r:
                r.raise_for_status()
                with open(tmp, "wb") as out:
                    for chunk in r.iter_bytes(1 << 20):
                        out.write(chunk)
            if tmp.stat().st_size != size:
                log.error("%s: got %d bytes, expected %d", name, tmp.stat().st_size, size)
                return 1
            tmp.replace(path)
    log.info("Kacho dataset is in %s", dest)
    return 0
