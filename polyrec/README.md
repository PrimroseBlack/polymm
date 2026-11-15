# polyrec

Records Polymarket's crypto **Up or Down** markets so that trading bots can be backtested on them.

- **Coins:** BTC, ETH, SOL, XRP, DOGE, HYPE, BNB
- **Windows:** 5 minutes, 15 minutes, 1 hour, 4 hours (28 market series)
- **What is kept:** every order-book message, every trade print, official results, and
  1-second reference prices, plus a ready-made one-row-per-second table.

Polymarket deletes a market's order book the moment its window closes, so this history can only
be captured live. Trades and results can be fetched afterwards (`polyrec backfill`).

The idea and the 1 Hz table layout follow
[Kacho's dataset](https://kacho.io/polymarket-5min-crypto-dataset) (5m markets, Mar–May 2026),
which `polyrec seed-kacho` downloads as a historical seed.

## How it works

```
                 ┌────────────── polyrec record (always running) ──────────────┐
Gamma API ──────▶│ discovery: current + next windows of all 28 series, every 30 s │
market websocket▶│ every message, verbatim, with a nanosecond receive time       │──▶ data/raw/*.jsonl(.zst)
CLOB REST /books▶│ snapshot every 30 s as a cross-check; every 1 s if the socket  │
Binance sockets ▶│ stops delivering. Reference prices recorded verbatim.          │
                 └───────────────────────────────────────────────────────────────┘
                 ┌────────────── polyrec derive (daily timer) ───────────────────┐
data/raw ───────▶│ replays the raw logs into Parquet: markets, ticks, l2, trades  │──▶ data/derived/
                 └───────────────────────────────────────────────────────────────┘
```

The recorder does no parsing: it writes what it receives and nothing else, so a parsing bug can
never lose data. Tables can be rebuilt from the raw logs at any time while those are retained
(14 days by default).

## Install on a VPS

Needs Python 3.11+ (Ubuntu 24.04 or Debian 12). 2 vCPU / 2 GB RAM is plenty; see
[Disk](#disk) for storage.

```bash
git clone <this repository> polymarket-crypto-recorder
```

```bash
sudo bash polymarket-crypto-recorder/deploy/install.sh
```

```bash
sudo -u polyrec /opt/polyrec/.venv/bin/polyrec -c /etc/polyrec.toml smoke
```

```bash
sudo systemctl enable --now polyrec.service polyrec-derive.timer
```

`smoke` takes about 40 seconds and prints PASS/FAIL per feed. **Run it before starting the
service** — see [What has and has not been verified](#what-has-and-has-not-been-verified).

Day to day:

```bash
journalctl -u polyrec -f
```

```bash
sudo -u polyrec /opt/polyrec/.venv/bin/polyrec -c /etc/polyrec.toml status
```

```bash
sudo -u polyrec /opt/polyrec/.venv/bin/polyrec -c /etc/polyrec.toml report --date 2026-11-16
```

`status` exits non-zero when the recorder is not running or the websocket is down, so it can be
wired into any monitor. Settings live in `/etc/polyrec.toml`
([all options](polyrec.example.toml)); restart the service after editing.

## Commands

| Command | What it does |
|---|---|
| `polyrec record` | The recorder. Runs until stopped; systemd restarts it. |
| `polyrec smoke` | Checks every live feed from this host. |
| `polyrec status` | Health of the running recorder. |
| `polyrec derive --pending` | Builds tables for every complete day not yet built (the timer runs this at 07:15 UTC). `--date YYYY-MM-DD` rebuilds one day. |
| `polyrec report --date D` | Coverage for a derived day: missing and incomplete windows per series. |
| `polyrec prune` | Deletes raw logs older than `raw_retention_days` that are already derived. |
| `polyrec backfill --start D --end D` | Downloads results and trades for past markets. `--coins`, `--durations`, `--no-trades` narrow it. Resumable. |
| `polyrec seed-kacho` | Downloads Kacho's dataset (~725 MB) into `data/seed/kacho/`. |

## The data

Everything is under `data_dir`. Times are UTC. A market belongs to the day its window **starts**.

```
raw/<stream>/<YYYY-MM-DD>/<stream>-<YYYYMMDD>T<HH>.jsonl[.zst]   ws, rest, meta, aux_<feed>
derived/markets/date=YYYY-MM-DD/markets.parquet
derived/ticks/date=YYYY-MM-DD/ticks.parquet
derived/l2/date=YYYY-MM-DD/l2.parquet
derived/trades/date=YYYY-MM-DD/trades.parquet
derived/ref/date=YYYY-MM-DD/ref.parquet
derived/_done/YYYY-MM-DD.json                                     coverage report
backfill/{markets,trades}/coin=<c>/duration=<d>/YYYY-MM-DD.parquet
seed/kacho/<coin>_{markets,ticks}.parquet
```

### `markets` — one row per window

`condition_id` (join key), `event_id`, `market_id`, `slug`, `coin`, `duration`, `market_start`,
`market_end`, `token_up`, `token_down`, then:

| Column | Meaning |
|---|---|
| `outcome` | Official result, `Up` / `Down`, read from Polymarket after resolution. Null if unresolved. |
| `price_to_beat`, `final_price` | The reference price at window start and end. Polymarket publishes these only **after** the window ends, and not at all for 4h markets. |
| `volume` | USDC volume at resolution. |
| `tick_size`, `fee_rate`, `resolution_source` | As listed when the market was discovered. |
| `n_ticks_window` / `expected_ticks` | Seconds of the window that have a tick / seconds in the window. |
| `rest_ticks` | Ticks that came from REST snapshots because the websocket was down. |
| `n_trades`, `n_l2` | Rows in the other tables. |
| `rest_checks`, `rest_mismatches` | Routine REST cross-checks and how many disagreed on the top of book. Some disagreement is normal: the two are read ~100–300 ms apart. |
| `ws_outcome` | Winner announced on the websocket, if seen. |

### `ticks` — one row per market per second

The book **as of** instant `t`: everything received before `t`, nothing after. Rows run from
`lead_seconds` before the window opens to its last second. A second with no trustworthy book
has no row.

| Column | Meaning |
|---|---|
| `condition_id`, `coin`, `duration` | |
| `t`, `ts_utc`, `sec` | Unix second, the same as a timestamp, and seconds since window start (negative before it opens). |
| `bu` `au` / `bd` `ad` | Best bid and ask of the Up / Down token (0–1 USDC). |
| `su` `sau` / `sd` `sad` | Shares resting at those four prices. |
| `du` `dau` / `dd` `dad` | USDC resting within 5¢ of the best bid and best ask, Up / Down. |
| `bid_px` `bid_sz` `ask_px` `ask_sz` | Top `depth_levels` levels of the **Up** book, best first. The Down book is its mirror image (Down bid at p = Up ask at 1 − p). |
| `src` | 0 = live websocket book, 1 = REST snapshot. |
| `age_ms` | Milliseconds since the book last changed. |

`bu au bd ad su sd sau sad du dd` have the same meaning as in Kacho's dataset. NaN means that
side of the book was empty.

### `l2` — every book change

For event-level backtests. Per token (`condition_id` + `outcome`), in receive order:

| `kind` | Meaning |
|---|---|
| 2 reset | A snapshot follows: clear the book. |
| 1 level | One level of that snapshot: set `side` (0 bid, 1 ask) at `price` to `size`. |
| 0 delta | Set that level to `size`; 0 removes it. |
| 3 desync | The feed dropped: the book is unknown until the next reset. |

`r` is the local receive time in nanoseconds, `ts_ms` the exchange timestamp, `src` 0 websocket /
1 REST. `polyrec.replay.replay(rows)` applies these and yields the book after each row.

### `trades`, `ref`

`trades`: `r`, `ts_ms`, `condition_id`, `coin`, `duration`, `outcome`, `side`, `price`, `size`,
`fee_rate_bps`, `tx_hash` — one row per trade print.

`ref`: `r`, `ts_ms`, `feed`, `symbol`, `price` — 1-second Binance closes (spot for six coins,
futures mark price for HYPE). These are a convenience signal, **not** the settlement price:
5m/15m/4h markets settle on a 60-second Chainlink average, hourly markets on the Binance 1h candle.

### Querying

```sql
-- DuckDB: implied probability of Up for BTC 5m markets, with the result
SELECT t.ts_utc, t.sec, (t.bu + t.au) / 2 AS p_up, m.outcome
FROM read_parquet('data/derived/ticks/*/ticks.parquet', hive_partitioning = true) t
JOIN read_parquet('data/derived/markets/*/markets.parquet', hive_partitioning = true) m USING (condition_id)
WHERE t.coin = 'btc' AND t.duration = '5m' AND t.date = '2026-11-16';
```

## Fees, for backtests

Takers pay `shares × 0.07 × p × (1 − p)` USDC (1.75% of notional at 50¢); makers pay nothing.
The rate in force is recorded per market in `fee_rate`. Taker orders on crypto markets are also
delayed (150 ms at the time of writing), which a 1 Hz table cannot model — use `l2` for that.

## Disk

Not yet measured on a real host; the websocket volume is the unknown. Measured so far:

- REST snapshots in fallback mode (websocket down): ~7.5 MB/minute uncompressed.
- Routine REST cross-checks: 30× less (~360 MB/day before compression).

After the first full day, check `du -sh /var/lib/polyrec/raw/* /var/lib/polyrec/derived/*` and
set `raw_retention_days` and `depth_levels` to fit. Start with at least 80 GB.

## What has and has not been verified

This was built in a sandbox that cannot open websocket connections, so:

| Part | Status |
|---|---|
| Discovery of all 28 series, REST snapshots, results, the replay into tables | Run against live Polymarket data. |
| Backfill of results and trades | Run against live Polymarket data. |
| Websocket client (subscribe, heartbeat, reconnect, stall detection) | Tested against a local stand-in server that follows Polymarket's documented protocol. **Not yet run against Polymarket itself.** |
| Binance reference feeds | Written from the documented message formats. **Not yet run.** If one fails, the recorder logs it and carries on. |
| `seed-kacho` download, `deploy/install.sh` | Not run end to end. |

`polyrec smoke` exercises the unverified feeds in under a minute.

## Development

```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
```

```bash
.venv/bin/pytest
```
