# polyrec

Records Polymarket's crypto **Up or Down** markets so that trading bots can be backtested on them.

- **Coins:** BTC, ETH, SOL, XRP, DOGE, HYPE, BNB
- **Windows:** 5 minutes, 15 minutes, 1 hour, 4 hours (28 market series)
- **What is kept:** every order-book message, every trade print, official results, the
  Chainlink prices the markets settle on, and a ready-made one-row-per-second table.

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
Chainlink feed  ▶│ stops delivering. Chainlink and Binance prices, verbatim.      │
Binance sockets ▶│                                                                │
                 └───────────────────────────────────────────────────────────────┘
                 ┌────────────── polyrec derive (daily timer) ───────────────────┐
data/raw ───────▶│ replays the raw logs into Parquet: markets, ticks, l2, trades  │──▶ data/derived/
                 └───────────────────────────────────────────────────────────────┘
```

The markets are spread over several websocket connections (`ws_connections`, default 8):
Polymarket drops a client that falls behind on a busy connection, and with the load split a
drop interrupts only that connection's markets, which REST snapshots cover until it is back.

The recorder does no parsing: it writes what it receives and nothing else, so a parsing bug can
never lose data. Tables can be rebuilt from the raw logs at any time while those are retained
(3 days by default).

## Install on a VPS

Needs Python 3.11+ (Ubuntu 24.04 or Debian 12). 2 vCPU / 2 GB RAM is plenty; see
[Disk](#disk) for storage.

```bash
git clone -b polyrec https://github.com/PrimroseBlack/polymm
```

```bash
sudo bash polymm/polyrec/deploy/install.sh
```

```bash
sudo -u polyrec /opt/polyrec/.venv/bin/polyrec -c /etc/polyrec.toml smoke
```

```bash
sudo systemctl enable --now polyrec.service polyrec-derive.timer
```

`smoke` takes about 40 seconds and prints PASS/FAIL per feed. **Run it before starting the
service** — see [What has and has not been verified](#what-has-and-has-not-been-verified).
For Chainlink prices, add credentials first: see [Chainlink prices](#chainlink-prices).

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

## Chainlink prices

The 5m, 15m and 4h markets settle on a 60-second average of Chainlink's price. Polymarket
streams that feed only to clients holding CLOB API credentials (an API key, secret and
passphrase tied to a wallet). Without them the recorder runs normally, skips this feed, and
says so in its log at startup.

On the VPS, after `install.sh`:

```bash
sudo /opt/polyrec/.venv/bin/polyrec clob-creds --out /etc/polyrec.env
```

```bash
sudo chown root:polyrec /etc/polyrec.env && sudo chmod 640 /etc/polyrec.env
```

```bash
sudo systemctl restart polyrec
```

`clob-creds` creates a brand-new, empty wallet just for the recorder, asks Polymarket for API
credentials for it, writes them to the file, and discards the wallet's key. Nothing of value is
tied to those credentials. Then run `smoke` again: it reports whether the Chainlink feed works.

If Polymarket rejects credentials from a wallet that has never traded (not verified either
way), use your trading wallet instead: `clob-creds --prompt` asks for its private key with
hidden input, uses it once in memory to sign Polymarket's login message, and stores only the
resulting API credentials. Those cannot place orders or move funds, but they do open that
account's private API, so keep `/etc/polyrec.env` private.

What gets recorded: the live Chainlink price and its 60-second average for all seven coins,
about once a second each. They appear as `cl` and `cl_twap` in `ticks` and as rows in `ref`.

## Commands

| Command | What it does |
|---|---|
| `polyrec record` | The recorder. Runs until stopped; systemd restarts it. |
| `polyrec smoke` | Checks every live feed from this host. |
| `polyrec status` | Health of the running recorder. |
| `polyrec derive --pending` | Builds tables for every complete day not yet built (the timer runs this at 07:15 UTC). `--date YYYY-MM-DD` rebuilds one day. |
| `polyrec report --date D` | Coverage for a derived day: missing and incomplete windows per series. |
| `polyrec prune` | Deletes raw hour-files that are already derived and older than `raw_retention_days`, never a day that has not been derived. Also drops `l2` partitions older than `l2_retention_days`, if set. |
| `polyrec trial` | Replays the last 30 minutes in a scratch folder and reports replay speed, projected table sizes per day, and whether the rebuilt books agree with the exchange. Changes nothing. |
| `polyrec backfill --start D --end D` | Downloads results and trades for past markets. `--coins`, `--durations`, `--no-trades` narrow it. Resumable. |
| `polyrec seed-kacho` | Downloads Kacho's dataset (~725 MB) into `data/seed/kacho/`. |
| `polyrec clob-creds` | Creates the API credentials the Chainlink feed needs. |
| `polyrec stats [coins]` | From the backfill: how often Up won per market type, and the combined result of every buy trade held to the end. A market-wide baseline, not a strategy. |

## The data

Everything is under `data_dir`. Times are UTC. A market belongs to the day its window **starts**.

```
raw/<stream>/<YYYY-MM-DD>/<stream>-<YYYYMMDD>T<HH>.jsonl[.zst]   ws, rest, meta, chainlink, aux_<feed>
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
| `volume` | USDC volume as Polymarket reports it 30 minutes after the window (it updates this figure in batches, so earlier readings are too low). |
| `tick_size`, `fee_rate`, `resolution_source` | As listed when the market was discovered. |
| `n_ticks_window` / `expected_ticks` | Seconds of the window that have a tick / seconds in the window. |
| `rest_ticks` | Ticks that came from REST snapshots because the websocket was down. |
| `n_trades`, `n_l2` | Rows in the other tables. |
| `resyncs`, `resync_dirty` | The exchange re-sends the full book on every trade. `resyncs` counts those snapshots; `resync_dirty` counts the ones that differed from the book rebuilt from the changes before it. This is the direct check that the replay is right: `resync_dirty` should be at or near zero. |
| `rest_checks`, `rest_mismatches` | Routine REST cross-checks and how many disagreed on the top of book. Disagreement here is expected on busy markets: the two are read ~100–300 ms apart. |
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
| `cl` | Chainlink price of the coin, as last received before `t`. Null if nothing arrived in the previous 30 s (or the feed is not configured). |
| `cl_twap` | Chainlink's 60-second average, same rules. At a window's start it is that window's price to beat; at its end, the settlement price. |

`bu au bd ad su sd sau sad du dd` have the same meaning as in Kacho's dataset. NaN means that
side of the book was empty.

### `l2` — every book change

For event-level backtests. Only the **Up** book is stored: the Down book is its mirror image
(a Down bid at p is an Up ask at 1 − p, same size). Set `l2_both_outcomes = true` to store
both. Per token (`condition_id` + `outcome`), in receive order:

| `kind` | Meaning |
|---|---|
| 2 reset | A snapshot follows: clear the book. |
| 1 level | One level of that snapshot: set `side` (0 bid, 1 ask) at `price` to `size`. |
| 0 delta | Set that level to `size`; 0 removes it. |
| 3 desync | The feed dropped: the book is unknown until the next reset. |

A full-book snapshot the replay already holds is stored as its differences only, so a reset
appears only after the feed was lost. `r` is the local receive time in nanoseconds, `ts_ms`
the exchange timestamp, `src` 0 websocket / 1 REST. `polyrec.replay.replay(rows)` applies these and yields the book after each row.

### `trades`, `ref`

`trades`: `r`, `ts_ms`, `condition_id`, `coin`, `duration`, `outcome`, `side`, `price`, `size`,
`fee_rate_bps`, `tx_hash` — one row per trade print.

`ref`: `r`, `ts_ms`, `feed`, `symbol`, `price`, `snapshot` — every reference price received.

| `feed` | What it is |
|---|---|
| `chainlink` | Live Chainlink price (`btcusd`, …). |
| `chainlink_twap` | Its 60-second average: what 5m, 15m and 4h markets settle on. |
| `binance_spot` | 1-second Binance closes for six coins (`BTCUSDT`, …; HYPE is not on Binance spot). Hourly markets settle on the Binance 1h candle. |

`snapshot` = 1 marks points the Chainlink feed replayed after a reconnect to fill a gap: their
`ts_ms` is when they happened, `r` is when they were received.

### Getting the data onto your own computer

The tables are plain Parquet files, so copying the folder is all it takes. From your own
machine (not the server), leaving out the large order-book-change table:

```bash
mkdir -p ~/polyrec-data && rsync -av --exclude 'l2/' root@YOUR_SERVER_IP:/var/lib/polyrec/derived/ ~/polyrec-data/derived/
```

Run it again any time: only new files are transferred. Add the history from `polyrec backfill`
and `polyrec seed-kacho`, or one day of `l2`, the same way:

```bash
rsync -av root@YOUR_SERVER_IP:/var/lib/polyrec/backfill root@YOUR_SERVER_IP:/var/lib/polyrec/seed ~/polyrec-data/
```

```bash
rsync -av root@YOUR_SERVER_IP:/var/lib/polyrec/derived/l2/date=2026-11-16 ~/polyrec-data/derived/l2/
```

### Querying

[DuckDB](https://duckdb.org) reads the files in place; so do pandas and polars. Run these from
`~/polyrec-data` (or `/var/lib/polyrec` on the server).

```sql
-- What has been recorded: markets and seconds of data per series
SELECT coin, duration, count(*) AS markets, sum(n_ticks_window) AS seconds_recorded
FROM read_parquet('derived/markets/*/markets.parquet', hive_partitioning = true)
GROUP BY ALL ORDER BY duration, coin;
```

```sql
-- Per-second implied probability of Up for BTC 5m markets, with Chainlink price and the result
SELECT t.ts_utc, t.sec, (t.bu + t.au) / 2 AS p_up, t.cl, m.price_to_beat, m.outcome
FROM read_parquet('derived/ticks/*/ticks.parquet', hive_partitioning = true) t
JOIN read_parquet('derived/markets/*/markets.parquet', hive_partitioning = true) m USING (condition_id)
WHERE t.coin = 'btc' AND t.duration = '5m' AND t.sec >= 0
ORDER BY t.ts_utc;
```

```python
import duckdb
ticks = duckdb.sql("SELECT * FROM read_parquet('derived/ticks/*/ticks.parquet', hive_partitioning = true) "
                   "WHERE coin = 'btc' AND duration = '5m' AND date = '2026-11-16'").df()
```

## Fees, for backtests

Takers pay `shares × 0.07 × p × (1 − p)` USDC (1.75% of notional at 50¢); makers pay nothing.
The rate in force is recorded per market in `fee_rate`. Taker orders on crypto markets are also
delayed (150 ms at the time of writing), which a 1 Hz table cannot model — use `l2` for that.

## Disk

The order-book feed is heavy. Measured on 16 Nov 2026 with all 28 series: about 3,800
messages a second, 8 GB of raw log an hour, compressing to roughly 0.8 GB an hour (~20 GB a
day). Budget for:

- the current hour, uncompressed (~8 GB);
- compressed raw logs from the start of the oldest day not yet derived (up to ~33 hours),
  or `raw_retention_days` if that is longer;
- the tables, which grow every day. `polyrec trial` projects their size from real data.

```bash
sudo -u polyrec /opt/polyrec/.venv/bin/polyrec -c /etc/polyrec.toml trial
```

The order-book-change table (`l2`) is nearly all of the table growth, about 2 GB a day.
`l2_retention_days = 10` keeps a rolling ten days of it and everything else forever; the
other tables together grow about 6 GB a month.

If it is still tight: lower `raw_retention_days`, lower `depth_levels`, record fewer
`durations`, or attach a larger volume and point `data_dir` at it.

## What has and has not been verified

| Part | Status |
|---|---|
| Discovery of all 28 series, REST snapshots, results, the replay into tables | Run against live Polymarket data. |
| Backfill of results and trades | Run against live Polymarket data. |
| Market websocket (subscribe, heartbeat, snapshots, adding markets on the fly) | Passed `polyrec smoke` against Polymarket from a Hetzner server in Helsinki. Reconnect and stall handling are tested only against a local stand-in. |
| Chainlink feed and `clob-creds` | Passed `polyrec smoke` against Polymarket, with credentials from a throwaway wallet. |
| Binance spot feed | Passed `polyrec smoke`. |
| Recording at full volume | Running on a Hetzner CPX22: ~3,800 messages a second across four connections at 12% CPU. One connection carrying everything was dropped by Polymarket every 30 seconds. |
| Rebuilt order books equal the exchange's | **Not yet confirmed on real data.** `polyrec trial` and the `resyncs` / `resync_dirty` columns measure it. Raw logs are verbatim, so tables can be rebuilt while those are kept. |
| A full day of unattended recording, and the daily derive at real data volumes | **Not yet run.** On synthetic data at the real rate the replay handles ~70,000 messages a second on one core. |
| `seed-kacho` download | Not run end to end. |

## Development

```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
```

```bash
.venv/bin/pytest
```
