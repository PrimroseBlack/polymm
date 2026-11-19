from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from pathlib import Path

from .config import load_config, load_env_file


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="polyrec", description=__doc__)
    p.add_argument("-c", "--config", help="path to polyrec.toml (default: ./polyrec.toml if present)")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("record", help="run the recorder (foreground; use systemd to keep it running)")
    s = sub.add_parser("smoke", help="check from this host that every live feed works")
    s.add_argument("--seconds", type=int, default=20)
    sub.add_parser("status", help="show the running recorder's status; exit 1 if it looks unhealthy")

    s = sub.add_parser("derive", help="build the Parquet tables from raw logs")
    g = s.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", help="UTC date (YYYY-MM-DD) of market start")
    g.add_argument("--pending", action="store_true", help="every complete day not yet derived")
    s.add_argument("--no-fetch", action="store_true", help="do not query Gamma for missing results")

    s = sub.add_parser("report", help="print the coverage report for a derived day")
    s.add_argument("--date", required=True)

    sub.add_parser("prune", help="delete raw logs that are derived and past raw_retention_days")
    s = sub.add_parser("trial", help="replay recent raw data in a scratch folder: speed, sizes, book check")
    s.add_argument("--minutes", type=int, default=30)

    s = sub.add_parser("backfill", help="download historical markets and trades (no order books exist)")
    s.add_argument("--start", required=True, help="first UTC date, YYYY-MM-DD")
    s.add_argument("--end", required=True, help="last UTC date, YYYY-MM-DD")
    s.add_argument("--coins", nargs="*")
    s.add_argument("--durations", nargs="*")
    s.add_argument("--no-trades", action="store_true", help="markets and results only")

    s = sub.add_parser("seed-kacho", help="download Kacho's Mar-May 2026 dataset as a historical seed")
    s.add_argument("--markets-only", action="store_true")

    s = sub.add_parser("backtest", help="replay rule and ML strategies against the recorded order book")
    s.add_argument("--coin", default="btc")
    s.add_argument("--duration", default="5m", choices=["5m", "15m", "1h", "4h"])
    s.add_argument("--latency", type=int, default=1, help="seconds between decision and execution (default 1)")
    s.add_argument("--shares", type=float, default=10.0, help="order size in shares (default 10)")

    s = sub.add_parser("stats", help="win rates and buy-and-hold P&L per market type, from the backfill")
    s.add_argument("coins", nargs="*", help="coins to include, e.g. btc eth (default: all)")

    s = sub.add_parser("clob-creds", help="create the API credentials the Chainlink feed needs")
    s.add_argument("--out", default="polyrec.env", help="file to write (default: ./polyrec.env)")
    g = s.add_mutually_exclusive_group()
    g.add_argument("--key-env", metavar="VAR", help="use an existing wallet: private key in this environment variable")
    g.add_argument("--prompt", action="store_true", help="use an existing wallet: type its private key (hidden)")
    s.add_argument("--force", action="store_true", help="replace the output file if it exists")

    a = p.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if a.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    cfg = load_config(a.config)
    load_env_file(cfg.env_file)
    load_env_file("polyrec.env", quiet=True)  # convenience for running from a checkout

    if a.cmd == "record":
        from .recorder import Recorder

        return asyncio.run(Recorder(cfg).run())
    if a.cmd == "smoke":
        from .smoke import smoke

        return asyncio.run(smoke(cfg, a.seconds))
    if a.cmd == "status":
        path = Path(cfg.data_dir) / "status.json"
        if not path.exists():
            print(f"no status file at {path}: the recorder has not run here")
            return 1
        st = json.loads(path.read_text())
        age = time.time() - st["time"]
        print(json.dumps(st, indent=2))
        problems = []
        if age > 60:
            problems.append(f"status is {age:.0f}s old: recorder not running")
        if not st["ws_connected"]:
            problems.append("market websocket is down" + (" (REST fallback active)" if st["rest_fallback"] else ""))
        for msg in problems:
            print("PROBLEM:", msg)
        return 1 if problems else 0
    if a.cmd == "derive":
        from .derive import derive_day, pending_days

        days = [a.date] if a.date else pending_days(cfg)
        if not days:
            print("nothing pending")
        for day in days:
            derive_day(cfg, day, fetch_missing=not a.no_fetch)
        return 0
    if a.cmd == "report":
        from .derive import print_report

        return print_report(cfg, a.date)
    if a.cmd == "trial":
        from .derive import trial

        return trial(cfg, a.minutes)
    if a.cmd == "prune":
        from .derive import prune

        prune(cfg)
        return 0
    if a.cmd == "backfill":
        from .backfill import backfill

        return backfill(cfg, a.start, a.end, a.coins, a.durations, trades=not a.no_trades)
    if a.cmd == "backtest":
        from .backtest import backtest

        return backtest(cfg, a.coin.lower(), a.duration, a.latency, a.shares)
    if a.cmd == "stats":
        from .stats import stats

        return stats(cfg, a.coins)
    if a.cmd == "clob-creds":
        from .creds import clob_creds_cmd

        return clob_creds_cmd(cfg, a.out, a.key_env, a.prompt, a.force)
    if a.cmd == "seed-kacho":
        from .backfill import seed_kacho

        return seed_kacho(cfg, markets_only=a.markets_only)
    return 2


if __name__ == "__main__":
    sys.exit(main())
