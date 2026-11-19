"""`polyrec stats`: baseline numbers from the backfilled history (how the whole market did)."""
from __future__ import annotations

from pathlib import Path

from .config import COINS, Config


def stats(cfg: Config, coins: list[str]) -> int:
    try:
        import duckdb
    except ImportError:
        print("this command needs duckdb:  pip install duckdb")
        return 1
    coins = [c.lower() for c in coins] or list(cfg.coins)
    bad = [c for c in coins if c not in COINS]
    if bad:
        print(f"unknown coin(s) {bad}; choose from {list(COINS)}")
        return 1
    root = Path(cfg.data_dir) / "backfill"
    markets, trades = f"{root}/markets/*/*/*.parquet", f"{root}/trades/*/*/*.parquet"
    if not list(root.glob("markets/*/*/*.parquet")):
        print(f"no backfilled markets under {root}: run `polyrec backfill` first")
        return 1
    inlist = ", ".join(f"'{c}'" for c in coins)
    order = "max(epoch(market_end) - epoch(market_start))"  # shortest windows first
    print("Windows and how often Up won (all backfilled history):")
    duckdb.sql(f"""
        SELECT coin, duration, count(*) AS windows, min(market_start)::DATE AS first_day, max(market_start)::DATE AS last_day,
               round(100.0 * avg(CASE WHEN outcome = 'Up' THEN 1 ELSE 0 END), 1) AS up_won_pct
        FROM read_parquet('{markets}', hive_partitioning = true)
        WHERE coin IN ({inlist}) AND outcome IS NOT NULL
        GROUP BY coin, duration ORDER BY coin, {order}""").show(max_rows=100)
    if not list(root.glob("trades/*/*/*.parquet")):
        print("no backfilled trades yet")
        return 0
    print("Every BUY trade, held to the end ($1 per winning share, $0 per losing one):")
    duckdb.sql(f"""
        SELECT t.coin, t.duration, count(*) AS buys,
               round(100.0 * avg(CASE WHEN t.outcome = m.outcome THEN 1 ELSE 0 END), 1) AS win_pct,
               round(sum(t.price * t.size)) AS usd_spent,
               round(sum(t.size * ((CASE WHEN t.outcome = m.outcome THEN 1 ELSE 0 END) - t.price))) AS pnl_before_fees,
               round(sum(t.size * 0.07 * t.price * (1 - t.price))) AS taker_fees_max
        FROM read_parquet('{trades}', hive_partitioning = true) t
        JOIN read_parquet('{markets}', hive_partitioning = true) m USING (condition_id)
        WHERE t.side = 'BUY' AND m.outcome IS NOT NULL AND t.coin IN ({inlist})
        GROUP BY t.coin, t.duration ORDER BY t.coin, {order.replace('market_', 'm.market_')}""").show(max_rows=100)
    return 0
